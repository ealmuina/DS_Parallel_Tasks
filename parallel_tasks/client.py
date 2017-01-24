import threading
from datetime import datetime
from queue import Queue

import Pyro4
import Pyro4.errors
from Pyro4 import socketutil as pyrosocket

from .libraries import heapq
from .libraries import log
from .libraries import utils
from .node import Node
from .worker import Worker

Pyro4.config.SERVERTYPE = "multiplex"
Pyro4.config.SERIALIZER = 'pickle'
Pyro4.config.SERIALIZERS_ACCEPTED.add('pickle')


@Pyro4.expose
class Client(Node):
    """
    Base class to inherit from when implementing a client program for using the Parallel Tasks system.
    """

    SUBTASKS_TIMEOUT = 30  # Time (seconds) waiting for assigned subtasks's result

    def __init__(self):
        super().__init__()

        self.workers_map = {}  # Maps a worker's URI to its corresponding WorkerInfo instance
        self.workers = []  # WorkerInfo heap storing the information about the known system workers
        self.workers_lock = threading.Lock()  # Lock for the concurrent use of self.workers and self.workers_map

        self.worker = Worker()  # System worker corresponding to this machine
        winfo = WorkerInfo(self.worker._local_uri)
        self.workers.append(winfo)
        self.workers_map[winfo.local_uri] = winfo

        self.log = log.Log('client')

        self.pending_tasks = set()
        self.pending_subtasks = Queue()
        self.pending_subtasks_dic = {}  # Maps a tuple (task_id, index) to its corresponding pending sub-task
        self.task_number = 0  # Integer used for tasks identifiers assignment

        threading.Thread(target=self._listen_loop, daemon=True).start()
        threading.Thread(target=self._subtasks_assign_loop, daemon=True).start()

        self.log.report('Initialized client.', True)

    def _listen_loop(self):
        """
        Listen to the network waiting for workers URIs.
        It's intended to run 'forever' on a separated thread.
        """

        with pyrosocket.createBroadcastSocket(('', 5555)) as listener:
            while True:
                try:
                    data, address = listener.recvfrom(1024)
                    uri = data.decode()
                except ConnectionResetError:
                    continue

                try:
                    winfo = WorkerInfo(uri)
                    if winfo.local_uri == self.worker._local_uri:
                        # Avoid to duplicate local worker.
                        continue

                    with self.workers_lock:
                        # Save or update information about the worker
                        old_winfo = self.workers_map.get(uri, None)

                        if old_winfo:
                            # It's an 'old known' worker, just update its info
                            old_winfo.load = winfo.load
                            heapq.siftup(self.workers, old_winfo.index)
                        else:
                            # New worker we didn't knew about!
                            self.workers_map[uri] = winfo
                            heapq.heappush(self.workers, winfo)

                    self.log.report('Received beep from worker %s.' % winfo.uri)

                except (TypeError, Pyro4.errors.PyroError):
                    # Invalid uri or TimeoutError, ConnectionClosedError
                    continue

    def _subtasks_assign_loop(self):
        """
        Check if the wait time for the result of some operation has expired.
        If this happens, the corresponding sub-task will be assigned to a new worker.
        It's intended to run 'forever' on a separated thread.
        """

        while True:
            t, st = self.pending_subtasks.get()

            if st.completed:
                # Sub-task is already completed. Continue iteration
                continue

            elapsed = datetime.now() - t
            if elapsed.total_seconds() > Client.SUBTASKS_TIMEOUT:
                # Waiting time exceeded, assign sub-task to a new worker

                with self.workers_lock:
                    winfo = heapq.heappop(self.workers)
                    self.workers_map.pop(winfo.uri)

                    try:
                        w = Pyro4.Proxy(winfo.uri)
                        w._pyroTimeout = Node.PYRO_TIMEOUT
                        w._pyroOneway.add('process')  # We don't need to wait for calls to n.process now

                        w.process(st.func, (st.task.id, st.index), self.uri)
                        st.time = datetime.now()

                        # Refresh the worker's load and put it back on the list
                        winfo.load = WorkerInfo(winfo.uri).load
                        heapq.heappush(self.workers, winfo)
                        self.workers_map[winfo.uri] = winfo

                        self.log.report('Subtask %s assigned to worker %s' % ((st.task.id, st.index), winfo.uri))

                    except Pyro4.errors.PyroError:
                        # TimeoutError, ConnectionClosedError
                        self.log.report(
                            "Attempted to assign subtask to worker %s, but it couldn't be accessed." % winfo.uri,
                            True, 'red')

            self.pending_subtasks.put((st.time, st))

    def close(self):
        super().close()
        self.worker.close()

    def get_data(self, subtask_id):
        """
        Return data corresponding to an uncompleted task.
        :param subtask_id: Sub-task whose task data is requested
        :return: data corresponding to subtask_id's task
        """

        if subtask_id in self.pending_subtasks_dic:
            subtask = self.pending_subtasks_dic[subtask_id]
            return subtask.task.data
        return None

    def get_module(self, module):
        """
        Return python module as a string.
        :param module: Module's name without the .py extension
        :return: string with the entire module's content
        """

        with open('parallel_tasks/libraries/%s.py' % module) as f:
            return f.read()

    def print_stats(self):
        """
        Print system statistics on console.
        """

        table = [('Worker', 'Operations', 'Total time', 'Average time')]

        with self.workers_lock:
            for winfo in self.workers:
                try:
                    n = Pyro4.Proxy(winfo.uri)
                    n._pyroTimeout = Node.PYRO_TIMEOUT

                    total_time = n.total_time
                    total_operations = n.total_operations
                    avg_time = total_time / total_operations if total_operations != 0 else 0

                    row = (winfo.uri.split('@')[-1], total_operations, total_time, avg_time)
                    table.append(tuple(map(str, row)))

                except Pyro4.errors.PyroError:
                    # Connection to worker couldn't be completed
                    continue

        utils.print_table(table)

    def report(self, subtask_id, result):
        """
        Report to client the result of an operation already completed by some worker.
        :param subtask_id: Identifier of the completed sub-task
        :param result: Operation's result
        """

        # Locate the corresponding sub-task with that 'subtask_id', and mark it as completed
        # If the sub-task isn't found, then it was already completed. End method call
        try:
            subtask = self.pending_subtasks_dic.pop(subtask_id)
        except KeyError:
            self.log.report(
                'A worker reported the result of an already completed operation. It will be dismissed.', True)
            return None  # to finish call

        subtask.completed = True
        current_task = subtask.task

        # Copy sub-task result to the corresponding task's one
        current_task.result[subtask.index] = result

        # Verify if task is now completed
        current_task.completed_subtasks += 1
        current_task.completed = current_task.completed_subtasks == len(current_task.result)
        self.log.report('Task %s progress is %s/100' %
                        (current_task.id, (current_task.completed_subtasks * 100 / len(current_task.result))), True)

        if current_task.completed:
            # Save task's result in file <current_task.id>.txt
            self.save_result(current_task)

            self.log.report(
                'Task %(id)s completed. You can check the result at the file %(id)s.txt.\nTotal time: %(time)s'
                % {'id': current_task.id, 'time': datetime.now() - current_task.time}, True, 'green')

            # Remove task from pending tasks list
            self.pending_tasks.remove(current_task)

    def save_result(self, task):
        """
        Save a task's results to a file. Not implemented as Client class is abstract.
        :param task: Task whose results will be saved
        """
        raise NotImplementedError()


class WorkerInfo:
    """
    Stores information relative to a worker. It's used to wrap a worker properties.
    """

    def __init__(self, uri):
        """
        Initialize a new WorkerInfo instance, which stores the load and uri of a system worker.
        :param uri: Pyro4 URI of the worker.
        """

        self.uri = uri
        self.index = -1

        worker = Pyro4.Proxy(self.uri)  # Raises TypeError if uri is not valid
        worker._pyroTimeout = Node.PYRO_TIMEOUT
        # PyroError will be raised on failure
        self.local_uri = worker.local_uri
        self.load = worker.load

    def __lt__(self, other):
        return self.load < other.load
