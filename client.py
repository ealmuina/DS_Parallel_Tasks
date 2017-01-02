import heapq
import threading
import time
from datetime import datetime
from queue import Queue

import Pyro4
from Pyro4 import socketutil as pyrosocket
from Pyro4.errors import PyroError

import log
from node import Node
from worker import Worker

Pyro4.config.COMMTIMEOUT = 5  # 5 seconds
Pyro4.config.SERVERTYPE = "multiplex"

Pyro4.config.SERIALIZER = 'pickle'
Pyro4.config.SERIALIZERS_ACCEPTED.add('pickle')


@Pyro4.expose
class Client(Node):
    """
    Base class to inherit from when implementing a client program for using the Parallel Tasks system.
    """

    SCANNER_TIMEOUT = 1  # Time (seconds) waiting for responses on the system scanner socket
    SCANNER_INTERVAL = 30  # Time (seconds) elapsed between system scans
    SUBTASKS_TIMEOUT = 60  # Time (seconds) waiting for assigned sub-tasks result

    def __init__(self):
        super().__init__()

        self.workers = []  # Accessible workers URI, prioritized by their load; stored as (load, URI)
        self.lock = threading.Lock()  # Lock for the concurrent use of self.workers

        self.worker = Worker()  # System worker corresponding to this machine

        self.log = log.Log('client')

        self.pending_tasks = set()
        self.pending_subtasks = Queue()
        self.pending_subtasks_dic = {}  # Maps a tuple (task_id, index) to its corresponding pending sub-task
        self.task_number = 0  # Integer used for tasks identifiers assignment

        threading.Thread(target=self._scan_loop).start()
        threading.Thread(target=self._subtasks_checker_loop).start()

        self.log.report('Cliente inicializado.', True)

    def _scan_loop(self):
        """
        Scan the system seeking for workers, and keep updated the priority queue with them.
        It's intended to run 'forever' on a separated thread.
        """

        while True:
            # Create a broadcast socket and send through it the word 'SCANNING'. Every worker that receive it should
            # respond back with its URI
            # All of the received uris will be put in a list first before further processing

            scanner = pyrosocket.createBroadcastSocket()
            scanner.settimeout(Client.SCANNER_TIMEOUT)

            updated_nodes = []
            try:
                scanner.sendto(b'SCANNING', ('255.255.255.255', 5555))
                while True:
                    try:
                        data, address = scanner.recvfrom(1024)
                    except ConnectionResetError:
                        # Connection closed. Continue iteration
                        continue

                    uri = data.decode()

                    try:
                        current_node = Pyro4.Proxy(uri)
                        updated_nodes.append((current_node.load, uri))

                    except PyroError:
                        # If received data isn't a valid uri,
                        # this exception will be thrown while trying to create a proxy
                        continue

            except OSError as e:
                if e.errno == 101:
                    # Network is disconnected. Local worker will be the only one used
                    updated_nodes.append((self.worker.load, self.worker.uri))

            heapq.heapify(updated_nodes)
            with self.lock:
                self.workers.clear()
                for n in updated_nodes:
                    self.workers.append(n)

            self.log.report('Sistema escaneado. Se detectaron %d workers.' % len(updated_nodes))
            time.sleep(Client.SCANNER_INTERVAL)  # rest some time before next scan

    def _subtasks_checker_loop(self):
        """
        Check if the wait time for the result of some operation has expired.
        If this happens, the corresponding sub-task will be assigned to a new worker.
        It's intended to run 'forever' on a separated thread.
        """

        while True:
            if len(self.workers) == 0:
                # No worker has been found on the system.
                continue

            t, st = self.pending_subtasks.get()
            elapsed = datetime.now() - t

            if st.completed:
                # Sub-task is already completed. Continue iteration
                continue

            if elapsed.total_seconds() > Client.SUBTASKS_TIMEOUT:
                # Wait time exceeded, assign sub-task to a new worker
                with self.lock:
                    load, uri = heapq.heappop(self.workers)
                    st.time = datetime.now()

                    try:
                        n = Pyro4.Proxy(uri)
                        n.process(st.func, (st.task.id, st.index), self.uri)
                        heapq.heappush(self.workers, (n.load, uri))

                        self.log.report('Asignada la subtarea %s al worker %s' % ((st.task.id, st.index), uri))

                    except PyroError:
                        self.log.report(
                            'Se intentó enviar subtarea al worker %s, pero no se encuentra accesible.' % uri,
                            True, 'red')

            self.pending_subtasks.put((st.time, st))

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
                'Un worker reportó el resultado de una operación ya completada. La respuesta será desechada.')
            return None

        subtask.completed = True

        current_task = subtask.task

        # Copy sub-task result to the corresponding task's one
        current_task.result[subtask.index] = result

        # Verify is task is now completed
        current_task.completed = True
        for x in current_task.result:
            current_task.completed = current_task.completed and x

        if current_task.completed:
            # Save task's result in file <current_task.id>.txt
            self.save_result(current_task)

            self.log.report(
                'Tarea %(id)s completada. Puede ver el resultado en el archivo %(id)s.txt.\nTiempo total: %(time)s'
                % {'id': current_task.id, 'time': datetime.now() - current_task.time}, True, 'green')

            # Remove task from pending tasks list
            self.pending_tasks.remove(current_task)

    def print_stats(self):
        """
        Print, on console, system statistics.
        """

        print('Worker', 'Operaciones', 'Tiempo total', 'Tiempo promedio', sep='\t')
        with self.lock:
            for load, uri in self.workers:
                try:
                    n = Pyro4.Proxy(uri)
                    avg_time = n.total_time / n.total_operations if n.total_operations != 0 else 0

                    print(n.ip_address, n.total_operations, n.total_time, avg_time, sep='\t')

                except PyroError:
                    # Connection to worker couldn't be completed
                    pass

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
