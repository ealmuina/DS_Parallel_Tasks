import heapq
import ipaddress
import os
import select
import threading
import time
from datetime import datetime
from queue import Queue

import Pyro4
import Pyro4.errors
from Pyro4 import socketutil as pyrosocket

import log
from node import Node

Pyro4.config.SERVERTYPE = "multiplex"
Pyro4.config.SERIALIZER = 'pickle'
Pyro4.config.SERIALIZERS_ACCEPTED.add('pickle')


@Pyro4.expose
class Client(Node):
    """
    Base class to inherit from when implementing a client program for using the Parallel Tasks system.
    """

    SCANNER_TIMEOUT = 1  # Time (seconds) waiting for responses on the system scanner socket
    SCANNER_INTERVAL = 15  # Time (seconds) elapsed between system scans
    SUBTASKS_TIMEOUT = 300  # Time (seconds) waiting for assigned sub-tasks result

    def __init__(self):
        super().__init__()

        self.workers = []  # Accessible workers URI, prioritized by their load; stored as (load, URI)
        self.lock = threading.Lock()  # Lock for the concurrent use of self.workers

        # self.worker = Worker()  # System worker corresponding to this machine

        self.log = log.Log('client')

        self.pending_tasks = set()
        self.pending_subtasks = Queue()
        self.pending_subtasks_dic = {}  # Maps a tuple (task_id, index) to its corresponding pending sub-task
        self.task_number = 0  # Integer used for tasks identifiers assignment

        threading.Thread(target=self._scan_loop, daemon=True).start()
        threading.Thread(target=self._subtasks_checker_loop, daemon=True).start()

        self.log.report('Cliente inicializado.', True)

    def _scan_loop(self):
        """
        Scan the system seeking for workers, and keep updated the priority queue with them.
        It broadcasts a known message on the network and to user-specified ip addresses on the 'ips.conf' file.
        It's intended to run 'forever' on a separated thread.
        """

        while True:
            # Create a broadcast socket and send through it the word 'SCANNING'. Every worker that receive it should
            # respond back with its URI
            # All of the received uris will be put in a set first before further processing

            scanner = pyrosocket.createBroadcastSocket(timeout=Client.SCANNER_TIMEOUT)

            uris = set()
            updated_nodes = []

            try:
                scanner.sendto(b'SCANNING', ('255.255.255.255', 5555))
                while True:
                    try:
                        uri = scanner.recv(1024).decode()
                        uris.add(uri)
                    except ConnectionResetError:
                        continue

            except OSError as e:
                scanner.close()

                if e.errno == 101:
                    # Network is disconnected. Local worker will be the only one used
                    # updated_nodes.append((self.worker.load, self.worker.uri))
                    pass

                else:
                    # Timeout expired. Subnet broadcast-scanning successful.
                    # Scan IP addresses read from ips.conf file if exists

                    if not os.path.exists('ips.conf'):
                        with open('ips.conf', 'w') as ips:
                            ips.write("# You can put in this file known workers IP addresses or networks to use.\n" +
                                      "# Please don't let empty lines.")

                    scanner = pyrosocket.createBroadcastSocket()
                    with open('ips.conf') as ips:
                        while True:
                            read, write, error = select.select([scanner], [scanner], [scanner])

                            while len(read) > 0:
                                uri = scanner.recv(1024).decode()
                                uris.add(uri)
                                read, write, error = select.select([scanner], [scanner], [scanner])

                            if len(write) > 0:
                                line = ips.readline()
                                if line == '':
                                    # ips.conf file reading completed
                                    break

                                try:
                                    if '/' in line:
                                        # Network
                                        net = ipaddress.IPv4Network(line)
                                        ip = net.broadcast_address
                                    else:
                                        # Single ip address
                                        ip = ipaddress.ip_address(line)
                                except ValueError:
                                    # Invalid entry in ips.conf.
                                    continue

                            try:
                                scanner.sendto(b'SCANNING', (str(ip), 5555))
                            except OSError:
                                # Network is unreachable
                                continue

                        # Finish reading late responses
                        scanner.settimeout(Client.SCANNER_TIMEOUT)
                        while True:
                            try:
                                uri = scanner.recv(1024).decode()
                                uris.add(uri)
                            except OSError:
                                # Timeout expired or unreachable network
                                scanner.close()
                                break

                    # Create list with tuples of found workers and their loads
                    for uri in uris:
                        try:
                            worker = Pyro4.Proxy(uri)
                            worker._pyroTimeout = Node.PYRO_TIMEOUT
                            updated_nodes.append((worker.load, uri))
                        except TypeError:
                            # Invalid uri
                            continue
                        except Pyro4.errors.PyroError:
                            # TimeoutError, ConnectionClosedError
                            continue

            # Update client's knowledge of system workers
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
            with self.lock:
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
                        n._pyroTimeout = Node.PYRO_TIMEOUT
                        n._pyroOneway.add('process')  # We don't need to wait for calls to n.process now

                        n.process(st.func, (st.task.id, st.index), self.uri)
                        heapq.heappush(self.workers, (n.load, uri))

                        self.log.report('Asignada la subtarea %s al worker %s' % ((st.task.id, st.index), uri), True)
                        print(self.workers)

                    except Pyro4.errors.PyroError:
                        # TimeoutError, ConnectionClosedError
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
        current_task.completed_subtasks += 1
        current_task.completed = current_task.completed_subtasks == len(current_task.result)
        self.log.report('Tarea %s completada al %s/100' %
                        (current_task.id, (current_task.completed_subtasks * 100 / len(current_task.result))), True)

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
                    n._pyroTimeout = Node.PYRO_TIMEOUT

                    total_time = n.total_time
                    total_operations = n.total_operations
                    avg_time = total_time / total_operations if total_operations != 0 else 0

                    print(n.ip_address, total_operations, total_time, avg_time, sep='\t')

                except Pyro4.errors.TimeoutError:
                    # Timeout expired. Connection to worker couldn't be completed
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
