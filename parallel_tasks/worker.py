import collections
import importlib
import ipaddress
import os
import subprocess
import threading
import time
from datetime import datetime, timedelta
from queue import Queue

import Pyro4
import Pyro4.errors
from Pyro4 import socketutil as pyrosocket

from .libraries import log
from .node import Node

# Pyro4.config.SERVERTYPE = "multiplex"
Pyro4.config.SERIALIZER = 'pickle'
Pyro4.config.SERIALIZERS_ACCEPTED.add('pickle')


@Pyro4.expose
class Worker(Node):
    MAX_CACHE_ENTRIES = 1000  # Maximum number of entries stored at a worker's cache
    MAX_COMPLETED_TASKS = 1000  # Maximum number of completed tasks whose result hasn't been delivered
    BEEP_INTERVAL = 3  # Time (seconds) elapsed between emitting beeps to the system

    def __init__(self):
        super().__init__()

        self.log = log.Log('worker_%s' % self.uri.split(':')[-1])
        self.pending_tasks = Queue()
        self.completed_tasks = Queue()

        self.cache = collections.OrderedDict()  # key=(task_id, client_uri), value=corresponding task data

        self.pending_tasks_count = 0  # Total of pending operations
        self.lock = threading.Lock()  # Lock for the concurrent use of self.pending_tasks_count

        # Info relative to the total operations executed and the time required to complete them
        self._total_operations = 0
        self._total_time = timedelta()

        threading.Thread(target=self._deliver_loop, daemon=True).start()
        threading.Thread(target=self._beep_loop, daemon=True).start()
        threading.Thread(target=self._process_loop, daemon=True).start()

        self.log.report('Worker inicializado.', True)

    def _beep_loop(self):
        """
        Send the worker's URI in order to be detected by clients in the network
        It broadcasts its URI on the network and to user-specified ip addresses on the 'ips.conf' file.
        It's intended to run 'forever' on a separated thread.
        """

        while True:
            # Create a broadcast socket and send through it the worker's URI.
            beeper = pyrosocket.createBroadcastSocket()
            try:
                # Broadcast on local network
                beeper.sendto(self.uri.encode(), ('255.255.255.255', 5555))

                # Beep IP addresses read from ips.conf file if exists
                if not os.path.exists('config/ips.conf'):
                    os.makedirs('config')
                    with open('config/ips.conf', 'w') as ips:
                        ips.write("# You can put in this file known clients IP addresses or networks that may use " +
                                  "a worker running on this computer.")

                with open('config/ips.conf') as ips:
                    for line in ips:
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

                        beeper.sendto(self.uri.encode(), (str(ip), 5555))
            except OSError:
                pass
            finally:
                beeper.close()

            time.sleep(Worker.BEEP_INTERVAL)  # rest some time before next beep

    def _deliver_loop(self):
        """
        Report subtasks results back to clients.
        It's intended to run 'forever' on a separated thread.
        """

        while True:
            result, subtask_id, client_uri = self.completed_tasks.get()

            try:
                start_time = datetime.now()

                client = Pyro4.Proxy(client_uri)
                client._pyroTimeout = Node.PYRO_TIMEOUT

                client.report(subtask_id, result)
                self._total_time += datetime.now() - start_time

                # Subtask completed and result delivered. Decrease number of pending subtasks.
                self._total_operations += 1
                with self.lock:
                    self.pending_tasks_count -= 1

                self.log.report('Result of operation %s has been delivered.' % str(subtask_id))

            except Pyro4.errors.PyroError:
                # TimeoutError, ConnectionClosedError
                # Save results if possible and try again later

                if len(self.completed_tasks.queue) < Worker.MAX_COMPLETED_TASKS:
                    self.completed_tasks.put((result, subtask_id, client_uri))
                else:
                    self.log.report(
                        'Operation %s was completed, but the client who requested it cannot be reached.' % str(
                            subtask_id), True, 'red')

    def _get_task_data(self, subtask_id, client_uri):
        """
        Return the data corresponding to the task associated with subtask_id.
        :param subtask_id: Tuple that identifies a subtask.
        :param client_uri: URI of the client who requested the subtask.
        :return: Data corresponding to the task.
        """

        task_id = subtask_id[0]
        key = (task_id, client_uri)

        if key in self.cache:
            # Data is already in cache. Return it directly
            self.cache.move_to_end(key)  # Update its time of use by moving it to the cache's bottom
            return self.cache[key]

        else:
            # Data isn't cached yet. Ask the client for it.
            try:
                client = Pyro4.Proxy(client_uri)
                client._pyroTimeout = Node.PYRO_TIMEOUT
                data = client.get_data(subtask_id)

                if len(self.cache) > Worker.MAX_CACHE_ENTRIES:
                    # Cache storage is full. Free some space!
                    self.cache.popitem(False)  # False to remove the LRU entry

                self.cache[key] = data
                return data

            except Pyro4.errors.PyroError:
                # TimeoutError, ConnectionClosedError
                # Client is not available. Operation will be ignored.
                return None

    def _load_function(self, func_path, client_uri):
        """
        Return the function at 'func_path'.
        :param func_path: String with the structure <module>.<function> indicating how to load the function
        :param client_uri: URI of the client who will be asked if function is not found locally
        :return: Python function as requested
        """

        # Modules sent by clients will be stored at the directory <worker's daemon port number>_temp
        wid = self.uri.split(':')[-1]
        temp_path = '%s_temp' % wid

        os.makedirs(temp_path, exist_ok=True)
        open('%s/__init__.py' % temp_path, 'w').close()  # Convert folder in a python package
        module, func = func_path.split('.')

        try:
            importlib.import_module(temp_path)  # Load package
            module = importlib.import_module('.' + module, temp_path)  # Import module from package
            func = getattr(module, func)  # Get function from module
            return func

        except ImportError:
            # Ups, there is not module with that name. Ask the client for it...
            try:
                client = Pyro4.Proxy(client_uri)
                client._pyroTimeout = Node.PYRO_TIMEOUT
                code = client.get_module(module)

                if len(os.listdir(temp_path)) > Worker.MAX_CACHE_ENTRIES:
                    # There is not space to save the code. Free some
                    mods = os.scandir(temp_path)
                    lru = min(mods, key=lambda x: x.stat().st_atime)  # Just to find out what module is the LRU
                    os.remove(lru.path)

                with open('%s/%s.py' % (temp_path, module), 'w') as f:
                    # Save it for the next time
                    f.write(code)

                # Try to call again. I bet this time module will be there
                return self._load_function(func_path, client_uri)

            except Pyro4.errors.PyroError:
                # TimeoutError, ConnectionClosedError
                # Client is not available. Operation will be ignored.
                return None

    def _process_loop(self):
        """
        Process pending subtasks and queue the results for delivering.
        It's intended to run 'forever' on a separated thread.
        """

        while True:
            func_path, subtask_id, client_uri = self.pending_tasks.get()
            data = self._get_task_data(subtask_id, client_uri)
            func = self._load_function(func_path, client_uri)

            if data and func:
                start_time = datetime.now()
                try:
                    result = func(data, subtask_id[1])
                    self._total_time += datetime.now() - start_time
                except Exception as e:
                    self.log.report(
                        'While subtask %s was executing the following exception occurred: %s.' % (subtask_id, type(e)),
                        True, 'red')
                else:
                    # Enqueue the result to be delivered
                    self.completed_tasks.put((result, subtask_id, client_uri))
                    self.log.report('Subtask %s result is ready.' % str(subtask_id))

    @property
    def ip_address(self):
        return self.ip

    @property
    def load(self):
        """
        Get worker's 'load', expressed as the product of the amount of pending operations by the average time it takes
        to complete one.
        :return: Float number indicating worker's load.
        """

        total_time = self._total_time.total_seconds()
        avg_time = total_time / self._total_operations if self._total_operations != 0 else 1
        with self.lock:
            return (self.pending_tasks_count + 1) * avg_time

    @property
    def total_operations(self):
        return self._total_operations

    @property
    def total_time(self):
        return self._total_time

    def close(self):
        super().close()
        subprocess.Popen(
            ['python', 'parallel_tasks/libraries/cleaning.py', self.uri])  # Process to clean temp code directory

    def process(self, func, subtask_id, client_uri):
        """
        Adds the task of evaluating the function indicated by 'func' on the data corresponding to the subtask, to the
        queue of pending operations.
        :param func: String with the format <module>.<function> indicating a function the client want to be executed
        with the subtask data. This function must be present at a module the client has access to, so it can be sent
        to the worker.
        :param subtask_id: Tuple (task_id, subtask_id at the task) used to identify the subtask requested.
        :param client_uri: String with the URI of the client.
        """

        with self.lock:
            self.pending_tasks_count += 1
        self.pending_tasks.put((func, subtask_id, client_uri))
