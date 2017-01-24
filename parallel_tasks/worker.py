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

Pyro4.config.SERVERTYPE = "multiplex"
Pyro4.config.SERIALIZER = 'pickle'
Pyro4.config.SERIALIZERS_ACCEPTED.add('pickle')


@Pyro4.expose
class Worker(Node):
    MAX_CACHE_ENTRIES = 1000  # Límite de registros que puede contener la cache
    MAX_COMPLETED_TASKS = 1000  # Límite de tareas completadas pendientes de entregar a sus respectivos clientes
    BEEP_INTERVAL = 3  # Time (seconds) elapsed between emitting beeps to the system

    def __init__(self):
        super().__init__()

        self.log = log.Log('worker_%s' % self.uri.split(':')[-1])
        self.pending_tasks = Queue()
        self.completed_tasks = Queue()

        self.cache = collections.OrderedDict()

        self.pending_tasks_count = 0  # Total de operaciones pendientes
        self.lock = threading.Lock()  # Lock para el uso de self.pending_tasks_count

        # Datos relativos al total de operaciones realizadas y el tiempo requerido para completarlas
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
            # Create a broadcast socket and send through it the word 'SCANNING'. Every worker that receive it should
            # respond back with its URI
            # All of the received uris will be put in a set first before further processing

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
        while True:
            result, subtask_id, client_uri = self.completed_tasks.get()

            try:
                start_time = datetime.now()

                client = Pyro4.Proxy(client_uri)
                client._pyroTimeout = Node.PYRO_TIMEOUT

                client.report(subtask_id, result)
                self._total_time += datetime.now() - start_time

                # Tarea completada y entregado el resultado. Decrementar la cantidad de tareas pendientes
                self._total_operations += 1
                with self.lock:
                    self.pending_tasks_count -= 1

                self.log.report('El resultado de la operación %s fue entregado' % str(subtask_id))

            except Pyro4.errors.PyroError:
                # TimeoutError, ConnectionClosedError
                # Save results if possible and try again later

                if len(self.completed_tasks.queue) < Worker.MAX_COMPLETED_TASKS:
                    self.completed_tasks.put((result, subtask_id, client_uri))
                else:
                    self.log.report(
                        'La operación %s fue completada, pero el cliente no pudo ser localizado.' % str(
                            subtask_id), True, 'red')

    def _get_task_data(self, subtask_id, client_uri):
        """Retorna los datos correspondientes a la tarea asociada a subtask_id."""

        task_id = subtask_id[0]
        key = (task_id, client_uri)

        if key in self.cache:
            self.cache.move_to_end(key)
            return self.cache[key]

        else:
            try:
                client = Pyro4.Proxy(client_uri)
                client._pyroTimeout = Node.PYRO_TIMEOUT
                data = client.get_data(subtask_id)

                if len(self.cache) > Worker.MAX_CACHE_ENTRIES:
                    self.cache.popitem(False)  # False to remove the LRU entry

                self.cache[key] = data
                return data

            except Pyro4.errors.PyroError:
                # TimeoutError, ConnectionClosedError
                # Cliente no diponible. Dar por completada la operación
                return None

    def _load_function(self, func_path, client_uri):
        wid = self.uri.split(':')[-1]
        temp_path = '%s_temp' % wid

        os.makedirs(temp_path, exist_ok=True)
        open('%s/__init__.py' % temp_path, 'w').close()
        module, func = func_path.split('.')

        try:
            importlib.import_module(temp_path)
            module = importlib.import_module('.' + module, temp_path)
            func = getattr(module, func)
            return func

        except ImportError:
            try:
                client = Pyro4.Proxy(client_uri)
                client._pyroTimeout = Node.PYRO_TIMEOUT
                code = client.get_module(module)

                with open('%s/%s.py' % (temp_path, module), 'w') as f:
                    f.write(code)

                if len(os.listdir(temp_path)) > Worker.MAX_CACHE_ENTRIES:
                    mods = os.scandir(temp_path)
                    lru = min(mods, key=lambda x: x.stat().st_atime)
                    os.remove(lru.path)

                return self._load_function(func_path, client_uri)

            except Pyro4.errors.PyroError:
                # TimeoutError, ConnectionClosedError
                # Cliente no diponible. Dar por completada la operación
                return None

    def _process_loop(self):
        """Procesa las tareas pendientes y entrega sus resultados a los clientes que las solicitaron."""

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
                        'Mientras se ejecutaba la subtarea %s ocurrió una excepción: %s' % (subtask_id, type(e)))
                else:
                    # Encolar el resultado para que sea entregado al cliente
                    self.completed_tasks.put((result, subtask_id, client_uri))
                    self.log.report('Resultado de la subtarea %s listo' % str(subtask_id))

    @property
    def ip_address(self):
        return self.ip

    @property
    def load(self):
        """Retorna la 'carga' del worker, expresada como el producto de la cantidad de operaciones que tiene pendientes
         de completar y el tiempo promedio que demora en completar una."""

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
        subprocess.Popen(['python', 'parallel_tasks/libraries/cleaning.py', self.uri])

    def process(self, func, subtask_id, client_uri):
        """Agrega la tarea de evaluar en data la función indicada por func, a la cola de tareas pendientes."""

        with self.lock:
            self.pending_tasks_count += 1
        self.pending_tasks.put((func, subtask_id, client_uri))
