import importlib
import threading
import time
from collections import deque
from datetime import datetime, timedelta
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
class Worker(Node):
    MAX_CACHE_ENTRIES = 1000  # Límite de registros que puede contener la cache
    MAX_COMPLETED_TASKS = 1000  # Límite de tareas completadas pendientes de entregar a sus respectivos clientes

    def __init__(self):
        super().__init__()

        self.log = log.Log('worker')
        self.pending_tasks = Queue()
        self.completed_tasks = Queue()

        self.cache = {}
        self.cache_timer = deque()  # Conserva los registros en el orden en que fueron insertados en la cache

        self.pending_tasks_count = 0  # Total de operaciones pendientes
        self.lock = threading.Lock()  # Lock para el uso de self.pending_tasks_count

        # Datos relativos al total de operaciones realizadas y el tiempo requerido para completarlas
        self._total_operations = 0
        self._total_time = timedelta()

        threading.Thread(target=self._deliver_loop, daemon=True).start()
        threading.Thread(target=self._listen_loop, daemon=True).start()
        threading.Thread(target=self._process_loop, daemon=True).start()

        self.log.report('Worker inicializado.', True)

    @property
    def load(self):
        """Retorna la 'carga' del worker, expresada como el producto de la cantidad de operaciones que tiene pendientes
         de completar y el tiempo promedio que demora en completar una."""

        total_time = self._total_time.total_seconds()
        avg_time = total_time / self._total_operations if self._total_operations != 0 else 1
        with self.lock:
            return (self.pending_tasks_count + 1) * avg_time

    @property
    def ip_address(self):
        return self.ip

    @property
    def total_operations(self):
        return self._total_operations

    @property
    def total_time(self):
        return self._total_time

    def process(self, func, subtask_id, client_uri):
        """Agrega la tarea de evaluar en data la función indicada por func, a la cola de tareas pendientes."""

        with self.lock:
            self.pending_tasks_count += 1
        self.pending_tasks.put((func, subtask_id, client_uri))

    def _listen_loop(self):
        """Escucha en espera de que algún cliente solicite su uri. Cuando esto ocurre, envía la uri al cliente."""

        listener = pyrosocket.createBroadcastSocket(('', 5555))  # TODO Chequear si esto funciona cambiando de red
        while True:
            try:
                data, address = listener.recvfrom(1024)
            except ConnectionResetError:
                continue

            if data.decode() == 'SCANNING':
                listener.sendto(self.uri.encode(), address)

    def _process_loop(self):
        """Procesa las tareas pendientes y entrega sus resultados a los clientes que las solicitaron."""

        while True:
            func, subtask_id, client_uri = self.pending_tasks.get()
            data = self._get_task_data(subtask_id, client_uri)

            # Cargar la función indicada por el string func
            module, func = func.split('.')
            module = importlib.import_module(module)
            func = getattr(module, func)

            if data:
                start_time = datetime.now()
                result = func(data, subtask_id[1])
                self._total_time += datetime.now() - start_time

                # Encolar el resultado para que sea entregado al cliente
                self.completed_tasks.put((result, subtask_id, client_uri))
                self.log.report('Resultado de la subtarea %s listo' % str(subtask_id))

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
            return self.cache[key]

        else:
            try:
                client = Pyro4.Proxy(client_uri)
                client._pyroTimeout = Node.PYRO_TIMEOUT
                data = client.get_data(subtask_id)

                if len(self.cache) > Worker.MAX_CACHE_ENTRIES:
                    d = self.cache_timer.pop()
                    self.cache.pop(d)

                self.cache_timer.appendleft(task_id)
                self.cache[key] = data
                return data

            except Pyro4.errors.PyroError:
                # TimeoutError, ConnectionClosedError
                # Cliente no diponible. Dar por completada la operación
                return None


if __name__ == '__main__':
    worker = Worker()
    while True:
        time.sleep(100)
