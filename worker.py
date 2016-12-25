import os
import threading
from collections import deque
from datetime import datetime, timedelta
from queue import Queue

import Pyro4
from Pyro4 import socketutil as pyrosocket
from Pyro4.errors import PyroError

import log
import matrix
from node import Node

Pyro4.config.COMMTIMEOUT = 5  # 5 seconds
Pyro4.config.SERVERTYPE = "multiplex"

Pyro4.config.SERIALIZER = 'pickle'
Pyro4.config.SERIALIZERS_ACCEPTED.add('pickle')


@Pyro4.expose
class Worker(Node):
    MAX_CACHE_SIZE = 100  # Límite de registros que puede contener la cache
    MAX_COMPLETED_TASKS = 10  # Límite de tareas completadas pendientes de entregar a sus respectivos clientes

    def __init__(self):
        super().__init__()

        self.log = log.Log('worker')
        self.pending_tasks = Queue()
        self.completed_tasks = Queue()

        self.cache = {}
        self.cache_timer = deque()  # Conserva los registros en el orden en que fueron insertados en la cache

        self.load = 0  # Total de operaciones pendientes
        self.lock = threading.Lock()  # Lock para el uso de self.load

        # Datos relativos al total de operaciones realizadas y el tiempo requerido para completarlas
        self.total_operations = 0
        self.total_time = timedelta()

        threading.Thread(target=self._listen_loop).start()
        threading.Thread(target=self._deliver_loop).start()

        for i in range(os.cpu_count()):
            threading.Thread(target=self._process_loop).start()

        self.log.report('Worker inicializado con %d hilos procesando solicitudes.' % os.cpu_count(), True)

    def get_load(self):
        """Retorna la 'carga' del worker, expresada como el producto de la cantidad de operaciones que tiene pendientes
         de completar y el tiempo promedio que demora en completar una."""

        total_time = self.total_time.total_seconds()
        avg_time = total_time / self.total_operations if total_time != 0 else 1
        return (self.load + 1) * avg_time

    def get_ip(self):
        return self.ip

    def get_total_operations(self):
        return self.total_operations

    def get_total_time(self):
        return self.total_time

    def process(self, data, func, subtask_id, client_uri):
        """Agrega la tarea de evaluar en data la función indicada por func, a la cola de tareas pendientes."""

        with self.lock:
            self.load += 1
        self.pending_tasks.put((data, func, subtask_id, client_uri))

    def _listen_loop(self):
        """Escucha en espera de que algún cliente solicite su uri. Cuando esto ocurre, envía la uri al cliente."""

        listener = pyrosocket.createBroadcastSocket(('', 5555))
        while True:
            try:
                data, address = listener.recvfrom(1024)
            except ConnectionResetError:
                # Se cerró la conexión antes de tiempo. Continuar iterando
                continue

            if data.decode() == 'SCANNING':
                listener.sendto(self.uri.encode(), address)

    def _process_loop(self):
        """Procesa las tareas pendientes y entrega sus resultados a los clientes que las solicitaron."""

        while True:
            data, func, subtask_id, client_uri = self.pending_tasks.get()

            if func == '+':
                func = matrix.vector_add
            elif func == '-':
                func = matrix.vector_sub
            elif func == '*':
                func = matrix.vector_mult
                task_data = self._get_task_data(subtask_id, client_uri)

                # Si task_data es None, entonces la tarea asociada ya fue completada. Hacer data = None en tal caso
                data = (data, task_data) if task_data else None

            if data:
                start_time = datetime.now()
                result = func(data)
                self.total_time += datetime.now() - start_time
                self.total_operations += 1

                # Encolar el resultado para que sea entregado al cliente
                self.completed_tasks.put((result, subtask_id, client_uri))

            # Tarea completada. Decrementar la cantidad de tareas pendientes
            with self.lock:
                self.load -= 1

    def _deliver_loop(self):
        while True:
            result, subtask_id, client_uri = self.completed_tasks.get()

            try:
                client = Pyro4.Proxy(client_uri)
                client.report(subtask_id, result)

            except PyroError:
                if len(self.completed_tasks.queue) < Worker.MAX_COMPLETED_TASKS:
                    self.completed_tasks.put((result, subtask_id, client_uri))
                else:
                    self.log.report(
                        'La operación con id %s fue completada, pero el cliente no pudo ser localizado.' % str(
                            subtask_id), True, 'red')

    def _get_task_data(self, subtask_id, client_uri):
        """Retorna los datos comunes a todas las subtareas de una tarea."""

        if subtask_id[0] in self.cache:
            return self.cache[subtask_id[0]]

        else:
            try:
                client = Pyro4.Proxy(client_uri)
                data = client.get_data(subtask_id)[1]

                if len(self.cache) > Worker.MAX_CACHE_SIZE:
                    d = self.cache_timer.pop()
                    self.cache.pop(d)

                self.cache_timer.appendleft(subtask_id[0])
                self.cache[subtask_id[0]] = data
                return data

            except PyroError:
                # Cliente no diponible. Dar por completada la operación
                return None


if __name__ == '__main__':
    worker = Worker()
