import os
import threading
import time
from collections import deque
from datetime import datetime, timedelta
from queue import Queue

import Pyro4
from Pyro4 import socketutil as pyrosocket
from Pyro4.errors import PyroError

import log
import matrix
import utils

Pyro4.config.COMMTIMEOUT = 1.5  # 1.5 seconds
Pyro4.config.SERVERTYPE = "multiplex"

Pyro4.config.SERIALIZER = 'pickle'
Pyro4.config.SERIALIZERS_ACCEPTED.add('pickle')


@Pyro4.expose
class Node:
    CHECK_IP_INTERVAL = 5  # Tiempo (segundos) transcurrido el cual se verificará si la IP sigue siendo la misma.
    MAX_CACHE_SIZE = 100  # Límite de registros que puede contener la cache

    def __init__(self):
        self.log = log.Log('node')
        self.pending_tasks = Queue()

        self.cache = {}
        self.cache_timer = deque()

        self.load = 0  # Total de operaciones pendientes
        self.lock = threading.Lock()  # Lock para el uso de self.load

        # Datos relativos al total de operaciones realizadas y el tiempo requerido para completarlas
        self.total_operations = 0
        self.total_time = timedelta()

        self.ip = utils.get_ip()
        threading.Thread(target=self._ip_address_check_loop).start()

        self._update_Pyro_daemon()

        threading.Thread(target=self._listen_loop).start()

        for i in range(os.cpu_count()):
            threading.Thread(target=self._process_loop).start()

        self.log.report('Nodo inicializado con %d hilos procesando solicitudes.' % os.cpu_count(), True)

    def get_load(self):
        """Retorna la 'carga' del nodo, expresada como el producto de la cantidad de operaciones que tiene pendientes
         de completar y el tiempo promedio que demora en completar una."""

        total_time = self.total_time.total_seconds()
        avg_time = self.total_operations / total_time if total_time > 0 else 1
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
                self.total_operations += 1

                try:
                    client = Pyro4.Proxy(client_uri)
                    client.set_report(subtask_id, result)

                except PyroError:
                    self.log.report(
                        'La operación con id %s fue completada, pero el cliente no pudo ser localizado.' % str(
                            subtask_id), True, 'red')

                self.total_time += datetime.now() - start_time

            # Tarea completada. Decrementar la cantidad de tareas pendientes
            with self.lock:
                self.load -= 1

    def _ip_address_check_loop(self):
        while True:
            ip = utils.get_ip()
            if ip != self.ip:
                self.ip = ip
                self._update_Pyro_daemon()

            time.sleep(Node.CHECK_IP_INTERVAL)

    def _update_Pyro_daemon(self):
        # Cerrar self.daemon si ya existía
        try:
            self.daemon.shutdown()
            # TODO Esto produce una excepcion en un hilo que el mismo crea y q no tengo forma de capturarla. WTF???
        except AttributeError:
            pass

        self.daemon = Pyro4.Daemon(host=utils.get_ip())
        self.uri = self.daemon.register(self, force=True).asString()
        threading.Thread(target=self.daemon.requestLoop).start()

        self.log.report('Dirección IP modificada a: %s' % utils.get_ip())

    def _get_task_data(self, subtask_id, client_uri):
        """Retorna los datos comunes a todas las subtareas de una tarea."""

        if subtask_id[0] in self.cache:
            return self.cache[subtask_id[0]]

        else:
            try:
                client = Pyro4.Proxy(client_uri)
                data = client.get_data(subtask_id)[1]

                if len(self.cache) > Node.MAX_CACHE_SIZE:
                    d = self.cache_timer.pop()
                    self.cache.pop(d)

                self.cache_timer.appendleft(subtask_id[0])
                self.cache[subtask_id[0]] = data
                return data

            except PyroError:
                # Cliente no diponible. Dar por completada la operación
                return None


if __name__ == '__main__':
    node = Node()
