import os
import threading
import time
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

    def __init__(self):
        self.log = log.Log('node')
        self.pending_tasks = Queue()

        self.load = 0  # Total de operaciones pendientes
        self.lock = threading.Lock()  # Lock para el uso de self.load

        # Datos relativos al total de operaciones realizadas y el tiempo requerido para completarlas
        self.total_operations = 0
        self.total_time = timedelta(0)

        self.ip = utils.get_ip()
        threading.Thread(target=self._ip_address_check_loop).start()

        self._update_Pyro_daemon()

        threading.Thread(target=self._listen_loop).start()

        for i in range(os.cpu_count()):
            threading.Thread(target=self._process_loop).start()

        self.log.report('Nodo inicializado con %d hilos procesando solicitudes.' % os.cpu_count(), True)

    def get_load(self):
        return self.load

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
            data, address = listener.recvfrom(1024)
            if data.decode() == 'SCANNING':
                listener.sendto(self.uri.encode(), address)

    def _process_loop(self):
        """Procesa las tareas pendientes y entrega sus resultados a los clientes que las solicitaron."""

        while True:
            data, func, subtask_id, client_uri = self.pending_tasks.get()
            with self.lock:
                self.load -= 1

            if func == '+':
                func = matrix.vector_add
            elif func == '-':
                func = matrix.vector_sub
            elif func == '*':
                func = matrix.vector_mult

            start_time = datetime.now()
            result = func(data)
            self.total_operations += 1
            self.total_time += datetime.now() - start_time

            try:
                client = Pyro4.Proxy(client_uri)
                client.set_report(subtask_id, result)

            except PyroError:
                self.log.report(
                    'La operación con id %s fue completada, pero el cliente no pudo ser localizado.' % str(subtask_id),
                    True, 'red')

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


if __name__ == '__main__':
    node = Node()
