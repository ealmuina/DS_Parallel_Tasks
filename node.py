import os
import threading
from queue import Queue

import Pyro4
from Pyro4 import socketutil as pyrosocket

import log
import matrix
import utils


@Pyro4.expose
class Node:
    def __init__(self):
        self.log = log.Log('node')
        self.load = 0
        self.lock = threading.Lock()
        self.connected = False
        self.pending_tasks = Queue()

        daemon = Pyro4.Daemon(host=utils.get_ip())
        self.uri = daemon.register(self).asString()
        threading.Thread(target=daemon.requestLoop).start()

        threading.Thread(target=self._listen_loop).start()

        for i in range(os.cpu_count()):
            threading.Thread(target=self._process_loop).start()

        self.log.report('Nodo inicializado con %d hilos procesando solicitudes.' % os.cpu_count())

    def get_load(self):
        """Retorna la carga de trabajos pendientes del nodo."""
        return self.load

    def process(self, data, func, subtask_id, client_uri):
        """Agrega la tarea de evaluar en data la función indicada por func, a la cola de tareas pendientes."""
        with self.lock:
            self.load += 1
        self.pending_tasks.put((data, func, subtask_id, client_uri))

    def join_to_system(self):
        """Integra el equipo al sistema distribuido."""
        self.connected = True

    def leave_system(self):
        """Se desconecta del sistema distribuido."""
        self.connected = False

    def _listen_loop(self):
        """Escucha en espera de que algún cliente solicite su uri. Cuando esto ocurre, envía la uri al cliente."""
        listener = pyrosocket.createBroadcastSocket(('', 5555))
        while True:
            if not self.connected:
                continue
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
                pass
            elif func == '*':
                pass

            result = func(data)
            client = Pyro4.Proxy(client_uri)
            client.get_report(subtask_id, result)


if __name__ == '__main__':
    pass
