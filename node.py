import threading
from queue import Queue

import Pyro4
from Pyro4 import socketutil as pyrosocket

import log
import utils


@Pyro4.expose
class Node:
    def __init__(self):
        self.log = log.Log('node')
        self.load = 0
        self.connected = False
        self.pending_tasks = Queue()

        daemon = Pyro4.Daemon(host=utils.get_ip())
        self.uri = daemon.register(self).asString()
        threading.Thread(target=daemon.requestLoop).start()

        threading.Thread(target=self._listen_loop).start()
        threading.Thread(target=self._process_loop).start()

    def get_load(self):
        """Retorna la carga de trabajos pendientes del nodo."""
        return self.load

    def process(self, data, func, client_uri):
        """Agrega la tarea de evaluar func en data a la cola de tareas pendientes."""
        self.load += 1
        self.pending_tasks.put((data, func, client_uri))

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
        """Procesa las tareas pendientes y las entrega los clientes que las solicitaron."""
        pass


if __name__ == '__main__':
    pass
