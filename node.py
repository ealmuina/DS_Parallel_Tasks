import socket
import threading

import Pyro4
from Pyro4 import socketutil as pyrosocket

import log


@Pyro4.expose
class Node:
    def __init__(self):
        self.log = log.Log('node')
        self.load = 0

        daemon = Pyro4.Daemon(host=socket.gethostname())
        self.uri = daemon.register(self).asString()
        threading.Thread(target=daemon.requestLoop).start()

        threading.Thread(target=self._listen_loop).start()

    def get_load(self):
        """Retorna la carga de trabajos pendientes del nodo."""
        return self.load

    def process(self, data, func):
        """Retorna el resultado de aplicar func a data."""
        return func(data)

    def _listen_loop(self):
        """Escucha en espera de que algún cliente solicite su uri. Cuando esto ocurre, envía la uri al cliente."""
        listener = pyrosocket.createBroadcastSocket((socket.gethostname(), 5555))
        while True:
            data, address = listener.recvfrom(1024)
            if data.decode() == 'SCANNING':
                listener.sendto(self.uri.encode(), address)


if __name__ == '__main__':
    pass
