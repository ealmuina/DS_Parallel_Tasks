import socket
import threading
import time
from queue import PriorityQueue

import Pyro4
from Pyro4 import socketutil as pyrosocket

import log
import node


class Client:
    SCANNER_TIMEOUT = 1  # Tiempo (segundos) de espera del socket que escanea el sistema en busca de nodos
    SCANNER_INTERVAL = 10  # Tiempo (segundos) entre escaneos del sistema

    def __init__(self):
        self.nodes = PriorityQueue()  # Mantiene los nodos accesibles, priorizados según su carga
        self.node = node.Node()  # Nodo del sistema correspondiente al equipo
        self.connected = False  # Indica si el equipo está conectado al sistema
        self.lock = threading.Lock()  # Lock para el uso de 'self.nodes'
        self.log = log.Log('client')

        # daemon = Pyro4.Daemon(host=socket.gethostname())
        # self.uri = daemon.register(self)
        # threading.Thread(target=daemon.requestLoop).start()
        threading.Thread(target=self._scan_loop).start()

        self.log.report('Cliente inicializado.')

    def _scan_loop(self):
        """Escanea la red en busca de nodos y actualiza una cola con prioridad según la carga de estos."""

        scanner = pyrosocket.createBroadcastSocket()
        scanner.settimeout(Client.SCANNER_TIMEOUT)

        while True:
            if not self.connected:
                continue

            updated_nodes = PriorityQueue()
            updated_nodes_list = []

            try:
                scanner.sendto(b'SCANNING', ('255.255.255.255', 5555))
                while True:
                    data, address = scanner.recvfrom(1024)
                    uri = data.decode()
                    _node = Pyro4.Proxy(uri)
                    updated_nodes.put((_node.get_load(), _node))
                    updated_nodes_list.append((_node.get_load(), _node))

            except socket.timeout:
                self.lock.acquire()
                self.nodes = updated_nodes
                self.lock.release()

                self.log.report('Sistema escaneado. La cola de nodos es ahora:\n{0}.'.format(updated_nodes_list))
                time.sleep(Client.SCANNER_INTERVAL)

    def add(self, a, b):
        """Adiciona dos matrices."""
        pass

    def sub(self, a, b):
        """Resta dos matrices."""
        pass

    def mult(self, a, b):
        """Multiplica dos matrices."""
        pass

    def join_to_system(self):
        """Integra el equipo al sistema distribuido."""
        self.connected = True

    def leave_system(self):
        """Se desconecta del sistema distribuido."""
        self.connected = False


if __name__ == '__main__':
    client = Client()
    client.join_to_system()
