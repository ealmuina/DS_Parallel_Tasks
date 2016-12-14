import socket
import threading
import time
from queue import Queue, PriorityQueue

import Pyro4
from Pyro4 import socketutil as pyrosocket
from Pyro4.errors import PyroError

import log
import node
import utils


class Client:
    SCANNER_TIMEOUT = 1  # Tiempo (segundos) de espera del socket que escanea el sistema en busca de nodos
    SCANNER_INTERVAL = 10  # Tiempo (segundos) entre escaneos del sistema

    def __init__(self):
        self.nodes = PriorityQueue()  # Nodos accesibles, priorizados según su carga
        self.node = node.Node()  # Nodo del sistema correspondiente al equipo
        self.connected = False
        self.lock = threading.Lock()  # Lock para el uso de 'self.nodes'
        self.log = log.Log('client')
        self.pending_tasks = []
        self.pending_subtasks = Queue()

        daemon = Pyro4.Daemon(host=utils.get_ip())
        self.uri = daemon.register(self)
        threading.Thread(target=daemon.requestLoop).start()

        threading.Thread(target=self._scan_loop).start()

        self.log.report('Cliente inicializado.')

    def _scan_loop(self):
        """Escanea la red en busca de nodos y actualiza una cola con prioridad según la carga de estos."""

        scanner = pyrosocket.createBroadcastSocket()
        scanner.settimeout(Client.SCANNER_TIMEOUT)

        while True:
            if not self.connected:
                continue

            updated_nodes = []

            try:
                scanner.sendto(b'SCANNING', ('255.255.255.255', 5555))
                while True:
                    data, address = scanner.recvfrom(1024)
                    uri = data.decode()
                    try:
                        _node = Pyro4.Proxy(uri)
                        updated_nodes.append((_node.get_load(), _node))
                    except PyroError:
                        # Si los datos recibidos no son una uri válida, al tratar de crear el proxy,
                        # esta excepción es lanzada.
                        continue
            except socket.timeout:
                with self.lock:
                    while not self.nodes.empty():
                        self.nodes.get()
                    for n in updated_nodes:
                        self.nodes.put(n)

                self.log.report('Sistema escaneado. La cola de nodos es ahora:\n%s.' % updated_nodes)
                time.sleep(Client.SCANNER_INTERVAL)

    def add(self, a, b):
        """Adiciona dos matrices."""
        pass

    def sub(self, a, b):
        """Resta dos matrices."""
        pass

    def mult(self, a, b):
        """Multiplica dos matrices."""

        # Función que calcula la sumatoria del producto componente a componente de dos vectores
        def func(v1, v2):
            result = 0
            for i in range(len(v1)):
                result += v1[i] * v2[i]
            return result

        pass
        # TODO Continuar

    def join_to_system(self):
        """Integra el equipo al sistema distribuido."""
        self.node.join_to_system()
        self.connected = True

    def leave_system(self):
        """Se desconecta del sistema distribuido."""
        self.node.leave_system()
        self.connected = False

    def get_report(self, task_id, result):
        """Reporta al cliente el resultado de una operacion solicitada por este a uno de los nodos del sistema."""
        pass


if __name__ == '__main__':
    client = Client()
    client.join_to_system()
