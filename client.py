import heapq
import socket
import threading
import time
from datetime import datetime
from queue import Queue

import Pyro4
from Pyro4 import socketutil as pyrosocket
from Pyro4.errors import PyroError

import log
import matrix
import node
import utils
from task import Task, Subtask


class Client:
    SCANNER_TIMEOUT = 1  # Tiempo (segundos) de espera del socket que escanea el sistema en busca de nodos
    SCANNER_INTERVAL = 10  # Tiempo (segundos) entre escaneos del sistema
    SUBTASKS_TIMEOUT = 30  # Tiempo (segundos) de espera por el resultado de una operacion asignada a un nodo

    def __init__(self):
        self.nodes = []  # Nodos accesibles, priorizados según su carga
        self.node = node.Node()  # Nodo del sistema correspondiente al equipo
        self.lock = threading.Lock()  # Lock para el uso de 'self.nodes'

        self.connected = False

        self.log = log.Log('client')

        self.pending_tasks = set()
        self.pending_subtasks = Queue()
        self.pending_subtasks_dic = {}  # Mapea una tupla (task_id, index) a la subtarea pendiente correspondiente
        self.task_number = 0  # Entero usado para asignar identificadores a las tareas

        daemon = Pyro4.Daemon(host=utils.get_ip())
        self.uri = daemon.register(self)
        threading.Thread(target=daemon.requestLoop).start()

        threading.Thread(target=self._scan_loop).start()
        threading.Thread(target=self._subtasks_checker_loop).start()

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
                        current_node = Pyro4.Proxy(uri)
                        updated_nodes.append((current_node.get_load(), uri, current_node))
                        # *** Se añadió la uri a la tupla para evitar excepción en el heapify si dos nodos
                        # *** tienen la misma prioridad
                    except PyroError:
                        # Si los datos recibidos no son una uri válida, al tratar de crear el proxy,
                        # esta excepción es lanzada.
                        continue

            except socket.timeout:
                heapq.heapify(updated_nodes)
                with self.lock:
                    self.nodes.clear()
                    for n in updated_nodes:
                        self.nodes.append(n)

            except OSError:
                # La red fue desconectada. Intentar de nuevo
                continue

            self.log.report('Sistema escaneado. Se detectaron %d nodos.' % len(updated_nodes))
            time.sleep(Client.SCANNER_INTERVAL)

    def _subtasks_checker_loop(self):
        """Chequea si ha expirado el tiempo de espera por el resultado de alguna operacion.
        Si esto ocurre, la asigna a un nuevo nodo."""

        while True:
            if len(self.nodes) == 0:
                # No se han encontrado nodos del sistema a los que asignar subtareas
                continue

            t, st = self.pending_subtasks.get()
            elapsed = datetime.now() - t

            if st.completed:
                # La subtarea ya fue completada. Seguir iterando
                continue

            if elapsed.total_seconds() > Client.SUBTASKS_TIMEOUT:
                # Tiempo de espera superado, asignar operacion a un nuevo nodo
                with self.lock:
                    load, uri, n = heapq.heappop(self.nodes)
                    st.time = datetime.now()
                    try:
                        n.process(st.data, st.func, (st.task.id, st.index), self.uri)
                        heapq.heappush(self.nodes, (load + 1, uri, n))
                        self.log.report('Asignada la subtarea %s al nodo %s' % ((st.task.id, st.index), uri))
                    except PyroError:
                        self.log.report('Se intentó enviar subtarea al nodo %s, pero no se encuentra accesible.' % uri)

            self.pending_subtasks.put((st.time, st))

    def add(self, a, b):
        """Adiciona dos matrices."""

        if len(a) != len(b):
            raise ArithmeticError('Las dimensiones de las matrices deben coincidir.')

        # Crear nueva tarea
        task = Task(len(a), self.task_number)
        self.pending_tasks.add(task)
        self.task_number += 1

        # Crear subtareas para la suma de las filas correspondientes en las matrices
        for i in range(len(a)):
            st = Subtask(task, i, (a[i], b[i]), '+')
            self.pending_subtasks.put((st.time, st))
            self.pending_subtasks_dic[(task.id, i)] = st

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

    def get_report(self, subtask_id, result):
        """Reporta al cliente el resultado de una operacion solicitada por este a uno de los nodos del sistema."""

        # Localizar la subtarea correspondiente al id y marcarla como completada.
        # Si no se encuentra la subtarea, entonces ya fue resuelta. Terminar el llamado al método.
        try:
            subtask = self.pending_subtasks_dic.pop(subtask_id)
        except KeyError:
            self.log.report('Un nodo reportó el resultado de una operación ya completada. La respuesta será desechada.')
            return None

        subtask.completed = True

        current_task = subtask.task

        # Copiar el resultado de la subtarea al de la tarea correspondiente
        current_task.result[subtask.index] = result

        # Verficar si la tarea fue completada
        current_task.completed = True
        for x in current_task.result:
            current_task.completed = current_task.completed and x

        # Si la tarea se completó, reportar resultado y eliminarla de la lista de tareas pendientes
        if current_task.completed:
            self.log.report('Resultado de la tarea %s:\n %s \nTiempo total: %s' % (
                current_task.id, current_task.result, datetime.now() - current_task.time))
            self.pending_tasks.remove(current_task)


if __name__ == '__main__':
    client = Client()
    client.join_to_system()

    a = matrix.get_random_matrix(1000, 150)
    b = matrix.get_random_matrix(1000, 150)
    client.add(a, b)
