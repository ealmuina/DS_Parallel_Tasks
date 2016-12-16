import heapq
import os
import threading
import time
from datetime import datetime
from queue import Queue

import Pyro4
from Pyro4 import socketutil as pyrosocket
from Pyro4.errors import CommunicationError

import log
import matrix
import node
import utils
from task import Task, Subtask


@Pyro4.expose
class Client:
    SCANNER_TIMEOUT = 1  # Tiempo (segundos) de espera del socket que escanea el sistema en busca de nodos
    SCANNER_INTERVAL = 10  # Tiempo (segundos) entre escaneos del sistema
    SUBTASKS_TIMEOUT = 30  # Tiempo (segundos) de espera por el resultado de una operacion asignada a un nodo
    CHECK_IP_INTERVAL = 5  # Tiempo (segundos) transcurrido el cual se verificará si la IP sigue siendo la misma.

    def __init__(self):
        self.nodes = []  # Nodos accesibles, priorizados según su carga
        self.node = node.Node()  # Nodo del sistema correspondiente al equipo
        self.lock = threading.Lock()  # Lock para el uso de 'self.nodes'

        self.ip = utils.get_ip()
        threading.Thread(target=self._ip_address_check_loop).start()

        self._update_Pyro_daemon()

        self.log = log.Log('client')

        self.pending_tasks = set()
        self.pending_subtasks = Queue()
        self.pending_subtasks_dic = {}  # Mapea una tupla (task_id, index) a la subtarea pendiente correspondiente
        self.task_number = 0  # Entero usado para asignar identificadores a las tareas

        threading.Thread(target=self._scan_loop).start()
        threading.Thread(target=self._subtasks_checker_loop).start()

        self.log.report('Cliente inicializado.', True)

    def _scan_loop(self):
        """Escanea la red en busca de nodos y actualiza una cola con prioridad según la carga de estos."""

        while True:
            scanner = pyrosocket.createBroadcastSocket()
            scanner.settimeout(Client.SCANNER_TIMEOUT)

            updated_nodes = []
            try:
                scanner.sendto(b'SCANNING', ('255.255.255.255', 5555))
                while True:
                    data, address = scanner.recvfrom(1024)
                    uri = data.decode()

                    try:
                        current_node = Pyro4.async(Pyro4.Proxy(uri))
                        load = current_node.get_load()
                        load.then(lambda l: updated_nodes.append((l, uri)))

                    except CommunicationError:
                        # TODO Chequear que la excepcion es correcta
                        # Si los datos recibidos no son una uri válida, al tratar de crear el proxy,
                        # esta excepción es lanzada.
                        continue

            except OSError as e:
                if e.errno == 101:
                    # La red está desconectada. Solo podrá ser usado el nodo propio.
                    updated_nodes.append((self.node.get_load(), self.node.uri))

            finally:
                heapq.heapify(updated_nodes)
                with self.lock:
                    self.nodes.clear()
                    for n in updated_nodes:
                        self.nodes.append(n)

            self.log.report('Sistema escaneado. Se detectaron %d nodos.' % len(updated_nodes), True)
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
                    load, uri = heapq.heappop(self.nodes)
                    st.time = datetime.now()

                    try:
                        n = Pyro4.async(Pyro4.Proxy(uri))
                        n.process(st.data, st.func, (st.task.id, st.index), self.uri)

                        load = n.get_load()
                        load.then(lambda l: heapq.heappush(self.nodes, (l, uri)))

                        self.log.report('Asignada la subtarea %s al nodo %s' % ((st.task.id, st.index), uri), True)

                    except CommunicationError:
                        self.log.report('Se intentó enviar subtarea al nodo %s, pero no se encuentra accesible.' % uri,
                                        True, 'red')

            self.pending_subtasks.put((st.time, st))

    def _ip_address_check_loop(self):
        while True:
            ip = utils.get_ip()
            if ip != self.ip:
                self.ip = ip
                self._update_Pyro_daemon()

            time.sleep(Client.CHECK_IP_INTERVAL)

    def _update_Pyro_daemon(self):
        # Cerrar self.daemon si ya existía
        try:
            self.daemon.shutdown()
        except AttributeError:
            pass

        self.daemon = Pyro4.Daemon(host=utils.get_ip())
        self.uri = self.daemon.register(self, force=True)
        threading.Thread(target=self.daemon.requestLoop).start()

    def _add_sub(self, a, b, subtract=False):
        """Adiciona o resta dos matrices"""

        if len(a) != len(b):
            raise ArithmeticError('Las dimensiones de las matrices deben coincidir.')

        # Crear nueva tarea
        task = Task(len(a), self.task_number)
        self.pending_tasks.add(task)
        self.task_number += 1

        # Crear subtareas para la suma de las filas correspondientes en las matrices
        for i in range(len(a)):
            st = Subtask(task, i, (a[i], b[i]), '-' if subtract else '+')
            self.pending_subtasks.put((st.time, st))
            self.pending_subtasks_dic[(task.id, i)] = st

    def add(self, a, b):
        """Adiciona dos matrices."""

        self._add_sub(a, b)

    def sub(self, a, b):
        """Resta dos matrices."""

        self._add_sub(a, b, True)

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
            # Guardar el resultado de la tarea en el archivo <current_task.id>.txt
            os.makedirs('results', exist_ok=True)
            file_result = open('results/%s.txt' % current_task.id, 'w')
            file_result.write(matrix.str_matrix(current_task.result))

            self.log.report(
                'Tarea %(id)s completada. Puede ver el resultado en el archivo %(id)s.txt.\nTiempo total: %(time)s'
                % {'id': current_task.id, 'time': datetime.now() - current_task.time}, True, 'green')
            self.pending_tasks.remove(current_task)


def print_console_error(message):
    print('\x1b[0;31;48m' + message + '\x1b[0m')


if __name__ == '__main__':
    Pyro4.config.SERVERTYPE = "multiplex"

    client = Client()

    while True:
        command = input().split()

        if command[0] == 'exec':
            try:
                function, values_file = command[1:]
                a, b = matrix.load_matrices(values_file)

                if function == 'add':
                    client.add(a, b)

                elif function == 'subtract':
                    client.sub(a, b)

                elif function == 'product':
                    client.mult(a, b)

                else:
                    print_console_error('Función incorrecta.')

            except ValueError:
                print_console_error('Cantidad de argumentos incorrecta.')
                print_console_error('La sintaxis es: exec <function> <values_file>.txt')

        elif command[0] == 'stats':
            # TODO Implementar los requerimientos para poder ejecutar el comando 'stats'
            pass

        else:
            print_console_error('No se reconoce el comando %s.' % command[0])
