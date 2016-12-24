import heapq
import os
import threading
import time
from datetime import datetime
from queue import Queue

import Pyro4
from Pyro4 import socketutil as pyrosocket
from Pyro4.errors import PyroError

import log
import matrix
from node import Node
from task import Task, Subtask
from worker import Worker

Pyro4.config.COMMTIMEOUT = 5  # 5 seconds
Pyro4.config.SERVERTYPE = "multiplex"

Pyro4.config.SERIALIZER = 'pickle'
Pyro4.config.SERIALIZERS_ACCEPTED.add('pickle')


@Pyro4.expose
class Client(Node):
    SCANNER_TIMEOUT = 1  # Tiempo (segundos) de espera del socket que escanea el sistema en busca de workers
    SCANNER_INTERVAL = 10  # Tiempo (segundos) entre escaneos del sistema
    SUBTASKS_TIMEOUT = 2  # Tiempo (segundos) de espera por el resultado de una operacion asignada a un worker

    def __init__(self):
        super().__init__()

        self.workers = []  # Workers accesibles, priorizados según su carga
        self.worker = Worker()  # Worker del sistema correspondiente al equipo
        self.lock = threading.Lock()  # Lock para el uso de 'self.workers'

        self.log = log.Log('client')

        self.pending_tasks = set()
        self.pending_subtasks = Queue()
        self.pending_subtasks_dic = {}  # Mapea una tupla (task_id, index) a la subtarea pendiente correspondiente
        self.task_number = 0  # Entero usado para asignar identificadores a las tareas

        threading.Thread(target=self._scan_loop).start()
        threading.Thread(target=self._subtasks_checker_loop).start()

        self.log.report('Cliente inicializado.', True)

    def _scan_loop(self):
        """Escanea la red en busca de workers y actualiza una cola con prioridad según la carga de estos."""

        while True:
            scanner = pyrosocket.createBroadcastSocket()
            scanner.settimeout(Client.SCANNER_TIMEOUT)

            updated_nodes = []
            try:
                scanner.sendto(b'SCANNING', ('255.255.255.255', 5555))
                while True:
                    try:
                        data, address = scanner.recvfrom(1024)
                    except ConnectionResetError:
                        # Se cerró la conexión antes de tiempo. Continuar iterando
                        continue

                    uri = data.decode()

                    try:
                        current_node = Pyro4.Proxy(uri)
                        updated_nodes.append((current_node.get_load(), uri))

                    except PyroError:
                        # TODO Chequear que la excepcion es correcta
                        # Si los datos recibidos no son una uri válida, al tratar de crear el proxy,
                        # esta excepción es lanzada.
                        continue

            except OSError as e:
                if e.errno == 101:
                    # La red está desconectada. Solo podrá ser usado el worker propio.
                    updated_nodes.append((self.worker.get_load(), self.worker.uri))

            finally:
                heapq.heapify(updated_nodes)
                with self.lock:
                    self.workers.clear()
                    for n in updated_nodes:
                        self.workers.append(n)

            self.log.report('Sistema escaneado. Se detectaron %d workers.' % len(updated_nodes))
            time.sleep(Client.SCANNER_INTERVAL)

    def _subtasks_checker_loop(self):
        """Chequea si ha expirado el tiempo de espera por el resultado de alguna operacion.
        Si esto ocurre, la asigna a un nuevo worker."""

        while True:
            if len(self.workers) == 0:
                # No se han encontrado workers del sistema a los que asignar subtareas
                continue

            t, st = self.pending_subtasks.get()
            elapsed = datetime.now() - t

            if st.completed:
                # La subtarea ya fue completada. Seguir iterando
                continue

            if elapsed.total_seconds() > Client.SUBTASKS_TIMEOUT:
                # Tiempo de espera superado, asignar operacion a un nuevo worker
                with self.lock:
                    load, uri = heapq.heappop(self.workers)
                    st.time = datetime.now()

                    try:
                        n = Pyro4.Proxy(uri)
                        data = st.data if not st.func in {'*'} else st.data[0]
                        n.process(data, st.func, (st.task.id, st.index), self.uri)
                        heapq.heappush(self.workers, (n.get_load(), uri))

                        self.log.report('Asignada la subtarea %s al worker %s' % ((st.task.id, st.index), uri))

                    except PyroError:
                        self.log.report(
                            'Se intentó enviar subtarea al worker %s, pero no se encuentra accesible.' % uri,
                            True, 'red')

            self.pending_subtasks.put((st.time, st))

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

        if len(a[0]) != len(b):
            raise ArithmeticError(
                "La cantidad de columnas de la matriz 'a' debe coincidir con la cantidad de filas de 'b'.")

        # Crear nueva tarea
        task = Task(len(a), self.task_number)
        self.pending_tasks.add(task)
        self.task_number += 1

        # Crear subtareas para el producto de las filas de 'a' por la matriz 'b'
        for i in range(len(a)):
            st = Subtask(task, i, (a[i], b), '*')
            self.pending_subtasks.put((st.time, st))
            self.pending_subtasks_dic[(task.id, i)] = st

    def set_report(self, subtask_id, result):
        """Reporta al cliente el resultado de una operacion solicitada por este a uno de los workers del sistema."""

        # Localizar la subtarea correspondiente al id y marcarla como completada.
        # Si no se encuentra la subtarea, entonces ya fue resuelta. Terminar el llamado al método.
        try:
            subtask = self.pending_subtasks_dic.pop(subtask_id)
        except KeyError:
            self.log.report(
                'Un worker reportó el resultado de una operación ya completada. La respuesta será desechada.')
            return None

        subtask.completed = True

        current_task = subtask.task

        # Copiar el resultado de la subtarea al de la tarea correspondiente
        current_task.result[subtask.index] = result

        # Verficar si la tarea fue completada
        current_task.completed = True
        for x in current_task.result:
            current_task.completed = current_task.completed and x

        if current_task.completed:
            # Guardar el resultado de la tarea en el archivo <current_task.id>.txt
            os.makedirs('results', exist_ok=True)
            file_result = open('results/%s.txt' % current_task.id, 'w')
            file_result.write(matrix.str_matrix(current_task.result))

            self.log.report(
                'Tarea %(id)s completada. Puede ver el resultado en el archivo %(id)s.txt.\nTiempo total: %(time)s'
                % {'id': current_task.id, 'time': datetime.now() - current_task.time}, True, 'green')

            # Eliminar la tarea de la lista de tareas pendientes
            self.pending_tasks.remove(current_task)

    def print_stats(self):
        print('Worker', 'Operaciones', 'Tiempo total', 'Tiempo promedio', sep='\t')
        with self.lock:
            for load, uri in self.workers:
                try:
                    n = Pyro4.Proxy(uri)

                    total_operations = n.get_total_operations()
                    total_time = n.get_total_time()
                    avg_time = total_time / total_operations if total_operations != 0 else 0

                    print(n.get_ip(), total_operations, total_time, avg_time, sep='\t')

                except PyroError:
                    # No se pudo completar la conexión al worker
                    pass

    def get_data(self, subtask_id):
        """Retorna los datos correspondientes a una subtarea si no ha sido completada."""

        if subtask_id in self.pending_subtasks_dic:
            subtask = self.pending_subtasks_dic[subtask_id]
            return subtask.data
        return None


def print_console_error(message):
    print('\x1b[0;31;48m' + message + '\x1b[0m')


if __name__ == '__main__':
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
            client.print_stats()

        else:
            print_console_error('No se reconoce el comando %s.' % command[0])
