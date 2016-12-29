import os

import matrix
from client import Client
from task import Task, Subtask


class MatrixClient(Client):
    def _add_sub(self, a, b, subtract=False):
        """Adiciona o resta dos matrices"""

        if len(a) != len(b):
            raise ArithmeticError('Las dimensiones de las matrices deben coincidir.')

        # Crear nueva tarea
        task = Task(len(a), self.task_number, (a, b))
        self.pending_tasks.add(task)
        self.task_number += 1

        # Crear subtareas para la suma de las filas correspondientes en las matrices
        for i in range(len(a)):
            st = Subtask(task, i, 'matrix.vector_sub' if subtract else 'matrix.vector_add')
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
        task = Task(len(a), self.task_number, (a, b))
        self.pending_tasks.add(task)
        self.task_number += 1

        # Crear subtareas para el producto de las filas de 'a' por la matriz 'b'
        for i in range(len(a)):
            st = Subtask(task, i, 'matrix.vector_mult')
            self.pending_subtasks.put((st.time, st))
            self.pending_subtasks_dic[(task.id, i)] = st

    def save_result(self, task):
        os.makedirs('results', exist_ok=True)
        file_result = open('results/%s.txt' % task.id, 'w')
        file_result.write(matrix.str_matrix(task.result))


def print_console_error(message):
    print('\x1b[0;31;48m' + message + '\x1b[0m')


if __name__ == '__main__':
    client = MatrixClient()

    while True:
        command = input().split()

        if command[0] == 'exec':
            try:
                function, values_file = command[1:]
                a, b = matrix.load_matrices(values_file)
                print('Matrices cargadas. Iniciando operación...')

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
