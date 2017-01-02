import os

import matrix
from client import Client
from task import Task, Subtask


class MatrixClient(Client):
    """
    Specific implementation of Client to use the system for matrices operations.
    """

    def _add_sub(self, a, b, subtract=False):
        """
        Add or subtract two matrices.
        :param a: matrix 1
        :param b: matrix 2
        :param subtract: Boolean indicating if operation executed will be addition (False) or subtraction (True)
        This method is intended to schedule the operation, not to return its result immediately.
        """

        if len(a) != len(b):
            raise ArithmeticError('Las dimensiones de las matrices deben coincidir.')

        # Create a new task
        task = Task(len(a), self.task_number, (a, b))
        self.pending_tasks.add(task)
        self.task_number += 1

        # Create sub-tasks for operating corresponding rows on matrices
        for i in range(len(a)):
            st = Subtask(task, i, 'matrix.vector_sub' if subtract else 'matrix.vector_add')
            self.pending_subtasks.put((st.time, st))
            self.pending_subtasks_dic[(task.id, i)] = st

    def add(self, a, b):
        """
        Add two matrices.
        :param a: matrix 1
        :param b: matrix 2
        :return: This method is intended to schedule the operation, not to return its result immediately.
        """

        self._add_sub(a, b)

    def sub(self, a, b):
        """
        Subtract two matrices.
        :param a: matrix 1
        :param b: matrix 2
        :return: This method is intended to schedule the operation, not to return its result immediately.
        """

        self._add_sub(a, b, True)

    def mult(self, a, b):
        """
        Multiply two matrices.
        :param a: matrix 1
        :param b: matrix 2
        :return: This method is intended to schedule the operation, not to return its result immediately.
        """

        if len(a[0]) != len(b):
            raise ArithmeticError(
                "La cantidad de columnas de la matriz 'a' debe coincidir con la cantidad de filas de 'b'.")

        # Create a new task
        task = Task(len(a), self.task_number, (a, b))
        self.pending_tasks.add(task)
        self.task_number += 1

        # Create sub-tasks for operating corresponding rows on matrices
        for i in range(len(a)):
            st = Subtask(task, i, 'matrix.vector_mult')
            self.pending_subtasks.put((st.time, st))
            self.pending_subtasks_dic[(task.id, i)] = st

    def save_result(self, task):
        """
        Save a task's results to a file.
        :param task: Task whose results will be saved
        """

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

                elif function == 'sub':
                    client.sub(a, b)

                elif function == 'mult':
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
