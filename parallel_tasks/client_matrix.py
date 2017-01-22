import cmd
import os

from .client import Client
from .libraries import matrix
from .task import Task, Subtask


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

        if len(a) != len(b) or len(a[0]) != len(b[0]):
            raise ArithmeticError('Las dimensiones de las matrices deben coincidir.')

        # Create a new task
        task = Task(len(a), self.task_number, (a, b))
        self.pending_tasks.add(task)
        self.task_number += 1

        # Create sub-tasks for operating corresponding rows on matrices
        for i in range(len(a)):
            st = Subtask(task, i, 'libraries.matrix.vector_sub' if subtract else 'libraries.matrix.vector_add')
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
            st = Subtask(task, i, 'libraries.matrix.vector_mult')
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

    def sub(self, a, b):
        """
        Subtract two matrices.
        :param a: matrix 1
        :param b: matrix 2
        :return: This method is intended to schedule the operation, not to return its result immediately.
        """

        self._add_sub(a, b, True)


class ClientShell(cmd.Cmd):
    prompt = ''

    def __init__(self):
        super().__init__()
        self.client = MatrixClient()

    def do_exec(self, arg):
        try:
            function, values_file = arg.split()

            operations = {
                'add': self.client.add,
                'sub': self.client.sub,
                'mult': self.client.mult
            }
            function = operations[function]

            a, b = matrix.load_matrices(os.path.join('input', values_file))
            print('Matrices cargadas. Iniciando operación...')
            function(a, b)

        except Exception as e:
            print('%s: %s' % (type(e).__name__, e))

    def do_EOF(self, arg):
        return -1

    def do_exit(self, arg):
        return -1

    def do_stats(self, arg):
        self.client.print_stats()
