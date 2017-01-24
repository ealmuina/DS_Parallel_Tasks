import cmd
import os

from .client import Client
from .libraries.task import Task, Subtask


class StringClient(Client):
    def to_upper(self, s):
        """
        Return a copy of the string with all the cased characters converted to uppercase
        :param s: String
        :return: Uppercase string
        """

        # Create a new task.
        task = Task(len(s), self.task_number, s)
        self.pending_tasks.add(task)
        self.task_number += 1

        # Create subtasks for operating with each character
        for i in range(len(s)):
            st = Subtask(task, i, "string.to_upper")
            self.pending_subtasks.put((st.time, st))
            self.pending_subtasks_dic[(task.id, i)] = st

    def save_result(self, task):
        """
        Save a task's results to a file.
        :param task: Task whose results will be saved
        """

        os.makedirs('results', exist_ok=True)
        file_result = open('results/%s.txt' % task.id, 'w')
        file_result.write(''.join(task.result))


class Shell(cmd.Cmd):
    prompt = ''

    def __init__(self):
        super().__init__()
        self.client = StringClient()

    def do_exec(self, arg):
        try:
            function, values_file = arg.split()

            operations = {
                'up': self.client.to_upper
            }
            function = operations[function]

            s = ''
            with open(os.path.join('input', values_file)) as f:
                s = f.read()
            self.client.log.report('Loaded file. Starting operation...', True)
            function(s)

        except Exception as e:
            print('%s: %s' % (type(e).__name__, e))

    def do_EOF(self, arg):
        return self.do_exit(arg)

    def do_exit(self, arg):
        self.client.close()
        return -1

    def do_stats(self, arg):
        self.client.print_stats()
