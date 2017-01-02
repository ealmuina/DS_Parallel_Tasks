from datetime import datetime


class Task:
    """
    Represent a task. It's used to wrap a task properties.
    """

    def __init__(self, length, task_id, data):
        self.completed = False
        self.result = [None] * length
        self.id = task_id
        self.time = datetime.now()
        self.data = data


class Subtask:
    """
    Represent a sub-task. It's used to wrap a sub-task properties.
    """

    def __init__(self, task, index, func):
        self.task = task
        self.index = index
        self.func = func

        self.completed = False
        self.time = datetime.min
