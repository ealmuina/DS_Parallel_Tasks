from datetime import datetime


class Task:
    def __init__(self, length, task_id):
        self.completed = False
        self.result = [None] * length
        self.id = task_id
        self.time = datetime.now()


class Subtask:
    def __init__(self, task, index, data, func):
        self.task = task
        self.index = index
        self.data = data
        self.func = func

        self.completed = False
        self.time = datetime.min
