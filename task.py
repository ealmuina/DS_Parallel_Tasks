class Task:
    def __init__(self, matrix_a, matrix_b, func):
        self.completed = False
        self.result = [[] * len(matrix_a)]
