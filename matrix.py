import random


def get_random_matrix(n, m):
    matrix = []
    for i in range(n):
        row = [random.randint(1, 100) for j in range(m)]
        matrix.append(row)
    return matrix


def str_matrix(a):
    pass


def vector_add(data):
    """Adiciona dos vectores de igual dimensión, componente a componente."""

    x, y = data
    result = []
    for i in range(len(x)):
        result.append(x[i] + y[i])
    return result


def vector_sub(data):
    pass


def vector_mult(data):
    pass


if __name__ == '__main__':
    print(get_random_matrix(2, 2))
