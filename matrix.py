import random


def get_random_matrix(n, m):
    matrix = []
    for i in range(n):
        row = [random.randint(1, 100) for j in range(m)]
        matrix.append(row)
    return matrix


def str_matrix(a):
    s = ''
    for i in range(len(a)):
        for j in range(len(a[i])):
            s += str(a[i][j]) + ' '
        if i != len(a) - 1:
            s += '\n'
    return s


def _vector_add_sub(data, subtract=False):
    """Adiciona o resta dos vectores de igual dimensión, componente a componente."""

    x, y = data
    result = []
    for i in range(len(x)):
        result.append(x[i] - y[i] if subtract else x[i] + y[i])
    return result


def vector_add(data):
    """Adiciona dos vectores de igual dimensión, componente a componente."""

    return _vector_add_sub(data)


def vector_sub(data):
    """Resta dos vectores de igual dimensión, componente a componente."""

    return _vector_add_sub(data, True)


def vector_mult(data):
    """Multiplica un vector por una matriz."""

    v, m = data
    result = [0] * len(m[0])

    for i in range(len(m)):
        for j in range(len(m[i])):
            result[j] += v[i] * m[i][j]

    return result


def load_matrices(file):
    # TODO Implementar correctamente
    return get_random_matrix(100, 1000), get_random_matrix(1000, 1000)


if __name__ == '__main__':
    print(get_random_matrix(2, 2))
