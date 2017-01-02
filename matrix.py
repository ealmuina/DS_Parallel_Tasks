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


def _vector_add_sub(data, row_index, subtract=False):
    a, b = data
    x = a[row_index]
    y = b[row_index]

    result = []
    for i in range(len(x)):
        result.append(x[i] - y[i] if subtract else x[i] + y[i])
    return result


def vector_add(data, row_index):
    return _vector_add_sub(data, row_index)


def vector_sub(data, row_index):
    return _vector_add_sub(data, row_index, True)


def vector_mult(data, row_index):
    a, m = data
    v = a[row_index]

    result = [0] * len(m[0])

    for i in range(len(m)):
        for j in range(len(m[i])):
            result[j] += v[i] * m[i][j]

    return result


def load_matrices(file):
    # TODO Implementar correctamente
    return get_random_matrix(100, 100), get_random_matrix(100, 100)


if __name__ == '__main__':
    print(get_random_matrix(2, 2))
