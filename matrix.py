import os
import random


def get_random_file(filename, n1, m1, n2, m2):
    with open(os.path.join('input', filename), 'w') as f:
        matrix1 = get_random_matrix(n1, m1)
        matrix2 = get_random_matrix(n2, m2)

        f.writelines(('%d %d\n' % (n1, m1), str_matrix(matrix1) + '\n'))
        f.writelines(('%d %d\n' % (n2, m2), str_matrix(matrix2) + '\n'))


def get_random_matrix(n, m):
    """
    Return a randomly generated matrix.
    :param n: Number of rows
    :param m: Number of columns
    :return: List of lists of integers, representing the generated matrix
    """

    matrix = []
    for i in range(n):
        row = [random.randint(1, 100) for j in range(m)]
        matrix.append(row)
    return matrix


def load_matrices(file):
    """
    Load matrices from a file.
    :param file: String with the file's path
    :return: Two lists of lists representing the loaded matrices
    """

    with open(file) as f:
        ans = []

        for k in range(2):
            matrix = []
            dimensions = f.readline()
            n, m = map(int, dimensions.split())

            for i in range(n):
                r = []
                row = f.readline().split()
                for j in range(m):
                    r.append(int(row[j]))
                matrix.append(r)

            ans.append(matrix)
        return tuple(ans)


def str_matrix(a):
    """
    Return the string representation of a matrix.
    :param a: List of lists of numbers, representing a matrix
    :return: String representation of the matrix
    """

    s = ''
    for i in range(len(a)):
        for j in range(len(a[i])):
            s += str(a[i][j]) + ' '
        if i != len(a) - 1:
            s += '\n'
    return s


def _vector_add_sub(data, row_index, subtract=False):
    """
    Add or subtract corresponding rows of two matrices.
    :param data: Tuple with two matrices
    :param row_index: Index of the matrices's rows to operate with
    :param subtract: Boolean flag indicating operation to made. True -> addition, False -> subtraction
    :return: Vector as a list with the operation result.
    """

    a, b = data
    x = a[row_index]
    y = b[row_index]

    result = []
    for i in range(len(x)):
        result.append(x[i] - y[i] if subtract else x[i] + y[i])
    return result


def vector_add(data, row_index):
    """
    Add corresponding rows of two matrices.
    :param data: Tuple with two matrices
    :param row_index: Index of the matrices's rows to operate with
    :return: Vector as a list with the operation result.
    """
    return _vector_add_sub(data, row_index)


def vector_mult(data, row_index):
    """
    Multiply a row of a matrix with another matrix.
    :param data: Tuple with two matrices
    :param row_index: Index of the first matrix's row to operate with
    :return: Vector as a list with the operation result.
    """

    a, m = data
    v = a[row_index]

    result = [0] * len(m[0])

    for i in range(len(m)):
        for j in range(len(m[i])):
            result[j] += v[i] * m[i][j]

    return result


def vector_sub(data, row_index):
    """
    Subtract corresponding rows of two matrices.
    :param data: Tuple with two matrices
    :param row_index: Index of the matrices's rows to operate with
    :return: Vector as a list with the operation result.
    """
    return _vector_add_sub(data, row_index, True)


if __name__ == '__main__':
    get_random_file('test.txt', 100, 100, 100, 100)
