import os
import random


def get_random_file(filename, length):
    with open(os.path.join('input', filename), 'w') as f:
        s = get_random_string(length)
        f.writelines((s + '\n',))


def get_random_string(length):
    result = ''
    for i in range(length):
        a = random.choice(('a', 'A'))
        result += chr(ord(a) + random.randint(0, 26))
    return result


def to_upper(c, index):
    return c[index].upper()
