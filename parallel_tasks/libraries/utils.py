import os
import socket


def get_ip():
    """
    Return the ip address of the current machine.
    :return: String with the ip address
    """

    if os.name == 'nt':
        return socket.gethostbyname(socket.gethostname())
    else:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            # doesn't even have to be reachable
            s.connect(('10.255.255.255', 0))
            ip = s.getsockname()[0]
        except OSError:
            ip = '127.0.0.1'
        finally:
            s.close()
        return ip


def print_table(table):
    col_width = [max(len(x) for x in col) for col in zip(*table)]
    for line in table:
        print((" " * 3).join("{:{}}".format(x, col_width[i]) for i, x in enumerate(line)))


if __name__ == '__main__':
    print(get_ip())
