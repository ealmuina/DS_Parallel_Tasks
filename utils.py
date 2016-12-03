import os
import socket


def get_ip():
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


if __name__ == '__main__':
    print(get_ip())
