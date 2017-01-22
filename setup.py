import argparse
import time

from parallel_tasks import client_matrix
from parallel_tasks import worker


def run_worker(args):
    worker.Worker()
    while True:
        time.sleep(100)


def run_client(args):
    {
        'matrix': client_matrix.ClientShell
    }[args.type]().cmdloop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    # Subparser for the 'worker' command
    parser_worker = subparsers.add_parser('worker')
    parser_worker.set_defaults(func=run_worker)

    # Subparser for the 'client' command
    parser_client = subparsers.add_parser('client')
    parser_client.add_argument('--type', default='matrix')
    parser_client.set_defaults(func=run_client)

    args = parser.parse_args()
    args.func(args)
