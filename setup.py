import argparse

import parallel_tasks.libraries.matrix as matrix
import parallel_tasks.libraries.string as string
from parallel_tasks import client_matrix, client_string
from parallel_tasks import worker


def run_worker(args):
    w = worker.Worker()
    print('Type "exit" or EOF to finish.')
    while True:
        try:
            s = input()
            if s == 'exit':
                w.close()
                break
        except EOFError:
            w.close()
            break


def run_client(args):
    {
        'matrix': client_matrix.Shell,
        'string': client_string.Shell
    }[args.type]().cmdloop()


def generate_matrix(args):
    matrix.get_random_file(args.filename, *args.dimensions)


def generate_string(args):
    string.get_random_file(args.filename, args.length)


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

    # Subparser for the 'generate_matrix' command
    parser_matrix = subparsers.add_parser('gen_matrix')
    parser_matrix.add_argument('filename')
    parser_matrix.add_argument('dimension', type=int, nargs=4)
    parser_matrix.set_defaults(func=generate_matrix)

    # Subparser for the 'generate_string' command
    parser_matrix = subparsers.add_parser('gen_string')
    parser_matrix.add_argument('filename')
    parser_matrix.add_argument('length', type=int)
    parser_matrix.set_defaults(func=generate_string)

    args = parser.parse_args()
    args.func(args)
