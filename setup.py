import argparse

import parallel_tasks.libraries.matrix as matrix
from parallel_tasks import client_matrix
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
        'matrix': client_matrix.Shell
    }[args.type]().cmdloop()


def generate(args):
    matrix.get_random_file(args.filename, *args.dimensions)


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

    # Subparser for the 'generate' command
    parser_generate = subparsers.add_parser('generate')
    parser_generate.add_argument('filename')
    parser_generate.add_argument('dimensions', type=int, nargs=4)
    parser_generate.set_defaults(func=generate)

    args = parser.parse_args()
    args.func(args)
