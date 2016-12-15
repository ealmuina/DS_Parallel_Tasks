from datetime import datetime


class Log:
    def __init__(self, name):
        self.name = name

    def report(self, message, on_console=False, message_type=None):
        file = open(self.name + '_log.txt', 'a')
        s = '[{0}]: {1}'.format(datetime.now(), message)
        file.write(s + '\n')
        file.close()

        if on_console:
            if message_type == 'red':
                s = '\x1b[6;30;41m' + s + '\x1b[0m'
            elif message_type == 'green':
                s = '\x1b[6;30;42m' + s + '\x1b[0m'
            print(s)


if __name__ == '__main__':
    log = Log('test')
    log.report('testing.')
