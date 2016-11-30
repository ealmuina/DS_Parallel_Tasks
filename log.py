from datetime import datetime


class Log:
    def __init__(self, name):
        self.name = name

    def report(self, message):
        file = open(self.name + '_log.txt', 'a')
        s = '[{0}]: {1}'.format(datetime.now(), message)
        file.write(s + '\n')
        file.close()
        print(s)


if __name__ == '__main__':
    log = Log('test')
    log.report('testing.')
