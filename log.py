import os
from datetime import datetime


class Log:
    """
    This class is intended to factorize reporting code.
    """

    def __init__(self, name):
        self.name = name

    def report(self, message, write_message=False, message_type=None):
        """
        Emit a report to the log's file if indicated.
        :param message: String of text to be reported
        :param write_message: Boolean value to indicate if message will be actually written on log
        :param message_type: String with value in ('red', 'green') indicating text color on console, if provided.
        """

        if not write_message:
            return None

        os.makedirs('logs', exist_ok=True)
        file = open('logs/' + self.name + '_log.txt', 'a')

        s = '[{0}]: {1}'.format(datetime.now(), message)
        file.write(s + '\n')
        file.close()

        if message_type == 'red':
            s = '\x1b[6;30;41m' + s + '\x1b[0m'
        elif message_type == 'green':
            s = '\x1b[6;30;42m' + s + '\x1b[0m'
        print(s)


if __name__ == '__main__':
    log = Log('test')
    log.report('testing.')
