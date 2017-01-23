import os
import shutil
import sys
import time


def remove_temp_folder(uri):
    time.sleep(3)
    wid = uri.split(':')[-1]
    temp_path = '%s_temp' % wid

    if os.path.exists(temp_path):
        shutil.rmtree(temp_path)


if __name__ == '__main__':
    remove_temp_folder(sys.argv[1])
