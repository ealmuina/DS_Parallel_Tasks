import threading
import time

import Pyro4

import utils


class Node():
    CHECK_IP_INTERVAL = 5  # Tiempo (segundos) transcurrido el cual se verificará si la IP sigue siendo la misma.
    PYRO_TIMEOUT = 5

    def __init__(self):
        self.ip = utils.get_ip()
        self.daemons = {}

        threading.Thread(target=self._ip_address_check_loop, daemon=True).start()

        self._update_Pyro_daemon()

    def _ip_address_check_loop(self):
        while True:
            ip = utils.get_ip()
            if ip != self.ip:
                self.ip = ip
                self._update_Pyro_daemon()

            time.sleep(Node.CHECK_IP_INTERVAL)

    def _update_Pyro_daemon(self):
        if self.ip in self.daemons:
            daemon, self.uri = self.daemons[self.ip]
        else:
            daemon = Pyro4.Daemon(host=self.ip)
            self.uri = daemon.register(self, force=True).asString()
            self.daemons[self.ip] = (daemon, self.uri)
            threading.Thread(target=daemon.requestLoop, daemon=True).start()

        try:
            self.log.report('Dirección IP modificada a: %s' % utils.get_ip())
        except AttributeError:
            # El nodo no tiene un log asociado
            pass
