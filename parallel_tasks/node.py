import random
import threading
import time

import Pyro4

from parallel_tasks.libraries import utils


@Pyro4.expose
class Node:
    CHECK_IP_INTERVAL = 5  # Tiempo (segundos) transcurrido el cual se verificará si la IP sigue siendo la misma.
    PYRO_TIMEOUT = 5
    MAX_PYRO_DAEMONS = 10

    def __init__(self):
        self.ip = utils.get_ip()
        self.daemons = {}

        local_daemon = Pyro4.Daemon()
        self._local_uri = local_daemon.register(self).asString()
        self.daemons['127.0.0.1'] = (local_daemon, self._local_uri)

        threading.Thread(target=local_daemon.requestLoop, daemon=True).start()
        threading.Thread(target=self._ip_address_check_loop, daemon=True).start()

        self._update_Pyro_daemons()

    def _ip_address_check_loop(self):
        while True:
            ip = utils.get_ip()
            if ip != self.ip:
                self.ip = ip
                self._update_Pyro_daemons()

                try:
                    self.log.report('Dirección IP modificada a: %s' % utils.get_ip())
                except AttributeError:
                    # El nodo no tiene un log asociado
                    pass

            time.sleep(Node.CHECK_IP_INTERVAL)

    def _update_Pyro_daemons(self):
        if self.ip in self.daemons:
            daemon, self.uri = self.daemons[self.ip]
        else:
            if len(self.daemons) == Node.MAX_PYRO_DAEMONS:
                self.daemons.pop(random.choice(list(self.daemons.keys())))

            ip = self.ip
            daemon = Pyro4.Daemon(host=ip)
            self.uri = daemon.register(self, force=True).asString()
            self.daemons[ip] = (daemon, self.uri)
            threading.Thread(target=daemon.requestLoop, args=(lambda: ip in self.daemons,), daemon=True).start()

    def close(self):
        self.log.report('Cerrando nodo %s...' % self.uri, True, 'red')

    @property
    def local_uri(self):
        return self._local_uri
