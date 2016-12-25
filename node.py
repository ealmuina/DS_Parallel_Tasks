import threading
import time

import Pyro4

import utils


class Node():
    CHECK_IP_INTERVAL = 5  # Tiempo (segundos) transcurrido el cual se verificará si la IP sigue siendo la misma.

    def __init__(self):
        self.ip = utils.get_ip()
        threading.Thread(target=self._ip_address_check_loop).start()

        self._update_Pyro_daemon()

    def _ip_address_check_loop(self):
        while True:
            ip = utils.get_ip()
            if ip != self.ip:
                self.ip = ip
                self._update_Pyro_daemon()

            time.sleep(Node.CHECK_IP_INTERVAL)

    def _update_Pyro_daemon(self):
        # Cerrar self.daemon si ya existía
        try:
            self.daemon.unregister(self)  # TODO Verificar si sera esta una solucion
            self.daemon.shutdown()
            # TODO Esto produce una excepcion en un hilo que el mismo crea y q no tengo forma de capturarla. WTF???
        except AttributeError:
            # self todavía no tiene un atributo daemon
            pass

        self.daemon = Pyro4.Daemon(host=utils.get_ip())
        self.uri = self.daemon.register(self, force=True).asString()
        threading.Thread(target=self.daemon.requestLoop).start()

        try:
            self.log.report('Dirección IP modificada a: %s' % utils.get_ip())
        except AttributeError:
            # El nodo no tiene un log asociado
            pass
