import random
import threading
import time

import Pyro4

from .libraries import utils


@Pyro4.expose
class Node:
    CHECK_IP_INTERVAL = 5  # Time (seconds) between verifications if IP address is still the same.
    PYRO_TIMEOUT = 5  # Time (seconds) after Pyro4 normal connections will be interrupted if unfinished.
    MAX_PYRO_DAEMONS = 10  # Maximum number of Pyro4 daemons in which the node will be registered at the same time.

    def __init__(self):
        self.ip = utils.get_ip()
        self.daemons = {}  # key=<ip address>, value=(<Pyro4 daemon>, <uri of the node at daemon>)

        local_daemon = Pyro4.Daemon()  # Special daemon bound to the localhost address
        self._local_uri = local_daemon.register(self).asString()
        self.daemons['127.0.0.1'] = (local_daemon, self._local_uri)

        threading.Thread(target=local_daemon.requestLoop, daemon=True).start()
        threading.Thread(target=self._ip_address_check_loop, daemon=True).start()

        self._update_Pyro_daemons()

    def _ip_address_check_loop(self):
        """
        Constantly check if node's IP address is still the same.
        If not, make the pertinent changes on node's status.
        It's intended to run 'forever' on a separated thread.
        """

        while True:
            ip = utils.get_ip()
            if ip != self.ip:
                self.ip = ip
                self._update_Pyro_daemons()

                try:
                    self.log.report('IP address changed to: %s' % utils.get_ip())
                except AttributeError:
                    # There is no log on this node. Just ignore that
                    pass

            time.sleep(Node.CHECK_IP_INTERVAL)  # rest some time before next check

    def _update_Pyro_daemons(self):
        """
        Update relationships between the object and the daemons it is running on.
        """

        if self.ip in self.daemons:
            # There is already a daemon where the object is registered with this IP, just update self.uri
            __, self.uri = self.daemons[self.ip]

        else:
            # This is a new IP for the node. Create a new daemon for it

            if len(self.daemons) == Node.MAX_PYRO_DAEMONS:
                # Daemons storage is full. Remove a random daemon (except for the local) to make room for the new one
                self.daemons.pop(random.choice(list(self.daemons.keys()).remove('127.0.0.1')))

            ip = self.ip
            daemon = Pyro4.Daemon(host=ip)
            self.uri = daemon.register(self, force=True).asString()  # force=True because of Pyro4 behavior
            self.daemons[ip] = (daemon, self.uri)

            # Run the daemon's requestLoop on a separated thread and set its end condition to be the removal of it's ip
            # from the self.daemons dictionary
            threading.Thread(target=daemon.requestLoop, args=(lambda: ip in self.daemons,), daemon=True).start()

    def close(self):
        """
        Release all resources owned by the node and clean its mess.
        """
        try:
            self.log.report('Closing node %s...' % self.uri, True)
        except AttributeError:
            # There is no log on this node. Just ignore that
            pass

    @property
    def local_uri(self):
        return self._local_uri
