# ===============================================================================
#
# Copyright (c) 2010, 2011, 2015 by Georg Fleig, Thomas Hauth and Stephan Riedel
#
# This file is part of ROCED.
#
# ROCED is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# ROCED is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with ROCED.  If not, see <http://www.gnu.org/licenses/>.
#
# ===============================================================================
from __future__ import unicode_literals

import collections
import functools
import getpass
import logging
import subprocess
import time

from Core import MachineRegistry


class Caching(dict):
    def __init__(self, validityPeriod=-1, redundancyPeriod=0):
        # type: (int, int)
        """ Decorator class to cache a method's/function's return value. Handles maximum age and errors.

        As long as data is "valid", the currently stored data will be returned. If none exists so far, it will be
        queried and stored.

        During the redundancy period, the function module will try to catch errors [return value "None"] by returning
        the previous result, if present. After the redundancy period, it will directly return the function's
        values/error.

        validityPeriod=0:               Read once, return forever
        validityPeriod>0:               As long as timeout is not reached, return value from cache.
        validityPeriod=-1:              Always call the function (only useful with error caching).

        These values are important, if the function raises an exception or returns with result None:
        redundancyPeriod=0:             If a value is cached, use this result instead
        redundancyPeriod>0:             As long as timeout is not reached, return value from cache instead (> validity!)
        redundancyPeriod=-1:            Return "None"

        Possible combinations:
        Caching(0,-1)                   Read once w/o error caching [regular memoization]
        Caching(x,0)|(x,y[>x])|(x,-1)   Read (valid for x seconds); /w error caching (for y seconds) or w/o
        Caching(-1,0)|(-1,y)            Read always; /w error caching (for y seconds)
        """
        super(Caching, self).__init__()
        self.__lastQueryTime = 0

        if redundancyPeriod > 0:
            self.__redundancy = redundancyPeriod
        elif redundancyPeriod == 0:
            # Always cache errors.
            self.__redundancy = True
        else:
            # No error caching.
            self.__redundancy = None

        if 0 < redundancyPeriod <= validityPeriod:
            # Error caching only makes sense, if it's longer than a single validity cycle.
            raise ValueError("redundancyPeriod (%i) <= validityPeriod (%i)." % (redundancyPeriod, validityPeriod))
        elif validityPeriod > 0:
            self.__validity = validityPeriod
        elif validityPeriod == 0:
            if redundancyPeriod >= 0:
                raise ValueError("redundancyPeriod (%i) & validityPeriod (%i) -> error caching is useless in this case."
                                 % (redundancyPeriod, validityPeriod))
            # Always return from cache (if possible)
            self.__validity = True
        elif redundancyPeriod < 0:
            # @Caching(-1,-1) has no effect at all.
            raise ValueError("redundancyPeriod (%i) & validityPeriod (%i) -> \"Do nothing\"."
                             % (redundancyPeriod, validityPeriod))
        else:
            # Never return from cache.
            self.__validity = None

    def __call__(self, function):
        """Since __init__ has import arguments, __call__ is only called once and receives the function!"""
        self.__function = function

        def wrapped_function(*args):
            """This is the wrapped function which "replaces" the original."""
            if not isinstance(args, collections.Hashable):
                # unhashable argument, e.g.: list
                return self.__function(*args)
            if (args in self and self.__validity is True or
                    (self.__validity is not None and time.time() < self.__lastQueryTime + self.__validity)):
                # Cache forever or timeout not yet reached.
                return self[args]
            else:
                # Intentionally querying new value(s).
                result = self.__missing__(args)
                logging.debug("Queried new values. Result: %s" % result)
                if result is not None:
                    self[args] = result
                    return self[args]
                else:
                    if self.__redundancy is None:
                        return result
                    elif self.__redundancy is True and args in self:
                        return self[args]
                    elif time.time() < self.__lastQueryTime + self.__redundancy and args in self:
                        return self[args]
                    else:
                        # This includes passing the timeout or not having a value stored at all
                        return result

        return wrapped_function

    def __missing__(self, key):
        """Method is called internally (if necessary), since we're inheriting from a dictionary."""
        try:
            ret = self.__function(*key)
            self.__lastQueryTime = time.time()
        except:
            ret = None
        return ret

    def __repr__(self):
        """Return the function's docstring."""
        return self.__function.__doc__

    def __get__(self, obj, objtype):
        """Support instance methods."""
        return functools.partial(self.__call__, obj)


class StateLogger(object):
    """ store information in a CSV file

        machine requirement

        site availability
        site running instances

        overall running machine types
    """
    pass


class ChangeNotifier(object):
    def __init__(self, machineReg):
        self.mr = machineReg
        self.mr.registerListener(self)
        self.cached = []

    def onEvent(self, evt):
        # type: (MachineRegistry.StatusChangedEvent) -> None
        if isinstance(evt, MachineRegistry.StatusChangedEvent):
            s = ("Machine type %s on site %s changed status from %s to %s." %
                 (self.mr.machines[evt.id].get(self.mr.reg_machine_type),
                  self.mr.machines[evt.id].get(self.mr.reg_site), evt.oldStatus, evt.newStatus))

            self.cachedNotify("Scale status changed", s)

    def cachedNotify(self, title, body):
        self.cached.append((title, body))

    @staticmethod
    def notify(title, body):
        try:
            import pynotify

            if pynotify.init("scale"):
                n = pynotify.Notification(title, body)
                n.show()
                n.set_timeout(5)
        except:
            # is ok
            pass

    def displayCachedNotifications(self):
        if len(self.cached) == 0:
            return

        newBody = ""

        for tuple_ in self.cached:
            newBody += "%s\n%s\n\n" % tuple_

        self.notify("scale", newBody)
        self.cached = []


class Shell(object):
    @staticmethod
    def executeCommand(command, environment=None, quiet=False):
        """Execute command in shell on localhost."""
        p = subprocess.Popen(command,
                             bufsize=0, executable=None,
                             shell=True,
                             stdin=None,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE,
                             env=environment)
        stdout, stderr = p.communicate()

        if not quiet:
            if not p.returncode == 0:
                logging.error("Shell command (localhost) failed (RC %i)." % p.returncode)
                logging.error("command: %s" % command)
                logging.error("stdout: %s" % stdout)
                logging.error("stderr: %s" % stderr)
            else:
                logging.info("Shell command (localhost) successful (RC %i)." % p.returncode)
                logging.info("command: %s" % command)
                logging.info("stdout: %s" % stdout)
            if stderr:
                logging.info("stderr: %s" % stderr)

        return p.returncode, stdout.decode(encoding="utf-8"), stderr.decode(encoding="utf-8")


class Ssh(object):
    def __init__(self, host, username, key, password=None, timeout=3, gatewayip=None,
                 gatewaykey=None, gatewayuser=None, ):
        """Perform various commands via SSH (shell commands, copy, ...).

        :param host:
        :param username:
        :param key:
        :param password:
        :param timeout:
        :param gatewayip:
        :param gatewaykey:
        :param gatewayuser:
        """
        self.__host = host
        self.__username = username
        self.__key = key
        # TODO: Allow password-login?
        self.__password = password
        self.__timeout = timeout

        self.__gatewayIp = gatewayip
        self.__gatewayKey = gatewaykey
        self.__gatewayUser = gatewayuser

    @property
    def host(self):
        """Hostname - read access necessary for unit test(s)."""
        return self.__host

    def canConnect(self, quiet=True):
        return self.handleSshCall("uname -a", quiet)[0] == 0

    def copyToRemote(self, localFileName, remoteFileName=""):
        p = subprocess.Popen(["scp",
                              "-o ConnectTimeout=" + str(self.__timeout),
                              "-o UserKnownHostsFile=/dev/null",
                              "-o StrictHostKeyChecking=no",
                              "-o PasswordAuthentication=no",
                              "-i", self.__key,
                              localFileName,
                              "%s@%s:%s" % (self.__username, self.__host, remoteFileName)],
                             bufsize=0, executable=None, stdin=None, stdout=subprocess.PIPE)
        p.wait()
        res = p.stdout.read()
        return p.returncode, res

    def handleSshCall(self, call, quiet=False):
        # type: (Union[str, unicode], bool) -> Tuple[int, str, str]
        """Perform SSH command on remote server.

        This function will redirect the call into a local shell, if user, hostname & gateway allow this.

        :param call:
        :param quiet:
        :return res: SSH call result. Consists of return-code, output, error
        :rtype res: Tuple(int, str, str)
        """
        if (self.__gatewayIp is None and
                    self.__host in ["localhost", "127.0.0.1", "::1", "", " ", None] and
                    self.__username == getpass.getuser()):
            logging.debug("Redirecting SSH call to local shell.")
            # Perform "quiet", since this method will already generate output.
            res = Shell.executeCommand(command=call, quiet=True)
            if res[0] != 0:
                res = self._executeRemoteCommand(call)
        else:
            if self.__gatewayIp is not None:
                # wrap SSH command in another SSH call
                call = "ssh -i %s %s@%s '%s'" % (self.__gatewayKey, self.__gatewayUser, self.__gatewayIp, call)
            # "regular" SSH call
            res = self._executeRemoteCommand(call)

        if not quiet:
            if res[0] == 255:
                logging.error("SSH connection could not be established!")
                logging.error("command: %s on %s" % (call, self.__host))
            elif not res[0] == 0:
                logging.error("SSH command on host %s failed! Return code: %i" % (self.__host, res[0]))
                logging.error("command: %s" % call)
                logging.error("stdout: %s" % res[1])
                logging.error("stderr: %s" % res[2])
            else:
                logging.info("SSH command successful! Return code: %i" % res[0])
                logging.info("command: %s on %s" % (call, self.__host))
                logging.info("stdout: %s" % res[1])
                if res[2]:
                    logging.info("stderr: %s" % res[2])

        return res

    @staticmethod
    def getSshOnMachine(machine):
        ip = machine.get(MachineRegistry.MachineRegistry.regHostname)
        key = machine.get(MachineRegistry.MachineRegistry.regSshKey) + ".private"

        if machine.get(MachineRegistry.MachineRegistry.regUsesGateway, False) is True:
            ssh = Ssh(ip, "root", key, None, 1,
                      machine.get(MachineRegistry.MachineRegistry.regGatewayIp),
                      machine.get(MachineRegistry.MachineRegistry.regGatewayKey),
                      machine.get(MachineRegistry.MachineRegistry.regGatewayUser))
        else:
            ssh = Ssh(ip, "root", key, None, 1)
        return ssh

    # protected
    def _executeRemoteCommand(self, command):
        # type (str) -> Tuple(int, str, str)
        """Perform SSH command on remote server. Don't call directly. Use handleSshCall.

        :param command:
        :returns:
        :rtype returncode: int
        :rtype stdout: str
        :rtype stderr: str
        """
        p = subprocess.Popen(["ssh",
                              "-o ConnectTimeout=" + str(self.__timeout),
                              "-o UserKnownHostsFile=/dev/null",
                              "-o StrictHostKeyChecking=no",
                              "-o PasswordAuthentication=no",
                              "-o LogLevel=quiet",
                              "-i", self.__key,
                              self.__username + "@" + self.__host,
                              command],
                             bufsize=0, executable=None, stdin=None, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        return (p.returncode,
                stdout.decode(encoding="utf-8"),
                stderr.decode(encoding="utf-8"))

    @staticmethod
    def debugOutput(logger, scope, result):
        logger.debug("[%s] SSH return code: %i" % (scope, result[0]))
        logger.debug("[%s] SSH stdout: %s" % (scope, result[1].strip()))
        if result[2]:
            logger.debug("[%s] SSH stderr: %s" % (scope, result[2].strip()))


class Vpn(object):
    """
          --> new var in machreg: cert_is_valid = None (by default)
                                                        True (on first generation)
                                                        False (after revocation)
          - new var in machreg: vpn_ip
    """

    # TODO: - Check if there is already a valid certificate with the same name

    def __init__(self):
        pass

    def makeCertificate(self, cert_name):
        """Generates a new certificate on OpenVPN host machine with pkitool.

        pkitool is a wrapper script for OpenSSL and is included in OpenVPN easy-rsa.
        """

        logging.info("creating certificate...")

        """
        cmd = "cd /etc/openvpn/easy-rsa/2.0/ && \
                source ./vars && \
                ./pkitool --pkcs12 --batch %s" %(cert_name)
        """

        cmd = "/etc/openvpn/vpn.sh -new_cert %s" % cert_name

        ssh = Ssh("ekpvpn", "root", "<user SSH key>", None, 1)
        (res1, count1, err1) = ssh.handleSshCall(cmd)

        if res1 == 0:
            logging.info("certificate \"%s\" successfully created!" % cert_name)
        else:
            logging.error("creation of certificate \"%s\" failed!" % cert_name)

        return res1

    def copyCertificate(self, cert_name, machine):
        """copies certificate to machine on public ip.

        For this method, passwordless login on vpn client must be enabled (SSH key).
        Otherwise scp will throw an error!
        --> useful command: ssh-copy-id -i ~/.ssh/id_rsa.pub user@server
        """

        logging.info("copying certificate to remote machine...")

        ip = machine.get(MachineRegistry.MachineRegistry.regHostname)

        # ip = "141.52.208.189"

        # cmd = "cd /etc/openvpn/easy-rsa/2.0/keys && \
        #         scp -o ConnectTimeout=3 \
        #     -o UserKnownHostsFile=/dev/null \
        #     -o StrictHostKeyChecking=no \
        #     -o PasswordAuthentication=no \
        #     -i ~/.ssh/id_rsa %s.p12 root@%s:/etc/openvpn/certs/." % (cert_name, ip)

        cmd = "/etc/openvpn/vpn.sh -copy_cert %s %s" % (cert_name, ip)

        ssh = Ssh("ekpvpn", "root", "<user SSH key>", None, 1)
        (res1, count1, err1) = ssh.handleSshCall(cmd)

        if res1 == 0:
            logging.info("\"%s.p12\" successfully copied!" % cert_name)
        else:
            logging.error("failed to copy \"%s.p12\"!" % cert_name)

        return res1

    def revokeCertificate(self, cert_name):
        """Revokes certificate with cert_name.

        After revocation the machine stays connected.
        Once the connection is closed, it can't connect again with the same cert.
        """

        logging.info("revoking certificate...")

        # cmd = "cd /etc/openvpn/easy-rsa/2.0 && \
        #         source ./vars && \
        #         ./revoke-full %s" % (cert_name)

        cmd = "/etc/openvpn/vpn.sh -revoke_cert %s" % cert_name

        ssh = Ssh("ekpvpn", "root", "<user SSH key>", None, 1)
        (res1, count1, err1) = ssh.handleSshCall(cmd)

        if res1 == 0:
            logging.info("certificate \"%s\" successfully revoked!" % cert_name)
        else:
            logging.error("revocation of certificate \"%s\" failed!" % cert_name)

        return res1

    def deleteCertificate(self, cert_name):
        """delete certificate after revocation"""

        logging.info("deleting certificate...")

        # cmd = "cd /etc/openvpn/easy-rsa/2.0/keys && \
        #         rm -rf %s.*" % (cert_name)

        cmd = "/etc/openvpn/vpn.sh -delete_cert %s" % cert_name

        ssh = Ssh("ekpvpn", "root", "<user SSH key>", None, 1)
        (res1, count1, err1) = ssh.handleSshCall(cmd)

        if res1 == 0:
            logging.info("certificate \"%s\" successfully deleted!" % cert_name)
        else:
            logging.error("deleting certificate \"%s\" failed!" % cert_name)

        return res1

    def connectVPN(self, cert_name, machine):
        """Connects machine to the openvpn server.

        All configurations for the connection are done via a config file (different for server
        & client).
        """

        logging.info("connecting to vpn server...")

        # cmd = "openvpn --daemon openvpn \
        #                 --config /etc/openvpn/c2n.client.conf \
        #                 --pkcs12 /etc/openvpn/certs/%s.p12" % (cert_name)

        cmd = "killall openvpn; sleep 5; /etc/openvpn/vpn.sh -connect %s; sleep 5" % cert_name

        ssh = Ssh.getSshOnMachine(machine)
        (res1, count1, err1) = ssh.handleSshCall(cmd)

        if res1 == 0:
            logging.info("client successfully connected to vpn server!")
        else:
            logging.error("connecting to vpn server failed!")

        return res1

    def disconnectVPN(self, machine):
        """Disconnects the vpn connection by killing the OpenVPN process.
        By killing the connection, the OpenVPN server receives a message, that the client
        no longer exists. This avoids error messages.
        """

        logging.info("disconnecting from vpn server...")

        # cmd = "killall openvpn"

        cmd = "/etc/openvpn/vpn.sh -disconnect"

        ssh = Ssh.getSshOnMachine(machine)
        (res1, count1, err1) = ssh.handleSshCall(cmd)

        if res1 == 0:
            logging.info("client successfully disconnected from vpn server!")
        else:
            logging.error("disconnecting from vpn server failed!")

        return res1

    def getIP(self, machine):
        """returns vpn ip of the machine"""

        # filter ifconfig output
        cmd = "ifconfig tap0 2> /dev/null | sed -rn \'s/.*r:([^ ]+) .*/\\1/p\'"

        ssh = Ssh.getSshOnMachine(machine)

        (res1, ip, err1) = ssh.handleSshCall(cmd)

        return res1, ip.rstrip()

    def disableSsh(self):
        """disable SSH connections for public ips"""
        pass
