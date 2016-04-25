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


import logging
import subprocess

from Core import MachineRegistry


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
        # type (MachineRegistry.StatusChangedEvent)
        if isinstance(evt, MachineRegistry.StatusChangedEvent):
            s = "Machine type %s on site %s changed status from %s to %s" % \
                (self.mr.machines[evt.id].get(self.mr.reg_machine_type),
                 self.mr.machines[evt.id].get(self.mr.reg_site), evt.oldStatus, evt.newStatus)

            self.cachedNotify("scale Status changed", s)

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
    def executeCommand(command, environment=None):
        logging.info("running shell command: " + str(command) + " on localhost")

        #        if splitCmd == True:
        #            command = shlex.split( command )

        p = subprocess.Popen(command,
                             bufsize=0, executable=None,
                             shell=True,
                             stdin=None,
                             stdout=subprocess.PIPE,
                             env=environment)
        res = p.communicate()

        if not p.returncode == 0:
            logging.error("Shell command failed: " + str(p.returncode))
            logging.error("command: " + str(command) + " on localhost")
            logging.error("output: " + str(res) + " on localhost")
        else:
            logging.info("Shell command successful: " + str(res))
            logging.info("command: " + str(command) + " on localhost")
            logging.info("Shell Done: " + str(p.returncode))

        return p.returncode, res[0]


class Ssh(object):
    def __init__(self, host, username, key, password=None, timeout=3, gatewayip=None,
                 gatewaykey=None, gatewayuser=None, ):
        self.host = host
        self.username = username
        self.key = key
        self.password = password
        self.timeout = timeout

        self.gatewayIp = gatewayip
        self.gatewayKey = gatewaykey
        self.gatewayUser = gatewayuser

    def canConnect(self, quiet=True):
        return self.handleSshCall("uname -a", quiet)[0] == 0

    def copyToRemote(self, localFileName, remoteFileName=""):
        p = subprocess.Popen(["scp",
                              "-o ConnectTimeout=" + str(self.timeout),
                              "-o UserKnownHostsFile=/dev/null",
                              "-o StrictHostKeyChecking=no",
                              "-o PasswordAuthentication=no",
                              "-i", self.key,
                              localFileName,
                              "%s@%s:%s" % (self.username, self.host, remoteFileName)],
                             bufsize=0, executable=None, stdin=None, stdout=subprocess.PIPE)
        p.wait()
        res = p.stdout.read()
        return p.returncode, res

    def handleSshCall(self, call, quiet=False):
        # type (str, bool) -> Tuple(int, str, str)
        """Perform SSH command on remote server.

        :param call:
        :param quiet:
        :return res: SSH call result. Consists of return-code, output, error
        :rtype res: Tuple(int, str, str)
        """
        if self.gatewayIp is not None:
            # wrap ssh command in another ssh call
            call = "ssh -i %s %s@%s '%s'" % (self.gatewayKey, self.gatewayUser, self.gatewayIp,
                                             call)

        res = self._executeRemoteCommand(call)

        if not quiet:
            if res[0] == 255:
                logging.error("SSH connection could not be established!")
                logging.error("command: " + call + " on " + self.host)
            elif not res[0] == 0:
                logging.error("SSH command on remote host failed! Return code: " + str(res[0]))
                logging.error("command: " + call + " on " + self.host)
                logging.error("stdout: " + str(res[1]))
                logging.error("stderr: " + str(res[2]))
            else:
                logging.info("SSH command successful! Return code: " + str(res[0]))
                logging.info("command: " + call + " on " + self.host)
                logging.info("stdout: " + str(res[1]))
                if res[2]:
                    logging.info("stderr: " + str(res[2]))

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
                              "-o ConnectTimeout=" + str(self.timeout),
                              "-o UserKnownHostsFile=/dev/null",
                              "-o StrictHostKeyChecking=no",
                              "-o PasswordAuthentication=no",
                              "-o LogLevel=quiet",
                              "-i", self.key,
                              self.username + "@" + self.host,
                              command],
                             bufsize=0, executable=None, stdin=None, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        return (p.returncode,
                stdout.decode(encoding="utf-8"),
                stderr.decode(encoding="utf-8"))

    @staticmethod
    def debugOutput(logger, scope, result):
        logger.debug("[" + scope + "] SSH return code: " + str(result[0]))
        logger.debug("[" + scope + "] SSH stdout: " + str(result[1].strip()))
        if result[2]:
            logger.debug("[" + scope + "] SSH stderr: " + str(result[2].strip()))


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

        ssh = Ssh("ekpvpn", "root", "<user ssh key>", None, 1)
        (res1, count1, err1) = ssh.handleSshCall(cmd)

        if res1 == 0:
            logging.info("certificate \"%s\" successfully created!" % cert_name)
        else:
            logging.error("creation of certificate \"%s\" failed!" % cert_name)

        return res1

    def copyCertificate(self, cert_name, machine):
        """copies certificate to machine on public ip.

        For this method, passwordless login on vpn client must be enabled (ssh key).
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

        ssh = Ssh("ekpvpn", "root", "<user ssh key>", None, 1)
        (res1, count1, err1) = ssh.handleSshCall(cmd)

        if res1 == 0:
            logging.info("\"%s.p12\" successfully copied!" %cert_name)
        else:
            logging.error("failed to copy \"%s.p12\"!" %cert_name)

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

        ssh = Ssh("ekpvpn", "root", "<user ssh key>", None, 1)
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

        ssh = Ssh("ekpvpn", "root", "<user ssh key>", None, 1)
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
        """disable ssh connections for public ips"""
        pass
