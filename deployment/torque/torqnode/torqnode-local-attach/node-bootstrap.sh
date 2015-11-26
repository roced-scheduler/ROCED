#!/bin/sh
# Parameter supplied:
# $1 torq server ip ( "10.8.0.1" )
# $2 torq server name ( "ekplx20" )
# $3 hostname ( "cloudxy" ) 
# $3 site type ( "ec2" or "euca" so far ) 


# vpn connect

echo "Setting system time via ntp ..."
ntpdate -u ntp.ubuntu.com

echo "Disabling firewall ..."
/etc/init.d/iptables stop

if [ "$4" = 'ec2' ]
  then
  	echo "Installing ec2 kernel modules for OpenVPN ..."
  	wget -O kmod.tar.gz http://s3.amazonaws.com/ec2-downloads/ec2-modules-2.6.16.33-xenU-x86_64.tgz
  	tar xvzf kmod.tar.gz -C /
  	depmod -a
  	modprobe tun
    echo "done"
fi

if [ "$4" = 'euca' ]
  then
  	echo "Installing euca kernel modules for OpenVPN ..."
  	tar xvzf /root/bstrap/kernel-2.6.28-11-generic-modules.tar.gz -C /root/bstrap/
  	cp -R /root/bstrap/kernel-2.6.28-11-generic-modules/2.6.28-11-generic /lib/modules/ 
  	depmod -a
  	modprobe tun
    echo "done"
fi

echo "Installing user <user> ssh keys ..."

rm -f /home/<user>/.ssh/id_rsa
rm -f /home/<user>/.ssh/id_rsa.pub


cp -f /root/bstrap/<user>-node-key.pub /home/<user>/.ssh/id_rsa.pub
cp -f /root/bstrap/<user>-node-key /home/<user>/.ssh/id_rsa

chmod 0600 /home/<user>/.ssh/id_rsa
chown <user>:<user> /home/<user>/.ssh/id_rsa.pub
chown <user>:<user> /home/<user>/.ssh/id_rsa

cat /root/bstrap/<user>-pbs-key.pub >> /home/<user>/.ssh/authorized_keys

echo "Connecting OpenVPN ..."
rm -f /root/ca.crt
rm -f /root/cloud-node.crt
rm -f /root/cloud-node.key
rm -f /root/vpn-to-ekp.conf

cp -f /root/bstrap/ca.crt /root/ca.crt
cp -f /root/bstrap/cloud-node.crt /root/cloud-node.crt
cp -f /root/bstrap/cloud-node.key /root/cloud-node.key
cp -f /root/bstrap/vpn-to-ekp.conf /root/vpn-to-ekp.conf

openvpn --daemon --config /root/vpn-to-ekp.conf 

# TODO: wait for if to come up...
sleep 5s

# nfs mount
echo "Mounting nfs /storage ..."
mount <nfs ip>:/var/cloudstorage /storage

echo "Setting hostname to $3"

hostname $3

echo "127.0.0.1 localhost.localdomain localhost $3" > /etc/hosts
echo "::1 localhost6.localdomain6 localhost6" >> /etc/hosts
echo "$1 $2" >> /etc/hosts

echo "$2" > /var/spool/torque/server_name

/etc/init.d/pbs_mom restart
