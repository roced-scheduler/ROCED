#!/bin/bash
# script to install Grid Engine from scratch

IP=`ifconfig eth0 | sed -rn 's/.*r:([^ ]+) .*/\1/p'`
ETC_HOST="${IP}	${HOSTNAME}"

echo ${ETC_HOST} >> /etc/hosts

scp ge62u5*.tar.gz /tmp/.

cd /tmp/
tar xvzf ge62u5_lx24-ia64.tar.gz
tar xvzf ge62u5_lx24-amd64.tar.gz
tar xvzf ge62u5_lx24-x86.tar.gz

rm -f *tar.gz

cd /tmp/ge6.2u5/
tar xvzf ge-6.2u5-bin-lx24-ia64.tar.gz
tar xvzf ge-6.2u5-common.tar.gz
tar xvzf ge-6.2u5-bin-lx24-amd64.tar.gz
tar xvzf ge-6.2u5-bin-lx24-x86.tar.gz

rm -f *tar.gz

mv /tmp/ge6.2u5 /opt/sge6.2u5

apt-get install binutils

export SGE_ROOT=/opt/sge6.2u5

cd ${SGE_ROOT}

./inst_sge -m
