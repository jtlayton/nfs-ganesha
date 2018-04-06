#!/bin/bash

if [ -z "${CEPH_MON_ADDR}" ]; then
	echo 'No mon addr specified!'
	exit 1
fi

sed -i "s/%mon_addr%/${CEPH_MON_ADDR}/" /etc/ceph/ceph.conf

rados_grace_tool start `hostname`
exec setpriv				\
	--reuid ganesha 		\
	--regid ganesha			\
	--clear-groups			\
	--inh-caps=-all			\
	--bounding-set=-all		\
	/usr/bin/ganesha.nfsd -L STDOUT -N NIV_DEBUG -p /tmp/ganesha.pid -F
