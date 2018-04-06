#!/bin/bash

#
# Simple script to kick off a set of ganesha servers under docker
#

get_mon_addr ()
{
	local monaddr=''

	monaddr=`grep 'mon addr =' /etc/ceph/ceph.conf |
			head -n 1 | awk '{print $4}'`
	eval "$1=$monaddr"
}

MON_ADDR=''
case $1 in
	start)
		shift
		get_mon_addr MON_ADDR
		for i in $@; do
			docker run --hostname ganesha-$i \
				   --name ganesha-$i \
				   --network=nfs-net \
				   --ip=172.18.1.$i \
				   --env CEPH_MON_ADDR=$MON_ADDR \
				   --detach \
				   jlayton/nfs-ganesha:ceph-cluster
		done
		;;
	stop)
		shift
		for i in $@; do
			docker stop ganesha-$i
			docker rm ganesha-$i
		done
		;;
	*)
		echo "Usage $0 {start|stop}"
		exit 1
esac
