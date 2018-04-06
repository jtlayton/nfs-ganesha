# start with minimal fedora base
FROM fedora:latest

RUN touch /var/lib/rpm/*

# get the copr plugin
RUN dnf install -y dnf-plugins-core --refresh

# This is all very experimental work for now, but eventually it (or
# something like it should be able to make its way to mainline. For now, this
# is being built from copr repos:
#
# https://copr.fedorainfracloud.org/coprs/jlayton/

# install jlayton's copr repos
RUN dnf copr enable -y jlayton/ceph
RUN dnf copr enable -y jlayton/nfs-ganesha

# I tried just calling dnf install -y nfs-ganesha-ceph, but it was failing
# with some db5 error inside RPM. This is a workaround for now. Eventually
# we should be able to remove this dnf call (one would hope).
# RUN dnf install -y kmod perl-Carp policycoreutils

# now install nfs-ganesha and Ceph FSAL
RUN dnf install -y nfs-ganesha-ceph

# delete package cache
RUN dnf clean all

# set up ceph.conf
WORKDIR /etc/ceph/
COPY ceph.conf /etc/ceph/

# set up ganesha config
WORKDIR /etc/ganesha/
COPY ganesha.conf /etc/ganesha/

# make a ceph dir, primarily for collecting ceph logfiles
WORKDIR /ceph
RUN chown ganesha:ganesha /ceph

# copy run script
WORKDIR /
COPY run-ganesha.sh /

# export NFS port
EXPOSE 2049

ENTRYPOINT ["/run-ganesha.sh"]
