GDOCK
-----
This repo contains a collection of scripts and config files for
generating nfs-ganesha docker images that are suitable for exporting
CephFS in a clustered configuration.

For now, this is all quite experimental and based on the StatefulSet
contoller which is not 100% what we need for a proper NFS cluster. Still,
it allows some experimentation with the basic concept of loosely aggregated
ganesha servers.
