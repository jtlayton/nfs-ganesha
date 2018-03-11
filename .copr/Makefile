srpm:
	dnf install -y git cmake make rpm-build krb5-devel flex bison librados-devel libcephfs-devel librgw-devel libnsl2-devel python-sphinx
	git submodule update --init --recursive
	mkdir build
	cd build ; cmake ../src -DRPCBIND=OFF -D_MSPAC_SUPPORT=OFF -DUSE_MAN_PAGE=ON
	$(MAKE) -C build srpm
	mv build/*.src.rpm $(outdir)
