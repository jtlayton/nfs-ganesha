srpm:
	dnf install -y git cmake make rpm-build krb5-devel flex bison
	git submodule update --init --recursive
	mkdir build
	cd build ; cmake ../src -DRPCBIND=OFF
	$(MAKE) -C build srpm
	mv build/*.src.rpm $(outdir)
