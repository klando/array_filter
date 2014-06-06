MODULES = array_filter
DATA_built = array_filter.sql uninstall_array_filter.sql
DOCS = README.array_filter

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/array_filter
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

deb: all
	mkdir -p ./debian/usr/share/doc/postgresql-arrayfilter-9.0/
	cp README.array_filter ./debian/usr/share/doc/postgresql-arrayfilter-9.0/
	mkdir -p ./debian/usr/share/postgresql/9.0/contrib/
	cp array_filter.sql ./debian/usr/share/postgresql/9.0/contrib/
	cp uninstall_array_filter.sql ./debian/usr/share/postgresql/9.0/contrib/
	mkdir -p ./debian/usr/lib/postgresql/9.0/lib/
	cp array_filter.so ./debian/usr/lib/postgresql/9.0/lib/
	dpkg-deb --build debian
	mv debian.deb ../postgresql-9.0-array-filter-1.0.0.deb
	rm -rf ./debian/usr
