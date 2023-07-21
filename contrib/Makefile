all = pyas p4thpydb p4thpymisc

.PHONY: $(all)


all: $(all)


pyas: 
	(git clone https://github.com/positively4th/pyas.git || echo) \
	&& ([ -f "pyas/Makefile" ] && make -C pyas || echo)

p4thpydb: 
	(git clone https://github.com/positively4th/p4thpydb.git || echo) \
	&& ([ -f "p4thpydb/Makefile" ] && make -C p4thpydb || echo)

p4thpymisc: 
	(git clone https://github.com/positively4th/p4thpymisc.git || echo) \
	&& ([ -f "p4thpymisc/Makefile" ] && make -C p4thpymisc || echo)

clean: 
	rm -rf $(all)


