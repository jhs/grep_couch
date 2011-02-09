# Builds the grep_couch tool

erls = $(wildcard src/*.erl)
beams = $(patsubst src/%.erl,ebin/%.beam,$(erls))

grep_couch: ebin.zip
	echo '#!/usr/bin/env escript' > $@
	echo '%%! -smp enable -escript main grep_couch' >> $@
	cat ebin.zip >> $@
	rm ebin.zip
	chmod +x $@

ebin:
	mkdir -p ebin

$(beams): ebin/%.beam: src/%.erl
	erlc -o ebin $<

ebin.zip: ebin $(beams)
	cd ebin && zip -9 ../ebin.zip *.beam

clean_ebin:
	rm -rf ebin
	rm -f ebin.zip

clean: clean_ebin
	rm -f grep_couch

# vim: sw=8 sts=8 noet
