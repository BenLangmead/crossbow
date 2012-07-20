all: doc bin package

CROSSBOW_VERSION=$(shell cat VERSION)
SF_BOWTIE_BASE=https://sourceforge.net/projects/bowtie-bio/files/bowtie
SF_BOWTIE_MID=
BOWTIE_VERSION=0.12.8
MACOS_VERSION=10.6

doc: doc/manual.html MANUAL
.PHONY: doc

doc/manual.html: MANUAL.markdown
	echo "<h1>Table of Contents</h1>" > .tmp.head
	pandoc -T "Crossbow $(CROSSBOW_VERSION) Manual" -B .tmp.head \
	       --css style.css -o $@ \
	       --from markdown --to HTML \
	       --table-of-contents $^

MANUAL: MANUAL.markdown
	perl doc/strip_markdown.pl < $^ > $@

.PHONY: bin32
bin32: bin/linux32/bowtie \
       bin/linux32/bowtie-build \
       bin/linux32/bowtie-debug \
       bin/linux32/bowtie-build-debug \
       bin/linux32/soapsnp \
       bin/linux32/soapsnp-debug \
       bin/mac32/bowtie \
       bin/mac32/bowtie-build \
       bin/mac32/bowtie-debug \
       bin/mac32/bowtie-build-debug \
       bin/mac32/soapsnp \
       bin/mac32/soapsnp-debug

.PHONY: bin64
bin64: bin/linux64/bowtie \
       bin/linux64/bowtie-build \
       bin/linux64/bowtie-debug \
       bin/linux64/bowtie-build-debug \
       bin/linux64/soapsnp \
       bin/linux64/soapsnp-debug \
       bin/mac64/bowtie \
       bin/mac64/bowtie-build \
       bin/mac64/bowtie-debug \
       bin/mac64/bowtie-build-debug \
       bin/mac64/soapsnp \
       bin/mac64/soapsnp-debug

.PHONY: bin
bin: bin64

#bin/linux32/bowtie: bowtie-$(BOWTIE_VERSION)-linux-i386.zip
#	mkdir -p bin/linux32
#	unzip $^ bowtie-$(BOWTIE_VERSION)/bowtie
#	mv bowtie-$(BOWTIE_VERSION)/bowtie $@
#	rm -rf bowtie-$(BOWTIE_VERSION)

#bin/linux32/bowtie-build: bowtie-$(BOWTIE_VERSION)-linux-i386.zip
#	mkdir -p bin/linux32
#	unzip $^ bowtie-$(BOWTIE_VERSION)/bowtie-build
#	mv bowtie-$(BOWTIE_VERSION)/bowtie-build $@
#	rm -rf bowtie-$(BOWTIE_VERSION)

#bin/linux32/bowtie-debug: bowtie-$(BOWTIE_VERSION)-linux-i386.zip
#	mkdir -p bin/linux32
#	unzip $^ bowtie-$(BOWTIE_VERSION)/bowtie-debug
#	mv bowtie-$(BOWTIE_VERSION)/bowtie-debug $@
#	rm -rf bowtie-$(BOWTIE_VERSION)

#bin/linux32/bowtie-build-debug: bowtie-$(BOWTIE_VERSION)-linux-i386.zip
#	mkdir -p bin/linux32
#	unzip $^ bowtie-$(BOWTIE_VERSION)/bowtie-build-debug
#	mv bowtie-$(BOWTIE_VERSION)/bowtie-build-debug $@
#	rm -rf bowtie-$(BOWTIE_VERSION)


bin/linux64/bowtie: bowtie-$(BOWTIE_VERSION)-linux-x86_64.zip
	mkdir -p bin/linux64
	unzip $^ bowtie-$(BOWTIE_VERSION)/bowtie
	mv bowtie-$(BOWTIE_VERSION)/bowtie $@
	rm -rf bowtie-$(BOWTIE_VERSION)

bin/linux64/bowtie-build: bowtie-$(BOWTIE_VERSION)-linux-x86_64.zip
	mkdir -p bin/linux64
	unzip $^ bowtie-$(BOWTIE_VERSION)/bowtie-build
	mv bowtie-$(BOWTIE_VERSION)/bowtie-build $@
	rm -rf bowtie-$(BOWTIE_VERSION)

bin/linux64/bowtie-debug: bowtie-$(BOWTIE_VERSION)-linux-x86_64.zip
	mkdir -p bin/linux64
	unzip $^ bowtie-$(BOWTIE_VERSION)/bowtie-debug
	mv bowtie-$(BOWTIE_VERSION)/bowtie-debug $@
	rm -rf bowtie-$(BOWTIE_VERSION)

bin/linux64/bowtie-build-debug: bowtie-$(BOWTIE_VERSION)-linux-x86_64.zip
	mkdir -p bin/linux64
	unzip $^ bowtie-$(BOWTIE_VERSION)/bowtie-build-debug
	mv bowtie-$(BOWTIE_VERSION)/bowtie-build-debug $@
	rm -rf bowtie-$(BOWTIE_VERSION)


#bin/mac32/bowtie: bowtie-$(BOWTIE_VERSION)-macos-$(MACOS_VERSION)-i386.zip
#	mkdir -p bin/mac32
#	unzip $^ bowtie-$(BOWTIE_VERSION)/bowtie
#	mv bowtie-$(BOWTIE_VERSION)/bowtie $@
#	rm -rf bowtie-$(BOWTIE_VERSION)

#bin/mac32/bowtie-build: bowtie-$(BOWTIE_VERSION)-macos-$(MACOS_VERSION)-i386.zip
#	mkdir -p bin/mac32
#	unzip $^ bowtie-$(BOWTIE_VERSION)/bowtie-build
#	mv bowtie-$(BOWTIE_VERSION)/bowtie-build $@
#	rm -rf bowtie-$(BOWTIE_VERSION)

#bin/mac32/bowtie-debug: bowtie-$(BOWTIE_VERSION)-macos-$(MACOS_VERSION)-i386.zip
#	mkdir -p bin/mac32
#	unzip $^ bowtie-$(BOWTIE_VERSION)/bowtie-debug
#	mv bowtie-$(BOWTIE_VERSION)/bowtie-debug $@
#	rm -rf bowtie-$(BOWTIE_VERSION)

#bin/mac32/bowtie-build-debug: bowtie-$(BOWTIE_VERSION)-macos-$(MACOS_VERSION)-i386.zip
#	mkdir -p bin/mac32
#	unzip $^ bowtie-$(BOWTIE_VERSION)/bowtie-build-debug
#	mv bowtie-$(BOWTIE_VERSION)/bowtie-build-debug $@
#	rm -rf bowtie-$(BOWTIE_VERSION)


bin/mac64/bowtie: bowtie-$(BOWTIE_VERSION)-macos-$(MACOS_VERSION)-x86_64.zip
	mkdir -p bin/mac64
	unzip $^ bowtie-$(BOWTIE_VERSION)/bowtie
	mv bowtie-$(BOWTIE_VERSION)/bowtie $@
	rm -rf bowtie-$(BOWTIE_VERSION)

bin/mac64/bowtie-build: bowtie-$(BOWTIE_VERSION)-macos-$(MACOS_VERSION)-x86_64.zip
	mkdir -p bin/mac64
	unzip $^ bowtie-$(BOWTIE_VERSION)/bowtie-build
	mv bowtie-$(BOWTIE_VERSION)/bowtie-build $@
	rm -rf bowtie-$(BOWTIE_VERSION)

bin/mac64/bowtie-debug: bowtie-$(BOWTIE_VERSION)-macos-$(MACOS_VERSION)-x86_64.zip
	mkdir -p bin/mac64
	unzip $^ bowtie-$(BOWTIE_VERSION)/bowtie-debug
	mv bowtie-$(BOWTIE_VERSION)/bowtie-debug $@
	rm -rf bowtie-$(BOWTIE_VERSION)

bin/mac64/bowtie-build-debug: bowtie-$(BOWTIE_VERSION)-macos-$(MACOS_VERSION)-x86_64.zip
	mkdir -p bin/mac64
	unzip $^ bowtie-$(BOWTIE_VERSION)/bowtie-build-debug
	mv bowtie-$(BOWTIE_VERSION)/bowtie-build-debug $@
	rm -rf bowtie-$(BOWTIE_VERSION)


bowtie-$(BOWTIE_VERSION)-macos-$(MACOS_VERSION)-i386.zip:
	wget --no-check-certificate $(SF_BOWTIE_BASE)$(SF_BOWTIE_MID)/$(BOWTIE_VERSION)/bowtie-$(BOWTIE_VERSION)-macos-$(MACOS_VERSION)-i386.zip/download

bowtie-$(BOWTIE_VERSION)-macos-$(MACOS_VERSION)-x86_64.zip:
	wget --no-check-certificate $(SF_BOWTIE_BASE)$(SF_BOWTIE_MID)/$(BOWTIE_VERSION)/bowtie-$(BOWTIE_VERSION)-macos-$(MACOS_VERSION)-x86_64.zip/download

bowtie-$(BOWTIE_VERSION)-linux-i386.zip:
	wget --no-check-certificate $(SF_BOWTIE_BASE)$(SF_BOWTIE_MID)/$(BOWTIE_VERSION)/bowtie-$(BOWTIE_VERSION)-linux-i386.zip/download

bowtie-$(BOWTIE_VERSION)-linux-x86_64.zip:
	wget --no-check-certificate $(SF_BOWTIE_BASE)$(SF_BOWTIE_MID)/$(BOWTIE_VERSION)/bowtie-$(BOWTIE_VERSION)-linux-x86_64.zip/download

.PHONY: package
package: bin
	bash util/package.bash
