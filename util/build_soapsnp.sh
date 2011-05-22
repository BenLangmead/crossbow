#!/bin/sh

##
# build_crossbow_jar
#
#  Author: Ben Langmead
#    Date: June 1, 2009
#
# Build the bowtie/soapsnp binaries for Linux (and possibly Mac) and
# debosit them in the bin directory.
#
# FIXME:
#  1. Directories are hardcoded
#  2. Assumes local machine is mac and remote is linux
#

usage() {
cat > /dev/stdout <<EOF
Usage: build_crossbow_jar [-m] [-u <URL>]

	-b........do bowtie too
	-m        compile Mac versions of binaries first
	-u <URL>  compile sources from given URL instead of SVN
	-h        show usage message

EOF
}

usagedie() {
	usage ; exit 1
}

DO_BOWTIE=0

while getopts u:mh OPT; do
	case "$OPT" in
	b)	DO_BOWTIE=1
		;;
	h)	usage ; exit 0
		;;
	\?)	# getopts issues an error message
		usagedie
		;;
	esac
done

linux_host=privet.umiacs.umd.edu
user=langmead

# Ensure we're in the Crossbow checkout dir
if [ ! -f util/build_soapsnp.sh ] ; then
	echo Must run in crossbow checkout directory
	exit 1
fi

# Bulldoze old .bin directory
rm -rf .bin
mkdir -p .bin

# Bulldoze old .build directory
rm -rf .build
mkdir -p .build

cd .build

# SOAPsnp source always comes from svn
svn co https://bowtie-bio.svn.sourceforge.net/svnroot/bowtie-bio/crossbow
mv crossbow/soapsnp soapsnp
rm -rf crossbow

if [ $DO_BOWTIE -ne 0 ] ; then
	# Bowtie source can come from CVS or from a URL
	if [ -z "$SRC_URL" ] ; then
		export CVS_RSH=ssh
		cvs -d :ext:${user}@${linux_host}:/fs/szdevel/src/cvsroot co bowtie
	else
		wget --no-check-certificate $SRC_URL
		unzip *.zip
		rm -f *.zip
		mv bowtie* bowtie
	fi
	if ! make -C bowtie BITS=32 bowtie bowtie-debug ; then
		echo "Error bulding bowtie 32"
		exit 1
	fi
fi

if ! make -C soapsnp BITS=32 soapsnp soapsnp-debug ; then
	echo "Error bulding soapsnp 32"
	exit 1
fi

mkdir -p ../.bin/mac32
if [ $DO_BOWTIE -ne 0 ] ; then
	cp bowtie/bowtie ../.bin/mac32
	cp bowtie/bowtie-debug ../.bin/mac32
fi
cp soapsnp/soapsnp ../.bin/mac32
cp soapsnp/soapsnp-debug ../.bin/mac32

if [ $DO_BOWTIE -ne 0 ] ; then
	make -C bowtie clean
fi
rm -f soapsnp/soapsnp soapsnp/soapsnp-debug

if [ $DO_BOWTIE -ne 0 ] ; then
	if ! make -C bowtie BITS=64 bowtie bowtie-debug ; then
		echo "Error bulding bowtie 64"
		exit 1
	fi
fi

if ! make -C soapsnp BITS=64 soapsnp soapsnp-debug ; then
	echo "Error bulding soapsnp 64"
	exit 1
fi

mkdir -p ../.bin/mac64
if [ $DO_BOWTIE -ne 0 ] ; then
	cp bowtie/bowtie ../.bin/mac64
	cp bowtie/bowtie-debug ../.bin/mac64
fi
cp soapsnp/soapsnp ../.bin/mac64
cp soapsnp/soapsnp-debug ../.bin/mac64

cd ..

# Prepare
ssh ${user}@${linux_host} \
	"rm -rf /tmp/.build_crossbow_tmp && " \
	"mkdir -p /tmp/.build_crossbow_tmp"

if [ $DO_BOWTIE -ne 0 ] ; then
	# Get Bowtie source
	if [ -z "$SRC_URL" ] ; then
		ssh ${user}@${linux_host} \
			"cd /tmp/.build_crossbow_tmp && " \
			"cvs -d /fs/szdevel/src/cvsroot co bowtie"
	else
		ssh ${user}@${linux_host} \
			"cd /tmp/.build_crossbow_tmp && " \
			"wget --no-check-certificate $SRC_URL && " \
			"unzip *.zip && " \
			"rm -f *.zip && " \
			"mv bowtie* bowtie"
	fi
	# Build Bowtie source; Get and build SOAPsnp source
	ssh ${user}@${linux_host} \
		"cd /tmp/.build_crossbow_tmp/bowtie && " \
		"make -j2 BITS=32 bowtie bowtie-debug"
fi

# Get and build SOAPsnp source
ssh ${user}@${linux_host} \
	"cd /tmp/.build_crossbow_tmp && " \
	"svn co https://bowtie-bio.svn.sourceforge.net/svnroot/bowtie-bio/crossbow && " \
	"cd crossbow/soapsnp && " \
	"make -j2 BITS=32 soapsnp soapsnp-debug"

mkdir -p .bin/linux32
if [ $DO_BOWTIE -ne 0 ] ; then
	scp ${user}@${linux_host}:/tmp/.build_crossbow_tmp/bowtie/bowtie \
	    ${user}@${linux_host}:/tmp/.build_crossbow_tmp/bowtie/bowtie-debug .bin/linux32
fi
scp ${user}@${linux_host}:/tmp/.build_crossbow_tmp/crossbow/soapsnp/soapsnp* .bin/linux32

if [ $DO_BOWTIE -ne 0 ] ; then
	ssh ${user}@${linux_host} \
		"cd /tmp/.build_crossbow_tmp/bowtie && " \
		"rm -f bowtie bowtie-debug && " \
		"make -j2 BITS=64 bowtie bowtie-debug"
fi

ssh ${user}@${linux_host} \
	"cd /tmp/.build_crossbow_tmp/crossbow/soapsnp && " \
	"rm -f soapsnp soapsnp-debug && " \
	"make -j2 BITS=64 soapsnp soapsnp-debug"

mkdir -p .bin/linux64
if [ $DO_BOWTIE -ne 0 ] ; then
	scp ${user}@${linux_host}:/tmp/.build_crossbow_tmp/bowtie/bowtie \
	    ${user}@${linux_host}:/tmp/.build_crossbow_tmp/bowtie/bowtie-debug .bin/linux64
fi
scp ${user}@${linux_host}:/tmp/.build_crossbow_tmp/crossbow/soapsnp/soapsnp* .bin/linux64

ssh ${user}@${linux_host} "rm -rf /tmp/.build_crossbow_tmp"
echo "PASSED"
echo "Binaries in .bin subdirectory"
