#!/bin/sh

##
# shared.sh
#
# Shared routines for getting fasta files & SNP info, composing them
# into the proper formats and indexes, and ultimately bundling them all
# into a reference jar.
#
# Needs appropriate helper scripts to exist in $CROSSBOW_HOME/reftools.
#
# Needs bowtie-build to be in the current dir, in the
# $CROSSBOW_BOWTIE_HOME directory, or in the $PATH.
#
# Needs 'mysql' to be in the $PATH.
#
# Needs a good deal of scratch space (~15GB) on the current partition
# so that the script has enough space to produce its output and make
# copies of certain large inputs, such as fasta files.
#

##
# Get a file with either wget or curl (whichever is available, wget
# being preferable)
#
get() {
	file=$1
	if ! wget --version >/dev/null 2>/dev/null ; then
		if ! curl --version >/dev/null 2>/dev/null ; then
			echo "Please install wget or curl somewhere in your PATH"
			exit 1
		fi
		curl -o `basename $1` $1
		return $?
	else
		wget -O `basename $1` $1
		return $?
	fi
}

##
# Check that ensembl_snps.pl script is there and that 'mysql' is in the
# path.
#
check_prereqs() {
	SCRIPT_DIR=$CROSSBOW_HOME/reftools
	[ -n "$1" ] && SCRIPT_DIR=$1 
	[ ! -f "$SCRIPT_DIR/ensembl_snps.pl" ] && echo "Can't find '$SCRIPT_DIR/ensembl_snps.pl'" && exit 1
	[ ! -f "$SCRIPT_DIR/fasta_cmap.pl" ] && echo "Can't find '$SCRIPT_DIR/fasta_cmap.pl'" && exit 1
	! which mysql >/dev/null 2>/dev/null && echo "Can't find 'mysql' in path" && exit 1
}

##
# Find a runnable bowtie-build binary.
#
find_bowtie_build() {
	# Try current dir
	BOWTIE_BUILD_EXE=./bowtie-build
	if ! $BOWTIE_BUILD_EXE --version >/dev/null 2>/dev/null ; then
		# Try $CROSSBOW_BOWTIE_HOME
		BOWTIE_BUILD_EXE="$CROSSBOW_BOWTIE_HOME/bowtie-build"
		if ! $BOWTIE_BUILD_EXE --version >/dev/null 2>/dev/null ; then
			# Try $PATH
			BOWTIE_BUILD_EXE=`which bowtie-build`
			if ! $BOWTIE_BUILD_EXE --version >/dev/null 2>/dev/null ; then
				echo "Error: Could not find runnable bowtie-build in current directory, in \$CROSSBOW_BOWTIE_HOME/bowtie-build, or in \$PATH"
				exit 1
			fi
		fi
	fi
}

##
# Make the jar file.
#
do_jar() {
	if [ ! -f jar/$INDEX.jar ]
	then
		# Jar it up
		jar cf $INDEX.jar cmap.txt cmap_long.txt sequences index snps
	else
		echo "$INDEX.jar already present"
	fi
}

##
# Get the genome fasta files and rename
#
do_get_fasta() {
	mkdir -p sequences
	cd sequences
	dir=`pwd`
	for ci in $CHRS_TO_INDEX ; do
		c=$ENSEMBL_PREFIX.dna.$ci
		F=${c}.fa.gz
		if [ ! -f $F ] ; then
			if ! get ${ENSEMBL_FTP}/$F ; then
				echo "Error: Unable to get '${ENSEMBL_FTP}/$F'"
				exit 1
			fi
		fi
	done
	ARGS="--cmap=cmap.txt --cmap-long=cmap_long.txt --suffix=.fa"
	if ! perl $SCRIPT_DIR/fasta_cmap.pl $ARGS -- $dir/*.fa.gz ; then
		echo "Error running: $SCRIPT_DIR/fasta_cmap.pl $ARGS -- $dir/*.fa.gz"
		exit 1
	fi
	# Gather output files into $INPUTS
	for fa in `ls $dir/*.fa` ; do
		[ -n "$INPUTS" ] && INPUTS="$INPUTS,"
		INPUTS="$INPUTS$fa"
	done
	cd ..
	[ ! -f sequences/cmap.txt ] && echo "Error: no sequences/cmap.txt created" && exit 1
	[ ! -f sequences/cmap_long.txt ] && echo "Error: no sequences/cmap_long.txt created" && exit 1
	mv sequences/cmap.txt .
	mv sequences/cmap_long.txt .
}

##
# Make the Bowtie index files.
#
do_index() {
	if [ ! -f index/$INDEX.1.ebwt ] ; then
		INPUTS=
		do_get_fasta
		mkdir -p index
		cd index
		CMD="$BOWTIE_BUILD_EXE $* $INPUTS $INDEX"
		echo Running $CMD
		if $CMD ; then
			echo "$INDEX index built"
		else
			echo "Index building failed; see error message"
		fi
		cd ..
	else
		echo "$INDEX.*.ebwt files already present"
	fi
}

##
# Obtain SNPs for the organism using the ensembl_snps.pl script, which
# in turn uses 'mysql' to query the Ensembl database.
#
do_snps() {
	if [ ! -d snps ] ; then
		# Create the SNP directory
		if ! perl $SCRIPT_DIR/ensembl_snps.pl --database=$ENSEMBL_SNP_DB --cb-out=snps --cb-cmap=cmap.txt ; then
			echo "Error: ensembl_snps.pl failed; aborting..."
			exit 1
		fi
	else
		echo "snps directory already present"
	fi
}
