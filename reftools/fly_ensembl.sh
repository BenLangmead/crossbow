#!/bin/sh

##
# fly_ensembl.sh
#
# Build a fly (D. melanogaster) reference jar from scratch using info
# from the current version of Ensembl.  Put results in subdirectory
# called "fly_ensembl_(ver)" where (ver) is the Ensembl version used.
#
# To build a colorspace version, run 'fly_ensembl.sh .c -C'.
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

SUFFIX=$1
shift
ENSEMBL_VER=59
ENSEMBL_SNP_VER=525a
ENSEMBL_PREFIX=Drosophila_melanogaster.BDGP5.25.$ENSEMBL_VER
ENSEMBL_ORGANISM=dmelanogaster
ENSEMBL_FTP=ftp://ftp.ensembl.org/pub/release-$ENSEMBL_VER/fasta/drosophila_melanogaster/dna
ENSEMBL_SNP_DB=drosophila_melanogaster_variation_${ENSEMBL_VER}_${ENSEMBL_SNP_VER}
INDEX=fly_ensembl_${ENSEMBL_VER}$SUFFIX
SIMPLE_NAME=$INDEX

# Change to jar scratch directory
mkdir -p $SIMPLE_NAME
cd $SIMPLE_NAME

# Compose the list of fasta files to download
BASE_CHRS""
BASE_CHRS="$BASE_CHRS chromosome.2L"
BASE_CHRS="$BASE_CHRS chromosome.2LHet"
BASE_CHRS="$BASE_CHRS chromosome.2R"
BASE_CHRS="$BASE_CHRS chromosome.2RHet"
BASE_CHRS="$BASE_CHRS chromosome.3L"
BASE_CHRS="$BASE_CHRS chromosome.3LHet"
BASE_CHRS="$BASE_CHRS chromosome.3R"
BASE_CHRS="$BASE_CHRS chromosome.3RHet"
BASE_CHRS="$BASE_CHRS chromosome.4"
BASE_CHRS="$BASE_CHRS chromosome.U"
BASE_CHRS="$BASE_CHRS chromosome.Uextra"
BASE_CHRS="$BASE_CHRS chromosome.X"
BASE_CHRS="$BASE_CHRS chromosome.XHet"
BASE_CHRS="$BASE_CHRS chromosome.YHet"
BASE_CHRS="$BASE_CHRS chromosome.dmel_mitochondrion_genome"
CHRS_TO_INDEX=$BASE_CHRS

[ -z "$CROSSBOW_HOME" ] && echo "CROSSBOW_HOME not set" && exit 1
source $CROSSBOW_HOME/reftools/shared.sh

check_prereqs
find_bowtie_build
do_index $*
do_snps
do_jar

cd ..
