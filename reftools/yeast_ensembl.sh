#!/bin/sh

##
# yeast_ensembl.sh
#
# Build a yeast (S. cerevisiae) reference jar from scratch using info
# from the current version of Ensembl.  Put results in subdirectory
# called "yeast_ensembl_(ver)" where (ver) is the Ensembl version used.
#
# To build a colorspace version, run 'human_ensembl.sh .c -C'.
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
ENSEMBL_SNP_VER=2a
ENSEMBL_PREFIX=Saccharomyces_cerevisiae.EF2.$ENSEMBL_VER
ENSEMBL_ORGANISM=scerevisiae
ENSEMBL_FTP=ftp://ftp.ensembl.org/pub/release-$ENSEMBL_VER/fasta/saccharomyces_cerevisiae/dna
ENSEMBL_SNP_DB=saccharomyces_cerevisiae_variation_${ENSEMBL_VER}_${ENSEMBL_SNP_VER}
INDEX=yeast_ensembl_${ENSEMBL_VER}$SUFFIX
SIMPLE_NAME=$INDEX

# Change to jar scratch directory
mkdir -p $SIMPLE_NAME
cd $SIMPLE_NAME

# Compose the list of fasta files to download
BASE_CHRS=
for i in 2-micron I II III IV IX Mito V VI VII VIII X XI XII XIII XIV XV XVI ; do
	BASE_CHRS="$BASE_CHRS chromosome.$i"
done
CHRS_TO_INDEX=$BASE_CHRS

[ -z "$CROSSBOW_HOME" ] && echo "CROSSBOW_HOME not set" && exit 1
source $CROSSBOW_HOME/reftools/shared.sh

check_prereqs
find_bowtie_build
do_index $*
do_snps
do_jar

cd ..
