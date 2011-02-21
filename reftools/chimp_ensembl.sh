#!/bin/sh

##
# chimp_ensembl.sh
#
# Build a chimp reference jar from scratch using info from the current
# version of Ensembl.  Put results in subdirectory called
# "chimp_ensembl_(ver)" where (ver) is the Ensembl version used.
#
# To build a colorspace version, run 'chimp_ensembl.sh .c -C'.
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
ENSEMBL_SNP_VER=21n
ENSEMBL_PREFIX=Pan_troglodytes.CHIMP2.1.$ENSEMBL_VER
ENSEMBL_ORGANISM=ptroglodytes
ENSEMBL_FTP=ftp://ftp.ensembl.org/pub/release-$ENSEMBL_VER/fasta/pan_troglodytes/dna
ENSEMBL_SNP_DB=pan_troglodytes_variation_${ENSEMBL_VER}_${ENSEMBL_SNP_VER}
INDEX=chimp_ensembl_${ENSEMBL_VER}$SUFFIX
SIMPLE_NAME=$INDEX

# Change to jar scratch directory
mkdir -p $SIMPLE_NAME
cd $SIMPLE_NAME

# Compose the list of fasta files to download
i=2
BASE_CHRS="chromosome.1"
while [ $i -lt 23 ] ; do
	if [ $i -eq 2 ] ; then
		# "Chromosome 2" comes in 2 parts
		BASE_CHRS="$BASE_CHRS chromosome.2a chromosome.2b"
	else
		BASE_CHRS="$BASE_CHRS chromosome.$i"
	fi
	i=`expr $i + 1`
done
BASE_CHRS="$BASE_CHRS chromosome.X chromosome.Y chromosome.MT chromosome.M chromosome.Un nonchromosomal"
CHRS_TO_INDEX=$BASE_CHRS

[ -z "$CROSSBOW_HOME" ] && echo "CROSSBOW_HOME not set" && exit 1
source $CROSSBOW_HOME/reftools/shared.sh

check_prereqs
find_bowtie_build
do_index $*
do_snps
do_jar

cd ..
