#!/bin/sh

##
# push.sh
#
# Run from Crossbow root directory (i.e. sh emr/util/push.sh X1.Y1.Z1
# X2.Y2.Z2 where X1.Y1.Z1 is the source version and X2.Y2.Z2 is the
# destination version).  Copies all of the S3-resident files from
# an S3 subdirectory corresponding to one version of Crossbow to
# another S3 subdirectory corresponding to another (usually newer)
# version.  Once that copy is done, the Perl-script infrastructure is
# copied from the local computer into the new S3 directory, overwriting
# the older versions of those files.  The S3CFG environent variable
# must be set to an appropriate .s3cfg file (config file for s3cmd).
#

d=`dirname $0`
d=$d/../..

VERSION_OLD=$1
[ -z "$VERSION_OLD" ] && echo "Must specify source version as argument" && exit 1
shift
VERSION_NEW=$1
[ -z "$VERSION_NEW" ] && echo "Must specify destination version as argument" && exit 1
[ -z "$S3CFG" ] && echo "S3CFG not set" && exit 1

s3cmd -c $S3CFG \
	 --acl-public --recursive cp \
	s3://crossbow-emr/$VERSION_OLD/ \
	s3://crossbow-emr/$VERSION_NEW

s3cmd -c $S3CFG \
	--acl-public \
	put \
	$d/*.pl $d/*.pm \
	s3://crossbow-emr/$VERSION_NEW/
