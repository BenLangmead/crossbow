#!/bin/sh

##
# push.sh
#
# Run from Crossbow root directory (i.e. sh emr/util/push.sh X.Y.Z
# where X.Y.Z is version).  Puts all of the Perl-script intrastructure
# into place.  Doesn't do anything about the binaries.  You either have
# to push those yourself or use the pull_push.sh script to move
# everything from one version to another first.  The S3CFG environent
# variable must be set to an appropriate .s3cfg file (config file for
# s3cmd).
#

d=`dirname $0`
d=$d/../..

VERSION=$1
[ -z "$VERSION" ] && echo "Must specify version as argument" && exit 1
[ -z "$S3CFG" ] && echo "S3CFG not set" && exit 1

s3cmd -c $S3CFG \
	--acl-public \
	put \
	$d/*.pl $d/*.pm \
	s3://crossbow-emr/$VERSION/
