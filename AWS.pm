#!/usr/bin/perl -w

##
# Author: Ben Langmead
#   Date: 2/14/2010
#
# Routines for getting and expanding jars from
#

package AWS;
use strict;
use warnings;

our $accessKey = "";
our $secretKey = "";

##
# If either $accessKey or $secretKey are not already set, look some
# more places for them.
#
sub ensureKeys($$) {
	my ($hadoop, $hadoop_arg) = @_;
	my $hadoopHome = $ENV{HADOOP_HOME};
	if(!defined($hadoopHome)) {
		$hadoop = $hadoop_arg if $hadoop_arg ne "";
		if(-x $hadoop) {
			$hadoopHome = `dirname $hadoop`;
			chomp($hadoopHome);
			$hadoopHome .= "/..";
		}
	}
	if($accessKey eq "") {
		if(defined($ENV{AWS_ACCESS_KEY_ID})) {
			$accessKey = $ENV{AWS_ACCESS_KEY_ID};
		} elsif(defined($hadoopHome)) {
			$accessKey = `grep fs.s3n.awsAccessKeyId $hadoopHome/conf/*.xml | sed 's/.*<value>//' | sed 's/<\\/value>.*//'`;
			$accessKey =~ s/\s.*$//; # In case we got multiple lines back
			if($accessKey eq "") {
				print STDERR "Couldn't get access key from $hadoopHome/conf/*.xml\n";
			}
		}
		if($accessKey eq "") {
			die "--accesskey was not specified, nor could the access ".
			    "key be retrived from an environment variable or from ".
			    "the \$HADOOP_HOME/conf directory\n";
		}
	}
	if($secretKey eq "") {
		if(defined($ENV{AWS_SECRET_ACCESS_KEY})) {
			$secretKey = $ENV{AWS_SECRET_ACCESS_KEY};
		} elsif(defined($hadoopHome)) {
			$secretKey = `grep fs.s3n.awsSecretAccessKey $hadoopHome/conf/*.xml | sed 's/.*<value>//' | sed 's/<\\/value>.*//'`;
			$secretKey =~ s/\s.*$//; # In case we got multiple lines back
			if($secretKey eq "") {
				print STDERR "Couldn't get secret key from $hadoopHome/conf/*.xml\n";
			}
		}
		if($secretKey eq "") {
			die "--secretkey was not specified, nor could the secret ".
			    "key be retrived from an environment variable or from ".
			    "the \$HADOOP_HOME/conf directory\n";
		}
	}
}

1;
