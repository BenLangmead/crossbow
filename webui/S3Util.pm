#!/usr/bin/perl -w

##
# S3Util.pm
#
# Utilities used by Crossbow and Myrna web interfaces.
#

package S3Util;

use strict;
use warnings;
use Carp;
use Net::Amazon::S3;

##
# Parse an S3 path into a (protocol, bucket, path) triple.  Note that
# the path may be empty.
#
sub parsePath($) {
	my $s = shift;
	my $proto = undef;
	$proto = "s3n" if $s =~ /^s3n:\/\//i;
	$proto = "s3" if $s =~ /^s3:\/\//i;
	$proto || return undef;
	$s =~ s/^s3n?:\/\///; # strip protocol
	$s ne "" || return ($proto, undef, undef);
	my @ss = split(/\//, $s);
	scalar(@ss) > 0 || return ($proto, undef, undef);
	my $bucket = shift @ss;
	my $path = undef;
	$path = join("/", @ss) if scalar(@ss) > 0;
	return ($proto, $bucket, $path);
}

##
# Get an S3 object.
#
sub s3($$) {
	my ($awsId, $awsSecret) = @_;
	return(Net::Amazon::S3->new(
		aws_access_key_id     => $awsId,
		aws_secret_access_key => $awsSecret,
		retry                 => 1
	));
}

##
# Get an S3 client object.
#
sub client($$) {
	my ($awsId, $awsSecret) = @_;
	return Net::Amazon::S3::Client->new(s3 => s3($awsId, $awsSecret));
}

##
# Check whether ID/password credentials are good.
#
sub checkCreds($$) {
	my ($awsId, $awsSecret) = @_;
	my $client = client($awsId, $awsSecret);
	if(eval { $client->buckets() }) {
		return 1;
	} else {
		return 0;
	}
}

##
# Check if an s3 file exists.
#
sub s3exists {
	my ($awsId, $awsSecret, $path, $verbose) = @_;
	my $s3 = s3($awsId, $awsSecret);
	if(!eval { $s3->buckets() }) {
		return (-1, "Bad AWS ID and/or Secret Key");
	}
	defined($s3) || return (-1, "Could not create client");
	my ($pr, $bu, $pa) = parsePath($path);
	defined($bu) || return (-1, "Could not parse path $path");
	if(defined($pa)) {
		my $l = $s3->list_bucket({bucket => $bu, prefix => $pa, max_keys => 1});
		defined($l) || return (0, "list_bucket returned 0");
		print Dumper($l) if $verbose;
		if(scalar(@{$l->{keys}})) {
			my $key = shift @{$l->{keys}};
			$key = $key->{key};
			substr($key, 0, length($pa)) eq $pa || die;
			substr($key, 0, length($pa)) = "";
			if($key eq "" || substr($key, 0, 1) eq "/") {
				return (1, "remainder: $key");
			} else {
				return (0, "remainder: $key");
			}
		} else {
			return (0, "");
		}
	} else {
		return (1, "");
	}
}

if($0 =~ /S3Util\.pm$/) {
	use Getopt::Long;
	my ($id, $key);
	GetOptions (
		"aws-id:s"         => \$id,
		"aws-secret-key:s" => \$key);
	if(defined($id) && defined($key)) {
		if(checkCreds($id, $key)) {
			print "Creds OK\n";
		} else {
			print "BAD CREDS\n";
		}
	}
}

1;
