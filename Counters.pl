#!/usr/bin/perl -w

##
# Counters.pl
#
#  Authors: Ben Langmead
#     Date: February 14, 2010
#
# Get all the counters and put them in the output directory.
#

use strict;
use warnings;
use Getopt::Long;
use POSIX qw/strftime/;
use FindBin qw($Bin); 
use lib $Bin;
use Get;
use Util;
use Tools;
use AWS;
use File::Path qw(mkpath);

{
	# Force stderr to flush immediately
	my $ofh = select STDERR;
	$| = 1;
	select $ofh;
}

my @counterUpdates = ();

sub counter($) {
	my $c = shift;
	print STDERR "reporter:counter:$c\n";
}

sub flushCounters() {
	for my $c (@counterUpdates) { counter($c); }
	@counterUpdates = ();
}

sub trim($) {
	my $string = shift;
	$string =~ s/^\s+//;
	$string =~ s/\s+$//;
	return $string;
}

my $ref = "";
my $dest_dir = "";
my $output = "";

sub dieusage {
	my $msg = shift;
	my $exitlevel = shift;
	$exitlevel = $exitlevel || 1;
	print STDERR "$msg\n";
	exit $exitlevel;
}

Tools::initTools();

GetOptions (
	"s3cmd:s"         => \$Tools::s3cmd_arg,
	"s3cfg:s"         => \$Tools::s3cfg,
	"jar:s"           => \$Tools::jar_arg,
	"accessid:s"      => \$AWS::accessKey,
	"secretid:s"      => \$AWS::secretKey,
	"hadoop:s"        => \$Tools::hadoop_arg,
	"wget:s"          => \$Tools::wget_arg,
	"destdir:s"       => \$dest_dir,
	"output:s"        => \$output) || dieusage("Bad option", 1);

$output ne "" || die "Must specify non-empty -output\n";
print STDERR "s3cmd: found: $Tools::s3cmd, given: $Tools::s3cmd_arg\n";
print STDERR "jar: found: $Tools::jar, given: $Tools::jar_arg\n";
print STDERR "hadoop: found: $Tools::hadoop, given: $Tools::hadoop_arg\n";
print STDERR "wget: found: $Tools::wget, given: $Tools::wget_arg\n";
print STDERR "s3cfg: $Tools::s3cfg\n";
print STDERR "local destination dir: $dest_dir\n";
print STDERR "output url: $output\n";
print STDERR "ls -al\n";
print STDERR `ls -al`;

sub pushResult($) {
	my $fn = shift;
	print STDERR "Pushing $fn\n";
	$output .= "/" unless $output =~ /\/$/;
	if($output =~ /^s3/i) {
		Get::do_s3_put($fn, $output, \@counterUpdates);
	} elsif($output =~ /^hdfs/i) {
		Get::do_hdfs_put($fn, $output, \@counterUpdates);
	} else {
		mkpath($output);
		(-d $output) || die "Could not create push directory $output\n";
		run("cp $fn $output") == 0 || die;
	}
}

my $warnings = 0;
sub warning($) {
	print STDERR shift;
	$warnings++;
}

while(<STDIN>) { }

my $countersFn = "counters_".strftime('%Y_%H_%M_%S',localtime).".txt";
open TMP, ">$countersFn" || die "Could not open $countersFn for writing\n";

my $counters = 0;
my $hadoop = Tools::hadoop();
my $jstr = `$hadoop job -list all | awk '\$1 ~ /^job/ && \$2 == 2 {print \$1}'`;
my @jobs = split(/[\n\r]+/, $jstr);
for my $job (@jobs) {
	my $sstr = `$hadoop job -status $job`;
	my @status = split(/[\n\r]+/, $sstr);
	my $section = "";
	for (@status) {
		next if /^\s*$/;  # skip blank lines
		next if /^Job:/;  # skip Job: lines
		next if /^file:/; # skip file: lines
		next if /^tracking URL:/;
		if(/^map[(][)] completion: (.*)$/) {
			$1 eq "1.0" || warning("Incomplete mappers:\n\"$_\"\n");
		}
		if(/^reduce[(][)] completion: (.*)$/) {
			$1 eq "1.0" || warning("Incomplete reducers:\n\"$_\"\n");
		}
		next if /^Counters:/;
		chomp;
		my $l = trim($_);
		if(/[=]/) {
			# Key=Value line
			$section ne "" || warning("No label before line:\n\"$_\"\n");
			my @s = split(/[=]/, $l);
			$#s == 1 || die;
			print TMP "$job\t$section\t$s[0]\t$s[1]\n";
			counter("Get counters,Counters,1");
			$counters++;
		} else {
			$section = $l;
		}
	}
}
close(TMP);

counter("Get counters,Counter files pushed,1");
print STDERR "Pushing counters file to $output\n";
pushResult($countersFn);

print STDERR "Collected $counters counters\n";
print STDERR "$warnings warnings\n";
