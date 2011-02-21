#!/usr/bin/perl -w

##
# CBFinish.pl
#
#  Authors: Ben Langmead & Michael C. Schatz
#     Date: October 20, 2009
#
# Put a proper chromosome name back onto all Crossbow records.
#
#  Author: Ben Langmead
#    Date: February 11, 2010
#

use strict;
use warnings;
use 5.004;
use Getopt::Long;
use IO::File;
use Carp;
use FindBin qw($Bin); 
use lib $Bin;
use Counters;
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

sub run($) {
	my $cmd = shift;
	print STDERR "Postprocess.pl: Running \"$cmd\"\n";
	return system($cmd);
}

# We want to manipulate counters before opening stdin, but Hadoop seems
# to freak out when counter updates come before the first <STDIN>.  So
# instead, we append counter updates to this list.
my @counterUpdates = ();

sub counter($) {
	my $c = shift;
	defined($c) || croak("Undefined counter update");
	print STDERR "reporter:counter:$c\n";
}

sub flushCounters() {
	for my $c (@counterUpdates) { counter($c); }
	@counterUpdates = ();
}

push @counterUpdates, "Postprocess,Invoked,1";

my $cmap_file = "";
my $cmap_jar = "";
my $dest_dir = "";
my $output = "";
my $cntfn = "";

sub dieusage {
	my $msg = shift;
	my $exitlevel = shift;
	$exitlevel = $exitlevel || 1;
	print STDERR "$msg\n";
	exit $exitlevel;
}

sub msg($) {
	my $m = shift;
	defined($m) || croak("Undefined message");
	$m =~ s/[\r\n]*$//;
	print STDERR "CBFinish.pl: $m\n";
}

Tools::initTools();

GetOptions (
	"output:s"        => \$output,
	"s3cmd:s"         => \$Tools::s3cmd_arg,
	"s3cfg:s"         => \$Tools::s3cfg,
	"jar:s"           => \$Tools::jar_arg,
	"accessid:s"      => \$AWS::accessKey,
	"secretid:s"      => \$AWS::secretKey,
	"hadoop:s"        => \$Tools::hadoop_arg,
	"wget:s"          => \$Tools::wget_arg,
	"cmap:s"          => \$cmap_file,
	"cmapjar:s"       => \$cmap_jar,
	"destdir:s"       => \$dest_dir,
	"counters:s"      => \$cntfn) || dieusage("Bad option", 1);

$dest_dir = "." if $dest_dir eq "";

msg("s3cmd: found: $Tools::s3cmd, given: $Tools::s3cmd_arg");
msg("jar: found: $Tools::jar, given: $Tools::jar_arg");
msg("hadoop: found: $Tools::hadoop, given: $Tools::hadoop_arg");
msg("wget: found: $Tools::wget, given: $Tools::wget_arg");
msg("s3cfg: $Tools::s3cfg");
msg("cmap_file: $cmap_file");
msg("cmap_jar: $cmap_jar");
msg("local destination dir: $dest_dir");
msg("Output dir: $output");
msg("ls -al");
msg(`ls -al`);

my %counters = ();
Counters::getCounters($cntfn, \%counters, \&msg, 1);
msg("Retrived ".scalar(keys %counters)." counters from previous stages\n");

if($cmap_jar ne "") {
	mkpath($dest_dir);
	(-d $dest_dir) || die "-destdir $dest_dir does not exist or isn't a directory, and could not be created\n";
}
if($cmap_file ne "" && ! -f $cmap_file) {
	die "-cmap file $cmap_file doesn't exist or isn't readable\n";
}

sub pushResult($) {
	my $fn = shift;
	msg("Pushing $fn");
	$output .= "/" unless $output =~ /\/$/;
	if($output =~ /^s3/i) {
		Get::do_s3_put($fn, $output, \@counterUpdates);
	} elsif($output =~ /^hdfs/i) {
		my $ret = Get::do_hdfs_put($fn, $output, \@counterUpdates);
		if($ret != 0) {
			msg("Fatal error: could not put result file $fn into HDFS directory $output");
			exit 1;
		}
	} else {
		mkpath($output);
		(-d $output) || die "Could not create output directory: $output\n";
		run("cp $fn $output") == 0 || die;
	}
}

my %cmap = ();
sub loadCmap($) {
	my $f = shift;
	if($f ne "" && -e $f) {
		open CMAP, "$f";
		while(<CMAP>) {
			chomp;
			my @s = split;
			next if $s[0] eq "" || $#s < 1;
			$cmap{$s[1]} = $s[0];
			push @counterUpdates, "Postprocess,Chromosome map entries loaded,1";
		}
		close(CMAP);
	}
}

if($cmap_jar ne "") {
	msg("Ensuring cmap jar is installed");
	Get::ensureFetched($cmap_jar, $dest_dir, \@counterUpdates);
	push @counterUpdates, "Postprocess,Calls to ensureJar,1";
	$cmap_file = "$dest_dir/cmap.txt";
	msg("Examining extracted files");
	msg("find $dest_dir");
	print STDERR `find $dest_dir`;
	unless(-f $cmap_file) {
		die "Extracting jar didn't create \"$dest_dir/cmap.txt\" file.\n";
	}
}

loadCmap($cmap_file) if $cmap_file ne "";

my %outfhs = ();
my %recs = ();
my $lines = 0;
while(<STDIN>) {
	next if /^\s*FAKE\s*$/;
	next if /^\s*$/;
	$lines++;
	flushCounters() if scalar(@counterUpdates) > 0;
	next unless $_ ne "";
	my @ss = split(/\t/);
	my $chr = $ss[0];
	$chr = $cmap{$chr} if defined($cmap{$chr});
	unless(defined($outfhs{$chr})) {
		counter("Postprocess,Chromosomes observed,1");
		$outfhs{$chr} = new IO::File(".tmp.CBFinish.pl.$$.$chr", "w");
	}
	$ss[0] = $chr;
	$ss[1] = int($ss[1]); # remove leading 0s
	print {$outfhs{$chr}} join("\t", @ss);
	$recs{$chr}++;
}
msg("Read $lines lines of output");
for my $chr (keys %outfhs) {
	counter("Postprocess,SNPs for chromosome $chr,$recs{$chr}");
	$outfhs{$chr}->close();
	my $fn = ".tmp.CBFinish.pl.$$.$chr";
	run("gzip -c < $fn > $chr.gz") == 0 || die "Couldn't gzip $fn\n";
	$fn = "$chr.gz";
	pushResult($fn);
	counter("Postprocess,Chromosome files pushed,1");
};
counter("Postprocess,0-SNP invocations,1") if $lines == 0;
flushCounters() if scalar(@counterUpdates) > 0;
