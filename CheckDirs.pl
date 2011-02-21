#!/usr/bin/perl

##
# MapWrap.pl
#
# Simple wrapper that mimics some of Hadoop's behavior during the
# Map step of a MapReduce computation.
#

use strict;
use warnings;
use Getopt::Long;
use FindBin qw($Bin); 
use lib $Bin;
use lib "$Bin/contrib";
use Cwd 'abs_path';
use Wrap;
use File::Path qw(mkpath);
use POSIX qw/strftime/;

my $input = "";
my $output = "";
my $intermediate = "";
my $force = 0;
my $verbose = 0;
my $VERSION = `cat $Bin/VERSION`; $VERSION =~ s/\s//g;

my $support = qq!
When requesting support, please include the full output printed here.
If a child process was the cause of the error, the output should
include the relevant error message from the child's error log.  You may
be asked to provide additional files as well.
!;

##
# Printer that prints to STDERR and, optionally, to a file for messages.
#
my $msgfn = "";
my $msgfh = undef;
sub msg($) {
	my $msg = shift;
	$msg =~ s/[\r\n]*$//;
	print STDERR "$msg\n";
	print {$msgfh} "$msg\n" if defined($msgfh);
}

##
# Printer that prints to STDERR and, optionally, to a file for counters.
#
my ($cntfn, $cntdir) = ("", "");
my $cntfh = undef;
sub cnt($) {
	my $msg = shift;
	$msg =~ s/[\r\n]*$//;
	print STDERR "$msg\n";
	print {$cntfh} "$msg\n" if defined($cntfh);
}

##
# Print an error message, a support message, then die with given
# exitlevel.
#
sub mydie($$) {
	my ($msg, $lev) = @_;
	msg("Fatal error $VERSION:D$lev: $msg");
	msg($support);
	exit $lev;
}

GetOptions (
	"messages:s"        => \$msgfn,
	"counters:s"        => \$cntdir,
	"intermediate:s"    => \$intermediate,
	"input:s"           => \$input,
	"output:s"          => \$output,
	"force"             => \$force) || die "Bad option\n";

if($msgfn ne "") {
	open($msgfh, ">>$msgfn") || mydie("Could not open message-out file $msgfn for writing", 15);
}
$input ne ""        || mydie("Must specify input directory with --input", 10);
$intermediate ne "" || mydie("Must specify intermediate directory with --intermediate", 10);
$output ne ""       || mydie("Must specify output directory with --output", 10);
$cntdir ne ""       || mydie("Must specify counters directory with --counters", 10);

msg("=== Directory checker ===");
msg("Time: ".strftime('%H:%M:%S %d-%b-%Y', localtime));
msg("Input: $input");
msg("Output: $output");
msg("Intermediate: $intermediate");
msg("Counters: $cntdir");
msg("Options: [ ".($force      ? "--force "        : "")."]");

sub checkDir {
	my ($dir, $forceoverride) = @_;
	if(-d $dir) {
		mydie("Output directory $dir already exists", 20) unless $force;
		if($force && !$forceoverride) {
			msg("Removing directory $dir due to --force");
			system("rm -rf $dir >/dev/null 2>/dev/null");
			-d $dir && mydie("Could not remove directory $dir", 30);
		}
	}
	mkpath($dir);
	(-d $dir) || mydie("Could not create new directory $dir", 40);
}
checkDir($output);
checkDir($intermediate);
if(defined($cntdir) && $cntdir ne "") {
	checkDir($cntdir);
}
close($msgfh) if $msgfn ne "";
