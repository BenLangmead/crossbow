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
use ForkManager;
use Wrap;
use File::Path qw(mkpath);
use POSIX qw/strftime/;

my $name = "";
my $stage = -1;
my $numStages = -1;
my $nmap = 1;
my $input = "";
my $output = "";
my $intermediate = "";
my $lineByLine = 0;
my $silentSkipping = 0;
my $force = 0;
my $keep = 0;
my $verbose = 0;
my $retries = 3; 
my $delay = 5;
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
my $cntfn = "";
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
	msg("Fatal error $VERSION:M$lev: $msg");
	msg($support);
	exit $lev;
}

GetOptions (
	"name:s"            => \$name,
	"stage:i"           => \$stage,
	"num-stages:i"      => \$numStages,
	"mappers:i"         => \$nmap,
	"output:s"          => \$output,
	"messages:s"        => \$msgfn,
	"counters:s"        => \$cntfn,
	"intermediate:s"    => \$intermediate,
	"input:s"           => \$input,
	"retries:i"         => \$retries,
	"delay:i"           => \$delay,
	"force"             => \$force,
	"line-by-line"      => \$lineByLine,
	"silent-skipping"   => \$silentSkipping,
	"keep-all"          => \$keep) || die "Bad option\n";

if($msgfn ne "") {
	open($msgfh, ">>$msgfn") || mydie("Could not open message-out file $msgfn for writing", 15);
}
$input ne "" || mydie("Must specify input directory with --input", 10);
$intermediate = "$output.map.pre" if $intermediate eq "";

if($name ne "") {
	msg("==========================");
	msg("Stage $stage of $numStages. $name");
	msg("==========================");
}
msg("Time: ".strftime('%H:%M:%S %d-%b-%Y', localtime));

msg("=== Map ===");
msg("# parallel mappers: $nmap");
msg("Input: $input");
msg("Output: $output");
msg("Intermediate: $intermediate");
msg("Retries / delay: $retries / $delay");
msg("Options: [ " .
    ($lineByLine ? "--line-by-line " : "").
    ($keep       ? "--keep-all "     : "").
    ($force      ? "--force "        : "")."]");

sub checkDir($) {
	my $dir = shift;
	if(-d $dir) {
		mydie("Output directory $dir already exists", 20) unless $force;
		if($force) {
			msg("Removing directory $dir due to --force");
			system("rm -rf $dir >/dev/null 2>/dev/null");
			-d $dir && mydie("Could not remove directory $dir", 30);
		}
	}
	mkpath($dir);
	(-d $dir) || mydie("Could not create new directory $dir", 40);
}

checkDir($output);
my $errDir = "$intermediate/map.err";
checkDir($errDir);
my $workingDir = "$intermediate/map.wds";
checkDir($workingDir);
if(defined($cntfn) && $cntfn ne "") {
	open($cntfh, ">>", "$cntfn") || mydie("Could not open counters file $cntfn", 45);
}

my $cmd = join(" ", @ARGV);
msg("Starting $nmap mappers with command:\n$cmd");

my $pm = new Parallel::ForkManager($nmap); 

# Setup a callback for when a child finishes up so we can
# get its exit code
my $childFailed = 0;
my $childFailedPid = 0;
$pm->run_on_finish(
	sub {
		my ($pid, $exit_code, $ident) = @_;
		if($exit_code != 0) {
			$childFailed = $exit_code;
			$childFailedPid = $pid;
		}
	}
);

my @inputs = ();
my $linewise = 0;
for my $inp (split(/,/, $input)) {
	$inp = abs_path($inp);
	-d $inp || -f $inp || mydie("No such input file or directory as \"$inp\"", 50);
	my @fs = ();
	if(-d $inp) {
		@fs = <$inp/*>;
	} else {
		push @fs, $inp;
	}
	if($lineByLine) {
		$linewise = 1;
		for my $f (@fs) {
			if($f =~ /\.gz$/) {
				open(INP, "gzip -dc $f |") || mydie("Could not open pipe 'gzip -dc $f |'", 60);
			} elsif($f =~ /\.bz2$/) {
				open(INP, "bzip2 -dc $f |") || mydie("Could not open pipe 'bzip2 -dc $f |'", 70);
			} else {
				open(INP, "$f") || mydie("Could not open $f for reading", 80);
			}
			while(<INP>) {
				my $add = 1;
				if($silentSkipping) {
					$add = 0 if /^\s*$/ || /^#/;
				}
				push @inputs, $_ if $add;
			}
			close(INP);
			$? == 0 || mydie("Bad exitlevel from input slurp: $?", 90);
		}
	} else {
		push @inputs, @fs;
	}
}

# Map from PIDs to the file(s) where the error message is likely to be
# if and when they fail
my %pidToErrfiles = ();
my %pidToInputs = ();
my $alreadyDumped = 0;
sub failDump() {
	return if $alreadyDumped;
	msg("******");
	msg("* Aborting master loop because child $childFailedPid failed");
	msg("* (other children may also have failed)");
	msg("* Input file or string was:");
	msg("*   $pidToInputs{$childFailedPid}:");
	msg("* Error message is in file: ".$pidToErrfiles{$childFailedPid}.", also printed below");
	msg("******");
	if(!open(ERR, $pidToErrfiles{$childFailedPid})) {
		msg("* (could not open)");
	} else {
		while(<ERR>) { msg("* $_"); }
		close(ERR);
	}
	msg("******");
	$alreadyDumped = 1;
}

my $fi = 0;
for my $f (@inputs) {
	$fi++;
	if($childFailed) { failDump(); last; }
	my $childPid = $pm->start;
	if($childPid != 0) {
		# I'm the parent
		my $ofn = sprintf "map-%05d", $childPid;
		$pidToErrfiles{$childPid} = "$errDir/$ofn";
		$pidToInputs{$childPid} = "$f";
		next; # spawn the next child
	}
	# I'm the child
	exit 0 if $childFailed;
	chomp($f);
	msg("Pid $$ processing input $f [$fi of ".scalar(@inputs)."]...");
	my $ofn = sprintf "map-%05d", $$;
	my $redir = ">$output/$ofn 2>$errDir/$ofn";
	my $wd = "$workingDir/$$";
	mkpath($wd);
	(-d $wd) || mydie("Could not create working directory $wd", 100);
	chdir($wd) || mydie("Could not change to working directory $wd", 110);
	for(my $i = 0; $i <= $retries; $i++) {
		if($linewise) {
			my $pipe = "| $cmd $redir";
			open(CMD, $pipe) || mydie("Could not open pipe '$pipe' for writing", 120);
			print CMD "$f\n";
			close(CMD);
			if($? != 0) {
				msg("Non-zero return ($?) after closing pipe '$pipe'");
				msg("Retrying in $delay seconds...");
				sleep($delay);
				next;
			}
		} else {
			my $ret = 1;
			my $fullcmd = "";
			if($f =~ /\.gz$/) {
				$fullcmd = "gzip -dc $f | $cmd $redir";
			} elsif($f =~ /\.bz2$/) {
				$fullcmd = "bzip2 -dc $f | $cmd $redir";
			} else {
				$fullcmd = "cat $f | $cmd $redir";
			}
			$ret = system($fullcmd);
			if($ret != 0) {
				msg("Non-zero return ($ret) after executing command '$fullcmd'");
				msg("Retrying in $delay seconds...");
				sleep($delay);
				next;
			}
		}
		$pm->finish;
	}
	mydie("Out of retries; aborting...", 130);
}
msg("Aborting master loop because child failed") if $childFailed;
$pm->wait_all_children;
if($childFailed) {
	failDump();
	mydie("Aborting because child with PID $childFailedPid exited abnormally", 140);
} else {
	msg("All children succeeded");
}

msg("-- Map counters --");
Wrap::getAndPrintLocalCounters($errDir, \&msg);
Wrap::getAndPrintLocalCounters($errDir, \&cnt) if defined($cntfh);

# No errors
unless($keep) {
	msg("Removing $intermediate (to keep, specify --keep-all)");
	system("rm -rf $intermediate");
}
