#!/usr/bin/perl

##
# ReduceWrap.pl
#
# Simple wrapper that mimics some of Hadoop's behavior during the
# Reduce step of a MapReduce computation.
#

use strict;
use warnings;
use Getopt::Long;
use FindBin qw($Bin);
use lib $Bin;
use lib "$Bin/contrib";
use Cwd 'abs_path';
use ForkManager;
use Sort;
use Wrap;
use File::Path qw(mkpath);
use POSIX qw/strftime/;
use List::Util qw[min max];

my $name = "";
my $stage = -1;
my $numStages = -1;
my $nred = 1;
my $ntasks = 1;
my $input = "";
my $output = "";
my $intermediate = "";
my $binFields = 0;
my $sortFields = 0;
my $sortSize = 0;
my $maxRecords = 800000;
my $maxFiles = 40;
my $force = 0;
my $keep = 0;
my $externalSort = 0;
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
	msg("Fatal error $VERSION:R$lev: $msg");
	msg($support);
	exit $lev;
}

GetOptions (
	"name:s"            => \$name,
	"stage:i"           => \$stage,
	"num-stages:i"      => \$numStages,
	"input:s"           => \$input,
	"output:s"          => \$output,
	"messages:s"        => \$msgfn,
	"counters:s"        => \$cntfn,
	"intermediate:s"    => \$intermediate,
	"reducers:i"        => \$nred,
	"tasks:i"           => \$ntasks,
	"bin-fields:i"      => \$binFields,
	"sort-fields:i"     => \$sortFields,
	"external-sort"     => \$externalSort,
	"S:i"               => \$sortSize,
	"size:i"            => \$sortSize,
	"max-sort-records:i"=> \$maxRecords,
	"max-sort-files:i"  => \$maxFiles,
	"force"             => \$force,
	"keep-all"          => \$keep,
	"verbose"           => \$verbose) || mydie("Bad option", 1);

$intermediate = "$output.reduce.pre" if $intermediate eq "";
if($msgfn ne "") {
	open($msgfh, ">>$msgfn") || mydie("Could not open message-out file $msgfn for writing", 15);
}

if($name ne "") {
	msg("==========================");
	msg("Stage $stage of $numStages. $name");
	msg("==========================");
}
msg("Time: ".strftime('%H:%M:%S %d-%b-%Y', localtime));

msg("=== Reduce ===");
msg("# parallel reducers: $nred");
msg("# reduce tasks: $ntasks");
msg("Input: $input");
msg("Output: $output");
msg("Intermediate: $intermediate");
msg("# bin, sort fields: $binFields, $sortFields");
msg("Total allowed sort memory footprint: $sortSize");
msg("Options: [ ".
    ($keep  ? "--keep-all " : "").
    ($force ? "--force "    : "")."]");

$sortSize = int((3 * 1024 * 1024)/$nred);

$input  ne "" || mydie("Must specify input directory with --input", 10);
$output ne "" || mydie("Must specify output directory with --output", 20);
-d $input || mydie("Input directory doesn't exist: \"$input\"", 30);
$sortFields >= $binFields || mydie("--sort-fields must be >= --bin-fields", 40);
$sortFields >= 1          || mydie("--sort-fields must be >= 1", 50);
$binFields  >= 1          || mydie("--bin-fields must be >= 1", 60);

sub checkDir($) {
	my $dir = shift;
	if(-d $dir) {
		mydie("Output directory $dir already exists", 70) unless $force;
		if($force) {
			msg("Removing directory $dir due to --force");
			system("rm -rf $dir >/dev/null 2>/dev/null");
			-d $dir && mydie("Could not remove directory $dir", 80);
		}
	}
	mkpath($dir);
	(-d $dir) || mydie("Could not create new directory $dir", 90);
	return abs_path($dir);
}
$output = checkDir($output);
my $errDir = "$intermediate/reduce.err";
$errDir = checkDir($errDir);
my $taskDir = "$intermediate/reduce.tasks";
$taskDir = checkDir($taskDir);
my $sortedTaskDir = "$intermediate/reduce.stasks";
$sortedTaskDir = checkDir($sortedTaskDir);
my $workingDir = "$intermediate/reduce.wds";
$workingDir = checkDir($workingDir);
my $binSizeDir = "$intermediate/reduce.binsz";
$binSizeDir = checkDir($binSizeDir);
if(defined($cntfn) && $cntfn ne "") {
	open($cntfh, ">>", "$cntfn") || mydie("Could not open counters file $cntfn", 95);
}

my $cmd = join(" ", @ARGV);
msg("Command:\n$cmd");

########################################
# Stage 1. Partition bins into tasks
########################################

my @taskFhs = ();
my @taskFns = ();

my $pm = new Parallel::ForkManager($nred); 

# Setup a callback for when a child finishes up so we can
# get its exit code
my $childFailed = 0;
my $childFailedPid = 0;
$pm->run_on_finish(
	sub {
		my ($pid, $exit_code, $ident) = @_;
		$childFailed = $exit_code != 0;
		$childFailedPid = $pid;
	}
);

##
# Count size of bins in each input file in parallel.
#
msg("Calculating per-input bin counts in parallel");
my $ninfiles = 0;
for my $dir (split(/,/, $input)) {
	$dir = abs_path($dir);
	-d $dir || mydie("No such input directory as \"$dir\"", 100);
	my @fs = <$dir/*>;
	$ninfiles += scalar(@fs);
}
my $fi = 0;
for my $dir (split(/,/, $input)) {
	$dir = abs_path($dir);
	-d $dir || mydie("No such input directory as \"$dir\"", 110);
	for my $f (<$dir/*>) {
		$fi++;
		$pm->start and next;
		msg("Pid $$ processing input $f [$fi of $ninfiles]...");
		my %binSizes = ();
		if($f =~ /\.gz$/) {
			open(F, "gzip -dc $f |") || mydie("Could not open gz file \"$f\" for reading", 120);
		} elsif($f =~ /\.bz2$/) {
			open(F, "bzip2 -dc $f |") || mydie("Could not open bzip2 file \"$f\" for reading", 130);
		} else {
			open(F, "$f") || mydie("Could not open \"$f\" for reading", 140);
		}
		while(<F>) {
			chomp;
			my @s = split(/\t/);
			my $joined = join("\t", @s[0..min($binFields-1, $#s)]);
			scalar(@s) >= $sortFields || $joined eq "FAKE" || mydie("$sortFields sort fields, but line doesn't have that many:\n$_", 150);
			my $k = $joined;
			$binSizes{$k}++;
		}
		my $ofn = sprintf "$binSizeDir/sizes-%05d", $$;
		open (COUT, ">$ofn") || mydie("Could not open \"$ofn\" for writing", 160);
		for my $k (keys %binSizes) {
			print COUT "$k\t$binSizes{$k}\n";
		}
		close(COUT);
		$pm->finish;
	}
}
$pm->wait_all_children;

##
# Sum all per-input sizes
#
msg("Summing per-input counts");
my %binSizes = ();
for my $f (<$binSizeDir/*>) {
	open (F, $f) || mydie("Could not open \"$f\" for reading", 170);
	while(<F>) {
		chomp;
		my @s = split /\t/;
		scalar(@s) >= 2 ||
			mydie("Too few fields in subtotal line in $f:\n$_", 180);
		my $k = join("\t", @s[0..($#s-1)]);
		$s[-1] == int($s[-1]) ||
			mydie("Malformed subtotal line in $f; final field isn't integer:\n$s[-1]", 190);
		$binSizes{$k} += $s[-1];
	}
	close(F);
}

##
# In one pass, allocate every bin to a task.  Greedily allocate each
# bin to the task with the fewest records in it.
#
msg("Factoring input into $ntasks tasks");
my %tasks = ();
my $nonemptyTasks = 0;
my @taskSzs = (0) x $ntasks;
for my $k (sort { $binSizes{$b} <=> $binSizes{$a} } keys %binSizes) {
	my $min = -1;
	for(my $i = 0; $i <= $#taskSzs; $i++) {
		if($taskSzs[$i] < $min || $min == -1) {
			$min = $taskSzs[$i];
			$tasks{$k} = $i;
		}
	}
	defined($tasks{$k}) || mydie("Couldn't map key \"$k\" to a task; sizes: @taskSzs", 200);
	$nonemptyTasks++ if $taskSzs[$tasks{$k}] == 0;
	$taskSzs[$tasks{$k}] += $binSizes{$k};
}

# Allocate and write bins
$fi = 0;
my %binPids = ();
for my $dir (split(/,/, $input)) {
	$dir = abs_path($dir);
	-d $dir || mydie("No such input directory as \"$dir\"", 210);
	for my $f (<$dir/*>) {
		$fi++;
		my $pid = $pm->start;
		$binPids{$pid} = 1;
		next if $pid;
		msg("Pid $$ processing input $f [$fi of $ninfiles]...");
		mkpath("$taskDir/$$");
		for(my $i = 0; $i < $ntasks; $i++) {
			my $nfn = sprintf "task-%05d", $i;
			push @taskFns, "$taskDir/$$/$nfn";
			my $cmd2 = ">$taskFns[-1]";
			push @taskFhs, undef;
			open ($taskFhs[-1], $cmd2) || mydie("Could not open pipe for writing: \"$cmd2\"", 220);
		}
		if($f =~ /\.gz$/) {
			open(F, "gzip -dc $f |") || mydie("Could not open gz file \"$f\" for reading", 230);
		} elsif($f =~ /\.bz2$/) {
			open(F, "bzip2 -dc $f |") || mydie("Could not open bzip2 file \"$f\" for reading", 240);
		} else {
			open(F, "$f") || mydie("Could not open \"$f\" for reading", 250);
		}
		while(<F>) {
			chomp;
			my @s = split(/\t/);
			my $k = join("\t", @s[0..min($binFields-1, $#s)]);
			defined($tasks{$k}) || mydie("Bin \"$k\" wasn't assigned a task!", 260);
			print {$taskFhs[$tasks{$k}]} "$_\n";
		}
		close(F);
		# Close task pipes.
		for(my $i = 0; $i < $ntasks; $i++) { close($taskFhs[$i]); }
		$pm->finish;
	}
}
$pm->wait_all_children;
msg("Factored $ninfiles files into $nonemptyTasks non-empty tasks");

########################################
# Stage 2. Sort and reduce each task
########################################

my @srPids = ();
my $reduceProcs = 0;

##
# Sort each bin of tuples prior to calling the reducer.
#
sub doSort($$$) {
	my ($task, $ntasks, $external) = @_;
	my @nfn = (); # bin inputs
	my $taskEmpty = 1;
	for my $k (keys %binPids) {
		my $subtask = sprintf "$taskDir/$k/task-%05d", $task;
		-f $subtask || mydie("No such input file as $subtask", 270);
		$taskEmpty = 0 if -s $subtask > 0;
		push @nfn, $subtask;
	}
	my $sfn = sprintf "$sortedTaskDir/stask-%05d", $$;
	-f $sfn && mydie("Sorted version of input file $sfn already exists", 280);
	length("$sortSize") > 0 || mydie("sortSize has length 0", 281);
	length("$sortFields") > 0 || mydie("sortFields has length 0", 282);
	if($external) {
		my $nfnstr = join(' ', @nfn);
		my $cmd = "sort -S $sortSize -k1,$sortFields $nfnstr > $sfn";
		system($cmd) == 0 || mydie("Sort command: '$cmd' failed", 284);
	} else {
		my $denom = min($nred, $ntasks);
		File::Sort::sort_file({
			I => \@nfn,
			o => $sfn,
			t => "\t",
			k => "1,$sortFields",
			y => max(int($maxRecords/$denom), 100),
			F => max(int($maxFiles/$denom), 3)
		});
	}
}

##
# Construct command for reducing the reduce task.
#
sub cmdifyReducer($) {
	my ($task) = @_;
	my $sfn = sprintf "$sortedTaskDir/stask-%05d", $$;
	-f $sfn || mydie("Sorted version of input file $sfn doesn't exist", 285);
	my $taskEmpty = (-s $sfn == 0);
	my $ofn = sprintf "$output/part-%05d", $$;
	-f $ofn && mydie("Output file $ofn already exists", 290);
	my $efn = sprintf "$errDir/epart-%05d", $$;
	-f $efn && mydie("Error file $efn already exists", 300);
	my $ret = ($taskEmpty ? undef : "cat $sfn | $cmd > $ofn 2> $efn");
	$reduceProcs++ if defined($ret);
	unless(defined($ret)) {
		msg("Pid $$ skipping task $task; input is empty");
	}
	return $ret;
}

# Map from PIDs to the file(s) where the error message is likely to be
# if and when they fail
my %pidToErrFiles = ();
my %pidToInputs = ();
my $alreadyDumped = 0;
sub failDump() {
	return if $alreadyDumped;
	msg("******");
	msg("* Aborting master loop because child $childFailedPid failed");
	msg("* (other children may also have failed)");
	msg("* Input file or string was:");
	msg("*   $pidToInputs{$childFailedPid}");
	msg("* Error message is in file: ".$pidToErrFiles{$childFailedPid}.", also printed below");
	msg("******");
	if(!open(ERR, $pidToErrFiles{$childFailedPid})) {
		msg("* (could not open)");
	} else {
		while(<ERR>) { msg("* $_"); }
		close(ERR);
	}
	msg("******");
	$alreadyDumped = 1;
}

##
# Open sort/reduce pipes.
#
for(my $i = 0; $i < $nonemptyTasks; $i++) {
	if($childFailed) { failDump(); last; }
	my $childPid = $pm->start;
	if($childPid != 0) {
		# I'm the parent
		my $efn = sprintf "$errDir/epart-%05d", $childPid;
		$pidToErrFiles{$childPid} = $efn;
		$pidToInputs{$childPid} = sprintf "$sortedTaskDir/stask-%05d", $childPid;
		next; # spawn the next child
	}
	# I'm the child
	exit 0 if $childFailed;
	my $nfn = sprintf "task-%05d", $i;
	my $wd = "$workingDir/$$";
	mkpath($wd);
	(-d $wd) || mydie("Could not create working directory $wd", 310);
	chdir($wd) || mydie("Could not change to working directory $wd", 320);
	my $cmd;
	#if($nonemptyTasks > 0) {
		msg("Pid $$ sorting task $nfn [".($i+1)." of ".max($nonemptyTasks, 1)."]...");
		doSort($i, $nonemptyTasks, $externalSort);
	#} else {
	#	# Make dummy input file
	#	my $sfn = sprintf "$sortedTaskDir/stask-%05d", $$;
	#	open(TMP, ">$sfn") || mydie("Could not touch dummy input file $sfn", 325);
	#	close(TMP);
	#}
	$cmd = cmdifyReducer($i);
	msg("Pid $$ reducing task $nfn [".($i+1)." of ".$nonemptyTasks."]...");
	exec($cmd) if defined($cmd);
	exit 0;
}
$pm->wait_all_children;
if($childFailed) {
	failDump(); # Dump offending file if we haven't already
	mydie("Aborting because child with PID $childFailedPid exited abnormally", 330);
}
if($nonemptyTasks == 0) {
	msg("WARNING: There was no input data");
}
msg("-- Reduce counters --");
Wrap::getAndPrintLocalCounters($errDir, \&msg);
Wrap::getAndPrintLocalCounters($errDir, \&cnt) if defined($cntfh);

# No errors
unless($keep) {
	msg("Removing $intermediate (to keep, specify --keep-all)");
	system("rm -rf $intermediate");
}
