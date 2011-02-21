#!/usr/bin/perl

##
# BinSort.pl
#
# A utility for binning and sorting input data in parallel.  Input
# files
#

use strict;
use warnings;
use Getopt::Long;
use FindBin qw($Bin); 
use lib $Bin;
use lib "$Bin/contrib";
use Cwd 'abs_path';
use ForkManager;
use IO::File;
use List::Util qw[min max];

my $input = "";
my $output = "";
my $intermediate = "";
my $prefix = "";
my $suffix = "";
my $delim = "\t";
my $sortSize = "";
my $cores = 0;
my $sortArgs = "";
my $verbose = 0;
my $force = 0;
my $keep = 0;
my $excludeUnmapped = 0;

my @bin = ();
my $binmapStr = "";
my %binmap = ();

GetOptions (
	"input:s"          => \$input,
	"intermediate:s"   => \$intermediate,
	"output:s"         => \$output,
	"bin:s"            => \@bin,
	"sort:s"           => \$sortArgs,
	"delim:s"          => \$delim,
	"S:i"              => \$sortSize,
	"size:i"           => \$sortSize,
	"cores:i"          => \$cores,
	"bin-map:s"        => \$binmapStr,
	"binmap:s"         => \$binmapStr,
	"exclude-unmapped" => \$excludeUnmapped,
	"prefix:s"         => \$prefix,
	"suffix:s"         => \$suffix,
	"keep-all"         => \$keep,
	"verbose"          => \$verbose,
	"force"            => \$force) || die "Bad option\n";

if(scalar(@ARGV) > 0) {
	$input .= "," if $input ne "";
	$input .= join(",", @ARGV);
}

# By default, limit the total size of all sorts to 2GB
$delim = "\t" if $delim eq "";

print STDERR "# parallel binners/sorters: $cores\n";
print STDERR "Input: $input\n";
print STDERR "Output: $output\n";
print STDERR "Sort memory footprint (total): $sortSize\n";
print STDERR "Output prefix/suffix: $prefix/$suffix\n";
print STDERR "Delimiter (ascii): ".ord($delim)."\n";
print STDERR "Options: [ ";
print STDERR "-keep-all " if $keep;
print STDERR "-force " if $force;
print STDERR "]\n";

sub checkDir($) {
	my $dir = shift;
	if(-d $dir) {
		die "Output directory $dir already exists" unless $force;
		if($force) {
			print STDERR "Removing directory $dir due to -force\n";
			system("rm -rf $dir >/dev/null 2>/dev/null");
			-d $dir && die "Could not remove directory $dir";
		}
	}
	system("mkdir -p $dir >/dev/null 2>/dev/null");
	-d $dir || die "Could not create new directory $dir";
}
checkDir("$output");
$intermediate = "$output.pre" if $intermediate eq "";
my $binsOut = "$intermediate/bins";
my $binsErr = "$intermediate/bins.err";
checkDir($binsOut);
checkDir($binsErr);
$output = abs_path($output);

##
# Make a string into an acceptible filename.
#
sub fsSanitize($) {
	my $f = shift;
	my $ret = "";
	for(my $i = 0; $i < length($f); $i++) {
		my $c = substr($f, $i, 1);
		if($c =~ /[.,#A-Za-z01-9_-]/) {
			$ret .= $c;
		} else {
			$ret .= "_";
		}
	}
	return $ret;
}

if($binmapStr ne "") {
	open (BINMAP, $binmapStr) || die "Could not open $binmapStr for reading\n";
	print "Bin map = {\n" if $verbose;
	while(<BINMAP>) {
		chomp;
		my @s = split /\t/;
		scalar(@s) == 2 || die "Expected key-tab-value, got:\n$_\n";
		my ($k, $v) = @s;
		defined($binmap{$k}) && print "WARNING: Key $k is mapped more than once\n";
		$binmap{$k} = fsSanitize($v);
		print "    $k => $binmap{$k}\n" if $verbose;
	}
	print "}\n" if $verbose;
	close(BINMAP);
}

print "Starting fork manager\n" if $verbose;
my $pm = new Parallel::ForkManager($cores);

# All bins must be >= 1
for my $b (@bin) { $b > 0 || die "A -bin was $b, but must be > 0\n"; }

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

# First, determine the number of input files
my $ninputs = 0;
for my $inp (split(/,/, $input)) {
	$inp = abs_path($inp);
	-d $inp || -f $inp || die "No such input file or directory as \"$inp\"\n";
	my @fs = ();
	if(-d $inp) { @fs = <$inp/*>; }
	else { push @fs, $inp; }
	$ninputs += scalar(@fs);
}
print STDERR "Found $ninputs input files\n";

# For each input dir
my %filesDone = ();
my %bases = ();
print STDERR "--- Bin ---\n";
my $fi = 0;
for my $inp (split(/,/, $input)) {
	$inp = abs_path($inp) if $inp ne "-";
	-d $inp || -f $inp || $inp eq "-" || die "No such input file or directory as \"$inp\"\n";
	my @fs = ();
	if(-d $inp) { @fs = <$inp/*>; }
	else { push @fs, $inp; }
	scalar(@fs) > 0 || die "No input files in directory \"$inp\"\n";
	# For each input file (in current dir)
	for my $f (@fs) {
		my $base = `basename $f`;
		chomp($base);
		defined($bases{$base}) && die "Attempted to process file $base more than once\n";
		$bases{$base} = 1; # parent keeps track of all the basenames
		$fi++;
		if($childFailed) {
			print STDERR "Aborting master loop because child failed\n";
			last;
		}
		$pm->start and next; # fork off a mapper for this input file
		print STDERR "Pid $$ processing input $f [$fi of $ninputs]...\n";
		if($f =~ /\.gz$/) {
			open INP, "gzip -dc $f |" || die "Could not open pipe 'gzip -dc $f |'";
		} elsif($f =~ /\.bz2$/) {
			open INP, "bzip2 -dc $f |" || die "Could not open pipe 'bzip2 -dc $f |'";
		} else {
			open INP, "$f" || die "Could not open $f for reading\n";
		}
		my $lastBin = undef;
		my $lastBinval = undef;
		my %outfhs = ();
		while(<INP>) {
			chomp;
			my @s = split /$delim/;
			my $binkey = "";
			# For each binning dimension
			for my $b (@bin) {
				$b <= scalar(@s) || die "Bad bin index $b; line only had ".scalar(@s)." tokens:\n$_\n";
				$binkey .= $s[$b-1];
			}
			if(defined($lastBin) && $binkey eq $lastBin) {
				# Fast, common case; do what we did last time
				defined($lastBinval) || die;
				print {$outfhs{$lastBinval}} "$_\n";
			} else {
				# Use -binmap to map the bin key.  If no mapping exists,
				# keep the same key (but sanitized).
				unless(defined($binmap{$binkey})) {
					next if $excludeUnmapped;
					# Make a mapping to a sanitized version of binkey
					$binmap{$binkey} = fsSanitize($binkey);
				}
				my $binval = $binmap{$binkey};
				unless(defined($outfhs{$binval})) {
					system("mkdir -p $binsOut/$base");
					my $ofn = "$binsOut/$base/$binval";
					print STDERR "Opened filehandle $ofn" if $verbose;
					print STDERR "; ".scalar(keys %outfhs)." open in PID $$\n" if $verbose;
					$outfhs{$binval} = new IO::File($ofn, "w");
					$outfhs{$binval} || die "Could not open $ofn for writing\n";
				}
				print {$outfhs{$binval}} "$_\n";
				$lastBin = $binkey;
				$lastBinval = $binval;
			}
		}
		# Close output handles
		for my $bin (keys %outfhs) { $outfhs{$bin}->close() };
		# Close input handle
		close(INP);
		$? == 0 || die "Bad exitlevel from input slurp: $?\n";
		$pm->finish; # end of fork
	}
}
print STDERR "Aborted master loop because child failed\n" if $childFailed;
$pm->wait_all_children;
if($childFailed) {
	die "Aborting because child with PID $childFailedPid exited abnormally\nSee previous output\n";
} else {
	print STDERR "All children succeeded\n";
}

# Now collect a list of all the binvals.  We couldn't have (easily)
# collected them in the previous loop because the binvals were known
# only to the child processes and not to the parent.  But we can
# reconstitute them based on the file names.
my %binvals = ();
for my $base (keys %bases) {
	for my $f (<$binsOut/$base/*>) {
		my $b = `basename $f`;
		chomp($b);
		$binvals{$b} = 1;
	}
}

#
$sortSize = int((3 * 1024 * 1024)/min($cores, scalar(keys %binvals)));
my $bi = 0;
my $sortCmd = "sort -S $sortSize $sortArgs";
print STDERR "--- Sort ---\n";
print STDERR "Sort command: $sortCmd\n";
for my $binval (sort keys %binvals) {
	$bi++;
	if($childFailed) {
		print STDERR "Aborting master loop because child failed\n";
		last;
	}
	$pm->start and next; # fork off a mapper for this input file
	print STDERR "Pid $$ processing bin $binval [$bi of ".scalar(keys %binvals)."]...\n";
	my $inps = "";
	for my $base (keys %bases) {
		if(-f "$binsOut/$base/$binval") {
			$inps .= "$binsOut/$base/$binval ";
		}
	}
	my $ret = system("$sortCmd $inps >$output/$prefix$binval$suffix 2>$binsErr/$binval");
	if($ret == 0 && !$keep) {
		# Delete all the files that were inputs to the sort
		system("rm -f $inps");
	}
	exit $ret;
}
$pm->wait_all_children;
if($childFailed) {
	die "Aborting because child with PID $childFailedPid exited abnormally\nSee previous output\n";
} else {
	print STDERR "All children succeeded\n";
}

print STDERR "DONE\n";
# No errors
unless($keep) {
	print STDERR "Removing $intermediate (to keep, specify -keep-all)\n";
	system("rm -rf $intermediate");
}
