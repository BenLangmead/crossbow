#!/usr/bin/perl -w

##
# Align.pl
#
# Align reads using Bowite.  Fetch reference jar (ensuring mutual
# exclusion among mappers) if necessary.
#
#  Author: Ben Langmead
#    Date: February 11, 2010
#

use strict;
use warnings;
use 5.004;
use Carp;
use Getopt::Long;
use IO::File;
use FindBin qw($Bin);
use lib $Bin;
use Get;
use Counters;
use Util;
use Tools;
use AWS;
use File::Path qw(mkpath);
use List::Util qw[min max];

{
	# Force stderr to flush immediately
	my $ofh = select STDERR;
	$| = 1;
	select $ofh;
}

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

my $ref = "";
my $dest_dir = "";
my $sam_passthru = 0;
my $partlen = 0;
my $discardReads = 0;
my $indexLocal = "";
my $qual = "phred33";
my $truncate = 0;
my $discardSmall = 0;
my $discardMate = 0;
my $straightThrough = 0;
my $test = 0;
my $cntfn = "";

my $bowtie = "";
my $bowtie_arg = "";

if(defined($ENV{BOWTIE_HOME})) {
	$bowtie = "$ENV{BOWTIE_HOME}/bowtie";
	unless(-x $bowtie) { $bowtie = "" };
}
if($bowtie eq "") {
	$bowtie = `which bowtie 2>/dev/null`;
	chomp($bowtie);
	unless(-x $bowtie) { $bowtie = "" };
}
$bowtie = "./bowtie" if ($bowtie eq "" && -x "./bowtie");

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
	print STDERR "Align.pl: $m\n";
}

Tools::initTools();

GetOptions (
	"bowtie:s"        => \$bowtie_arg,
	"s3cmd:s"         => \$Tools::s3cmd_arg,
	"s3cfg:s"         => \$Tools::s3cfg,
	"jar:s"           => \$Tools::jar_arg,
	"accessid:s"      => \$AWS::accessKey,
	"secretid:s"      => \$AWS::secretKey,
	"hadoop:s"        => \$Tools::hadoop_arg,
	"wget:s"          => \$Tools::wget_arg,
	"refjar:s"        => \$ref,
	"partlen:i"       => \$partlen,
	"index-local:s"   => \$indexLocal,
	"discard-reads:f" => \$discardReads,
	"qual:s"          => \$qual,
	"sampass"         => \$sam_passthru,
	"truncate:i"      => \$truncate,
	"discard-mate:i"  => \$discardMate,
	"discard-small"   => \$discardSmall,
	"straight-through"=> \$straightThrough,
	"counters:s"      => \$cntfn,
	"destdir:s"       => \$dest_dir,
	"test"            => \$test) || dieusage("Bad option", 1);

msg("s3cmd: found: $Tools::s3cmd, given: $Tools::s3cmd_arg");
msg("jar: found: $Tools::jar, given: $Tools::jar_arg");
msg("hadoop: found: $Tools::hadoop, given: $Tools::hadoop_arg");
msg("wget: found: $Tools::wget, given: $Tools::wget_arg");
msg("s3cfg: $Tools::s3cfg");
msg("bowtie: found: $bowtie, given: $bowtie_arg");
msg("partition len: $partlen");
msg("ref: $ref");
msg("quality: $qual");
msg("truncate at: $truncate");
msg("discard mate: $discardMate");
msg("discard reads < truncate len: $discardSmall");
msg("SAM passthrough: $sam_passthru");
msg("Straight through: $straightThrough");
msg("local index path: $indexLocal");
msg("counters: $cntfn");
msg("dest dir: $dest_dir");
msg("bowtie args: @ARGV");
msg("ls -al");
msg(`ls -al`);

my %counters = ();
Counters::getCounters($cntfn, \%counters, \&msg, 1);
msg("Retrived ".scalar(keys %counters)." counters from previous stages\n");

if($sam_passthru) {
	my $alsUnpaired = 0;
	my $alsPaired = 0;
	my $alsUnpairedTot = 0;
	my $line = "";
	my $skipped = 0;
	my $downloaded = 0;
	while(<STDIN>) {
		next if /^\s*FAKE\s*$/;
		next if /^\s*$/;
		$downloaded++;
		if($discardReads != 0 && rand() < $discardReads) {
			$skipped++; next;
		}
		# Tokenize preprocessed read line
		chomp;
		my @ts = split(/\t/, $_);
		$#ts == 2 || $#ts == 4 || die "Expected either 3 or 5 tokens, got:\n$_\n";
		# Tokenize read name
		my @ntok = split(/;/, $ts[0]);
		for(my $i = 0; $i <= $#ntok; $i++) {
			if($ntok[$i] =~ /^SM:/) {
				# Tokenize SAM alignment details
				my @stok = split(/,/, substr($ntok[$i], 3));
				$#stok == 4 || die "Expected 5 SAM alignment tokens, got:\n$_\n";
				my ($chr, $pos, $fw, $mapq, $cigar) = @stok;
				my $oms = ($mapq == 0 ? 1 : 0);
				length($cigar) > 0 || die "Expected CIGAR string of non-zero length:\n$_\n";
				my $part = $pos / $partlen;
				$fw eq "0" || $fw eq "1" || die "Bad SM:fw field: $fw\n$_\n";
				$fw = ($fw ? "+" : "-");
				my $mate = 0; # TODO: be smart about mates
				#$line = sprintf("%s\t%010d\t%010d\t$fw\t%s\t%s\t$oms\t$cigar\t$mate\t", $chr, $part, $pos, $ts[1], $ts[2]);
				# TODO: be smart about propagating some read and quality
				# information forward
				my $len = length($ts[0]);
				$line = sprintf("%s\t%010d\t%010d\t$fw\t$len\t$oms\t$cigar\t$mate\t", $chr, $part, $pos);
			}
		}
		$line =~ /[\n\r]/ && die "Bad whitespace in line:\n$line\n";
		my @ls = split(/\t/, $line);
		# what <- list("",          # Chr
		#              integer(0),  # Part
		#              integer(0),  # ChrOff
		#              "",          # Orient
		#              integer(0),  # SeqLen
		#              integer(0),  # Oms
		#              "",          # CIGAR
		#              "",          # Mate
		#              "")          # Lab
		$#ls == 8 || die "Expected 9 fields in SAM passthroughput output:\n$line\n";
		$ls[1] == int($ls[1]) || die "Expected 2nd field to be numeric:\n$line\n";
		$ls[2] == int($ls[2]) || die "Expected 3rd field to be numeric:\n$line\n";
		$ls[4] == int($ls[4]) || die "Expected 5th field to be numeric:\n$line\n";
		$ls[5] == int($ls[5]) || die "Expected 6th field to be numeric:\n$line\n";
		print "$line\n";
		$alsUnpairedTot++;
		if(++$alsUnpaired >= 10000) {
			counter("Bowtie,Alignments (unpaired) passed through from SAM,".$alsUnpaired);
			$alsUnpaired = 0;
		}
	}
	counter("Bowtie,Alignments (unpaired) passed through from SAM,".$alsUnpairedTot);
	counter("Bowtie,Alignments (paired) passed through from SAM,".$alsPaired);
	counter("Bowtie,Alignments passed through from SAM,".($alsUnpaired+$alsPaired));
	counter("Bowtie,Reads skipped,".$skipped);
	counter("Bowtie,Reads downloaded,".$downloaded);
	# Note: SAM passthrough mode doesn't require that -refjar, -jar,
	# -dstdir, bowtie, etc be specified
	exit 0;
}

$ref ne "" || $indexLocal ne "" || $test ||
	die "Neither -ref nor -index-local specified; must specify one\n";
$dest_dir = "." if $dest_dir eq "";

mkpath($dest_dir);
(-d $dest_dir) || die "-destdir $dest_dir does not exist or isn't a directory, and could not be created\n";

$bowtie = $bowtie_arg if $bowtie_arg ne "";
unless(-x $bowtie) {
	# No bowtie? die
	if($bowtie_arg ne "") {
		die "Specified -bowtie, \"$bowtie\" doesn't exist or isn't executable\n";
	} else {
		die "bowtie couldn't be found in BOWTIE_HOME, PATH, or current directory; please specify -bowtie\n";
	}
}
chmod 0777, $bowtie;

##
# Run bowtie, ensuring that index exists first.
#
my $jarEnsured = 0;
sub runBowtie($$) {
	my ($fn, $efn) = @_;
	my $args = join(" ", @ARGV);
	msg("  ...ensuring reference jar is installed first");
	my $index_base;
	if($indexLocal ne "") {
		$index_base = $indexLocal;
	} else {
		if($ref ne "" && !$jarEnsured) {
			Get::ensureFetched($ref, $dest_dir, \@counterUpdates);
			flushCounters();
			$jarEnsured = 1;
		}
		# Find all index file sets
		my @indexes = <$dest_dir/index/*.rev.1.ebwt>;
		for(my $i = 0; $i < scalar(@indexes); $i++) {
			# convert to basename
			$indexes[$i] =~ s/\.rev\.1\.ebwt$//;
		}
		if(scalar(@indexes) > 1) {
			# There was more than one index; pick the first one
			msg("Warning: More than one index base: @indexes");
			msg("ls -al $dest_dir/index");
			msg(`ls -al $dest_dir/index\n`);
			msg("Using $indexes[0]");
		} elsif(scalar(@indexes) == 0) {
			# There were no indexes; abort
			msg("Could not find any files ending in .rev.1.ebwt in $dest_dir/index:");
			msg("ls -al $dest_dir/index");
			msg(`ls -al $dest_dir/index\n`);
			die;
		}
		$index_base = "$indexes[0]";
	}
	# Check that all index files are present
	for my $i ("1", "2", "3", "4", "rev.1", "rev.2") {
		my $f = "$index_base.$i.ebwt";
		(-f $f) || die "Did not successfully install index file $f\n";
	}
	(-s "$index_base.1.ebwt" == -s "$index_base.rev.1.ebwt") ||
		die "Mismatched file sizes for .1.ebwt and rev.1.ebwt\n";
	(-s "$index_base.2.ebwt" == -s "$index_base.rev.2.ebwt") ||
		die "Mismatched file sizes for .2.ebwt and rev.2.ebwt\n";
	# Set up bowtie invocation
	my $cmd = "$bowtie $args --12 $fn $index_base 2>$efn";
	msg("Running: $cmd");
	return $cmd;
}

my $sthruCmd = ""; # command for bowtie in straight-through mode
my $efn = ".tmp.Align.pl.$$.err"; # bowtie stderr dump
if($straightThrough) {
	$sthruCmd = runBowtie("-", $efn);
	open OUT, "| $sthruCmd" || die "Could not open '| $sthruCmd' for writing";
} else {
	open OUT, ">.tmp.$$" || die "Could not open .tmp.$$ for writing";
}
my $records = 0;
my $downloaded = 0;
my $skipped = 0;
my $truncSkipped = 0;
my $pass = 0;
my $unpairedPass = 0;
my $pairedPass = 0;
my $matesSkipped = 0;
my $truncated = 0;

##
# q is a decoded solexa qual; return a decoded phred qual.
#
my @sol2phredMap = (
	 0,  1,  1,  1,  1,  1,  1,  2,  2,  3,  # -10
	 3,  4,  4,  5,  5,  6,  7,  8,  9, 10,  #   0
	10, 11, 12, 13, 14, 15, 16, 17, 18, 19,  #  10
);
sub sol2phred($) {
	my $q = shift;
	return 0 if $q < -10;
	return $sol2phredMap[$q + 10] if $q < 20;
	return $q;
}

##
# Argument is a quality string.  Update counters and convert to phred+33.
#
my %qualCnts = ();
my %rawQualCnts = ();
my $qualOff = $qual =~ /33$/ ? 33 : 64;
my $qualSol = $qual =~ /^solexa/i;
sub processQuals($) {
	my $qs = shift;
	my $ret = "";
	for(my $i = 0; $i < length($qs); $i++) {
		my $q = ord(substr($qs, $i, 1));
		$rawQualCnts{int($q/10)}++;
		$q -= $qualOff;
		$q = sol2phred($q) if $qualSol;
		$qualCnts{int($q/10)}++;
		$ret .= chr($q+33);
	}
	return $ret;
}

if($test) {
	$qualOff = 33;
	$qualSol = 0;
	my $q = processQuals("I");
	$q eq "I" || die;
	$qualOff = 64;
	$qualSol = 1;
	$q = processQuals('6789:;<=>?'.'@ABCDEFGHI'.'JKLMNOPQRS');
	$q eq q|!""""""##$$%%&&'()*++,-./01234| || die;
	$qualSol = 0;
	$q = processQuals('ABCDEFGHIJ');
	$q eq q|"#$%&'()*+| || die;
	msg("PASSED all tests");
	%qualCnts = ();
	%rawQualCnts = ();
}

# Shunt all of the input to a file
my %lens = ();
my $first = 1;
my $lastLine = "";
while(<STDIN>) {
	next if /^\s*FAKE\s*$/;
	next if /^\s*$/;
	msg("Read first line of stdin:\n$_") if $first;
	$first = 0;
	$lastLine = $_;
	chomp;
	$downloaded++;
	if($discardReads != 0 && rand() < $discardReads) {
		$skipped++; next;
	}
	my @altok = split(/\t/);
	scalar(@altok) == 3 || scalar(@altok) == 5 || die "Bad number of read tokens ; expected 3 or 5:\n$_\n";
	my $pe = (scalar(@altok) == 5);
	my $len1 = length($altok[1]);
	my $len2 = 0;
	if($pe) {
		if($discardMate > 0) {
			if($discardMate == 1) {
				# First mate is discarded, second is promoted to the
				# first slot
				$altok[1] = $altok[3];
				$altok[2] = $altok[4];
				$len1 = length($altok[1]);
			} else {
				# Second mate is discarded by virtue of $pe = 0
			}
			$matesSkipped++;
			$pe = 0;
			# $len2 remains =0
		} else {
			# Mate is intact; tally its length
			$len2 = length($altok[3]); $lens{$len2}++;
		}
	}
	$lens{$len1}++;
	# Is it so small that we should discard it?
	if($truncate > 0 && $discardSmall &&
	   ($len1 < $truncate || ($len2 > 0 && $len2 < $truncate)))
	{
		# Yes, discard
		$truncSkipped++;
		next;
	}
	# Print alignment after truncating it
	my $nlen1 = $len1;
	$nlen1 = min($truncate, $len1) if $truncate > 0;
	$truncated++ if ($nlen1 < $len1);
	if($pe) {
		my $nlen2 = $len2;
		$nlen2 = min($truncate, $len2) if $truncate > 0;
		$truncated++ if ($nlen2 < $len2);
		my ($nm, $s1, $q1, $s2, $q2) = (@altok);
		($q1, $q2) = (processQuals($q1), processQuals($q2));
		$pass++; $pairedPass++;
		print OUT "r\t".
			substr($s1, 0, $nlen1)."\t".
			substr($q1, 0, $nlen1)."\t".
			substr($s2, 0, $nlen2)."\t".
			substr($q2, 0, $nlen2)."\n";
	} else {
		$pass++; $unpairedPass++;
		my ($nm, $s1, $q1) = (@altok);
		$q1 = processQuals($q1);
		print OUT "r\t".
			substr($s1, 0, $nlen1)."\t".
			substr($q1, 0, $nlen1)."\n";
	}
	$records++;
}
msg("Read last line of stdin:\n$lastLine");
msg("$records reads downloaded\n");
counter("Bowtie,Reads downloaded,$downloaded");
counter("Bowtie,Reads (all) passing filters,$pass");
counter("Bowtie,Reads (unpaired) passing filters,$unpairedPass");
counter("Bowtie,Reads (paired) passing filters,$pairedPass");
counter("Bowtie,Reads skipped due to -discard-reads,$skipped");
counter("Bowtie,Reads skipped due to -truncate-discard,$truncSkipped");
counter("Bowtie,Mates skipped due to -discard-mate,$matesSkipped");
counter("Bowtie,Reads (mates) truncated due to -truncate*,$truncated");
for my $len (keys %lens) {
	counter("Bowtie,Reads of length $len,$lens{$len}");
}
for my $qual (keys %rawQualCnts) {
	counter("Bowtie,Occurrences of raw quality value [".($qual*10).":".($qual*10+10)."),$rawQualCnts{$qual}");
}
for my $qual (keys %qualCnts) {
	counter("Bowtie,Occurrences of phred-33 quality value [".($qual*10).":".($qual*10+10)."),$qualCnts{$qual}");
}
close(OUT);
if($straightThrough) {
	if($? != 0) {
		msg("Fatal error: Bowtie exited with level $?:");
		open(EFN, "$efn") || die "Could not open '$efn' for reading\n";
		while(<EFN>) { msg($_); }
		close(EFN);
		die;
	}
}
msg("$downloaded reads downloaded");

if($records > 0 && !$straightThrough) {
	counter("Bowtie,Reads downloaded,$downloaded");
	# Print a bit of the reads file, for sanity-checking purposes
	my $fn = ".tmp.$$";
	msg("head -4 $fn:");
	msg(`head -4 $fn`);
	msg("tail -4 $fn:");
	msg(`tail -4 $fn`);
	my $cmd = runBowtie($fn, $efn);
	my $ret = Util::run($cmd);
	if($ret != 0) {
		msg("Fatal error: Bowtie exited with level $?:");
		open(EFN, "$efn") || die "Could not open '$efn' for reading\n";
		while(<EFN>) { msg($_); }
		close(EFN);
		die;
	}
	unlink($fn);
}
if($records > 0) {
	open SUMM, $efn || die "Could not open $efn for reading\n";
	while(<SUMM>) {
		if(/reads with at least one reported alignment/) {
			/: ([0-9]+)/;
			my $num = $1;
			$num == int($num) || die "Expected number: $num\n$_";
			counter("Bowtie,Reads with at least 1 reported alignment,$num");
		} elsif(/reads that failed to align/) {
			/: ([0-9]+)/;
			my $num = $1;
			$num == int($num) || die "Expected number: $num\n$_";
			counter("Bowtie,Reads that failed to align,$num");
		} elsif(/reads with alignments suppressed due to -m/) {
			/: ([0-9]+)/;
			my $num = $1;
			$num == int($num) || die "Expected number: $num\n$_";
			counter("Bowtie,Reads with alignments suppressed due to -m,$num");
		} elsif(/reads with alignments sampled due to -M/) {
			/: ([0-9]+)/;
			my $num = $1;
			$num == int($num) || die "Expected number: $num\n$_";
			counter("Bowtie,Reads with alignments sampled due to -M,$num");
		}
	}
	close(SUMM);
	unlink($efn);
	msg("$records reads aligned");
}
print "FAKE\n";
counter("Bowtie,Reads given to Bowtie,$records");
