#!/usr/bin/perl -w

##
# Soapsnp.pl
#
# Batch alignments streaming in on STDIN and send them to SOAPsnp.
# Alignments are binned by partition and sorted by reference offset.
# Fetch reference jar (ensuring mutual exclusion among reducers) if
# necessary.
#
#  Author: Ben Langmead
#    Date: February 11, 2010
#

use strict;
use warnings;
use 5.004;
use Carp;
use Getopt::Long;
use FindBin qw($Bin); 
use lib $Bin;
use Counters;
use Get;
use Tools;
use Util;
use AWS;
use File::Path qw(mkpath);

{
	# Force stderr to flush immediately
	my $ofh = select STDERR;
	$| = 1;
	select $ofh;
}

my @counterUpdates = ();

sub msg($) {
	my $m = shift;
	$m =~ s/\s*$//;
	defined($m) || croak("Undefined message");
	print STDERR "Soapsnp.pl: $m\n";
}

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
my $type = "s3";
my $file = "";
my $dest_dir = "";

my $soapsnp = "";
my $soapsnp_arg = "";

my $plen = 2000000;
my $args = "";
my $refdir = "";
my $snpdir = "";
my $haploidstr = "";
my $dryRun = 0;
my $baseQual = '!';
my $diploid_args = "";
my $haploid_args = "";
my $replaceUnderscores = 0;
my $discardRefBins = 0;
my $cntfn = "";

Tools::initTools();
$soapsnp = Tools::lookFor("soapsnp", "SOAPSNP_HOME", "soapsnp");

sub dieusage {
	my $msg = shift;
	my $exitlevel = shift;
	$exitlevel = $exitlevel || 1;
	print STDERR "$msg\n";
	exit $exitlevel;
}

GetOptions (
	"soapsnp:s"          => \$soapsnp_arg,
	"s3cmd:s"            => \$Tools::s3cmd_arg,
	"s3cfg:s"            => \$Tools::s3cfg,
	"jar:s"              => \$Tools::jar_arg,
	"accessid:s"         => \$AWS::accessKey,
	"secretid:s"         => \$AWS::secretKey,
	"hadoop:s"           => \$Tools::hadoop_arg,
	"wget:s"             => \$Tools::wget_arg,
	"refjar:s"           => \$ref,
	"destdir:s"          => \$dest_dir,
	"refdir:s"           => \$refdir,
	"snpdir:s"           => \$snpdir,
	"partition=i"        => \$plen,
	"args:s"             => \$args,
	"diploid_args:s"     => \$diploid_args,
	"haploid_args:s"     => \$haploid_args,
	"haploids:s"         => \$haploidstr,
	"dryrun"             => \$dryRun,
	"replace-uscores"    => \$replaceUnderscores,
	"counters:s"         => \$cntfn,
	"basequal=s"         => \$baseQual,
	"discard-ref-bins=f" => \$discardRefBins) || dieusage("Bad option", 1);

if($replaceUnderscores) {
	$args =~ s/_/ /g;
	$diploid_args =~ s/_/ /g;
	$haploid_args =~ s/_/ /g;
}

$diploid_args = "-r 0.00005 -e 0.0001" if $diploid_args eq "";
$haploid_args = "-r 0.0001" if $haploid_args eq "";
$haploid_args .= " -m";

msg("soapsnp: found: $soapsnp, given: $soapsnp_arg");
msg("s3cmd: found: $Tools::s3cmd, given: $Tools::s3cmd_arg");
msg("jar: found: $Tools::jar, given: $Tools::jar_arg");
msg("hadoop: found: $Tools::hadoop, given: $Tools::hadoop_arg");
msg("wget: found: $Tools::wget, given: $Tools::wget_arg");
msg("s3cfg: $Tools::s3cfg");
msg("soapsnp args: $args");
msg("refdir: $refdir");
msg("snpdir: $snpdir");
msg("partition length: $plen");
msg("haploid ids: $haploidstr");
msg("haploid arguments: $haploid_args");
msg("diploid arguments: $diploid_args");
msg("base quality value: $baseQual");
msg("discard SNP bins: $discardRefBins");
msg("dryrun: $dryRun");
msg("ls -al");
print STDERR `ls -al`;

my %counters = ();
Counters::getCounters($cntfn, \%counters, \&msg, 1);
msg("Retrived ".scalar(keys %counters)." counters from previous stages\n");

msg("Checking that 1 or more reads aligned in Align stage...");
my $alreps = defined($counters{"Bowtie"}{"Reads with at least 1 reported alignment"});
if(defined($alreps) && $alreps > 0) {
	msg("Yes, $alreps alignments reported in Align stage");
} else {
	msg("Warning: no alignments reported in Align stage; SOAPsnp will not be run");
}

$refdir ne "" || $ref ne "" || die "Must specify either -refdir <path> or -ref <url> and -destdir\n";
$refdir ne "" || $dest_dir ne "" || die "Must specify either -refdir <path> or -ref <url> and -destdir\n";
$snpdir ne "" || $ref ne "" || die "Must specify either -snpdir <path> or -ref <url> and -destdir\n";
$snpdir ne "" || $dest_dir ne "" || die "Must specify either -snpdir <path> or -ref <url> and -destdir\n";
$refdir = "$dest_dir/sequences" if $refdir eq "";
$snpdir = "$dest_dir/snps" if $snpdir eq "";
$dest_dir eq "" || (-d $dest_dir) || mkpath($dest_dir);
$dest_dir eq "" || (-d $dest_dir) || die "-destdir $dest_dir does not exist or isn't a directory, and could not be created\n";

$soapsnp = $soapsnp_arg if $soapsnp_arg ne "";
if(! -x $soapsnp) {
	if($soapsnp_arg ne "") {
		die "Specified -soapsnp, \"$soapsnp\" doesn't exist or isn't executable\n";
	} else {
		die "soapsnp couldn't be found in SOAPSNP_HOME, PATH, or current directory; please specify -soapsnp\n";
	}
}
chmod 0777, $soapsnp;

my $lchr = -1;
my $lpart = -1;
my $als = 0;
my $alstot = 0;
my $ranges = 0;

# Record which chromosomes are haploid; assume all others are diploid
my %hapHash = ();
if($haploidstr ne "none" && $haploidstr ne "all") {
	my @haploids = split /[,]/, $haploidstr;
	for my $h (@haploids) { $hapHash{$h} = 1; }
}

sub lookAtFile() {
	my $f = shift;
	msg("ls -l $snpdir/chr$lchr.snps");
	print STDERR `ls -l $snpdir/chr$lchr.snps`;
}

##
# Flush quality range counters.
#
my %qualCnts = ();
my $qualCntsToFlush = 0;
sub flushQualCounters() {
	for my $qual (keys %qualCnts) {
		counter("SOAPsnp,Occurrences of quality value [".($qual*10).":".($qual*10+10).") in wrapper,$qualCnts{$qual}");
		$qualCnts{$qual} = 0;
	}
	$qualCntsToFlush = 0;
}

##
# Argument is a quality string.  Update counters.
#
sub processQuals($) {
	for(my $i = 0; $i < length($_[0]); $i++) {
		my $q = ord(substr($_[0], $i, 1))-33;
		$qualCnts{int($q/10)}++;
		$qualCntsToFlush++;
	}
	flushQualCounters() if $qualCntsToFlush > 10000;
}

my $maxlen = 1; # per-partition maximum read length
open TMP, ">.tmp.$plen.0" || die;
my $jarEnsured = 0;
while(1) {
	# Extract the chromosome and partition key
	my $line = <STDIN>;
	if(defined($line)) {
		next if substr($line, 0, 1) eq '#';
		next if $line =~ /^\s*FAKE\s*$/;
		next if $line =~ /^\s*$/;
	}
	my $chromo;
	my $parti;
	my $lmaxlen = $maxlen;
	if(defined($line)) {
		# Parse chromosome and partition for this alignment
		my @s = split(/[\t]/, $line);
		($chromo, $parti) = ($s[0], $s[1]);
		my $len = length($s[4]);
		processQuals($s[5]);
		if($parti != $lpart || $chromo != $lchr) {
			# New partition so start a separate tally
			$maxlen = $len;
		} else {
			$maxlen = $len if $len > $maxlen;
		}
	} else {
		# No more input; last partition was final
		print STDERR "Read the last line of input\n";
		last if $als == 0; # bail if there are no alignments to flush
		$parti = $lpart+1; # force alignments to flush
	}
	# If either the partition or the chromosome is different...
	if($parti != $lpart || $chromo != $lchr) {
		close(TMP);
		# If there are any alignments to flush...
		if($als > 0) {
			if($discardRefBins == 0 || rand() > $discardRefBins) {
				#
				# Set up range based on partition id and partition length
				#
				my $irange = $plen * int($lpart);
				my $frange = $irange + $plen;
				my $rname = ".range_".$irange."_$frange";
				$ranges++;
				open RANGE, ">$rname" || die;
				print RANGE "$lchr\t$irange\t$frange\n";
				close(RANGE);
				
				counter("SOAPsnp wrapper,Ranges processed,1");
				counter("SOAPsnp wrapper,Alignments processed,$als");
				
				#
				# Run SOAPsnp
				#
				my $date = `date`; chomp($date);
				msg("Genotyping chromosome $lchr $irange-$frange using $als alignments: $date");
				my $ploid = $diploid_args;
				if(defined($hapHash{$lchr}) || $haploidstr eq "all") {
					msg("  chromosome $lchr is haploid; using args \"$haploid_args\"");
					$ploid = $haploid_args;
				} else {
					msg("  chromosome $lchr is diploid; using args \"$diploid_args\"");
				}
				
				msg("head -4 .tmp.$plen.$lpart:");
				print STDERR `head -4 .tmp.$plen.$lpart`;
				msg("tail -4 .tmp.$plen.$lpart:");
				print STDERR `tail -4 .tmp.$plen.$lpart`;
				if($ref ne "" && !$jarEnsured) {
					Get::ensureFetched($ref, $dest_dir, \@counterUpdates);
					flushCounters();
					$jarEnsured = 1;
					unless(-d "$dest_dir/sequences") {
						msg("Extracting jar didn't create 'sequences' subdirectory.");
						msg("find $dest_dir");
						print STDERR `find $dest_dir`;
						exit 1;
					}
				}
				
				my $snpsArg = "";
				my $snpsFn = "$snpdir/chr$lchr.snps";
				if(! -f $snpsFn) {
					$snpsFn = "$snpdir/$lchr.snps";
					if(! -f $snpsFn) {
						counter("SOAPsnp wrapper,SNP files missing,1");
						msg("Warning: $snpsFn doesn't exist");
						msg("ls -l $snpdir");
						msg(`ls -l $snpdir`);
						$snpsFn = "";
					}
				}
				if($snpsFn ne "") {
					msg("ls -l $snpsFn");
					msg(`ls -l $snpsFn`);
					$snpsArg = "-s $snpsFn";
				} else {
					msg("Warning: neither $snpdir/chr$lchr.snps nor $snpdir/$lchr.snps exist; not using known SNPs");
					msg("ls -al $snpdir");
					msg(`ls -al $snpdir`);
				}
				
				my $refFn = "$refdir/chr$lchr.fa";
				if(! -f $refFn) {
					$refFn = "$refdir/$lchr.fa";
					if(! -f $refFn) {
						counter("SOAPsnp wrapper,Sequence files missing,1");
						msg("Fatal error: $refFn doesn't exist");
						msg("ls -l $refdir");
						print STDERR `ls -l $refdir`;
						die;
					}
				}
				
				my $partFn = ".tmp.$plen.$lpart";
				if(! -f $partFn) {
					counter("SOAPsnp wrapper,Alignment files missing,1");
					msg("Warning: $partFn doesn't exist");
					msg("ls -al");
					print STDERR `ls -al`;
				}
				
				my $cmd = "${soapsnp} ".
				          "-i $partFn ". # alignments
				          "-d $refFn ". # reference sequence
				          "-o .tmp.snps ". # output file
				          "$snpsArg ". # known SNP file
				          "-z '$baseQual' ". # base quality value
				          "-L $lmaxlen ". # maximum read length
				          "-c ". # Crossbow
				          "-H ". # Hadoop output
				          "-T $rname ". # region
				          "$ploid ". # ploidy/rate args
				          "$args ". # other arguments
				          ">.soapsnp.$$.stdout ".
				          "2>.soapsnp.$$.stderr";
				msg("$cmd");
	
				my $ret = $dryRun ? 0 : system($cmd);
	
				msg("soapsnp returned $ret");
				msg("command: $cmd");
				open OUT, ".soapsnp.$$.stdout";
				msg("stdout from soapsnp:");
				while(<OUT>) { print STDERR $_; } close(OUT);
				open ERR, ".soapsnp.$$.stderr";
				msg("stderr from soapsnp:");
				while(<ERR>) { print STDERR $_; } close(ERR);
				msg("range: $lchr\t$irange\t$frange");
	
				msg("head -4 .tmp.snps:");
				print STDERR `head -4 .tmp.snps`;
				msg("tail -4 .tmp.snps:");
				print STDERR `tail -4 .tmp.snps`;
	
				die "Dying following soapsnp returning non-zero $ret" if $ret;
				
				#
				# Read and print called SNPs
				#
				my $snpsreported = 0;
				open SNPS, ".tmp.snps";
				while(<SNPS>) {
					chomp;
					my @ss = split(/\t/);
					my $known = $ss[0] eq 'K';
					shift @ss if $known;
					my $snpoff = $ss[1];
					$snpoff == int($snpoff) || die "SNP offset isn't a number: $snpoff";
					if($snpoff < $irange || $snpoff >= $frange) {
						counter("SOAPsnp wrapper,Out-of-range SNPs trimmed,1");
						msg("Skipping $snpoff because it's outside [$irange, $frange) $_");
						next;
					}
					$ss[1] = sprintf "%011d", $snpoff;
					print "K\t" if $known;
					print join("\t", @ss)."\n";
					$snpsreported++;
				}
				close(SNPS);
				counter("SOAPsnp wrapper,SNPs reported,$snpsreported");
				msg("Reported $snpsreported SNPs");
			} else {
				counter("SOAPsnp wrapper,SNP bins skipped,1");
				msg("Skipped bin with $als alignments");
			}
			$als = 0;
		}
		open TMP, ">.tmp.$plen.$parti" || die;
		$lpart = $parti;
		$lchr = $chromo;
	}
	last unless defined($line);
	print TMP "$line";
	$als++;
	$alstot++;
}
counter("SOAPsnp,0-range invocations,1") if $ranges == 0;
counter("SOAPsnp,0-alignment invocations,1") if $alstot == 0;
close(TMP);
flushQualCounters();
flushCounters() if scalar(@counterUpdates) > 0;
