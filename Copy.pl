#!/usr/bin/perl -w

##
# Copy.pl
#
#  Authors: Michael C. Schatz & Ben Langmead
#     Date: 6/26/2009
#
# Mapper for Crossbow bulk copies of FASTQ/SAM/BAM reads.
#

use strict;
use warnings;
use Getopt::Long;
use POSIX ":sys_wait_h";
use FindBin qw($Bin); 
use lib $Bin;
use Counters;
use Get;
use Util;
use AWS;
use Tools;
use File::Basename;
use File::Path qw(mkpath rmtree);

{
	# Force stderr to flush immediately
	my $ofh = select STDERR;
	$| = 1;
	select $ofh;
}

my %delayedCounters = ();

sub counter($) {
	my $c = shift;
	print STDERR "reporter:counter:$c\n";
}

sub flushDelayedCounters($) {
	my $name = shift;
	for my $k (keys %delayedCounters) {
		counter("$name,$k,$delayedCounters{$k}");
		delete $delayedCounters{$k};
	}
}

my $compress = "gzip";
my $push = "";
my $helpflag = undef;
my $skipfirst = undef;
my $owner = undef;
my $stopAfter = 0;
my $maxPerFile = 0;
my $keep = 0;
my $verbose = 0;
my $labReadGroup = 0;
my $cntfn = "";

sub msg($) {
	my $m = shift;
	return unless defined($m);
	$m =~ s/[\r\n]*$//;
	print STDERR "Copy.pl: $m\n";
}

Tools::initTools();

GetOptions(
	"compress:s"   => \$compress,
	"push:s"       => \$push,
	"samtools:s"   => \$Tools::samtools_arg,
	"s3cmd:s"      => \$Tools::s3cmd_arg,
	"s3cfg:s"      => \$Tools::s3cfg,
	"md5:s"        => \$Tools::md5_arg,
	"accessid:s"   => \$AWS::accessKey,
	"secretid:s"   => \$AWS::secretKey,
	"hadoop:s"     => \$Tools::hadoop_arg,
	"stop:i"       => \$stopAfter,
	"maxperfile:i" => \$maxPerFile,
	"keep"         => \$keep,
	"h"            => \$helpflag,
	"s"            => \$skipfirst,
	"owner:s"      => \$owner,
	"label-rg"     => \$labReadGroup,
	"counters:s"   => \$cntfn,
	"verbose"      => \$verbose)
	|| die "GetOptions failed\n";

my %counters = ();
Counters::getCounters($cntfn, \%counters, \&msg, 1);
msg("Retrived ".scalar(keys %counters)." counters from previous stages");

my $ws = 0;

$labReadGroup = 0 unless ($labReadGroup);
$stopAfter = 0 unless($stopAfter);
$maxPerFile = 500000 unless($maxPerFile);

my $firstEnsureS3cmd = 1;
my $s3cmdHasListMD5 = 1;

# Reverse any capitalization we may have done in cb.pl
$push =~ s/^S3N/s3n/;
$push =~ s/^S3/s3/;
$push =~ s/^HDFS/hdfs/;

if ($push =~ /^s3/) {
	msg("Checking availability of s3cmd") if $verbose;
	Tools::ensureS3cmd();
} else {
	msg("s3cmd not needed") if $verbose;
}

my $unpaired = 0;
my $paired = 0;
my $totunpaired = 0;
my $totpaired = 0;

if (defined $owner && $push ne "") {
	my $hadoop = Tools::hadoop();
	msg("Creating destination directory and setting owner") if $verbose;
	Util::run("$hadoop fs -mkdir $push");
	Util::run("$hadoop fs -chown $owner $push >&2");
}

##
# Calculate the md5 hash of an object in S3 using s3cmd.
#
sub s3md5($) {
	my $path = shift;
	my $s3cmd = Tools::s3cmd();
	$s3cmdHasListMD5 = system("$s3cmd ls --list-md5 >/dev/null 2>&1") == 0;
	return "" unless $s3cmdHasListMD5;
	$path = Get::s3cmdify($path);
	my $md = `$s3cmd --list-md5 ls $path | awk '{print \$4}'`;
	chomp($md);
	length($md) == 32 || die "Bad MD5 obtained from s3: $md\n";
	return $md;
}

##
# Push a file from the local filesystem to another filesystem (perhaps
# HDFS, perhaps S3) using hadoop fs -cp.
#
sub pushBatch($) {
	my $file = shift;
	-e $file || die "No such file $file";
	$push ne "" || die "pushBatch() called but no destination is set";
	my $pushDest = "local filesystem";
	if   ($push =~ /^hdfs:/i) { $pushDest = "HDFS"; }
	elsif($push =~ /^s3n?:/i) { $pushDest = "S3"; }
	counter("Short read preprocessor,Read files pushed to $pushDest,".(-s $file));

	if($compress eq "bzip2" || $compress eq "bz2") {
		Util::runAndWait("bzip2 $file >&2", "bzip2") == 0 || die "bzip2 command failed";
		$file .= ".bz2";
		-e $file || die "No such file $file after bzip2 compression";
	} elsif($compress eq "gzip" || $compress eq "gz") {
		Util::runAndWait("gzip $file >&2", "gzip") == 0 || die "gzip command failed";
		$file .= ".gz";
		-e $file || die "No such file $file after compression";
	} elsif($compress eq "none") {
		## nothing to do
	} elsif($compress ne "") {
		die "Did not recognize compression type $compress";
	}
	-e $file || die "No such file $file";

	my $md5 = Tools::md5();
	my $md = Util::trim(Util::backtickRun("cat $file | $md5 | cut -d' ' -f 1"));
	length($md) == 32 || die "Bad MD5 calculated locally: $md";

	if ($push =~ /^hdfs:/i) {
		my $hadoop = Tools::hadoop();
		Util::runAndWait("$hadoop fs -put $file $push >&2", "hadoop fs -put") == 0 ||
			die "hadoop fs -put command failed";
		if (defined $owner) {
			Util::run("$hadoop fs -chown $owner $push/$file >&2") == 0 ||
				die "hadoop fs -chown command failed";
		}
	} elsif($push =~ /^s3n?:/i) {
		my $s3cmd = Tools::s3cmd();
		# For s3cmd, change s3n -> s3 and remove login info
		my $s3cmd_push = Get::s3cmdify($push);
		my $cmd = "$s3cmd put $file $s3cmd_push/$file >&2";
		Util::run($cmd) == 0 || die "Command failed: $cmd";
		my $rmd5 = s3md5("$push/$file");
		$rmd5 eq "" || $md eq $rmd5 || die "Local MD5 $md does not equal S3 md5 $rmd5 for file $s3cmd_push/$file";
	} else {
		$push .= "/" unless $push =~ /\/$/;
		mkpath($push);
		(-d $push) || die "Could not create -push destination directory $push\n";
		my $cmd = "cp $file $push >&2 2>/dev/null";
		Util::run($cmd) == 0 || die "Command failed: $cmd";
	}

	counter("Short read preprocessor,Read data pushed to $pushDest (compressed),".(-s $file)) if $compress ne "";
}

## Download a file with wget
sub wget($$$) {
	my ($fname, $url, $md) = @_;
	my $rc = Util::run("wget -O $fname $url >&2");
	die "wget failed: $url $rc\n" if $rc;
}

## Download a file with hadoop fs -get
sub hadoopget($$$) {
	my ($fname, $url, $md) = @_;
	my $hadoop = Tools::hadoop();
	my $rc = Util::runAndWait("$hadoop fs -get $url $fname >&2", "hadoop fs -get");
	die "hadoop get failed: $url $rc\n" if $rc;
}

## Download a file with s3cmd get
sub s3get($$$) {
	my ($fname, $url, $md) = @_;
	my $s3cmd = Tools::s3cmd();
	$url = Get::s3cmdify($url);
	my $rc = Util::run("$s3cmd get $url $fname >&2");
	die "s3cmd get failed: $url $rc\n" if $rc;
}

## Fetch a file
sub fetch($$$) {
	my ($fname, $url, $md) = @_;
	defined($md) || die;
	msg("Fetching $url $fname $md");

	if(! -f $fname) {
		if ($url =~ /^hdfs:/) { hadoopget($fname, $url, $md); }
		elsif ($url =~ /^s3n?:/) { s3get($fname, $url, $md); }
		elsif ($url =~ /^ftp:/ || $url =~ /^https?:/) { wget($fname, $url, $md); }
		elsif ($url ne $fname) { Util::run("cp $url ./$fname >&2"); }
		-f $fname || die "Failed to copy $url to $fname\n";
		(-s $fname) > 0 || die "File obtained from URL $url was empty; bad URL?\n";
	
		if ($md ne "0") {
			my $md5 = Tools::md5();
			my $omd5 = `cat $fname | $md5 | cut -d' ' -f 1`;
			chomp($omd5);
			$omd5 eq $md || die "MD5 mismatch for $fname; expected \"$md\", got \"$omd5\"";
			counter("Short read preprocessor,MD5s checked,2");
		}
	}

	counter("Short read preprocessor,Read data fetched,".(-s $fname));
	
	my $newfname = $fname;
	if($fname =~ /\.gz$/ || $fname =~ /\.gzip$/) {
		$newfname =~ s/\.gzi?p?$//;
		Util::runAndWait("gzip -dc $fname > $newfname", "gzip -dc") == 0 || die "Error while gunzipping $fname";
		counter("Short read preprocessor,Read data fetched (uncompressed),".(-s $newfname));
		counter("Short read preprocessor,Read data fetched (un-gzipped),".(-s $newfname));
	} elsif($fname =~ /\.bz2$/ || $fname =~ /\.bzip2$/)  {
		$newfname =~ s/\.bzi?p?2$//;
		Util::runAndWait("bzip2 -dc $fname > $newfname", "bzip2 -dc") == 0 || die "Error while bzip2 decompressing $fname";
		counter("Short read preprocessor,Read data fetched (uncompressed),".(-s $newfname));
		counter("Short read preprocessor,Read data fetched (un-bzip2ed),".(-s $newfname));
	} elsif($fname =~ /\.bam$/) {
		my $samtools = Tools::samtools();
		$newfname =~ s/\.bam$/.sam/;
		Util::runAndWait("$samtools view $fname > $newfname", "samtools") == 0 ||
			die "Error performing BAM-to-SAM $fname";
		counter("Short read preprocessor,Read data fetched (uncompressed),".(-s $newfname));
		counter("Short read preprocessor,Read data fetched (BAM-to-SAM),".(-s $newfname));
	} elsif($fname =~ /\.sra$/) {
		my $sra_conv = Tools::sra();
		$newfname =~ s/\.sra$/.fastq/;
		mkpath("./sra_tmp");
		Util::runAndWait("$sra_conv $fname -O ./sra_tmp > /dev/null", "fastq-dump") == 0 ||
			die "Error performing SRA-to-FASTQ $fname";
		Util::runAndWait("cat ./sra_tmp/* > $newfname", "cat") == 0 ||
			die "Error copying resuld of SRA-to-FASTQ $fname";
		counter("Short read preprocessor,Read data fetched (uncompressed),".(-s $newfname));
		counter("Short read preprocessor,Read data fetched (un-SRAed),".(-s $newfname));
		rmtree("./sra_tmp");
	}
	return $newfname;
}

##
# Utility function that returns the reverse complement of its argument
#
sub revcomp($$) {
	my ($r, $color) = @_;
	$r = reverse($r);
	$r =~ tr/aAcCgGtT/tTgGcCaA/ unless $color;
	return $r;
}

my ($name, $seq, $qual, $readGroup) = (undef, undef, undef, undef);
my $rtot = 0;

##
# Parse optional fields from a SAM record.
#
sub parseSAMOptionals($$) {
	my ($opts, $hash) = @_;
	my @ops = split(/\s+/, $opts);
	for my $o (@ops) {
		my @co = split(/:/, $o);
		$#co >= 2 || die;
		my ($nm, $ty) = ($co[0], $co[1]);
		shift @co;
		shift @co;
		$hash->{"$nm:$ty"} = join(":", @co);
	}
}

##
# Parse a record out of a SAM input file.
#
sub parseSAM($$) {
	my ($fh, $color) = @_;
	my $samLine = <$fh>;
	unless(defined($samLine)) {
		$name = undef;
		return;
	}
	chomp($samLine);
	my @stok = split(/\t/, $samLine);
	defined($stok[10]) || die "Malformed SAM line; not enough tokens:\n$samLine\n";
	($name, $seq, $qual) = ($stok[0], $stok[9], $stok[10]);
	my ($flags,   $chr,     $pos,     $mapq,    $cigar) =
	   ($stok[1], $stok[2], $stok[3], $stok[4], $stok[5]);
	$flags == int($flags) || die "SAM flags field must be an integer; was $flags\n$samLine\n";
	my $fw = ($flags & 16) == 0;
	if($fw) {
		$seq = revcomp($seq, $color);
		$qual = reverse $qual;
	}
	$fw = ($fw ? 1 : 0);
	my %opts;
	my $optstr = "";
	for(my $i = 11; $i <= $#stok; $i++) {
		$optstr .= " " if $optstr ne "";
		$optstr .= $stok[$i];
	}
	parseSAMOptionals($optstr, \%opts);
	if($labReadGroup && defined($opts{"RG:Z"})) {
		$readGroup = $opts{"RG:Z"};
	} elsif($labReadGroup) {
		$ws++;
		msg("No read group for read $name\n$samLine\n$_");
		die;
		$readGroup = "no-group";
	} else {
		$readGroup = undef;
	}
	$name =~ s/\s.*//;
	$name = "RN:$name;SM:$chr,$pos,$fw,$mapq,$cigar";
}

##
# Parse a record out of a FASTQ input file.
#
sub parseFastq($$) {
	my ($fh, $color) = @_;
	$name = <$fh>;
	return unless defined($name);
	chomp($name);
	$seq = <$fh>;
	unless(defined($seq))   { $name = undef; return; }
	chomp($seq);
	my $name2 = <$fh>;
	unless(defined($name2)) { $name = undef; return; }
	$qual = <$fh>;
	unless(defined($qual))  { $name = undef; return; }
	chomp($qual);
	$name =~ s/\s.*//;
	$name = "RN:$name";
}

##
# Parse a record from an input file.  Could be many lines.
#
sub parseRead($$$) {
	my ($fh, $sam, $color) = @_;
	if($sam) {
		parseSAM($fh, $color);
	} else {
		parseFastq($fh, $color);
	}
}

##
# Handle the copy for a single unpaired entry
#
sub doUnpairedUrl($$$$$) {
	my ($url, $md, $lab, $format, $color) = @_;
	my @path = split /\//, $url;
	my $fn = $path[-1];
	my $of;
	my $sam = $format =~ /^sam$/i;
	if(defined($lab)) {
		$lab =~ /[:\s]/ && die "Label may not contain a colon or whitespace character; was \"$lab\"\n";
	}
	
	# fetch the file
	my $origFn = $fn;
	$fn = fetch($fn, $url, $md);
	
	# turn FASTQ entries into single-line reads
	my $fh;
	open($fh, $fn) || die "Could not open input file $fn";
	my $r = 0;
	my $fileno = 1;
	open($of, ">${fn}_$fileno.out") || die "Could not open output file ${fn}_$fileno.out";
	my $fn_nospace = $fn;
	$fn_nospace =~ s/[\s]+//g;
	my $rname = "FN:".$fn_nospace; # Add filename
	while(1) {
		last if($stopAfter != 0 && $rtot >= $stopAfter);
		parseRead($fh, $sam, $color);
		last unless(defined($name));
		my $fullname = $rname;
		if($labReadGroup) {
			defined($readGroup) || die;
			$fullname .= ";LB:$readGroup";
			$delayedCounters{"Unpaired reads with label $readGroup"}++;
		} elsif(defined($lab)) {
			$fullname .= ";LB:$lab";
			$delayedCounters{"Unpaired reads with label $lab"}++;
		}
		$fullname .= ";$name";
		print $of "$fullname\t$seq\t$qual\n";
		$r++; $rtot++;
		if($maxPerFile > 0 && ($r % $maxPerFile) == 0) {
			close($of);
			if($push ne "") {
				pushBatch("${fn}_$fileno.out");
				system("rm -f ${fn}_$fileno.out ${fn}_$fileno.out.* >&2");
			}
			$fileno++;
			open($of, ">${fn}_$fileno.out") || die "Could not open output file ${fn}_$fileno.out";
		}
		$totunpaired++;
		if(++$unpaired >= 100000) {
			counter("Short read preprocessor,Unpaired reads,$unpaired");
			$unpaired = 0;
		}
	}
	counter("Short read preprocessor,Unpaired reads,$unpaired");
	close($fh);
	close($of);
	flushDelayedCounters("Short read preprocessor");

	# Remove input file
	system("rm -f $fn $origFn >&2") unless $keep;
	if($push ne "") {
		# Push and remove output files
		pushBatch("${fn}_$fileno.out");
		system("rm -f ${fn}_$fileno.out ${fn}_$fileno.out.* >&2");
	} else {
		# Just keep the output files around
	}
}

##
# Handle the copy for a single paired entry
#
sub doPairedUrl($$$$$$$) {
	my ($url1, $md51, $url2, $md52, $lab, $format, $color) = @_;
	my @path1 = split /\//, $url1;
	my @path2 = split /\//, $url2;
	my ($fn1, $fn2) = ($path1[-1], $path2[-1]);
	my $origFn1 = $fn1;
	my $origFn2 = $fn2;
	$fn1 = fetch($fn1, $url1, $md51);
	$fn2 = fetch($fn2, $url2, $md52);
	my $sam = $format =~ /^sam$/i;
	if(defined($lab)) {
		$lab =~ /[:\s]/ && die "Label may not contain a colon or whitespace character; was \"$lab\"\n";
	}
	
	# turn FASTQ pairs into tuples
	my ($fh1, $fh2);
	open($fh1, $fn1) || die "Could not open input file $fn1";
	open($fh2, $fn2) || die "Could not open input file $fn2";
	my $r = 0;
	my $fileno = 1;
	my $of;
	open($of, ">${fn1}_$fileno.out") || die;
	my $fn1_nospace = $fn1;
	$fn1_nospace =~ s/[\s]+//g;
	my $rname .= "FN:".$fn1_nospace; # Add filename
	while(1) {
		last if($stopAfter != 0 && $rtot >= $stopAfter);
		parseRead($fh1, $sam, $color);
		my ($name1, $seq1, $qual1) = ($name, $seq, $qual);
		parseRead($fh2, $sam, $color);
		defined($name) == defined($name1) ||
			die "Mate files didn't come together properly: $fn1,$fn2\n";
		last unless defined($name);
		my $fullname = $rname;
		if($labReadGroup) {
			defined($readGroup) || die;
			$fullname .= ";LB:$readGroup";
			$delayedCounters{"Pairs with label $readGroup"}++;
		} elsif(defined($lab)) {
			$fullname .= ";LB:$lab";
			$delayedCounters{"Pairs with label $lab"}++;
		}
		$fullname .= ";$name";
		print $of "$fullname\t$seq1\t$qual1\t$seq\t$qual\n";
		$r++;
		$rtot += 2;
		if($maxPerFile > 0 && ($r % $maxPerFile) == 0) {
			close($of);
			if($push ne "") {
				pushBatch("${fn1}_$fileno.out");
				system("rm -f ${fn1}_$fileno.out ${fn1}_$fileno.out.* >&2");
			}
			$fileno++;
			open($of, ">${fn1}_$fileno.out") || die "Could not open output file ${fn1}_$fileno.out";
		}
		$totpaired++;
		if(++$paired >= 100000) {
			counter("Short read preprocessor,Paired reads,$paired");
			$paired = 0;
		}
	}
	counter("Short read preprocessor,Paired reads,$paired");
	close($fh1);
	close($fh2);
	close($of);
	flushDelayedCounters("Short read preprocessor");

	# Remove input files
	system("rm -f $fn1 $origFn1 >&2") unless $keep;
	system("rm -f $fn2 $origFn2 >&2") unless $keep;
	if($push ne "") {
		# Push and remove output files
		pushBatch("${fn1}_$fileno.out");
		system("rm -f ${fn1}_$fileno.out ${fn1}_$fileno.out.* >&2");
	} else {
		# Just keep the output files around
	}
}

##
# Add user's credentials to an s3 or s3n URI if necessary
#
sub addkey($) {
	my $url = shift;
	return $url unless $url =~ /^s3n?:/i;
	AWS::ensureKeys($Tools::hadoop, $Tools::hadoop_arg);
	if($url =~ /s3n?:\/\/[^\@]*$/ && defined($AWS::accessKey)) {
		my $ec2key = $AWS::accessKey.":".$AWS::secretKey;
		$url =~ s/s3:\/\//s3:\/\/$ec2key\@/;
		$url =~ s/s3n:\/\//s3n:\/\/$ec2key\@/;
	}
	return $url;
}

##
# Give URL, return likely format string.  Default to fastq.
#
sub urlToFormat($) {
	my $url = shift;
	if($url =~ /\.sam$/i || $url =~ /\.bam$/i) {
		return "sam";
	} else {
		return "fastq";
	}
}

while (<>) {
	# Skip comments and whitespace lines
	chomp;
	my @s = split(/\s+/);
	msg("Line: $_");
	if ($skipfirst) {
		my $trimmed = shift @s;
		msg("-s trimmed \"$trimmed\" from line:\n$_");
	}
	if(scalar(@s) == 0) { # Skip empty or whitespace-only lines
		counter("Short read preprocessor,Empty lines,1");
		next;
	}
	if($s[0] =~ /^\s*#/) {  # Skip lines beginning with hash
		counter("Short read preprocessor,Comment lines,1");
		msg("Skipping comment line");
		next;
	} else {
		msg("Not a comment line");
	}
	unless(defined($s[1])) {
		counter("Short read preprocessor,Malformed lines,1");
		msg("Line malformed:\n$_");
		msg("Skipping...");
		next;
	}
	my ($url1, $md51) = (addkey($s[0]), $s[1]);
	my $color = 0; # TODO

	my $turl1 = fileparse($url1);
	if($#s >= 3) {
		# If s[4] is defined, it contains the sample label
		msg("Doing paired-end entry $turl1");
		my ($url2, $md52) = (addkey($s[2]), $s[3]);
		doPairedUrl($url1, $md51, $url2, $md52, $s[4], urlToFormat($url1), $color);
		counter("Short read preprocessor,Paired URLs,1");
	} else {
		# If s[2] is defined, it contains the sample label
		msg("Doing unpaired entry $turl1");
		doUnpairedUrl($url1, $md51, $s[2], urlToFormat($url1), $color);
		counter("Short read preprocessor,Unpaired URLs,1");
	}
	msg("Total unpaired reads: $totunpaired");
	msg("Total paired reads: $totpaired");
}
print "FAKE\n";

counter("Short read preprocessor,Warnings,$ws");
msg("Warnings: $ws");
flushDelayedCounters("Short read preprocessor");
