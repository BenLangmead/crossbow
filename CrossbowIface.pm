#!/usr/bin/perl -w

##
# Author: Ben Langmead
#   Date: February 11, 2010
#
# Use 'elastic-mapreduce' ruby script to invoke an EMR job described
# in a dynamically-generated JSON file.  Constructs the elastic-
# mapreduce invocation from paramteres/defaults/environment variables.
#

package CrossbowIface;
use strict;
use warnings;
use Getopt::Long;
use FindBin qw($Bin);
use List::Util qw[min max];
use Cwd 'abs_path';
use lib $Bin;
use Tools;
use File::Path qw(mkpath);

##
# Function interface for invoking the generic Crossbow wrapper.
#
sub crossbow {

scalar(@_) == 7 || die "Must specify 7 arguments";

our @args = @{$_[0]};
our $scr   = $_[1];
our $usage = $_[2];
our $msg   = $_[3];
our $msgf  = $_[4];
our $emsg  = $_[5];
our $emsgf = $_[6];

defined($msg)   || ($msg   = sub { print @_ });
defined($msgf)  || ($msgf  = sub { printf @_ });
defined($emsg)  || ($emsg  = sub { print STDERR @_ });
defined($emsgf) || ($emsgf = sub { printf STDERR @_ });

our $APP = "Crossbow";
our $app = lc $APP;
our $VERSION = `cat $Bin/VERSION`; $VERSION =~ s/\s//g;
if($VERSION eq "") {
	$VERSION = `cat $Bin/VERSION_CROSSBOW`; $VERSION =~ s/\s//g;
}

our $umaskOrig = umask();

sub dieusage($$$) {
	my ($text, $usage, $lev) = @_;
	$emsg->("$text\n");
	$emsg->("$usage\n");
	exit $lev;
}

our $warnings = 0;
sub warning($) {
	my $str = shift;
	$emsg->("$str\n");
	$warnings++;
}

# AWS params
our $awsEnv = 0;
our $emrScript = "";
our $hadoopVersion = "";
our $accessKey = "";
our $secretKey = "";
our $keypair = "";
our $keypairFile = "";
our $zone = "";
our $credentials = "";
our $swap = 0; # to add

# EMR params
our $dryrun = 0;
our $name = "";
our $waitJob = 0;
our $instType = "";
our $numNodes = 1;
our $reducersPerNode = 0;
our $emrArgs = "";
our $noLogs = 0;
our $logs = "";
our $noEmrDebugging = 0;

# Job params
our $input  = "";
our $output = "";
our $intermediate = "";
our $partitionLen = 0;
our $justAlign = 0;
our $resumeAlign = 0;
our $resumeSnps = 0;
our $keepAll = 0;
our $keepIntermediate = 0;

# Lobal job params
our $localJob = 0;
our $test = 0;
our $inputLocal  = "";
our $outputLocal = "";
our $intermediateLocal = "";
our $cores = 0;
our $dontForce = 0;
our $bowtie = "";
our $samtools = "";
our $sra = "";
our $useSamtools = 0;
our $useSraToolkit = 0;
our $soapsnp = "";
our $externalSort = 0;
our $maxSortRecords = 800000;
our $maxSortFiles = 40;

# Hadoop job params
our $hadoopJob = 0;
our $hadoop_arg = "";
our $hadoopStreamingJar_arg = "";

# Preprocessing
our $preprocess = 0;
our $justPreprocess = 0;
our $preprocOutput = "";
our $preprocCompress = "";
our $preprocStop = 0;
our $preprocMax = 0;

# Crossbow params
our $ref = "";
our $bt_args = "";
our $qual = "";
our $discardAll = 0;
our $discardReads = 0;
our $discardRefBins = 0;
our $indexLocal = "";
our $truncate = 0;
our $truncateDiscard = 0;
our $cmapLocal = "";
our $sequencesLocal = "";
our $snpsLocal = "";
our $ss_args = "";
our $ss_hap_args = "";
our $ss_dip_args = "";
our $haploids = "";
our $allHaploids = 0;

# Other parmams
our $tempdir = "";
our $slaveTempdir = "";
our $splitJars = 0;
our $verbose = 0;

sub absPath($) {
	my $path = shift;
	defined($path) || die;
	if($path =~ /^hdfs:/i || $path =~ /^s3n?:/i || $path eq "") {
		return $path;
	}
	$path =~ s/^~/$ENV{HOME}/;
	my $ret = abs_path($path);
	defined($ret) || die "abs_path turned $path into undef\n";
	return $ret;
}

##
# A tiny log facility in case we need to report what we did to the user.
#
our $checkExeMsg = "";
sub checkExeLog($) {
	my $text = shift;
	$checkExeMsg .= $text;
	$emsg->($text) if $verbose;
}

##
# Can I run the executable and receive error 256?  This is a little
# more robust than -x, but it requires that the executable return 1
# immediately if run without arguments.
#
sub canRun {
	my ($nm, $f, $exitlevel) = @_;
	$exitlevel = 0 unless defined($exitlevel);
	my $ret = system("$f 2>/dev/null >/dev/null") >> 8;
	return 1 if $ret == $exitlevel;
	if($ret != 1 && $ret != 255) {
		return 0;
	}
	if($nm eq "Rscript" || $nm eq "R") {
		checkExeLog("  Checking whether R has appropriate R/Bioconductor packages...\n");
		my $packages = "";
		for my $pack ("lmtest", "multicore", "IRanges", "geneplotter") {
			$packages .= "suppressPackageStartupMessages(library($pack)); print('Found required package $pack'); ";
		}
		my $out = `$f -e \"$packages print('All packages found')\" 2>&1`;
		checkExeLog($out);
		$ret = $? >> 8;
		return $ret == $exitlevel;
	}
	return 1;
}

##
# Scan the bin subdirectory for a working version of the given program.
#
sub scanPrebuiltBin {
	my ($nm, $base, $exitlevel) = @_;
	defined($nm) || die;
	defined($base) || die;
	$exitlevel = 0 unless defined($exitlevel);
	my @ret = ();
	for my $f (<$base/bin/*>) {
		checkExeLog("     Scanning directory: $f\n");
		for my $f2 (<$f/$nm>) {
			next unless -f $f2;
			checkExeLog("       Found candidate: $f2\n");
			checkExeLog("         Runnable?...");
			if(canRun($nm, $f2, $exitlevel)) {
				checkExeLog("YES\n");
				push @ret, $f2;
			} else {
				checkExeLog("no\n");
			}
		}
	}
	if($nm eq "Rscript" || $nm eq "R") {
		my $path = "$Bin/R/bin/Rscript";
		checkExeLog("     I'm searching for R or Rscript, so scanning directory: $path\n");
		if(canRun($nm, $path, $exitlevel)) {
			push @ret, $path;
		}
	}
	if(scalar(@ret) > 0) {
		@ret = sort @ret;
		checkExeLog("       Settling on $ret[-1]\n");
		return $ret[-1];
	} else {
		checkExeLog("       No runnable candidates\n");
		return "";
	}
}

##
# Require that an exe be specified and require that it's there.
#
sub checkExe {
	my ($path, $nm, $env, $sub, $arg, $dieOnFail, $exitlevel) = @_;
	$exitlevel = 0 unless defined($exitlevel);
	$nm ne "" || die "Empty name\n";
	defined($path) || die "Path for $nm undefined\n";
	checkExeLog("Searching for '$nm' binary...\n");
	checkExeLog(sprintf "   Specified via $arg?....%s\n", (($path ne "") ? "YES" : "no"));
	if($path ne "") {
		my $cr = canRun($nm, $path, $exitlevel);
		checkExeLog(sprintf("     Runnable?....%s\n", ($cr ? "YES" : "no")));
		return $path if $cr;
		die "Error: $arg specified, but path $path does not point to something $APP can execute\n";
	}
	my $envSpecified = defined($ENV{$env}) && $ENV{$env} ne "";
	checkExeLog(sprintf "   \$$env specified?....%s\n", ($envSpecified ? "YES ($ENV{$env})" : "no"));
	if($envSpecified) {
		my $envPath = $ENV{$env};
		$envPath .= "/$sub" if $sub ne "";
		$envPath .= "/$nm";
		my $cr = canRun($nm, $envPath, $exitlevel);
		checkExeLog(sprintf "     Runnable?....%s\n", ($cr ? "YES" : "no"));
		return $envPath if $cr;
	}
	checkExeLog("   Checking $Bin/bin...\n");
	$path = scanPrebuiltBin($nm, $Bin);
	return $path if $path ne "";
	checkExeLog("   Checking \$PATH...\n");
	$path = `which $nm 2>/dev/null`;
	if(defined($path)) {
		chomp($path);
		if($path) {
			checkExeLog("     Found '$path'...\n");
			my $cr = canRun($nm, $path, $exitlevel);
			checkExeLog(sprintf "       Runnable?....%s\n", ($cr ? "YES" : "no"));
			return $path if $cr;
		} else {
			checkExeLog("     Didn't find anything...\n");
		}
	}
	$emsg->("Error: Could not find '$nm' executable\n");
	if($hadoopJob) {
		$emsg->("Note: for Hadoop jobs, required executables must be located at the same path on all cluster nodes including the master.\n");
	}
	unless($verbose) {
		$emsg->("Here's what I tried:\n");
		$emsg->($checkExeMsg);
	}
	exit 1 if $dieOnFail;
	return "";
}

@ARGV = @args;

my $help = 0;

Getopt::Long::Configure("no_pass_through");
GetOptions (
# AWS params
	"aws-env"                   => \$awsEnv,
	"emr-script:s"              => \$emrScript,
	"elastic-mapreduce:s"       => \$emrScript,
	"hadoop-version:s"          => \$hadoopVersion,
	"accessid:s"                => \$accessKey,
	"secretid:s"                => \$secretKey,
	"keypair|key-pair:s"        => \$keypair,
	"key-pair-file:s"           => \$keypairFile,
	"zone|region:s"             => \$zone,
	"credentials:s"             => \$credentials,
# EMR params
	"dryrun"                    => \$dryrun,
	"dry-run"                   => \$dryrun,
	"name:s"                    => \$name,
	"instance-type:s"           => \$instType,
	"stay-alive"                => \$waitJob,
	"wait-on-fail"              => \$waitJob,
	"nodes:i"                   => \$numNodes,
	"instances|num-instances:i" => \$numNodes,
	"emr-args:s"                => \$emrArgs,
	"no-logs"                   => \$noLogs,
	"logs:s"                    => \$logs,
	"no-emr-debug"              => \$noEmrDebugging,
	"swap:i"                    => \$swap,
# Job params
	"input:s"                   => \$input,
	"output:s"                  => \$output,
	"intermediate:s"            => \$intermediate,
	"partition-len:i"           => \$partitionLen,
	"just-align"                => \$justAlign,
	"resume-align"              => \$resumeAlign,
	"resume-snps"               => \$resumeSnps,
	"local-job"                 => \$localJob,
	"hadoop-job"                => \$hadoopJob,
	"keep-all"                  => \$keepAll,
	"keep-intermediates"        => \$keepIntermediate,
	"test"                      => \$test,
# Local job params
	"input-local:s"             => \$inputLocal,
	"output-local:s"            => \$outputLocal,
	"intermediate-local:s"      => \$intermediateLocal,
	"cores:i"                   => \$cores,
	"cpus:i"                    => \$cores,
	"max-sort-records:i"        => \$maxSortRecords,
	"max-sort-files:i"          => \$maxSortFiles,
	"dont-overwrite"            => \$dontForce,
	"no-overwrite"              => \$dontForce,
	"bowtie:s"                  => \$bowtie,
	"samtools:s"                => \$samtools,
	#"fastq-dump:s"              => \$sra,
	"sra-toolkit:s"             => \$sra,
	"soapsnp:s"                 => \$soapsnp,
	"external-sort"             => \$externalSort,
# Hadoop job params
	"hadoop:s"                  => \$hadoop_arg,
	"streaming-jar:s"           => \$hadoopStreamingJar_arg,
# Crossbow params
	"reference:s"               => \$ref,
	"index-local:s"             => \$indexLocal,
	"quality|qual|quals:s"      => \$qual,
	"bowtie-args:s"             => \$bt_args,
	"discard-reads:f"           => \$discardReads,
	"discard-all:f"             => \$discardAll,
	"discard-ref-bins:f"        => \$discardRefBins,
	"truncate|truncate-length:i"=> \$truncate,
	"truncate-discard:i"        => \$truncateDiscard,
	"cmap-local:s"              => \$cmapLocal,
	"sequences-local:s"         => \$sequencesLocal,
	"snps-local:s"              => \$snpsLocal,
	"ss-args:s"                 => \$ss_args,
	"ss-hap-args:s"             => \$ss_hap_args,
	"ss-dip-args:s"             => \$ss_dip_args,
	"soapsnp-args:s"            => \$ss_args,
	"soapsnp-hap-args:s"        => \$ss_hap_args,
	"soapsnp-dip-args:s"        => \$ss_dip_args,
	"haploids:s"                => \$haploids,
	"all-haploids"              => \$allHaploids,
# Preprocessing params
	"preprocess"                => \$preprocess,
	"just-preprocess"           => \$justPreprocess,
	"crossbow"                  => sub { $justPreprocess = 0 },
	"pre-output:s"              => \$preprocOutput,
	"preproc-output:s"          => \$preprocOutput,
	"preprocess-output:s"       => \$preprocOutput,
	"pre-compress:s"            => \$preprocCompress,
	"preproc-compress:s"        => \$preprocCompress,
	"preprocess-compress:s"     => \$preprocCompress,
	"pre-stop:i"                => \$preprocStop,
	"pre-filemax:i"             => \$preprocMax,
# Other parmams
	"tempdir:s"                 => \$tempdir,
	"slave-tempdir:s"           => \$slaveTempdir,
	"split-jars"                => \$splitJars,
	"verbose"                   => \$verbose,
	"version"                   => \$VERSION,
	"help"                      => \$help
) || dieusage("Error parsing options", $usage, 1);

dieusage("", $usage, 0) if $help;

# This function generates random strings of a given length
sub randStr($) {
	my $len = shift;
	my @chars = ('a'..'z', 'A'..'Z', '0'..'9', '_');
	my $str = "";
	foreach (1..$len) {
		$str .= $chars[int(rand(scalar(@chars)))];
	}
	return $str;
}
srand(time ^ $$);
my $randstr = randStr(10);

# See http://aws.amazon.com/ec2/instance-types/

our %instTypeNumCores = (
	"m1.small" => 1,
	"m1.large" => 2,
	"m1.xlarge" => 4,
	"c1.medium" => 2,
	"c1.xlarge" => 8,
	"m2.xlarge" => 2,
	"m2.2xlarge" => 4,
	"m2.4xlarge" => 8,
	"cc1.4xlarge" => 8
);

our %instTypeSwap = (
	"m1.small"    => (2 *1024), #  1.7 GB
	"m1.large"    => (8 *1024), #  7.5 GB
	"m1.xlarge"   => (16*1024), # 15.0 GB
	"c1.medium"   => (2 *1024), #  1.7 GB
	"c1.xlarge"   => (8 *1024), #  7.0 GB
	"m2.xlarge"   => (16*1024), # 17.1 GB
	"m2.2xlarge"  => (16*1024), # 34.2 GB
	"m2.4xlarge"  => (16*1024), # 68.4 GB
	"cc1.4xlarge" => (16*1024)  # 23.0 GB
);

our %instTypeBitsMap = (
	"m1.small" => 32,
	"m1.large" => 64,
	"m1.xlarge" => 64,
	"c1.medium" => 32,
	"c1.xlarge" => 64,
	"m2.xlarge" => 64,
	"m2.2xlarge" => 64,
	"m2.4xlarge" => 64,
	"cc1.4xlarge" => 64
);

##
# Return the appropriate configuration string for setting the number of fields
# to bin on.  This depends on the Hadoop version.
#
sub partitionConf($) {
	my $binFields = shift;
	my @vers = split(/\./, $hadoopVersion);
	scalar(@vers >= 2) || die "Could not parse Hadoop version: \"$hadoopVersion\"\n";
	my ($hadoopMajorVer, $hadoopMinorVer) = ($vers[0], $vers[1]);
	my $hadoop18Partition = "num.key.fields.for.partition=$binFields";
	my $hadoop19Partition = "mapred.text.key.partitioner.options=-k1,$binFields";
	if($hadoopMajorVer == 0 && $hadoopMinorVer < 19) {
		return $hadoop18Partition;
	}
	return $hadoop19Partition;
}

##
# Return the parameter used to configure Hadoop.  In older versions it
# was -jobconf; in newer versions, it's -D.
#
sub confParam() {
	my @vers = split(/\./, $hadoopVersion);
	scalar(@vers >= 2) || die "Could not parse Hadoop version: \"$hadoopVersion\"\n";
	my ($hadoopMajorVer, $hadoopMinorVer) = ($vers[0], $vers[1]);
	if($hadoopMajorVer == 0 && $hadoopMinorVer < 19) {
		return "-jobconf\", \"";
	}
	return "-D\", \"";
}

##
# Return the parameter used to ask streaming Hadoop to cache a file.
#
sub cacheFile() {
	my @vers = split(/\./, $hadoopVersion);
	scalar(@vers >= 2) || die "Could not parse Hadoop version: \"$hadoopVersion\"\n";
	my ($hadoopMajorVer, $hadoopMinorVer) = ($vers[0], $vers[1]);
	#if($hadoopMajorVer == 0 && $hadoopMinorVer < 19) {
		return "-cacheFile";
	#}
	#return "-files";
}

sub validateInstType($) {
	defined($instTypeNumCores{$_[0]}) || die "Bad --instance-type: \"$_[0]\"\n";
}

sub instanceTypeBits($) {
	defined($instTypeBitsMap{$_[0]}) || die "Bad --instance-type: \"$_[0]\"\n";
	return $instTypeBitsMap{$_[0]};
}

$hadoopVersion = "0.20" if !defined($hadoopVersion) || $hadoopVersion eq "";
my $appDir = "$app-emr/$VERSION";
$accessKey = $ENV{AWS_ACCESS_KEY_ID} if
	$accessKey eq "" && $awsEnv && defined($ENV{AWS_ACCESS_KEY_ID});
$secretKey = $ENV{AWS_SECRET_ACCESS_KEY} if
	$secretKey eq "" && $awsEnv && defined($ENV{AWS_SECRET_ACCESS_KEY});
$name = "$APP-$VERSION" if $name eq "";
$qual = "phred33" if $qual eq "";
($qual eq "phred33" || $qual eq "phred64" || $qual eq "solexa64") ||
	dieusage("Bad quality type: $qual", $usage, 1);
$instType = "c1.xlarge" if $instType eq "";
validateInstType($instType);
$cores = 1 if $cores == 0 && $localJob;
$cores = ($instTypeNumCores{$instType} || 1) if $cores == 0;
$cores > 0 || die;
$swap = ($instTypeSwap{$instType} || 0) if $swap == 0;
$reducersPerNode = $cores if $reducersPerNode == 0;
$reducersPerNode > 0 || die;
$partitionLen = 1000000 if $partitionLen == 0;
$bt_args = "-M 1" if $bt_args eq "";
$ref eq "" || $ref =~ /\.jar$/ || dieusage("--reference must end with .jar", $usage, 1);
$numNodes = 1 if !$numNodes;
$haploids = "none" if $haploids eq "";
$haploids = "all" if $allHaploids;
$ss_args = "-2 -u -n -q" if $ss_args eq "";
$ss_hap_args = "-r 0.0001" if $ss_hap_args eq "";
$ss_dip_args = "-r 0.00005 -e 0.0001" if $ss_dip_args eq "";
$justAlign = 0 unless(defined($justAlign));
$resumeAlign = 0 unless(defined($resumeAlign));
$preprocess = 0 unless(defined($preprocess));
$justPreprocess = 0 unless(defined($justPreprocess));
$preprocStop = 0 unless(defined($preprocStop));
$preprocOutput eq "" || $preprocess ||
	warning( "Warning: --pre-output is specified but --preprocess is not");
$preprocCompress eq "" || $preprocess ||
	warning("Warning: --pre-compress is specified but --preprocess is not");
$preprocStop == 0 || $preprocess ||
	warning("Warning: --pre-stop is specified but --preprocess is not");
$preprocMax == 0 || $preprocess ||
	warning("Warning: --pre-filemax is specified but --preprocess is not");
$preprocCompress = "gzip" if $preprocCompress eq "";
$preprocCompress = "gzip" if $preprocCompress eq "gz";
$preprocMax = 500000 if !$preprocMax;
$preprocCompress eq "gzip" || $preprocCompress eq "none" ||
	dieusage("--pre-compress must be \"gzip\" or \"none\"", $usage, 1);
$tempdir = "/tmp/$app-$randstr" unless $tempdir ne "";
my $scriptTempdir = "$tempdir/invoke.scripts";
mkpath($scriptTempdir);
if(!$hadoopJob && !$localJob) {
	$slaveTempdir = "/mnt/$$" if $slaveTempdir eq "";
} else {
	$slaveTempdir = "$tempdir" if $slaveTempdir eq "";
}
-d $tempdir || die "Could not create temporary directory \"$tempdir\"\n";
if(!$hadoopJob && !$localJob) {
	if($waitJob) {
		$emrArgs .= " " if ($emrArgs ne "" && $emrArgs !~ /\s$/);
		$emrArgs .= "--alive";
	}
	unless($noEmrDebugging) {
		$emrArgs .= " " if ($emrArgs ne "" && $emrArgs !~ /\s$/);
		$emrArgs .= "--enable-debugging";
	}
}

my $failAction = "TERMINATE_JOB_FLOW";
$failAction = "CANCEL_AND_WAIT" if $waitJob;

($discardReads >= 0.0 && $discardReads <= 1.0) ||
	die "--discard-reads must be in [0,1], was: $discardReads\n";
length("$discardReads") > 0 || die "--discard-reads was empty\n";
($discardRefBins >= 0.0 && $discardRefBins <= 1.0) ||
	die "--discard-ref-bins must be in [0,1], was: $discardRefBins\n";
length("$discardRefBins") > 0 || die "--discard-ref-bins was empty\n";
($discardAll >= 0.0 && $discardAll <= 1.0) ||
	die "--discard-all must be in [0,1], was: $discardAll\n";
$discardReads = $discardAll if $discardReads == 0;
$discardRefBins = $discardAll if $discardRefBins == 0;

##
# Parse a URL, extracting the protocol and type of program that will
# be needed to download it.
#
sub parse_url($) {
	my $s = shift;
	defined($s) || croak();
	my @ss = split(/[:]/, $s);
	if($ss[0] =~ /s3n?/i) {
		return "s3";
	} elsif($ss[0] =~ /hdfs/i) {
		return "hdfs";
	} else {
		return "local";
	}
}

$input = absPath($input);
$output = absPath($output);
$intermediate = absPath($intermediate);
$ref = absPath($ref);
$indexLocal = absPath($indexLocal);
$preprocOutput = absPath($preprocOutput);
$tempdir = absPath($tempdir);

my $resume = $resumeAlign || $resumeSnps;

#
# Work out which phases are going to be executed
#
my %stages = (
	"preprocess"  => 0,
	"align"       => 0,
	"snps"        => 0,
	"postprocess" => 0
);

my ($firstStage, $lastStage) = ("", "");
if($justPreprocess) {
	$stages{preprocess} = 1;
} elsif($justAlign) {
	# --just-align specified.  Either preprocess and align (input =
	# manifest) or just align (input = preprocessed reads).
	$stages{preprocess} = 1 if $preprocess;
	$stages{align} = 1;
} elsif($resumeAlign) {
	$stages{snps} = 1;
	$stages{postprocess} = 1;
} elsif($resumeSnps) {
	$stages{postprocess} = 1;
} else {
	$stages{preprocess} = 1 if $preprocess;
	$stages{align} = 1;
	$stages{snps} = 1;
	$stages{postprocess} = 1;
}
# Determine first and last stages
for my $s ("preprocess", "align", "snps", "postprocess") {
	if(defined($stages{$s}) && $stages{$s} != 0) {
		$firstStage = $s if $firstStage eq "";
		$lastStage = $s;
	}
}
$firstStage ne "" || die;
$lastStage ne "" || die;
my $numStages = 0;
for my $k (keys %stages) { $numStages += $stages{$k}; }

$useSraToolkit = $stages{preprocess};
$useSamtools = $stages{align} && 0;
my $useBowtie = $stages{align};
my $sraToolkit = $stages{preprocess};
my $useSoapsnp = $stages{snps};
my $pre = "CROSSBOW_";
$bowtie   =~ s/^~/$ENV{HOME}/;
$samtools =~ s/^~/$ENV{HOME}/;
$soapsnp  =~ s/^~/$ENV{HOME}/;
$sra      =~ s/^~/$ENV{HOME}/;
if($test) {
	$verbose = 1;
	my $failed = 0;
	if($localJob || $hadoopJob) {
		# Check for binaries
		$bowtie   = checkExe($bowtie,   "bowtie",    "${pre}BOWTIE_HOME",     "",    "--bowtie"  ,    0);
		$samtools = checkExe($samtools, "samtools",  "${pre}SAMTOOLS_HOME",   "",    "--samtools",    0) if $useSamtools;
		$soapsnp  = checkExe($soapsnp,  "soapsnp",   "${pre}SOAPSNP_HOME",    "",    "--soapsnp" ,    0);
		$sra      = checkExe($sra,      "fastq-dump","${pre}SRATOOLKIT_HOME", "",    "--sra-toolkit", 0, 4);
		$msg->("Summary:\n");
		$msgf->("  bowtie: %s\n",     ($bowtie   ne "" ? "INSTALLED at $bowtie"   : "NOT INSTALLED"));
		$msgf->("  samtools: %s\n",   ($samtools ne "" ? "INSTALLED at $samtools" : "NOT INSTALLED")) if $useSamtools;
		$msgf->("  soapsnp: %s\n",    ($soapsnp  ne "" ? "INSTALLED at $soapsnp"  : "NOT INSTALLED"));
		$msgf->("  fastq-dump: %s\n", ($sra      ne "" ? "INSTALLED at $sra"      : "NOT INSTALLED"));
		$msg->("Hadoop note: executables must be runnable via the SAME PATH on all nodes.\n") if $hadoopJob;
		$failed = $bowtie eq "" || ($useSamtools && $samtools eq "") || $soapsnp eq ""; #|| $sra eq ""; 
		if($failed) {
			$msg->("FAILED install test\n");
		} elsif($sra eq "") {
			$msg->("PASSED WITH ***WARNING***: SRA toolkit fastq-dump not found; .sra inputs won't work but others will\n");
		} else {
			$msg->("PASSED install test\n");
		}
	} else {
		$emrScript = checkExe($emrScript, "elastic-mapreduce", "${pre}EMR_HOME", "", "--emr-script", 0);
		$msg->("Summary:\n");
		$msgf->("  elastic-mapreduce: %s\n", ($emrScript ne "" ? "INSTALLED at $emrScript" : "NOT INSTALLED"));
		$failed = $emrScript eq "";
		$msg->($failed ? "FAILED install test\n" : "PASSED install test\n");
	}
	exit $failed ? 1 : 0;
}
if($localJob || $hadoopJob) {
	# Check for binaries
	$bowtie    = checkExe($bowtie,   "bowtie",     "${pre}BOWTIE_HOME",     "",    "--bowtie"  ,    1) if $useBowtie;
	$samtools  = checkExe($samtools, "samtools",   "${pre}SAMTOOLS_HOME",   "",    "--samtools",    1) if $useSamtools;
	$soapsnp   = checkExe($soapsnp,  "soapsnp",    "${pre}SOAPSNP_HOME",    "",    "--soapsnp" ,    1) if $useSoapsnp;
	$sra       = checkExe($sra,      "fastq-dump", "${pre}SRATOOLKIT_HOME", "",    "--sra-toolkit", 0, 4) if $useSraToolkit;
	if($sra eq "") {
		print STDERR "***WARNING***\n";
		print STDERR "***WARNING***: SRA toolkit fastq-dump not found; .sra inputs won't work but others will\n";
		print STDERR "***WARNING***\n";
	}
} else {
	$emrScript = checkExe($emrScript, "elastic-mapreduce", "${pre}EMR_HOME", "", "--emr-script", 1);
}

# Parse input, output and intermediate directories
if($inputLocal eq "") {
	defined($input) || die;
	$input = "hdfs://$input" if parse_url($input) eq "local";
} else {
	parse_url($inputLocal) eq "local" || die "--input-local specified non-local URL: $inputLocal\n";
	$input = $inputLocal;
}
if($outputLocal eq "") {
	defined($output) || die;
	$output = "hdfs://$output" if parse_url($output) eq "local";
} else {
	parse_url($outputLocal) eq "local" || die "--output-local specified non-local URL: $outputLocal\n";
	$output = $outputLocal;
}
if(!$hadoopJob && !$localJob) {
	# If the user hasn't specified --no-logs and hasn't specified a --log-uri
	# via --emr-args, then specify a subdirectory of the output directory as
	# the log dir.
	$logs = "${output}_logs" if $logs eq "";
	if(!$noLogs && $emrArgs !~ /-log-uri/) {
		$emrArgs .= " " if ($emrArgs ne "" && $emrArgs !~ /\s$/);
		$emrArgs .= "--log-uri $logs ";
	}
	if($hadoopVersion ne "0.20") {
		if($hadoopVersion ne "0.18") {
			print STDERR "Error: Expected hadoop version 0.18 or 0.20, got $hadoopVersion\n";
			exit 1;
		}
		$emrArgs .= " " if ($emrArgs ne "" && $emrArgs !~ /\s$/);
		$emrArgs .= "--hadoop-version=0.18 ";
	}
}
my $intermediateSet = ($intermediate ne "" || $intermediateLocal ne "");
if($intermediateLocal eq "") {
	if($intermediate eq "") {
		if($localJob) {
			$intermediate = "$tempdir/$app/intermediate/$$";
		} else {
			$intermediate = "hdfs:///$app/intermediate/$$";
		}
	}
} else {
	parse_url($intermediateLocal) eq "local" || die "--intermediate-local specified non-local URL: $intermediateLocal\n";
	$intermediate = $intermediateLocal;
}

$output ne "" || dieusage("Must specify --output", $usage, 1);
if(!$localJob && !$hadoopJob) {
	parse_url($output) eq "s3" || die "Error: In cloud mode, --output path must be an S3 path; was: $output\n";
}
if($resume && $intermediateSet) {
	die "Cannot specify both --resume-* and --intermediate; specify intermediate directory\n".
	    "to be resumed using --input.  --intermediate is automatically set to --input\n";
}
if($intermediate eq "" && $localJob) {
	$intermediate = "$tempdir/$app/intermediate";
} elsif($intermediate eq "") {
	$intermediate = "hdfs:///tmp/$app" if $intermediate eq "";
}
$input  ne "" || dieusage("Must specify --input", $usage, 1);
if(!$localJob && !$hadoopJob) {
	parse_url($input) eq "s3" || die "Error: In cloud mode, --input path must be an S3 path; was: $input\n";
}
if($localJob && !$justPreprocess) {
	$snpsLocal ne "" || die "Must specify --snps-local when --local-job is specified\n";
	$sequencesLocal ne "" || die "Must specify --sequences-local when --local-job is specified\n";
	$cmapLocal ne "" || die "Must specify --cmap-local when --local-job is specified\n";
	$indexLocal ne "" || die "Must specify --index-local when --local-job is specified\n";
}

sub checkArgs($$) {
	my ($args, $param) = @_;
	if($args =~ /[\t\n\r]/) {
		die "$param \"$args\" has one or more illegal whitespace characters\n";
	} elsif($args =~ /[_]/) {
		$emsg->("$param \"$args\" contains underscores; this may confuse $APP\n");
	}
	$args =~ s/ /_/g;
	$args =~ /\s/ && die "$param still has whitespace after space conversion: \"$args\"\n";
	return $args;
}
$ss_args = checkArgs($ss_args, "--ss-args");
$ss_hap_args = checkArgs($ss_hap_args, "--ss-hap-args");
$ss_dip_args = checkArgs($ss_dip_args, "--ss-dip-args");

sub upperize($) {
	my $url = shift;
	$url =~ s/^s3n/S3N/;
	$url =~ s/^s3/S3/;
	$url =~ s/^hdfs/HDFS/;
	return $url;
}

#
# If the caller has provided all the relevant individual parameters,
# bypass the credentials file.
#
my $credentialsFile = "";
if($credentials eq "" && $accessKey ne "" && $secretKey ne "") {
	my ($regionStr, $keypairStr, $keypairFileStr) = ("", "", "");
	$regionStr      = "--region=$zone"               if $zone ne "";
	$keypairStr     = "--key-pair=$keypair"          if $keypair ne "";
	$keypairFileStr = "--key-pair-file=$keypairFile" if $keypairFile ne "";
	$credentials = "--access-id=$accessKey --private-key=$secretKey $keypairStr $keypairFileStr $regionStr";
} elsif($credentials ne "") {
	$credentialsFile = $credentials;
	$credentials = "-c $credentials";
}

my $intermediateUpper = upperize($intermediate);
$ref ne "" || $justPreprocess || $localJob ||
	dieusage("Must specify --reference OR --just-preprocess", $usage, 1);
$ref eq "" || $ref =~ /\.jar$/ || dieusage("--reference must end with .jar", $usage, 1);
$indexLocal eq "" || -f "$indexLocal.1.ebwt" || dieusage("--index-local \"$indexLocal\" path doesn't point to an index", $usage, 1);
$sequencesLocal eq "" || -d $sequencesLocal || dieusage("--sequences-local \"$sequencesLocal\" path doesn't point to a directory", $usage, 1);
$snpsLocal eq "" || -d $snpsLocal || dieusage("--snps-local \"$snpsLocal\" path doesn't point to a directory", $usage, 1);
$cmapLocal eq "" || -f $cmapLocal || dieusage("--cmap-local \"$cmapLocal\" path doesn't point to a readable file", $usage, 1);

if(!$localJob && !$hadoopJob && defined($ref) && $ref ne "") {
	parse_url($ref) eq "s3" || die "Error: In cloud mode, --reference path must be an S3 path; was: $ref\n";
}

# Remove inline credentials from URLs
$input =~ s/:\/\/[^\/]@//;
$output =~ s/:\/\/[^\/]@//;
$ref =~ s/:\/\/[^\/]@//;
my $refIdx = $ref;
$refIdx =~ s/\.jar$/.idx.jar/ if $splitJars;
my $refSnp = $ref;
$refSnp =~ s/\.jar$/.snp.jar/ if $splitJars;
my $refCmap = $ref;
$refCmap =~ s/\.jar$/.cmap.jar/ if $splitJars;
my $refSnpUpper = upperize($refSnp);
my $refCmapUpper = upperize($refCmap);
my $refIdxUpper = upperize($refIdx);

# Remove trailing slashes from output
$output =~ s/[\/]+$//;

my $hadoop = "";
my $hadoopStreamingJar = "";
if(!$localJob && !$hadoopJob) {
} elsif($hadoopJob) {
	# Look for hadoop script here on the master
	if($hadoop_arg eq "") {
		if(defined($ENV{HADOOP_HOME})) {
			$hadoop = "$ENV{HADOOP_HOME}/bin/hadoop";
			chomp($hadoop);
		}
		if($hadoop eq "" || system("$hadoop version 2>/dev/null >/dev/null") != 0) {
			$hadoop = `which hadoop 2>/dev/null`;
			chomp($hadoop);
		}
	} else {
		$hadoop = $hadoop_arg;
	}
	if(system("$hadoop version 2>/dev/null >/dev/null") != 0) {
		if($hadoop_arg ne "") {
			die "Specified --hadoop: '$hadoop_arg' cannot be run\n";
		} else {
			die "Cannot find working 'hadoop' in PATH or HADOOP_HOME/bin; please specify --hadoop\n";
		}
	}
	# Now look for hadoop streaming jar file here on the master
	my $hadoopHome;
	if($hadoopStreamingJar_arg eq "") {
		$hadoopHome = `dirname $hadoop`;
		$hadoopHome = `dirname $hadoopHome`;
		chomp($hadoopHome);
		$hadoopStreamingJar = "";
		my @hadoopStreamingJars = <$hadoopHome/contrib/streaming/hadoop-*-streaming.jar>;
		$hadoopStreamingJar = $hadoopStreamingJars[0] if scalar(@hadoopStreamingJars) > 0;
	} else {
		$hadoopStreamingJar = $hadoopStreamingJar_arg;
	}
	unless(-f $hadoopStreamingJar) {
		if($hadoopStreamingJar_arg ne "") {
			die "Specified --streaming-jar: '$hadoopStreamingJar_arg' cannot be found\n";
		} else {
			die "Cannot find streaming jar in $hadoopHome/contrib/streaming; please specify --streaming-jar\n";
		}
	}
	$hadoopStreamingJar =~ /hadoop-([^\/\\]*)-streaming.jar/;
	$hadoopVersion = $1;
	$hadoopVersion =~ s/\+.*$//; # trim patch indicator
} elsif($localJob) {
	system("sort < /dev/null") == 0 || die "Could not invoke 'sort'; is it in the PATH?\n";
}

# Set up the --samtools, --bowtie, and --R arguments for each script invocation
my $bowtie_arg = "";
my $samtools_arg = "";
my $soapsnp_arg = "";
if($localJob || $hadoopJob) {
	if($useSamtools) {
		$samtools ne "" || die;
		$msg->("$APP expects 'samtools' to be at path $samtools on the workers\n") if $hadoopJob;
		$samtools_arg = "--samtools $samtools";
	}

	if($useBowtie) {
		$bowtie ne "" || die;
		$msg->("$APP expects 'bowtie' to be at path $bowtie on the workers\n") if $hadoopJob;
		$bowtie_arg = "--bowtie $bowtie";
	}
	
	if($useSoapsnp) {
		$soapsnp ne "" || die;
		$msg->("$APP expects 'soapsnp' to be at path $soapsnp on the workers\n") if $hadoopJob;
		$soapsnp_arg = "--soapsnp $soapsnp";
	}
}

# Set up some variables to save us some typing:

my $cachef = cacheFile();
my $ec2CacheFiles =
qq!	"$cachef", "s3n://$appDir/Get.pm#Get.pm",
	"$cachef", "s3n://$appDir/Counters.pm#Counters.pm",
	"$cachef", "s3n://$appDir/Util.pm#Util.pm",
	"$cachef", "s3n://$appDir/Tools.pm#Tools.pm",
	"$cachef", "s3n://$appDir/AWS.pm#AWS.pm"!;

my $hadoopCacheFiles = qq! \\
	-file '$Bin/Get.pm' \\
	-file '$Bin/Counters.pm' \\
	-file '$Bin/Util.pm' \\
	-file '$Bin/Tools.pm' \\
	-file '$Bin/AWS.pm' \\
!;

my $inputPreproc = $input;
my $outputPreproc = ($preprocOutput ne "" ? $preprocOutput : "$intermediate/preproc");
$outputPreproc = $output if $justPreprocess;
my $outputPreprocUpper = upperize($outputPreproc);
my $bits = instanceTypeBits($instType);
$bits == 32 || $bits == 64 || die "Bad samtoolsBits: $bits\n";
my $forceStr = ($dontForce ? "" : "--force");
my $keepAllStr = $keepAll ? "--keep-all" : "";

my $preprocArgs = "";
$preprocArgs .= " --compress=$preprocCompress";
$preprocArgs .= " --stop=$preprocStop";
$preprocArgs .= " --maxperfile=$preprocMax";
$preprocArgs .= " --s";
$preprocArgs .= " --push=$outputPreprocUpper";

my $samtoolsCacheFiles = qq!"$cachef",   "s3n://$appDir/samtools$bits#samtools"!;
my $sraCacheFiles      = qq!"$cachef",   "s3n://$appDir/fastq-dump$bits#fastq-dump"!;

my $conf = confParam();

my $preprocessJson = qq!
{
  "Name": "Preprocess short reads",
  "ActionOnFailure": "$failAction",
  "HadoopJarStep": {
    "Jar": "/home/hadoop/contrib/streaming/hadoop-$hadoopVersion-streaming.jar",
    "Args": [
      "${conf}mapred.reduce.tasks=0",
      "-input",       "$inputPreproc",
      "-output",      "$outputPreproc",
      "-mapper",      "s3n://$appDir/Copy.pl $preprocArgs",
      "-inputformat", "org.apache.hadoop.mapred.lib.NLineInputFormat",
      $ec2CacheFiles,
      $sraCacheFiles,
      $samtoolsCacheFiles
    ]
  }
}!;

my $preprocessHadoop = qq!
echo ==========================
echo Stage \$phase of $numStages. Preprocess
echo ==========================
date
$hadoop jar $hadoopStreamingJar \\
	-D mapred.reduce.tasks=0 \\
	-input $inputPreproc \\
	-output $outputPreproc \\
	-mapper '$Bin/Copy.pl $samtools_arg $preprocArgs' \\
	$hadoopCacheFiles \\
	-inputformat org.apache.hadoop.mapred.lib.NLineInputFormat

[ \$? -ne 0 ] && echo "Non-zero exitlevel from Preprocess stage" && exit 1
phase=`expr \$phase + 1`
!;

my $preprocessSh = qq!
perl $Bin/MapWrap.pl \\
	--stage \$phase \\
	--num-stages $numStages \\
	--name Preprocess \\
	--input $inputPreproc \\
	--output $outputPreproc \\
	--counters ${output}_counters/counters.txt \\
	--messages cb.local.\$\$.out \\
	--line-by-line \\
	--silent-skipping \\
	$keepAllStr \\
	$forceStr \\
	--mappers $cores -- \\
		perl $Bin/Copy.pl \\
			--compress=$preprocCompress \\
			--stop=$preprocStop \\
			--maxperfile $preprocMax \\
			--push $outputPreproc \\
			--counters ${output}_counters/counters.txt

[ \$? -ne 0 ] && echo "Non-zero exitlevel from Preprocess stage" && exit 1
if [ \$phase -gt 1 -a $keepIntermediate -eq 0 -a $keepAll -eq 0 ] ; then
	echo "Removing $inputPreproc (to keep, specify --keep-all or --keep-intermediates)"
	rm -rf $inputPreproc
fi
phase=`expr \$phase + 1`
!;

my $inputAlign  = (($firstStage eq "align") ? $input  : $outputPreproc);
my $outputAlign = (($lastStage  eq "align") ? $output : "$intermediate/align");
$truncate = max($truncate, $truncateDiscard);
$truncateDiscard = $truncateDiscard > 0 ? "--discard-small" : "";

my $alignArgs = "";
$alignArgs .= " --discard-reads=$discardReads";
$alignArgs .= " --ref=$refIdxUpper";
$alignArgs .= " --destdir=$slaveTempdir";
$alignArgs .= " --partlen=$partitionLen";
$alignArgs .= " --qual=$qual";
$alignArgs .= " --truncate=$truncate";
$alignArgs .= " $truncateDiscard";
$alignArgs .= " --";
$alignArgs .= " --partition $partitionLen";
$alignArgs .= " --mm -t --hadoopout --startverbose";
$alignArgs .= " $bt_args";

my $alignJson = qq!
{
  "Name": "$APP Step 1: Align with Bowtie", 
  "ActionOnFailure": "$failAction", 
  "HadoopJarStep": { 
    "Jar": "/home/hadoop/contrib/streaming/hadoop-$hadoopVersion-streaming.jar", 
    "Args": [ 
      "${conf}mapred.reduce.tasks=0",
      "-input",       "$inputAlign",
      "-output",      "$outputAlign",
      "-mapper",      "s3n://$appDir/Align.pl $alignArgs",
      "$cachef",   "s3n://$appDir/bowtie$bits#bowtie",
      $ec2CacheFiles
    ] 
  }
}!;

my $alignHadoop = qq!
echo ==========================
echo Stage \$phase of $numStages. Align
echo ==========================
date
$hadoop jar $hadoopStreamingJar \\
	-D mapred.reduce.tasks=0 \\
	-input $inputAlign \\
	-output $outputAlign \\
	-mapper '$Bin/Align.pl $bowtie_arg $alignArgs' \\
	$hadoopCacheFiles

[ \$? -ne 0 ] && echo "Non-zero exitlevel from Align streaming job" && exit 1
phase=`expr \$phase + 1`
!;

my $preprocOutputSpecified = $preprocOutput ne "" ? "1" : "0";

my $alignSh = qq!
perl $Bin/MapWrap.pl \\
	--stage \$phase \\
	--num-stages $numStages \\
	--name Align \\
	--input $inputAlign \\
	--output $outputAlign \\
	--counters ${output}_counters/counters.txt \\
	--messages cb.local.\$\$.out \\
	$keepAllStr \\
	$forceStr \\
	--mappers $cores -- \\
		perl $Bin/Align.pl \\
			$bowtie_arg \\
			--discard-reads=$discardReads \\
			--index-local=$indexLocal \\
			--partlen=$partitionLen \\
			--qual=$qual \\
			--counters ${output}_counters/counters.txt \\
			--truncate=$truncate \\
			$truncateDiscard \\
			-- \\
			--partition $partitionLen \\
			--mm -t --hadoopout --startverbose \\
			$bt_args

[ \$? -ne 0 ] && echo "Non-zero exitlevel from Align stage" && exit 1
if [ \$phase -gt 1 -a $keepIntermediate -eq 0 -a $keepAll -eq 0 -a $preprocOutputSpecified -eq 0 ] ; then
	echo "Removing $inputAlign (to keep, specify --keep-all or --keep-intermediates)"
	rm -rf $inputAlign
fi
phase=`expr \$phase + 1`
!;

my $snpInput = "$intermediate/align";
my $snpOutput = "$intermediate/snps";

my $snpTasks = $numNodes * $reducersPerNode * 4;
my $snpArgs = "--discard-ref-bins=$discardRefBins ".
              "--refjar=$refSnpUpper ".
              "--destdir=$slaveTempdir ".
              "--soapsnp=$soapsnp ".
              "--args=$ss_args ".
              "--haploid_args=$ss_hap_args ".
              "--diploid_args=$ss_dip_args ".
              "--basequal=\! ".
              "--partition=$partitionLen ".
              "--haploids=$haploids ".
              "--replace-uscores";

my $inputSnp = ($resumeAlign ? $input: "$intermediate/align");
my $outputSnp = "$intermediate/snps";
my $snpsPartitionConf = partitionConf(2);
my $snpsJson = qq!
{
  "Name": "$APP Step 2: Call SNPs with SOAPsnp", 
  "ActionOnFailure": "$failAction", 
  "HadoopJarStep": { 
    "Jar": "/home/hadoop/contrib/streaming/hadoop-$hadoopVersion-streaming.jar", 
    "Args": [
      "${conf}stream.num.map.output.key.fields=3",
      "${conf}$snpsPartitionConf",
      "${conf}mapred.reduce.tasks=$snpTasks",
      "-input",       "$snpInput",
      "-output",      "$snpOutput",
      "-mapper",      "cat",
      "-reducer",     "s3n://$appDir/Soapsnp.pl $snpArgs",
      "-partitioner", "org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner",
      "$cachef",   "s3n://$appDir/soapsnp$bits#soapsnp",
      $ec2CacheFiles
    ] 
  }
}!;

my $snpsHadoop = qq!
echo ==========================
echo Stage \$phase of $numStages. Call SNPs
echo ==========================
date
$hadoop jar $hadoopStreamingJar \\
	-D stream.num.map.output.key.fields=3 \\
	-D $snpsPartitionConf \\
	-D mapred.reduce.tasks=$snpTasks \\
	-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \\
	-input $inputSnp \\
	-output $outputSnp \\
	-mapper 'cat' \\
	-reducer '$Bin/Soapsnp.pl $soapsnp_arg $snpArgs' \\
	$hadoopCacheFiles

[ \$? -ne 0 ] && echo "Non-zero exitlevel from Call SNPs streaming job" && exit 1
phase=`expr \$phase + 1`
!;

$externalSort = $externalSort ? "--external-sort" : "";
my $snpsSh = qq!
perl $Bin/ReduceWrap.pl \\
	--stage \$phase \\
	--num-stages $numStages \\
	--name "Call SNPs" \\
	--input $snpInput \\
	--output $snpOutput \\
	--counters ${output}_counters/counters.txt \\
	--messages cb.local.\$\$.out \\
	--reducers $cores \\
	--tasks $snpTasks \\
	--bin-fields 2 \\
	--sort-fields 3 \\
	--max-sort-records $maxSortRecords \\
	--max-sort-files $maxSortFiles \\
	$externalSort \\
	$keepAllStr \\
	$forceStr \\
	-- \\
		perl $Bin/Soapsnp.pl \\
			$soapsnp_arg \\
			--discard-ref-bins=$discardRefBins \\
			--args="$ss_args" \\
			--snpdir="$snpsLocal" \\
			--refdir="$sequencesLocal" \\
			--haploid_args="$ss_hap_args" \\
			--diploid_args="$ss_dip_args" \\
			--basequal=\! \\
			--partition=$partitionLen \\
			--haploids="$haploids" \\
			--counters ${output}_counters/counters.txt \\
			--replace-uscores

[ \$? -ne 0 ] && echo "Non-zero exitlevel from SNP calling stage" && exit 1
if [ \$phase -gt 1 -a $keepIntermediate -eq 0 -a $keepAll -eq 0 ] ; then
	echo "Removing $snpInput (to keep, specify --keep-all or --keep-intermediates)"
	rm -rf $snpInput
fi
phase=`expr \$phase + 1`
!;

my $inputDummy = "s3n://$app-emr/dummy-input";
my $outputUpper = upperize($output);
my $countersArgs = "";
$countersArgs   .= " --output=${outputUpper}_${app}_counters";

my $countersJson = qq!
{
  "Name": "Get counters", 
  "ActionOnFailure": "$failAction", 
  "HadoopJarStep": { 
    "Jar": "/home/hadoop/contrib/streaming/hadoop-$hadoopVersion-streaming.jar", 
    "Args": [ 
      "${conf}mapred.reduce.tasks=1",
      "-input",       "$inputDummy",
      "-output",      "${output}_${app}_counters/ignoreme1",
      "-mapper",      "cat",
      "-reducer",     "s3n://$appDir/Counters.pl $countersArgs",
      $ec2CacheFiles
    ]
  }
}!;
my $countersSh = qq!
!;

my $inputPostproc = "$intermediate/snps";
my $outputPostproc = "$output/${app}_results";

my $postprocArgs = "";
$postprocArgs   .= " --cmapjar=$refCmapUpper";
$postprocArgs   .= " --destdir=$slaveTempdir";
$postprocArgs   .= " --output=$outputUpper";

my $postprocPartitionConf = partitionConf(1);
my $postprocJson = qq!
{
  "Name": "$APP Step 3: Postprocess", 
  "ActionOnFailure": "$failAction", 
  "HadoopJarStep": { 
    "Jar": "/home/hadoop/contrib/streaming/hadoop-$hadoopVersion-streaming.jar", 
    "Args": [ 
      "${conf}stream.num.map.output.key.fields=2",
      "${conf}$postprocPartitionConf",
      "${conf}mapred.reduce.tasks=30",
      "-input",       "$inputPostproc", 
      "-output",      "$output/ignoreme2",
      "-mapper",      "cat", 
      "-reducer",     "s3n://$appDir/CBFinish.pl $postprocArgs",
      "-partitioner", "org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner", 
      $ec2CacheFiles
    ] 
  }
}!;

my $postprocHadoop = qq!
echo ==========================
echo Stage \$phase of $numStages. Postprocess
echo ==========================
date
$hadoop jar $hadoopStreamingJar \\
	-D stream.num.map.output.key.fields=2 \\
	-D $postprocPartitionConf \\
	-D mapred.reduce.tasks=30 \\
	-input $inputPostproc \\
	-output $output/ignoreme2 \\
	-mapper 'cat' \\
	-reducer '$Bin/CBFinish.pl $postprocArgs' \\
	$hadoopCacheFiles \\
	-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner

rm -rf $output/ignoreme2
[ \$? -ne 0 ] && echo "Non-zero exitlevel from Postprocess streaming job" && exit 1
phase=`expr \$phase + 1`
!;

my $postprocSh = qq!
perl $Bin/ReduceWrap.pl \\
	--stage \$phase \\
	--num-stages $numStages \\
	--name Postprocess \\
	--input $inputPostproc \\
	--output $outputPostproc \\
	--counters ${output}_counters/counters.txt \\
	--messages cb.local.\$\$.out \\
	--reducers $cores \\
	--tasks 1 \\
	--bin-fields 1 \\
	--sort-fields 2 \\
	--max-sort-records $maxSortRecords \\
	--max-sort-files $maxSortFiles \\
	$externalSort \\
	$keepAllStr \\
	$forceStr \\
	-- \\
		perl $Bin/CBFinish.pl \\
			--cmap=$cmapLocal \\
			--counters ${output}_counters/counters.txt \\
			--output="$outputPostproc"

[ \$? -ne 0 ] && echo "Non-zero exitlevel from Postprocess stage" && exit 1
if [ \$phase -gt 1 -a $keepIntermediate -eq 0 -a $keepAll -eq 0 ] ; then
	echo "Removing $inputPostproc (to keep, specify --keep-all or --keep-intermediates)"
	rm -rf $inputPostproc
fi
phase=`expr \$phase + 1`
!;

my $jsonFile = "$scriptTempdir/cb.$$.json";
my $runJsonFile = "$scriptTempdir/cb.$$.json.sh";
my $runHadoopFile = "$scriptTempdir/cb.$$.hadoop.sh";
my $runLocalFile = "$scriptTempdir/cb.$$.sh";
umask 0077;
my $json = "";
open JSON, ">$jsonFile" || die "Error: Could not open $jsonFile for writing\n";
my $sh = "";
open SH, ">$runLocalFile" || die "Error: Could not open $runLocalFile for writing\n";
my $had = "";
open HADOOP, ">$runHadoopFile" || die "Error: Could not open $runHadoopFile for writing\n";
$json .= "[";
$sh .= "#!/bin/sh\n\nphase=1\n";
$sh .= "rm -f cb.local.\$\$.out\n";
$sh .= qq!
perl $Bin/CheckDirs.pl \\
	--input $input \\
	--intermediate $intermediate \\
	--output $output \\
	--counters ${output}_counters \\
	--messages cb.local.\$\$.out \\
	$forceStr
!;
$had .= "#!/bin/sh\n\nphase=1\n";
#$had .= "rm -f cb.hadoop.\$\$.out\n";
if($stages{preprocess}) {
	$json .= "," if $json ne "[";
	$json .= $preprocessJson;
	$had .= $preprocessHadoop;
	$sh .= $preprocessSh;
}
if($stages{align}) {
	$json .= "," if $json ne "[";
	$json .= $alignJson;
	$had .= $alignHadoop;
	$sh .= $alignSh;
}
if($stages{snps}) {
	$json .= "," if $json ne "[";
	$json .= $snpsJson;
	$had .= $snpsHadoop;
	$sh .= $snpsSh;
}
if($stages{postprocess}) {
	$json .= "," if $json ne "[";
	$json .= $postprocJson;
	$had .= $postprocHadoop;
	$sh .= $postprocSh;
}
$json .= "," if $json ne "[";
$json .= $countersJson;
$sh .= "echo \"All output to console recorded in cb.local.\$\$.out\"\n";
$sh .= "date ; echo DONE\n";
#$had .= "echo \"All output to console recorded in cb.hadoop.\$\$.out\"\n";
$had .= "date ; echo DONE\n";
$json .= "\n]\n";
print JSON $json;
close(JSON);
print SH $sh;
close(SH);
print HADOOP $had;
close(HADOOP);
umask $umaskOrig;

if(!$localJob && !$hadoopJob) {
	$cores == 1 || $cores == 2 || $cores == 4 || $cores == 8 || die "Bad number of cores: $cores\n";
}
$name =~ s/"//g;
(defined($emrScript) && $emrScript ne "") || $localJob || $hadoopJob || die;
my $cmdJson = qq!$emrScript \\
    $credentials \\
    --create \\
    $emrArgs \\
    --name "$name" \\
    --num-instances $numNodes \\
    --instance-type $instType \\
    --json $jsonFile \\
    --bootstrap-action s3://elasticmapreduce/bootstrap-actions/configurations/latest/memory-intensive \\
    --bootstrap-name "Set memory-intensive mode" \\
    --bootstrap-action s3://elasticmapreduce/bootstrap-actions/configure-hadoop \\
    --bootstrap-name "Configure Hadoop" \\
      --args "-s,mapred.job.reuse.jvm.num.tasks=1,-s,mapred.tasktracker.reduce.tasks.maximum=$cores,-s,io.sort.mb=100" \\
    --bootstrap-action s3://elasticmapreduce/bootstrap-actions/add-swap \\
    --bootstrap-name "Add Swap" \\
      --args "$swap"
!;

my $cmdSh = "sh $runLocalFile";
my $cmdHadoop = "sh $runHadoopFile";

if($dryrun) {
	open RUN, ">$runJsonFile" || die "Error: Could not open $runJsonFile for writing\n";
	print RUN "#!/bin/sh\n";
	print RUN $cmdJson; # include argument passthrough
	close(RUN);
}

$msg->("\n");
$msg->("$APP job\n");
$msg->("------------\n");
$msg->("Job json in: $jsonFile\n") if (!$localJob && !$hadoopJob);
$msg->("Job command in: $runJsonFile\n") if (!$localJob && !$hadoopJob && $dryrun);
$msg->("Local commands in: $runLocalFile\n") if $localJob;
$msg->("Hadoop streaming commands in: $runHadoopFile\n") if $hadoopJob;
if($dryrun) {
	$msg->("Exiting without running command because of --dryrun\n");
} else {
	$msg->("Running...\n");
	my $pipe;
	if($localJob) {
		$pipe = "$cmdSh 2>&1 |";
	} elsif($hadoopJob) {
		$pipe = "$cmdHadoop 2>&1 |";
	} else {
		$pipe = "$cmdJson 2>&1 |";
	}
	open(CMDP, $pipe) || die "Could not open pipe '$pipe' for reading\n";
	while(<CMDP>) { $msg->($_); }
	close(CMDP);
}
$msg->("$warnings warnings\n") if $warnings > 0;

}

1;
