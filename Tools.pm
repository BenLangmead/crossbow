#!/usr/bin/perl -w

##
# Author: Ben Langmead
#   Date: 2/14/2010
#
# Routines for getting and expanding jars from
#

package Tools;
use strict;
use warnings;
use AWS;
use FindBin qw($Bin);

# Prefix to use for environment variables.  E.g. in Myrna, we dont look for
# MYRNA_FASTQ_DUMP_HOME before we look for FASTQ_DUMP_HOME.
our $pre = "";

our $s3cmd_arg = "";
our $s3cmd = "";
our $s3cfg = "";
our $hadoop_arg = "";
our $hadoop = "";
our $fastq_dump_arg = "";
our $fastq_dump = "";
our $soapsnp_arg = "";
our $soapsnp = "";
our $samtools_arg = "";
our $samtools = "";
our $bowtie_arg = "";
our $bowtie = "";
our $jar = "";
our $jar_arg = "";
our $wget = "";
our $wget_arg = "";
our $md5 = "";
our $md5_arg = "";
our $r = "";
our $r_arg = "";
our $unzip = "";

my $hadoopEnsured = 0;
sub ensureHadoop() {
	return if $hadoopEnsured;
	$hadoop = $hadoop_arg if $hadoop_arg ne "";
	if(system("$hadoop version >&2") != 0) {
		if($hadoop_arg ne "") {
			die "--hadoop argument \"$hadoop\" doesn't exist or isn't executable\n";
		} else {
			die "hadoop could not be found in HADOOP_HOME or PATH; please specify --hadoop\n";
		}
	}
	$hadoopEnsured = 1;
}
sub hadoop() { ensureHadoop(); return $hadoop; }

# Bowtie
my $bowtieEnsured = 0;
sub ensureBowtie() {
	return if $bowtieEnsured;
	$bowtie = $bowtie_arg if $bowtie_arg ne "";
	if(! -x $bowtie) {
		if($bowtie_arg ne "") {
			die "--bowtie argument \"$bowtie\" doesn't exist or isn't executable\n";
		} else {
			die "bowtie could not be found in BOWTIE_HOME or PATH; please specify --bowtie\n";
		}
	}
	$bowtieEnsured = 1;
}
sub bowtie() { ensureBowtie(); return $bowtie; }

# SOAPsnp
my $soapsnpEnsured = 0;
sub ensureSoapsnp() {
	return if $soapsnpEnsured;
	$soapsnp = $soapsnp_arg if $soapsnp_arg ne "";
	if(! -x $soapsnp) {
		if($soapsnp_arg ne "") {
			die "--soapsnp argument \"$soapsnp\" doesn't exist or isn't executable\n";
		} else {
			die "soapsnp could not be found in SOAPSNP_HOME or PATH; please specify --soapsnp\n";
		}
	}
	$soapsnpEnsured = 1;
}
sub soapsnp() { ensureSoapsnp(); return $soapsnp; }

my $samtoolsEnsured = 0;
sub ensureSamtools() {
	return if $samtoolsEnsured;
	$samtools = $samtools_arg if $samtools_arg ne "";
	if(! -x $samtools) {
		if($samtools_arg ne "") {
			die "--samtools argument \"$samtools\" doesn't exist or isn't executable\n";
		} else {
			die "samtools could not be found in SAMTOOLS_HOME or PATH; please specify --samtools\n";
		}
	}
	$samtoolsEnsured = 1;
}
sub samtools() { ensureSamtools(); return $samtools; }

my $fqdumpEnsured = 0;
sub ensureFastqDump() {
	return if $fqdumpEnsured;
	$fastq_dump = $fastq_dump_arg if $fastq_dump_arg ne "";
	my $ret = 0;
	if($fastq_dump ne "") {
		$ret = system("$fastq_dump -h >&2 >/dev/null") >> 8;
	}
	if($ret != 0) {
		if($fastq_dump_arg ne "") {
			die "--fastq-dump argument \"$fastq_dump\" doesn't exist or isn't executable\n";
		} else {
			die "fastq-dump could not be found in FASTQ_DUMP_HOME or PATH; please specify --fastq-dump\n";
		}
	}
	$fqdumpEnsured = 1;
}
sub fastq_dump() { ensureFastqDump(); return $fastq_dump; }

##
# Write a temporary s3cfg file with appropriate keys.
#
sub writeS3cfg($) {
	my ($env) = @_;
	AWS::ensureKeys($hadoop, $hadoop_arg, $env);
	my $cfgText = qq{
[default]
access_key = $AWS::accessKey
secret_key = $AWS::secretKey
acl_public = False
bucket_location = US
debug_syncmatch = False
default_mime_type = binary/octet-stream
delete_removed = False
dry_run = False
encrypt = False
force = False
gpg_command = /usr/bin/gpg
gpg_decrypt = \%(gpg_command)s -d --verbose --no-use-agent --batch --yes --passphrase-fd \%(passphrase_fd)s -o \%(output_file)s \%(input_file)s
gpg_encrypt = \%(gpg_command)s -c --verbose --no-use-agent --batch --yes --passphrase-fd \%(passphrase_fd)s -o \%(output_file)s \%(input_file)s
gpg_passphrase = 
guess_mime_type = False
host_base = s3.amazonaws.com
host_bucket = \%(bucket)s.s3.amazonaws.com
human_readable_sizes = False
preserve_attrs = True
proxy_host = 
proxy_port = 0
recv_chunk = 4096
send_chunk = 4096
simpledb_host = sdb.amazonaws.com
use_https = False
verbosity = WARNING
};
	open S3CFG, ">.s3cfg" || die "Could not open .s3cfg\n";
	print S3CFG $cfgText;
	close(S3CFG);
}

my $s3cmdEnsured = 0;
sub ensureS3cmd($) {
	my ($env) = @_;
	return if $s3cmdEnsured;
	$s3cmd = $s3cmd_arg if $s3cmd_arg ne "";
	if(system("$s3cmd --version >&2") != 0) {
		if($s3cmd_arg ne "") {
			die "-s3cmd argument \"$s3cmd\" doesn't exist or isn't executable\n";
		} else {
			die "s3cmd could not be found in S3CMD_HOME or PATH; please specify -s3cmd\n";
		}
	}
	if($s3cfg eq "") {
		writeS3cfg($env) unless -f ".s3cfg";
		$s3cfg = ".s3cfg";
	}
	$s3cmdEnsured = 1;
}
sub s3cmd($) { ensureS3cmd($_[0]); return "$s3cmd -c $s3cfg"; }

my $md5Ensured = 0;
sub ensureMd5() {
	return if $md5Ensured;
	$md5 = $md5_arg if $md5_arg ne "";
	unless(-x $md5) {
		if($md5_arg ne "") {
			die "-md5 argument \"$md5\" doesn't exist or isn't executable\n";
		} else {
			die "md5 or md5sum could not be found in PATH; please specify -md5\n";
		}
	}
	$md5Ensured = 1;
}
sub md5() { ensureMd5(); return $md5; }

my $wgetEnsured = 0;
sub ensureWget() {
	return if $wgetEnsured;
	$wget = $wget_arg if $wget_arg ne "";
	unless(-x $wget) {
		if($wget_arg ne "") {
			die "-wget argument \"$wget_arg\" doesn't exist or isn't executable\n";
		} else {
			die "wget could not be found in PATH; please specify -wget\n";
		}
	}
	$wgetEnsured = 1;
}
sub wget() { ensureWget(); return $wget; }

my $jarEnsured = 0;
sub ensureJar() {
	return if $jarEnsured;
	$jar = $jar_arg if $jar_arg ne "";
	unless(-x $jar) {
		if($jar_arg ne "") {
			die "-jar argument \"$jar_arg\" doesn't exist or isn't executable\n";
		} else {
			die "jar could not be found in PATH; please specify -jar\n";
		}
	}
	$jarEnsured = 1;
}
sub jar() { ensureJar(); return $jar; }

# Rscript
my $rscriptEnsured = 0;
sub ensureRscript() {
	return if $rscriptEnsured;
	$r = $r_arg if $r_arg ne "";
	if(! -x $r) {
		if($r_arg ne "") {
			die "--R argument \"$r_arg\" doesn't exist or isn't executable\n";
		} else {
			die "Rscript could not be found in R_HOME or PATH; please specify --R\n";
		}
	}
	$rscriptEnsured = 1;
}
sub Rscript() { ensureRscript(); return $r; }

sub unzip(){ return $unzip; }

sub initTools() {

	# Read the tool name from the 'TOOLNAME' file.  We'll use an all-caps
	# version of this as our environment variable prefix.
	if(open(NAME, "$Bin/TOOLNAME")) {
		$pre = <NAME>;
		$pre =~ s/^\s*//;
		$pre =~ s/\s*$//;
		$pre = uc $pre;
		$pre .= "_";
		close(NAME);
	} else {
		$pre = "";
		print STDERR "Warning: No TOOLNAME file in tool directory: Bin\n";
	}
	
	#
	# jar
	#
	
	if($pre ne "" && defined($ENV{"${pre}JAVA_HOME"})) {
		my $h = $ENV{"${pre}JAVA_HOME"};
		$jar = "$h/bin/jar";
		unless(-x $jar) { $jar = "" };
	}
	elsif(defined($ENV{JAVA_HOME})) {
		$jar = "$ENV{JAVA_HOME}/bin/jar";
		unless(-x $jar) { $jar = "" };
	}
	if($jar eq "") {
		$jar = `which jar 2>/dev/null`;
		chomp($jar);
		unless(-x $jar) { $jar = "" };
	}
	
	##unzip
	if($unzip eq ""){
	    $unzip = `which unzip 2>/dev/null`;
	    chomp($unzip);
	    unless(-x $unzip){ $unzip = "" };
	}

	
	#
	# s3cmd
	#

	if($pre ne "" && defined($ENV{"${pre}S3CMD_HOME"})) {
		my $h = $ENV{"${pre}S3CMD_HOME"};
		$s3cmd = "$h/s3cmd";
		unless(-x $s3cmd) { $s3cmd = "" };
	}
	elsif(defined($ENV{S3CMD_HOME})) {
		$s3cmd = "$ENV{S3CMD_HOME}/s3cmd";
		unless(-x $s3cmd) { $s3cmd = "" };
	}
	if($s3cmd eq "") {
		$s3cmd = `which s3cmd 2>/dev/null`;
		chomp($s3cmd);
		unless(-x $s3cmd) { $s3cmd = "" };
	}

	#
	# hadoop
	#

	if($pre ne "" && defined($ENV{"${pre}HADOOP_HOME"})) {
		my $h = $ENV{"${pre}HADOOP_HOME"};
		$hadoop = "$h/bin/hadoop";
		unless(-x $hadoop) { $hadoop = "" };
	}
	elsif(defined($ENV{HADOOP_HOME})) {
		$hadoop = "$ENV{HADOOP_HOME}/bin/hadoop";
		unless(-x $hadoop) { $hadoop = "" };
	}
	if($hadoop eq "") {
		$hadoop = `which hadoop 2>/dev/null`;
		chomp($hadoop);
		unless(-x $hadoop) { $hadoop = "" };
	}

	#
	# fastq-dump
	#
	if($pre ne "" && defined($ENV{"${pre}FASTQ_DUMP_HOME"})) {
		my $h = $ENV{"${pre}FASTQ_DUMP_HOME"};
		$fastq_dump = "$h/fastq-dump";
		unless(-x $fastq_dump) { $fastq_dump = "" };
	}
	elsif(defined($ENV{FASTQ_DUMP_HOME})) {
		$fastq_dump = "$ENV{FASTQ_DUMP_HOME}/fastq-dump";
		unless(-x $fastq_dump) { $fastq_dump = "" };
	}
	if($fastq_dump eq "") {
		$fastq_dump = `which fastq-dump 2>/dev/null`;
		chomp($fastq_dump);
		unless(-x $fastq_dump) { $fastq_dump = "" };
	}
	if($fastq_dump eq "") {
		$fastq_dump = "./fastq-dump";
		chomp($fastq_dump);
		unless(-x $fastq_dump) { $fastq_dump = "" };
	}

	#
	# bowtie
	#

	if($pre ne "" && defined($ENV{"${pre}BOWTIE_HOME"})) {
		my $h = $ENV{"${pre}BOWTIE_HOME"};
		$bowtie = "$h/bowtie";
		unless(-x $bowtie) { $bowtie = "" };
	}
	elsif(defined($ENV{BOWTIE_HOME})) {
		$bowtie = "$ENV{BOWTIE_HOME}/bowtie";
		unless(-x $bowtie) { $bowtie = "" };
	}
	if($bowtie eq "") {
		$bowtie = `which bowtie 2>/dev/null`;
		chomp($bowtie);
		unless(-x $bowtie) { $bowtie = "" };
	}
	if($bowtie eq "" && -f "./bowtie") {
		$bowtie = "./bowtie";
		chomp($bowtie);
		chmod 0777, $bowtie;
		unless(-x $bowtie) { $bowtie = "" };
	}

	#
	# soapsnp
	#

	if($pre ne "" && defined($ENV{"${pre}SOAPSNP_HOME"})) {
		my $h = $ENV{"${pre}SOAPSNP_HOME"};
		$soapsnp = "$h/soapsnp";
		unless(-x $soapsnp) { $soapsnp = "" };
	}
	elsif(defined($ENV{SOAPSNP_HOME})) {
		$soapsnp = "$ENV{SOAPSNP_HOME}/soapsnp";
		unless(-x $soapsnp) { $soapsnp = "" };
	}
	if($soapsnp eq "") {
		$soapsnp = `which soapsnp 2>/dev/null`;
		chomp($soapsnp);
		unless(-x $soapsnp) { $soapsnp = "" };
	}
	if($soapsnp eq "" && -f "./soapsnp") {
		$soapsnp = "./soapsnp";
		chomp($soapsnp);
		chmod 0777, $soapsnp;
		unless(-x $soapsnp) { $soapsnp = "" };
	}

	#
	# samtools
	#

	if($pre ne "" && defined($ENV{"${pre}SAMTOOLS_HOME"})) {
		my $h = $ENV{"${pre}SAMTOOLS_HOME"};
		$samtools = "$h/samtools";
		unless(-x $samtools) { $samtools = "" };
	}
	elsif(defined($ENV{SAMTOOLS_HOME})) {
		$samtools = "$ENV{SAMTOOLS_HOME}/samtools";
		unless(-x $samtools) { $samtools = "" };
	}
	if($samtools eq "") {
		$samtools = `which samtools 2>/dev/null`;
		chomp($samtools);
		unless(-x $samtools) { $samtools = "" };
	}
	if($samtools eq "") {
		$samtools = "./samtools";
		chomp($samtools);
		unless(-x $samtools) { $samtools = "" };
	}

	#
	# Rscript
	#

	if($pre ne "" && defined($ENV{"${pre}R_HOME"})) {
		my $h = $ENV{"${pre}R_HOME"};
		$r = "$h/bin/Rscript";
		unless(-x $r) { $r = "" };
	}
	elsif(defined($ENV{R_HOME})) {
		$r = "$ENV{R_HOME}/bin/Rscript";
		unless(-x $r) { $r = "" };
	}
	if($r eq "") {
		$r = `which Rscript 2>/dev/null`;
		chomp($r);
		unless(-x $r) { $r = "" };
	}
	if($r eq "" && -x "Rscript") {
		$r = "Rscript";
	}
	
	# md5/md5sum, for checking integrity of downloaded files
	$md5 = `which md5 2>/dev/null`;
	chomp($md5);
	$md5 = "" unless(-x $md5);
	if($md5 eq "") {
		$md5 = `which md5sum 2>/dev/null`;
		chomp($md5);
		$md5 = "" unless(-x $md5);
	}
	
	# wget, for downloading files over http or ftp
	$wget = `which wget 2>/dev/null`;
	chomp($wget);
	unless(-x $wget) { $wget = "" };
	
	# expand s3cmd if it's present
	if(-f "s3cmd.tar.gz") {
		system("tar zxvf s3cmd.tar.gz >/dev/null");
	}
}

##
# Look (a) relative to an environment variable, (b) in the path, and
# (c) in the current directory for an executable.  Return where we
# found it, or "" if we didn't.
#
sub lookFor($$$) {
	my ($exe, $env, $envsub) = @_;
	my $tool = "";
	if(defined($ENV{$env})) {
		$tool = "$ENV{$env}/$envsub";
		unless(-x $tool) { $tool = "" };
	}
	if($tool eq "") {
		$tool = `which $exe 2>/dev/null`;
		chomp($tool);
		unless(-x $tool) { $tool = "" };
	}
	$tool = "./$exe" if ($tool eq "" && -x "./$exe");
	return $tool;
}

##
# Purge the environment down to a few essentials.  This fixes an issue
# whereby some environment changes made by hadoop.sh mess with future
# invocations of hadoop.
#
sub purgeEnv() {
	foreach my $k (keys %ENV) {
		next if $k eq "PATH";
		next if $k eq "PWD";
		next if $k eq "HOME";
		next if $k eq "USER";
		next if $k eq "TERM";
		next if $k eq "JAVA_HOME";
		delete $ENV{$k};
	}
	$ENV{SHELL}="/bin/sh";
}

##
# Given a bowtie argument string, look for obvious problems.
#
sub checkBowtieParams($$) {
	my ($args, $version) = @_;
	return 1;
}

##
# Given a bowtie argument string, look for obvious problems.
#
sub checkSoapsnpParams($$) {
	my ($args, $version) = @_;
	return 1;
}

1;
