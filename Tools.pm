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

our $s3cmd_arg = "";
our $s3cmd = "";
our $s3cfg = "";
our $hadoop_arg = "";
our $hadoop = "";
our $sra_conv_arg = "";
our $sra_conv = "";
our $samtools_arg = "";
our $samtools = "";
our $jar = "";
our $jar_arg = "";
our $wget = "";
our $wget_arg = "";
our $md5 = "";
our $md5_arg = "";
my $r = "";

my $hadoopEnsured = 0;
sub ensureHadoop() {
	return if $hadoopEnsured;
	$hadoop = $hadoop_arg if $hadoop_arg ne "";
	if(system("$hadoop -version >&2") != 0) {
		if($hadoop_arg ne "") {
			die "--hadoop argument \"$hadoop\" doesn't exist or isn't executable\n";
		} else {
			die "hadoop could not be found in HADOOP_HOME or PATH; please specify --hadoop\n";
		}
	}
	$hadoopEnsured = 1;
}
sub hadoop() { ensureHadoop(); return $hadoop; }

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

my $sraEnsured = 0;
sub ensureSRAConvert() {
	return if $sraEnsured;
	$sra_conv = $sra_conv_arg if $sra_conv_arg ne "";
	my $ret = system("$sra_conv -H >&2 >/dev/null") >> 8;
	if($ret != 4) {
		if($sra_conv_arg ne "") {
			die "--sraconv argument \"$sra_conv\" doesn't exist or isn't executable\n";
		} else {
			die "fastq-dump could not be found in SRATOOLKIT_HOME or PATH; please specify --sraconv\n";
		}
	}
	$sraEnsured = 1;
}
sub sra() { ensureSRAConvert(); return $sra_conv; }

##
# Write a temporary s3cfg file with appropriate keys.
#
sub writeS3cfg() {
	AWS::ensureKeys($hadoop, $hadoop_arg);
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
sub ensureS3cmd() {
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
		writeS3cfg() unless -f ".s3cfg";
		$s3cfg = ".s3cfg";
	}
	$s3cmdEnsured = 1;
}
sub s3cmd() { ensureS3cmd(); return "$s3cmd -c $s3cfg"; }

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

sub initTools() {
	if(defined($ENV{JAVA_HOME})) {
		$jar = "$ENV{JAVA_HOME}/bin/jar";
		unless(-x $jar) { $jar = "" };
	}
	if($jar eq "") {
		$jar = `which jar 2>/dev/null`;
		chomp($jar);
		unless(-x $jar) { $jar = "" };
	}
	
	if(defined($ENV{S3CMD_HOME})) {
		$s3cmd = "$ENV{S3CMD_HOME}/s3cmd";
		unless(-x $s3cmd) { $s3cmd = "" };
	}
	if($s3cmd eq "") {
		$s3cmd = `which s3cmd 2>/dev/null`;
		chomp($s3cmd);
		unless(-x $s3cmd) { $s3cmd = "" };
	}
	
	if(defined($ENV{HADOOP_HOME})) {
		$hadoop = "$ENV{HADOOP_HOME}/bin/hadoop";
		unless(-x $hadoop) { $hadoop = "" };
	}
	if($hadoop eq "") {
		$hadoop = `which hadoop 2>/dev/null`;
		chomp($hadoop);
		unless(-x $hadoop) { $hadoop = "" };
	}

	if(defined($ENV{SRATOOLKIT_HOME})) {
		$sra_conv = "$ENV{SRATOOLKIT_HOME}/fastq-dump";
		unless(-x $sra_conv) { $sra_conv = "" };
	}
	if($sra_conv eq "") {
		$sra_conv = `which fastq-dump 2>/dev/null`;
		chomp($sra_conv);
		unless(-x $sra_conv) { $sra_conv = "" };
	}
	if($sra_conv eq "") {
		$sra_conv = "./fastq-dump";
		chomp($sra_conv);
		unless(-x $sra_conv) { $sra_conv = "" };
	}

	if(defined($ENV{SAMTOOLS_HOME})) {
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
	
	if(defined($ENV{R_HOME})) {
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
	
	$md5 = `which md5 2>/dev/null`;
	chomp($md5);
	$md5 = "" unless(-x $md5);
	if($md5 eq "") {
		$md5 = `which md5sum 2>/dev/null`;
		chomp($md5);
		$md5 = "" unless(-x $md5);
	}
	
	$wget = `which wget 2>/dev/null`;
	chomp($wget);
	unless(-x $wget) { $wget = "" };
	
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
