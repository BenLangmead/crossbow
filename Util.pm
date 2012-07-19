#!/usr/bin/perl -w

##
# Author: Ben Langmead
#   Date: 3/12/2010
#
# Various utility functions.
#

package Util;
use strict;
use warnings;
use POSIX ":sys_wait_h";
use FindBin qw($Bin); 
use lib $Bin;
use Tools;

##
# Parse a URL, extracting the protocol and type of program that will
# be needed to download it.
#
sub parse_url_proto($) {
	my @s = split(/[:]/, $_[0]);
	defined($s[0]) || return "local";
	if($s[0] =~ /^s3n?/i) {
		return "s3";
	} elsif($s[0] =~ /^hdfs/i) {
		return "hdfs";
	} else {
		return "local";
	}
}

##
# Return true iff given url is local.
#
sub is_local($) {
	return parse_url_proto($_[0]) eq "local";
}

##
# Print command to stderr, run it, return its exitlevel.
#
sub run($) {
	my $cmd = shift;
	print STDERR "$cmd\n";
	return system($cmd);
}

##
# Run given command and wait for it to finish, printing wait messages
# to stderr periodically.  Return its exitlevel.
#
sub runAndWait($$) {
	my ($cmd, $shortname) = @_;
	print STDERR "$cmd\n";
	my $f = fork();
	if($f == 0) {
		# Run the command, echoing its stdout to our stdout
		open(CMD, "$cmd |");
		while(<CMD>) { print $_; }
		close(CMD);
		# Check its exitlevel
		my $ret = $?;
		# Write its exitlevel to a file.  TODO: is there a better way
		# to do this?
		open(OUT, ">.Util.pm.$$") || die "Could not open .Util.pm.$$ for writing\n";
		print OUT "$ret\n";
		close(OUT);
		exit $ret;
	}
	print STDERR "runAndWait: Child's PID is $f\n";
	my $ret;
	my $cnt = 0;
	while(1) {
		$ret = waitpid(-1, &WNOHANG);
		last if $ret == $f;
		sleep (5);
		my $secs = ++$cnt * 5;
		print STDERR "Waiting for $shortname (it's been $secs secs)...\n";
	}
	my $lev = int(`cat .Util.pm.$ret`);
	unlink(".Util.pm.$ret");
	return $lev;
}

##
# Run given command, return its output.
#
sub backtickRun($) {
	my ($cmd) = @_;
	print STDERR "$cmd\n";
	return `$cmd`;
}

##
# Run given command and wait for it to finish, printing wait messages
# to stderr periodically.  Return its output.
#
sub backtickAndWait($$) {
	my ($cmd, $shortname) = @_;
	print STDERR "$cmd\n";
	my $f = fork();
	if($f == 0) {
		open(TMP, ">.tmp.Get.pm") || die;
		open(CMD, "$cmd |") || die;
		while(<CMD>) { print TMP $_; }
		close(CMD);
		my $ret = $?;
		close(TMP);
		exit $ret;
	}
	print STDERR "runAndWait: Child's PID is $f\n";
	my $ret;
	my $cnt = 0;
	while(1) {
		$ret = waitpid(-1, &WNOHANG);
		last if $ret == $f;
		sleep (5);
		my $secs = ++$cnt * 5;
		print STDERR "Waiting for $shortname (it's been $secs secs)...\n";
	}
	return `cat .tmp.Get.pm`;
}

##
# Return version of argument with leading and trailing whitespace
# removed.
#
sub trim($) {
	my $string = shift;
	$string =~ s/^\s+//;
	$string =~ s/\s+$//;
	return $string;
}

1;
