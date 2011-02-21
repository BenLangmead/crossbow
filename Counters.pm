#!/usr/bin/perl -w

##
# Counters.pl
#
#  Authors: Ben Langmead
#     Date: February 14, 2010
#
# When it comes to counters, there are several complicating factors.
# First, single-computer mode accesses counters in a very different way
# from Hadoop or Cloud modes.  
#
# Get all the counters and put them in the output directory.
#

package Counters;
use strict;
use warnings;
use Fcntl qw(:DEFAULT :flock); # for locking
use FindBin qw($Bin); 
use lib $Bin;
use File::Path qw(mkpath);
use Tools;
use Util;
use AWS;
use Util;
use Carp;

##
# Given a directory with stderr output from a single-computer-mode
# stage ($dir), an output filename ($outfn), and a function to send
# warning and error messages to ($msg), parse all the counter updates
# into a counter hash and then write the hash to the file at $outfn.
#
sub dumpLocalCounters($$$) {
	my ($dir, $outfn, $msg) = @_;
	-d $dir || die "No such input file or directory as \"$dir\"\n";
	my @fs = ();
	@fs = <$dir/*>;
	my %counters = ();
	for my $f (@fs) {
		if($f =~ /\.gz$/) {
			open INP, "gzip -dc $f |" || die "Could not open pipe 'gzip -dc $f |'";
		} elsif($f =~ /\.bz2$/) {
			open INP, "bzip2 -dc $f |" || die "Could not open pipe 'bzip2 -dc $f |'";
		} else {
			open INP, "$f" || die "Could not open $f for reading\n";
		}
		while(<INP>) {
			if(/^reporter:counter:/) {
				chomp;
				$_ = substr($_, length("reporter:counter:"));
				my @us = split(/,/);
				if(scalar(@us) != 3) {
					$msg->("Warning: Ill-formed counter updated line:\n$_");
				}
				$counters{$us[0]}{$us[1]} += $us[2];
			}
		}
		close(INP);
		$? == 0 || die "Bad exitlevel from input slurp: $?\n";
	}
	open(CNT, ">>$outfn") || die "Could not open file '$outfn' for appending\n";
	for my $k1 (sort keys %counters) {
		for my $k2 (sort keys %{$counters{$k1}}) {
			print CNT "pid=$$\t$k1\t$k2\t$counters{$k1}{$k2}\n";
		}
	}
	close(CNT);
}

##
# Use the 'hadoop' script to (a) determine what jobs have completed,
# and (b) populate a hash with all the counter values.
#
# Note: the caller has to know the job id of the .
#
sub getHadoopCounters($$$$) {
	my ($cnth, $selectjob, $msg, $verbose) = @_;
	$msg->("In getHadoopCounters:");
	my $counters = 0; # overall
	my $hadoop = Tools::hadoop();
	my $jstr = `$hadoop job -list all | awk '\$1 ~ /^job/ && \$2 == 2 {print \$1}'`;
	my @jobs = split(/[\n\r]+/, $jstr);
	my $jobfound = 0;
	$selectjob = sub {return 1} unless defined($selectjob);
	for my $job (@jobs) {
		if(!$selectjob->($job)) {
			$msg->("  Skipping job $job") if $verbose;
		} else {
			$msg->("  Examining job $job") if $verbose;
		}
		$jobfound++;
		my $sstr = `$hadoop job -status $job`;
		my @status = split(/[\n\r]+/, $sstr);
		my $seccounters = 0; # per section
		my $section = "";
		for (@status) {
			next if /^\s*$/;  # skip blank lines
			next if /^Job:/;  # skip Job: lines
			next if /^file:/; # skip file: lines
			next if /^tracking URL:/;
			if(/^map[(][)] completion: (.*)$/) {
				$1 eq "1.0" || $msg->("Warning: Incomplete mappers:\n\"$_\"\n");
			}
			if(/^reduce[(][)] completion: (.*)$/) {
				$1 eq "1.0" || $msg->("Warning: Incomplete reducers:\n\"$_\"\n");
			}
			next if /^Counters:/;
			chomp;
			my $l = Util::trim($_);
			if(/[=]/) {
				# Key=Value line
				$section ne "" || $msg->("No label before line:\n\"$_\"\n");
				my @s = split(/[=]/, $l);
				$#s == 1 || die;
				$cnth->{$section}{$s[0]} = $s[1];
				$counters++;
				$seccounters++;
			} else {
				$msg->("      section had $seccounters counters") if $verbose && $section ne "";
				$section = $l;
				$seccounters = 0;
				$msg->("    Found section: $section") if $verbose;
			}
		}
		$msg->("      section had $seccounters counters") if $verbose && $section ne "";
	}
}

##
# Sift through a local directory of stderr output files, extract and
# compile all the counter updates into the '$counters' hashref.
#
sub getLocalCounters($$$$) {
	my ($fn, $counters, $msg, $verbose) = @_;
	open(CNTS, $fn) || die "Could not open counter file '$fn'";
	while(<CNTS>) {
		my @s = split(/\t/);
		scalar(@s) == 3 || die "Ill-formatted counter line; must have 3 fields:\n$_\n";
		$counters->{$s[0]}{$s[1]} = $s[2];
	}
	close(CNTS);
}

##
# Get counters from previous stages.
#
sub getCounters($$$$) {
	my ($cntfn, $counters, $msg, $verbose) = @_;
	if(!defined($cntfn) || $cntfn eq "") {
		# Try to get counters from Hadoop
		Counters::getHadoopCounters($counters, undef, $msg, $verbose);
	} else {
		# Try to get counters from specified file
		Counters::getLocalCounters($cntfn, $counters, $msg, $verbose);
	}
}

1;
