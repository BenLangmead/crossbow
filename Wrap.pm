#!/usr/bin/perl -w

##
# Author: Ben Langmead
#   Date: 3/28/2010
#
# Helpful utilities for Hadoop-like wrappers.
#

package Wrap;

##
# Pretty-print a hash filled with counter information.
#
sub printCounters($$) {
	my ($counters, $msg) = @_;
	for my $k1 (sort keys %$counters) {
		for my $k2 (sort keys %{$counters->{$k1}}) {
			$msg->("$k1\t$k2\t$counters->{$k1}{$k2}");
		}
	}
}

##
# Parse all counter updates in a directory of Hadoop-like output.
#
sub getLocalCounters($$$) {
	my ($dir, $counters, $msg) = @_;
	-d $dir || die "No such input file or directory as \"$dir\"\n";
	my @fs = ();
	@fs = <$dir/*>;
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
				$counters->{$us[0]}{$us[1]} += $us[2];
			}
		}
		close(INP);
		$? == 0 || die "Bad exitlevel from input slurp: $?\n";
	}
}

##
# Parse all counter updates in a directory of Hadoop-like output then
# pretty-print it.
#
sub getAndPrintLocalCounters($$) {
	my ($dir, $msg) = @_;
	my %counters = ();
	getLocalCounters($dir, \%counters, $msg);
	printCounters(\%counters, $msg);
}

1;
