#!/usr/bin/perl -w

##
# fasta_cmap.pl
#
# Scan a list of FASTA files in order.  For each name line encountered
# (again, in order), replace the name with a 0-based integer and add an
# entry to a file (the cmap file) that maps the integer to the true
# name.  All of the non-alphanumeric characters in the name are first
# coverted to underscores before being stored in the cmap file.  To
# reduce peak disk usage, each FASTA file is deleted after it is
# scanned.
#
# E.g.:
#
# perl fasta_cmap.pl --cmap=my.cmap -- chr1.fa chr2.fa chrMT.fa
#

use strict;
use warnings;
use Getopt::Long;

my $out = "cmap.txt";
my $outLong = "cmap_long.txt";
my $suffix = ".cmap.fa";
my $delete = 1;

GetOptions(
	"suffix=s" => \$suffix,
	"cmap=s" => \$out,
	"cmap-long=s" => \$outLong,
	"no-delete" => sub {$delete = 0}) || die "Bad options";

$out ne "" || die;
open (CMAP, ">$out") || die "Could not open '$out' for writing";
open (CMAPL, ">$outLong") || die "Could not open '$outLong' for writing";
my $idx = 0;
my $cmfafh = undef;
for my $f (@ARGV) {
	print STDERR "Processing fasta file $f...\n";
	open (FA, (($f =~ /\.gz$/) ? "gzip -dc $f |" : $f)) || die "Could not open '$f' for reading";
	while(<FA>) {
		if(/^>/) {
			my $oname = substr($_, 1);
			chomp($oname);
			my $name = $oname;
			my $nameShort = $name;
			$nameShort =~ s/\s.*//; # truncate short name at first whitespace
			$name =~ s/[^a-zA-Z01-9]/_/g;
			$nameShort =~ s/[^a-zA-Z01-9]/_/g;
			print STDERR "  Processing sequence '$oname' (converted to: '$name', short: '$nameShort')...\n";
			close($cmfafh) if defined($cmfafh);
			$cmfafh = undef;
			open($cmfafh, ">$idx$suffix") || die "Could not open $idx$suffix for writing";
			defined($cmfafh) || die "Filhandle not defined after opening $idx$suffix for writing";
			print {$cmfafh} ">$idx\n";
			print CMAP "$nameShort\t$idx\n";
			print CMAPL "$name\t$idx\n";
			$idx++;
		} else {
			defined($cmfafh) || die;
			print {$cmfafh} $_;
		}
	}
	close (FA);
	unlink($f) if $delete;
}
close($cmfafh) if defined($cmfafh);
close(CMAP);
close(CMAPL);
