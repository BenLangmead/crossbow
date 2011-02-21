#!/usr/bin/perl

#
# sanity_check.pl
#
# Authors: Ben Langmead & Michael C. Schatz
#    Date: 10/12/2009
#
# Run from the root directory of an expanded reference har to see how
# often the reference character matches one of the SNP alleles at all
# positions with SNPs.  If only about half of them match, chances are
# good that the reference FASTA files are mismatched or misaligned with
# the dbSNP snps.
# 

use warnings;
use strict;

for my $f (split(/\s+/, `ls sequences/*.fa`)) {
	my $bad = 0;
	my %badc = ('A' => 0, 'C' => 0, 'G' => 0, 'T' => 0);
	my $good = 0;
	open FA, $f || die;
	my $s = $f;
	$s =~ s/\.fa$/.snps/;
	$s =~ s/^sequences/snps/;
	print STDERR "Processing $f/$s\n";
	my $seq = "";
	while(<FA>) {
		chomp;
		next if /^>/;
		$seq .= $_;
	}
	close(FA);
	open SNPS, $s || die;
	while(<SNPS>) {
		chomp;
		my @s = split;
		my ($a, $c, $t, $g) = ($s[5], $s[6], $s[7], $s[8]);
		my $refc = uc substr($seq, $s[1]-1, 1);
		if($refc eq 'A' && $a == 0.0) {
			$badc{A}++; $bad++;
		} elsif($refc eq 'C' && $c == 0.0) {
			$badc{C}++; $bad++;
		} elsif($refc eq 'G' && $g == 0.0) {
			$badc{G}++; $bad++;
		} elsif($refc eq 'T' && $t == 0.0) {
			$badc{T}++; $bad++;
		} else {
			$good++;
		}
	}
	close(SNPS);
	print "Matched: $good, Mismatched: $bad\n";
	print "   Bad As: $badc{A}\n";
	print "   Bad Cs: $badc{C}\n";
	print "   Bad Gs: $badc{G}\n";
	print "   Bad Ts: $badc{T}\n";
}
