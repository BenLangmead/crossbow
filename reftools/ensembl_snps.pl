#!/usr/bin/perl -w

use strict;
use warnings;
use Getopt::Long;
use File::Path qw(mkpath);

my $user = "anonymous";
my $host = "ensembldb.ensembl.org";
my $port = 5306;
my $database = "homo_sapiens_variation_59_37d";
my $crossbowOut = ""; # output dir for Crossbow-style output
my $crossbowCmap = ""; # Crossbow-style chromosome name map
my $limit = 0;

my $noChr = 0;
my $noOffset = 0;
my $noName = 0;
my $noAlleles = 0;
my $noValidation = 0;
my $noSummValid = 0;

my $listDbs = "<off>";

my $verbose = 0;
my $dryRun = 0;

my $printUsage = 0;

my $usage = qq!
Usage: perl ensembl_snps.pl [options]*

Options (defaults in [ ]):
  --user=<str>       Let mysql user = <str> [anonymous]
  --host=<str>       mysql host to connect to [ensembldb.ensembl.org]
  --port=<int>       mysql port [5306]
  --database=<str>   mysql databse [homo_sapiens_variation_59_37d]
                     (This changes\!  See notes below.)
  --limit=<int>      Limit number of results to max of <int> [no limit]

  --list-dbs=<str>   Just list databases with <str> in the name, exit.
                     If <str> is empty, lists all databases [off]

  --no-chr           Suppress chromosome name in output [off]
  --no-off           Suppress chromosome offset in output [off]
  --no-name          Suppress variant name in output [off]
  --no-alleles       Suppress allele string in output [off]
  --no-validation    Suppress validation status string [off]
  
  --no-summ-valid    Script summarizes validation status with "1" (at
                     least 1 validation type) or "0" (none) by default.
                     Specify this to see the raw validation string.

  --cb-out=<path>    Output Crossbow-style 'snps' dir to <path> [off]
                     (--no-* options are ignored)
  --cb-cmap=<path>   Use chromosome name map at <path>

  --verbose          Print queries and commands [off]
  --dry-run          Exit without making query; enables --verbose [off]

TODO:
  * Retrieve and print allele info

See http://uswest.ensembl.org/info/data/mysql.html for info about hosts
and ports.

Use --list-dbs to determine available databases.  E.g. to see all the
human variation databases, try:

  perl ensembl_snps.pl --list-dbs homo_sapiens_variation

!;

GetOptions (
	"user:s"         => \$user,
	"host:s"         => \$host,
	"port:i"         => \$port,
	"database:s"     => \$database,
	"limit:i"        => \$limit,
	"no-chr"         => \$noChr,
	"no-off"         => \$noOffset,
	"no-name"        => \$noName,
	"no-alleles"     => \$noAlleles,
	"no-validation"  => \$noValidation,
	"no-summ-valid"  => \$noSummValid,
	"list-dbs:s"     => \$listDbs,
	"cb-out|crossbow-out:s" => \$crossbowOut,
	"cb-cmap|crossbow-cmap:s" => \$crossbowCmap,
	"verbose"        => \$verbose,
	"dryrun|dry-run" => \$dryRun,
	"help|h|usage|?" => \$printUsage) || die "Bad option";

my %cmap = ();
if($crossbowCmap ne "") {
	open(CMAP, $crossbowCmap) || die;
	while(<CMAP>) {
		chomp;
		my @s = split(/\t/);
		defined($s[1]) || die "Bad cmap line:\n$_\n";
		$cmap{$s[0]} = $s[1];
	}
	close(CMAP);
}

$verbose = 1 if $dryRun;
if($printUsage) { print $usage; exit 0; }

mkpath($crossbowOut) if $crossbowOut ne "";

if($listDbs ne "<off>") {
	my $cmd = "mysql --user=$user --host=$host --port=$port -e \"show databases;\"";
	open CMD, "$cmd |";
	while(<CMD>) {
		if($listDbs ne "") { next unless /$listDbs/i; }
		print $_;
	}
	close(CMD);
	exit 0;
}

my $limitStr = $limit > 0 ? "LIMIT $limit" : "";

my $outputList = "";
if(!$noName || $crossbowOut ne "") {
	$outputList .= "," if $outputList ne "";
	$outputList .= "vf.variation_name";
}
if(!$noChr || $crossbowOut ne "") {
	$outputList .= "," if $outputList ne "";
	$outputList .= "sq.name";
}
if(!$noOffset || $crossbowOut ne "") {
	$outputList .= "," if $outputList ne "";
	$outputList .= "vf.seq_region_start";
}
if(!$noAlleles || $crossbowOut ne "") {
	$outputList .= "," if $outputList ne "";
	$outputList .= "vf.allele_string";
}
if(!$noValidation || $crossbowOut ne "") {
	$outputList .= "," if $outputList ne "";
	$outputList .= "v.validation_status";
}
# TODO: get "validated" info
if($outputList eq "") {
	print STDERR "No fields selected, quitting\n";
	exit 0;
}

my $query =
	"SELECT CONCAT_WS(' ', $outputList) ".
	"FROM variation_feature vf, seq_region sq, variation v ".
	"WHERE vf.seq_region_id = sq.seq_region_id ".
	  "AND vf.seq_region_end = vf.seq_region_start ".
	  "AND vf.variation_id = v.variation_id ".
	  "$limitStr;";

print STDERR "Query:\n$query\n" if $verbose;

my $cmd =
	"mysql --batch --user=$user --host=$host --port=$port ".
	"-e \"use $database; $query\"";

print STDERR "Command:\n$cmd\n" if $verbose;

exit 0 if $dryRun;

open CMD, "$cmd |";
my $results = 0;
my %fhs = ();
while(<CMD>) {
	chomp;
	next if /^CONCAT/;
	$results++;
	# Remove mysql output crud and output as tab-delimited lines
	s/\s+/\t/g;
	if($crossbowOut ne "") {
		# Parse record
		my ($name, $chr, $offset, $alleles, $valstr) = split(/\t/);
		defined($name)   || die;
		defined($offset) || die;
		defined($chr)    || die;
		my $chrCmap = $chr;
		$chrCmap =~ s/\s.*//;
		$chrCmap =~ s/[^a-zA-Z01-9]/_/g;
		$chr = $cmap{$chrCmap} if defined($cmap{$chrCmap});
		my $fn = "$crossbowOut/$chr.snps";
		unless(defined($fhs{$fn})) {
			open($fhs{$fn}, ">$fn") || die "Could not open $fn for writing\n";
		}
		my @alss = split(/\//, $alleles);
		my %als = ();
		for my $a (@alss) {
			$als{$a} = 1 if ($a eq "A" || $a eq "C" || $a eq "G" || $a eq "T");
		}
		next if scalar(keys %als) < 2; # not a SNP
		for my $k (keys %als) { $als{$k} = ((1.0 * $als{$k}) / scalar(keys %als)); }
		my $val = ((defined($valstr) && $valstr ne "") ? "0" : "1");
		# Crossbow file fields are:
		# 1. Chromosome ID
		# 2. 1-based offset into chromosome
		# 3. Whether SNP has allele frequency information (1 = yes, 0 = no)
		# 4. Whether SNP is validated by experiment (1 = yes, 0 = no)
		# 5. Whether SNP is actually an indel (1 = yes, 0 = no)
		# 6. Frequency of A allele, as a decimal number
		# 7. Frequency of C allele, as a decimal number
		# 8. Frequency of T allele, as a decimal number
		# 9. Frequency of G allele, as a decimal number
		# 10. SNP id (e.g. a dbSNP id such as rs9976767)
		printf {$fhs{$fn}} "$chr\t$offset\t0\t$val\t0\t%0.4f\t%0.4f\t%0.4f\t%0.4f\t$name\n",
		       ($als{A} || 0), ($als{C} || 0), ($als{T} || 0), ($als{G} || 0);
	} else {
		my @s = split(/\t/);
		if(!($noValidation || $noSummValid)) {
			if($s[-1] =~ /\//) {
				push @s, "0";
			} else {
				$s[-1] = "1";
			}
		}
		print join("\t", @s)."\n";
	}
}
print STDERR "$results results\n";
close(CMD);
for my $k (keys %fhs) { close($fhs{$k}); }
