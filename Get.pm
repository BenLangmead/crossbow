#!/usr/bin/perl -w

##
# Author: Ben Langmead
#   Date: 2/14/2010
#
# Routines for getting and expanding jars from
#

package Get;
use strict;
use warnings;
use Fcntl qw(:DEFAULT :flock); # for locking
use FindBin qw($Bin); 
use lib $Bin;
use File::Path qw(mkpath);
use File::Basename;
use Tools;
use AWS;
use Util;
use Carp;

##
# Parse a URL, extracting the protocol and type of program that will
# be needed to download it.
#
sub parse_url($) {
	my ($ref) = @_;
	my $type;
	my @s = split(/[:]/, $ref);
	my $proto = $s[0];
	$proto = lc $proto;
	if($proto =~ /s3n?/) {
		$type = "s3";
		$ref =~ s/^s3n/s3/; # because s3cmd doesn't like s3n://
	} elsif($proto eq "ftp" || $proto eq "http") {
		$type = "wget";
	} elsif($proto eq "hdfs") {
		$type = "hdfs";
	} else {
		$type = "local";
		(-f $ref || -d $ref) || croak("URL referring to local file $ref doesn't exist or cannot be read\n");
		return ("", $type);
	}
	return ($proto, $type);
}

##
# Prepare an s3 URL for use with s3cmd.
#
sub s3cmdify($) {
	my $path = shift;
	$path =~ s/^S3N:/s3n:/;
	$path =~ s/^S3:/s3:/;
	$path =~ s/^s3n:/s3:/;
	# Note: this is a good way to strip out the access ID and secret
	# key ID.  It's better than using a regular expression because it's
	# hard to think of an expression that correctly handles slashes in
	# the secret key ID (which is possible).
	AWS::ensureKeys($Tools::hadoop, $Tools::hadoop_arg);
	my $ec2key = $AWS::accessKey.":".$AWS::secretKey;
	my $idx = index($path, $ec2key);
	if($idx != -1) {
		# Remove ID:secret and the @ on the end
		substr($path, $idx, length($ec2key)+1) = "";
	}
	return $path;
}

sub do_s3cmd($) {
	my ($args) = @_;
	my $s3cmd = Tools::s3cmd();
	my $cmd = "$s3cmd $args";
	print STDERR "Get.pm:do_s3cmd: $cmd\n";
	my $out = Util::backtickAndWait($cmd, "s3cmd");
	$? && croak("Exitlevel from \"$cmd\" was $?\n");
	return ($?, $out);
}

sub do_s3_get($$$$$) {
	my ($file, $base, $dest_dir, $counters, $retries) = @_;
	$file = s3cmdify($file);
	my $file_arg = $file;
	mkpath($dest_dir);
	my $cmd = "rm -f $dest_dir/$base >&2";
	print STDERR "Get.pm:do_s3_get: $cmd\n";
	system($cmd);
	my $ret;
	while($retries >= 0) {
		my $out;
		($ret, $out) = do_s3cmd("get --force $file_arg $dest_dir/$base >&2");
		(-f "$dest_dir/$base") || croak("Did not create $dest_dir/$base - wrong URL?\n");
		push @{$counters}, "Fetcher,s3cmd return $ret,1";
		push @{$counters}, "Fetcher,Bytes obtained with s3cmd get,".(-s "$dest_dir/$base");
		push @{$counters}, "Fetcher,Files obtained with s3cmd get,1";
		return $ret if $ret == 0;
		system("rm -f $dest_dir/$base* $dest_dir/.$base*");
		$retries--;
	}
	return $ret;
}

sub do_s3_put($$$) {
	my ($file, $dest, $counters) = @_;
	$dest = s3cmdify($dest);
	$dest .= "/" unless $dest =~ /\/$/;
	my $base = fileparse($file);
	my ($ret, $out) = do_s3cmd("put $file $dest$base >&2");
	push @{$counters}, "Fetcher,Bytes uploaded with s3cmd put,".(-s "$file");
	push @{$counters}, "Fetcher,Files uploaded with s3cmd put,1";
}

sub do_hdfs_get($$$$) {
	my ($file, $base, $dest_dir, $counters) = @_;
	defined($base) || croak("Must define base\n");
	defined($dest_dir) || croak("Must define dest_dir\n");
	$file =~ s/^HDFS:/hdfs:/;
	my $hadoop = Tools::hadoop();
	mkpath($dest_dir);
	my $cmd = "$hadoop dfs -get $file $dest_dir/$base >&2";
	print STDERR "Get.pm:do_hdfs_get: $cmd\n";
	my $ret = Util::runAndWait($cmd, "hadoop dfs -get");
	print STDERR "Get.pm:returned $ret\n";
	push @{$counters}, "Fetcher,hadoop dfs -get return $ret,1";
	push @{$counters}, "Fetcher,Bytes obtained with hadoop dfs -get,".(-s "$dest_dir/$base");
	push @{$counters}, "Fetcher,Files obtained with hadoop dfs -get,1";
	return $ret;
}

##
# Put a local file into HDFS.
#
sub do_hdfs_put($$$) {
	my ($file, $dest, $counters) = @_;
	$dest =~ s/^HDFS:/hdfs:/;
	$dest .= "/" unless $dest =~ /\/$/;
	my $base = fileparse($file);
	my $hadoop = Tools::hadoop();
	# Ensure HDFS directory exists
	my $cmd = "$hadoop dfs -mkdir $dest >&2";
	Util::runAndWait($cmd, "$hadoop dfs -mkdir");
	# Put the file
	$cmd = "$hadoop dfs -put $file $dest$base >&2";
	print STDERR "Get.pm:do_hdfs_put: $cmd\n";
	my $ret = Util::runAndWait($cmd, "$hadoop dfs -put");
	# Update counters
	push @{$counters}, "Fetcher,hadoop dfs -put return $ret,1";
	push @{$counters}, "Fetcher,Bytes uploaded with hadoop dfs -put,".(-s $file);
	push @{$counters}, "Fetcher,Files uploaded with hadoop dfs -put,1";
	return $ret;
}

sub do_local($$$$) {
	my ($file, $base, $dest_dir, $counters) = @_;
	mkpath($dest_dir);
	my $cmd = "cp $file $dest_dir/$base >&2";
	print STDERR "Get.pm:do_local: $cmd\n";
	my $ret = Util::run($cmd);
	push @{$counters}, "Fetcher,cp return $ret,1";
	push @{$counters}, "Fetcher,Bytes obtained with cp,".(-s "$dest_dir/$base");
	push @{$counters}, "Fetcher,Files obtained with cp,1";
	return $ret;
}

##
# Workaround for the situation where the change of FTP dir is
# forbidden, but fetching the file itself is permitted (this seems to
# happen e.g. on the NCBI 1000genomes server sometimes).
#
sub fix_wget_url($) {
	my $url = shift;
	my @us = split(/\//, $url);
	my $ret = "";
	return $url if $#us <= 3;
	$ret .= join("/", ($us[0], $us[1], $us[2]))."/";
	shift @us; shift @us; shift @us;
	$ret .= join("%2f", @us);
	return $ret;
}

##
# Get a file over http or ftp using wget.
#
sub do_wget($$$$) {
	my ($file, $base, $dest_dir, $counters) = @_;
	my $url = fix_wget_url($file);
	my $wget = Tools::wget();
	mkpath($dest_dir);
	my $cmd = "$wget $url -O $dest_dir/$base >&2";
	print STDERR "Get.pm:do_wget: $cmd\n";
	my $ret = Util::run($cmd);
	push @{$counters}, "Fetcher,wget return $ret,1";
	push @{$counters}, "Fetcher,Bytes obtained with wget,".(-s "$dest_dir/$base");
	push @{$counters}, "Fetcher,Files obtained with wget,1";
	return $ret;
}

sub lsDir($) {
	my ($dir) = @_;
	print STDERR "Get.pm:lsDir: About to parse URL $dir\n";
	my ($proto, $type) = parse_url($dir);
	my @fs = ();
	if($type eq "s3") {
		print STDERR "Get.pm:lsDir: About to handle S3\n";
		$dir = s3cmdify($dir);
		$dir .= "/" if $dir !~ /\/$/;
		my ($ret, $out) = do_s3cmd("ls $dir");
		my @fls = split(/[\r\n]+/, $out);
		for (@fls) {
			next if /^Bucket/;
			my @fs2 = split(/[\s]+/, $_);
			push @fs, $fs2[-1];
		}
	} elsif($type eq "local") {
		print STDERR "Get.pm:lsDir: About to handle local\n";
		my $out = Util::backtickRun("ls -1 $dir");
		my @fls = split(/[\r\n]+/, $out);
		$dir =~ s/\/$//;
		for my $f (@fls) { push @fs, "$dir/$f"; }
	} else {
		my $fsstr = "dfs";
		print STDERR "Get.pm:lsDir: About to handle HDFS\n";
		my $hadoop = Tools::hadoop();
		my $out = `$hadoop $fsstr -ls $dir`;
		my @fls = split(/[\r\n]+/, $out);
		for (@fls) {
			next if /^Found/;
			my @fs2 = split(/[\s]+/, $_);
			my $f = $fs2[-1];
			$f = "hdfs://".$f if ($f =~ /^\// && $type eq "hdfs");
			push @fs, $f;
		}
	}
	return @fs;
}

##
# Ensure all of the files in the source directory have been copied into
# dest_dir.
#
sub ensureDirFetched($$$) {
	my ($dir, $dest_dir, $counters) = @_;
	$dir =~ s/^S3N/s3n/;
	$dir =~ s/^S3/s3/;
	$dir =~ s/^HDFS/hdfs/;
	my $dirDoneFile = $dir;
	$dirDoneFile =~ s/[\/:]/_/g;
	mkpath($dest_dir);
	$dirDoneFile = "$dest_dir/.dir.$dirDoneFile";
	unless(-f $dirDoneFile) {
		$dir .= "/" unless $dir =~ /\/$/;
		my @files = lsDir($dir);
		for(@files) {
			print STDERR "Get.pm:ensureDirFetched: About to be fetched: $_\n";
		}
		for(@files) {
			print STDERR "ensureDirFetched: Fetching directory file $_\n";
			ensureFetched($_, $dest_dir, $counters);
		}
		Util::run("touch $dirDoneFile");
	}
}

##
# Do not return until the given file has been obtained and the "done"
# flag file has been installed.
#
# If the thing being decompressed is an R installation, we do a little
# ad-hoc fixup to ensure it likes the new directory it's in.
#
sub ensureFetched {
	my (
		$file,          # Path/URL of file to get
		$dest_dir,      # Directory to copy it to and/or extract it in
		$counters,      # Ref to array to store counter updates in
		$doRfixup,      # If it's R that's being extracted and this is
		                # true, we set RHOME and modify Rscript
		                # accordingly
		$lockSub) = @_; # A parameterless subroutine to call if and
		                # when we get the lock
	
	print STDERR "Get.pm:ensureFetched: called on \"$file\"\n";
	$file =~ s/^S3N/s3n/;
	$file =~ s/^S3/s3/;
	$file =~ s/^HDFS/hdfs/;
	my $base = fileparse($file);
	print STDERR "Get.pm:ensureFetched: base name \"$base\"\n";
	mkpath($dest_dir);
	my $done_file = "$dest_dir/.$base.done";
	my $lock_file = "$dest_dir/.$base.lock";
	print STDERR "ls -al $dest_dir/*$base* $dest_dir/.*$base*\n";
	print STDERR `ls -al $dest_dir/*$base* $dest_dir/.*$base*\n`;
	my ($proto, $type) = parse_url($file);
	print STDERR "Pid $$: Checking for done file $done_file\n";
	if(! -f $done_file) {
		print STDERR "Pid $$: Done file $done_file was NOT present\n";
		#
		# Use perl portable file locking to prevent race conditions when
		# there are multiple mappers per machine.
		#
		system("touch $lock_file");
		print STDERR "Pid $$: Attempting to obtain lock...\n";
		open(FH, "<$lock_file") or croak("Can't open lock file \"$lock_file\": $!");
		if(flock(FH, LOCK_EX | LOCK_NB)) {
			# Got the lock; it's up to me to download and explode the jar file
			print STDERR "Pid $$: got the lock; downloading file...\n";
			print STDERR "Pid $$:   file name: $base\n";
			my $cmd = "rm -f $dest_dir/$base >&2";
			print STDERR "$cmd\n";
			system($cmd);
			my $ret;
			print STDERR "Pid $$:   downloading file...\n";
			if($type eq "s3") {
				$ret = do_s3_get($file, $base, $dest_dir, $counters, 3);
			} elsif($type eq "hdfs") {
				$ret = do_hdfs_get($file, $base, $dest_dir, $counters);
			} elsif($type =~ /https?/ || $proto eq "ftp") {
				$ret = do_wget($file, $base, $dest_dir, $counters);
			} else {
				$type eq "local" || croak("Bad type: $type\n");
				$ret = do_local($file, $base, $dest_dir, $counters);
			}
			print STDERR "ls -al $dest_dir/$base\n";
			print STDERR `ls -al $dest_dir/$base`;
			if($ret != 0) {
				system("rm -f $dest_dir/$base* $dest_dir/.$base*");
				flock(FH, LOCK_UN);
				close(FH);
				print STDERR "Return value from download task was $ret\n";
				croak("Return value from download task was $ret\n");
			}
			if(! -f "$dest_dir/$base") {
				flock(FH, LOCK_UN);
				close(FH);
				print STDERR "Return value from download task was $ret but the file $dest_dir/$base doesn't exist\n";
				croak("Return value from download task was $ret but the file $dest_dir/$base doesn't exist\n");
			}
			if($base =~ /\.jar$/) {
				print STDERR "Pid $$:   extract jar\n";
				my $jar_exe = Tools::jar();
				$cmd = "cd $dest_dir && $jar_exe xf $base >&2";
				print STDERR "$cmd\n";
				$ret = Util::runAndWait($cmd, "jar xf");
			} elsif($base =~ /\.tar\.gz$/ || $base =~ /\.tgz$/) {
				$cmd = "cd $dest_dir && tar zxf $base >&2";
				print STDERR "$cmd\n";
				$ret = Util::runAndWait($cmd, "tar zxf");
			} elsif($base =~ /\.tar.bz2$/) {
				$cmd = "cd $dest_dir && tar jxf $base >&2";
				print STDERR "$cmd\n";
				$ret = Util::runAndWait($cmd, "tar jxf");
			}
			print STDERR "ls -al $dest_dir/$base\n";
			print STDERR `ls -al $dest_dir/$base`;
			if($ret != 0) {
				system("rm -rf $dest_dir/$base* $dest_dir/.$base*");
				flock(FH, LOCK_UN);
				close(FH);
				croak("Return value from extract task was $ret\n");
			}
			my $size = -s "$dest_dir/$base";
			push @{$counters}, "Fetcher,File and size $base and $size,1";
			push @{$counters}, "Fetcher,Bytes obtained,$size";
			push @{$counters}, "Fetcher,Files obtained,1";
			if(defined($doRfixup)) {
				# This is a silly fixup we have to do if we want R and Rscript
				# to run in their new home.
				print STDERR "Setting RHOME = \"$dest_dir/$doRfixup\"\n";
				print STDERR "Writing new \"$dest_dir/$doRfixup/bin/R\" script\n";
				open(RSC, "$dest_dir/$doRfixup/bin/R") ||
					croak("Could not open '$dest_dir/$doRfixup/bin/R' for reading");
				open(RSCN, ">$dest_dir/$doRfixup/bin/R.new") ||
					croak("Could not open '$dest_dir/$doRfixup/bin/R.new' for writing");
				while(<RSC>) {
					if(/^R_HOME_DIR=/) {
						print STDERR "Modifying R_HOME_DIR\n";
						print RSCN "R_HOME_DIR=$dest_dir/$doRfixup\n";
					} else { print RSCN $_; }
				}
				close(RSC); close(RSCN);
				system("mv $dest_dir/$doRfixup/bin/R.new $dest_dir/$doRfixup/bin/R");
				system("chmod a+x $dest_dir/$doRfixup/bin/R");
				push @{$counters}, "Fetcher,R path fixups performed,1";
			}
			# Call user-supplied function
			if(defined($lockSub)) { $lockSub->(); }
			system("touch $done_file");
		} else {
			print STDERR "Pid $$: didn't get the lock; waiting for master to finish\n";
			my $sleeps = 0;
			while(! -f $done_file) {
				sleep(3);
				if((++$sleeps % 10) == 0) {
					my $secs = $sleeps * 3;
					print STDERR "Pid $$: still waiting (it's been $secs seconds)\n";
				}
			}
			print STDERR "Pid $$: master finished; continuing\n";
		}
		close(FH);
	} else {
		print STDERR "Pid $$: done file $done_file was there already; continuing\n";
	}
	(-f $done_file) || croak("Pid $$: about to exit ensureFetched, but done file $done_file doesn't exist\n");
}

##
# Check if a local, hdfs or s3 (or other Hadoop-supported fs) file or
# directory exists.
#
sub fs_exists {
	my $path = shift;
	my $rc;
	if(Util::is_local($path)) {
		$rc = Util::run("stat $path >& /dev/null");
	} else {
		my $hadoop = Tools::hadoop();
		$path =~ s/^hdfs:\/\///i;
		$rc = Util::run("($hadoop fs -stat $path) >& /dev/null");
	}
	return !$rc;
}

##
# Put a file into a a local, hdfs or s3 (or other Hadoop-supported fs)
# path.
#
# $src must be a path to a file
#
# $dst must be a path to a directory; it can't specify the destination
# filename - the basename from $src is preserved
#
sub fs_put {
	my ($src, $dst) = @_;
	my $base = fileparse($src);
	$dst .= "/" unless $dst =~ /\/$/;
	my $fulldst = "$dst$base";
	if(fs_exists($fulldst)) {
		print STDERR "WARNING: replacing old $dst from hdfs\n";
		if(Util::is_local($fulldst)) {
			Util::run("rm -rf $fulldst >&2");
		} else {
			my $hadoop = Tools::hadoop();
			if($fulldst =~ /^hdfs:/i) {
				my $fd = $fulldst;
				$fd =~ s/^hdfs:\/\///i;
				Util::run("$hadoop dfs -rmr $fulldst >&2");
			} else {
				Util::run("$hadoop fs -rmr $fulldst >&2");
			}
		}
	}
	my $rc;
	if(Util::is_local($src) && Util::is_local($dst)) {
		mkpath($dst);
		$rc = Util::run("cp $src $dst >&2");
	} else {
		my $hadoop = Tools::hadoop();
		if($dst =~ /^hdfs:/i) {
			my ($d, $fd) = ($dst, $fulldst);
			$d =~ s/^hdfs:\/\///i;
			$fd =~ s/^hdfs:\/\///i;
			Util::run("$hadoop dfs -mkdir $dst >&2");
			$rc = Util::run("$hadoop dfs -put $src $fd >&2");
		} else {
			Util::run("$hadoop fs -mkdir $dst >&2");
			$rc = Util::run("$hadoop fs -put $src $fulldst >&2");
		}
	}
	die "Can't load $src to $dst ($rc)\n" if $rc;
}

##
# Remove a file in a local, hdfs or s3 (or other Hadoop-supported fs)
# path.
#
sub fs_remove {
	my ($path) = @_;
	my $rc;
	if(Util::is_local($path)) {
		$rc = Util::run("rm -rf $path >&2");
	} else {
		my $hadoop = Tools::hadoop();
		$path =~ s/^hdfs:\/\///i;
		$rc = Util::run("$hadoop fs -rmr $path >&2");
	}
	return $rc;
}

1;
