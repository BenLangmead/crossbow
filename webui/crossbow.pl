#!/usr/bin/perl -w

##
# Crossbow web interface.  Requires S3Util.pm and CrossbowIface.pm in
# the same directory.
#

use strict;
use warnings;
use CGI;
use CGI::Ajax;
use Net::Amazon::S3;
use FindBin qw($Bin);
use lib $Bin;
use CrossbowIface;
use S3Util;
use CGI::Carp qw(fatalsToBrowser);

my $VERSION = "1.2.0";
my $debugLev = 0;
my $cgi  = CGI->new();
my $ajax = CGI::Ajax->new(submitClicked  => \&submitClicked,
                          checkS3URL     => \&checkS3URL,
                          checkS3Creds   => \&checkS3Creds,
                          checkRefURL    => \&checkRefURL,
                          checkInputURL  => \&checkInputURL,
                          checkOutputURL => \&checkOutputURL);
$ajax->js_encode_function('encodeURIComponent');
$ajax->JSDEBUG($debugLev);
print $ajax->build_html( $cgi, \&main );

##
# Verify that given input URL exists.
#
sub checkInputURL {
	my ($awsId, $awsSecret, $url) = @_;
	my ($ret, $err);
	($ret, $err) = eval { S3Util::s3exists($awsId, $awsSecret, $url); };
	my $recheck = "(<a href=\"javascript:jsCheckInputURL()\">Re-check input URL...</a>)";
	unless(defined($ret)) {
		if($debugLev > 0) {
			return "<font color='red'>Error: s3exists died with message \"$@\": \"$url\"</font> $recheck";
		} else {
			return "<font color='red'>Error: s3exists died with message \"$@\"</font> $recheck";
		}
	}
	if($ret < -1 || $ret > 1) {
		if($debugLev > 0) {
			return "<font color='red'>Error: Return value from s3exists was $ret: \"$url\"</font> $recheck";
		} else {
			return "<font color='red'>Error: Return value from s3exists was $ret</font> $recheck";
		}
	}
	if($ret == 1) {
		if($debugLev > 0) {
			return "<font color='green'>Verified: \"$url\"</font>";
		} else {
			return "<font color='green'>Verified</font>";
		}
	} elsif($ret == -1) {
		if($debugLev > 0) {
			return "<font color='red'>Error: $err: \"$url\"</font> $recheck";
		} else {
			return "<font color='red'>Error: $err</font> $recheck";
		}
	} else {
		$ret == 0 || croak();
		if($debugLev > 0) {
			return "<font color='red'>Error: Input URL does not exist: \"$url\"</font> $recheck"
		} else {
			return "<font color='red'>Error: Input URL does not exist</font> $recheck"
		}
	}
}

##
# Verify that given reference-jar URL exists.
#
sub checkRefURL {
	my ($awsId, $awsSecret, $url) = @_;
	my ($ret, $err);
	($ret, $err) = eval { S3Util::s3exists($awsId, $awsSecret, $url); };
	my $recheck = "(<a href=\"javascript:jsCheckRefURL()\">Re-check reference URL...</a>)";
	unless(defined($ret)) {
		if($debugLev > 0) {
			return "<font color='red'>Error: s3exists died with message \"$@\": \"$url\"</font> $recheck";
		} else {
			return "<font color='red'>Error: s3exists died with message \"$@\"</font> $recheck";
		}
	}
	if($ret < -1 || $ret > 1) {
		if($debugLev > 0) {
			return "<font color='red'>Error: Return value from s3exists was $ret: \"$url\"</font> $recheck";
		} else {
			return "<font color='red'>Error: Return value from s3exists was $ret</font> $recheck";
		}
	}
	if($ret == 1) {
		if($debugLev > 0) {
			return "<font color='green'>Verified: \"$url\"</font>";
		} else {
			return "<font color='green'>Verified</font>";
		}
	} elsif($ret == -1) {
		if($debugLev > 0) {
			return "<font color='red'>Error: $err: \"$url\"</font> $recheck";
		} else {
			return "<font color='red'>Error: $err</font> $recheck";
		}
	} else {
		$ret == 0 || croak();
		if($debugLev > 0) {
			return "<font color='red'>Error: Reference jar URL does not exist: \"$url\"</font> $recheck"
		} else {
			return "<font color='red'>Error: Reference jar URL does not exist</font> $recheck"
		}
	}
}

##
# Verify that given output URL does not exist.
#
sub checkOutputURL {
	my ($awsId, $awsSecret, $url) = @_;
	my ($ret, $err);
	($ret, $err) = eval { S3Util::s3exists($awsId, $awsSecret, $url); };
	my $recheck = "(<a href=\"javascript:jsCheckOutputURL()\">Re-check output URL...</a>)";
	unless(defined($ret)) {
		if($debugLev > 0) {
			return "<font color='red'>Error: s3exists died with message \"$@\": \"$url\"</font> $recheck";
		} else {
			return "<font color='red'>Error: s3exists died with message \"$@\"</font> $recheck";
		}
	}
	if($ret < -1 || $ret > 1) {
		if($debugLev > 0) {
			return "<font color='red'>Error: Return value from s3exists was $ret: \"$url\"</font> $recheck";
		} else {
			return "<font color='red'>Error: Return value from s3exists was $ret</font> $recheck";
		}
	}
	if($ret == 0) {
		if($debugLev > 0) {
			return "<font color='green'>Verified: \"$url\"</font>";
		} else {
			return "<font color='green'>Verified</font>";
		}
	} elsif($ret == -1) {
		if($debugLev > 0) {
			return "<font color='red'>Error: $err: \"$url\"</font> $recheck";
		} else {
			return "<font color='red'>Error: $err</font> $recheck";
		}
	} else {
		$ret == 1 || croak();
		if($debugLev > 0) {
			return "<font color='red'>Error: Output URL already exists: \"$url\"</font> $recheck"
		} else {
			return "<font color='red'>Error: Output URL already exists</font> $recheck"
		}
	}
}

##
# Check if the given S3 credentials work.
#
sub checkS3Creds {
	my ($awsId, $awsSecret) = @_;
	my $ret = eval { S3Util::checkCreds($awsId, $awsSecret); };
	my $recheck = "(<a href=\"javascript:jsCheckS3Creds()\">Re-check credentials...</a>)";
	unless(defined($ret)) {
		if($debugLev > 0) {
			return "<font color='red'>Error: checkCreds died with message \"$@\": \"$awsId\", \"$awsSecret\"</font> $recheck";
		} else {
			return "<font color='red'>Error: checkCreds died with message \"$@\"</font> $recheck";
		}
	}
	if($ret == 1) {
		if($debugLev > 0) {
			return "<font color='green'>Verified: \"$awsId\", \"$awsSecret\"</font>";
		} else {
			return "<font color='green'>Verified</font>";
		}
	} else {
		if($debugLev > 0) {
			return "<font color='red'>Error: Bad AWS ID and/or Secret Key: \"$awsId\", \"$awsSecret\"</font> ";
		} else {
			return "<font color='red'>Error: Bad AWS ID and/or Secret Key</font> $recheck";
		}
	}
}

#
# Form elements:
#
#  AWSId: text
#  AWSSecret: password
#  AWSKeyPair: text
#  JobName: text
#  JobType: radio (just-preprocess | crossbow)
#  InputURL: text
#  OutputURL: text
#  InputType: radio (manifest | preprocessed)
#  TruncateLength: text (blank or 0 = don't truncate)
#  TruncateDiscard: check
#  DiscardFraction: text (blank or 0 = don't discard)
#  QualityEncoding: dropdown (Phred+33 | Phred+64 | Solexa+64)
#  Genome: dropdown (bunch of genomes)
#  SpecifyRef: check
#  Ref: text
#  BowtieOpts: text
#  SoapsnpOpts: text
#  SoapsnpOptsHap: text
#  SoapsnpOptsDip: text
#  Haploids: text
#  HaploidsList: text
#  ClusterWait: check
#  NumNodes: text
#  InstanceType: dropdown (c1.xlarge)
#

sub submitClicked {
	my ($awsId,
	    $awsSecret,
	    $keyPairName,
	    $name,
	    $jobType,
	    $inputURL,
	    $outputURL,
	    $inputType,
	    $truncLen,
	    $truncDiscard,
	    $discardFrac,
	    $qual,
	    $genome,
	    $specifyRef,
	    $ref,
	    $bowtieOpts,
	    $soapsnpOpts,
	    $soapsnpOptsHap,
	    $soapsnpOptsDip,
	    $haploids,
	    $haploidsList,
	    $clusterWait,
	    $numNodes,
	    $instanceType) = @_;

	##
	# Map from short names to URLs for the pre-built reference jars.
	#
	my %refMap = (
		"hg18_130" => "s3n://crossbow-refs/hg18.jar",
		"mm9_130"  => "s3n://crossbow-refs/mm9.jar",
		"e_coli"   => "s3n://crossbow-refs/e_coli.jar"
	);
	
	$name = "Crossbow" unless defined($name) && $name ne "";
	$jobType eq "--just-preprocess" || $jobType eq "--crossbow" || croak("Bad JobType: $jobType");
	$numNodes == int($numNodes) || croak("NumNodes is not an integer: $numNodes");
	
	my @as = ();
	push @as, "--accessid=$awsId";
	push @as, "--secretid=$awsSecret";
	push @as, "--key-pair=$keyPairName" if defined($keyPairName) && $keyPairName ne "";
	push @as, "--emr-script=\"/var/www/cgi-bin/elastic-mapreduce\"";
	push @as, "--name=\"$name\"";
	push @as, "$jobType";
	push @as, "--input=$inputURL";
	push @as, "--output=$outputURL";
	if($jobType eq "just-preprocess") {
		# Preprocess job
	} else {
		# Crossbow job
		$truncDiscard = "--truncate-length" unless $truncDiscard ne "";
		push @as, "$truncDiscard=$discardFrac" if $truncLen > 0;
		push @as, "--discard-reads=$truncLen" if $discardFrac > 0;
		push @as, "--quality=$qual";
		push @as, "--preprocess" if $inputType eq "manifest";
		if($specifyRef) {
			# User-specified ref URL
			my ($proto, $bucket, $path) = S3Util::parsePath($ref);
			defined($proto)  || croak("Could not parse reference path: $ref");
			defined($bucket) || croak("Could not parse bucket in reference path: $ref");
			defined($path)   || croak("Could not parse path in reference path: $ref");
			# TODO: check if reference exists
			push @as, "--ref=$ref";
		} else {
			# Pre-built ref
			defined($refMap{$genome}) || croak("Bad genome short name: \"$genome\"");
			push @as, "--ref=$refMap{$genome}";
		}
		push @as, "--bowtie-args=$bowtieOpts";
		push @as, "--soapsnp-args=$bowtieOpts";
		push @as, "--soapsnp-args=$soapsnpOpts";
		push @as, "--soapsnp-hap-args=$soapsnpOptsHap";
		push @as, "--soapsnp-dip-args=$soapsnpOptsDip";
		if($haploids eq "all-diploid") {
			# no arg
		} elsif($haploids eq "all-haploid") {
			push @as, "--all-haploids";
		} elsif($haploids eq "all-diploid-except") {
			push @as, "--haploids=$haploidsList";
		} else {
			croak("Bad value for haplids: \"$haploids\"");
		}
	}
	push @as, "$clusterWait";
	push @as, "--instances=$numNodes";
	push @as, "--verbose";
	push @as, "--instance-type=$instanceType";
	
	my $stdout = "";
	my $stderr = "";

	my $stdoutf = sub { $stdout .= $_[0]; };
	my $stdoutff = sub {
		my $str = shift @_;
		$stdout .= sprintf $str, @_;
	};
	my $stderrf = sub { $stderr .= $_[0]; };
	my $stderrff = sub {
		my $str = shift @_;
		$stderr .= sprintf $str, @_;
	};
	if(!defined($ENV{HOME})) {
		$stderr .= "Had to define HOME in myrna.pl\n";
		$ENV{HOME} = "/var/www/cgi-bin";
	}
	CrossbowIface::crossbow(\@as, "crossbow.pl", "(no usage)", $stdoutf, $stdoutff, $stderrf, $stderrff);
	
	my $jobid = "";
	$stdout =~ /Created job flow (.*)/;
	$jobid = $1 if defined($1);
	
	my $resultHtml = "";
	if($jobid eq "") {
		my $asStr = "";
		for my $a (@as) {
			next unless $a ne "";
			$asStr .= "$a\n";
		}
		# Error condition
		$resultHtml .= <<HTML;
			<font color="red"><b>Error invoking Crossbow. Job not submitted.</b></font>
			
			<br>Arguments given to Crossbow driver script:
			<pre>$asStr</pre>
			
			Standard output from driver:
			<pre>$stdout</pre>
			
			Standard error from driver:
			<pre>$stderr</pre>
HTML
	} else {
		# Everything seemed to go fine
		$resultHtml .= <<HTML;
			<br>
			Job created; MapReduce job ID = $jobid
			<br>
			Go to the
			<a href="https://console.aws.amazon.com/elasticmapreduce" target="_blank">
			AWS Console's Elastic MapReduce</a> tab to monitor your
			job.
HTML
	}
	return $resultHtml;
}

sub main {
	my $html = "";
	$html .= <<HTML;
<html>
<head>
</head>
<body>
<script src="http://jotform.com/js/form.js?v2.0.1347" type="text/javascript"></script>
<style type="text/css">
.main {
  font-family:"Verdana";
  font-size:11px;
  color:#666666;
}
.tbmain{ 
 /* Changes on the form */
 background: white !important;
}
.left{
  /* Changes on the form */
  color: black !important; 
  font-family: Verdana !important;
  font-size: 12px !important;
}
.right{
  /* Changes on the form */
  color: black !important; 
  font-family: Verdana !important;
  font-size: 12px !important;
}
.check{
  color: black !important; 
  font-family: Verdana !important;
  font-size: 10px !important;
}
.head{
  color:#333333;
  font-size:20px;;
  text-decoration:underline;
  font-family:"Verdana";
}
td.left {
  font-family:"Verdana";
  font-size:12px;
  color:black;
}
.pagebreak{
  font-family:"Verdana";
  font-size:12px;
  color:black;
}
.tbmain{
  height:100%;
  background:white;
}
span.required{
  font-size: 13px !important;
  color: red !important;
}

div.backButton{
    background: transparent url("http://jotform.com//images/btn_back.gif") no-repeat scroll 0 0;
    height:16px;
    width:53px;
    float:left;
    margin-bottom:15px;
    padding-right:5px;
}
div.backButton:hover{
    background: transparent url("http://jotform.com//images/btn_back_over.gif") no-repeat scroll 0 0;
}
div.backButton:active{
    background: transparent url("http://jotform.com//images/btn_back_down.gif") no-repeat scroll 0 0;
}
div.nextButton{
    background: transparent url("http://jotform.com//images/btn_next.gif") no-repeat scroll 0 0;
    height:16px;
    width:53px;
    float: left;
    margin-bottom:15px;
    padding-right:5px;
}
div.nextButton:hover{
    background: transparent url("http://jotform.com//images/btn_next_over.gif") no-repeat scroll 0 0;
}
div.nextButton:active{
    background: transparent url("http://jotform.com//images/btn_next_down.gif") no-repeat scroll 0 0;
}
.pageinfo{
    padding-right:5px;
    margin-bottom:15px;
    float:left;
}
 
</style> 
<table width="100%" cellpadding="2" cellspacing="0" class="tbmain">
<tr><td class="topleft" width="10" height="10">&nbsp;</td>
<td class="topmid">&nbsp;</td>
<td class="topright" width="10" height="10">&nbsp;</td>
  </tr>
<tr>
<td class="midleft" width="10">&nbsp;&nbsp;&nbsp;</td>
<td class="midmid" valign="top">
<form accept-charset="utf-8"  action="/crossbowform" method="post" name="form">
<div id="main"> 
<div class="pagebreak"> 
<table width="520" cellpadding="5" cellspacing="0">
 <tr >
  <td class="left" colspan=2>
   <h2>Crossbow $VERSION</h2>
  </td>
 </tr>
 <tr >
  <td width="165" class="left" >
   <label >AWS ID <span class="required">*</span></label>
  </td>
  <td class="right" >
   <input type="text"
    onblur="validate(this,'Required')"
    onkeypress="jsResetCheckS3Creds()"
    size="25" name="AWSId" class="text" value="" onmouseover="ddrivetip('Your AWS Access Key ID, usually 20 characters long (not your Secret Access Key or your Account ID).', 200)" onmouseout="hideddrivetip()" maxlength="100" maxsize="100"></input>
  </td>
 </tr>
 <tr >
  <td width="165" class="left" >
   <label >AWS Secret Key <span class="required">*</span></label>
  </td>
  <td class="right" >
   <input type="password"
    onblur="validate(this,'Required')"
    onkeypress="jsResetCheckS3Creds()"
    size="50" name="AWSSecret" class="text" value="" onmouseover="ddrivetip('Your AWS Secret Access Key, usually 40 characters long (not your Access Key ID or your Account ID).', 200)" onmouseout="hideddrivetip()" maxlength="100" maxsize="100"></input>
  </td>
 </tr>
 <tr >
  <td width="165" class="left" >
   <label >AWS Keypair Name</label>
  </td>
  <td class="right" >
   <input type="text"
    size="30" name="AWSKeyPair" class="text" value="gsg-keypair" onmouseover="ddrivetip('Name of the keypair that AWS should install on the cluster, allowing you to log in.', 200)" onmouseout="hideddrivetip()" maxlength="100" maxsize="100"></input>
   <a href="https://console.aws.amazon.com/ec2/home#c=EC2&s=KeyPairs" target="_blank">Look it up</a>
  </td>
 </tr>
 <tr >
  <td width="165" class="left" >
  </td>
  <td class="right" >
   <span id="credcheck" class="check"><a href="javascript:jsCheckS3Creds()">Check credentials...</a></span>
  </td>
 </tr>
 <tr >
  <td colspan="2" >
   <hr>
  </td>
 </tr>
 <tr >
  <td width="165" class="left" >
   <label >Job name</label>
  </td>
  <td class="right" >
   <input type="text" size="30" name="JobName" class="text" value="Crossbow" onmouseover="ddrivetip('Name given to Elastic MapReduce job.', 200)" onmouseout="hideddrivetip()" maxlength="100" maxsize="100"></input>
  </td>
 </tr>
 <tr >
  <td width="165" class="left" valign="top" >
   <label>Job type</label>
  </td>
  <td class="right">
   <input type="radio" class="other" name="JobType" onclick="enableApp()" onmouseover="ddrivetip('Run the Crossbow pipeline, starting with a manifest file or preprocessed reads, and ending with Crossbow results.', 200)" onmouseout="hideddrivetip()" value="--crossbow" checked  /> 
    <label class="left">Crossbow</label> <br /> 
   <input type="radio" class="other" name="JobType" onclick="disableApp()" onmouseover="ddrivetip('Just run the Preprocess step and place preprocessed reads at Output URL.', 200)" onmouseout="hideddrivetip()" value="--just-preprocess" /> 
    <label class="left">Just preprocess reads</label> <br /> 
  </td>
 </tr>
 <tr >
  <td colspan="2" >
   <hr>
  </td>
 </tr>
 <tr >
  <td width="165" class="left" >
   <label >Input URL <span class="required">*</span></label>
  </td>
  <td class="right" >
   <input type="text" size="60" name="InputURL"
    onmouseover="ddrivetip('S3 URL where manifest file or preprocessed reads are located.', 200)"
    onmouseout="hideddrivetip()"
    class="text" value="s3n://"
    onblur="validate(this,'Required')"
    onkeypress="jsResetCheckInputURL()"
    maxlength="400" maxsize="400" />
  </td>
 </tr>
 <tr >
  <td width="165" class="left" >
  </td>
  <td class="right" >
   <div id="inputcheck" class="check"><a href="javascript:jsCheckInputURL()">Check that input URL exists...</a></div>
  </td>
 </tr>
 
 <tr >
  <td width="165" class="left" >
   <label >Output URL <span class="required">*</span></label>
  </td>
  <td class="right" >
   <input type="text" size="60" name="OutputURL"
    onmouseover="ddrivetip('S3 URL where Crossbow output should be placed.', 200)"
    onmouseout="hideddrivetip()"
    class="text" value="s3n://"
    onblur="validate(this,'Required')"
    onkeypress="jsResetCheckOutputURL()"
    maxlength="400" maxsize="400" />
  </td>
 </tr>
 <tr >
  <td width="165" class="left" >
  </td>
  <td class="right" >
   <div id="outputcheck" class="check"><a href="javascript:jsCheckOutputURL()">Check that output URL doesn't exist...</a></div>
  </td>
 </tr>
 <tr >
  <td colspan="2" >
   <hr>
  </td>
 </tr>
 <tr >
  <td width="165" class="left" valign="top" >
   <label id="app-input-type-label">Input type</label>
  </td>
  <td class="right">
   <input type="radio" id="app-input-type-radio-preprocess" class="other" name="InputType" name="InputType" onmouseover="ddrivetip('Input URL points to a directory of files that have already been preprocessed by Crossbow.', 200)" onmouseout="hideddrivetip()" value="preprocessed" checked  /> 
    <label id="app-input-type-preprocess-label">Preprocessed reads</label> <br /> 
   <input type="radio" id="app-input-type-radio-manifest" class="other" name="InputType" name="InputType" onmouseover="ddrivetip('Input URL points to a manifest file listing publicly-readable URLs of input FASTQ files; FASTQ files are both preprocessed and analyzed.', 200)" onmouseout="hideddrivetip()" value="manifest"   /> 
    <label id="app-input-type-manifest-label">Manifest file</label> <br /> 
  </td>
 </tr>
 <tr >
  <td width="165" class="left" >
   <label id="app-truncate-length-label">Truncate length</label>
  </td>
  <td class="right" >
   <input type="text" size="5" id="app-truncate-length-text" class="text" name="TruncateLength" onmouseover="ddrivetip('Specifies N such that reads longer than N bases are truncated to length N by removing bases from the 3\\' end.', 200)" onmouseout="hideddrivetip()" class="text" value="0" onblur="validate(this,'Numeric')" maxlength="5" maxsize="5" />
   <span class="main">&nbsp(If blank or 0, truncation is disabled)</span>
  </td>
 </tr>
 <tr >
  <td width="165" class="left" valign="top" >
  </td>
  <td valign="top" class="right">
   <input id="app-skip-truncate-check" type="checkbox" class="other"
    name="TruncateDiscard"
    value="--truncate-discard" /> 
    <label id="app-skip-truncate-label">Skip reads shorter than truncate length</label> <br /> 
  </td>
 </tr>
 <tr >
  <td width="165" class="left" >
   <label id="app-discard-fraction-label">Discard fraction</label>
  </td>
  <td class="right" >
   <input id="app-discard-fraction-text" type="text" size="5" name="DiscardFraction" onmouseover="ddrivetip('Randomly discard specified fraction of the input reads.  Useful for testing purposes.', 200)" onmouseout="hideddrivetip()" class="text" value="0" onblur="validate(this,'Numeric')" maxlength="5" maxsize="5" />
  </td>
 </tr>
 <tr >
  <td width="165" class="left"  valign="top" >
   <label id="app-quality-label">Quality encoding</label>
  </td>
  <td class="right">
   <select id="app-quality-dropdown" class="other" name="QualityEncoding" onmouseover="ddrivetip('Quality value encoding scheme used for input reads.', 200)" onmouseout="hideddrivetip()">
    <option value="phred33">Phred+33</option>
    <option value="phred64">Phred+64</option>
    <option value="solexa64">Solexa+64</option>
   </select>
  </td>
 </tr>
 <tr >
  <td width="165" class="left"  valign="top" >
   <label id="app-genome-label">Genome/Annotation</label>
  </td>
  <td class="right">
   <select id="app-genome-dropdown" class="other" name="Genome" onmouseover="ddrivetip('Genome assembly to use as reference genome and annotation database to use for prior SNP probabilities.', 200)" onmouseout="hideddrivetip()" >
    <option value="hg18_130">Human (v36, dbSNP 130)</option>
    <option value="mm9_130">Mouse (v37, dbSNP 130)</option>
    <option value="e_coli">E. coli O157:H7</option>
   </select>
  </td>
 </tr>
 <tr>
  <td width="165" class="left"  valign="top" >
  </td>
  <td class="right">
   <input id="app-specify-ref-check" type="checkbox" onclick="updateElements()" onmouseover="ddrivetip('Specify an S3 url for a reference jar.', 200)" onmouseout="hideddrivetip()" class="other"
    value="1"
    name="SpecifyRef"
    /> 
    <label id="app-specify-ref-label">Specify reference jar URL:</label> <br />
   <br/>
   <!-- Reference URL text box -->
   <input id="app-specify-ref-text"
    disabled
    type="text"
    size="50"
    name="Ref"
    onblur="validate(this,'Required')"
    onkeypress="jsResetCheckRefURL()"
    onmouseover="ddrivetip('Specify an S3 url for a reference jar.', 200)"
    onmouseout="hideddrivetip()"
    value="s3n://" class="text" value=""  maxlength="100" maxsize="100" />
  </td>
 </tr>
 <tr>
  <td width="165" class="left" valign="top" >
  </td>
  <td class="right">
   <div id="refcheck" class="check"><a href="javascript:jsCheckRefURL()">Check that reference jar URL exists...</a></div>
  </td>
 </tr>
 <tr >
  <td width="165" class="left" >
   <label id="app-bowtie-options-label">Bowtie options</label>
  </td>
  <td class="right" >
   <input id="app-bowtie-options-text" type="text" size="50" name="BowtieOpts" onmouseover="ddrivetip('Options to pass to Bowtie in the Align stage.', 200)" onmouseout="hideddrivetip()" class="text" value="-m 1"  maxlength="400" maxsize="400" />
  </td>
 </tr>
 <tr >
  <td width="165" class="left" >
   <label id="app-soapsnp-options-label">SOAPsnp options</label>
  </td>
  <td class="right" >
   <input id="app-soapsnp-options-text" type="text" size="50" name="SoapsnpOpts" onmouseover="ddrivetip('Options to pass to SOAPsnp in the Call SNPs stage.', 200)" onmouseout="hideddrivetip()" class="text" value="-2 -u -n -q"  maxlength="500" maxsize="500" />
  </td>
 </tr>
 <tr >
  <td width="165" class="left" >
   <label id="app-soapsnp-haploid-options-label">Additional SOAPsnp options for haploids</label>
  </td>
  <td class="right" >
   <input id="app-soapsnp-diploid-options-text" type="text" size="50" name="SoapsnpOptsHap" onmouseover="ddrivetip('Options to pass to SOAPsnp in the Call SNPs stage when the reference chromosome is haploid.', 200)" onmouseout="hideddrivetip()" class="text" value="-r 0.0001"  maxlength="500" maxsize="500" />
  </td>
 </tr>
 <tr >
  <td width="165" class="left" >
   <label id="app-soapsnp-diploid-options-label">Additional SOAPSNP options for diploids</label>
  </td>
  <td class="right" >
   <input id="app-soapsnp-diploid-options-text" type="text" size="50" name="SoapsnpOptsDip" onmouseover="ddrivetip('Options to pass to SOAPsnp in the Call SNPs stage when the reference chromosome is diploid.', 200)" onmouseout="hideddrivetip()" class="text" value="-r 0.00005 -e 0.0001"  maxlength="500" maxsize="500" />
  </td>
 </tr>
 <tr >

  <td width="165" class="left"  valign="top" >
   <label id="app-ploidy-label" >Chromosome ploidy</label>
  </td>
  <td class="right">
   <input id="app-ploidy1-radio" type="radio"  class="other" name="Haploids" onclick="updateElements()" value="all-diploid" checked /> 
    <label id="app-ploidy1-label">All chrosmosomes are diploid</label> <br /> 
   <input id="app-ploidy2-radio" type="radio"  class="other" name="Haploids" onclick="updateElements()" value="all-haploid" /> 
    <label id="app-ploidy2-label">All are haploid</label> <br /> 
   <input id="app-ploidy3-radio" type="radio"  class="other" name="Haploids" onclick="updateElements()" value="all-diploid-except" /> 
    <label id="app-ploidy3-label">All are diploid except: </label> 
    <input id="app-ploidy-text" disabled type="text" size="50" name="HaploidsList" onmouseover="ddrivetip('Comma-separated list of names of chromosomes that should be considered haploid.', 200)" onmouseout="hideddrivetip()" class="text" value=""  maxlength="100" maxsize="100" />
    <br />
  </td>
 </tr>
 <tr >
  <td colspan="2" >
   <hr>
  </td>
 </tr>
 <tr >
  <td width="165" class="left" valign="top" >
   <label id="options-label">Options</label>
  </td>
  <td valign="top" class="right">
   <input id="wait-check" type="checkbox" onmouseover="ddrivetip('Typically the cluster is terminated as soon as the job either completes or aborts.  Check this to keep the cluster running either way.', 200)" onmouseout="hideddrivetip()" class="other"
    name="ClusterWait"
    value="--stay-alive" />
    <label id="wait-label">Keep cluster running after job finishes/aborts</label> <br /> 
  </td>
 </tr>
 <tr >
  <td width="165" class="left" >
   <label ># EC2 instances</label>
  </td>
  <td class="right" >
   <input type="text" size="5" name="NumNodes" onmouseover="ddrivetip('Number of Amazon EC2 instances (virtual computers) to use for this computation.', 200)" onmouseout="hideddrivetip()" class="text" value="1" onblur="validate(this,'Numeric')" maxlength="5" maxsize="5" />
  </td>
 </tr>
 <tr >
  <td width="165" class="left"  valign="top" >
   <label><a href="http://aws.amazon.com/ec2/instance-types/" target="_blank">Instance type</a></label>
  </td>
  <td class="right">
   <select class="other" name="InstanceType" onmouseover="ddrivetip('Type of EC2 instance (virtual computer) to use; c1.xlarge is strongly recommended.', 200)" onmouseout="hideddrivetip()">
    <option value="c1.xlarge">c1.xlarge (recommended)</option>
    <option value="c1.medium">c1.medium</option>
    
    <option value="m2.xlarge">m2.xlarge</option>
    <option value="m2.2xlarge">m2.2xlarge</option>
    <option value="m2.4xlarge">m2.4xlarge</option>
    
    <option value="m1.xlarge">m1.xlarge</option>
    <option value="m1.large">m1.large</option>
    <option value="m1.small">m1.small</option>
   </select>
  </td>
 </tr>
 <tr >
  <td width="165" class="left" valign="top">
   <span class="main">Made with the help of</span>
   <br>
   <a href="http://www.jotform.com/" target="_blank">
   <img border=0 width=115
    src="http://www.jotform.com/images/jotform.gif"
    alt="Made with the help of JotForm" /></a>
  </td>
  <td class="right">
  <input type="button" class="btn" value="Submit"
   onclick="document.getElementById('result1').innerHTML = '<img border=0 src=\\'/wait.gif\\' /> Creating job, please wait ...' ;
    submitClicked(
    ['AWSId',
     'AWSSecret',
     'AWSKeyPair',
     'JobName',
     'JobType',
     'InputURL',
     'OutputURL',
     'InputType',
     'TruncateLength',
     'TruncateDiscard',
     'DiscardFraction',
     'QualityEncoding',
     'Genome',
     'SpecifyRef',
     'Ref',
     'BowtieOpts',
     'SoapsnpOpts',
     'SoapsnpOptsHap',
     'SoapsnpOptsDip',
     'Haploids',
     'HaploidsList',
     'ClusterWait',
     'NumNodes',
     'InstanceType'],
     ['result1'])" />
 </td>
 <tr >
  <td colspan="2" class="right">
  <span class="main"><b>Please cite</b>:
  Langmead B, Schatz MC, Lin J, Pop M, Salzberg SL.
    <a href="http://genomebiology.com/2009/10/11/R134">Searching for SNPs with cloud computing</a>. <i>Genome Biology</i> 10:R134.</span>
  </td>
 </tr>
 <tr >
  <td colspan="2" >
   <hr> <!-- Horizontal rule -->
  </td>
 </tr>
 <tr>
  <td colspan=2 id="result1" class="right">
    <!-- Insert result here -->
  </td>
 </tr>
</table>
</div>
</div>
</form>
</td>
<td class="midright" width="10">&nbsp;&nbsp;&nbsp;</td>
</tr>
<tr>
 <td class="bottomleft" width="10" height="10">&nbsp;</td>
 <td class="bottommid">&nbsp;</td>
 <td class="bottomright" width="10" height="10">&nbsp;</td>
</tr>
</table>
<script type="text/javascript">

var isAppRegex=/^app-/;
var isLabel=/-label\$/;

function haploidTextEnabled() {
	var sel;
	for(i = 0; i < document.form.Haploids.length; i++) {
		if(document.form.Haploids[i].checked) {
			sel = i;
			break;
		}
	}
	return sel == 2;
}

function updateElements() {
	if(document.form.SpecifyRef.checked) {
		document.form.Ref.disabled = false;
		document.form.Ref.style.color = "black";
		document.form.Genome.disabled = true;
	} else {
		document.form.Ref.disabled = true;
		document.form.Ref.style.color = "gray";
		document.form.Genome.disabled = false;
	}
	if(haploidTextEnabled()) {
		document.form.HaploidsList.disabled = false;
		document.form.HaploidsList.style.color = "black";
	} else {
		document.form.HaploidsList.disabled = true;
		document.form.HaploidsList.style.color = "gray";
	}
}

function checkS3ExistsWait(div) {
	document.getElementById(div).innerHTML = '<img border=0 width=18 src=\\'/wait.gif\\' />';
}

function enableApp() {
	var elts = document.getElementsByTagName('*');
	var count = elts.length;
	for(i = 0; i < count; i++) {
		var element = elts[i]; 
		if(isAppRegex.test(element.id)) {
			// Yes, this is an app-related form element that should be re-enabled
			element.disabled = false;
			if(isLabel.test(element.id) || element.type == "text") {
				element.style.color = "black";
			}
		}
	}
	updateElements();
}
function disableApp() {
	var elts = document.getElementsByTagName('*');
	var count = elts.length;
	for(i = 0; i < count; i++) {
		var element = elts[i]; 
		if(isAppRegex.test(element.id)) {
			// Yes, this is an app-related form element that should be disabled
			element.disabled = true;
			if(isLabel.test(element.id) || element.type == "text") {
				element.style.color = "gray";
			}
		}
	}
}

function jsResetCheckS3Creds() {
	document.getElementById('credcheck').innerHTML = '<a href=\\'javascript:jsCheckS3Creds()\\'>Check credentials...</a>';
}

function jsCheckS3Creds() {
	document.getElementById('credcheck').innerHTML = "Checking, please wait...";
	checkS3Creds(['AWSId', 'AWSSecret'], ['credcheck']);
}

function jsResetCheckRefURL() {
	document.getElementById('refcheck').innerHTML = '<a href=\\'javascript:jsCheckRefURL()\\'>Check that reference jar URL exists...</a>';
}

function jsCheckRefURL() {
	document.getElementById('refcheck').innerHTML = "Checking, please wait...";
	checkInputURL(['AWSId', 'AWSSecret', 'Ref'], ['refcheck']);
}

function jsResetCheckInputURL() {
	document.getElementById('inputcheck').innerHTML = '<a href=\\'javascript:jsCheckInputURL()\\'>Check that input URL exists...</a>';
}

function jsCheckInputURL() {
	document.getElementById('inputcheck').innerHTML = "Checking, please wait...";
	checkInputURL(['AWSId', 'AWSSecret', 'InputURL'], ['inputcheck']);
}

function jsResetCheckOutputURL() {
	document.getElementById('outputcheck').innerHTML = '<a href=\\'javascript:jsCheckOutputURL()\\'>Check that output URL doesn\\'t exist...</a>';
}

function jsCheckOutputURL() {
	document.getElementById('outputcheck').innerHTML = "Checking, please wait...";
	checkOutputURL(['AWSId', 'AWSSecret', 'OutputURL'], ['outputcheck']);
}

validate();

</script>

<!-- Google analytics code -->
<script type="text/javascript">
var gaJsHost = (("https:" == document.location.protocol) ? "https://ssl." : "http://www.");
document.write(unescape("%3Cscript src='" + gaJsHost + "google-analytics.com/ga.js' type='text/javascript'%3E%3C/script%3E"));
</script>
<script type="text/javascript">
var pageTracker = _gat._getTracker("UA-5334290-1");
pageTracker._trackPageview();
</script>
<!-- End google analytics code -->

</body>
</html>
HTML
	return $html;
}

exit 0;
__END__
