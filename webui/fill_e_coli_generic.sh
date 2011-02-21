#!/bin/sh

#
# fill_e_coli_generic.sh
#
# Uses Applescript/Safari to fill in the Crossbow Web UI form
# generically (i.e. with placeholders for AWS credentials and bucket
# name) for the E. coli example.
#

CROSSBOW_URL=http://ec2-184-73-43-172.compute-1.amazonaws.com/cgi-bin/crossbow.pl

cat >.fill_e_coli.applescript <<EOF
tell application "Safari"
	activate
	tell (make new document) to set URL to "$CROSSBOW_URL"
	delay 6
	set doc to document "$CROSSBOW_URL"
	log (doc's name)
	do JavaScript "document.forms['form']['AWSId'].value     = '<YOUR-AWS-ID>'" in doc
	do JavaScript "document.forms['form']['AWSSecret'].value = '<YOUR-AWS-SECRET-KEY>'" in doc
	do JavaScript "document.forms['form']['JobName'].value   = 'Crossbow-Ecoli'" in doc
	do JavaScript "document.forms['form']['InputURL'].value  = 's3n://<YOUR-BUCKET>/example/e_coli/small.manifest'" in doc
	do JavaScript "document.forms['form']['OutputURL'].value = 's3n://<YOUR-BUCKET>/example/e_coli/output_small'" in doc
	do JavaScript "document.forms['form']['InputType'][1].checked = 1" in doc
	do JavaScript "document.forms['form']['InputType'][0].checked = 0" in doc
	do JavaScript "document.forms['form']['QualityEncoding'].value = 'phred33'" in doc
	do JavaScript "document.forms['form']['Genome'].value = 'e_coli'" in doc
	do JavaScript "document.forms['form']['NumNodes'].value = '1'" in doc
	do JavaScript "document.forms['form']['InstanceType'].value = 'c1.xlarge'" in doc
	do JavaScript "document.forms['form']['Haploids'].value = 'all-haploid'" in doc
	do JavaScript "document.forms['form']['Haploids'][1].checked = 1" in doc
end tell
EOF

osascript .fill_e_coli.applescript
rm -f .fill_e_coli.applescript
