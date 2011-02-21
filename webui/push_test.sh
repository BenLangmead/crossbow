#!/bin/sh

# push.sh
#
# Run from the crossbow base directory (i.e. sh webui/push.sh) to copy
# the appropriate files to the EC2 web server.  The EC2_KEYPAIR
# environment variable must point to the id_rsa-gsg-keypair (or
# similarly named) file with your keypair.
#

[ -z "$EC2_KEYPAIR" ] && echo "Must set EC2_KEYPAIR" && exit 1

[ ! -d webui ] && echo "Run from CROSSBOW_HOME" && exit 1

ARGS=$*
[ -z "$ARGS" ] && ARGS="ec2-75-101-218-11.compute-1.amazonaws.com ec2-184-73-43-172.compute-1.amazonaws.com"

for i in $ARGS ; do
	echo $i

	# Move perl scripts to cgi-bin
	sed -e 's/my $debugLev = 0;/my $debugLev = 2;/' < webui/crossbow.pl > webui/crossbow.test.pl
	ssh -i $EC2_KEYPAIR root@$i mkdir -p /var/www/cgi-bin/test
	scp -i $EC2_KEYPAIR webui/S3Util.pm Tools.pm AWS.pm CrossbowIface.pm webui/wait.gif cb_emr root@$i:/var/www/cgi-bin/test/
	scp -i $EC2_KEYPAIR VERSION root@$i:/var/www/cgi-bin/test/VERSION_CROSSBOW
	scp -i $EC2_KEYPAIR webui/crossbow.test.pl root@$i:/var/www/cgi-bin/test/crossbow.pl
	scp -i $EC2_KEYPAIR webui/wait.gif root@$i:/home/webuser/helloworld/htdocs/
	ssh -i $EC2_KEYPAIR root@$i chmod a+x /var/www/cgi-bin/test/*.pl

	# URL to surf to
	echo "http://$i/cgi-bin/test/crossbow.pl\n";
done
