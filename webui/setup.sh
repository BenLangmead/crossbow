#!/bin/sh

##
# setup.sh
#
# Not-quite-automated set of commands that should be run on a new EC2
# instance to get it ready to run the Crossbow or Myrna web interfaces.
#
# EC2 changes pretty often, so your mileage may vary.
#

sudo yum -y install cpan gcc libxml2-devel

sudo cpan
#o conf prerequisites_policy follow
#o conf commit
#install CPAN::Bundle
#reload cpan
#install Class::Accessor CGI::Ajax Net::Amazon::S3 MIME::Types
#install Net::Amazon::S3
