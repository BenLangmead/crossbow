#
#  Author: Ben Langmead
#    Date: 9/26/2009
#
# Package Crossbow files for release.
#

VERSION=`cat VERSION`
PKG_BASE=.pkg
APP=crossbow
PKG=.pkg/$APP-${VERSION}

echo "Should have already run 'make doc' to make documentation"

rm -rf $PKG_BASE 
mkdir -p $PKG

# Copy Crossbow sources
cp *.pl *.pm $PKG/
for i in cb ; do
	for j in emr hadoop local ; do
		cp ${i}_$j $PKG/
		chmod a+x $PKG/${i}_$j
	done
done
chmod a+x $PKG/*.pl

# Copy modified-SOAPsnp sources
mkdir -p $PKG/soapsnp
cp soapsnp/*.cc \
   soapsnp/*.h \
   soapsnp/COPYING \
   soapsnp/readme \
   soapsnp/release \
   soapsnp/makefile \
   $PKG/soapsnp/

# Include the Bowtie and SOAPsnp binaries for 32-bit and 64-bit Linux/Mac
mkdir -p $PKG/bin/linux32
mkdir -p $PKG/bin/linux64
mkdir -p $PKG/bin/mac32
mkdir -p $PKG/bin/mac64
cp bin/linux32/* $PKG/bin/linux32/
cp bin/linux64/* $PKG/bin/linux64/
cp bin/mac32/* $PKG/bin/mac32/
cp bin/mac64/* $PKG/bin/mac64/

# Copy contrib dir
mkdir -p $PKG/contrib
cp contrib/* $PKG/contrib

# Copy reftools dir
mkdir -p $PKG/reftools
rm -f reftools/*.jar
cp reftools/* $PKG/reftools
chmod a+x $PKG/reftools/*

# Copy example dir
mkdir -p $PKG/example
for i in e_coli mouse17 ; do
	mkdir -p $PKG/example/$i
	cp example/$i/copy.manifest $PKG/example/$i/
	cp example/$i/small.manifest $PKG/example/$i/
	cp example/$i/full.manifest $PKG/example/$i/
done

# Copy doc dir
mkdir -p $PKG/doc
cp doc/*.html $PKG/doc
cp doc/*.css $PKG/doc
mkdir -p $PKG/doc/images
cp -r doc/images/*.png $PKG/doc/images/

cp VERSION NEWS MANUAL LICENSE* TUTORIAL $PKG/

pushd $PKG_BASE
zip -r $APP-${VERSION}.zip $APP-${VERSION}
popd
cp $PKG_BASE/$APP-${VERSION}.zip .
