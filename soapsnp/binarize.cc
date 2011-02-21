/*
 * binarize.cc
 *
 *  Created on: May 20, 2009
 *      Author: Ben Langmead
 *
 *  Serialize binarized sequences to files so that they can be memory-
 *  mapped in future invocations.
 */

#include "soap_snp.h"
#include <getopt.h>

using namespace std;

int usage() {
	cerr<<"SoapSNP binarize version 1.02 "<<endl;
	cerr<<"\nLicense GPLv3+: GNU GPL version 3 or later <http://gnu.org/licenses/gpl.html>"<<endl;
	cerr<<"This is free software: you are free to change and redistribute it."<<endl;
	cerr<<"There is NO WARRANTY, to the extent permitted by law.\n"<<endl;

	exit(1);
	return 0;
}

int readme() {
	return usage();
}

int main(int argc, char **argv) {
	int c;
	bool refine_mode;
	string ref_seq, dbsnp, outdir = ".";
	while((c = getopt(argc, argv, "d:s:o:2h?")) != -1) {
		switch(c) {
			case 'd': {
				// The reference genome in fasta format
				ref_seq = optarg;
				break;
			}
			case 's': {
				// Optional: A pre-formated dbSNP table
				dbsnp = optarg;
				break;
			}
			case 'o': {
				// Optional: Output directory (default: .)
				outdir = optarg;
				break;
			}
			case '2': {
				// Refine prior probability based on dbSNP information
				refine_mode = true;
				break;
			}
			case 'h':readme();break;
			case '?':usage();break;
			default: cerr << "Unknown error in command line parameters" << endl;
		}
	}
	if(ref_seq.empty()) {
		cerr << "Error: Must specify reference sequence using -d" << endl;
		usage();
		exit(1);
	}
	ifstream ref_seq_in(ref_seq.c_str());
	ifstream dbsnp_in(dbsnp.c_str());
	Genome * genome = new Genome(ref_seq_in, dbsnp_in, outdir.c_str());
	delete genome;
	return 0;
}
