#include "soap_snp.h"
#include <getopt.h>

using namespace std;

int usage() {
	cerr<<"SoapSNP version 1.02, Crossbow modifications (last changed 10/10/2010)"<<endl;
	cerr<<"Compulsory Parameters:"<<endl;
	cerr<<"-i <FILE> Input SORTED Soap Result"<<endl;
	cerr<<"-d <FILE> Reference Sequence in fasta format"<<endl;
	cerr<<"-o <FILE> Output consensus file"<<endl;
	cerr<<"Optional Parameters:(Default in [])"<<endl;
	cerr<<"-z <Char> ASCII chracter standing for quality==0 [@]"<<endl;
	cerr<<"-g <Double> Global Error Dependency Coefficient, 0.0(complete dependent)~1.0(complete independent)[0.9]"<<endl;
	cerr<<"-p <Double> PCR Error Dependency Coefficient, 0.0(complete dependent)~1.0(complete independent)[0.5]"<<endl;
	cerr<<"-r <Double> novel altHOM prior probability [0.0005]"<<endl;
	cerr<<"-e <Double> novel HET prior probability [0.0010]"<<endl;
	cerr<<"-t set transition/transversion ratio to 2:1 in prior probability"<<endl;
	cerr<<"-s <FILE> Pre-formated dbSNP information"<<endl;
	cerr<<"-2 specify this option will REFINE SNPs using dbSNPs information [Off]"<<endl;
	cerr<<"-a <Double> Validated HET prior, if no allele frequency known [0.1]"<<endl;
	cerr<<"-b <Double> Validated altHOM prior, if no allele frequency known[0.05]"<<endl;
	cerr<<"-j <Double> Unvalidated HET prior, if no allele frequency known [0.02]"<<endl;
	cerr<<"-k <Double> Unvalidated altHOM rate, if no allele frequency known[0.01]"<<endl;
	cerr<<"-u Enable rank sum test to give HET further penalty for better accuracy. [Off]"<<endl;
	//cerr<<"-n Enable binomial probability calculation to give HET for better accuracy. [Off]"<<endl;
	cerr<<"-m Enable monoploid calling mode, this will ensure all consensus as HOM and you probably should SPECIFY higher altHOM rate. [Off]"<<endl;
	cerr<<"-q Only output potential SNPs. Useful in Text output mode. [Off]"<<endl;
	cerr<<"-M <FILE> Output the quality calibration matrix; the matrix can be reused with -I if you rerun the program"<<endl;
	cerr<<"-I <FILE> Input previous quality calibration matrix. It cannot be used simutaneously with -M"<<endl;
	cerr<<"-L <short> maximum length of read [45]"<<endl;
	cerr<<"-Q <short> maximum FASTQ quality score [40]"<<endl;
	cerr<<"-F <int> Output format. 0: Text; 1: GLFv2; 2: GPFv2.[0]"<<endl;
	cerr<<"-E <String> Extra headers EXCEPT CHROMOSOME FIELD specified in GLFv2 output. Format is \"TypeName1:DataName1:TypeName2:DataName2\"[""]"<<endl;
	cerr<<"-T <FILE> Only call consensus on regions specified in FILE. Format: ChrName\\tStart\\tEnd."<<endl;
	cerr<<"-c Use the crossbow input format [Off]"<<endl;
	cerr<<"-K In -q mode, print consensus info for every dbsnp pos even if there's no SNP [Off]"<<endl;
	//cerr<<"-S <FILE> Output summary of consensus"<<endl;
	cerr<<"-H Print Hadoop status updates" << endl;
	cerr<<"-v Verbose mode"<<endl;
	cerr<<"-h Display this help"<<endl;

	cerr<<"\nLicense GPLv3+: GNU GPL version 3 or later <http://gnu.org/licenses/gpl.html>"<<endl;
	cerr<<"This is free software: you are free to change and redistribute it."<<endl;
	cerr<<"There is NO WARRANTY, to the extent permitted by law.\n"<<endl;

	exit(1);
	return 0;
}

int readme() {
	return usage();
}

unsigned long poscalled = 0;
unsigned long poscalled_knownsnp = 0;
unsigned long poscalled_uncov_uni = 0;
unsigned long poscalled_uncov = 0;
unsigned long poscalled_n_no_depth = 0;
unsigned long poscalled_nonref = 0;
unsigned long poscalled_reported = 0;

unsigned long alignments_read = 0;
unsigned long alignments_read_unique = 0;
unsigned long alignments_read_unpaired = 0;
unsigned long alignments_read_paired = 0;

int main ( int argc, char * argv[]) {
	// This part is the default values of all parameters
	Parameter * para = new Parameter;
	std::string alignment_name, consensus_name;
	bool is_matrix_in = false; // Generate the matrix or just read it?
	int c;
	Files files;
	while((c=getopt(argc,argv,"Ki:d:o:z:g:p:r:e:ts:2a:b:j:k:unmqM:I:L:Q:S:F:E:T:clhHv")) != -1) {
		switch(c) {
			case 'i':
			{
				// Soap Alignment Result
				files.soap_result.clear();
				files.soap_result.open(optarg);
				if( ! files.soap_result) {
					cerr<<"No such file or directory:"<<optarg<<endl;
					exit(1);
				}
				alignment_name = optarg;
				cerr << "-i is set to " << alignment_name << endl;
				break;
			}
			case 'd':
			{
				// The reference genome in fasta format
				files.ref_seq.clear();
				files.ref_seq.open(optarg);
				if( ! files.ref_seq) {
					cerr<<"No such file or directory:"<<optarg<<endl;
					exit(1);
				}
				files.ref_seq.clear();
				cerr << "-d is set to " << optarg << endl;
				break;
			}
			case 'o':
			{
				files.consensus.clear();
				files.consensus.open(optarg);
				if( ! files.consensus ) {
					cerr<<"Cannot creat file:" <<optarg <<endl;
					exit(1);
				}
				files.consensus.clear();
				consensus_name = optarg;
				cerr << "-o is set to " << consensus_name << endl;
				break;
			}
			case 'z':
			{
				// The char stands for quality==0 in fastq format
				para->q_min = optarg[0];
				if(para->q_min == 33) {
					clog<<"Standard Fastq System Set"<<endl;
				}
				else if(para->q_min == 64) {
					clog<<"Illumina Fastq System Set"<<endl;
				}
				else {
					clog<<"Other types of Fastq files?? Are you sure?"<<endl;
				}
				para->q_max = para->q_min + 40;
				break;
			}
			case 'g':
			{
				para->global_dependency= log10(atof(optarg));
				cerr << "-g is set to " << para->global_dependency << endl;
				break;
			}
			case 'p':
			{
				para->pcr_dependency= log10(atof(optarg));
				cerr << "-p is set to " << para->pcr_dependency << endl;
				break;
			}
			case 'r':
			{
				para->althom_novel_r = atof(optarg);
				cerr << "-r is set to " << para->althom_novel_r << endl;
				break;
			}
			case 'e':
			{
				para->het_novel_r=atof(optarg);
				cerr << "-e is set to " << para->het_novel_r << endl;
				break;
			}
			case 't':
			{
				cerr << "-t is set" << endl;
				para->transition_dominant=true;
				break;
			}
			case 'K':
			{
				cerr << "-K is set" << endl;
				para->dump_dbsnp_evidence=true;
				break;
			}
			case 's':
			{
				// Optional: A pre-formated dbSNP table
				cerr << "-s is set" << endl;
				files.dbsnp.clear();
				files.dbsnp.open(optarg);
				if(!files.ref_seq) {
					cerr << "No such file or directory:" << optarg << endl;
					exit(1);
				}
				files.dbsnp.clear();
				break;
			}
			case '2':
			{
				// Refine prior probability based on dbSNP information
				cerr << "-2 is set" << endl;
				para->refine_mode = true;
				break;
			}
			case 'a':
			{
				para->althom_val_r=atof(optarg);
				cerr << "-a is set to " << para->althom_val_r << endl;
				break;
			}
			case 'b':
			{
				para->het_val_r=atof(optarg);
				cerr << "-b is set to " << para->het_val_r << endl;
				break;
			}
			case 'j':
			{
				para->althom_unval_r=atof(optarg);
				cerr << "-j is set to " << para->althom_unval_r << endl;
				break;
			}
			case 'k':
			{
				para->het_unval_r=atof(optarg);
				cerr << "-k is set to " << para->het_unval_r << endl;
				break;
			}
			case 'u':
			{
				cerr << "-u is set" << endl;
				para->rank_sum_mode = true;
				break;
			}
			case 'n':
			{
				cerr << "-n is set" << endl;
				para->binom_mode = true;
				break;
			}
		 	case 'm':
			{
				cerr << "-m is set" << endl;
				para->is_monoploid=1;
				break;
			}
			case 'q':
			{
				cerr << "-q is set" << endl;
				para->is_snp_only=1;
				break;
			}
			case 'M':
			{
				files.matrix_file.close(); files.matrix_file.clear();
				// Output the calibration matrix
				files.matrix_file.open(optarg, fstream::out);
				if( ! files.matrix_file) {
					cerr<<"Cannot creat file :"<<optarg<<endl;
					exit(1);
				}
				files.matrix_file.clear();
				cerr << "-M is set to " << optarg << endl;
				break;
			}
			case 'I':
			{
				files.matrix_file.close(); files.matrix_file.clear();
				// Input the calibration matrix
				files.matrix_file.open(optarg, fstream::in);
				if( ! files.matrix_file) {
					cerr<<"No such file or directory:"<<optarg<<endl;
					exit(1);
				}
				files.matrix_file.clear();
				is_matrix_in = true;
				cerr << "-I is set to " << optarg << endl;
				break;
			}
			case 'S':
			{
				//files.summary.open(optarg);
				//// Output the summary of consensus
				//if( ! files.summary ) {
				//	cerr<<"No such file or directory: "<<optarg<<endl;
				//	exit(1);
				//}
				break;
			}
			case 'L':
			{
				para->read_length = atoi(optarg);
				cerr << "-L is set to " << (int)para->read_length << endl;
				break;
			}
			case 'Q':
			{
				para->q_max = optarg[0];
				if(para->q_max < para->q_min) {
					cerr<< "FASTQ quality character error: Q_MAX > Q_MIN" <<endl;
				}
				cerr << "-Q is set to " << para->q_max << endl;
				break;
			}
			case 'F': {
				para->glf_format = atoi(optarg);
				cerr << "-F is set to " << optarg << endl;
				break;
			}
			case 'E': {
				para->glf_header = optarg;
				cerr << "-E is set to " << optarg << endl;
				break;
			}
			case 'l': {
				cerr << "-l is set" << endl;
				para->do_recal = false;
				break;
			}
			case 'T': {
				files.region.clear();
				files.region.open(optarg);
				files.region.clear();
				para->region_only = true;
				cerr << "-T is set to " << optarg << endl;
				break;
			}
			case 'c': {
				para->format = CROSSBOW_FORMAT;
				cerr << "-c is set" << endl;
				break;
			}
			case 'v': para->verbose = true; break;
			case 'H': para->hadoop_out = true; break;
			case 'h':readme();break;
			case '?':usage();break;
			default: cerr<<"Unknown error in command line parameters"<<endl;
		}
	}
	if( !files.consensus || !files.ref_seq || !files.soap_result ) {
		// These are compulsory parameters
		usage();
	}
	//Read the chromosomes into memory
	Genome * genome = new Genome(files.ref_seq, files.dbsnp, true);
	files.ref_seq.close();
	files.dbsnp.close();
	clog<<"Reading Chromosome and dbSNP information Done."<<endl;
	if(para->region_only && files.region) {
		genome->read_region(files.region, para);
		clog<<"Read target region done."<<endl;
	}
	if(para->glf_format) { // GLF or GPF
		files.consensus.close();
		files.consensus.clear();
		files.consensus.open(consensus_name.c_str(), ios::binary);
		if(!files.consensus) {
			cerr<<"Cannot write result to the specified output file."<<endl;
			exit(255);
		}
		if (1==para->glf_format) {
			files.consensus<<'g'<<'l'<<'f';
		}
		else if (2==para->glf_format) {
			files.consensus<<'g'<<'p'<<'f';
		}
		int major_ver = 0;
		int minor_ver = 0;
		files.consensus.write(reinterpret_cast<char*>(&major_ver), sizeof(major_ver));
		files.consensus.write(reinterpret_cast<char*>(&minor_ver), sizeof(minor_ver));
		if(!files.consensus.good()) {
			cerr<<"Broken ofstream after version."<<endl;
			exit(255);
		}
		std::string temp("");
		for(std::string::iterator iter=para->glf_header.begin();iter!=para->glf_header.end(); iter++) {
			if (':'==(*iter)) {
				int type_len(temp.size()+1);
				files.consensus.write(reinterpret_cast<char*>(&type_len), sizeof(type_len));
				files.consensus.write(temp.c_str(), temp.size()+1)<<flush;
				temp = "";
			}
			else {
				temp+=(*iter);
			}
		}
		if(!files.consensus.good()) {
			cerr<<"Broken ofstream after tags."<<endl;
			exit(255);
		}
		if(temp != "") {
			int type_len(temp.size()+1);
			files.consensus.write(reinterpret_cast<char*>(&type_len), sizeof(type_len));
			files.consensus.write(temp.c_str(), temp.size()+1)<<flush;
			temp = "";
		}
		int temp_int(12);
		files.consensus.write(reinterpret_cast<char*>(&temp_int), sizeof(temp_int));
		files.consensus.write("CHROMOSOMES", 12);
		temp_int = genome->chromosomes.size();
		files.consensus.write(reinterpret_cast<char*>(&temp_int), sizeof(temp_int));
		files.consensus<<flush;
		if(!files.consensus.good()) {
			cerr<<"Broken ofstream after writting header."<<endl;
			exit(255);
		}
	}
	Prob_matrix * mat = new Prob_matrix;
	if(!is_matrix_in) {
		// Read the soap result and give the calibration matrix
		if(para->format == SOAP_FORMAT) {
			clog << "Training correction matrix in SOAP format"; logTime(); clog << endl;
			mat->matrix_gen<Soap_format>(files.soap_result, para, genome);
		} else {
			clog << "Training correction matrix in Crossbow format"; logTime(); clog << endl;
			mat->matrix_gen<Crossbow_format>(files.soap_result, para, genome);
		}
		if (files.matrix_file) {
			clog << "Writing correction matrix"; logTime(); clog << endl;
			mat->matrix_write(files.matrix_file, para);
		}
	}
	else {
		clog << "Reading correction matrix"; logTime(); clog << endl;
		mat->matrix_read(files.matrix_file, para);
	}
	files.matrix_file.close();
	clog << "Correction Matrix Done "; logTime(); clog << endl;
	mat->prior_gen(para);
	if(para->verbose) clog << "Just did prior_gen" << endl;
	mat->rank_table_gen();
	if(para->verbose) clog << "Just did rank_table_gen" << endl;
	Call_win *info = new Call_win(para->read_length, 1000);
	if(para->verbose) clog << "Just allocated Call_win" << endl;
	info->initialize(0);
	//Call the consensus
	files.soap_result.close();
	files.soap_result.clear();
	files.soap_result.open(alignment_name.c_str());
	files.soap_result.clear();
	if(para->verbose) clog << "Just reopened alignment file" << endl;
	alignments_read = 0;
	alignments_read_unique = 0;
	if(para->format == SOAP_FORMAT) {
		info->soap2cns<Soap_format>(files.soap_result, files.consensus, genome, mat, para);
	} else {
		info->soap2cns<Crossbow_format>(files.soap_result, files.consensus, genome, mat, para);
	}
	if(para->verbose) clog << "Just called soap2cns" << endl;
	files.soap_result.close();
	files.consensus.close();
	if(para->hadoop_out) {
		cerr << "reporter:counter:SOAPsnp,Alignments read," << alignments_read << endl;
		cerr << "reporter:counter:SOAPsnp,Unique alignments read," << alignments_read_unique << endl;
		cerr << "reporter:counter:SOAPsnp,Unpaired alignments read," << alignments_read_unpaired << endl;
		cerr << "reporter:counter:SOAPsnp,Paired alignments read," << alignments_read_paired << endl;
		cerr << "reporter:counter:SOAPsnp,Positions called," << (poscalled-poscalled_reported) << endl;
		cerr << "reporter:counter:SOAPsnp,Positions called with known SNP info," << poscalled_knownsnp << endl;
		cerr << "reporter:counter:SOAPsnp,Positions called uncovered by unique alignments," << poscalled_uncov_uni << endl;
		cerr << "reporter:counter:SOAPsnp,Positions called uncovered by any alignments," << poscalled_uncov << endl;
		cerr << "reporter:counter:SOAPsnp,Positions with non-reference allele called," << poscalled_nonref << endl;
	}
	if(para->verbose) {
		clog << "Alignments read: " << alignments_read << endl;
		clog << "Unique alignments read: " << alignments_read_unique << endl;
		clog << "Unpaired alignments read: " << alignments_read_unpaired << endl;
		clog << "Paired alignments read: " << alignments_read_paired << endl;
		clog << "Positions called: " << (poscalled-poscalled_reported) << endl;
		clog << "Positions called with known SNP info: " << poscalled_knownsnp << endl;
		clog << "Positions called uncovered by unique alignments: " << poscalled_uncov_uni << endl;
		clog << "Positions called uncovered by any alignments: " << poscalled_uncov << endl;
		clog << "Positions with non-reference allele called: " << poscalled_nonref << endl;
	}
	clog << "Consensus Done!"; logTime(); clog << endl;
	return 0;
}

