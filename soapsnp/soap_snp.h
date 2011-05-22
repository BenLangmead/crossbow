#ifndef SOAP_SNP_HH_
#define SOAP_SNP_HH_
#include <iostream>
#include <fstream>
#include <sstream>
#include <cstring>
#include <cstdlib>
#include <map>
#include <vector>
#include <cmath>
#include <iomanip>
#include <cassert>
#include <time.h>
typedef unsigned long long ubit64_t;
typedef unsigned int ubit32_t;
typedef double rate_t;
typedef unsigned char small_int;
using namespace std;
const size_t capacity = sizeof(ubit64_t)*8/4;
const char abbv[17]={'A','M','W','R','M','C','Y','S','W','Y','T','K','R','S','K','G','N'};
const ubit64_t glf_base_code[8]={1,2,8,4,15,15,15,15}; // A C T G
const ubit64_t glf_type_code[10]={0,5,15,10,1,3,2,7,6,11};// AA,CC,GG,TT,AC,AG,AT,CG,CT,GT



// Some global variables
class Files {
public:
	ifstream soap_result, ref_seq, dbsnp, region;
	ofstream consensus, summary;
	fstream matrix_file;
	Files(){
		soap_result.close();
		ref_seq.close();
		dbsnp.close();
		consensus.close();
		summary.close();
		matrix_file.close();
		region.close();
	};
};

typedef enum {
	SOAP_FORMAT = 1,
	BOWTIE_FORMAT,
	CROSSBOW_FORMAT
} alignment_format;

class Parameter {
public:
	char q_min; // The char stands for 0 in fastq
	char q_max; // max quality score
	small_int read_length; // max read length
	bool is_monoploid; // Is it an monoploid? chrX,Y,M in man.
	bool is_snp_only;  // Only output possible SNP sites?
	bool refine_mode; // Refine prior probability using dbSNP
	bool rank_sum_mode; // Use rank sum test to refine HET quality
	bool binom_mode; // Use binomial test to refine HET quality
	bool transition_dominant; // Consider transition/transversion ratio?
	int glf_format; // Generate Output in GLF format
	bool region_only; // Only report consensus in specified region
	std::string glf_header; // Header of GLF format
	rate_t althom_novel_r, het_novel_r; // Expected novel prior
	rate_t althom_val_r, het_val_r; // Expected Validated dbSNP prior
	rate_t althom_unval_r, het_unval_r; // Expected Unvalidated dbSNP prior
	rate_t global_dependency, pcr_dependency; // Error dependencies, 1 is NO dependency
	alignment_format format;
	bool do_recal, verbose, dump_dbsnp_evidence;
	bool hadoop_out;
// Default onstruction
	Parameter(){
		q_min = 64;
		q_max = 64+40;
		read_length = 45;
		is_monoploid = is_snp_only = refine_mode = rank_sum_mode = binom_mode = transition_dominant = region_only =false;
		glf_format = 0;
		glf_header = "";
		althom_novel_r=0.0005, het_novel_r=0.0010;
		althom_val_r=0.05, het_val_r=0.10;
		althom_unval_r=0.01, het_unval_r=0.02;
		global_dependency= log10(0.9), pcr_dependency= log10(0.5); // In Log10 Scale
		format = SOAP_FORMAT;
		do_recal = true;
		verbose = false;
		hadoop_out = false;
		dump_dbsnp_evidence = false;
	};
};

extern unsigned long alignments_read;
extern unsigned long alignments_read_unique;
extern unsigned long alignments_read_unpaired;
extern unsigned long alignments_read_paired;

class Crossbow_format {
	// Crossbow alignment result
	std::string read_id, read, qual, chr_name, mms;
	int part, read_len, position, hit;
	unsigned mate;
	char strand;
public:
	Crossbow_format() { }
	friend std::istringstream & operator>>(std::istringstream & alignment, Crossbow_format & bowf) {
		alignment >> bowf.chr_name
		          >> bowf.part
		          >> bowf.position
		          >> bowf.strand
		          >> bowf.read
		          >> bowf.qual
		          >> bowf.hit
		          >> bowf.mms
		          >> bowf.mate
		          >> bowf.read_id;
		bowf.read_len = bowf.read.length(); // infer
		bowf.hit++;
		alignments_read++;
		if(bowf.hit == 1)  alignments_read_unique++;
		if(bowf.mate == 0) alignments_read_unpaired++;
		if(bowf.mate > 0)  alignments_read_paired++;
		return alignment;
	}
	friend std::ostream & operator<<(std::ostream & o, Crossbow_format & bowf) {
		o << bowf.read_id << '\t'
		  << bowf.read << '\t'
		  << bowf.qual << '\t'
		  << bowf.hit << '\t'
		  << (bowf.mate < 3 ? "aab"[bowf.mate] : '?') << '\t'
		  << bowf.read_len << '\t'
		  << bowf.strand << '\t'
		  << bowf.chr_name << '\t'
		  << bowf.position << '\t'
		  << "0";
		return o;
	}
	char get_base(std::string::size_type coord) {
		return read[coord];
	}
	char get_qual(std::string::size_type coord) {
		return qual[coord];
	}
	bool is_fwd() {
		return (strand=='+');
	}
	int get_read_len() {
		return read_len;
	}
	inline int get_pos() {
		return position;
	}
	std::string get_chr_name() {
		return chr_name;
	}
	int get_hit() {
		return hit;
	}
	bool is_unique() {
		return (hit==1);
	}
	bool is_N(int coord) {
		return (read[coord] == 'N');
	}
	unsigned get_mate() const { return mate; }
};

/**
 * Note that SOAPsnp does not read reference information from the
 * alignment; it gets all reference information from the Genome
 * structure.
 */
class Soap_format {
	// Soap alignment result
	std::string read_id, read, qual, chr_name;
	int hit, read_len, position, mismatch;
	char ab, strand;
	unsigned mate;
	// 'ab' is not used in consensus/SNP calling, just for printing out
	// the alignment
public:
	Soap_format(){;};
	friend std::istringstream & operator>>(std::istringstream & alignment, Soap_format & soap) {
		alignment >> soap.read_id
		          >> soap.read
		          >> soap.qual
		          >> soap.hit       // # alignments w/ same # mms
		          >> soap.ab        // whether it's mate a/b
		          >> soap.read_len
		          >> soap.strand
		          >> soap.chr_name
		          >> soap.position
		          >> soap.mismatch; // mismatch string
		if(soap.mismatch > 200) {
			// Refine the read so that the read contains an insertion
			// w/r/t reference
			int indel_pos,indel_len;
			string temp("");
			alignment >> indel_pos;
			indel_len = soap.mismatch-200;
			for(int i = 0; i != indel_len; i++) {
				temp = temp+'N';
			}
			soap.read = soap.read.substr(0,indel_pos)+temp+soap.read.substr(indel_pos,soap.read_len-indel_pos);
			soap.qual = soap.qual.substr(0,indel_pos)+temp+soap.qual.substr(indel_pos,soap.read_len-indel_pos);
		}
		else if (soap.mismatch > 100) {
			// Refine the read so that the read contains an deletion
			// w/r/t reference
			int indel_pos,indel_len;
			alignment >> indel_pos;
			indel_len = soap.mismatch-100;
			soap.read = soap.read.substr(0,indel_pos) + soap.read.substr(indel_pos+indel_len, soap.read_len-indel_pos-indel_len);
			soap.qual = soap.qual.substr(0,indel_pos) + soap.qual.substr(indel_pos+indel_len, soap.read_len-indel_pos-indel_len);
		}
		soap.position -= 1;
		soap.mate = 0;
		return alignment;
	}
	friend std::ostream & operator<<(std::ostream & o, Soap_format & soap) {
		o<<soap.read_id<<'\t'<<soap.read<<'\t'<<soap.qual<<'\t'<<soap.hit<<'\t'<<soap.ab<<'\t'<<soap.read_len<<'\t'<<soap.strand<<'\t'<<soap.chr_name<<'\t'<<soap.position<<'\t'<<soap.mismatch;
		return o;
	}
	char get_base(std::string::size_type coord) {
		return read[coord];
	}
	char get_qual(std::string::size_type coord) {
		return qual[coord];
	}
	bool is_fwd(){
		return (strand=='+');
	}
	int get_read_len(){
		return read_len;
	}
	inline int get_pos(){
		return position;
	}
	std::string get_chr_name(){
		return chr_name;
	}
	int get_hit(){
		return hit;
	}
	bool is_unique(){
		return (hit==1);
	}
	bool is_N(int coord) {
		return (read[coord] == 'N');
	}
	unsigned get_mate() const { return mate; }
};

// dbSNP information
class Snp_info {
	bool validated;
	bool hapmap_site;
	bool indel_site;
	rate_t * freq; // elements record frequency of ACTG
	string name;
public:
	Snp_info(){
		validated=hapmap_site=indel_site=false;
		freq = new rate_t [4];
		memset(freq,0,sizeof(rate_t)*4);
	}
	Snp_info(const Snp_info & other) {
		validated = other.validated;
		hapmap_site = other.hapmap_site;
		indel_site = other.indel_site;
		freq = new rate_t [4];
		memcpy(freq, other.freq, sizeof(rate_t)*4);
	}
	~Snp_info(){
		delete [] freq;
	}
	/**
	 * Here's where the SNP format is defined (beyond the first two
	 * fields, which hold the chromosome name and offset).
	 */
	friend std::istringstream& operator>>(std::istringstream & s,
	                                      Snp_info & snp_form)
	{
		s >> snp_form.hapmap_site
		  >> snp_form.validated
		  >> snp_form.indel_site
		  >> snp_form.freq[0]  // A
		  >> snp_form.freq[1]  // C
		  >> snp_form.freq[2]  // T
		  >> snp_form.freq[3]  // G
		  >> snp_form.name;
		return s;
	}
	Snp_info & operator=(Snp_info& other) {
		this->validated = other.validated;
		this->hapmap_site = other.hapmap_site;
		this->indel_site = other.indel_site;
		this->name = other.name;
		this->freq = new rate_t [4];
		memcpy(this->freq, other.freq, sizeof(rate_t)*4);
		return *this;

	}
	bool is_validated(){
		return validated;
	}
	bool is_hapmap(){
		return hapmap_site;
	}
	bool is_indel(){
		return indel_site;
	}
	rate_t get_freq(char bin_base_2bit) {
		return freq[bin_base_2bit];
	}
	const string& get_name() {
		return name;
	}
};

// Chromosome(Reference) information
class Chr_info {
	ubit32_t len;
	ubit32_t elts;
	ubit64_t* bin_seq; // Sequence in binary format
	bool bin_seq_is_mm; // bin_seq array is memory-mapped?
	// region_mask is initilized lazily, only of the user specifies -T.
	// The Parameter.region_only flag will be set iff region_mask is
	// initialized.
	ubit64_t* region_mask;
	// 4bits for one base: 1 bit dbSNPstatus, 1bit for N, followed two bit of base A: 00, C: 01, T: 10, G:11,
	// Every ubit64_t could store 16 bases
	map<ubit64_t, Snp_info*> dbsnp;
	vector<pair<int, int> > regions;
public:
	Chr_info(){
		bin_seq_is_mm = false;
		len = 0;
		elts = 0;
		bin_seq = NULL;
		region_mask = NULL;
		regions.clear();
	};
	Chr_info(const Chr_info & other);
	~Chr_info(){
		if(!bin_seq_is_mm) {
			delete [] bin_seq;
		}
		delete [] region_mask;
	}
	ubit32_t length() {
		return len;
	}
	ubit64_t get_bin_base(std::string::size_type pos) {
		return (bin_seq[pos/capacity]>>(pos%capacity*4))&0xF; // All 4 bits
	}
	int binarize(std::string & seq);
	void dump_binarized(std::string fn);
	int insert_snp(std::string::size_type pos, Snp_info & new_snp, bool quiet);
	int region_mask_ini();
	bool is_in_region(std::string::size_type pos) {
		if(region_mask == NULL) return true;
		return (region_mask[pos/64]>>(63-pos%64))&1;
	}
	int set_region(int start, int end);
	/**
	 * The only place this is called is in Call_win::call_cns when it
	 * passes the result to snp_p_prior_gen in order to generate a
	 * prior probability for each diploid genotype.
	 */
	Snp_info * find_snp(ubit64_t pos) {
		return dbsnp.find(pos)->second;
	}
	ubit64_t * get_region() {
		return region_mask;
	}
	ubit64_t * get_bin_seq() {
		return bin_seq;
	}
	ubit32_t get_elts() {
		return elts;
	}
	const std::vector<pair<int, int> >& get_regions() {
		return regions;
	}
};

typedef std::string Chr_name;

class Genome {
public:
	map<Chr_name, Chr_info*> chromosomes;

	Genome(ifstream & fasta, ifstream & known_snp, bool quiet);
	~Genome();

	/// Add a new chromosome to the map
	bool add_chr(Chr_name &);

	/// Read in and parse a region file
	int read_region(std::ifstream & region, Parameter * para);
};

class Prob_matrix {
public:
	rate_t *p_matrix, *p_prior; // Calibration matrix and prior probabilities
	rate_t *base_freq, *type_likely, *type_prob; // Estimate base frequency, conditional probability, and posterior probablity
	rate_t *p_rank, *p_binom; // Ranksum test and binomial test on HETs
	Prob_matrix();
	~Prob_matrix();
	template<typename T> int matrix_gen(std::ifstream & alignment, Parameter * para, Genome * genome);
	int matrix_read(std::fstream & mat_in, Parameter * para);
	int matrix_write(std::fstream & mat_out, Parameter * para);
	int prior_gen(Parameter * para);
	int rank_table_gen();

};

template<typename T>
int Prob_matrix::matrix_gen(std::ifstream & alignment, Parameter * para, Genome * genome) {
	// Read Alignment files
	T soap;
	ubit64_t * count_matrix = new ubit64_t [256*256*4*4];
	memset(count_matrix, 0, sizeof(ubit64_t)*256*256*4*4);
	map<Chr_name, Chr_info*>::iterator current_chr;
	current_chr = genome->chromosomes.end();
	ubit64_t ref(0);
	std::string::size_type coord;
	if(para->do_recal) {
		// For each alignment
		for(std::string line; getline(alignment, line);) {
			std::istringstream s(line);
			// Parse the alignment
			if(s >> soap) {
				if(soap.get_pos() < 0) {
					continue;
				}
				// In the overloaded "+" above, soap.position will be substracted by 1 so that coordiates start from 0
				if (current_chr == genome->chromosomes.end() || current_chr->first != soap.get_chr_name()) {
					current_chr = genome->chromosomes.find(soap.get_chr_name());
					if(current_chr == genome->chromosomes.end()) {
						for(map<Chr_name, Chr_info*>::iterator test = genome->chromosomes.begin();test != genome->chromosomes.end();test++) {
							cerr<<'!'<<(test->first)<<'!'<<endl;
						}
						cerr<<"Assertion Failed: Chromosome: !"<<soap.get_chr_name()<<"! NOT found"<<endl;
						exit(255);
					}
				}
				else {
					;
				}
				if (soap.is_unique()) {
					for(coord = 0; coord != soap.get_read_len(); coord++) {
						if (soap.is_N(coord)) {
							;
						}
						else {
							if(! (soap.get_pos()+coord<current_chr->second->length())) {
								cerr<<soap<<endl;
								cerr<<"The program found the above read has exceed the reference length:\n";
								cerr<<"The read is aligned to postion: "<<soap.get_pos()<<" with read length: "<<soap.get_read_len()<<endl;
								cerr<<"Reference: "<<current_chr->first<<" FASTA Length: "<<current_chr->second->length()<<endl;
								exit(255);
							}
							ref = current_chr->second->get_bin_base(soap.get_pos()+coord);
							if ( (ref&12) !=0 ) {
								// This is an N on reference or a dbSNP which should be excluded from calibration
								;
							}
							else {
								if(soap.is_fwd()) {
									// forward strand
									count_matrix[(((ubit64_t)soap.get_qual(coord))<<12) | (coord<<4) | ((ref&0x3)<<2) | (soap.get_base(coord)>>1)&3] += 1;
								}
								else {
									// reverse strand
									count_matrix[(((ubit64_t)soap.get_qual(coord))<<12) | ((soap.get_read_len()-1-coord)<<4) | ((ref&0x3)<<2) | (soap.get_base(coord)>>1)&3] += 1;
								}
							}
						}
					}
				}
			}
		}
	}
	ubit64_t o_base/*o_based base*/, t_base/*theorecical(supposed) base*/, type, sum[4], same_qual_count_by_type[16], same_qual_count_by_t_base[4], same_qual_count_total, same_qual_count_mismatch;
	char q_char/*fastq quality char*/;

	const ubit64_t sta_pow=10; // minimum number to say statistically powerful
	for(q_char=para->q_min; q_char<=para->q_max ;q_char++) {
		memset(same_qual_count_by_type, 0, sizeof(ubit64_t)*16);
		memset(same_qual_count_by_t_base, 0, sizeof(ubit64_t)*4);
		same_qual_count_total = 0;
		same_qual_count_mismatch = 0;
		for(coord=0; coord != para->read_length ; coord++) {
			for(type=0;type!=16;type++) {
				// If the sample is small, then we will not consider the effect of read cycle.
				same_qual_count_by_type[type] += count_matrix[ ((ubit64_t)q_char<<12) | coord <<4 | type];
				same_qual_count_by_t_base[(type>>2)&3] += count_matrix[ ((ubit64_t)q_char<<12) | coord <<4 | type];
				same_qual_count_total += count_matrix[ ((ubit64_t)q_char<<12) | coord <<4 | type];
				if(type % 5 != 0) {
					// Mismatches
					same_qual_count_mismatch += count_matrix[ ((ubit64_t)q_char<<12) | coord <<4 | type];
				}
			}
		}
		for(coord=0; coord != para->read_length ; coord++) {
			memset(sum, (ubit64_t)0, sizeof(ubit64_t)*4);
			// Count of all ref base at certain coord and quality
			for(type=0;type!=16;type++) {
				sum[(type>>2)&3] += count_matrix[ ((ubit64_t)q_char<<12) | (coord <<4) | type]; // (type>>2)&3: the ref base
			}
			for(t_base=0; t_base!=4; t_base++) {
				for(o_base=0; o_base!=4; o_base++) {
					if (count_matrix[ ((ubit64_t)q_char<<12) | (coord <<4) | (t_base<<2) | o_base] > sta_pow) {
						// Statistically powerful
						p_matrix [ ((ubit64_t)(q_char-para->q_min)<<12) | (coord <<4) | (t_base<<2) | o_base] = ((double)count_matrix[ ((ubit64_t)q_char<<12) | (coord <<4) | (t_base<<2) | o_base]) / sum[t_base];
					}
					else if (same_qual_count_by_type[t_base<<2|o_base] > sta_pow) {
						// Smaller sample, given up effect from read cycle
						p_matrix [ ((ubit64_t)(q_char-para->q_min)<<12) | (coord <<4) | (t_base<<2) | o_base] =  ((double)same_qual_count_by_type[t_base<<2|o_base]) / same_qual_count_by_t_base[t_base];
					}
					else if (same_qual_count_total > 0){
						// Too small sample, given up effect of mismatch types
						if (o_base == t_base) {
							p_matrix [ ((ubit64_t)(q_char-para->q_min)<<12) | (coord <<4) | (t_base<<2) | o_base] = ((double)(same_qual_count_total-same_qual_count_mismatch))/same_qual_count_total;
						}
						else {
							p_matrix [ ((ubit64_t)(q_char-para->q_min)<<12) | (coord <<4) | (t_base<<2) | o_base] = ((double)same_qual_count_mismatch)/same_qual_count_total;
						}
					}

					// For these cases like:
					// Ref: G o_base: G x10 Ax5. When calculate the probability of this allele to be A,
					// If there's no A in reference gives observation of G, then the probability will be zero,
					// And therefore exclude the possibility of this pos to have an A
					// These cases should be avoid when the dataset is large enough
					// If no base with certain quality is o_based, it also doesn't matter
					if( (p_matrix [ ((ubit64_t)(q_char-para->q_min)<<12) | (coord <<4) | (t_base<<2) | o_base]==0) || p_matrix [ ((ubit64_t)(q_char-para->q_min)<<12) | (coord <<4) | (t_base<<2) | o_base] ==1) {
						if (o_base == t_base) {
							p_matrix [ ((ubit64_t)(q_char-para->q_min)<<12) | (coord <<4) | (t_base<<2) | o_base] = (1-pow(10, -((q_char-para->q_min)/10.0)));
							if(p_matrix [ ((ubit64_t)(q_char-para->q_min)<<12) | (coord <<4) | (t_base<<2) | o_base]<0.25) {
								p_matrix [ ((ubit64_t)(q_char-para->q_min)<<12) | (coord <<4) | (t_base<<2) | o_base] = 0.25;
							}
						}
						else {
							p_matrix [ ((ubit64_t)(q_char-para->q_min)<<12) | (coord <<4) | (t_base<<2) | o_base] = (pow(10, -((q_char-para->q_min)/10.0))/3);
							if(p_matrix [ ((ubit64_t)(q_char-para->q_min)<<12) | (coord <<4) | (t_base<<2) | o_base]>0.25) {
								p_matrix [ ((ubit64_t)(q_char-para->q_min)<<12) | (coord <<4) | (t_base<<2) | o_base] = 0.25;
							}
						}
					}
				}
			}
		}
	}
	delete [] count_matrix;

	// Note: from now on, the first 8 bit of p_matrix is its quality score, not the FASTQ char
	return 1;
}

struct Pos_info {
	unsigned char ori;
	small_int base_info[4*2*64*256];
#ifdef FAST_BOUNDS
	small_int coordmin, coordmax;
	char qmin, qmax;
#endif
	int pos, depth, dep_uni, repeat_time;
	int dep_pair, dep_uni_pair;
	int count_uni[4];
	int q_sum[4];
	int count_all[4];

	Pos_info(){
		ori = 0xFF;
		memset(base_info,0,sizeof(small_int)*4*2*64*256);
		pos = -1;
		memset(count_uni,0,sizeof(int)*4);
		memset(q_sum,0,sizeof(int)*4);
		depth = 0;
		dep_uni = 0;
		dep_pair = 0;
		dep_uni_pair = 0;
		repeat_time = 0;
#ifdef FAST_BOUNDS
		coordmin = coordmax = 0;
		qmin = qmax = 0;
#endif
		memset(count_all,0,sizeof(int)*4);
	}

	static void clear(Pos_info* p, int num) {
		memset((void*)p, 0, num * sizeof(Pos_info));
	}
};

class Call_win {
public:
	ubit64_t win_size;
	ubit64_t read_len;
	Pos_info * sites; // a single Pos_info is 50 bytes or so
	Call_win(ubit64_t read_length, ubit64_t window_size=1000) {
		sites = new Pos_info [window_size+read_length];
		win_size = window_size;
		read_len = read_length;
	}
	~Call_win(){
		delete [] sites;
	}

	int initialize(ubit64_t start);
	int recycle(int start = -1);
	int call_cns(Chr_name call_name, Chr_info* call_chr, ubit64_t call_length, Prob_matrix * mat, Parameter * para, std::ofstream & consensus);
	template<typename T> int soap2cns(std::ifstream & alignment, std::ofstream & consensus, Genome * genome, Prob_matrix * mat, Parameter * para);
	int snp_p_prior_gen(double * real_p_prior, Snp_info* snp, Parameter * para, char ref);
	double rank_test(Pos_info & info, char best_type, rate_t * p_rank, Parameter * para);
	double normal_value(double z);
	double normal_test(int n1, int n2, double T1, double T2);
	double table_test(rate_t *p_rank, int n1, int n2, double T1, double T2);
};

/**
 * Loop over SNP-calling windows.
 */
template<typename T>
int Call_win::soap2cns(std::ifstream & alignment, std::ofstream & consensus, Genome * genome, Prob_matrix * mat, Parameter * para) {
	T soap;
	map<Chr_name, Chr_info*>::iterator current_chr, prev_chr;
	current_chr = prev_chr = genome->chromosomes.end();
	int coord, sub;
	int last_start(0);
	int aln = 0;
	for(std::string line; getline(alignment, line);) {
		std::istringstream s(line);
		if(s >> soap) {
			aln++;
			if(para->verbose) {
				clog << "Processing alignment " << aln << endl;
			}
			if(soap.get_pos() < 0) {
				continue;
			}
			if (current_chr == genome->chromosomes.end() ||
			    current_chr->first != soap.get_chr_name())
			{
				// Moved on to a new Chromosome
				if(current_chr != genome->chromosomes.end()) {
					// This it not the first chromosome, so we ha
					while(current_chr->second->length() > sites[win_size-1].pos) {
						call_cns(current_chr->first, current_chr->second, win_size, mat, para, consensus);
						recycle();
						last_start = sites[win_size-1].pos;
					}
					call_cns(current_chr->first, current_chr->second, current_chr->second->length()%win_size, mat, para, consensus);
					recycle();
				}
				// Get the chromosome info corresponding to the next
				// chunk of alignments
				current_chr = genome->chromosomes.find(soap.get_chr_name());
				initialize(0);
				if(para->verbose) {
					clog << "Returned from initialize(0) for chromosome " << current_chr->first << endl;
				}
				last_start = 0;
				if(para->glf_format) {
					cerr << "Processing " << current_chr->first << endl;
					int temp_int(current_chr->first.size()+1);
					consensus.write(reinterpret_cast<char *> (&temp_int), sizeof(temp_int));
					consensus.write(current_chr->first.c_str(), current_chr->first.size()+1);
					temp_int = current_chr->second->length();
					consensus.write(reinterpret_cast<char *> (&temp_int), sizeof(temp_int));
					consensus<<flush;
					if (!consensus.good()) {
						cerr<<"Broken IO stream after writing chromosome info."<<endl;
						exit(255);
					}
					assert(consensus.good());
				}
			}
			else {
				;
			}
			Chr_info *chr = current_chr->second;
			if(para->region_only && !chr->is_in_region(soap.get_pos())) {
				continue;
			}
			if(soap.get_pos() < last_start) {
				cerr << "Errors in sorting:" << soap.get_pos() << "<" << last_start << endl;
				exit(255);
			}
			// Call the previous window
			int aln_win = soap.get_pos() / win_size;
			int last_aln_win = last_start / win_size;
			if (aln_win > last_aln_win) {
				// We should call the base here
				call_cns(current_chr->first, current_chr->second,
				         win_size, mat, para, consensus);
				if(aln_win > last_aln_win+1) {
					recycle(aln_win * win_size);
				} else {
					recycle();
				}
				last_start = sites[win_size-1].pos;
				if((last_start + 1) / win_size == 1000) {
					cerr << "Called " << last_start;
				}
			}
			last_start = soap.get_pos();
			// Commit the read information
			for(coord = 0; coord < soap.get_read_len(); coord++) {
				const int pos = soap.get_pos() + coord;
				if(!chr->is_in_region(pos)) {
					continue;
				}
				if(pos / win_size == soap.get_pos() / win_size ) {
					// In the same sliding window
					sub = pos % win_size;
				}
				else {
					sub = pos % win_size + win_size; // Use the tail to store the info so that it won't intervene the uncalled bases
				}
				sites[sub].depth += 1;
				if(soap.get_mate() > 0) sites[sub].dep_pair += 1;
				sites[sub].repeat_time += soap.get_hit();
				if((soap.is_N(coord)) ||
				   soap.get_qual(coord) < para->q_min ||
				   sites[sub].dep_uni >= 0xFF)
				{
					// An N, low quality or meaningless huge depth
					continue;
				}
				if(soap.get_hit() == 1) {
					sites[sub].dep_uni += 1;
					if(soap.get_mate() > 0) sites[sub].dep_uni_pair += 1;
					int rcoord = coord;
					// Update the covering info: 4x2x64x64 matrix, base x strand x q_score x read_pos, 2-1-6-6 bits for each
					if(soap.is_fwd()) {
						// Binary strand: 0 for plus and 1 for minus
						sites[sub].base_info[(((ubit64_t)(soap.get_base(coord)&0x6)|0))<<14 | ((ubit64_t)(soap.get_qual(coord)-para->q_min))<<8 | coord ] += 1;
					} else {
						rcoord = (soap.get_read_len()-1-coord);
						sites[sub].base_info[(((ubit64_t)(soap.get_base(coord)&0x6)|1))<<14 | ((ubit64_t)(soap.get_qual(coord)-para->q_min))<<8 | rcoord ] += 1;
					}
#ifdef FAST_BOUNDS
					char qu = soap.get_qual(coord) - para->q_min;
					if(qu+1 > sites[sub].qmax || sites[sub].qmax == 0) sites[sub].qmax = qu+1;
					if(qu+1 < sites[sub].qmin || sites[sub].qmin == 0) sites[sub].qmin = qu+1;
					if(rcoord+1 > sites[sub].coordmax || sites[sub].coordmax == 0) sites[sub].coordmax = rcoord+1;
					if(rcoord+1 < sites[sub].coordmin || sites[sub].coordmin == 0) sites[sub].coordmin = rcoord+1;
#endif
					// Update # of unique alignments having the given
					// unambiguous base
					sites[sub].count_uni[(soap.get_base(coord)>>1)&3] += 1;
					// Update sum-of-Phreds
					sites[sub].q_sum[(soap.get_base(coord)>>1)&3] += (soap.get_qual(coord)-para->q_min);
				}
				// Update # of alignments having the given unambiguous base
				sites[sub].count_all[(soap.get_base(coord)>>1)&3] += 1;
			}
		}
	} // end loop over alignments
	if(aln == 0) {
		cerr << "Error: did not read any alignments" << endl;
		exit(1);
	}
	while(current_chr->second->length() > sites[win_size-1].pos) {
		int ret = call_cns(current_chr->first, current_chr->second,
		                   win_size, mat, para, consensus);
		recycle();
		last_start = sites[win_size-1].pos;
		if(ret == -2) break;
	}
	call_cns(current_chr->first, current_chr->second,
	         current_chr->second->length() % win_size,
	         mat, para, consensus);
	alignment.close();
	consensus.close();
	return 1;
}

static inline void logTime() {
	struct tm *current;
	time_t now;
	time(&now);
	current = localtime(&now);
	clog << setfill('0') << setw(2)
	     << current->tm_hour << ":"
	     << setfill('0') << setw(2)
	     << current->tm_min << ":"
	     << setfill('0') << setw(2)
	     << current->tm_sec;
}

#endif /*SOAP_SNP_HH_*/
