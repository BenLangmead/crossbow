#include "soap_snp.h"

/**
 * Insert a mapping from a chromosome name to a pointer to a chromosome
 * info structure.
 */
bool Genome::add_chr(Chr_name & name) {
	Chr_info * new_chr = new Chr_info;
	pair<map<Chr_name, Chr_info*>::iterator, bool> insert_pair;
	insert_pair=chromosomes.insert(pair<Chr_name, Chr_info*>(name,new_chr));
	return insert_pair.second;
}

Genome::~Genome(){
	for( map<Chr_name, Chr_info*>::iterator iter=chromosomes.begin(); iter!= chromosomes.end(); iter++ ){
		;
	}
}
Chr_info::Chr_info(const Chr_info & other) {
	dbsnp = other.dbsnp;
	len = other.len;
	elts = other.elts;
	if (len%capacity==0) {
		bin_seq = new ubit64_t [len/capacity];
		memcpy(bin_seq, other.bin_seq, sizeof(ubit64_t)*len/capacity);
	}
	else {
		bin_seq = new ubit64_t [1+len/capacity];
		memcpy(bin_seq, other.bin_seq, sizeof(ubit64_t)*len/capacity);
	}
	regions = other.regions;
}

int Chr_info::binarize(std::string & seq) {
	len = seq.length();
	//cerr<<len<<endl;
	// 4bit for each base
	// Allocate memory
	if (len%capacity==0) {
		elts = len/capacity;
		bin_seq = new ubit64_t [elts];
		memset(bin_seq,0,sizeof(ubit64_t)* elts);
	}
	else {
		elts = 1+len/capacity;
		bin_seq = new ubit64_t [elts];
		memset(bin_seq,0,sizeof(ubit64_t)*(elts));
	}

	// Add each base, 7 is 0b111
	for(std::string::size_type i=0;i!=seq.length();i++) {
		bin_seq[i/capacity] |= ((((ubit64_t)seq[i]>>1)&7)<<(i%capacity*4));
	}
	return 1;
}

/**
 * Dump the bin_seq sequence to a file with the given name.
 */
void Chr_info::dump_binarized(std::string fn) {
	ofstream of(fn.c_str(), ios_base::binary | ios_base::out);
	of.write((const char *)bin_seq, elts*sizeof(ubit64_t));
	of.close();
}

int Chr_info::insert_snp(std::string::size_type pos, Snp_info & snp_form, bool quiet) {
	Snp_info * new_snp = new Snp_info;
	*new_snp = snp_form;
	pair<map<ubit64_t, Snp_info*>::iterator, bool> insert_pair;
	if(dbsnp.find(pos) != dbsnp.end()) {
		if(!quiet) {
			cerr << "Warning: SNP has already been inserted at position " << pos << endl;
			cerr << "         new SNP: " << snp_form.get_name()
				 << ", old SNP: " << dbsnp.find(pos)->second->get_name() << endl;
		}
		return 0;
	}
	pair<ubit64_t, Snp_info*> p(pos,new_snp);
	insert_pair = dbsnp.insert(p);
	if(insert_pair.second) {
		// Successful insertion
		// Modify the binary sequence! Mark SNPs
		bin_seq[pos/capacity] |= (1ULL<<(pos%capacity*4+3));
	} else {
		cerr << "Warning: SNP insertion failed for SNP with name "
		     << snp_form.get_name() << " at position " << pos << endl;
		return 0;
	}
	return 1;
}

int Chr_info::set_region(int start, int end) {
	if(start<0) {
		start = 0;
	}
	else if (start >= len) {
		start = len;
	}

	if(end<0) {
		end = 0;
	}
	else if (end >= len) {
		// BTL: Modified from 'end = len' per bug report
		end = len - 1;
	}
	if (start > end) {
		cerr<<"Invalid region: "<<start<<"-"<<end<<endl;
		exit(255);
	}
	if(start/64 == end/64) {
		region_mask[start/64] |= ((~((~(0ULL))<<(end-start+1)))<<(63-end%64));
	}
	else {
		if(start % 64) {
			region_mask[start/64] |= (~((~(0ULL))<<(64-start%64)));
		}
		else {
			region_mask[start/64] = ~(0ULL);
		}
		region_mask[end/64] |= ((~(0ULL))<<(63-end%64));
		if(end/64-start/64>1) {
			memset(region_mask+start/64+1, 0xFF, sizeof(ubit64_t)*(end/64-start/64-1));
		}
	}
	regions.push_back(make_pair(start, end));
	return 1;
}

/**
 * Initialize the region mask.  Everything's 0 to begin with.
 */
int Chr_info::region_mask_ini(){
	if(len%64==0) {
		region_mask = new ubit64_t [len/64];
		memset(region_mask, 0, sizeof(ubit64_t)*(len/64));
	}
	else {
		region_mask = new ubit64_t [len/64+1];
		memset(region_mask, 0, sizeof(ubit64_t)*(len/64+1));
	}
	return 1;
}

/**
 * Read and parse a region file, specified via the -T option.
 */
int Genome::read_region(std::ifstream & region, Parameter * para) {
	Chr_name current_name(""), prev_name("");
	int start, end;
	map<Chr_name, Chr_info*>::iterator chr_iter;
	// Lines appear to be formatted as: name, start, end
	for(std::string buff; getline(region,buff); ) {
		std::istringstream s(buff);
		if(s >> current_name >> start >> end) {
			if(current_name != prev_name) {
				chr_iter = chromosomes.find(current_name);
				if(chr_iter == chromosomes.end()) {
					// Chromosome was not known
					cerr << "Unexpected Chromosome:" << current_name<<endl;
					continue;
				}
				if(NULL == chr_iter->second->get_region()) {
					chr_iter->second->region_mask_ini();
				}
			}
			chr_iter->second->set_region(start-para->read_length, end-1);
			prev_name = current_name;
		}
		else {
			cerr<<"Wrong format in target region file"<<endl;
			return 0;
		}
	}
	return 1;
}

/**
 * Read and parse a genome from a single fasta file, which is assumed
 * to be organized by chromosome.  Also read and parse the SNP file.
 */
Genome::Genome(std::ifstream &fasta, std::ifstream & known_snp, bool quiet)
{
	// As we read in the characters, we store them in seq.  We
	// eventually binarize them into the bin_seq field of the
	// respective Chr_info
	std::string seq("");
	Chr_name current_name("");
	map<Chr_name, Chr_info*>::iterator chr_iter;
	// Read the fasta file
	size_t lines = 0, chars = 0;
	for(std::string buff; getline(fasta,buff); ) {
		// Name line?
		lines++;
		if('>' == buff[0]) {
			// Fasta id
			// Deal with previous chromosome
			if(chromosomes.find(current_name) != chromosomes.end()) {
				// The previous chromosome is finished, so binarize it
				chr_iter = chromosomes.find(current_name);
				chr_iter->second->binarize(seq);
			}
			// Insert new chromosome
			std::string::size_type i;
			for(i = 1; !isspace(buff[i]) && i != buff.length(); i++) {
				;
			}
			Chr_name new_chr_name(buff, 1, i-1);
			if(!add_chr(new_chr_name)) {
				std::cerr << "Insert Chromosome " << new_chr_name << " Failed!\n";
			}
			current_name = new_chr_name;
			seq = "";
		}
		else {
			// Append line to sequence
			chars += buff.length();
			seq += buff;
		}
	}
	clog << "Read " << chars << " from " << lines << " lines of input FASTA sequence "; logTime(); clog << endl;
	if(seq.length() != 0 && chromosomes.find(current_name) != chromosomes.end()) {
		// Binarize the final chromosome
		chr_iter = chromosomes.find(current_name);
		chr_iter->second->binarize(seq);
	}
	clog << "Finished loading and binarizing chromosome "; logTime(); clog << endl;
	lines = 0;
	if(known_snp) {
		// Read in the SNP file
		Chr_name current_name;
		Snp_info snp_form;
		std::string::size_type pos;
		for(std::string buff; getline(known_snp, buff); ) {
			// Format: Chr\tPos\thapmap?\tvalidated?\tis_indel?\tA\tC\tT\tG\trsID\n
			lines++;
			std::istringstream s(buff);
			// Read chromosome name and position
			s >> current_name >> pos;
			// Snp_info has a special operator>> that reads the rest
			// of the line; see soap_snp.h
			s >> snp_form;
			if(chromosomes.find(current_name) != chromosomes.end()) {
				// The SNP is located on an valid chromosome
				pos -= 1; // Coordinates starts from 0
				// Stick the SNP in a chromosome-specific map that maps
				// positions to SNP_Infos
				(chromosomes.find(current_name)->second)->insert_snp(pos, snp_form, quiet);
			}
		}
		// Now possibly dump SNPs
	}
	clog << "Finished parsing " << lines << " known SNPs "; logTime(); clog << endl;
}
