#include "soap_snp.h"

int Call_win::initialize(ubit64_t start) {
	std::string::size_type i;
	for(i = 0; i != read_len + win_size; i++) {
		sites[i].pos = i + start;
	}
	return 1;
}

int Call_win::recycle(int start) {
	std::string::size_type i;
	// Move the
	if(sites[win_size].depth > 0 && start == -1) {
		for(i = 0; i != read_len ; i++) {
			sites[i].pos         = sites[i+win_size].pos;
			sites[i].ori         = sites[i+win_size].ori;
			sites[i].depth       = sites[i+win_size].depth;
			sites[i].repeat_time = sites[i+win_size].repeat_time;
			sites[i].dep_uni     = sites[i+win_size].dep_uni;
			sites[i].dep_pair    = sites[i+win_size].dep_uni;
			sites[i].dep_uni_pair= sites[i+win_size].dep_uni;
#ifdef FAST_BOUNDS
			sites[i].coordmin    = sites[i+win_size].coordmin;
			sites[i].coordmax    = sites[i+win_size].coordmax;
			sites[i].qmin        = sites[i+win_size].qmin;
			sites[i].qmax        = sites[i+win_size].qmax;
#endif
			memcpy(sites[i].base_info, sites[i+win_size].base_info, sizeof(small_int)*4*2*64*256); // 4 types of bases, 2 strands, max quality score is 64, and max read length 256
			memcpy(sites[i].count_uni, sites[i+win_size].count_uni, sizeof(int)*4);
			memcpy(sites[i].q_sum,     sites[i+win_size].q_sum,     sizeof(int)*4);
			memcpy(sites[i].count_all, sites[i+win_size].count_all, sizeof(int)*4);
		}
	} else {
		Pos_info::clear(&sites[0], read_len);
		if(start == -1) {
			for(i = 0; i != read_len ; i++) {
				sites[i].ori = 0xFF;
				sites[i].pos = sites[i+win_size].pos;
			}
		} else {
			for(i = 0; i != read_len ; i++) {
				sites[i].ori = 0xFF;
				sites[i].pos = start + i;
			}
		}
	}
	// Fill in a window's worth of 0s
	Pos_info::clear(&sites[read_len], win_size);
	for(i = read_len; i != read_len + win_size; i++) {
		sites[i].ori = 0xFF;
		sites[i].pos = sites[i-1].pos+1;
	}
	return 1;
}

extern unsigned long poscalled;            // positions called
extern unsigned long poscalled_knownsnp;   // ... where there was a known SNP
extern unsigned long poscalled_uncov_uni;  // ... uncovered by unique reads
extern unsigned long poscalled_uncov;      // ... uncovered by any reads
extern unsigned long poscalled_n_no_depth; // ... where ref=N and there's no reads
extern unsigned long poscalled_nonref;     // ... where allele other than ref was called
extern unsigned long poscalled_reported;   // ... # positions called already counted

static unsigned long report_every = 100000;

int Call_win::call_cns(Chr_name call_name,
                       Chr_info* call_chr,
                       ubit64_t call_length,
                       Prob_matrix * mat,
                       Parameter * para,
                       std::ofstream & consensus)
{
	std::string::size_type coord;
	small_int k;
	ubit64_t o_base, strand;
	char allele1, allele2, genotype, type, type1/*best genotype*/, type2/*suboptimal genotype*/, base1, base2, base3;
	int i, q_score, q_adjusted, qual1, qual2, qual3, q_cns, all_count1, all_count2, all_count3;
	int global_dep_count, *pcr_dep_count;
	pcr_dep_count = new int [para->read_length*2];
	double  rank_sum_test_value, binomial_test_value;
	bool is_out;
	double * real_p_prior = new double [16];

	if(para->verbose) {
		clog << "  call_cns called with chr " << call_name
		     << ", first pos: " << sites[0].pos
		     << ", call length:" << call_length
		     << ", is SNP only: " << para->is_snp_only
		     << ", is region only: " << para->region_only
		     << ", get_regions().size(): " << call_chr->get_regions().size()
		     << ", <" << call_chr->get_regions()[0].first
		     << ", " << call_chr->get_regions()[0].second << ">" << endl;
	}

	// Special case: the user selected just one region in SNP-only
	// mode; skip this window if it doesn't overlap that region
	if(para->is_snp_only &&
	   para->region_only &&
	   call_chr->get_regions().size() == 1)
	{
		if(call_chr->get_regions()[0].first >= sites[0].pos + call_length) {
			// Skip this window - too early
			if(para->verbose) {
				clog << "  Skipping " << sites[0].pos << " because it's too early" << endl;
			}
			return -1;
		}
		if(call_chr->get_regions()[0].second <= sites[0].pos) {
			// Skip this window - too late
			if(para->verbose) {
				clog << "  Skipping " << sites[0].pos << " because it's too late" << endl;
			}
			return -2;
		}
	}
	// Iterate over every reference position that we'd like to call
	for(std::string::size_type j = 0; j != call_length; j++) {
		if(para->region_only && !call_chr->is_in_region(sites[j].pos)) {
			// Skip region that user asked us to skip using -T
			continue;
		}
		if((++poscalled % report_every) == 0) {
			poscalled_reported += report_every;
			if(para->verbose) {
				clog << "  Processed " << poscalled << " positions" << endl;
			}
			if(para->hadoop_out) {
				cerr << "reporter:counter:SOAPsnp,Positions called," << report_every << endl;
			}
		}
		// Get "original" reference base
		sites[j].ori = (call_chr->get_bin_base(sites[j].pos))&0xF;
		// Check whether this is a known SNP that we should dump the
		// consensus for even if -q is specified
		bool known_snp = (((sites[j].ori & 0x8) != 0) && para->dump_dbsnp_evidence);
		if((sites[j].ori & 0x8) != 0) poscalled_knownsnp++;

		// Check whether we can skip this reference position entirely
		// because (a) we're only interested in SNPs, and (b) the
		// position is not covered by any evidence that we can use to
		// call SNPs.
		if(sites[j].dep_uni == 0) poscalled_uncov_uni++;
		if(sites[j].depth == 0) poscalled_uncov++;
		if(sites[j].dep_uni == 0 && para->is_snp_only) {
			assert(sites[j].count_uni[0] == 0);
			assert(sites[j].count_uni[1] == 0);
			assert(sites[j].count_uni[2] == 0);
			assert(sites[j].count_uni[3] == 0);
			if(known_snp) {
				// This is a known-SNP site that is not covered by any
				// alignments; if the user asked us to dump all dbSNP
				// evidence, then just print a brief record indicating
				// there was no coverage at the site.
				consensus << "K"
				          << '\t' << call_name // chromosome name
				          << '\t' << (sites[j].pos+1)
				          << '\t' << ("ACTGNNNN"[(sites[j].ori & 0x7)]) // ref allele
				          << '\t' << "no-coverage"
				          << endl;
			}
			continue;
		}
		// N on the reference, no "depth"
		bool n_no_dep = ((sites[j].ori & 4) != 0)/*an N*/ && sites[j].depth == 0;
		if(n_no_dep) poscalled_n_no_depth++;
		if(!para->is_snp_only && n_no_dep) {
			// CNS text format:
			// ChrID\tPos\tRef\tCns\tQual\tBase1\tAvgQ1\tCountUni1\tCountAll1\tBase2\tAvgQ2\tCountUni2\tCountAll2\tDepth\tRank_sum\tCopyNum\tSNPstauts\n"
			if(!para->glf_format) {
				consensus << call_name
				          << '\t'
				          << (sites[j].pos+1)
				          << "\tN\tN\t0\tN\t0\t0\t0\tN\t0\t0\t0\t0\t1.000\t255.000\t0"
				          << endl;
			}
			else if (para->glf_format) {
				consensus << (unsigned char)(0xF<<4|0) << (unsigned char)(0<<4|0xF)<<flush;
				for(type=0;type!=10;type++) {
					consensus<<(unsigned char)0;
				}
				consensus<<flush;
				if(!consensus.good()) {
					cerr<<"Broken ofstream after writting Position "<<(sites[j].pos+1)<<" at "<<call_name<<endl;
					exit(255);
				}
			}
			continue;
		}
		base1 = 0, base2 = 0, base3 = 0;
		qual1 = -1, qual2 = -2, qual3 = -3;
		all_count1 = 0, all_count2 = 0, all_count3 = 0;
		// .dep_uni = Depth of unique bases?
		if(sites[j].dep_uni) {
			// This position is uniquely covered by at least one
			// nucleotide.  BTL: This loop seems to collect the most
			// frequent three bases according to sum-of-Phred-calls
			// for that base.  sites[].q_sum is already calculated
			for(i = 0; i != 4; i++) {
				// i is four kind of alleles
				if(sites[j].q_sum[i] >= qual1) {
					base3 = base2;
					qual3 = qual2;
					base2 = base1;
					qual2 = qual1;
					base1 = i;
					qual1 = sites[j].q_sum[i];
				}
				else if (sites[j].q_sum[i] >= qual2) {
					base3 = base2;
					qual3 = qual2;
					base2 = i;
					qual2  = sites[j].q_sum[i];
				}
				else if (sites[j].q_sum[i] >= qual3) {
					base3 = i;
					qual3  = sites[j].q_sum[i];
				}
				else {
					;
				}
			}
			if(qual1 == 0) {
				// Adjust the best base so that things won't look ugly
				// if the pos is not covered
				base1 = (sites[j].ori & 7);
			}
			else if(qual2 ==0 && base1 != (sites[j].ori & 7)) {
				base2 = (sites[j].ori & 7);
			}
			else {
				;
			}
		} // if(sites[j].dep_uni)
		else {
			// This position is covered by all repeats
			for(i = 0; i != 4; i++) {
				if(sites[j].count_all[i] >= all_count1) {
					base3 = base2;
					all_count3 = all_count2;
					base2 = base1;
					all_count2 = all_count1;
					base1 = i;
					all_count1 = sites[j].count_all[i];
				}
				else if (sites[j].count_all[i] >= all_count2) {
					base3 = base2;
					all_count3 = all_count2;
					base2 = i;
					all_count2  = sites[j].count_all[i];
				}
				else if (sites[j].count_all[i] >= all_count3) {
					base3 = i;
					all_count3  = sites[j].count_all[i];
				}
			}
			if(all_count1 == 0) {
				// none found
				base1 = (sites[j].ori&7);
			}
			else if(all_count2 == 0 && base1 != (sites[j].ori&7)) {
				base2 = (sites[j].ori&7);
			}
		}

		// Calculate likelihood
		for(genotype = 0; genotype != 16; genotype++){
			mat->type_likely[genotype] = 0.0;
		}

		//
		// The next set of nested loops is looping over (a) the H, q
		// and c dimensions of the 4-dim recal matrix, then (b) over
		// all aligned bases matching that H, q and c, then (c) over
		// all possible alleles for the current reference position.
		// The result is that each aligned base's mojo gets spread
		// across the candidate alleles according to the equations in
		// the Genome Res paper.
		//

#ifdef FAST_BOUNDS
		char qmin = (sites[j].qmin == 0 ? 1 : sites[j].qmin-1);
		char qmax = (sites[j].qmax == 0 ? 0 : sites[j].qmax-1);
		small_int coordmin = (sites[j].coordmin == 0 ? 1 : sites[j].coordmin-1);
		small_int coordmax = (sites[j].coordmax == 0 ? 0 : sites[j].coordmax-1);
#endif
		// Looping over haplo-genotypes (H) in the 4-dim table?
		for(o_base = 0; o_base != 4; o_base++) {
			if(sites[j].count_uni[o_base] == 0) {
				// No unique alignments with this reference haplotype
				continue;
			}
			// Reset the
			global_dep_count = -1;
			memset(pcr_dep_count, 0, sizeof(int) * 2 * para->read_length);
			// Looping over quality scores (q) in the 4-dim table
#ifdef FAST_BOUNDS
			for(q_score = qmax; q_score >= qmin; q_score--) {
#else
			for(q_score = para->q_max - para->q_min; q_score != -1; q_score--) {
#endif
				// Looping over cycles (c) in the 4-dim table
#ifdef FAST_BOUNDS
				for(coord = coordmin; coord <= coordmax; coord++) {
#else
				for(coord = 0; coord != para->read_length; coord++) {
#endif
					// Looping over reference strands
					for(strand = 0; strand != 2; strand++) {
						// Now iterate over all the aligned bases with:
						//  (a) character 'o_base'
						//  (b) ...aligned to reference strand 'strand'
						//  (c) ...with quality score 'q_score'
						//  (d) ...generated in sequencing cycle 'coord'
						const int bi = o_base << 15 | strand << 14 | q_score << 8 | coord;
						for(k = 0; k != sites[j].base_info[bi]; k++) {
							// pcr_dep_count is indexed by coordinate,
							// and cares about which strand was read
							if(pcr_dep_count[strand*para->read_length+coord] == 0) {
								global_dep_count += 1; // sets it to 0
							}
							pcr_dep_count[strand*para->read_length+coord] += 1;
							// This is where the dependency coefficient
							// is calculated and taken into account.
							// q_score is iterated over in an outer
							// loop.
							q_adjusted = int( pow(10, (log10(q_score) +
							                           (pcr_dep_count[strand*para->read_length+coord]-1) *
							                              para->pcr_dependency +
							                           global_dep_count*para->global_dependency)) + 0.5 );
							if(q_adjusted < 1) {
								q_adjusted = 1;
							}
							// For all 10 diploid alleles...
							for(allele1 = 0; allele1 != 4; allele1++) {
								for(allele2 = allele1; allele2 != 4; allele2++) {
									// Here's where we calculate P(D|T)
									// given all the P(dk|T)s
									double hm = mat->p_matrix[((ubit64_t)q_adjusted << 12) | (coord << 4) | (allele1 << 2) | o_base];
									double hn = mat->p_matrix[((ubit64_t)q_adjusted << 12) | (coord << 4) | (allele2 << 2) | o_base];
									mat->type_likely[allele1 << 2 | allele2] +=
										// Here's where we calculate
										// P(dk|T) given P(dk|Hm) and
										// P(dk|Hn); see p8 of the
										// Genome Res paper
										log10(0.5 * hm + 0.5 * hn);
								}
							}
						}
					}
				}
			}
		}

		//
		// The GLF format takes information about copy-number depth.
		//
		if(1==para->glf_format) {
			// Generate GLFv2 format
			int copy_num;
			if(sites[j].depth == 0) {
				copy_num = 15;
			}
			else {
				copy_num = int(1.442695041*log(sites[j].repeat_time/sites[j].depth));
				if(copy_num > 15) {
					copy_num = 15;
				}
			}
			if(sites[j].depth > 255) {
				sites[j].depth = 255;
			}
			consensus << (unsigned char)(glf_base_code[sites[j].ori&7]<<4|((sites[j].depth>>4)&0xF))<<(unsigned char)((sites[j].depth&0xF)<<4|copy_num&0xF)<<flush;
			type1 = 0;
			// Find the largest likelihood
			for (allele1=0; allele1!=4; allele1++) {
				for (allele2=allele1; allele2!=4; allele2++) {
					genotype = allele1 << 2 | allele2;
					if (mat->type_likely[genotype] > mat->type_likely[type1]) {
						type1 = genotype;
					}
				}
			}
			for(type = 0; type != 10; type++) {
				if(mat->type_likely[type1] -
				   mat->type_likely[glf_type_code[type]] > 25.5)
				{
					consensus << (unsigned char)255;
				} else {
					consensus << (unsigned char)(unsigned int)
						(10 * (mat->type_likely[type1] -
						       mat->type_likely[glf_type_code[type]]));
				}
			}
			consensus << flush;
			if(!consensus.good()) {
				cerr << "Broken ofstream after writing Position " << (sites[j].pos+1) << " at " << call_name << endl;
				exit(255);
			}
			continue;
		}
		// Calculate prior probability
		memcpy(real_p_prior, &mat->p_prior[((ubit64_t)sites[j].ori&0x7)<<4], sizeof(double)*16);
		if ( (sites[j].ori & 0x8) && para->refine_mode) {
			// Refine the prior probability by taking into account that
			// this position is the site of a known SNP
			snp_p_prior_gen(real_p_prior, call_chr->find_snp(sites[j].pos), para, sites[j].ori);
		}
		// Given priors and likelihoods, calculate posteriors and keep
		// the two genotypes with the highest posterior probabilities.
		memset(mat->type_prob, 0, sizeof(rate_t) * 17);
		type2 = type1 = 16;
		for (allele1 = 0; allele1 != 4; allele1++) {
			for (allele2 = allele1; allele2 != 4; allele2++) {
				genotype = allele1 << 2 | allele2;
				if (para->is_monoploid && allele1 != allele2) {
					continue;
				}
				mat->type_prob[genotype] = mat->type_likely[genotype] + log10(real_p_prior[genotype]) ;

				if (mat->type_prob[genotype] >= mat->type_prob[type1] || type1 == 16) {
					type2 = type1;
					type1 = genotype; // new most-likely genotype
				}
				else if (mat->type_prob[genotype] >= mat->type_prob[type2] || type2 ==16) {
					type2 = genotype; // new second-most-likely genotype
				}
			}
		}
		if(2 == para->glf_format) {
			// Generate GLFv2 format
			int copy_num;
			if(sites[j].depth == 0) {
				copy_num = 15;
			}
			else {
				copy_num = int(1.442695041*log(sites[j].repeat_time/sites[j].depth));
				if(copy_num>15) {
					copy_num = 15;
				}
			}
			if(sites[j].depth >255) {
				sites[j].depth = 255;
			}
			consensus<<(unsigned char)(glf_base_code[sites[j].ori&7]<<4|((sites[j].depth>>4)&0xF))<<(unsigned char)((sites[j].depth&0xF)<<4|copy_num&0xF)<<flush;
			type1 = 0;
			// Find the largest likelihood
			for (allele1=0; allele1!=4; allele1++) {
				for (allele2=allele1; allele2!=4; allele2++) {
					genotype = allele1<<2|allele2;
					if (mat->type_prob[genotype] > mat->type_prob[type1]) {
						type1 = genotype;
					}
				}
			}
			for(type=0;type!=10;type++) {
				if(mat->type_prob[type1]-mat->type_prob[glf_type_code[type]]>25.5) {
					consensus<<(unsigned char)255;
				}
				else {
					consensus<<(unsigned char)(unsigned int)(10*(mat->type_prob[type1]-mat->type_prob[glf_type_code[type]]));
				}
			}
			consensus<<flush;
			if(!consensus.good()) {
				cerr<<"Broken ofstream after writting Position "<<(sites[j].pos+1)<<" at "<<call_name<<endl;
				exit(255);
			}
			continue;
		}
		is_out = true; // Check if the position needs to be output, useful in snp-only mode

		if (para->rank_sum_mode) {
			rank_sum_test_value = rank_test(sites[j], type1, mat->p_rank, para);
		}
		else {
			rank_sum_test_value = 1.0;
		}

		if(rank_sum_test_value == 0.0) {
			// avoid double genotype overflow
			q_cns = 0;
		}
		else {
			// Quality of the consensus call is related to the
			// difference between the probabilities of the first and
			// second most probable calls.
			q_cns = (int)(10*(mat->type_prob[type1] -
			                  mat->type_prob[type2]) +
			              10*log10(rank_sum_test_value));
		}

		if ((type1 & 3) == ((type1 >> 2) & 3)) { // Called Homozygous
			if (qual1 > 0 && base1 != (type1 & 3)) {
				// Wired: best base is not the consensus!
				q_cns = 0;
			}
			else if (/*qual2>0 &&*/ q_cns > qual1-qual2) {
				// Should not bigger than this
				q_cns = qual1-qual2;
			}
		}
		else {	// Called Heterozygous
			if(sites[j].q_sum[base1] > 0 &&
			   sites[j].q_sum[base2] > 0 &&
			   type1 == (base1 < base2 ? (base1 << 2 | base2) : (base2 << 2 | base1)))
			{
				// The best bases are in the heterozygote

				// Quality is limited by the difference in quality
				// between the second-best call and the third-best call
				if (q_cns > qual2-qual3) {
					q_cns = qual2-qual3;
				}
			}
			else {	// Ok, wired things happened
				q_cns = 0;
			}
		}
		if(q_cns > 99) {
			q_cns = 99;
		}
		if (q_cns < 0) {
			q_cns = 0;
		}
		// ChrID\tPos\tRef\tCns\tQual\tBase1\tAvgQ1\tCountUni1\tCountAll1\tBase2\tAvgQ2\tCountUni2\tCountAll2\tDepth\tRank_sum\tCopyNum\tSNPstauts\n"
		bool non_ref = (abbv[type1] != "ACTGNNNN"[(sites[j].ori&0x7)] && sites[j].depth > 0);
		if(non_ref) poscalled_nonref++;
		if(!para->is_snp_only || known_snp || non_ref) {
			if(base1 < 4 && base2 < 4) {
				if(known_snp && !non_ref) consensus << "K\t";
				consensus << call_name // chromosome name
				          << '\t' << (sites[j].pos+1) // position
				          << '\t' << ("ACTGNNNN"[(sites[j].ori & 0x7)]) // reference allele
				          << '\t' << abbv[type1] // called type
				          << '\t' << q_cns // quality of call
				          << '\t' << ("ACTGNNNN"[base1]) // base1 call
				          << '\t' << (sites[j].q_sum[base1] == 0 ? 0 : sites[j].q_sum[base1]/sites[j].count_uni[base1])
				          << '\t' << sites[j].count_uni[base1]
				          << '\t' << sites[j].count_all[base1]
				          << '\t' << ("ACTGNNNN"[base2]) // base2 call
				          << '\t' << (sites[j].q_sum[base2]==0?0:sites[j].q_sum[base2]/sites[j].count_uni[base2])
				          << '\t' << sites[j].count_uni[base2]
				          << '\t' << sites[j].count_all[base2]
				          << '\t' << sites[j].depth
				          << '\t' << sites[j].dep_pair
				          << '\t' << showpoint << rank_sum_test_value
				          << '\t' << (sites[j].depth == 0 ? 255 : (double)(sites[j].repeat_time)/sites[j].depth)
				          << '\t' << ((sites[j].ori & 8) ? 1 : 0) // dbSNP locus?
				          << endl;
			}
			else if(base1 < 4) {
				if(known_snp && !non_ref) consensus << "K\t";
				consensus << call_name // chromosome name
				          << '\t' << (sites[j].pos+1) // position
				          << '\t' << ("ACTGNNNN"[(sites[j].ori&0x7)]) // reference char
				          << '\t' << abbv[type1] // called type
				          << '\t' << q_cns // quality of call
				          << '\t' << ("ACTGNNNN"[base1]) // first heterozygous base
				          << '\t' << (sites[j].q_sum[base1] == 0 ? 0 : sites[j].q_sum[base1]/sites[j].count_uni[base1])
				          << '\t' << sites[j].count_uni[base1]
				          << '\t' << sites[j].count_all[base1]
				          << '\t' << "N\t0\t0\t0"
				          << '\t' << sites[j].depth
				          << '\t' << sites[j].dep_pair
				          << '\t' << showpoint << rank_sum_test_value
				          << '\t' << (sites[j].depth == 0 ? 255 : (double)(sites[j].repeat_time)/sites[j].depth)
				          << '\t' << ((sites[j].ori & 8) ? 1 : 0) // dbSNP locus?
				          << endl;
			}
			else {
				if(known_snp && !non_ref) consensus << "K\t";
				consensus << call_name
				          << '\t'
				          << (sites[j].pos+1)
				          << "\tN\tN\t0\tN\t0\t0\t0\tN\t0\t0\t0\t0\t0\t1.000\t255.000\t0"
				          << endl;
			}
		}
	}
	delete [] real_p_prior;
	delete [] pcr_dep_count;
	return 1;
}
