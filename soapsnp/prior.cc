#include "soap_snp.h"

int Prob_matrix::prior_gen(Parameter * para) {
	char t_base, allele1, allele2;
	// Note, the above parameter should be changed to a more reasonable one
	for(t_base=0;t_base!=4;t_base++) {
		for(allele1=0;allele1!=4;allele1++) {
			for(allele2=allele1;allele2!=4;allele2++) {
				if(allele1 == t_base && allele2 == t_base) {
					// refHOM
					p_prior[t_base<<4|allele1<<2|allele2] = 1;
				}
				else if (allele1 == t_base || allele2 == t_base) {
					// refHET: 1 ref 1 alt
					p_prior[t_base<<4|allele1<<2|allele2] = para->het_novel_r;
				}
				else if (allele1 == allele2) {
					// altHOM
					p_prior[t_base<<4|allele1<<2|allele2] = para->althom_novel_r;
				}
				else {
					// altHET: 2 diff alt base
					p_prior[t_base<<4|allele1<<2|allele2] = para->het_novel_r * para->althom_novel_r;
				}
				if( para->transition_dominant && ((allele1^t_base) == 0x3 || (allele2^t_base) == 0x3)) {
					// transition
					p_prior[t_base<<4|allele1<<2|allele2] *= 4;
				}
				//std::cerr<<"ACTG"[t_base]<<"\t"<<"ACTG"[allele1]<<"ACTG"[allele2]<<"\t"<<p_prior[t_base<<4|allele1<<2|allele2]<<endl;
			}
		}
	}
	for(allele1=0;allele1!=4;allele1++) {
		for(allele2=allele1;allele2!=4;allele2++) {
			// Deal with N
			p_prior[0x4<<4|allele1<<2|allele2] = (allele1==allele2? 1: (2*para->het_novel_r)) * 0.25 *0.25;
			p_prior[0x5<<4|allele1<<2|allele2] = (allele1==allele2? 1: (2*para->het_novel_r)) * 0.25 *0.25;
			p_prior[0x6<<4|allele1<<2|allele2] = (allele1==allele2? 1: (2*para->het_novel_r)) * 0.25 *0.25;
			p_prior[0x7<<4|allele1<<2|allele2] = (allele1==allele2? 1: (2*para->het_novel_r)) * 0.25 *0.25;
		}
	}
	return 1;
}

/**
 * Generate a prior probability for each diploid genotype given SNPdb
 * allele frequency data.
 */
int Call_win::snp_p_prior_gen(double * real_p_prior, Snp_info* snp,
                              Parameter * para, char ref)
{
	if (snp->is_indel()) {
		return 0;
	}
	char base, allele1, allele2;
	int allele_count;
	allele_count = 0;
	for (base=0; base != 4; base ++) {
		if(snp->get_freq(base)>0) {
			// The base is found in dbSNP
			allele_count += 1;
		}
	}
	if(allele_count <= 1) {
		// Should never occur

		// BTL: Yes, this can occur, when all subjects in a HapMap
		// population have different alleles from the reference.

		//cerr<<"Previous Extract SNP error."<<endl;
		//exit(255);
		//return -1;
	}
	char t_base = (ref&0x3);
	for(allele1=0;allele1!=4;allele1++) {
		for(allele2=allele1;allele2!=4;allele2++) {

			// Note: site are either HapMap or not HapMap.  When sites
			// are from HapMap, SOAPsnp trusts the allele frequencies.

			if(!snp->is_hapmap()) {
				// Real HapMap Sites
				if(snp->get_freq(allele1) > 0 && snp->get_freq(allele2) > 0) {
					// Here the frequency is just a tag to indicate SNP alleles in non-HapMap sites
					if(allele1 == allele2 && allele1 == t_base) {
						// refHOM
						real_p_prior[allele1<<2|allele2] = 1;
					}
					else if (allele1 == t_base || allele2 == t_base) {
						// refHET: 1 ref 1 alt
						real_p_prior[allele1<<2|allele2] = snp->is_validated()?para->het_val_r:para->het_unval_r;
					}
					else if (allele1 == allele2) {
						real_p_prior[allele1<<2|allele2] =  snp->is_validated()?para->althom_val_r:para->althom_unval_r;
					}
					else {
						// altHET: 2 diff alt base
						real_p_prior[allele1<<2|allele2] = snp->is_validated()?para->het_val_r:para->het_unval_r;
					}
				}
			}
			else {
				// Real HapMap Sites
				if(snp->get_freq(allele1) > 0 && snp->get_freq(allele2) > 0) {
					real_p_prior[allele1<<2|allele2] = (allele1==allele2?1:(2*para->het_val_r))*snp->get_freq(allele1)*snp->get_freq(allele2);
				}
			}
		}
	}
	return 1;
}
