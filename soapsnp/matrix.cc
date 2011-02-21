#include "soap_snp.h"
Prob_matrix::Prob_matrix(){
	int i;
	// p_matrix has 1 million entires; rate_t is a double
	p_matrix = new rate_t [256*256*4*4]; // 8bit: q_max, 8bit: read_len, 4bit: number of types of all mismatch/match 4x4
	p_prior = new rate_t [8*4*4]; // 8(ref ACTGNNNN) * diploid(4x4)
	base_freq = new rate_t [4]; // 4 base
	type_likely = new rate_t [16+1]; //The 17th element rate_t[16] will be used in comparison
	type_prob = new rate_t [16+1];
	p_rank = new rate_t [64*64*2048]; // 6bit: N; 5bit: n1; 11bit; T1
	p_binom = new rate_t [256*256]; // Total * case
	for(i=0;i!=256*256*4*4;i++) {
		p_matrix[i] = 1.0;
	}
	for(i=0;i!=8*4*4;i++) {
		p_prior[i] = 1.0;
	}
	for(i=0;i!=4;i++) {
		base_freq[i] = 1.0;
	}
	for(i=0;i!=16+1;i++) {
		type_likely[i] = 0.0; // LOG10 Scale
		type_prob[i] = 0.0; // LOG10 Scale
	}
	for(i=0;i!=64*64*2048;i++) {
		p_rank[i] = 1.0;
	}
	for(i=0;i!=256*256;i++) {
		p_binom[i] = 1.0;
	}
}

Prob_matrix::~Prob_matrix(){
	delete [] p_matrix; // 8bit: q_max, 8bit: read_len, 4bit: number of types of all mismatch/match 4x4
	delete [] p_prior; // 8(ref ACTGNNNN) * diploid(4x4)
	delete [] base_freq; // 4 base
	delete [] type_likely; //The 17th element rate_t[16] will be used in comparison
	delete [] type_prob;
	delete [] p_rank; // 6bit: N; 5bit: n1; 11bit; T1
	delete [] p_binom; // Total * case;
}

int Prob_matrix::matrix_read(std::fstream &mat_in, Parameter * para) {
	int q_char, type;
	std::string::size_type coord;
	for(std::string line; getline(mat_in, line);) {
		std::istringstream s(line);
		s>>q_char>>coord;
		for(type=0;type!=16;type++) {
			s>>p_matrix [ ((ubit64_t)q_char<<12) | (coord <<4) | type];
		}
	}
	return 1;
}

int Prob_matrix::matrix_write(std::fstream &mat_out, Parameter * para) {
	for( char q_char = para->q_min; q_char <= para->q_max; q_char++ ) {
		for( std::string::size_type coord=0; coord != para->read_length; coord++) {
			mat_out<<((ubit64_t)q_char-para->q_min)<<'\t'<<coord;
			for(char type=0;type!=16;type++) {
				mat_out<<'\t'<<scientific<<showpoint<<setprecision(16)<<p_matrix [ ((ubit64_t)(q_char-para->q_min)<<12) | (coord <<4) | type];
			}
			mat_out<<endl;
		}
	}
	return 1;
}
