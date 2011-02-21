BITS_FLAG =
ifeq (32,$(BITS))
BITS_FLAG = -m32
endif
ifeq (64,$(BITS))
BITS_FLAG = -m64
endif

DEFINE =
CXX = g++
CXXFLAGS = #-MMD -MP -MF #-g3 -Wall -maccumulate-outgoing-args
CXXFLAGS_RELEASE = -fomit-frame-pointer -O3 -ffast-math -funroll-loops -mmmx -msse -msse2 -msse3 -fmessage-length=0 -DNDEBUG -DFAST_BOUNDS
CXXFLAGS_DEBUG = -g -g3 -O0
LFLAGS =

all: soapsnp
.PHONY: all

soapsnp: call_genotype.cc chromosome.cc matrix.cc normal_dis.cc prior.cc rank_sum.cc main.cc soap_snp.h makefile
	$(CXX) $(CXXFLAGS) $(CXXFLAGS_RELEASE) $(BITS_FLAG) call_genotype.cc chromosome.cc matrix.cc normal_dis.cc prior.cc rank_sum.cc main.cc -o $@ $(LFLAGS)

soapsnp-debug: call_genotype.cc chromosome.cc matrix.cc normal_dis.cc prior.cc rank_sum.cc main.cc soap_snp.h makefile
	$(CXX) $(CXXFLAGS) $(CXXFLAGS_DEBUG) $(BITS_FLAG) call_genotype.cc chromosome.cc matrix.cc normal_dis.cc prior.cc rank_sum.cc main.cc -o $@ $(LFLAGS)

binarize: call_genotype.cc chromosome.cc matrix.cc normal_dis.cc prior.cc rank_sum.cc binarize.cc soap_snp.h makefile
	$(CXX) $(CXXFLAGS) call_genotype.cc chromosome.cc matrix.cc normal_dis.cc prior.cc rank_sum.cc binarize.cc -o binarize $(LFLAGS)

.PHONY: clean
clean:
	rm -f *.o soapsnp
