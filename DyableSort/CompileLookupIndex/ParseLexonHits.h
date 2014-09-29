#include ".\ParseClusterHits.h"

// The lexon hit list must be parsed to separate out the 
// different doc hash divisions in a given word division. This 
// is so a pointer can be stored to the start of each doc hash 
// division for more efficient lookup. In addition each doc hash 
// division size in bytes is recorded before the division so it 
// can be lookuped up correctly. This is done after all the lexon
// hits have been sorted.

// Also consideration must be given to a parallel implementation.
// This is done by further grouping doc hash divisions into parallel
// classes. During retrieve requests are made by foreign clients
// to look for a doc id in a given hash division. Each client has
// exclusive control over their segment of the doc hash divisions.
// If a client recieves a request from a foreign client it will try
// looking for a document in the correct hash division and return
// the complete hit item if the doc id is found.

// In terms of searching for hit items, a combination of hashing,
// a binary and linear search is performed. First the doc hash
// division is calculated for a doc id. Then a binary search is 
// performed in blocks of ALIGN_BLOCK_SIZE bytes. Once the correct
// block has been found a linear search is peformed in the block
// (more details given in retrieve).
class CParseLexonHits : public CParseClusterHits {

	/*******************************************
	/* these must be a power of two
	*******************************************/
	// this defines the hash size for title hits
	static const int TITLE_HASH_SIZE = 16;
	// this defines the hash size for other hits
	static const int OTHER_HASH_SIZE = 128;
	// this defines the hash size for image hits
	static const int IMAGE_HASH_SIZE = 8;
	// this defines the hash size for excerpt hits
	static const int EXCERPT_HASH_SIZE = 32;

	// stores the sorted lexon hit list
	CCompression m_lexon_hit_comp;
	// this stores the number of cluster hits (in bytes) 
	// stored in a respective division of doc hash boundary
	CMemoryChunk<CMemoryChunk<_int64> > m_doc_hash_bound_num;
	// this stores the hash size for each of the different hit types
	CMemoryChunk<int> m_hit_hash_size;

	// this adds a lexon hit - read from the sorted buffer
	// @param hit_item - the first hit item in the current word division
	inline void AddLexonHit(SMergedHitItem &hit_item) {

		int client = hit_item.doc_id % LOOKUP_CLIENT_NUM;
		int doc_hash = hit_item.doc_id % DOC_HASH_SIZE;
		doc_hash >>= m_hit_hash_size[hit_item.hit_type_index];

		m_hit_list[hit_item.hit_type_index].AddLexonHit(hit_item, client);
		m_doc_hash_bound_num[hit_item.hit_type_index][doc_hash] += hit_item.hit_size;
	}

public:

	CParseLexonHits() {
		m_doc_hash_bound_num.AllocateMemory(4);
		m_hit_hash_size.AllocateMemory(4);
		m_hit_hash_size[EHitType::IMAGE_HIT] = min(DOC_HASH_SIZE, IMAGE_HASH_SIZE);
		m_hit_hash_size[EHitType::EXCERPT_HIT] = min(DOC_HASH_SIZE, EXCERPT_HASH_SIZE);
		m_hit_hash_size[EHitType::OTHER_HIT] = min(DOC_HASH_SIZE, OTHER_HASH_SIZE);
		m_hit_hash_size[EHitType::TITLE_HIT] = min(DOC_HASH_SIZE, TITLE_HASH_SIZE);
	}

	// this initializes the lexon hits - preparing to be parsed
	// @param hit_item - used to store the first cluster hit
	void InitializeLexonHits(SMergedHitItem &hit_item) {

		m_lexon_hit_comp.LoadIndex("../lexon_hit_list");
		hit_item.RetrieveLexonHitItem(m_lexon_hit_comp);

		int base_size = CMath::LogBase2(DOC_HASH_SIZE - 1);
		for(int i=0;i<4;i++) {
			m_doc_hash_bound_num[i].AllocateMemory(m_hit_hash_size[i]);
			m_hit_hash_size[i] = base_size - CMath::LogBase2(m_hit_hash_size[i] - 1);
		}
	}

	// this is the entry function for parsing a given word division 
	// @param curr_word_div - the current word division being processed
	// @param hit_item - the first hit item in the current word division
	void ParseLexonWordDivision(uLong curr_word_div, SMergedHitItem &hit_item) {

		cout<<"Parsing Lexon Word Div "<<curr_word_div<<endl;
		for(int i=0;i<m_doc_hash_bound_num.OverflowSize();i++) {
			m_doc_hash_bound_num[i].InitializeMemoryChunk(0);
		}

		for(int i=0;i<4;i++) {
			m_hit_list[i].ResetDocumentHashOffset();
		}

		// first adds the hit item in the current word division left over
		// from the last parsing of the sorted cluster hit list
		AddLexonHit(hit_item);

		while(m_lexon_hit_comp.AskNextHitAvailable()) {
			hit_item.RetrieveLexonHitItem(m_lexon_hit_comp);
			if(hit_item.word_id_offset != curr_word_div) 
				return;

			AddLexonHit(hit_item);
		}	

		hit_item.word_id_offset = INFINITE;
	}

	// This writes the lookup tree structure to file. That is the
	// number of bytes used to store hits in each spatial division.
	void WriteHistogramBoundary(CCompression &comp) {

		for(int j=0;j<4;j++) {
			for(int i=0;i<m_doc_hash_bound_num[j].OverflowSize();i++) {
				comp.AskBufferOverflow(6);
				comp.AddEscapedItem(m_doc_hash_bound_num[j][i]);
			}
		}
	}
};