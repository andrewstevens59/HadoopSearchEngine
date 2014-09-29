#include "./HitTypeWordDivision.h"

// This is the class that creates the final hit list index that includes 
// the spatial tree at the start of the word division. The tree should 
// already been sorted partially by cluster and then by pulse score. All
// that needs to be done is to parse a word division and store pointers 
// to the start of different cluster bucket divisions similar to lexon 
// hits. During retrieve flood search is performed to try and perform an 
// optimal search.

// In terms of a parallel implementation each spatial block is divided
// up amongst different parallel clients. Searching is done by each client
// independent of the others with the exception of the exchange of lexon 
// hits. Each client operates seperately on their section of the spatial 
// hiearchy. However when searching is finished the clients must communicate
// again to facilitate the local ranking procedure (details in retrieve).
class CParseClusterHits {

	// this stores the sorted cluster hits
	CFileComp m_cluster_hit_set;
	// Stores the previous doc_id processed
	_int64 m_prev_doc_id;
	// This stores the current byte offset
	_int64 m_byte_offset;

	// This is used to start a new client for the cluster hits
	void BeginNewClusterClient() {
		m_hit_list[EXCERPT_HIT_INDEX].StartNewDiv();
		m_hit_list[TITLE_HIT_INDEX].StartNewDiv();
	}

	// This adds a hit item of any type and places it in the correct
	// hit type list and assigns it a client id based upon the doc
	// id in relation to the spatial hiearchy.
	// @param hit_item - this is the hit item that's being added 
	void AddHitItem(SHitItem &hit_item) {

		if(hit_item.word_id != m_lookup_comp.BytesStored() / SLookupIndex::WORD_DIV_BYTE_SIZE) {
			cout<<"Index Error";getchar();
		}
		
		int hit_index = hit_item.HitTypeIndex();
		m_hit_list[hit_index].AddClusterHit(hit_item);

		if(hit_item.doc_id > m_prev_doc_id) {
			m_prev_doc_id = hit_item.doc_id.Value();
			m_index_block.doc_num++;
		}
	}

	// This is the entry function for parsing a given word division 
	// @param hit_list_file - this stores all the hits for this sort div
	// @param curr_word_div - the current word division being processed
	// @param hit_item - the first hit item in the current word division
	// @param begin_client - this is a function pointer that starts a new client 
	//                     - for cluster or image hits
	void ParseWordDivision(CFileComp &hit_list_file, int curr_word_div,
		SHitItem &hit_item, void (CParseClusterHits::*begin_client)()) {

		if(hit_item.word_id != curr_word_div) {
			return;
		}

		(this->*begin_client)();

		// first adds the hit item in the current word division left over
		// from the last parsing of the sorted cluster hit list
		AddHitItem(hit_item);

		while(hit_item.ReadHitWordOrder(hit_list_file)) {

			if(begin_client == &CParseClusterHits::BeginNewClusterClient) {
				m_byte_offset += 10;
				if((m_byte_offset % 1000000) == 0) {
					float percent = (float)m_byte_offset / 
						hit_list_file.CompBuffer().BytesStored();
					cout<<(percent * 100)<<"% Done"<<endl;
				}
			}

			if(hit_item.word_id != curr_word_div) {
				return;
			}

			AddHitItem(hit_item);
		}	

		hit_item.word_id = INFINITE;
	}


protected:

	// This stores the sort division being processed
	int m_sort_div;
	// This is used to store the sorted cluster and lexon hits
	// for each hit type (EXCERPT, TITLE, OTHER, IMAGE)
	CMemoryChunk<CHitTypeWordDivision> m_hit_list;
	// This stores the current index block being processed
	SLookupIndex m_index_block;

	// This stores the lookup index that stores pointers
	// to the hit items and the tree structure
	CCompression m_lookup_comp;

	// This returns the sort division being processed 
	inline int SortDiv() {
		return m_sort_div;
	}

public:

	CParseClusterHits() {
	}

	// this initializes the cluster hits - preparing to be parsed
	// @param cluster_hit - used to store the first cluster hit
	void InitializeClusterHits(SHitItem &cluster_hit) {

		m_cluster_hit_set.OpenReadFile(CUtility::ExtendString
			("GlobalData/SortedHits/sorted_base_hits", SortDiv()));

		cluster_hit.ReadHitWordOrder(m_cluster_hit_set);
		m_byte_offset = 0;
	}

	// This is called to parse cluster hits
	// @param curr_word_div - the current word division being processed
	// @param hit_item - the first hit item in the current word division
	void ParseClusterHits(int curr_word_div, SHitItem &hit_item) {

		m_prev_doc_id = -1;
		m_index_block.doc_num = 0;
		ParseWordDivision(m_cluster_hit_set, curr_word_div, 
			hit_item, &CParseClusterHits::BeginNewClusterClient);
	}

	// This returns a predicate specifying whether all hits have been parsed
	inline bool IsNextHitAvailable() {
		return m_cluster_hit_set.CompBuffer().AskNextHitAvailable();
	}
	
};