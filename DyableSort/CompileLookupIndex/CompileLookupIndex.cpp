#include "./ParseClusterHits.h"

// This class is responsible creating the list of byte offsets with 
// regards to the different hit lists. This is so the start of a word
// division can be jumped to during retrieval. It's possible that a 
// word divsion is empty in this case an offset still needs to be 
// stored to allow for correct lookup. Also offsets relating to the 
// spatial tree boundary needs to be stored for lookup. To find the 
// correct number of bytes to offset for each word division, every
// word hit in a word division needs to be parsed. One last final note
// multiple parallel client sets are created for retrieval meaning that
// offsets for each client set needs to be stored seperately.
class CCompileLookupIndex : public CParseClusterHits {

	// This fills in any gaps in the word id's 
	// @param word_id_gap - the number of word id's that don't have any hits
	void FillInGap(int word_id_gap) {

		m_index_block.doc_num = 0;
		for(int j=0; j<word_id_gap-1; j++) {
			// first fills in the word id gaps
			m_index_block.WriteDocumentCount(m_lookup_comp);

			for(int i=0; i<2; i++) {
				m_hit_list[i].FinishCurrDiv(m_lookup_comp);
			}
		}
	}

	// This is called when all the hits in the given 
	// word id set have been processed and it's time
	// to write the final lookup index
	void FinishLookupIndex() {

		for(int i=0; i<2; i++) {
			m_hit_list[i].StartNewDiv();
		}

		m_index_block.WriteDocumentCount(m_lookup_comp);

		for(int i=0; i<2; i++) {
			m_hit_list[i].FinishCurrDiv(m_lookup_comp);
		}

		m_lookup_comp.FinishCompression();
	}

	// This creates all the indexes for a given 
	// hit list division at startup
	void InitilizeFinalLookupTree() {

		m_hit_list.AllocateMemory(2);
		m_hit_list[TITLE_HIT_INDEX].Initialize
			("GlobalData/Retrieve/title_hit", SortDiv());
		m_hit_list[EXCERPT_HIT_INDEX].Initialize
			("GlobalData/Retrieve/excerpt_hit", SortDiv());

		m_lookup_comp.Initialize(CUtility::ExtendString
			("GlobalData/Retrieve/lookup", SortDiv()), 
			SLookupIndex::WORD_DIV_BYTE_SIZE * LOOKUP_DIV_SIZE);
	}

public:

	CCompileLookupIndex() {
	}

	// This is the main entry point for compiling the 
	// final lookup index. The two different hitlists
	// lexon and cluster must be merged.
	// @param sort_div - this is the current sort division being processed
	void CompileLookupIndex(int sort_div) {

		m_sort_div = sort_div;
		InitilizeFinalLookupTree();

		SHitItem cluster_hit;
		InitializeClusterHits(cluster_hit);
		int curr_word_div = -1;

		while(cluster_hit.word_id != INFINITE) {

			for(int i=0; i<2; i++) {
				m_hit_list[i].StartNewDiv();
			}

			int prev_word_div = curr_word_div;
			curr_word_div = cluster_hit.word_id;
			FillInGap(curr_word_div - prev_word_div);

			// processes cluster hits in the current word division
			ParseClusterHits(curr_word_div, cluster_hit);

			m_index_block.WriteDocumentCount(m_lookup_comp);

			for(int i=0; i<2; i++) {
				m_hit_list[i].FinishCurrDiv(m_lookup_comp);
				m_hit_list[i].StoreHitSegmentInfo();
			}
		}

		FinishLookupIndex();
	}
};

int main(int argc, char *argv [])
{

	if(argc < 2)return 0; 
	int sort_div = atoi(argv[1]);

	CBeacon::InitializeBeacon(sort_div, 2222);
	CMemoryElement<CCompileLookupIndex> compile;
	compile->CompileLookupIndex(sort_div);
	compile.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();

}