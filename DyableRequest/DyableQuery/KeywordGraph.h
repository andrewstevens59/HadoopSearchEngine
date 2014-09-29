#include "../ExpandABTree.h"

// This is used to find an approximation of the expected reward of 
// the top N nodes using the keyword graph.
class CKeywordGraph1 {

	// This defines the number of iterations to perform
	static const int IT_NUM = 5;

	// This stores the set of doc ids attatched to a given doc id
	struct SDocSet {
		// This stores the local document id
		int doc_id;
		// This stores the ptr to the next keyword
		SDocSet *next_ptr;
	};

	// This stores the link set
	struct SLink {
		// This stores the local doc_id
		int doc_id;
		// This stores the link weight
		float link_weight;
		// This stores the ptr to the next link
		SLink *next_ptr;
	};

	// This stores the exp rew for each node
	struct SExpRew {
		// This stores the link ptr
		SLink *link_ptr;
		// This stores the propagated score
		float prop_score;
		// This stores the doc score
		float doc_score;

		SExpRew() {
			link_ptr = NULL;
		}
	};

	// This stores the reverse keyword doc id map
	CMemoryChunk<SDocSet *> m_doc_set;
	// This stores the link cluster for each doc set
	CMemoryChunk<SExpRew> m_link_set;

	// This stores the links
	CLinkedBuffer<SLink> m_link_buff;
	// This stores the document buff
	CLinkedBuffer<SDocSet> m_keyword_buff;
	// This stores the occupied document link sets
	CLinkedBuffer<int> m_occ_div_buff;

	// This normalises the link clusters
	void NormalizeLinkClusters() {

		int *div;
		m_occ_div_buff.ResetPath();
		while((div = m_occ_div_buff.NextNode()) != NULL) {

			float net_link_weight = 0;
			SLink *curr_ptr = m_link_set[*div].link_ptr;
			while(curr_ptr != NULL) {
				net_link_weight += curr_ptr->link_weight;
				curr_ptr = curr_ptr->next_ptr;
			}

			curr_ptr = m_link_set[*div].link_ptr;
			while(curr_ptr != NULL) {
				curr_ptr->link_weight /= net_link_weight;
				curr_ptr = curr_ptr->next_ptr;
			}
		}
	}

public:

	CKeywordGraph1() {
		m_link_buff.Initialize();
		m_keyword_buff.Initialize();
		m_occ_div_buff.Initialize();
	}

	// This initializes the keyword graph
	void Initialize(int keyword_num, int doc_num) {
		m_link_set.AllocateMemory(doc_num);
		m_doc_set.AllocateMemory(keyword_num, NULL);
	}

	// This returns the expected reward for a given ode
	inline float ExpRew(int doc_id) {
		return m_link_set[doc_id].doc_score;
	}

	// This adds a keyword doc id mapping
	// @param keyword_id - this is the local keyword id
	// @param doc_id - this is the local doc id
	void AddKeywordDocMapping(int keyword_id, int doc_id) {

		SDocSet *ptr = m_keyword_buff.ExtendSize(1);
		ptr->doc_id = doc_id;
		SDocSet *prev_ptr = m_doc_set[keyword_id];
		m_doc_set[keyword_id] = ptr;
		ptr->next_ptr = prev_ptr;
	}

	// This adds a link to the graph
	// @param keyword_id - this is the local keyword id
	// @param doc_id - this is the local doc id
	// @param keyword_occur - this is the occurrence of the keyword
	// @param doc_score - this is the keyword score of the document
	void AddLinkToGraph(int keyword_id, int doc_id, int keyword_occur) {

		SDocSet *curr_ptr = m_doc_set[keyword_id];
		while(curr_ptr != NULL) {

			if(curr_ptr->doc_id != doc_id) {
				SLink *ptr = m_link_buff.ExtendSize(1);
				ptr->doc_id = curr_ptr->doc_id;
				ptr->link_weight = 1.0f;

				SLink *prev_ptr = m_link_set[doc_id].link_ptr;
				m_link_set[doc_id].link_ptr = ptr;
				ptr->next_ptr = prev_ptr;

				if(prev_ptr == NULL) {
					m_occ_div_buff.PushBack(doc_id);
					m_link_set[doc_id].doc_score = 1.0f;
					m_link_set[doc_id].prop_score = 0;
				}
			}

			curr_ptr = curr_ptr->next_ptr;
		}	
	}

	// This approximates the expected reward for each node in the set
	void ApproximateExpRew() {

		int *div;
		for(int i=0; i<IT_NUM; i++) {
			m_occ_div_buff.ResetPath();
			while((div = m_occ_div_buff.NextNode()) != NULL) {

				SExpRew &exp_rew = m_link_set[*div];
				SLink *curr_ptr = m_link_set[*div].link_ptr;

				while(curr_ptr != NULL) {
					exp_rew.prop_score += m_link_set[curr_ptr->doc_id].doc_score;
					curr_ptr = curr_ptr->next_ptr;
				}
			}

			m_occ_div_buff.ResetPath();
			while((div = m_occ_div_buff.NextNode()) != NULL) {
				SExpRew &exp_rew = m_link_set[*div];
				exp_rew.doc_score = exp_rew.prop_score;
				exp_rew.prop_score = 0;
			}
		}
	}
};