#include "../ExpandABTree.h"

// This class is used to find documents that have a high traversal probability
// while at the same time avoiding documents that are too similar to documents
// already in the set. The general procedure is to find an approximation to the
// limiting distribution and select the node with the highest traversal probabilty.
// Nodes with a high recurrence time are removed from the set.
class CExpRew {

	// This stores one of the links
	struct SLink {
		// This stores the ptr to the next link
		SLink *next_ptr;
		// This stores the local doc id
		int doc_id;
		//	This stores the link weight
		float link_weight;
	};

	// This stores a reverse link
	struct SReverseLink {
		// This stores the ptr to the next link
		SReverseLink *next_ptr;
		// This stores the local doc id
		int doc_id;
	};


	// This stores one of the documents
	struct SDocument {
		// This stores the traversal probability
		float trav_prob;
		// This is the updated back buffer
		float back_buff;
		// This stores the ptr to the document
		SDocMatch *doc_ptr;
		// This stores the ptr to the first forward link
		SLink *forward_link_ptr;
		// This stores the ptr to the first reverse link
		SReverseLink *reverse_link_ptr;
		// This stores the past iteration count
		u_short it_num;
		// This stores the hop count
		uChar hop_count;
	};

	// This is used to rank documents
	struct SRankDoc {
		// This stores the traversal probability
		float trav_prob;
		// This stores the ptr to the document
		SDocMatch *doc_ptr;
		// This stores the local doc id
		int doc_id;
	};

	// This stores the set of document matches
	static CHashDictionary<int> m_doc_map;
	// This stores the link set map
	static CArrayList<SDocument> m_link_set;
	// This stores the set of links
	CLinkedBuffer<SLink> m_link_buff1;
	// This stores the set of links
	CLinkedBuffer<SReverseLink> m_link_buff2;
	// This stores hte set of active documents
	CArrayList<int> m_active_doc_buff;

	// This stores the rank queue
	CLimitedPQ<SRankDoc> m_doc_queue;
	// This stores the bfs queue
	CQueue<SDocument *> m_bfs_queue;
	// This stores the current iteration
	u_short m_it_num;

	// This is used to compare documents
	static int CompareDocuments(const SRankDoc &arg1, const SRankDoc &arg2) {

		if(arg1.doc_ptr->title_div_num < arg2.doc_ptr->title_div_num) {
			return -1;
		}

		if(arg1.doc_ptr->title_div_num > arg2.doc_ptr->title_div_num) {
			return 1;
		}

		if(arg1.trav_prob < arg2.trav_prob) {
			return -1;
		}

		if(arg1.trav_prob > arg2.trav_prob) {
			return 1;
		}

		if(arg1.doc_ptr->rank < arg2.doc_ptr->rank) {
			return -1;
		}

		if(arg1.doc_ptr->rank > arg2.doc_ptr->rank) {
			return 1;
		}

		return 0;
	}

	// This updates the recurrence time
	void UpdateRecurrenceTime(float &sum, int doc_id, int hop_count) {

		SDocument &dst_doc = m_link_set[doc_id];

		if(dst_doc.it_num == m_it_num) {

			if(dst_doc.hop_count > 1) {
				sum += 1.0f / hop_count;
			}

		} else {
			dst_doc.hop_count = hop_count + 1;
			m_bfs_queue.AddItem(&dst_doc);
		}
	}

	// This finds the average recurrence time of a given node
	float FindRecurrenceTime(SDocument *src_ptr) {

		float sum = 0;
		m_bfs_queue.Reset();
		m_bfs_queue.AddItem(src_ptr);
		src_ptr->hop_count = 0;
		m_it_num++;

		for(int i=0; i<10; i++) {

			if(m_bfs_queue.Size() == 0) {
				return sum;
			}

			SDocument *doc_ptr = m_bfs_queue.PopItem();

			doc_ptr->it_num = m_it_num;
			SReverseLink *curr_ptr2 = doc_ptr->reverse_link_ptr;
			while(curr_ptr2 != NULL) {
				UpdateRecurrenceTime(sum, curr_ptr2->doc_id, doc_ptr->hop_count);
				curr_ptr2 = curr_ptr2->next_ptr;
			}
		}
		cout<<sum<<" ";
		return sum;
	}

public:

	CExpRew() {
		m_it_num = 0;
		m_doc_queue.Initialize(20, CompareDocuments);
		m_bfs_queue.Initialize();
		m_doc_map.Initialize(10000);
		m_link_buff1.Initialize();
		m_link_buff2.Initialize();
		m_link_set.Initialize(1024);
		m_active_doc_buff.Initialize(1024);
	}

	// This resets the exp rew class
	void Reset() {
		m_it_num = 0;
		m_doc_map.Reset();
		m_link_buff1.Restart();
		m_link_buff2.Restart();
		m_link_set.Resize(0);
		m_active_doc_buff.Resize(0);
	}

	// This adds a neighbour node
	static void AddNeighbourNode(SDocMatch *doc_ptr) {
		m_doc_map.AddWord((char *)&doc_ptr->doc_id, sizeof(S5Byte));
		if(m_doc_map.AskFoundWord() == false) {
			m_link_set.ExtendSize(1);
			m_link_set.LastElement().doc_ptr = doc_ptr;
			m_link_set.LastElement().trav_prob = 1.0f;
			m_link_set.LastElement().it_num = 0;
			m_link_set.LastElement().forward_link_ptr = NULL;
			m_link_set.LastElement().reverse_link_ptr = NULL;
		}
	}

	// This adds a link to the set
	int AddLink(S5Byte &src, S5Byte &dst, uChar match) {

		int src_id = m_doc_map.FindWord((char *)&src, sizeof(S5Byte));
		int dst_id = m_doc_map.FindWord((char *)&dst, sizeof(S5Byte));
		if(dst_id < 0) {
			return src_id;
		}

		SLink *prev_ptr1 = m_link_set[src_id].forward_link_ptr;
		if(prev_ptr1 == NULL) {
			m_active_doc_buff.PushBack(src_id);
		}

		m_link_set[src_id].forward_link_ptr = m_link_buff1.ExtendSize(1);
		m_link_set[src_id].forward_link_ptr->next_ptr = prev_ptr1;
		m_link_set[src_id].forward_link_ptr->doc_id = dst_id;
		m_link_set[src_id].forward_link_ptr->link_weight = match;

		SReverseLink *prev_ptr2 = m_link_set[dst_id].reverse_link_ptr;

		if(prev_ptr2 == NULL) {
			m_active_doc_buff.PushBack(dst_id);
		}

		m_link_set[dst_id].reverse_link_ptr = m_link_buff2.ExtendSize(1);
		m_link_set[dst_id].reverse_link_ptr->next_ptr = prev_ptr2;
		m_link_set[dst_id].reverse_link_ptr->doc_id = src_id;

		return src_id;
	}

	// This finds the approximate traversal probability
	void ApproxTravProb(int it_num) {

		for(int i=0; i<m_active_doc_buff.Size(); i++) {
			int id = m_active_doc_buff[i];
			m_link_set[id].back_buff = 0;
		}

		for(int j=0; j<it_num; j++) {

			for(int i=0; i<m_active_doc_buff.Size(); i++) {
				SDocument &doc = m_link_set[m_active_doc_buff[i]];

				SLink *curr_ptr = doc.forward_link_ptr;

				while(curr_ptr != NULL) {
					m_link_set[curr_ptr->doc_id].back_buff += curr_ptr->link_weight * doc.trav_prob;
					curr_ptr = curr_ptr->next_ptr;
				}
			}

			float sum = 0;
			for(int i=0; i<m_active_doc_buff.Size(); i++) {
				SDocument &doc = m_link_set[m_active_doc_buff[i]];
				doc.trav_prob += doc.back_buff;
				sum += doc.trav_prob;
				doc.back_buff = 0;
			}

			for(int i=0; i<m_active_doc_buff.Size(); i++) {
				m_link_set[m_active_doc_buff[i]].trav_prob /= sum;

			}
		}

		SRankDoc rank_doc;
		m_doc_queue.Reset();
		for(int i=0; i<m_active_doc_buff.Size(); i++) {
			SDocument &doc = m_link_set[m_active_doc_buff[i]];

			if(doc.doc_ptr->is_added == false && doc.trav_prob > 0.01f) {
				doc.doc_ptr->is_valid = false;
			}
		}
	}

	// This returns the next ranked node
	SDocMatch *NextNode() {

		if(m_doc_queue.Size() == 0) {
			ApproxTravProb(25);
		}

		if(m_doc_queue.Size() == 0) {
			return NULL;
		}

		SRankDoc doc = m_doc_queue.PopItem();	
		doc.doc_ptr->is_added = true;

		if(doc.trav_prob > 0.01) {
			return NextNode();
		}

		return doc.doc_ptr;
	}

	// This normalizes a link set
	void NormalizeLinkSet(int doc_id) {

		float sum = 0;
		SLink *curr_ptr = m_link_set[doc_id].forward_link_ptr;

		while(curr_ptr != NULL) {
			sum += curr_ptr->link_weight;
			curr_ptr = curr_ptr->next_ptr;
		}

		curr_ptr = m_link_set[doc_id].forward_link_ptr;

		while(curr_ptr != NULL) {
			curr_ptr->link_weight /= sum;
			curr_ptr = curr_ptr->next_ptr;
		}
	}
};
CHashDictionary<int> CExpRew::m_doc_map;
CArrayList<CExpRew::SDocument> CExpRew::m_link_set;