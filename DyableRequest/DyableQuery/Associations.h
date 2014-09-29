#include "./KeywordSet.h"

// This stores a given association
struct SAssoc {
	// This stores the association id
	S5Byte id;
	// This stores the number of times a given association
	// id occurs among multiple query terms
	uChar inst_num;
	// This stores the occurrence of the association
	float weight;
};

struct SAssocPtr {
	SAssoc *ptr;
};


// This class is responsible for storing associations for each term.
// Once all associations have been retrieved the top N associations
// must be selected. The top N associations are then supplemented
// into the set of search terms to be used when retrieving search results.
class CAssociations {

	// This is used to find the number of instances of a given association
	CHashDictionary<int> m_assoc_map;
	// This stores the set of associations
	static CArrayList<SAssoc> m_assoc_buff;
	// This is used to find the set of top ranking associations
	static CLimitedPQ<SAssocPtr> m_assoc_queue;

	// This is used to compare associations
	static int CompareAssoc(const SAssocPtr &arg1, const SAssocPtr &arg2) {

		if(arg1.ptr->inst_num < arg2.ptr->inst_num) {
			return -1;
		}

		if(arg1.ptr->inst_num > arg2.ptr->inst_num) {
			return 1;
		}

		if(arg1.ptr->weight < arg2.ptr->weight) {
			return -1;
		}

		if(arg1.ptr->weight > arg2.ptr->weight) {
			return 1;
		}

		return 0;
	}


public:

	CAssociations() {
		m_assoc_map.Initialize(1000);
		m_assoc_buff.Initialize(512);
		m_assoc_queue.Initialize(1024, CompareAssoc);
	}

	// This adds the set of associations to the set
	void AddAssociations(COpenConnection &conn) {

		static u_short num;
		static SAssoc assoc;
		assoc.inst_num = 0;
		conn.Receive((char *)&num, sizeof(u_short));

		for(int i=0; i<num; i++) {
			conn.Receive((char *)&assoc.id, sizeof(S5Byte));
			conn.Receive((char *)&assoc.weight, sizeof(float));

			int id = m_assoc_map.AddWord((char *)&assoc.id, sizeof(S5Byte));
			if(m_assoc_map.AskFoundWord() == false) {
				m_assoc_buff.PushBack(assoc);
			}
				
			m_assoc_buff[id].weight = max(m_assoc_buff[id].weight, assoc.weight);
			m_assoc_buff[id].inst_num++;
		}
	}

	// This adds the associated terms to the keyword set
	static void AddAssociatedTermsToKeywordSet(int num = 0) {

		if(num == 0) {
			return;
		}

		SAssocPtr ptr;
		m_assoc_queue.Initialize(num, CompareAssoc);
		for(int i=0; i<m_assoc_buff.Size(); i++) {
			ptr.ptr = &m_assoc_buff[i];
			m_assoc_queue.AddItem(ptr);
		}

		while(m_assoc_queue.Size() > 0) {
			ptr = m_assoc_queue.PopItem();
			CKeywordSet::AddAdditionalDisplayKeyword(ptr.ptr->id);
		}
	}

	// This ranks the final set of associations and adds them to the term set
	static void CompileAssociations(CArray<SAssocPtr> &assoc_buff) {

		SAssocPtr ptr;
		for(int i=0; i<m_assoc_buff.Size(); i++) {
			ptr.ptr = &m_assoc_buff[i];
			m_assoc_queue.AddItem(ptr);
		}

		assoc_buff.Initialize(m_assoc_queue.Size());
		m_assoc_queue.CopyNodesToBuffer(assoc_buff);
		m_assoc_queue.Reset();
	}
};
CArrayList<SAssoc> CAssociations::m_assoc_buff;
CLimitedPQ<SAssocPtr> CAssociations::m_assoc_queue;