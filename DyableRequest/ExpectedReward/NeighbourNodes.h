#include "./ExpRew.h"

// This is used to assign the neighbourhood score to each document
// in the query term set. The neighbourhood score is the distance
// of a node from the relevant documents of seed nodes.
class CNeighbourNodes {

	// This stores the set of neighbour nodes
	static CArrayList<SDocMatch *> m_neighbour_buff;
	// This stores the top ranking neighbour nodes
	static CLimitedPQ<SDocMatch *> m_neighbour_queue;

	// This is used to compare neighbour nodes
	static int CompareNeighbourNodes(SDocMatch *const &arg1, SDocMatch *const &arg2) {

		if(arg1->match_score < arg2->match_score) {
			return -1;
		}

		if(arg1->match_score > arg2->match_score) {
			return 1;
		}

		return 0;
	}

public:

	CNeighbourNodes() {
	}

	// This sets up neighbour nodes
	static void Initialize() {
		m_neighbour_queue.Initialize(20, CompareNeighbourNodes);
		m_neighbour_buff.Initialize(1024);
	}

	// This adds a neighbour node
	static void AddNeighbourNode(SDocMatch *doc_ptr) {
		m_neighbour_buff.PushBack(doc_ptr);
	}

    // This updates the neighbourhood score of the closet matching
    // nodes for a given related document to a seed document.
	static void UpdateNeighbourHoodScore(S5Byte &node, uChar weight) {

		int start = 0;
		int end = m_neighbour_buff.Size(); 

		while(end - start > 1) {

			int mid = (end + start) >> 1;

			if(m_neighbour_buff[mid]->doc_id > node) {
				end = mid;
			} else {
				start = mid;
			}
		}

		SDocMatch &pt1 = *m_neighbour_buff[start];

		if(pt1.doc_id == node && weight > 7) {
			pt1.is_valid = false;
			return;
		} 

		pt1.match_score += weight / min(100, abs(pt1.doc_id.Value() - node.Value()) + 1);

		if(start < m_neighbour_buff.Size() - 1) {
			SDocMatch &pt2 = *m_neighbour_buff[start+1];
			pt2.match_score += weight / min(100, abs(pt2.doc_id.Value() - node.Value()) + 1); 
		} else if(start > 0) {
			SDocMatch &pt2 = *m_neighbour_buff[start-1];
			pt2.match_score += weight / min(100, abs(pt2.doc_id.Value() - node.Value()) + 1); 
		}
	}

	// This loads the neighbourhood queue
	static bool LoadQueue() {

		m_neighbour_queue.Reset();

		for(int i=0; i<m_neighbour_buff.Size(); i++) {
			SDocMatch &node = *m_neighbour_buff[i];

			m_neighbour_queue.AddItem(&node);
		}

		return m_neighbour_queue.Size() > 0;
	}

	// This returns the next ranked neighbour node
	static bool NextNode(S5Byte &node) {

		if(m_neighbour_queue.Size() == 0) {
			if(LoadQueue() == false) {
				return false;
			}
		}

		SDocMatch *ptr = m_neighbour_queue.PopItem();
		node = ptr->doc_id;

		return true;
	}
};
CArrayList<SDocMatch *> CNeighbourNodes::m_neighbour_buff;
CLimitedPQ<SDocMatch *> CNeighbourNodes::m_neighbour_queue;