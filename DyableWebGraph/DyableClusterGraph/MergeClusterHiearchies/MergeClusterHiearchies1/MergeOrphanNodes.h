#include "../../../../MapReduce.h"

// This is used to merge orphan nodes together. This is done
// on the basis of travseral probability. That is orphan nodes
// with similar traversal probability are merged together. 
// The number of nodes merged in a given grouped depends on 
// the difference between the lowest and largest traversal 
// probability. Also a cap is placed on the maximum number
// of nodese allowed in a given group.
class CMergeOrphanNodes : public CNodeStat {

	// This stores the traversal probability 
	// and the total node number in a given group
	struct SOrphanNode {
		// This stores the total number of nodes in a group
		int total_node_num;
		// This stores the traversal probability of a given node
		float trav_prob;
		// This stores the reverse mapping once sorted
		int id;
		// This stores the node id
		S5Byte node_id;
	};

	// A ptr to an orphan node
	struct SOrphanNodePtr {
		SOrphanNode *ptr;
	};

	// This is the maximum number of nodes allowed in a given cluster
	int m_max_node_num;
	// This stores the set of orpan nodes
	CLinkedBuffer<SOrphanNode> m_node_set;
	// This stores the set of sorted orphan nodes
	CArray<SOrphanNodePtr> m_orphan_node_ptr;
	// This stores the cluster mapping
	CMemoryChunk<int> m_cluster_map;

	// This is used to sort the set of orphan nodes
	static int CompareOrphanNodes(const SOrphanNodePtr &arg1, const SOrphanNodePtr &arg2) {

		if(arg1.ptr->trav_prob < arg2.ptr->trav_prob) {
			return 1;
		}

		if(arg1.ptr->trav_prob > arg2.ptr->trav_prob) {
			return -1;
		}

		return 0;
	}

	// This sorts the orphan nodes by their traversal probability
	void SortOrphanNodes() {

		SOrphanNode *ptr;
		m_node_set.ResetPath();
		m_orphan_node_ptr.Initialize(m_node_set.Size());
		while((ptr = m_node_set.NextNode()) != NULL) {
			m_orphan_node_ptr.ExtendSize(1);
			m_orphan_node_ptr.LastElement().ptr = ptr;
		}

		m_cluster_map.AllocateMemory(m_node_set.Size());
		CSort<SOrphanNodePtr> sort(m_node_set.Size(), CompareOrphanNodes);
		sort.HybridSort(m_orphan_node_ptr.Buffer());
	}

public:

	CMergeOrphanNodes() {
		m_node_set.Initialize();
	}

	// This sets the maximum number of nodes allowed in a given cluster
	inline void SetMaxClusNodeNum(int max_node_num) {
		m_max_node_num = max_node_num;
	}

	// This adds an orphan node
	void AddOrphanNode(SHiearchyStat &stat) {
		SOrphanNode *ptr = m_node_set.ExtendSize(1);
		ptr->node_id = stat.clus_id;
		ptr->total_node_num = stat.total_node_num;
		ptr->trav_prob = stat.pulse_score;
		ptr->id = m_node_set.Size() - 1;
	}

	// This creates the cluster mapping for the orphan nodes
	// @param max_child_num - the maximum number of nodes that can be joined
	//                      - at any one time
	int CreateClusterMapping(int max_child_num) {

		if(m_node_set.Size() == 0) {
			return 0;
		}

		SortOrphanNodes();

		SOrphanNode *ptr;
		int total_clus_num = 0;
		int curr_group_num = 0;
		int cluster_num = CNodeStat::GetClientID() + 1;
		int total_node_num = 0;

		for(int i=0; i<m_orphan_node_ptr.Size(); i++) {
			ptr = m_orphan_node_ptr[i].ptr;

			curr_group_num++;
			total_node_num += ptr->total_node_num;
			if(total_node_num > m_max_node_num || curr_group_num > max_child_num) {
				curr_group_num = 0;
				total_node_num = 0;
				cluster_num += CNodeStat::GetClientNum();
				total_clus_num++;
			}

			m_cluster_map[ptr->id] = cluster_num;
		}

		return total_clus_num + 1;
	}

	// This returns the cluster mapping for a given node
	inline int ClusterMapping(int id) {
		return m_cluster_map[id];
	}
};