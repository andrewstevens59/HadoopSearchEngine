#include "../../../MapReduce.h"

// This takes the set of merged binary links are 
// creates a clustered link set. 
class CCreateClusteredLinkSet : public CNodeStat {

	// This defines the hash breadth to use for grouping 
	// dst nodes together
	static const int HASH_BREADTH = 0xFFFFFF;

	// Used for sorting
	struct SWLinkPtr {
		SWLink *ptr;
	};

	// This is used to all dst nodes that have been hashed to
	// a particular bucket to look for merged links.
	struct SHashLinkNode {
		// This stores the dst node that's been hashed
		S5Byte dst;
		// This stores the accumulative link weight for this link
		float acc_link_weight;
		// This stores a pointer to the next node
		SHashLinkNode *next_ptr;

		// This adds a new dst node to this hash division
		// @param node - this is one of the dst nodes
		// @param link_weight - this is the traversal probability of the link
		bool IsDuplicate(S5Byte &node, float link_weight) {

			SHashLinkNode *curr_ptr = this;
			while(curr_ptr != NULL) {
				if(node == curr_ptr->dst) {
					curr_ptr->acc_link_weight += link_weight;
					return true;
				}
				curr_ptr = curr_ptr->next_ptr;
			}

			return false;
		}
	};

	// This is a buffer used to store all the merged link nodes
	CLinkedBuffer<SHashLinkNode> m_dst_node_buff;
	// This is the hash map used to group dst nodes together
	CMemoryChunk<SHashLinkNode *> m_hash_node_buff;
	// This stores all the occupied hash divisions
	CArrayList<int> m_occ_hash_div;
	// This stores the set of links in priority order
	CLinkedBuffer<SWLink> m_link_buff;
	// This stores the set of links in priority order
	CArrayList<SWLinkPtr> m_link_ptr;
	// This stores the set of links in priority order
	CLimitedPQ<SWLinkPtr> m_link_queue;

	// This stores the final binary link set
	CHDFSFile m_bin_link_set;
	// This stores the hashed link set once all link clusters
	// have been hashed to seperate bins
	CFileSet<CHDFSFile> m_hash_link_set;
	// This stores all the back pulse rank scores
	CFileSet<CHDFSFile> m_back_set;

	// This is used to compare w_links
	static int CompareWLinks(const SWLinkPtr &arg1, const SWLinkPtr &arg2) {

		if(arg1.ptr->link_weight < arg2.ptr->link_weight) {
			return -1;
		}

		if(arg1.ptr->link_weight > arg2.ptr->link_weight) {
			return 1;
		}

		return 0;
	}

	// This is called to perform the initial merging of links. That is 
	// it's possible that more than a single link exists between any 
	// two nodes. In this case the links need to be merged and the 
	// link weight updated. 
	void MergeLinkWeights() {

		static SWLink w_link;
		static SWLinkPtr ptr;
		
		for(int i=0; i<m_occ_hash_div.Size(); i++) {
			SHashLinkNode *curr_ptr = m_hash_node_buff[m_occ_hash_div[i]];

			while(curr_ptr != NULL) {
				w_link.dst = curr_ptr->dst;
				w_link.link_weight = curr_ptr->acc_link_weight;

				m_link_buff.PushBack(w_link);
				ptr.ptr = &m_link_buff.LastElement();
				m_link_queue.AddItem(ptr);
				curr_ptr = curr_ptr->next_ptr;
			}

			m_hash_node_buff[m_occ_hash_div[i]] = NULL;
		}
		
		m_occ_hash_div.Resize(0);
		m_dst_node_buff.Restart();
	}

	// This adds a link to the hash buffer in order to look for 
	// duplicated links. The link weight for duplicated links is
	// accumulated.
	// @param w_link - this is one of the binary links in the link set
	inline void AddLink(SWLink &w_link) {

		if(w_link.link_weight < 0) {
			// a dummy link used as a placeholder
			return;
		}

		int hash = (int)(w_link.dst.Value() % HASH_BREADTH);
		if(m_hash_node_buff[hash] == NULL) {
			m_occ_hash_div.PushBack(hash);
			m_hash_node_buff[hash] = m_dst_node_buff.ExtendSize(1);

			SHashLinkNode *curr_ptr = m_hash_node_buff[hash];
			curr_ptr->next_ptr = NULL;
			curr_ptr->acc_link_weight = w_link.link_weight;
			curr_ptr->dst = w_link.dst;
			return;
		}

		SHashLinkNode *curr_ptr = m_hash_node_buff[hash];
		if(curr_ptr->IsDuplicate(w_link.dst, w_link.link_weight)) {
			return;
		}

		// adds the link if it's not a duplicate
		SHashLinkNode *prev_ptr = m_hash_node_buff[hash];
		m_hash_node_buff[hash] = m_dst_node_buff.ExtendSize(1);

		curr_ptr = m_hash_node_buff[hash];
		curr_ptr->next_ptr = prev_ptr;
		curr_ptr->dst = w_link.dst;
		curr_ptr->acc_link_weight = w_link.link_weight;
	}

	// This places the cluster link set into different hash bins based 
	// upon the src node. This is so the back buffer can be matched up
	// to the src node for each link cluster.
	// @param src - this is the current src node
	void HashClusterLinkSet(_int64 &src) {

		if(src < 0) {
			return;
		}

		static SPulseMap pulse_map;
		pulse_map.pulse_score = 1.0f / CNodeStat::GetBaseNodeNum();

		m_link_ptr.Resize(0);
		float net_link_weight = 0;
		while(m_link_queue.Size() > 0) {
			SWLinkPtr ptr = m_link_queue.PopItem();
			m_link_ptr.PushBack(ptr);
			net_link_weight += ptr.ptr->link_weight;
		}
	
		int hash = (int)(src % m_hash_link_set.SetNum());
		CHDFSFile &fin_link_set = m_hash_link_set.File(hash);

		fin_link_set.AddEscapedItem(m_link_ptr.Size());
		fin_link_set.WriteCompObject5Byte(src);
	
		pulse_map.node = src;
		pulse_map.WritePulseMap(m_back_set.File(hash));

		if(pulse_map.node >= GetGlobalNodeNum()) {
			cout<<"exceed "<<pulse_map.node.Value();getchar();
		}

		for(int i=0; i<m_link_ptr.Size(); i++) {
			SWLink &w_link = *m_link_ptr[i].ptr;
			w_link.link_weight /= net_link_weight;

			fin_link_set.WriteCompObject(w_link.dst);
			fin_link_set.WriteCompObject(w_link.link_weight);
		}
	}

	// This cycles through the link set and groups nodes
	void CreateLinkClusters() {

		SWLink w_link;
		_int64 curr_src = -1;
		m_dst_node_buff.Initialize(1000);
		m_occ_hash_div.Initialize(1024);
		m_hash_node_buff.AllocateMemory(HASH_BREADTH, NULL);

		while(w_link.ReadWLink(m_bin_link_set)) {

			if(curr_src != w_link.src.Value()) {
				MergeLinkWeights();
				HashClusterLinkSet(curr_src);
				curr_src = w_link.src.Value();
				m_link_buff.Restart();
				m_link_queue.Reset();
			}

			AddLink(w_link);
		}
		
		MergeLinkWeights();
		HashClusterLinkSet(curr_src);
	}


public:

	CCreateClusteredLinkSet() {
		CHDFSFile::Initialize();
	}

	// This takes a cluster link set from the integrated binary link set
	// @param is_keyword_set - true if the keyword set is being used, hence culling
	// @param max_clus_link_num - this is the maximum number of links allowed with culling
	void CreateClusterLinkSet(int client_id, int hash_div_num, 
		bool is_keyword_set, int max_clus_link_num) {

		CNodeStat::SetClientID(client_id);
		CNodeStat::SetClientNum(hash_div_num);
		CNodeStat::SetHashDivNum(hash_div_num);
		CNodeStat::LoadNodeLinkStat("GlobalData/ClusterHiearchy/stat");
		CBeacon::InitializeBeacon(GetClientID());

		m_link_queue.Initialize(max_clus_link_num, CompareWLinks);
		m_link_buff.Initialize(2048);
		m_link_ptr.Initialize(2048);

		m_back_set.OpenWriteFileSet("LocalData/back_wave_pass",
			hash_div_num, GetClientID());

		m_bin_link_set.OpenReadFile(CUtility::ExtendString
			("LocalData/bin_link_file", GetClientID()));

		if(is_keyword_set == true) {
			m_hash_link_set.OpenWriteFileSet("GlobalData/LinkSet/keyword_hash_link_set",
				hash_div_num, GetClientID());
		} else {
			m_hash_link_set.OpenWriteFileSet("GlobalData/LinkSet/webgraph_hash_link_set",
				hash_div_num, GetClientID());
		}

		CreateLinkClusters();
	}

};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);
	int hash_div_num = atoi(argv[2]);
	bool is_keyword_set = atoi(argv[3]);
	int max_clus_link_num = atoi(argv[4]);
	
	CMemoryElement<CCreateClusteredLinkSet> set;
	set->CreateClusterLinkSet(client_id, hash_div_num,
		is_keyword_set, max_clus_link_num);

	set.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();
}
