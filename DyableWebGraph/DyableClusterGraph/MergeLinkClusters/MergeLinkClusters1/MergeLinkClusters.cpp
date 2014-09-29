#include "../../../../MapReduce.h"

// This stores all the accumulated s_links that are remapped
// at every level of the hiearchy
const char *ACC_S_LINK_DIR = "GlobalData/SummaryLinks/acc_s_links";
// This stores all the mapped s_link nodes
const char *SLINK_NODE_DIR = "GlobalData/SummaryLinks/s_link_node_set";

// This is used to merge existing link clusters together based upon the 
// cluster node mapping generated on this pass. Links that are subsumed
// are removed from the link set and added to the self traversal 
// probability of the node. An important aspect of merging link clusters
// is the detection of links that have been merged together. In this
// case the traversal proability of the merged links need to be added
// together. A single link replaces all the merged links. All the merged
// links are added as a summary link to the LinkCluster class. Also the
// creation level of the new merged link needs to be recorded. The creation
// level just refers to the level at which a link has come into existence.

// The detection of merged links is accomplished through the use of a hash
// map. The hash map just stores all the dst nodes for merged link clusters
// with the same src id. Dst nodes are added to their hash division in 
// sorted order. It's possible to detect duplicate dst nodes by just running
// through a occupied hash division and look for duplicate dst node id's.
class CMergeLinkClusters : public CNodeStat {

	// This defines the hash set size
	static const int HASH_SET_SIZE = 100000;

	// This stores one of the links in the set grouped by src node
	struct SClusterSet {
		// This stores the c_link
		SClusterLink c_link;
		// This stores the merged link weight
		// This stores a ptr to the next linked node
		SClusterSet *next_ptr;
	};

	// This stores one of the nodes in the hash dictionary
	struct SHashNode {
		// This stores the dst node
		S5Byte dst;
		// This stores the accumulated link weight
		float acc_weight;
		// This is a flag indicating whether or not the link has been merged
		bool is_link_merged;
		// This stores a ptr to the next hash node
		SHashNode *next_ptr;
	};

	// This stores the src node map
	CObjectHashMap<S5Byte> m_src_map;
	// This stores the set of links for each src node
	CArrayList<SClusterSet *> m_src_buff;
	// This stores the set of links
	CLinkedBuffer<SClusterSet> m_link_set_buff;

	// This stores the hash dictionary for the dst nodes
	CMemoryChunk<SHashNode *> m_dst_hash_map;
	// This stores all the occupied hash divisions
	CArrayList<int> m_occ_hash_div;
	// This stores the set of hash nodes
	CLinkedBuffer<SHashNode> m_hash_node_buff;

	// This stores the new link set
	CHDFSFile m_curr_link_set;
	// This stores the current links that are being added on
	// a given level of the hiearchy
	CHDFSFile m_curr_s_link_file;

	// This stores the set of base binary links
	CSegFile m_base_bin_links;
	// This stores the set of cluster binary links
	CSegFile m_clus_bin_links;
	// This stores the s_link node set on a given level of the hiearchy
	CSegFile m_s_link_node_file;

	// This stores the set of neighbour nodes that compose 
	// the src and dst nodes of all binary links
	CFileSet<CHDFSFile> m_neighbour_node_set;
	// This stores the node boundary for each each hash division
	CMemoryChunk<_int64> m_div_node_bound;

	// This stores the current level being processed
	int m_curr_level;
	// This storest the new number of links in the link set
	_int64 m_new_link_num;

	// This defines a function that computes the hash map for an object
	static int HashCode(const S5Byte &item) {
		return (int)S5Byte::Value(item);
	}

	// This defines a function to compare two nodes for equality
	static bool Equal(const S5Byte &arg1, const S5Byte &arg2) {
		return S5Byte::Value(arg1) == S5Byte::Value(arg2);
	}

	// This adds a s_link when it is subsumed. This happens 
	// when the two nodes it joins are merged. 
	// @param w_link - this is one of the summary links 
	//               - for a merged node
	inline void AddSLink(SClusterLink &c_link) {

		static SSummaryLink s_link;
		s_link.src = c_link.base_link.src;
		s_link.dst = c_link.base_link.dst;
		s_link.create_level = c_link.c_level;
		s_link.subsume_level = (uChar)m_curr_level;
		s_link.trav_prob = c_link.link_weight;
		s_link.WriteSLink(m_curr_s_link_file);

		m_s_link_node_file.WriteCompObject(s_link.src);
		m_s_link_node_file.WriteCompObject(s_link.dst);
	}

	// This finds the client set a s_link belongs to.
	// @param node - the node that belongs to the s_link 
	//             - that is being attatched to a client
	int ClientSetID(S5Byte &node) {

		int start = 0; 
		int end = m_div_node_bound.OverflowSize();
		// does a binary search to find the right client
		while(end - start > 1) {
			int mid = (start + end) >> 1;
			if(node.Value() >= m_div_node_bound[mid]) {
				start = mid;
				continue;
			}
			end = mid;
		}

		if(node < m_div_node_bound[start] || node >= m_div_node_bound[start+1]) {
			cout<<"Bound Align Error "<<node.Value();getchar();
		}

		return start;
	}

	// This adds the src and dst node to the neighbour set
	inline void AddLinkToNeighbourSet(SClusterLink &c_link) {

		SBLink &b_link = c_link.cluster_link;
		int src_id = ClientSetID(b_link.src);
		int dst_id = ClientSetID(b_link.dst);

		c_link.WriteLink(m_curr_link_set);
		SClusterLink::WriteBaseLink(m_base_bin_links, c_link);
		SClusterLink::WriteClusterLink(m_clus_bin_links, c_link);

		m_neighbour_node_set.File(src_id).WriteCompObject(b_link.src);
		m_neighbour_node_set.File(dst_id).WriteCompObject(b_link.dst);

		m_new_link_num++;
	}

	// This adds a dst node to the hash map
	// @param c_link - this is the current link being processed
	void AddDstNodeToHashMap(SClusterLink &c_link) {

		SBLink &b_link = c_link.cluster_link;
		int hash = b_link.dst.Value() % HASH_SET_SIZE;

		SHashNode *curr_ptr = m_dst_hash_map[hash];
		while(curr_ptr != NULL) {
			if(curr_ptr->dst == b_link.dst) {
				curr_ptr->acc_weight += c_link.link_weight;
				curr_ptr->is_link_merged = true;
				return;
			}

			curr_ptr = curr_ptr->next_ptr;
		}	

		SHashNode *prev_ptr = m_dst_hash_map[hash];
		m_dst_hash_map[hash] = m_hash_node_buff.ExtendSize(1);
		curr_ptr = m_dst_hash_map[hash];
		curr_ptr->next_ptr = prev_ptr;
		curr_ptr->is_link_merged = false;
		curr_ptr->acc_weight = c_link.link_weight;
		curr_ptr->dst = b_link.dst;

		if(prev_ptr == NULL) {
			m_occ_hash_div.PushBack(hash);
		}
	}

	// This finds a particular dst node in the set
	// @param c_link - this is the current link being processed
	SHashNode *FindDstNodeInHashMap(SClusterLink &c_link) {

		SBLink &b_link = c_link.cluster_link;
		int hash = b_link.dst.Value() % HASH_SET_SIZE;

		SHashNode *curr_ptr = m_dst_hash_map[hash];
		while(curr_ptr != NULL) {
			if(curr_ptr->dst == b_link.dst) {
				return curr_ptr;
			}

			curr_ptr = curr_ptr->next_ptr;
		}	

		return NULL;
	}

	// This looks duplicate dst nodes in a given hash division 
	// to find duplicate links. Any duplicate links are merged 
	// together.  A merged link is handled by summing traversal 
	// probability an adding a summary link to the link set, while 
	// removing all other links that made up the merged link. 
	void AddSummaryLinks() {
	
		for(int i=0; i<m_src_map.Size(); i++) {

			// firsts adds all the links to the set
			SClusterSet *curr_ptr = m_src_buff[i];
			while(curr_ptr != NULL) {
				AddDstNodeToHashMap(curr_ptr->c_link);
				curr_ptr = curr_ptr->next_ptr;
			}

			// next processes all the links in the set
			curr_ptr = m_src_buff[i];
			while(curr_ptr != NULL) {
				SHashNode *hash_ptr = FindDstNodeInHashMap(curr_ptr->c_link);
				SClusterLink &c_link = curr_ptr->c_link;

				if(hash_ptr->is_link_merged == true) {
					// This adds the summary link
					AddSLink(curr_ptr->c_link);

					if(hash_ptr->acc_weight < 0) {
						curr_ptr = curr_ptr->next_ptr;
						continue;
					}

					c_link.base_link = c_link.cluster_link;
					c_link.c_level = (uChar)m_curr_level + 1;
					c_link.link_weight = hash_ptr->acc_weight;
					// This ensures that the link is not used more than once
					hash_ptr->acc_weight = -1.0f;
				} 

				AddLinkToNeighbourSet(c_link);
				curr_ptr = curr_ptr->next_ptr;
			}

			for(int j=0; j<m_occ_hash_div.Size(); j++) {
				m_dst_hash_map[m_occ_hash_div[j]] = NULL;
			}

			m_occ_hash_div.Resize(0);
			m_hash_node_buff.Restart();
		}
	}

	// This processes a given set of link clusters for a hash division.
	// This means grouping link clusters together based on src id.
	// @param link_clus_file - this stores all the link clusters that have
	//                       - been grouped by src id
	void ProcessLinkCluster(CHDFSFile &link_clus_file) {

		SClusterLink c_link;
		while(c_link.ReadLink(link_clus_file)) {

			SBLink &b_link = c_link.cluster_link;
			int id = m_src_map.Put(b_link.src);
			if(m_src_map.AskFoundWord() == false) {
				m_src_buff.PushBack(NULL);
			}

			SClusterSet *prev_ptr = m_src_buff[id];
			SClusterSet *curr_ptr = m_link_set_buff.ExtendSize(1);
			m_src_buff[id] = curr_ptr;
			curr_ptr->c_link = c_link;
			curr_ptr->next_ptr = prev_ptr;

			if(c_link.link_weight < 0) { 
				cout<<"Neg Link";getchar();
			}
		}
	}

	// This initializes the different link sets
	void Initialize() {

		m_new_link_num = 0;
		m_curr_s_link_file.OpenWriteFile(CUtility::ExtendString
			(ACC_S_LINK_DIR, m_curr_level, ".set", 
			CNodeStat::GetClientID() + CNodeStat::GetClientNum()));
		m_s_link_node_file.OpenWriteFile(CUtility::ExtendString
			(SLINK_NODE_DIR, m_curr_level, ".set",
			CNodeStat::GetClientID() + CNodeStat::GetClientNum()));

		m_base_bin_links.OpenWriteFile(CUtility::ExtendString
			("LocalData/base_node_set", GetInstID(), ".set", CNodeStat::GetClientID()));
		m_clus_bin_links.OpenWriteFile(CUtility::ExtendString
			("LocalData/cluster_node_set", GetInstID(), ".set", CNodeStat::GetClientID()));

		m_curr_link_set.OpenWriteFile(CUtility::ExtendString
			("GlobalData/LinkSet/bin_link_set", CNodeStat::GetInstID(),
			".set", CNodeStat::GetClientID()));

		m_neighbour_node_set.OpenWriteFileSet(NeighbourNodeDir(),
			CNodeStat::GetClientNum(), CNodeStat::GetClientID());

		m_div_node_bound.ReadMemoryChunkFromFile("GlobalData/ClusterHiearchy/cluster_boundary");
	}

	// This writes the final link number in this set
	void StoreLinkNum() {

		CHDFSFile link_num_file;
		link_num_file.OpenWriteFile(CUtility::ExtendString
			("LocalData/link_num", CNodeStat::GetInstID(),
			".set", CNodeStat::GetClientID()));
		link_num_file.WriteObject(m_new_link_num);
	}

	// This returns the directory of the neighbour node set 
	char *NeighbourNodeDir() {
		strcpy(CUtility::SecondTempBuffer(), CUtility::ExtendString
			("LocalData/neighbour_node_set", GetInstID(), ".set"));

		return CUtility::SecondTempBuffer();
	}

public:

	CMergeLinkClusters() {
		CHDFSFile::Initialize();
	}
	
	// This is the entry function that merges link clusters together
	// once all nodes in the link cluster have been remapped appropriately.
	// @param curr_level - this is the current level in the hiearchy being processed
	void MergeLinkClusters(int client_id, int client_num, int inst_id, int curr_level) {	

		CNodeStat::SetClientID(client_id);
		CNodeStat::SetClientNum(client_num);
		CNodeStat::SetInstID(inst_id);
		m_curr_level = curr_level;

		Initialize();
		m_src_map.Initialize(HashCode, Equal);
		m_src_buff.Initialize(10000);
		m_link_set_buff.Initialize();
		m_occ_hash_div.Initialize(10000);
		m_dst_hash_map.AllocateMemory(HASH_SET_SIZE, NULL);
		m_hash_node_buff.Initialize();

		CHDFSFile link_clus_file;
		for(int i=0; i<CNodeStat::GetClientNum(); i++) {
			link_clus_file.OpenReadFile(CUtility::ExtendString
				("LocalData/link_clus", CNodeStat::GetInstID(),
				".client", i, ".set", CNodeStat::GetClientID()));

			ProcessLinkCluster(link_clus_file);
		}

		AddSummaryLinks();
		StoreLinkNum();
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);
	int client_num = atoi(argv[2]);
	int inst_id = atoi(argv[3]);
	int curr_level = atoi(argv[4]);
	
	CBeacon::InitializeBeacon(client_id, 5555 + inst_id);

	CMemoryElement<CMergeLinkClusters> set;
	set->MergeLinkClusters(client_id, client_num, inst_id, curr_level);
	set.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();
}