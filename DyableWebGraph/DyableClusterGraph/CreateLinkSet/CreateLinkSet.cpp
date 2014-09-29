#include "../../../MapReduce.h"

// This defines the directory where the final hashed cluster
// link is stored after processing
const char *FIN_LINK_SET_DIR = "GlobalData/LinkSet/hash_link_set";

// This is used once to create the link set initially for each client.
// Due to the way reduce link works it's necessary to store the traversal
// probability explicity for each dst node. This is so link weights can
// be superimposed when links are merged. Also there is no continuous
// node spectrum assigned to a client. Instead hashing is used to hash
// nodes to individual clients.	When the link set is updated and nodes
// removed, then nodes are further hashed on their new cluster assignment.
// The link set starts off with the initial base nodes but these are 
// later replaced with their hiearchial cluster assignments.

// Before the link set can be processed it must be broken up into a binary link
// set and any duplicate links must be handled. A duplicate link is the same 
// link that exists between any two nodes. Duplicate links are merged together
// and the individual traversal probabilities added together.
class CCreateLinkSet : public CNodeStat {

	// This defines the hash breadth to use for grouping 
	// dst nodes together
	static const int HASH_BREADTH = 3333333;

	// This is used to all dst nodes that have been hashed to
	// a particular bucket to look for merged links.
	struct SHashLinkNode {
		// This stores a pointer to the next node
		SHashLinkNode *next_ptr;
		// This stores the accumulative link weight for this link
		float acc_link_weight;
		// This stores the dst node that's been hashed
		S5Byte dst;
		// This is a predicate indicating a keyword link
		uChar is_keyword_link;

		// This adds a new dst node to this hash division
		// @param node - this is one of the dst nodes
		// @param link_weight - this is the traversal probability of the link
		bool IsDuplicate(S5Byte &node, float link_weight, bool is_keyword_link) {

			SHashLinkNode *curr_ptr = this;
			while(curr_ptr != NULL) {
				if(node == curr_ptr->dst) {
					curr_ptr->acc_link_weight += link_weight;
					curr_ptr->is_keyword_link |= is_keyword_link;
					return true;
				}
				curr_ptr = curr_ptr->next_ptr;
			}

			return false;
		}
	};

	// This stores the set of neighbour nodes that compose 
	// the src and dst nodes of all binary links
	CFileSet<CHDFSFile> m_neighbour_node_set;
	// This is a buffer used to store all the merged link nodes
	CLinkedBuffer<SHashLinkNode> m_dst_node_buff;
	// This is the hash map used to group dst nodes together
	CMemoryChunk<SHashLinkNode *> m_hash_node_buff;
	// This stores all the occupied hash divisions
	CArrayList<int> m_occ_hash_div;
	// This stores the net link weight
	float m_net_link_weight;
	// This stores the total number of links in the set 
	_int64 m_link_num;

	// This stores the set of keyword pulse rank scores
	CHDFSFile m_key_pulse_score_file;
	// This stores the set of webgraph pulse rank scores
	CHDFSFile m_web_pulse_score_file;

	// This stores the set of keyword links
	CHDFSFile m_keyword_link_set;
	// This stores the set of webgraph links
	CHDFSFile m_webgraph_link_set;

	// This stores the new link set
	CHDFSFile m_curr_link_set;
	// This stores the set of base binary links
	CSegFile m_base_bin_links;
	// This stores the set of cluster binary links
	CSegFile m_clus_bin_links;

	// This writes the final link number in this set
	void StoreLinkNum() {

		CHDFSFile link_num_file;
		link_num_file.OpenWriteFile(CUtility::ExtendString
			("LocalData/link_num", CNodeStat::GetInstID(),
			".set", CNodeStat::GetClientID()));
		link_num_file.WriteObject(m_link_num);
	}

	// This adds one of the c_links to the set
	// @param w_link - the current link being processed
	// @param c_link - the current link being processed
	void AddCLink(SWLink &w_link, SClusterLink &c_link) {

		c_link.WriteLink(m_curr_link_set);

		SClusterLink::WriteClusterLink(m_clus_bin_links, c_link);
		SClusterLink::WriteBaseLink(m_base_bin_links, c_link);

		int hash = w_link.src.Value() % GetHashDivNum();
		m_neighbour_node_set.File(hash).WriteCompObject(w_link.src);

		hash = w_link.dst.Value() % GetHashDivNum();
		m_neighbour_node_set.File(hash).WriteCompObject(w_link.dst);

		m_link_num++;
	}

	// This is called to perform the initial merging of links. That is 
	// it's possible that more than a single link exists between any 
	// two nodes. In this case the links need to be merged and the 
	// link weight updated. 
	// @param src_node - this is the src node for the link set
	// @param node_trav_prob - this is the traversal probability of the src node
	void MergeLinkWeights(S5Byte &src_node, float node_trav_prob) {

		static SWLink w_link;
		static SClusterLink c_link;
		c_link.c_level = 0;
		
		for(int i=0; i<m_occ_hash_div.Size(); i++) {
			SHashLinkNode *curr_ptr = m_hash_node_buff[m_occ_hash_div[i]];

			while(curr_ptr != NULL) {
				w_link.src = src_node;
				w_link.dst = curr_ptr->dst;
				w_link.link_weight = curr_ptr->acc_link_weight / m_net_link_weight;
				w_link.link_weight *= node_trav_prob;

				int hash = w_link.src.Value() % GetHashDivNum();
				if(hash != GetClientID()) {
					cout<<"hash mismatch "<<hash;getchar();
				}

				c_link.base_link = w_link;
				c_link.cluster_link = w_link;
				c_link.link_weight = w_link.link_weight;

				AddCLink(w_link, c_link);
				curr_ptr = curr_ptr->next_ptr;
			}

			m_hash_node_buff[m_occ_hash_div[i]] = NULL;
		}
		
		m_occ_hash_div.Resize(0);
		m_dst_node_buff.Restart();
	}

	// This adds a link to the hash buffer in order to look for 
	// duplicated links. The link weight for duplicated links is accumulated.
	// @param w_link - this is one of the binary links in the link set
	// @param is_keyword_link - true if a keyword link is stored
	inline void AddLink(SWLink &w_link, bool is_keyword_link) {

		int hash = (int)(w_link.dst.Value() % HASH_BREADTH);
		if(m_hash_node_buff[hash] == NULL) {
			m_occ_hash_div.PushBack(hash);
			m_hash_node_buff[hash] = m_dst_node_buff.ExtendSize(1);

			SHashLinkNode *curr_ptr = m_hash_node_buff[hash];
			curr_ptr->next_ptr = NULL;
			curr_ptr->acc_link_weight = w_link.link_weight;
			curr_ptr->dst = w_link.dst;
			curr_ptr->is_keyword_link = is_keyword_link;
			return;
		}

		SHashLinkNode *curr_ptr = m_hash_node_buff[hash];
		if(curr_ptr->IsDuplicate(w_link.dst, w_link.link_weight, is_keyword_link)) {
			return;
		}
		
		// adds the link if it's not a duplicate
		SHashLinkNode *prev_ptr = m_hash_node_buff[hash];
		m_hash_node_buff[hash] = m_dst_node_buff.ExtendSize(1);

		curr_ptr = m_hash_node_buff[hash];
		curr_ptr->next_ptr = prev_ptr;
		curr_ptr->dst = w_link.dst;
		curr_ptr->acc_link_weight = w_link.link_weight;
		curr_ptr->is_keyword_link = is_keyword_link;
	}

	// This creates a new link set where the traversal probability
	// of every node in the link set has been attatched. The 
	// traversal probability is just the stationary distribution
	// of the src node divided by the number of outgoing links.
	// @param link_set_file - this is the current link set being processed
	// @param is_keyword_link - true if a keyword link is being processed
	bool ProcessLinkCluster(CHDFSFile &link_set_file, SWLink &w_link, bool is_keyword_link) {

		static uLong cluster_size;
		if(link_set_file.GetEscapedItem(cluster_size) < 0) {
			return false;
		}

		link_set_file.ReadCompObject(w_link.src);

		for(uLong i=0; i<cluster_size; i++) {
			link_set_file.ReadCompObject(w_link.dst);
			link_set_file.ReadCompObject(w_link.link_weight);

			if(w_link.link_weight < 0) {
				cout<<"Neg Link";getchar();
			}

			if(is_keyword_link == false) {
				w_link.link_weight *= 0.1f;
			}

			if(w_link.dst >= CNodeStat::GetBaseNodeNum()) {
				continue;
			}

			AddLink(w_link, is_keyword_link);
			m_net_link_weight += w_link.link_weight;
		}

		return true;
	}

	// This merges the set of keyword links and webgraph links
	// @param key_pulse_map - this is the current keyword pulse map
	void MergeLinkSet(SPulseMap &key_pulse_map) {

		SWLink w_link;
		SPulseMap web_pulse_map;

		while(ProcessLinkCluster(m_webgraph_link_set, w_link, false)) {
			web_pulse_map.ReadPulseMap(m_web_pulse_score_file);

			if(w_link.src != web_pulse_map.node) {
				cout<<"Src Mismatch1";getchar();
			}

			if(key_pulse_map.node == web_pulse_map.node) {
				web_pulse_map.pulse_score += key_pulse_map.pulse_score;

				if(w_link.src != key_pulse_map.node) {
					cout<<"Src Mismatch3";getchar();
				}

				if(!ProcessLinkCluster(m_keyword_link_set, w_link, true)) {
					key_pulse_map.node.SetMaxValue();
				} 

				key_pulse_map.ReadPulseMap(m_key_pulse_score_file);

				if(w_link.src != web_pulse_map.node) {
					cout<<"Src Mismatch2";getchar();
				}
			} 
			
			MergeLinkWeights(w_link.src, web_pulse_map.pulse_score / 2);
			m_net_link_weight = 0;
		}
	}

	// This is the entry function called to create the updated
	// link set with link traversal probabilities added.
	void InitializeLinkSet() {

		m_net_link_weight = 0;
		m_key_pulse_score_file.OpenReadFile(CUtility::ExtendString
			(KEYWORD_PULSE_SCORE_DIR, CNodeStat::GetClientID()));

		m_web_pulse_score_file.OpenReadFile(CUtility::ExtendString
			(WEBGRAPH_PULSE_SCORE_DIR, CNodeStat::GetClientID()));

		m_dst_node_buff.Initialize(1024);
		m_hash_node_buff.AllocateMemory(HASH_BREADTH, NULL);
		m_occ_hash_div.Initialize(10000);

		SPulseMap key_pulse_map;
		key_pulse_map.ReadPulseMap(m_key_pulse_score_file);

		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			cout<<"LinkSet "<<i<<" Out Of "<<CNodeStat::GetClientNum()<<endl;
			m_keyword_link_set.OpenReadFile(CUtility::ExtendString
				("GlobalData/LinkSet/keyword_hash_link_set", 
				i, ".set", CNodeStat::GetClientID()));

			m_webgraph_link_set.OpenReadFile(CUtility::ExtendString
				("GlobalData/LinkSet/webgraph_hash_link_set", 
				i, ".set", CNodeStat::GetClientID()));

			MergeLinkSet(key_pulse_map);
		}

		if(key_pulse_map.ReadPulseMap(m_key_pulse_score_file)) {
			cout<<"still more";getchar();
		}
	}

	// This returns the directory of the neighbour node set 
	char *NeighbourNodeDir() {
		strcpy(CUtility::SecondTempBuffer(), CUtility::ExtendString
			("LocalData/neighbour_node_set", GetInstID(), ".set"));

		return CUtility::SecondTempBuffer();
	}

public:

	CCreateLinkSet() {
		m_link_num = 0;
	}

	// This is the entry function
	void CreateLinkSet(int client_id, int client_num, 
		int hash_div_num, int inst_id, _int64 base_node_num) {

		CNodeStat::SetHashDivNum(hash_div_num);
		CNodeStat::SetClientID(client_id);
		CNodeStat::SetClientNum(client_num);
		CNodeStat::SetInstID(inst_id);
		CNodeStat::SetBaseNodeNum(base_node_num);
		CHDFSFile::Initialize();

		m_neighbour_node_set.OpenWriteFileSet(NeighbourNodeDir(),
			CNodeStat::GetHashDivNum(), CNodeStat::GetClientID());

		m_base_bin_links.OpenWriteFile(CUtility::ExtendString
			("LocalData/base_node_set0.set", CNodeStat::GetClientID()));
		m_clus_bin_links.OpenWriteFile(CUtility::ExtendString
			("LocalData/cluster_node_set0.set", CNodeStat::GetClientID()));

		m_curr_link_set.OpenWriteFile(CUtility::ExtendString
			("GlobalData/LinkSet/bin_link_set0.set", CNodeStat::GetClientID()));

		InitializeLinkSet();
		StoreLinkNum();
	}

};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);
	int client_num = atoi(argv[2]);
	int inst_id = atoi(argv[3]);
	int hash_div_num = atoi(argv[4]);
	_int64 base_node_num = CANConvert::AlphaToNumericLong
		(argv[5], strlen(argv[5]));

	CBeacon::InitializeBeacon(client_id, 5555 + inst_id);
	CMemoryElement<CCreateLinkSet> link;
	link->CreateLinkSet(client_id, client_num, hash_div_num, inst_id, base_node_num);
	link.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();

	return 0;
}