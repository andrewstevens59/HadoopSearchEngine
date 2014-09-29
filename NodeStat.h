#include "./FileStorage.h"

// This defines the directory where the
// cluster hiearchy is stored
const char *CLUSTER_HIEARCHY_DIR = "GlobalData/ClusterHiearchy/cluster_hiearchy";
// This stores the directory of the subsumed traversal
// probability of each node in the set
const char *SUBSUMED_PROB_DIR = "GlobalData/PulseRank/subsumed_pulse";

// This stores the directory of the link set
const char *LINK_SET_DIR = "GlobalData/LinkSet/virtual_link_set";
// This stores the directory of the link set
const char *SUBM_SET_DIR = "GlobalData/LinkSet/submerged_link_set";
// This stores the directory where the link clusters 
// are stored -> in an ABTree
const char *LINK_CLUS_DIR = "GlobalData/LinkCluster";
// This stores the directory where the pulse scores are stored
const char *KEYWORD_PULSE_SCORE_DIR = "GlobalData/PulseRank/keyword_pulse_score";
// This stores the directory where the pulse scores are stored
const char *WEBGRAPH_PULSE_SCORE_DIR = "GlobalData/PulseRank/webgraph_pulse_score";
// This is the directory that stores all node id's that
// belong to a particular client
const char *CLIENT_NODE_SET_DIR = "LocalData/client_node_set_dir";

// This defines a LocalData structure that stores information relating to the
// lookup offset into various comp buffers. This information is used 
// during retrieval to gain access to different word divisions.
struct SLookupIndex {

	// This defines the number of bytes needed for each lookup word div
	// # clients, # bytes for lookup, # of hit types + doc number
	static const int WORD_DIV_BYTE_SIZE =  (5 * 2) + 5;

	// This stores the offset for each hit type
	struct SHitTypeSet {
		// This stores the hit offset for each of the different hits
		_int64 hit_pos[2];

		SHitTypeSet() {
			for(int i=0; i<2; i++) {
				hit_pos[i] = 0;
			}
		}
	};


	// This stores the number of documents in a word division
	_int64 doc_num;
	// This stores the offset for each type for each client
	SHitTypeSet client_offset;

	SLookupIndex() {
		doc_num = 0;
	}

	// This reads a lookup index block from file
	// @param lookup_index - this is where the lookup information is stored
	// @param word_id - this is the offset into the word division
	void ReadLookupIndex(CCompression &lookup_index, int word_id) {

		SHitPosition hit_pos;
		int byte_offset = word_id * WORD_DIV_BYTE_SIZE;
		hit_pos.comp_block = byte_offset / lookup_index.BufferSize();
		hit_pos.hit_offset = byte_offset % lookup_index.BufferSize();
		lookup_index.SetCurrentHitPosition(hit_pos);
	}

	// This reads a lookup index block from file
	// @param lookup_index - this is where the lookup information is stored
	void ReadLookupIndex(CCompression &lookup_index) {

		lookup_index.NextHit((char *)&doc_num, 5);

		for(int j=0; j<2; j++) {
			// cycles through each of the different hit types 
			lookup_index.NextHit((char *)&client_offset.hit_pos[j], 5);
		}
	}

	// This writes the document count for the word div
	// @param lookup_index - this is where the lookup information is stored
	inline void WriteDocumentCount(CCompression &lookup_index) {
		lookup_index.AskBufferOverflow(WORD_DIV_BYTE_SIZE);
		lookup_index.AddHitItem5Bytes(doc_num);
	}
};

// This just stores two nodes in a link
struct SBLink {
	// stores the src
	S5Byte src;
	// stores the dst
	S5Byte dst;

	SBLink() {
	}

	SBLink(S5Byte &src, S5Byte &dst) {
		this->src = src;
		this->dst = dst;
	}

	// This writes a binary link to file
	template <class X>
	void WriteBinaryLink(X &file) {
		WriteBinaryLink(file, *this);
	}

	// This reads a binary link from file
	template <class X>
	bool ReadBinaryLink(X &file) {
		return ReadBinaryLink(file, *this);
	}

	// This writes a binary link to file
	template <class X>
	static void WriteBinaryLink(X &file, SBLink &b_link) {
		file.AskBufferOverflow(sizeof(S5Byte) << 1);
		file.WriteCompObject(b_link.src);
		file.WriteCompObject(b_link.dst);
	}

	// This reads a binary link from file
	template <class X>
	static bool ReadBinaryLink(X &file, SBLink &b_link) {
		if(!file.ReadCompObject(b_link.src)) {
			return false;
		}

		file.ReadCompObject(b_link.dst);
		return true;
	}

	// This transfers a b_link from one file to another
	template <class X>
	static bool TransferBLink(X &from_file, X &to_file) {
		static SBLink b_link;
		if(!ReadBinaryLink(from_file, b_link)) {
			return false;
		}

		WriteBinaryLink(to_file, b_link);
		return true;
	}
};

// This defines a weighted binary links that joins
// two arbitrary nodes in the graph.
struct SWLink : public SBLink {
	// This is the weight of the link
	float link_weight;

	// This writes the weighted link
	template <class X>
	void WriteWLink(X &file) {
		file.AskBufferOverflow(sizeof(SWLink));
		SBLink::WriteBinaryLink(file);
		file.WriteCompObject(link_weight);
	}

	// This reads the weighted link
	template <class X>
	bool ReadWLink(X &file) {
		if(!SBLink::ReadBinaryLink(file)) {
			return false;
		}

		file.ReadCompObject(link_weight);
		return true;
	}
};

// This stores a cluster mapping between a base node and
// hiearchial cluster assignment
struct SClusterMap {
	// this stores the base_node
	S5Byte base_node;
	// this stores the current cluster
	S5Byte cluster;

	// This reads a cluster mapping from file
	template <class X>
	bool ReadClusterMap(X &file) {
		return ReadClusterMap(file, *this);
	}

	// This reads a cluster mapping from file
	template <class X>
	static bool ReadClusterMap(X &file, SClusterMap &map) {
		if(!file.ReadCompObject(map.base_node)) {
			return false;
		}

		file.ReadCompObject(map.cluster);
		return true;
	}

	// This writes a cluster mapping to file
	template <class X>
	void WriteClusterMap(X &file) {
		WriteClusterMap(file, *this);
	}

	// This writes a cluster mapping to file
	template <class X>
	static void WriteClusterMap(X &file, SClusterMap &map) {
		file.AskBufferOverflow(sizeof(SClusterMap));
		file.WriteCompObject(map.base_node);
		file.WriteCompObject(map.cluster);
	}

	// This transfers a cluster map from one file to another
	template <class X>
	static bool TransferClusterMap(X &from_file, X &to_file) {
		SClusterMap map;
		if(!ReadClusterMap(from_file, map)) {
			return false;
		}

		WriteClusterMap(to_file, map);
		return true;
	}
};

// This is a weighted clustermap that is used for voting as to
// the cluster label for a node
struct SWClusterMap: public SClusterMap {

	// This stores the weight of the cluster map
	float weight;

	// This reads a cluster mapping from file
	template <class X> bool ReadClusterMap(X &file) {
		if(!file.ReadCompObject(base_node)) {
			return false;
		}

		file.ReadCompObject(cluster);
		file.ReadCompObject(weight);
		return true;
	}

	// This writes a cluster mapping to file
	template <class X> void WriteClusterMap(X &file) {
		file.AskBufferOverflow(sizeof(SClusterMap) + sizeof(float));
		file.WriteCompObject(base_node);
		file.WriteCompObject(cluster);
		file.WriteCompObject(weight);
	}
};

// This is used to remap one cluster mapping to another cluster mapping
struct SRemapClusterMap: public SClusterMap {

	// This stores the cluster remap value
	S5Byte cluster_remap;
	// This stores the cluster assignment weight
	float clus_weight;
	// This stores the weight of the cluster map
	float weight;

	// This reads a cluster mapping from file
	template <class X> bool ReadClusterMap(X &file) {
		if(!file.ReadCompObject(base_node)) {
			return false;
		}

		file.ReadCompObject(cluster);
		file.ReadCompObject(cluster_remap);
		file.ReadCompObject(clus_weight);
		file.ReadCompObject(weight);

		return true;
	}

	// This writes a cluster mapping to file
	template <class X> void WriteClusterMap(X &file) {
		file.AskBufferOverflow(sizeof(SClusterMap) + sizeof(float));
		file.WriteCompObject(base_node);
		file.WriteCompObject(cluster);
		file.WriteCompObject(cluster_remap);
		file.WriteCompObject(clus_weight);
		file.WriteCompObject(weight);
	}
};

// This defines a binary link that stores the current cluster mapping
// between two nodes as well as the base level assignment between nodes.
// Base levels refers to the finest granuality of the nodes before any
// clustering is attatched to a node.
struct SClusterLink {

	// This stores the traversal probability of the link
	float link_weight;
	// This stores the creation level of the link
	uChar c_level;

	// This stores the clustered b_link
	SBLink cluster_link;
	// This stores the base b_link
	SBLink base_link;

	// This reads a cluster link from file
	template <class X>
	inline static bool ReadClusterLink(X &file, SClusterLink &link) {
		return link.cluster_link.ReadBinaryLink(file);
	}

	// This writes a cluster link from file
	template <class X>
	inline static void WriteClusterLink(X &file, SClusterLink &link) {
		link.cluster_link.WriteBinaryLink(file);
	}

	// This reads a base link from file
	template <class X>
	inline static bool ReadBaseLink(X &file, SClusterLink &link) {
		return link.base_link.ReadBinaryLink(file);
	}

	// This writes a base link from file
	template <class X>
	inline static void WriteBaseLink(X &file, SClusterLink &link) {
		link.base_link.WriteBinaryLink(file);
	}

	// This reads a link from file
	template <class X>
	bool ReadLink(X &file) {
		return ReadLink(file, *this);
	}

	// This reads a link from file
	template <class X>
	static bool ReadLink(X &file, SClusterLink &link) {

		if(!file.ReadCompObject(link.link_weight)) {
			return false;
		}

		file.ReadCompObject(link.c_level);
		file.ReadCompObject(link.cluster_link.src);
		file.ReadCompObject(link.cluster_link.dst);
		file.ReadCompObject(link.base_link.src);
		file.ReadCompObject(link.base_link.dst);
		return true;
	}

	// This writes a link to file
	template <class X>
	void WriteLink(X &file) {
		WriteLink(file, *this);
	}

	// This writes a link to file
	template <class X>
	static void WriteLink(X &file, SClusterLink &link) {
		
		file.AskBufferOverflow(sizeof(SClusterLink));
		file.WriteCompObject(link.link_weight);
		file.WriteCompObject(link.c_level);
		file.WriteCompObject(link.cluster_link.src);
		file.WriteCompObject(link.cluster_link.dst);
		file.WriteCompObject(link.base_link.src);
		file.WriteCompObject(link.base_link.dst);
	}

	// This transfers a cluster link from one file to another
	template <class X>
	static bool TransferLink(X &from_file, X &to_file) {
		static SClusterLink link;
		if(!ReadLink(from_file, link)) {
			return false;
		}

		WriteLink(to_file, link);
		return true;
	}
};

// This stores statistics relating to the hiearchy. This 
// includes the number of subtrees below the current node
// and the working cluster id of the subtree.
struct SHiearchyStat {
	// Stores the total number stats in the tree
	int total_subtrees;
	// This is the number of nodes in the cluster segment
	int total_node_num;
	// This stores the pulse score of all the subsumed nodes
	float pulse_score;
	// Stores the cluster id
	S5Byte clus_id;

	// Reads a hiearchy stat from file
	template <class X>
	bool ReadHiearchyStat(X &file) {
		return ReadHiearchyStat(file, *this);
	}

	// Reads a hiearchy stat from file
	template <class X>
	static bool ReadHiearchyStat(X &file,
		SHiearchyStat &stat) {

		if(!file.ReadCompObject(stat.clus_id)) {
			return false;
		}

		file.ReadCompObject(stat.total_subtrees);
		file.ReadCompObject(stat.total_node_num);
		file.ReadCompObject(stat.pulse_score);
		return true;
	}

	// Writes a hiearchy stat to file
	template <class	X>
	void WriteHiearchyStat(X &file) {
		WriteHiearchyStat(file, *this);
	}

	// Writes a hiearchy stat to file
	template <class	X>
	static void WriteHiearchyStat(X &file, SHiearchyStat &stat) {

		file.AskBufferOverflow(sizeof(SHiearchyStat));
		file.WriteCompObject(stat.clus_id);
		file.WriteCompObject(stat.total_subtrees);
		file.WriteCompObject(stat.total_node_num);
		file.WriteCompObject(stat.pulse_score);
	}

	// This transfers a hiearchy stat from one file to another
	template <class	X>
	static bool TransferHiearchyStat(X &from_file, X &to_file) {
		static SHiearchyStat stat;
		if(!ReadHiearchyStat(from_file, stat)) {
			return false;
		}

		WriteHiearchyStat(to_file, stat);
		return true;
	}
};

// This stores one of the possible summary links
struct SSummaryLink {
	// This stores the src node for a summary link
	S5Byte src;
	// This stores the dst node for a summary link
	S5Byte dst;
	// This stores the level at which a link is created
	uChar create_level;
	// This stores the level at which a link is subsumed
	uChar subsume_level;
	// This stores the traversal probability of 
	// a summary link
	float trav_prob;
	// This is a predicate specifying whether it's 
	// a forward or backward link
	bool is_forward_link;

	// This writes a s_link to file
	template <class X>
	void WriteSLink(X &file) {
		file.WriteCompObject(src);
		file.WriteCompObject(dst);
		file.WriteCompObject(trav_prob);

		subsume_level <<= 1;
		if(is_forward_link) {
			subsume_level |= 0x01;
		}

		file.WriteCompObject(subsume_level);
		file.WriteCompObject(create_level);
		subsume_level >>= 1;
	}

	// This writes a s_link to file
	template <class X>
	void WriteReducedSLink(X &file) {
		file.WriteCompObject(src);
		file.WriteCompObject(dst);
		file.WriteCompObject(trav_prob);

		create_level <<= 1;
		if(is_forward_link == true) {
			create_level |= 0x01;
		}

		file.WriteCompObject(create_level);
		create_level >>= 1;
	}

	// This reads a s_link to file
	template <class X>
	bool ReadReducedSLink(X &file) {
		if(!file.ReadCompObject(src)) {
			return false;
		}
		file.ReadCompObject(dst);
		file.ReadCompObject(trav_prob);
		file.ReadCompObject(create_level);

		is_forward_link = (create_level & 0x01) == 0x01;
		create_level >>= 1;
		return true;
	}

	// This reads a s_link from a buffer
	// @param buff - this is where the s_link is stored
	// @return the number of bytes used to store the reduced s_link
	int ReadReducedSLink(char buff[]) {
		memcpy((char *)&src, buff, 5);
		memcpy((char *)&dst, buff + 5, 5);
		memcpy((char *)&trav_prob, buff + 10, sizeof(float));
		memcpy((char *)&create_level, buff + 14, sizeof(uChar));

		is_forward_link = (create_level & 0x01) == 0x01;
		create_level >>= 1;

		return 15;
	}

	// This reads a s_link from file
	template <class X>
	bool ReadSLink(X &file) {
		if(!file.ReadCompObject(src)) {
			return false;
		}

		file.ReadCompObject(dst);
		file.ReadCompObject(trav_prob);

		file.ReadCompObject(subsume_level);
		file.ReadCompObject(create_level);

		is_forward_link = (subsume_level & 0x01) == 0x01;
		subsume_level >>= 1;
		return true;
	}
};

// This stores the number of children and the total 
// number of nodes under this one
struct SSubTree {
	// This stores the number of children
	uLong child_num;
	// This stores the total number of nodes
	uLong node_num;
	// This stores the net traversal probability
	float net_trav_prob;
	// This stores a ptr to the next stat in 
	// a subtree of the hiearchy
	SSubTree *next_ptr;

	void Initialize() {
		node_num = 0;
		child_num = 0;
		net_trav_prob = 0;
	}

	// This adds a subtree to the linked list
	inline void AddSubtree(SSubTree &subtree) {
		child_num = subtree.child_num;
		node_num = subtree.node_num;
		net_trav_prob = subtree.net_trav_prob;
	}

	// This reads a stat from file
	template <class X>
	bool ReadSubTree(X &file) {
		return ReadSubTree(file, *this);
	}

	// This writes a stat to file
	template <class X>
	void WriteSubTree(X &file) {
		WriteSubTree(file, *this);
	}

	// This reads a stat from file
	static bool ReadSubTree(CFileComp &file, SSubTree &subtree) {
		if(file.GetEscapedItem(subtree.child_num) < 0) {
			return false;
		}

		file.GetEscapedItem(subtree.node_num);
		file.ReadCompObject(subtree.net_trav_prob);
		return true;
	}

	// This writes a stat to file
	static void WriteSubTree(CFileComp &file, SSubTree &subtree) {
		file.AddEscapedItem(subtree.child_num);
		file.AddEscapedItem(subtree.node_num);
		file.WriteCompObject(subtree.net_trav_prob);
	}

	// This reads a stat from file
	template <class X>
	static bool ReadSubTree(X &file, SSubTree &subtree) {
		if(file.GetEscapedItem(subtree.child_num) < 0) {
			return false;
		}

		file.GetEscapedItem(subtree.node_num);
		file.ReadCompObject(subtree.net_trav_prob);
		return true;
	}

	// This writes a stat to file
	template <class X>
	static void WriteSubTree(X &file, SSubTree &subtree) {
		file.AddEscapedItem(subtree.child_num);
		file.AddEscapedItem(subtree.node_num);
		file.WriteCompObject(subtree.net_trav_prob);
	}
};

// This stores information relating to an ABNode in the hiearchy
struct SABNode {
	// stores the number of children
	uLong child_num;
	// stores the total number of s_links
	uLong s_link_num;
	// stores the total number of nodes in the region
	uLong total_node_num;
	// stores the traversal probability of the node
	float trav_prob;
	// stores the tree level of the node
	uChar level;
	// stores the number of bytes that make up the header
	uChar header_byte_num;

	// This resets the node
	void Reset() {
		child_num = 0;
		total_node_num = 0;
		trav_prob = 0;
	}

	// This reads an ab_node from file
	template <class X> 
	bool ReadABNode(X &file) {
		if(!file.ReadCompObject(trav_prob)) {
			return false;
		}

		file.ReadCompObject(level);
		file.GetEscapedItem(s_link_num);
		file.GetEscapedItem(child_num);
		file.GetEscapedItem(total_node_num);
		return true;
	}

	// This creats an ab_node from a byte buffer
	// @param buff - this is the buffer that contains
	//             - the ab node
	// @return the number of bytes that compose the ab_node
	int ReadABNode(char buff[]) {

		char *start_ptr = buff;
		memcpy((char *)&trav_prob, buff, 4);
		memcpy((char *)&level, buff, 1);
		level = (uChar)buff[4];
		buff += 5;
				
		s_link_num = 0; 
		buff += CArray<char>::GetEscapedItem(buff, (char *)&s_link_num);

		child_num = 0; 
		buff += CArray<char>::GetEscapedItem(buff, (char *)&child_num);

		total_node_num = 0; 
		buff += CArray<char>::GetEscapedItem(buff, (char *)&total_node_num);
		
		header_byte_num = (int)(buff - start_ptr);
		return (int)header_byte_num;
	}

	// This writes a ab_node to file
	template <class X> 
	void WriteABNode(X &file) {
		file.WriteCompObject(trav_prob);
		file.WriteCompObject(level);
		file.AddEscapedItem(s_link_num);
		file.AddEscapedItem(child_num);
		file.AddEscapedItem(total_node_num);
	}
};

// This defines the discrete version of the pulse score
// where the traversal probability is encoded as an 8-bit integer
struct S8BitPulseMap {
	// This stores the node
	S5Byte node;
	// This stores the 8-bit version of the pulse score
	uChar pulse_score;

	// This reads a pulse map from file
	template <class X>
	bool Read8BitPulseMap(X &file) {
		if(!file.ReadCompObject(node)) {
			return false;
		}

		file.ReadCompObject(pulse_score);
		return true;
	}

	// This writes a pulse map from file
	template <class X>
	void Write8BitPulseMap(X &file) {
		file.WriteCompObject(node);
		file.WriteCompObject(pulse_score);
	}
};

// This stores the mapping between a node and pulse score
struct SPulseMap {
	// This stores the node
	S5Byte node;
	// This stores the pulse score
	float pulse_score;

	// This reads a pulse map from file
	template <class X>
	bool ReadPulseMap(X &file) {
		return ReadPulseMap(file, *this);
	}

	// This reads a pulse map from file
	template <class X>
	static bool ReadPulseMap(X &file, SPulseMap &pulse_map) {
		if(!file.ReadCompObject(pulse_map.node)) {
			return false;
		}

		file.ReadCompObject(pulse_map.pulse_score);
		return true;
	}

	// This writes a pulse map to file
	template <class X>
	void WritePulseMap(X &file) {
		WritePulseMap(file, *this);
	}

	// This writes a pulse map to file
	template <class X>
	static void WritePulseMap(X &file, SPulseMap &pulse_map) {
		file.AskBufferOverflow(sizeof(S5Byte) + sizeof(float));
		file.WriteCompObject(pulse_map.node);
		file.WriteCompObject(pulse_map.pulse_score);
	}

	// This transfers a pulse map from one file to another
	template <class X>
	static bool TransferPulseMap(X &from_file, X &to_file) {

		static SPulseMap map;
		if(!SPulseMap::ReadPulseMap(from_file, map)) {
			return false;
		}

		SPulseMap::WritePulseMap(to_file, map);
		return true;
	}
};

// This stores various statistics about the number of nodes and links 
// in the set. This is used for creating the hiearchy and is also
// used to help synchronize the different clients.
class CNodeStat {

	// stores the number of nodes and links
	struct SNodeLinkStat {
		_int64 m_node_num;
		_int64 m_link_num;
	};
	
	// defines a 64-bit boundary 
	struct SClusBoundary {
		_int64 start;
		_int64 end;
	};

	// stores the current number of nodes
	// and links considering the current level hiearchy
	static SNodeLinkStat m_curr_stat;
	// stores the total number of nodes 
	// and links at the base level of the hiearchy
	static SNodeLinkStat m_global_stat;
	// stores the number of base nodes (spidered nodes)
	static _int64 m_base_node_num;
	// stores the boundary of cluster assignments given to 
	// this client 
	static SClusBoundary m_cluster_bound;

	// stores the local process client id
	static int m_client_id;
	// stores the number of clients 
	static int m_client_num;
	// stores the total number of instances of a graph partition
	static int m_inst_num;
	// stores an instance of the graph being processed
	static int m_inst_id;
	// stores the number of hash divisions
	static int m_hash_div_num;
	
	// stores the number of levels in the hiearchy
	static int m_hiearchy_level_num;

	// stores the previous number of clients
	static int m_prev_client_num;


public:

	CNodeStat() {
	}

	// This is called only once to initialize the global node count.
	// Stats have been found from compiling the dictionaries in hitlist
	static void InitializeNodeLinkStat(const char str[]) {

		CHDFSFile file;
		_int64 base_url_num;
		_int64 ext_url_num;
		file.OpenReadFile(str);

		// reads the number of base nodes
		file.ReadCompObject(base_url_num);
		// reads the total number of links
		file.ReadCompObject(ext_url_num);
		SetGlobalNodeNum(ext_url_num);
		SetBaseNodeNum(base_url_num);
		SetCurrNodeNum(ext_url_num);
	}

	// This loads in node statistics relating to 
	// the cluster hiearchy
	// @param str - director name
	static void LoadNodeLinkStat(const char str[]) {
		CHDFSFile file;
		file.OpenReadFile(str);
		file.ReadObject(m_curr_stat);
		file.ReadObject(m_global_stat);
		file.ReadObject(m_base_node_num);

		file.ReadObject(m_cluster_bound);
		file.ReadObject(m_hiearchy_level_num);
	}

	// only need to call this function
	// if clustering needs to begin from
	// a previous location
	inline void LoadNodeLinkStat() {
		LoadNodeLinkStat("GlobalData/ClusterHiearchy/stat");
	}

	// number of links currently in the set
	inline static void SetCurrLinkNum(const _int64 &link_num) {
		m_curr_stat.m_link_num = link_num;
	}
	// number of nodes currently in the set
	inline static void SetCurrNodeNum(const _int64 &node_num) {
		m_curr_stat.m_node_num = node_num;
	}
	// number of links currently in the set
	inline static _int64 GetCurrLinkNum() {
		return m_curr_stat.m_link_num;
	}
	// number of nodes currently in the set
	inline static _int64 GetCurrNodeNum() {
		return m_curr_stat.m_node_num;
	}

	// number of links currently in the set
	inline static void SetGlobalLinkNum(const _int64 &link_num) {
		m_global_stat.m_link_num = link_num;
	}
	// number of nodes currently in the set
	inline static void SetGlobalNodeNum(const _int64 &node_num) {
		m_global_stat.m_node_num = node_num;
	}
	// number of links currently in the set
	inline static _int64 GetGlobalLinkNum() {
		return m_global_stat.m_link_num;
	}
	// number of nodes currently in the set
	inline static _int64 GetGlobalNodeNum() {
		return m_global_stat.m_node_num;
	}

	// gets the number of base nodes
	inline static _int64 GetBaseNodeNum() {
		return m_base_node_num;
	}
	// sets the number of base nodes
	inline static void SetBaseNodeNum(const _int64 &base_node_num) {
		m_base_node_num = base_node_num;
	}

	// sets the number of clients
	inline static void SetClientNum(int client_num) {
		m_client_num = client_num;
	}
	// gets the number of clients
	inline static int GetClientNum() {
		return m_client_num;
	}

	// sets the processe client id
	inline static void SetClientID(int client_id) {
		m_client_id = client_id;
	}
	// gets the process client id
	inline static int GetClientID() {
		return m_client_id;
	}

	// sets the total number of graph instances
	inline static void SetInstNum(int inst_num) {
		m_inst_num = inst_num;
	}
	// sets the total number of graph instances
	inline static int GetInstNum() {
		return m_inst_num;
	}

	// sets the total number of graph instances
	inline static void SetInstID(int inst_id) {
		m_inst_id = inst_id;
	}
	// sets the total number of graph instances
	inline static int GetInstID() {
		return m_inst_id;
	}

	// sets the total number of hash divisions
	inline static void SetHashDivNum(int hash_div_num) {
		m_hash_div_num = hash_div_num;
	}
	// sets the total number of graph instances
	inline static int GetHashDivNum() {
		return m_hash_div_num;
	}

	// gets the cluster boundary for this client
	inline static void GetClusterBoundary(_int64 &start, _int64 &end) {
		start = m_cluster_bound.start;
		end = m_cluster_bound.end;
	}

	// sets the cluster boundary for this client
	inline static void SetClusterBoundary(_int64 start, _int64 end) {
		m_cluster_bound.start = start;
		m_cluster_bound.end = end;
	}

	// returns the start of the cluster boundary
	inline static _int64 ClusterBoundOffset() {
		return m_cluster_bound.start;
	}
	
	// returns the with of the the cluster boundary
	inline static _int64 ClusterBoundWidth() {
		return m_cluster_bound.end - m_cluster_bound.start;
	}

	// sets the number of levels in the hiearchy
	inline static void SetHiearchyLevelNum(int level_num) {
		m_hiearchy_level_num = level_num;
	}

	// gets the number of levels in the hiearchy
	inline static int GetHiearchyLevelNum() {
		return m_hiearchy_level_num;
	}

	// sets the previous client number
	inline static void SetPrevClientNum(int prev_client_num) {
		m_prev_client_num = prev_client_num;
	}

	// gets the previous client number
	inline static int GetPrevClientNum() {
		return m_prev_client_num;
	}

	// writes all the node and link states to file
	void WriteNodeLinkStat(const char str[] = "GlobalData/ClusterHiearchy/stat") {

		CHDFSFile file;
		file.OpenWriteFile(str);

		file.WriteObject(m_curr_stat);
		file.WriteObject(m_global_stat);
		file.WriteObject(m_base_node_num);

		file.WriteObject(m_cluster_bound);
		file.WriteObject(m_hiearchy_level_num);
	}
};

int CNodeStat::m_client_id;
int CNodeStat::m_client_num;
CNodeStat::SNodeLinkStat CNodeStat::m_curr_stat;
CNodeStat::SNodeLinkStat CNodeStat::m_global_stat;
_int64 CNodeStat::m_base_node_num;
int CNodeStat::m_inst_num;
int CNodeStat::m_inst_id;
int CNodeStat::m_hash_div_num;

int CNodeStat::m_hiearchy_level_num;
CNodeStat::SClusBoundary CNodeStat::m_cluster_bound;
int CNodeStat::m_prev_client_num;
