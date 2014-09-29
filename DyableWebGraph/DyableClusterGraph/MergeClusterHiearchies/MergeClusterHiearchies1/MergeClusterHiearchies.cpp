#include "./MergeOrphanNodes.h"

// This is the cluster hiearchy that is repsonsible for storing a 
// mapping between every base node and it's location in the hiearchy.
// This is done by simply assigning a unique integer to every base
// node that represents it's location in the hiearchy. Nodes are 
// recursively grouped by specifying a range of indexes under which
// a given level of nodes placed. In order to achive this nodes that 
// have the same cluster id must be ordered sequentially beside each
// other. This is done using merge clusters. 

// When updating a segment of nodes the current cluster id is assigned
// to a group of sequential nodes and any other segments that have the
// same cluster id are grouped together. Each node in the combined 
// segments is remapped. The remapping of a node in a segment is simply
// its local offset in the segment plus the global offset of the combined
// segments. This is determined by simply counting the number of nodes
// that have been grouped so far. 

// The information relating to the hiearchy itself is stored in an ABTree. 
// An ABTree is an externally linked data structure that stores the number
// of child nodes under a given cluster and also the summary links that 
// exist on a given level of the hiearchy. Like cluster segments, ABTrees
// must also be moved around in external memory so nodes grouped under the
// same cluster id appear in the same segment of external memory.

// This class is responsible for maintaining the hiearchy that is
// built up on each pass. This is so nodes can be appropriately
// mapped to a given location in the hiearchy. The hiearchy itself
// just consists of the number of children under a node an the range
// of node indexes under a node. This is built in a similar way to 
// segments where nodes are continously merged on remapping. The 
// mapping itself is stored in clusters in a depth first manner. It's
// these clusters that are physically moved around when merging nodes.

// A subtree in the hiearchy is stored as
// 1. Cluster ID
// 2. The current number of subtrees contained
// 3. Followed by all the stats
class CMergeClusterHiearchies : public CNodeStat {

	// This stores an individual node in a cluster segment
	struct SSegNode {
		// This is a ptr to the next node in the segment
		SSegNode *next_ptr;
		// This stores the global document id
		S5Byte base_node;
	};

	// This stores a particular existing segment of nodes. It's 
	// important the base offset be stored so nodes can be correctly
	// remapped at their new cluster location. It's important that the 
	// segnodes be kept in consistent order for node remapping reasons.
	struct SSegment {
		// This stores a pointer to the head node in the segment
		SSegNode *head_node_ptr;
		// This stores a pointer to the tail node in the segment
		SSegNode *tail_node_ptr;
		// This stores a pointer to the next segment
		SSegment *next_ptr;
		// This stores the base offset for all nodes 
		// in this given segment -> this is used to 
		// calculate the new cluster mapping
		S5Byte base_offset;

		SSegment() {
			head_node_ptr = NULL;
		}

		// This adds a node to the current segment
		// @param node - this is the base node that makes
		//             - up the original mapping
		// @param buff - this stores an individual node
		void AddNode(const S5Byte &node, 
			CLinkedBuffer<SSegNode> &buff) {

			if(head_node_ptr == NULL) {
				head_node_ptr = buff.ExtendSize(1);
				tail_node_ptr = head_node_ptr;
			} else {
				tail_node_ptr->next_ptr = buff.ExtendSize(1);
				tail_node_ptr = tail_node_ptr->next_ptr;
			}

			tail_node_ptr->next_ptr = NULL;
			tail_node_ptr->base_node = node;
		}
	};

	// This groups cluster segments that have the same root cluster
	// mapping together. This is so that they can be parsed in 
	// memory in order to assign an updated cluster assignment. This
	// is done by adding an offset to the local node id that belongs
	// to a cluster segment. It's important that the order that the
	// segments appear in stays consistent for mapping reasons. 
	struct SGroupSegments {
		// This stores the current number of nodes in the 
		// combined cluster of segments
		int total_node_num;
		// This stores the accumulative traversal 
		// probability of a given segment
		float trav_prob;
		// This is a ptr to the head segment
		SSegment *head_seg_ptr;
		// This is a ptr to the tail segment
		SSegment *tail_seg_ptr;

		SGroupSegments() {
			total_node_num = 0;
			head_seg_ptr = NULL;
			trav_prob = 0;
		}

		// This adds a new segment to the group
		// @param hiearchy_file - this stores an existing segment
		// @param seg_buff - this stores a segment 
		// @param node_buff - this stores a node in a segment
		// @param stat - this stores the statistics relating 
		//             - to a given hiearchy
		void AddSegment(CHDFSFile &hiearchy_file, 
			CLinkedBuffer<SSegment> &seg_buff,
			CLinkedBuffer<SSegNode> &node_buff,
			SHiearchyStat &stat) {

			if(head_seg_ptr == NULL) {
				head_seg_ptr = seg_buff.ExtendSize(1);
				tail_seg_ptr = head_seg_ptr;
			} else {
				tail_seg_ptr->next_ptr = seg_buff.ExtendSize(1);
				tail_seg_ptr = tail_seg_ptr->next_ptr;
			}

			tail_seg_ptr->next_ptr = NULL;
			// The previous global node assignment is
			// stored in the cluster id
			tail_seg_ptr->base_offset = stat.clus_id;

			total_node_num += stat.total_node_num;
			trav_prob += stat.pulse_score;

			static S5Byte node;
			for(int i=0; i<stat.total_node_num; i++) {
				hiearchy_file.ReadCompObject(node);
				// This retrieves the base node id for a
				// grouped node under this cluster segment 
				tail_seg_ptr->AddNode(node, node_buff);
			}
		}
	};

	// This stores a group of hiearchy subtrees. Due to the 
	// way subtrees are stored, each subtree must be placed
	// onto the end of the linked list
	struct SGroupHiearchy {
		// This stores a ptr to the first subtree
		SSubTree *head;
		// This stores a ptr to the last subtree
		SSubTree *tail;
		// This stores the new accumalative node stat
		SSubTree acc_stat;
		// This stores the total number of hiearchy subtrees
		// that have been grouped together
		int total_subtrees;

		SGroupHiearchy() {
			head = NULL;
			acc_stat.Initialize();
			total_subtrees = 0;
		}

		// This adds a subtree to the current group
		// @param hiearchy_file - this stores an existing hiearchy subtree
		// @param buff - this stores an individual node
		void AddSubTrees(CHDFSFile &hiearchy_file, SHiearchyStat &stat,
			CLinkedBuffer<SSubTree> &buff) {

			static SSubTree subtree;
			// read in the summary subtree first
			subtree.ReadSubTree(hiearchy_file);

			acc_stat.child_num++;
			acc_stat.node_num += stat.total_node_num;
			acc_stat.net_trav_prob += subtree.net_trav_prob;
			total_subtrees += stat.total_subtrees;

			if(head == NULL) {
				head = buff.ExtendSize(1);
				tail = head;
			} else {
				tail->next_ptr = buff.ExtendSize(1);
				tail = tail->next_ptr;
			}

			tail->AddSubtree(subtree);
			tail->next_ptr = NULL;

			// read in the other subtrees
			for(int i=1; i<stat.total_subtrees; i++) {
				subtree.ReadSubTree(hiearchy_file);

				tail->next_ptr = buff.ExtendSize(1);
				tail = tail->next_ptr;
				tail->AddSubtree(subtree);
				tail->next_ptr = NULL;
			}
		}

		// This writes all the subtrees to file 
		// @param hiearchy_file - this is used to store all the subtrees
		void WriteSubTrees(CHDFSFile &hiearchy_file) {

			// only create a summary subtree if there is
			// more than a single subtree being joined
			acc_stat.WriteSubTree(hiearchy_file);

			SSubTree *curr_ptr = head;
			while(curr_ptr != NULL) {
				curr_ptr->WriteSubTree(hiearchy_file);
				curr_ptr = curr_ptr->next_ptr;
			}
		}
	};

	// This combines both the segments and the hiearchy subtrees
	// together under a single cluster id
	struct SGroupAll {
		// this stores all the segments
		SGroupSegments segments;
		// this stores all the hiearchies
		SGroupHiearchy hiearchy;

		// This writes the current cluster stat to file
		// @param clus_id - the new cluster id for the hiearchy
		void WriteClusterStat(const S5Byte &clus_id, CHDFSFile &file) {

			static SHiearchyStat stat;
			stat.total_subtrees = hiearchy.total_subtrees;
			// don't forget to account for the summary subtree
			stat.total_subtrees++;

			stat.total_node_num = segments.total_node_num;
			stat.pulse_score = segments.trav_prob;
			stat.clus_id = clus_id;

			stat.WriteHiearchyStat(file);
		}
	};

	// This is a buffer used to store all the seg nodes
	CLinkedBuffer<SSegNode> m_seg_node_buff;
	// This is used to store a given segment of nodes
	CLinkedBuffer<SSegment> m_segment_buff;
	// This is a buffer used to store all the stat nodes
	CLinkedBuffer<SSubTree> m_tree_buff;

	// This stores the mapping between a cluster id
	// and a group of segments
	CMemoryChunk<SGroupAll> m_group;
	// This stores the local cluster id for a given node 
	// in a hash division 
	CArrayList<int> m_local_clus_id;
	// This stores the mapping between a cluster id
	// and a group of segments
	CObjectHashMap<S5Byte> m_node_map;
	// This is used to store the mapping between a node and a cluster
	CObjectHashMap<S5Byte> m_cluster_map;

	// This stores the current base node offset for all 
	// nodes parsed by a given client this is a local 
	// node offset so will need to be readjusted accordingly
	_int64 m_curr_base_offset;
	// This is used to assign cluster labels to orphan nodes
	CMergeOrphanNodes m_orphan_node;
	// This is the maximum number of nodes allowed in a given cluster
	int m_max_node_num;

	// This file stores the final mapping between the existing 
	// node set and the new node set for this client. This is
	// for the global node set.
	CSegFile m_global_map_file;
	// This stores the mapping between cluster nodes and the base
	// nodes. This is used to map s_links down to the unique 
	// base level node mapping.
	CSegFile m_base_map_file;
	// This file stores the final mapping between existing
	// nodes at the coarsest level of granuality and the new 
	// node set. This is only done for nodes at the top level.
	CSegFile m_local_map_file;
	// This stores the current node set for this client
	CSegFile m_node_set_file;
	// This stores the mapping between a base node and it's
	// cluster assignment -> this is continuously remapped
	CHDFSFile m_hiearchy_file;

	// This is the order file which is used for testing that 
	// dictates the order in which cluster hiearchies are joined
	CHDFSFile m_order_join_file;
	// THis stores the orphan node mapping which is used for testing
	CHDFSFile m_orphan_clus_file;

	// This defines a function that computes the hash map for an object
	static int HashCode(const S5Byte &item) {
		return (int)S5Byte::Value(item);
	}

	// This defines a function to compare two nodes for equality
	static bool Equal(const S5Byte &arg1, const S5Byte &arg2) {
		return S5Byte::Value(arg1) == S5Byte::Value(arg2);
	}

	// This is used for testing 
	void PrintHiearchy() {

		m_node_map.ResetNextObject();
		int clus_group = 0;
		for(int i=0; i<m_group.OverflowSize(); i++) {
			S5Byte &cluster = m_node_map.NextSeqObject();
			SGroupSegments &seg_list = m_group[i].segments;
			cout<<"Node: "<<clus_group<<endl;

			SSegment *curr_ptr = seg_list.head_seg_ptr;
			while(curr_ptr != NULL) {
				SSegNode *curr_seg = curr_ptr->head_node_ptr;
				while(curr_seg != NULL) {
					cout<<curr_seg->base_node.Value()<<endl;
					curr_seg = curr_seg->next_ptr;
					clus_group++;
				}
				curr_ptr = curr_ptr->next_ptr;
			}
		}
	}

	// This cycles through the set of clustered segments and writes the
	// set of base nodes according to their location in the hiearchy. 
	// Also the mapping between local and global nodes is created as well
	// as creating the new node set for this client.
	// @param ptr - this stores a pointer to a group of clustered segments
	void CycleThroughSegments(SGroupSegments &ptr) {

		SClusterMap clus_map;
		SSegment *seg_ptr = ptr.head_seg_ptr;
		_int64 curr_cluster = m_curr_base_offset;

		while(seg_ptr != NULL) {
			SSegNode *node_ptr = seg_ptr->head_node_ptr;
			// writes the local cluster mapping
			clus_map.base_node = seg_ptr->base_offset;
			clus_map.cluster = curr_cluster;
			clus_map.WriteClusterMap(m_local_map_file);

			m_order_join_file.WriteCompObject(clus_map.base_node);

			// cycles through all the nodes in a segment
			while(node_ptr != NULL) {
				// updates the global cluster mapping
				clus_map.base_node = seg_ptr->base_offset++;
				clus_map.cluster = m_curr_base_offset++;
				clus_map.WriteClusterMap(m_global_map_file);

				// updates the base node mapping
				clus_map.base_node = clus_map.cluster;
				clus_map.cluster = node_ptr->base_node;
				clus_map.WriteClusterMap(m_base_map_file);

				m_hiearchy_file.WriteCompObject(node_ptr->base_node);
				node_ptr = node_ptr->next_ptr;
			}

			seg_ptr = seg_ptr->next_ptr;
		}
	}

	// This writes the hiearchy once all segments and subtrees have 
	// been grouped according to their cluster id.
	void StoreHiearchy() {

		SPulseMap pulse_map;
		for(int i=0; i<m_group.OverflowSize(); i++) {
			SGroupAll &group = m_group[i];

			pulse_map.node = m_curr_base_offset;
			pulse_map.pulse_score = group.segments.trav_prob;

			// update the node set file
			m_node_set_file.WriteCompObject(pulse_map.node);

			group.WriteClusterStat(pulse_map.node, m_hiearchy_file);
			CycleThroughSegments(group.segments);
			group.hiearchy.WriteSubTrees(m_hiearchy_file);
		}
	}

	// This loads a subtree of the hiearchy and groups it with other
	// subtrees with the same cluster id. It also groups segments of
	// nodes together based upon their cluster id.
	// @param hiearchy_file - this stores all the subtrees for the 
	//                      - current hiearchy
	// @param orphan_num - this is the current orphan number
	void LoadHiearchy(CHDFSFile &hiearchy_file, int &orphan_num) {

		SHiearchyStat stat;
		SClusterMap clus_map;
		while(stat.ReadHiearchyStat(hiearchy_file)) {
			int id = m_node_map.Get(stat.clus_id);
			id = m_local_clus_id[id];

			m_orphan_clus_file.WriteCompObject(id);

			if(id < 0) {
				// this assigns the orphan node mapping
				clus_map.cluster = m_orphan_node.ClusterMapping(orphan_num++);
				id = m_cluster_map.Put(clus_map.cluster);
			} 

			clus_map.cluster = id;
			clus_map.base_node = stat.clus_id;
			clus_map.WriteClusterMap(m_orphan_clus_file);

			SGroupAll &group = m_group[id];
			group.segments.AddSegment(hiearchy_file,
				m_segment_buff, m_seg_node_buff, stat);

			group.hiearchy.AddSubTrees(hiearchy_file, stat, m_tree_buff);
		}
	}

	// This takes all the merged hiearchies and cluster node maps and
	// cycles through each hash division.
	// @param max_child_num - the maximum number of nodes that can be joined
	//                      - at any one time
	void ProcessHiearchy(int max_child_num) {

		m_orphan_clus_file.OpenWriteFile(CUtility::ExtendString
			("LocalData/local1_clus", GetInstID(), ".set", GetClientID()));

		int orphan_clus_num = m_orphan_node.CreateClusterMapping(max_child_num);
		m_group.AllocateMemory(m_cluster_map.Size() + orphan_clus_num);

		int orphan_num = 0;
		CHDFSFile hiearchy_file;
		for(int i=0; i<CNodeStat::GetClientNum(); i++) {
			hiearchy_file.OpenReadFile(CUtility::ExtendString
				("LocalData/hiearchy", CNodeStat::GetInstID(), ".client",
				i, ".set", CNodeStat::GetClientID()));

			LoadHiearchy(hiearchy_file, orphan_num);
		}

		StoreHiearchy();

		if(m_cluster_map.Size() != m_group.OverflowSize()) {
			cout<<"Clus Group Size Mis";getchar();
		}
	}

	// This loads the cluster mapping into memory for the current hash division
	void LoadClusterMapping() {

		SClusterMap map;
		SHiearchyStat stat;
		uChar is_orphan_node;
		CHDFSFile clus_map_file;
		for(int i=0; i<CNodeStat::GetClientNum(); i++) {
			clus_map_file.OpenReadFile(CUtility::ExtendString
				("LocalData/cluster_map", CNodeStat::GetInstID(), ".client",
				i, ".set", CNodeStat::GetClientID()));

			while(map.ReadClusterMap(clus_map_file)) {
				clus_map_file.ReadCompObject(is_orphan_node);

				m_node_map.Put(map.base_node);
				if(m_node_map.AskFoundWord()) {
					cout<<"Node Already Exists "<<map.base_node.Value();getchar();
				}

				if(is_orphan_node == true) {
					SHiearchyStat::ReadHiearchyStat(clus_map_file, stat);
					m_orphan_node.AddOrphanNode(stat);
					m_local_clus_id.PushBack(-1);
					continue;
				}

				m_local_clus_id.PushBack(m_cluster_map.Put(map.cluster));
			}
		}
	}

	// This opens the different hiearchy files
	void Initialize() {

		m_hiearchy_file.OpenWriteFile(CUtility::
			ExtendString(CLUSTER_HIEARCHY_DIR, GetInstID(),
			".set", CNodeStat::GetClientID()));

		m_node_set_file.OpenWriteFile(CUtility::ExtendString
			("LocalData/node_set", GetInstID(),
			".set", CNodeStat::GetClientID()));

		m_global_map_file.OpenWriteFile(CUtility::ExtendString
			("LocalData/global_node_map", GetInstID(),
			".set", CNodeStat::GetClientID()));

		m_base_map_file.OpenWriteFile(CUtility::ExtendString
			("LocalData/base_node_map", GetInstID(),
			".set", CNodeStat::GetClientID()));

		m_local_map_file.OpenWriteFile(CUtility::ExtendString
			("LocalData/local_node_map", GetInstID(),
			".set", CNodeStat::GetClientID()));

		m_order_join_file.OpenWriteFile(CUtility::ExtendString
			("LocalData/order_join", GetInstID(), ".set", GetClientID()));
	}

public:

	CMergeClusterHiearchies() {
		m_seg_node_buff.Initialize();
		m_segment_buff.Initialize();
		m_tree_buff.Initialize();
		CHDFSFile::Initialize();
	}

	// This is the entry function
	// @param node_offset_start - this is the start global node offset for the set
	// @param node_offset_end - this is the end global node offset for the set
	// @param max_clus_node_num - this is the maximum number of nodes allowed
	//                          - in a given cluster
	// @param max_child_num - the maximum number of nodes that can be joined
	//                      - at any one time
	void MergeClusterHiearchies(int hash_div, int hash_div_num, 
		int inst_id, int div_node_num, _int64 node_offset_start, 
		_int64 node_offset_end, int max_clus_node_num, int max_child_num) {

		CNodeStat::SetClientID(hash_div);
		CNodeStat::SetClientNum(hash_div_num);
		CNodeStat::SetInstID(inst_id);
		
		m_curr_base_offset = node_offset_start;
		m_max_node_num = max_clus_node_num;

		m_orphan_node.SetMaxClusNodeNum(max_clus_node_num);
		m_local_clus_id.Initialize(div_node_num);
		m_node_map.Initialize(HashCode, Equal);
		m_cluster_map.Initialize(HashCode, Equal);

		Initialize();
		LoadClusterMapping();
		ProcessHiearchy(max_child_num);

		if(m_curr_base_offset != node_offset_end) {
			cout<<"Boundary Align Error "<<m_curr_base_offset<<" "<<node_offset_end;getchar();
		}

		CHDFSFile node_num_file;
		node_num_file.OpenWriteFile(CUtility::ExtendString
			("LocalData/node_num", CNodeStat::GetInstID(), 
			".set", CNodeStat::GetClientID()));
		node_num_file.WriteCompObject(m_group.OverflowSize());
	}

};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);
	int client_num = atoi(argv[2]);
	int inst_id = atoi(argv[3]);
	int hash_buff_size = atoi(argv[4]);
	int max_clus_node_num = atoi(argv[5]);
	int max_child_num = atoi(argv[6]);
	_int64 node_offset_start = CANConvert::AlphaToNumericLong
		(argv[7], strlen(argv[7]));
	_int64 node_offset_end = CANConvert::AlphaToNumericLong
		(argv[8], strlen(argv[8]));

	CBeacon::InitializeBeacon(client_id, 5555 + inst_id);
	CMemoryElement<CMergeClusterHiearchies> set;
	set->MergeClusterHiearchies(client_id, client_num, inst_id, hash_buff_size, 
		node_offset_start, node_offset_end, max_clus_node_num, max_child_num);
	set.DeleteMemoryElement();

	CBeacon::SendTerminationSignal();
}