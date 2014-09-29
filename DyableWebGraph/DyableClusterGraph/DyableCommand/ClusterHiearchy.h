#include "./LinkCluster.h"

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

class CClusterHiearchy : public CCommunication {

	// This stores the current node set for this client
	CFileSet<CSegFile> m_node_set;
	// This stores the mapping between a base node and it's
	// cluster assignment -> this is continuously remapped
	CFileSet<CHDFSFile> m_hiearchy_set;
	// This stores the node boundary for each each hash division
	CMemoryChunk<_int64> m_div_node_bound;
	// This defines the maximum number of nodes that can 
	// exist under any node in the hiearchy
	int m_max_clus_node_num;

	// This takes all the merged hiearchies and cluster node maps and
	// cycles through each hash division.
	// @param max_clus_node_num - this is the maximum number of 
	//                          - nodes that can exist under any 
	//                          - node in the hiearchy
	void MergeClusterHiearchies(int max_child_num) {

		int hash_buff_size = CNodeStat::GetCurrNodeNum() / CNodeStat::GetHashDivNum();
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			CString arg("Index ");
			arg += i;
			arg += " ";
			arg += CNodeStat::GetHashDivNum();
			arg += " ";
			arg += CNodeStat::GetInstID();
			arg += " ";
			arg += hash_buff_size;
			arg += " ";
			arg += m_max_clus_node_num;
			arg += " ";
			arg += max_child_num;
			arg += " ";
			arg += m_div_node_bound[i];
			arg += " ";
			arg += m_div_node_bound[i+1];

			ProcessSet().CreateRemoteProcess("../MergeClusterHiearchies/Debug/"
				"MergeClusterHiearchies.exe", arg.Buffer(), i);
		}

		ProcessSet().WaitForPendingProcesses();
	}

	// This distributes cluster hiearchies to their respective hash division
	void DistributeClusterHiearchies() {

		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			CString arg("Index ");
			arg += i;
			arg += " ";
			arg += CNodeStat::GetHashDivNum();
			arg += " ";
			arg += CNodeStat::GetInstID();
			arg += " ";
			arg += m_max_clus_node_num;

			ProcessSet().CreateRemoteProcess("../DistributeClusterHiearchies/Debug/"
				"DistributeClusterHiearchies.exe", arg.Buffer(), i);
		}

		ProcessSet().WaitForPendingProcesses();
	}

	// This is called when the cluster hiearchy is first created. A number
	// of different hiearchy blocks needs to be created with node stats
	// stored for each cluster. Since each node is just mapped to a single
	// cluster to start off with each stat will have a length of 1.
	// @param key_pulse_rank_file - this contains the set of keyword pulse rank scores
	// @param web_pulse_rank_file - this contains the set of webgraph pulse rank scores
	// @param node_num - this is the global number of nodes for this link set
	void InitializeHiearchy(CHDFSFile &key_pulse_rank_file, 
		CHDFSFile &web_pulse_rank_file, _int64 &node_num) {

		SSubTree subtree;
		SHiearchyStat stat;
		stat.total_node_num = 1;
		stat.total_subtrees = 1;
		subtree.child_num = 0;
		subtree.node_num = 1;

		SPulseMap key_pulse_map;
		SPulseMap web_pulse_map;
		key_pulse_map.ReadPulseMap(key_pulse_rank_file);

		while(web_pulse_map.ReadPulseMap(web_pulse_rank_file)) {
			if(web_pulse_map.node >= CNodeStat::GetBaseNodeNum()) {
				return;
			}

			if(key_pulse_map.node == web_pulse_map.node) {
				web_pulse_map.pulse_score += key_pulse_map.pulse_score;
				if(!key_pulse_map.ReadPulseMap(key_pulse_rank_file)) {
					key_pulse_map.node.SetMaxValue();
				}
			}

			node_num++;
			stat.pulse_score = web_pulse_map.pulse_score / 2;
			stat.clus_id = web_pulse_map.node;
			subtree.net_trav_prob = stat.pulse_score;
			int hash = stat.clus_id.Value() % CNodeStat::GetHashDivNum();

			stat.WriteHiearchyStat(m_hiearchy_set.File(hash));
			m_hiearchy_set.File(hash).WriteCompObject(stat.clus_id);
			subtree.WriteSubTree(m_hiearchy_set.File(hash));

			m_node_set.File(hash).WriteCompObject(stat.clus_id);
		}

		if(key_pulse_map.node.IsMaxValueSet() == false) {
			cout<<"still more";getchar();
		}
	}

	// This finds the node offset for each hash division
	void FindHashDivNodeBoundary() {

		CMemoryChunk<_int64> local_node_num;
		CMemoryChunk<_int64> global_node_num(CNodeStat::GetHashDivNum(), 0);
		m_div_node_bound.AllocateMemory(CNodeStat::GetHashDivNum() + 1);

		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			local_node_num.ReadMemoryChunkFromFile(CUtility::ExtendString
				("LocalData/node_num", CNodeStat::GetInstID(), ".set", i));

			for(int j=0; j<CNodeStat::GetHashDivNum(); j++) {
				global_node_num[j] += local_node_num[j];
			}
		}

		_int64 offset = 0;
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			m_div_node_bound[i] = offset;
			offset += global_node_num[i];
		}

		if(offset != GetGlobalNodeNum()) {
			cout<<"Global Bound Mismatch";getchar();
		}

		m_div_node_bound.LastElement() = offset;
		m_div_node_bound.WriteMemoryChunkToFile
			("GlobalData/ClusterHiearchy/cluster_boundary");
	}

	// This sets up the various file ready for use
	void Initialize() {

		m_node_set.OpenWriteFileSet("LocalData/node_set",
			CNodeStat::GetHashDivNum(), GetInstID());
		m_hiearchy_set.OpenWriteFileSet(CLUSTER_HIEARCHY_DIR,
			CNodeStat::GetHashDivNum(), GetInstID());
	}

public:

	CClusterHiearchy() {
	}

	// This is called when the cluster hiearchy is first created.
	// @param max_clus_node_num - this is the maximum number of 
	//                          - nodes that can exist under any 
	//                          - node in the hiearchy
	void InitializeHiearchy(int max_clus_node_num) {

		Initialize();
		cout<<"Max Clus Node Num "<<max_clus_node_num<<endl;

		_int64 node_num = 0;
		CHDFSFile key_pulse_rank_file;
		CHDFSFile web_pulse_rank_file;
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			cout<<"Cluster Hiearchy "<<i<<" Out Of "<<CNodeStat::GetHashDivNum()<<endl;
			key_pulse_rank_file.OpenReadFile(CUtility::
				ExtendString(KEYWORD_PULSE_SCORE_DIR, i));
			web_pulse_rank_file.OpenReadFile(CUtility::
				ExtendString(WEBGRAPH_PULSE_SCORE_DIR, i));

			InitializeHiearchy(key_pulse_rank_file, web_pulse_rank_file, node_num);
		}

		if(node_num != GetBaseNodeNum()) {
			cout<<"Global Node Num Mismatch";getchar();
		}

		node_num = GetBaseNodeNum();
		m_max_clus_node_num = max_clus_node_num;
		CNodeStat::SetCurrNodeNum(node_num);
		CNodeStat::SetGlobalNodeNum(node_num);
		CNodeStat::SetBaseNodeNum(node_num);
		m_node_set.CloseFileSet();
		m_hiearchy_set.CloseFileSet();
	}

	// This sets the maximum number of nodes allowed in a cluster
	// @param max_clus_node_num - this is the maximum number of 
	//                          - nodes that can exist under any 
	//                          - node in the hiearchy
	inline void SetMaxClusterNodeNum(int max_clus_node_num) {
		m_max_clus_node_num = max_clus_node_num;
	}

	// This updates the new node number based upon total number
	// of unique clusters.
	static void AssignNodeNum() {

		int node_num = 0;
		_int64 new_node_num = 0;
		CHDFSFile node_num_file;
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			node_num_file.OpenReadFile(CUtility::ExtendString
				("LocalData/node_num", CNodeStat::GetInstID(), ".set", i));
			node_num_file.ReadCompObject(node_num);
			new_node_num += node_num;
		}

		CNodeStat::SetCurrNodeNum(new_node_num);
	}

	// This is the entry function called to perform the update of 
	// the cluster mapping for segments of nodes. This is done by
	// using the cluster node stats to summarize a segment of 
	// clusters and then group segments together. Specifically 
	// cluster segments are hashed by their cluster id and grouped
	// this way. A given bucket division is then processed to group
	// all corresponding segments together. A similar thing is done
	// for the hiearchy subtrees.
	// @param max_child_num - the maximum number of nodes that can be joined
	//                      - at any one time
	void UpdateHiearchy(int max_child_num) {
	
		CSegFile node_set_file(CUtility::ExtendString
			("LocalData/node_set", CNodeStat::GetInstID(), ".set"));

		CSegFile clus_stat_file(CUtility::ExtendString
			("LocalData/clus_stat_file", GetInstID(), ".set"));

		CSegFile local_clus_file("LocalData/local_clus");

		CMapReduce::ExternalHashMap("WriteClusterMapping", node_set_file,
			local_clus_file, clus_stat_file, CUtility::ExtendString
			("LocalData/clust_stat", GetInstID()), sizeof(S5Byte), sizeof(S5Byte));

		DistributeClusterHiearchies();
		FindHashDivNodeBoundary();
		MergeClusterHiearchies(max_child_num);
		AssignNodeNum();
	}

};

// This finds the set of orphan nodes. This is done by looking for nodes that 
// don't have any matching src or dst nodes that belong to a link.
class CTestOrphanNodes : public CNodeStat {

	// This stores the set of neighbour nodes
	CTrie m_neighbour_node;
public:

	CTestOrphanNodes() {
	}

	// This finds the set of neighbour nodes
	void FindNeighbourNodes() {

		S5Byte node;
		m_neighbour_node.Initialize(4);
		CHDFSFile link_set_file;
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			link_set_file.OpenReadFile(CUtility::ExtendString
				("LocalData/cluster_node_set", GetInstID(), ".set", i));

			while(link_set_file.ReadCompObject(node)) {
				m_neighbour_node.AddWord((char *)&node, sizeof(S5Byte));
			}
		}
	}

	// This checks if a node is a neighbour node
	inline bool IsNeighbourNode(S5Byte &node) {
		return m_neighbour_node.FindWord((char *)&node, sizeof(S5Byte)) >= 0;
	}
};

// This is used to test that cluster hiearhies are grouped correctly based upon
// their cluster label. Consequently the global cluster mapping and the local 
// cluster mapping is also checked.
class CTestGroupClusterHiearchies : public CTestOrphanNodes {

	// This stores the set of nodes
	CTrie m_node_map;
	// This stores the set of cluster maps
	CArrayList<SClusterMap> m_clus_map_buff;

	// This stores the node map for the cluster hiearchy local nodes
	CTrie m_hiearchy_node_map;
	// This stores the set of hiearchy stats for each hiearchy
	CArrayList<SHiearchyStat> m_hiearchy_stat_buff;
	// This stores the set of hiearchy nodes 
	CVector<CArrayList<S5Byte> > m_hiearchy_node_set;
	// This stores the set of hiearchy subtrees
	CVector<CArrayList<SSubTree> > m_hiearchy_subtree;

	// This stores the set of hiearchy stats for each hiearchy
	CArrayList<SHiearchyStat> m_mapped_hiearchy_stat_buff;
	// This stores the set of hiearchy nodes 
	CVector<CArrayList<S5Byte> > m_mapped_hiearchy_node_set;
	// This stores the set of hiearchy subtrees
	CVector<CArrayList<SSubTree> > m_mapped_hiearchy_subtree;
	// This stores the set of base offsets corresponsing to the 
	// global cluster mapping
	CVector<CArrayList<S5Byte> > m_mapped_hiearchy_base_offset;
	// This stores the set of hiearchy stats
	CVector<CArrayList<SHiearchyStat> > m_hiearchy_set;

	// This stores the local cluster mapping
	CHDFSFile m_local_clus_file;
	// This stores the current set of hiearchies
	CHDFSFile m_hiearchy_file;

	// This stores the set of global nodes
	CArrayList<SClusterMap> m_global_node_set;
	// This stores the set of local nodes
	CArrayList<SClusterMap> m_local_node_set;
	// This stores the set of pulse rank nodes
	CArrayList<SPulseMap> m_pulse_map_set;

	// This stores the global set of cluster maps
	CHDFSFile m_global_map_file;
	// This stores the local set of cluster maps
	CHDFSFile m_local_map_file;
	// This is the file that stores the pulse rank scores
	// for this particular client (have been hashed)
	CSegFile m_pulse_rank_file;
	// This stores the current node set for this client
	CSegFile m_node_set_file;
	// This is the order file which is used for testing that 
	// dictates the order in which cluster hiearchies are joined
	CHDFSFile m_order_join_file;

	// This stores the global node set
	CTrie m_global_node_map;
	// This stores the local node set
	CTrie m_local_node_map;
	// This stores the final cluster map
	CTrie m_cluster_map;

	// This stores the client boundary for this instance
	CMemoryChunk<_int64> m_client_boundary;

	// This loads the set of cluster mappings
	void LoadClusMapSet() {
		
		m_node_map.Initialize(4);
		m_clus_map_buff.Initialize(4);

		SClusterMap clus_map;
		int is_orphan_node;
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			m_local_clus_file.OpenReadFile(CUtility::ExtendString
				("LocalData/local1_clus", GetInstID(), ".set", i));

			while(m_local_clus_file.ReadCompObject(is_orphan_node)) {
				clus_map.ReadClusterMap(m_local_clus_file);

				if(IsNeighbourNode(clus_map.base_node) == (is_orphan_node < 0)) {
					cout<<"Not an orphan node";getchar();
				}

				clus_map.cluster <<= 8;
				clus_map.cluster |= i;

				m_node_map.AddWord((char *)&clus_map.base_node, sizeof(S5Byte));
				if(!m_node_map.AskFoundWord()) {
					m_clus_map_buff.PushBack(clus_map);
				}
			}
		}
	}

	// This merges hiearchies together based upon their cluster mapping
	void MergeHiearchies() {

		m_mapped_hiearchy_stat_buff.Initialize();

		S5Byte node;
		int clus;
		SClusterMap clus_map;
		CTrie local_map(4);
		int cluster_num = m_node_map.Size();

		for(int k=0; k<CNodeStat::GetHashDivNum(); k++) {
			m_order_join_file.OpenReadFile(CUtility::ExtendString
				("LocalData/order_join", GetInstID(), ".set", k));

			while(m_order_join_file.ReadCompObject(node)) {
				int set = m_hiearchy_node_map.FindWord((char *)&node, sizeof(S5Byte));
				if(set < 0) {
					cout<<node.Value();getchar();
				}

				S5Byte &node = m_hiearchy_stat_buff[set].clus_id;
				int id = m_node_map.FindWord((char *)&node, sizeof(S5Byte));
				if(id >= 0) {
					clus = m_clus_map_buff[id].cluster.Value();
				} else {
					m_node_map.AddWord((char *)&node, sizeof(S5Byte));
					clus_map.base_node = node;
					clus_map.cluster = cluster_num;
					m_clus_map_buff.PushBack(clus_map);
					clus = cluster_num++;
				}

				clus = local_map.AddWord((char *)&clus, sizeof(int));
				if(!local_map.AskFoundWord()) {
					m_mapped_hiearchy_stat_buff.PushBack(m_hiearchy_stat_buff[set]);
					m_mapped_hiearchy_stat_buff.LastElement().total_subtrees++;

					m_mapped_hiearchy_base_offset.ExtendSize(1);
					m_mapped_hiearchy_base_offset.LastElement().Initialize();
					m_mapped_hiearchy_node_set.ExtendSize(1);
					m_mapped_hiearchy_node_set.LastElement().Initialize();
					m_mapped_hiearchy_subtree.ExtendSize(1);
					m_mapped_hiearchy_subtree.LastElement().Initialize();
					// make room for the root subtree
					m_mapped_hiearchy_subtree.LastElement().ExtendSize(1);
					m_hiearchy_set.ExtendSize(1);
					m_hiearchy_set.LastElement().Initialize();
				} else {
					m_mapped_hiearchy_stat_buff[clus].pulse_score += m_hiearchy_stat_buff[set].pulse_score;
					m_mapped_hiearchy_stat_buff[clus].total_subtrees += m_hiearchy_stat_buff[set].total_subtrees;
					m_mapped_hiearchy_stat_buff[clus].total_node_num += m_hiearchy_stat_buff[set].total_node_num;
				}

				m_hiearchy_set[clus].PushBack(m_hiearchy_stat_buff[set]);

				for(int j=0; j<m_hiearchy_node_set[set].Size(); j++) {
					m_mapped_hiearchy_node_set.LastElement().PushBack
						(m_hiearchy_node_set[set][j]);
				}

				for(int j=0; j<m_hiearchy_subtree[set].Size(); j++) {
					m_mapped_hiearchy_subtree.LastElement().PushBack
						(m_hiearchy_subtree[set][j]);
				}

				for(int j=0; j<m_hiearchy_node_set[set].Size(); j++) {
					S5Byte base_node = node.Value() + j;
					m_mapped_hiearchy_base_offset.LastElement().PushBack(base_node);
				}
			}
		}
	}

	// This creates the new cluster mapping for global and local nodes
	void CreateNewClusterMapping() {

		m_global_node_set.Initialize();
		m_local_node_set.Initialize();
		m_pulse_map_set.Initialize();

		m_global_node_map.Initialize(4);
		m_local_node_map.Initialize(4);
		m_cluster_map.Initialize(4);
		m_pulse_map_set.Initialize();

		SPulseMap pulse_map;
		SClusterMap clus_map;
		int local_cluster = 0;
		int global_cluster = 0;
		for(int i=0; i<m_mapped_hiearchy_node_set.Size(); i++) {

			for(int j=0; j<m_hiearchy_set[i].Size(); j++) {
				clus_map.base_node = m_hiearchy_set[i][j].clus_id;
				clus_map.cluster = local_cluster;
				m_local_node_map.AddWord((char *)&clus_map.base_node, sizeof(S5Byte));
				if(m_local_node_map.AskFoundWord()) {
					cout<<"Local Node Already Exists";getchar();
				}
				m_local_node_set.PushBack(clus_map);
			}

			m_cluster_map.AddWord((char *)&local_cluster, sizeof(int));
			pulse_map.node = local_cluster;
			pulse_map.pulse_score = m_mapped_hiearchy_stat_buff[i].pulse_score;
			m_pulse_map_set.PushBack(pulse_map);

			for(int j=0; j<m_mapped_hiearchy_base_offset[i].Size(); j++) {
				clus_map.base_node = m_mapped_hiearchy_base_offset[i][j];
				clus_map.cluster = global_cluster;

				m_global_node_map.AddWord((char *)&clus_map.base_node, sizeof(S5Byte));
				if(m_global_node_map.AskFoundWord()) {
					cout<<"Global Node Already Exists "<<clus_map.base_node.Value();getchar();
				}

				m_global_node_set.PushBack(clus_map);
				global_cluster++;
				local_cluster++;
			}
		} 
	}

	// This tests the global and local cluster mapping
	void TestGlobalAndLocalClusterMapping() {

		int local_num = 0;
		int global_num = 0;
		SClusterMap clus_map;
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			m_local_map_file.OpenReadFile(CUtility::ExtendString
				("LocalData/local_node_map", GetInstID(), ".set", i));

			while(clus_map.ReadClusterMap(m_local_map_file)) {
				int id = m_local_node_map.FindWord((char *)&clus_map.base_node, sizeof(S5Byte));
				if(m_local_node_set[id].cluster != clus_map.cluster) {
					cout<<"Local Cluster Mismatch";getchar();
				}
				local_num++;
			}

			m_global_map_file.OpenReadFile(CUtility::ExtendString
				("LocalData/global_node_map", GetInstID(), ".set", i));

			while(clus_map.ReadClusterMap(m_global_map_file)) {
				int id = m_global_node_map.FindWord((char *)&clus_map.base_node, sizeof(S5Byte));

				if(m_global_node_set[id].cluster != clus_map.cluster) {
					cout<<"Global Cluster Mismatch";getchar();
				}
				global_num++;
			}
		}

		if(global_num != m_global_node_map.Size()) {
			cout<<"Global Num Mismatch";getchar();
		}

		if(local_num != m_local_node_map.Size()) {
			cout<<"Local Num Mismatch";getchar();
		}
	}

	// This tests the pulse rank mapping 
	void TestPulseRankMapping() {

		int offset = 0;
		SPulseMap pulse_map;
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			m_pulse_rank_file.OpenReadFile(CUtility::ExtendString
				("LocalData/pulse_set", GetInstID(), ".set", i));

			while(pulse_map.ReadPulseMap(m_pulse_rank_file)) {
				int id = m_cluster_map.FindWord((char *)&pulse_map.node, sizeof(int));
				if(m_pulse_map_set[id].node != pulse_map.node) {
					cout<<"Pulse Rank Node Mismatch";getchar();
				}

				if(abs(m_pulse_map_set[id].pulse_score - pulse_map.pulse_score) > 0.000001) {
					cout<<"Pulse Rank Mismatch";getchar();
				}
				offset++;
			}
		}

		if(offset != m_cluster_map.Size()) {
			cout<<"Pulse Rank Size Mismatch";getchar();
		}
	}

	// This tests the updated node set 
	void TestNodeSet() {

		S5Byte node;
		int offset = 0;
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			m_node_set_file.OpenReadFile(CUtility::ExtendString
				("LocalData/node_set", GetInstID(), ".set", i));

			while(m_node_set_file.ReadCompObject(node)) {
				if(m_cluster_map.FindWord((char *)&node, sizeof(int)) < 0) {
					cout<<"Node Not Found";getchar();
				}
				offset++;
			}
		}

		if(offset != m_cluster_map.Size()) {
			cout<<"Node Set Size Mismatch";getchar();
		}
	}

	// This checks the hiearchy stat
	void CheckHiearchyStat(SHiearchyStat &stat) {
		int id = m_cluster_map.FindWord((char *)&stat.clus_id, sizeof(int));
		if(id < 0) {
			cout<<stat.clus_id.Value();getchar();
		}

		if(abs(m_mapped_hiearchy_stat_buff[id].pulse_score - stat.pulse_score) > 0.000001) {
			cout<<"Pulse Score Mismatch";getchar();
		}

		if(m_mapped_hiearchy_stat_buff[id].total_node_num != stat.total_node_num) {
			cout<<"Total Node Num Mismatch";getchar();
		}

		if(m_mapped_hiearchy_stat_buff[id].total_subtrees != stat.total_subtrees) {
			cout<<"Total Subtree Mismatch "<<
				m_mapped_hiearchy_stat_buff[id].total_subtrees<<" "
				<<stat.total_subtrees;getchar();
		}
	}

	// This checks the set of cluster nodes
	void CheckClusterNodes(CHDFSFile &hiearchy_file, SHiearchyStat &stat) {

		int id = m_cluster_map.FindWord((char *)&stat.clus_id, sizeof(int));

		S5Byte node;
		for(int i=0; i<stat.total_node_num; i++) {
			// This retrieves the base node id for a
			// grouped node under this cluster segment 
			m_hiearchy_file.ReadCompObject(node);
			if(m_mapped_hiearchy_node_set[id][i] != node) {
				cout<<"Node Mismatch";getchar();
			}
		}
	}

	// This checks the set of subtrees
	void CheckSubtTree(CHDFSFile &hiearchy_file, SHiearchyStat &stat) {

		int id = m_cluster_map.FindWord((char *)&stat.clus_id, sizeof(int));

		SSubTree tree;
		for(int i=0; i<stat.total_subtrees; i++) {
			// This retrieves the base node id for a
			// grouped node under this cluster segment 
			SSubTree::ReadSubTree(m_hiearchy_file, tree);
	
			if(m_mapped_hiearchy_subtree[id][i].child_num != tree.child_num) {
				cout<<"Child Num Mismatch";getchar();
			}

			if(m_mapped_hiearchy_subtree[id][i].node_num != tree.node_num) {
				cout<<"Node Num Mismatch";getchar();
			}

			if(abs(m_mapped_hiearchy_subtree[id][i].net_trav_prob - tree.net_trav_prob) > 0.000001) {
				cout<<"Trav Prob Mismatch";getchar();
			}
		}
	}

public:

	CTestGroupClusterHiearchies() {

		m_hiearchy_node_map.Initialize(4);
		m_hiearchy_stat_buff.Initialize(4);
	}

	// This loads the cluster hiearchy set before transformation
	void LoadClusterHiearchySet() {

		FindNeighbourNodes();
		static SSubTree tree;
		static SHiearchyStat stat;
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			m_hiearchy_file.OpenReadFile(CUtility::
				ExtendString(CLUSTER_HIEARCHY_DIR, GetInstID(), ".set", i));

			while(SHiearchyStat::ReadHiearchyStat(m_hiearchy_file, stat)) {
				int id = m_hiearchy_node_map.AddWord((char *)&stat.clus_id, sizeof(S5Byte));
				if(m_hiearchy_node_map.AskFoundWord()) {
					cout<<"Hiearchy Already Exists";getchar();
				}

				m_hiearchy_stat_buff.PushBack(stat);
				m_hiearchy_node_set.ExtendSize(1);
				m_hiearchy_node_set.LastElement().Initialize();
				for(int i=0; i<stat.total_node_num; i++) {
					// This retrieves the base node id for a
					// grouped node under this cluster segment 
					m_hiearchy_file.ReadCompObject(stat.clus_id);
					m_hiearchy_node_set.LastElement().PushBack(stat.clus_id);
				}

				m_hiearchy_subtree.ExtendSize(1);
				m_hiearchy_subtree.LastElement().Initialize();
				for(int i=0; i<stat.total_subtrees; i++) {
					// this transfers a subtree from one file to another
					SSubTree::ReadSubTree(m_hiearchy_file, tree);
					m_hiearchy_subtree.LastElement().PushBack(tree);
				}
			}
		}
	}

	// This is the entry function
	void TestGroupClusterHiearchies() {

		m_client_boundary.ReadMemoryChunkFromFile
			("GlobalData/ClusterHiearchy/cluster_boundary");

		LoadClusMapSet();
		MergeHiearchies();
		CreateNewClusterMapping();

		for(int i=0; i<m_mapped_hiearchy_subtree.Size(); i++) {
			m_mapped_hiearchy_subtree[i][0].node_num = 
				m_mapped_hiearchy_stat_buff[i].total_node_num;
			m_mapped_hiearchy_subtree[i][0].net_trav_prob = 
				m_mapped_hiearchy_stat_buff[i].pulse_score;
			m_mapped_hiearchy_subtree[i][0].child_num = 
				m_hiearchy_set[i].Size();
		}

		TestGlobalAndLocalClusterMapping();
		TestNodeSet();
		//TestPulseRankMapping();

		SHiearchyStat stat;
		int offset = 0;
		_int64 total_num = 0;
		CMemoryChunk<bool> hiearchy_used(m_cluster_map.Size(), false);
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			m_hiearchy_file.OpenReadFile(CUtility::
				ExtendString(CLUSTER_HIEARCHY_DIR, GetInstID(), ".set", i));

			while(SHiearchyStat::ReadHiearchyStat(m_hiearchy_file, stat)) {
				int id = m_cluster_map.FindWord((char *)&stat.clus_id, sizeof(int));
				hiearchy_used[id] = true;
				CheckHiearchyStat(stat);
				CheckClusterNodes(m_hiearchy_file, stat);
				CheckSubtTree(m_hiearchy_file, stat);
				total_num += stat.total_node_num;
				offset++;
			}
		}

		if(total_num != (m_client_boundary.LastElement() - m_client_boundary[0])) {
			cout<<"Boundary Error";getchar();
		}

		if(offset != m_cluster_map.Size()) {
			cout<<"Hiearchy Num Mismatch";getchar();
		}

		for(int i=0; i<hiearchy_used.OverflowSize(); i++) {
			if(hiearchy_used[i] == false) {
				cout<<"Hiearchy Not Found";getchar();
			}
		}
	}
};

// This is used to test for the correct operation of cluster hiearchy
class CTestClusterHiearchy : public CCommunication {

	// This stores the set of nodes
	CTrie m_node_map;
	// This stores the set of cluster maps
	CArrayList<SClusterMap> m_clus_map_buff;

	// This stores the local cluster mapping
	CSegFile m_local_clus_file;
	// This stores statistics relating ot a hiearchy
	CSegFile m_clus_stat_file;

	// This loads the set of cluster mappings
	void LoadClusMapSet() {

		SClusterMap clus_map;

		for(int i=0; i<CCommunication::WavePassClasses(); i++) {
			m_local_clus_file.OpenReadFile(CUtility::ExtendString
				("LocalData/local_clus", GetInstID(), ".set", i));

			while(clus_map.ReadClusterMap(m_local_clus_file)) {
				m_node_map.AddWord((char *)&clus_map.base_node, sizeof(S5Byte));
				if(!m_node_map.AskFoundWord()) {
					m_clus_map_buff.PushBack(clus_map);
				}
			}
		}
	}

public:

	CTestClusterHiearchy() {
		m_node_map.Initialize(4);
		m_clus_map_buff.Initialize(4);
	}

	// This is the entry function
	void TestClusterHiearchy() {

		LoadClusMapSet();

		uChar map_bytes;
		SClusterMap clus_map;
		m_clus_stat_file.OpenReadFile(CUtility::
			ExtendString("LocalData/clus_stat_file", GetInstID(), ".set0"));

		while(m_clus_stat_file.ReadCompObject(map_bytes)) {
			if(map_bytes == 0) {
				m_clus_stat_file.ReadCompObject(clus_map.base_node);
				continue;
			}

			clus_map.ReadClusterMap(m_clus_stat_file);
			int id = m_node_map.FindWord((char *)&clus_map.base_node, sizeof(S5Byte));
			if(m_clus_map_buff[id].base_node != clus_map.base_node) {
				cout<<"Base Node Mismatch";getchar();
			}

			if(m_clus_map_buff[id].cluster != clus_map.cluster) {
				cout<<"Cluster Mismatch";getchar();
			}
		}
	}
};
