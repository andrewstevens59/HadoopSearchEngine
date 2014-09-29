#include "./SummaryLinks.h"

// This is used once to create the link set initially for each client.
// Due to the way reduce link works it's necessary to store the traversal
// probability explicity for each dst node. This is so link weights can
// be superimposed when links are merged. Also there is no continuous
// node spectrum assigned to a client. Instead hashing is used to hash
// nodes to individual clients.	When the link set is updated and nodes
// removed, then nodes are further hashed on their new cluster assignment.
// The link set starts off with the initial base nodes but these are 
// later replaced with their hiearchial cluster assignments.

// This class is also used to update the link set when new cluster mappings
// have been created. This is done in parallel using an external hash map.
// The cluster mapping itself does not use a continous node spectrum, instead
// cluster node mappings falls on the boundary between existing global node
// boundaries. This means that gaps must be left between node assignments that
// respect the number of nodes that have been merged under a given cluster.

// Before the link set can be processed it must be broken up into a binary link
// set and any duplicate links must be handled. A duplicate link is the same 
// link that exists between any two nodes. Duplicate links are merged together
// and the individual traversal probabilities added together.
class CLinkCluster : public	CCommunication {

	// This is used to store the test link set used
	// in retrieval to load the webgraph into memory
	CSegFile m_test_link_set_file;

	// This takes the set of hashed binary links and 
	// merges them based upon their src node
	void MergeLinkClusters() {

		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			CString arg("Index ");
			arg += i;
			arg += " ";
			arg += CNodeStat::GetHashDivNum();
			arg += " ";
			arg += CNodeStat::GetInstID();
			arg += " ";
			arg += CNodeStat::GetHiearchyLevelNum();

			ProcessSet().CreateRemoteProcess("../MergeLinkClusters/Debug/"
				"MergeLinkClusters.exe", arg.Buffer(), i);
		}

		ProcessSet().WaitForPendingProcesses();
	}

	// This takes the set of mapped binary links are hashes them 
	// based upon their src node for further processing
	void DistributeLinkClusters() {

		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			CString arg("Index ");
			arg += i;
			arg += " ";
			arg += CNodeStat::GetHashDivNum();
			arg += " ";
			arg += CNodeStat::GetInstID();
			arg += " ";
			arg += CNodeStat::GetHiearchyLevelNum();

			ProcessSet().CreateRemoteProcess("../DistributeLinkClusters/Debug/"
				"DistributeLinkClusters.exe", arg.Buffer(), i);
		}

		ProcessSet().WaitForPendingProcesses();
	}

	// This initializes the various attributes
	void CreateLinkSet(int prev_client_num) {

		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			CString arg("Index ");
			arg += i;
			arg += " ";
			arg += prev_client_num;
			arg += " ";
			arg += CNodeStat::GetInstID();
			arg += " ";
			arg += CNodeStat::GetHashDivNum();
			arg += " ";
			arg += CNodeStat::GetBaseNodeNum();

			ProcessSet().CreateRemoteProcess("../CreateLinkSet/Debug/"
				"CreateLinkSet.exe", arg.Buffer(), i);
		}

		ProcessSet().WaitForPendingProcesses();
	}

	 // This creates the test link set 
	 void CreateTestLinkSet() {

		m_test_link_set_file.OpenWriteFile(CUtility::ExtendString
			("GlobalData/LinkSet/test_link_set", (int)0));

		SClusterLink c_link;
		CHDFSFile bin_link_set;
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			cout<<"TestLinkSet "<<i<<" Out Of "<<CNodeStat::GetHashDivNum()<<endl;
			bin_link_set.OpenReadFile(CUtility::ExtendString
				("GlobalData/LinkSet/bin_link_set0", ".set", i));

			while(c_link.ReadLink(bin_link_set)) {
				SClusterLink::WriteBaseLink(m_test_link_set_file, c_link);
				m_test_link_set_file.WriteCompObject(c_link.link_weight);
			}
		}

		m_test_link_set_file.CloseFile();
	}

public:

	CLinkCluster() {
		CNodeStat::SetHiearchyLevelNum(0);
	}

	// This is the entry function called to create the updated
	// link set with link traversal probabilities added.
	// @param prev_client_num - the previous number of clients
	//                        - used to create the clustered link set
	void InitializeLinkSet(bool is_test, int prev_client_num) {

		CreateLinkSet(prev_client_num);
		AssignLinkNum();

		if(is_test == true) {
			CreateTestLinkSet();
		}
	}

	// This determines the new link number across the entire set
	static void AssignLinkNum() {

		 _int64 link_num;
		 _int64 net_link_num = 0;
		 CHDFSFile link_num_file;
		 for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			 link_num_file.OpenReadFile(CUtility::ExtendString
				("LocalData/link_num", CNodeStat::GetInstID(), ".set", i));
			 link_num_file.ReadObject(link_num);
			 net_link_num += link_num;
		 }

		 CNodeStat::SetCurrLinkNum(net_link_num);
	 }

	// This is called to merge link clusters based upon their
	// src cluster mapping. Each node in the set of binary 
	// links must be remapped based upon the updated cluster mapping.
	void UpdateLinkSet() {

		CSegFile global_map_file(CUtility::ExtendString
			("LocalData/global_node_map", GetInstID(), ".set"));
		CSegFile local_map_file(CUtility::ExtendString
			("LocalData/local_node_map", GetInstID(), ".set"));

		CSegFile base_bin_links(CUtility::ExtendString
			("LocalData/base_node_set", GetInstID(), ".set"));
		CSegFile clus_bin_links(CUtility::ExtendString
			("LocalData/cluster_node_set", GetInstID(), ".set"));

		CMapReduce::ExternalHashMap("WriteOrderedMapsOnly", base_bin_links, global_map_file,
			base_bin_links, CUtility::ExtendString("LocalData/map_base_node", GetInstID()),
			sizeof(S5Byte), sizeof(S5Byte));

		CMapReduce::ExternalHashMap("WriteOrderedMapsOnly", clus_bin_links, local_map_file,
			clus_bin_links, CUtility::ExtendString("LocalData/map_cluster_node", GetInstID()), 
			sizeof(S5Byte), sizeof(S5Byte));

		DistributeLinkClusters();
		MergeLinkClusters();
		AssignLinkNum();
	}
};

// This class is used to test the correct functionality of MergeLinkSet.
// In particular it checks that link clusters are merged correctly and 
// the correct set of s_links are created. Also it checks that duplicate
// links are merged and the link weight is updated correctly.
class CTestLinkCluster : public CNodeStat {

	// This stores the set of nodes
	CTrie m_node_map;
	// This stores the set of cluster maps
	CArrayList<SClusterMap> m_clus_map_buff;
	// This stores the set of mapped nodes
	CArrayList<SClusterMap> m_mapped_clus_buff;

	// This stores the set of cluster links
	CArrayList<SClusterLink> m_clus_link_set;
	// This stores the set of s_links for this link set
	CArrayList<SSummaryLink> m_s_link_set;
	
	// This stores the checksum for the link set
	_int64 m_check_sum;
	// This stores each of the links in the updated link set 
	CTrie m_link_lookup;

	// This stores the local cluster mapping
	CSegFile m_local_clus_file;
	// This stores statistics relating ot a hiearchy
	CSegFile m_clus_stat_file;

	// This loads the set of cluster mappings
	void LoadClusMapSet(const char dir[]) {

		m_node_map.Initialize(4);
		m_clus_map_buff.Initialize(4);

		SClusterMap clus_map;
		CHDFSFile clus_map_file;
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			clus_map_file.OpenReadFile(CUtility::ExtendString
				(dir, GetInstID(), ".set", i));

			while(clus_map.ReadClusterMap(clus_map_file)) {
				m_node_map.AddWord((char *)&clus_map.base_node, sizeof(S5Byte));
				if(!m_node_map.AskFoundWord()) {
					m_clus_map_buff.PushBack(clus_map);
				}
			}
		}
	}

	// This applies the mapping to the node set 
	void ApplyMapping(const char dir[]) {

		S5Byte node;
		SClusterMap clus_map;
		CHDFSFile node_set_file;
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			node_set_file.OpenReadFile(CUtility::ExtendString
				(dir, GetInstID(), ".set", i));

			while(node_set_file.ReadCompObject(node)) {
				int id = m_node_map.FindWord((char *)&node, sizeof(S5Byte));
				m_mapped_clus_buff.PushBack(m_clus_map_buff[id]);
			}
		}
	}

	// This checks the mapped set output
	void CheckMappedOutput() {

		S5Byte node;
		int offset = 0;
		CHDFSFile output_file;
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			output_file.OpenReadFile(CUtility::ExtendString
				("LocalData/cluster_node_set", GetInstID(), ".set", i));

			while(output_file.ReadCompObject(node)) {
				if(m_mapped_clus_buff[offset].cluster != node) {
					cout<<"Clus Map Mismatch2";getchar();
				}
				offset++;
			}
		}

		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			output_file.OpenReadFile(CUtility::ExtendString
				("LocalData/base_node_set", GetInstID(), ".set", i));

			while(output_file.ReadCompObject(node)) {
				if(m_mapped_clus_buff[offset].cluster != node) {
					cout<<"Clus Map Mismatch1";getchar();
				}
				offset++;
			}
		}

		if(offset != m_mapped_clus_buff.Size()) {
			cout<<"Map Num Mismatch";getchar();
		}
	}

	// This loads the cluster link set
	void LoadClusterLinkSet() {
		
		SClusterLink c_link;
		CHDFSFile bin_link_set;
		m_clus_link_set.Initialize();
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			bin_link_set.OpenReadFile(CUtility::ExtendString
				("GlobalData/LinkSet/bin_link_set", CNodeStat::GetInstID(), ".set", i));

			while(c_link.ReadLink(bin_link_set)) {
				m_clus_link_set.PushBack(c_link);
			}
		}
	}

	// This applies the cluster mapping to the base link nodes
	void ApplyMappingToBaseNodes() {

		for(int i=0; i<m_clus_link_set.Size(); i++) {
			SClusterLink &c_link = m_clus_link_set[i];
			int id1 = m_node_map.FindWord((char *)&c_link.base_link.src, sizeof(S5Byte));
			int id2 = m_node_map.FindWord((char *)&c_link.base_link.dst, sizeof(S5Byte));
			c_link.base_link.src = m_clus_map_buff[id1].cluster;
			c_link.base_link.dst = m_clus_map_buff[id2].cluster;
		}
	}

	// This applies the cluster mapping to cluster nodes
	void ApplyMappingToClusterNodes() {

		for(int i=0; i<m_clus_link_set.Size(); i++) {
			SClusterLink &c_link = m_clus_link_set[i];
			int id1 = m_node_map.FindWord((char *)&c_link.cluster_link.src, sizeof(S5Byte));
			int id2 = m_node_map.FindWord((char *)&c_link.cluster_link.dst, sizeof(S5Byte));
			c_link.cluster_link.src = m_clus_map_buff[id1].cluster;
			c_link.cluster_link.dst = m_clus_map_buff[id2].cluster;
		}
	}

	// This checks for duplicate s_links
	void CheckDuplicateLinks() {

		CTrie duplicate_map(4);
		for(int i=0; i<m_clus_link_set.Size(); i++) {
			SClusterLink &c_link = m_clus_link_set[i];
			duplicate_map.AddWord((char *)&c_link.base_link, sizeof(S5Byte) * 2);
			if(duplicate_map.AskFoundWord()) {
				cout<<"Duplicate Link Found "<<c_link.base_link.src.Value()
					<<" "<<c_link.base_link.dst.Value();getchar();
			}
		}
	}

	// This creates the set of s_links and the updated link set 
	void CreateUpdatedLinkSet() {

		CArrayList<float> link_weight(4);
		CArrayList<bool> assigned(4);
		m_s_link_set.Initialize();
		m_link_lookup.Initialize(4);
		CArrayList<SClusterLink> temp_link_set(m_clus_link_set.Size());

		CTrie link_map(4);
		SSummaryLink s_link;
		for(int i=0; i<m_clus_link_set.Size(); i++) {
			SBLink &b_link = m_clus_link_set[i].cluster_link;

			if(b_link.src.Value() == b_link.dst.Value()) {
				// add as a summary link
				s_link.src = m_clus_link_set[i].base_link.src;
				s_link.dst = m_clus_link_set[i].base_link.dst;
				s_link.subsume_level = (uChar)CNodeStat::GetHiearchyLevelNum();
				s_link.trav_prob = m_clus_link_set[i].link_weight;
				s_link.create_level = m_clus_link_set[i].c_level;
				m_s_link_set.PushBack(s_link);
				continue;
			}

			int id = link_map.AddWord((char *)&b_link, sizeof(SBLink));
			if(link_map.AskFoundWord()) {
				link_weight[id] += m_clus_link_set[i].link_weight;
				assigned[id] = true;
			} else {
				link_weight.PushBack(m_clus_link_set[i].link_weight);
				assigned.PushBack(false);
			}
		}

		m_check_sum = 0;
		SClusterLink c_link;
		CMemoryChunk<bool> taken(link_map.Size(), false);
		for(int i=0; i<m_clus_link_set.Size(); i++) {
			SBLink &b_link = m_clus_link_set[i].cluster_link;

			if(b_link.src.Value() == b_link.dst.Value()) {
				continue;
			}

			int id = link_map.FindWord((char *)&b_link, sizeof(SBLink));
			if(assigned[id]) {

				// add a s_link to summarize all merged links
				s_link.src = m_clus_link_set[i].base_link.src;
				s_link.dst = m_clus_link_set[i].base_link.dst;
				s_link.subsume_level = (uChar)CNodeStat::GetHiearchyLevelNum();
				s_link.trav_prob = m_clus_link_set[i].link_weight;
				s_link.create_level = m_clus_link_set[i].c_level;
				m_s_link_set.PushBack(s_link);

				// more than one link has been merged
				if(!taken[id]) {

					taken[id] = true;
					c_link.base_link = m_clus_link_set[i].cluster_link;
					c_link.cluster_link = c_link.base_link;
					c_link.link_weight = link_weight[id];
					c_link.c_level = (uChar)CNodeStat::GetHiearchyLevelNum() + 1;

					SBLink &b_link = c_link.base_link;
					m_link_lookup.AddWord((char *)&b_link, 10);
					// add a new link to summarize
					temp_link_set.PushBack(c_link);
					m_check_sum += c_link.base_link.src.Value()
						+ c_link.base_link.dst.Value();
				}
			} else {

				SBLink &b_link = m_clus_link_set[i].base_link;
				m_link_lookup.AddWord((char *)&b_link, 10);
				// no link has been merged
				temp_link_set.PushBack(m_clus_link_set[i]);
				m_check_sum += m_clus_link_set[i].base_link.src.Value()
					+ m_clus_link_set[i].base_link.dst.Value();
			}
		}

		m_clus_link_set.MakeArrayListEqualTo(temp_link_set);
	}

public:

	CTestLinkCluster() {
	}

	// This loads the set 
	void LoadLinkSet() {
		m_mapped_clus_buff.Initialize(4);

		LoadClusterLinkSet();

		if(m_clus_link_set.Size() != CNodeStat::GetCurrLinkNum()) {
			cout<<"LinkNum Mismatch1 "<<m_clus_link_set.Size()<<" "
				<<CNodeStat::GetCurrLinkNum();getchar();
		}

		LoadClusMapSet("LocalData/local_node_map");
		ApplyMapping("LocalData/cluster_node_set");

		ApplyMappingToClusterNodes();

		LoadClusMapSet("LocalData/global_node_map");
		ApplyMapping("LocalData/base_node_set");

		ApplyMappingToBaseNodes();
	}

	// This returns the set of s_links
	inline CArrayList<SSummaryLink> &SLinkSet() {
		return m_s_link_set;
	}

	// This is the entry function
	void TestLinkCluster() {

		CheckDuplicateLinks();
		CreateUpdatedLinkSet();
		int offset = 0;

		if(m_clus_link_set.Size() != CNodeStat::GetCurrLinkNum()) {
			cout<<"LinkNum Mismatch "<<m_clus_link_set.Size()<<" "
				<<CNodeStat::GetCurrLinkNum();getchar();
		}

		SClusterLink c_link;
		CHDFSFile link_set_file;
		_int64 compare_check_sum = 0;
		CMemoryChunk<bool> is_link_used(m_clus_link_set.Size(), false);
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			link_set_file.OpenReadFile(CUtility::ExtendString
				("GlobalData/LinkSet/bin_link_set", CNodeStat::GetInstID(), ".set", i));

			while(c_link.ReadLink(link_set_file)) {
				SBLink &b_link = c_link.base_link;
				int id = m_link_lookup.FindWord((char *)&b_link, 10);
				compare_check_sum += b_link.src.Value() + b_link.dst.Value();
				offset++;

				if(is_link_used[id] == true) {
					cout<<"Link Already Used";getchar();
				}

				is_link_used[id] = true;

				if(id < 0) {
					cout<<b_link.src.Value()<<" "<<b_link.dst.Value();getchar();
				}

				if(abs(m_clus_link_set[id].link_weight - c_link.link_weight) > 0.000001) {
					cout<<m_clus_link_set[id].cluster_link.src.Value()<<" "<<m_clus_link_set[id].cluster_link.dst.Value();getchar();
					cout<<c_link.cluster_link.src.Value()<<" "<<c_link.cluster_link.dst.Value();getchar();
					cout<<"Link Weight Mismatch "<<m_clus_link_set[id].link_weight<<" "<<c_link.link_weight;getchar();
				}

				if(m_clus_link_set[id].base_link.src.Value() != c_link.base_link.src.Value()) {
					cout<<"Base Link Mismatch";getchar();
				}

				if(m_clus_link_set[id].base_link.dst.Value() != c_link.base_link.dst.Value()) {
					cout<<"Base Link Mismatch";getchar();
				}

				if(m_clus_link_set[id].c_level != c_link.c_level) {
					cout<<"c level Mismatch";getchar();
				}

				if(m_clus_link_set[id].cluster_link.src.Value() != c_link.cluster_link.src.Value()) {
					cout<<"Cluster Link Mismatch";getchar();
				}

				if(m_clus_link_set[id].cluster_link.dst.Value() != c_link.cluster_link.dst.Value()) {
					cout<<"Cluster Link Mismatch";getchar();
				}
			}
		}

		for(int i=0; i<is_link_used.OverflowSize(); i++) {
			if(is_link_used[i] == false) {
				cout<<"Link Not Found "<<m_clus_link_set[i].base_link.src.Value()
					<<" "<<m_clus_link_set[i].base_link.dst.Value();getchar();
			}
		}

		if(offset != m_clus_link_set.Size()) {
			cout<<"CLink Num Mismatch";getchar();
		}

		if(m_check_sum != compare_check_sum) {
			cout<<"Checksum Error "<<m_check_sum<<" "<<compare_check_sum;getchar();
		}
	}
};