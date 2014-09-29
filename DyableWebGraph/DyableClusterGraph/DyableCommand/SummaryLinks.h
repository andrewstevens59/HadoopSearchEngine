#include "./TestSubsumedSLinkBounds.h"

// In order to approximate the webgraph using a compressed graph, 
// it's necessary to store a number of summary links for each merged 
// node. A summary link is a link with high traversal probability. 
// Due to the large number of links attatched to a node as a result 
// of continued compression, it's not possible to store all the 
// links for a merged node. Instead only a select number of links
// are chosen to represent a certain percentage of the traversal
// probability. It's also highly likely that links will be merged
// together in the compression process giving them higher weight.
// Merged links are therefore more likely to be chosen as summary 
// links as the compression continues. Note that a summary link 
// could be a forward or backward link.

// The LocalData structure used to store cut links is as follows. Each
// cut link is stored along with it's node in a link cluster. This
// means that only the dst node has to be stored for a link. Now
// the dst node stored is the cluster index. This means that each
// node must in turn store it's cluster index along with its 
// traversal probability. The size of the link cluster is stored
// along with the src node sequentially in memory. Link clusters
// are also packed into blocks of external memory in a semi-breadth
// first fashion allowing efficeint access to a link cluster

// The approach taken to embed summary links in nodes based upon 
// their traversal probability is as follows. Every time a link
// is merged, a new summary link is created. The creation level
// is the level at which a link is first merged. All original
// links have a creation level of zero. When a link is subsumed
// becuase the two nodes it joins have been merged then the subsume
// level needs to be recorded. A summary link cannot exist any
// higher then its subsume level. When a summary link is subsumed
// it needs to be recorded. Each subsumed link needs to be matched
// up to its creation link counterpart. This is so each summary 
// link can be sorted appropriately. 
class CSummaryLinks : public CCommunication {

	// This stores the s_link node file
	CSegFile m_s_link_node_file;
	// This stores the base cluster mapping
	CSegFile m_base_clus_map_file;

public:

	CSummaryLinks() {
	}

	// This takes the set of existing s_links and updates the cluster 
	// mapping for each node. A supplied cluster map file is needed 
	// to know what to map which node to. An external hash map is used
	// to update the entire node set.
	// @param map_file - this stores the mapping between each node and
	//                 - it's updated cluster mapping
	// @param CNodeStat::GetHashDivNum() - the number of hash division
	void UpdateSLinkMapping() {

		m_base_clus_map_file.SetFileName(CUtility::ExtendString
			("LocalData/base_node_map", GetInstID(), ".set"));
		m_s_link_node_file.SetFileName(CUtility::ExtendString
			(SLINK_NODE_DIR, CNodeStat::GetHiearchyLevelNum(), ".set"));

		CMapReduce::ExternalHashMap("WriteOrderedMapsOnly", m_s_link_node_file, 
			m_base_clus_map_file, m_s_link_node_file, CUtility::ExtendString
			("LocalData/s_link_node_map", GetInstID()), sizeof(S5Byte), sizeof(S5Byte));

		CNodeStat::SetHiearchyLevelNum(CNodeStat::GetHiearchyLevelNum() + 1);
	}
};

// This is used to test for the correct functionality of s_link 
// creation and mapping after each compression cycle.
class CTestSummaryLinks : public CNodeStat {

	// This stores the set of s_links
	CTrie m_s_link_map;
	// This stores the id of unique s_links
	CArrayList<int> m_s_link_id;

	// This stores the accumulated s_link node set
	CArrayList<S5Byte> m_acc_node_set;
	// This stores the global cluster mapping
	CTrie m_global_node_map;
	// This stores the set of global nodes
	CArrayList<SClusterMap> m_global_node_set;
	// This stores all the s_links that belong to this client
	CHDFSFile m_acc_s_link_file;

	// This checks the s_links for both the accumulative file and also the current file
	void CheckSLinks(CHDFSFile &s_link_file, _int64 &check_sum,
		CArrayList<SSummaryLink> &s_link_set) {

		SSummaryLink s_link;
		while(s_link.ReadSLink(s_link_file)) {
			int id = m_s_link_map.FindWord((char *)&s_link, 10);
			id = m_s_link_id[id];

			check_sum += s_link.src.Value() + s_link.dst.Value();

			if(s_link.create_level != s_link_set[id].create_level) {
				cout<<"Create Level Mismatch";getchar();
			}

			if(s_link.src.Value() != s_link_set[id].src.Value()) {
				cout<<"src Mismatch";getchar();
			}

			if(s_link.dst.Value() != s_link_set[id].dst.Value()) {
				cout<<"dst Mismatch";getchar();
			}

			if(abs(s_link.trav_prob - s_link_set[id].trav_prob) > 0.000001) {
				cout<<"Traversal Probability Mismatch "<<s_link.trav_prob
					<<" "<<s_link_set[id].trav_prob;getchar();
			}

			if(s_link.subsume_level != s_link_set[id].subsume_level) {
				cout<<"Subsume Level Mismatch";getchar();
			}
		}
	}

	// This remaps the s_links to the new cluster mapping
	void RemapSLinks(CArrayList<SSummaryLink> &s_link_set) {

		SBLink b_link;
		_int64 check_sum = 0;
		SSummaryLink s_link;
		m_s_link_id.Initialize(4);
		for(int i=0; i<s_link_set.Size(); i++) {
			SSummaryLink &s_link = s_link_set[i];
			check_sum += s_link.src.Value() + s_link.dst.Value();

			m_s_link_map.AddWord((char *)&s_link, 10);
			if(!m_s_link_map.AskFoundWord()) {
				m_s_link_id.PushBack(i);	
			}
		}

		CHDFSFile s_link_file;
		_int64 comp_check_sum = 0;
		for(int i=0; i<CNodeStat::GetHashDivNum() << 1; i++) {
			s_link_file.OpenReadFile(CUtility::ExtendString
				(ACC_S_LINK_DIR, CNodeStat::GetHiearchyLevelNum()-1, ".set", i));
			CheckSLinks(s_link_file, comp_check_sum, s_link_set);
		}

		if(check_sum != comp_check_sum) {
			cout<<"Checksum Error4 "<<check_sum<<" "<<comp_check_sum;getchar();
		}
	}

	// This checks the s_links for duplicates
	void CheckSLinkDuplicates(CArrayList<SSummaryLink> &s_link_set) {

		CTrie s_link_lookup(4);
		for(int i=0; i<s_link_set.Size(); i++) {
			SSummaryLink &s_link = s_link_set[i];
			s_link_lookup.AddWord((char *)&s_link, 11);
			if(s_link_lookup.AskFoundWord()) {
				cout<<"Duplicate SLink Error "<<
					s_link.src.Value()<<" "<<s_link.dst.Value()<<" "<<(int)s_link.create_level<<" "<<i;getchar();
			}
		}
	}

public:

	CTestSummaryLinks() {
	}

	// This loads the accumulated s_links
	void LoadAccSLinks() {

		S5Byte node;
		CHDFSFile node_file;
		m_acc_node_set.Initialize();

		for(int i=0; i<CNodeStat::GetHashDivNum() << 1; i++) {
			node_file.OpenReadFile(CUtility::ExtendString
				(SLINK_NODE_DIR, CNodeStat::GetHiearchyLevelNum(), ".set", i));

			while(node_file.ReadCompObject(node)) {
				m_acc_node_set.PushBack(node);
			}
		}
	}

	// This checks the s_link mapping for accumulated s_links
	void CheckAccumulatedSLinks() {

		m_global_node_map.Initialize(4);
		SClusterMap clus_map;
		CHDFSFile global_clus_map_file;
		m_global_node_set.Initialize();
		for(int i=0; i<GetHashDivNum(); i++) {
			global_clus_map_file.OpenReadFile(CUtility::ExtendString
				("LocalData/base_node_map", GetInstID(), ".set", i));

			while(clus_map.ReadClusterMap(global_clus_map_file)) {
				m_global_node_map.AddWord((char *)&clus_map.base_node, sizeof(S5Byte));
				m_global_node_set.PushBack(clus_map);
			}
		}

		for(int i=0; i<m_acc_node_set.Size(); i++) {
			int id = m_global_node_map.FindWord((char *)&m_acc_node_set[i], sizeof(S5Byte));
			m_acc_node_set[i] = m_global_node_set[id].cluster;
		}

		S5Byte node;
		int offset = 0;
		CSegFile node_file;

		for(int i=0; i<CNodeStat::GetHashDivNum() << 1; i++) {
			node_file.OpenReadFile(CUtility::ExtendString
				(SLINK_NODE_DIR, CNodeStat::GetHiearchyLevelNum()-1, ".set", i));

			while(node_file.ReadCompObject(node)) {
				if(node != m_acc_node_set[offset++]) {
					cout<<"Node Mismatch";getchar();
				}
			}
		}
	}

	// This is the entry function
	void TestSummaryLinks(CArrayList<SSummaryLink> &s_link_set) {

		m_s_link_map.Initialize(4);
		m_s_link_id.Initialize(4);
	
		CheckSLinkDuplicates(s_link_set);
		RemapSLinks(s_link_set);
	}
};

// This class remaps the test links set originally present using 
// the global cluster mapping. It then checks that the composed 
// set of s_links and c_links are all accounted for in the test
// link set.
class CTestLinkSet : public CNodeStat {

	// This stores the global cluster mapping
	CTrie m_node_map;
	// This stores the cluster mapping
	CArrayList<SClusterMap> m_cluster_map;
	// This stores the link weight
	CArrayList<float> m_link_weight;
	// This stores the set of test links
	CTrie m_test_link_set;
	// This is true if a test link has been found
	CArrayList<bool> m_test_link_used;

	// This loads the base node mapping
	void LoadBaseNodeMapping() {

		SClusterMap clus_map;
		m_node_map.Initialize(4);
		m_cluster_map.Initialize(4);
		CHDFSFile global_map_file;
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			global_map_file.OpenReadFile(CUtility::ExtendString
				("LocalData/base_node_map", GetInstID(), ".set", i));

			while(clus_map.ReadClusterMap(global_map_file)) {
				m_node_map.AddWord((char *)&clus_map.base_node, sizeof(S5Byte));
				if(m_node_map.AskFoundWord()) {
					cout<<"Node Already Exists";getchar();
				}
				m_cluster_map.PushBack(clus_map);
			}
		}
	}

	// This checks the c_links
	void CheckCLinks() {

		SClusterLink c_link;
		CHDFSFile c_link_file;
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			c_link_file.OpenReadFile(CUtility::ExtendString
				("GlobalData/LinkSet/bin_link_set", GetInstID(), ".set", i));

			while(c_link.ReadLink(c_link_file)) {
				SBLink &b_link = c_link.base_link;
				int src_id = m_node_map.FindWord((char *)&b_link.src, sizeof(S5Byte));
				int dst_id = m_node_map.FindWord((char *)&b_link.dst, sizeof(S5Byte));
				b_link.src = m_cluster_map[src_id].cluster;
				b_link.dst = m_cluster_map[dst_id].cluster;

				int id = m_test_link_set.FindWord((char *)&b_link, sizeof(SBLink));
				if(id < 0) {
					continue;
				}

				if(m_test_link_used[id] == true) {
					cout<<"link already used";getchar();
				}

				m_test_link_used[id] = true;
			}
		}
	}

	// This checks the s_links
	void CheckSLinks() {

		SBLink b_link;
		CHDFSFile s_link_node_file;

		for(int j=0; j<CNodeStat::GetHiearchyLevelNum(); j++) {
			for(int i=0; i<CNodeStat::GetHashDivNum() << 1; i++) {
				s_link_node_file.OpenReadFile(CUtility::ExtendString
					(SLINK_NODE_DIR, j, ".set", i));

				while(b_link.ReadBinaryLink(s_link_node_file)) {

					int id = m_test_link_set.FindWord((char *)&b_link, sizeof(SBLink));
					if(id < 0) {
						continue;
					}

					m_test_link_used[id] = true;
				}
			}
		}
	}

public:

	CTestLinkSet() {
	}

	// This loads the test link set
	void LoadTestLinkSet() {

		LoadBaseNodeMapping();
		m_test_link_set.Initialize(4);
		m_test_link_used.Initialize(4);
		SBLink b_link;

		CHDFSFile test_link_file;
		test_link_file.OpenReadFile(CUtility::ExtendString
			("GlobalData/LinkSet/test_link_set", (int)0));

		float link_weight;
		while(b_link.ReadBinaryLink(test_link_file)) {
			test_link_file.ReadCompObject(link_weight);
			m_test_link_set.AddWord((char *)&b_link, sizeof(SBLink));
			if(!m_test_link_set.AskFoundWord()) {
				m_test_link_used.PushBack(false);
			}
		}

		CheckCLinks();
		CheckSLinks();

		for(int i=0; i<m_test_link_used.Size(); i++) {
			SBLink b_link = *(SBLink *)m_test_link_set.GetWord(i);
			if(m_test_link_used[i] == false) {
				cout<<"Test Link Not Found "<<b_link.src.Value()<<" "<<b_link.dst.Value();getchar();
			}
		}

	}
};