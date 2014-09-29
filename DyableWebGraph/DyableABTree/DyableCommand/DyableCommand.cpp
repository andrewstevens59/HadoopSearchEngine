#include "./MapExcerptKeywords.h"

// This is the control class that is used to create the ABTrees from the 
// completed hiearchy. Firstly all the summary links must be sorted and
// embedded in the hiearchy at the correct location. Following this ABTrees
// are created so that they store all the summary links and pointers to
// child ABTrees. Also the number of nodes under a given ABNode is stored
// each link can be correctly located in the hiearchy.
class CABTreeControl : public CNodeStat {

	// This is used to map excerpt keywords to the hiearchy node set
	CMapExcerptKeywords m_excerpt_map;
	// This is used to spawn different processes
	CProcessSet m_process_set;
	// This stores the node boundary for different client's 
	// this allows s_links to be grouped to a client
	CMemoryChunk<_int64> m_client_boundary;

	// This finds the root node boundary
	void FindRootNodeBound() {

		int level_num;
		CHDFSFile level_num_file;
		level_num_file.OpenReadFile("GlobalData/ClusterHiearchy/level_num0");
		level_num_file.ReadObject(level_num);
		CNodeStat::SetHiearchyLevelNum(level_num);

		m_client_boundary.ReadMemoryChunkFromFile
			("GlobalData/ClusterHiearchy/cluster_boundary");
	}

	// This creates the cluster node mapping for each of the 
	// webgraph instances. 
	void CreateClusterNodeMapping() {

		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			CString arg("Index ");
			arg += i;
			arg += " ";
			arg += m_client_boundary[i];
			arg += " ";
			arg += m_client_boundary[i+1];

			m_process_set.CreateRemoteProcess("../CreateClusterNodeMap"
				"/Debug/CreateClusterNodeMap.exe", arg.Buffer(), i);
		}

		m_process_set.WaitForPendingProcesses();
	}

	// This creates the set of ab_trees
	// @param max_slink_num - this is the maximum number of s_links that can be
	//                      - grouped under any one node in the hiearchy
	void CreateABTrees(int max_slink_num) {

		for(int j=0; j<CNodeStat::GetHashDivNum(); j++) {
			CString arg("Index ");
			arg += j;
			arg += " ";
			arg += CNodeStat::GetHashDivNum();
			arg += " ";
			arg += CNodeStat::GetHiearchyLevelNum();
			arg += " ";
			arg += max_slink_num;
			arg += " ";
			arg += m_client_boundary[j];
			arg += " ";
			arg += m_client_boundary[j+1];

			m_process_set.CreateRemoteProcess("../CreateABTree"
				"/Debug/CreateABTree.exe", arg.Buffer(), j);
		}

		m_process_set.WaitForPendingProcesses();
	}

	// This is used to distribute the s_links to each of the different ab_trees
	void DistributeLinks() {

		for(int j=0; j<CNodeStat::GetHashDivNum(); j++) {
			CString arg("Index ");
			arg += j;
			arg += " ";
			arg += CNodeStat::GetHashDivNum();
			arg += " ";
			arg += CNodeStat::GetHiearchyLevelNum();

			m_process_set.CreateRemoteProcess("../DistributeSummaryLinks"
				"/Debug/DistributeSummaryLinks.exe", arg.Buffer(), j);
		}

		m_process_set.WaitForPendingProcesses();
	}

	// This remaps the s_link set base node mapping to the cluster node mapping
	void RemapSLinkSet() {

		CSegFile s_link_node_file;
		CSegFile mapped_s_link_node_file;
		CSegFile base_clus_map_file;
		for(int i=0; i<CNodeStat::GetHiearchyLevelNum(); i++) {

			base_clus_map_file.SetFileName(BACKWARD_CLUS_MAP_DIR);
			s_link_node_file.SetFileName(CUtility::ExtendString
				(SLINK_NODE_DIR, i, ".set"));
			mapped_s_link_node_file.SetFileName(CUtility::ExtendString
				("LocalData/s_link_node_set", i, ".set"));

			CMapReduce::ExternalHashMap("WriteOrderedMapsOnly", s_link_node_file, 
				base_clus_map_file, mapped_s_link_node_file, CUtility::ExtendString
				("LocalData/s_link_node_map", GetInstID()), sizeof(S5Byte), sizeof(S5Byte));
		}
	}

public:

	CABTreeControl() {
		CHDFSFile::Initialize();
	}

	// This is the entry function
	// @param hash_div_num - this is the number of hash divisions that 
	//                     - make up the cut link set
	// @param max_slink_num - this is the maximum number of s_links that can be
	//                      - grouped under any one node in the hiearchy
	void ABTreeControl(int inst_num, int hash_div_num, int max_slink_num,
		int client_num, bool is_test, int max_process_num, const char *work_dir) {

		CNodeStat::SetInstNum(inst_num);
		CNodeStat::SetClientNum(client_num);
		CNodeStat::SetHashDivNum(hash_div_num);
		CNodeStat::SetClientID(0);

		m_process_set.SetMaximumClientNum(max_process_num);
		CMapReduce::SetMaximumProcessNum(max_process_num);
		
		m_process_set.SetWorkingDirectory(CUtility::ExtendString
			(DFS_ROOT, "DyableWebGraph/DyableABTree/DyableCommand/"));

		FindRootNodeBound();
		CreateClusterNodeMapping();
		RemapSLinkSet();

		m_excerpt_map.ProcessExcerptKeywords();
		DistributeLinks();

		if(is_test == true) { 
			CTestMapBackwardClusMap test3;
			test3.TestMapBackwardClusMap();

			CTestMapExcerptKeywords test2;
			test2.LoadExcerptKeywords();
			test2.TestMapExcerptKeywords();
		}

		CreateABTrees(max_slink_num);
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int inst_num = atoi(argv[1]);
	int hash_div_num = atoi(argv[2]);
	int client_num = atoi(argv[3]);
	bool is_test = atoi(argv[4]);
	int max_process_num = atoi(argv[5]);
	int max_s_link_num = atoi(argv[6]);
	const char *work_dir = argv[7];

	CBeacon::InitializeBeacon(0, 2222);
	CMemoryElement<CABTreeControl> command;
	command->ABTreeControl(inst_num, hash_div_num, max_s_link_num,
		client_num, is_test, max_process_num, work_dir);

	command.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();

	return 0;
}
