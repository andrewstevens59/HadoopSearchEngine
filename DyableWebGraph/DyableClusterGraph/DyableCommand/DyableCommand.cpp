#include "./WavePass.h"

// This class is responsible for coordinating the different 
// ClusterGraph modules. This includes assigning cluster labels,
// processing the cluster hiearchy, merging link sets, mapping
// s_links and WavePass for clustering.
class CClusterGraphControl : public CCommunication {

	// This is responsible for merging and updating the link set
	CLinkCluster m_link_cluster;
	// This is responsible for updating the cluster hiearchy
	CClusterHiearchy m_hiearchy;
	// This is responsible for updating the s_link mapping
	CSummaryLinks m_s_links;
	// This is responsible for perforing wave pass
	CWavePass m_wave_pass;
	// This is responsible for creating the cluster mapping
	CAssignClusterLabel m_assign_map;

	// This performs a cluster pass on the link set
	// @param wave_pass_cycles - the number of wave pass cycles to perform
	// @param max_child_num - this is the maximum number of nodes that can
	//                      - be grouped in a given cluster group
	// @param is_test - true if the test framework is being used
	void PerformClusterPass(int wave_pass_cycles, int max_child_num, bool is_test) {

		/*if(state < 0 || state >= 0) {
			cout<<"Performing Wave Pass"<<endl;
			m_wave_pass.WavePass(wave_pass_cycles);
		}

		if(is_test == true) {
			CTestWavePass test6;
			test6.PrduceClusterAssignment(CCommunication::WavePassInstNum(), 
				wave_pass_cycles, CNodeStat::GetHashDivNum());
			test6.TestWavePass(CNodeStat::GetHashDivNum());
			test6.CheckClusterLabelAssign(CNodeStat::GetHashDivNum());
		}*/

		CTestGroupClusterHiearchies test3;
		CTestLinkCluster test4;
		CTestSummaryLinks test5; 

static int c=0;

if(c== 1) {

		cout<<"Creating Join Links"<<endl;
		m_assign_map.AssignLinkWeightClusterLabel(wave_pass_cycles, max_child_num);
}

c = 1;

		/*if(is_test == true) {
			CTestAssignClusterLabel test1;
			test1.TestAssignClusterLabel();
		}*/


		if(is_test == true) {
			test3.LoadClusterHiearchySet();
		}

		cout<<"Updating Hiearchy"<<endl;
		m_hiearchy.UpdateHiearchy(max_child_num);
	
		if(is_test == true) {
			test3.TestGroupClusterHiearchies();
			CTestClusterHiearchy test2;
			test2.TestClusterHiearchy();

			test4.LoadLinkSet();
		}

		cout<<"Updating Link Set"<<endl;
		m_link_cluster.UpdateLinkSet();

		if(is_test == true) {
			test4.TestLinkCluster();
			test5.LoadAccSLinks();
		}

		cout<<"Update SLink Mapping"<<endl;
		m_s_links.UpdateSLinkMapping();

		if(is_test == true) {
			CTestSubsumedSLinkBounds test6;
			test6.TestSubsumedSLinkBounds();
			test5.CheckAccumulatedSLinks();
			test5.TestSummaryLinks(test4.SLinkSet());
		}

		if(is_test == true) {
			CTestLinkSet test7;
			test7.LoadTestLinkSet();
		}
	}

	// This builds the entire hiearchy
	// @param wave_pass_cycles - the number of wave pass cycles to perform
	// @param max_child_num - this is the maximum number of nodes that can
	//                      - be grouped in a given cluster group
	// @param top_level_node_num - this is the maximum number of nodes that
	//                           - can exist on the top level	
	// @param is_test - true if the test framework is being used
	void CreateHiearchy(int wave_pass_cycles, int max_child_num, 
		int top_level_node_num, bool is_test) {

		_int64 link_num = CNodeStat::GetGlobalLinkNum();
		while(CNodeStat::GetCurrNodeNum() > 1000) {

			if(CNodeStat::GetHiearchyLevelNum() >= 25) {
				break;
			}

			cout<<"Level: "<<CNodeStat::GetHiearchyLevelNum()<<" ****************************"<<endl;
			cout<<"LinkNum: "<<CNodeStat::GetCurrLinkNum()<<" ****************************"<<endl;
			cout<<"NodeNum: "<<CNodeStat::GetCurrNodeNum()<<" ****************************"<<endl;
		
			PerformClusterPass(wave_pass_cycles, max_child_num, is_test);
		}

		CHDFSFile level_num_file;
		int level_num = CNodeStat::GetHiearchyLevelNum();
		level_num_file.OpenWriteFile(CUtility::ExtendString
			("GlobalData/ClusterHiearchy/level_num", GetInstID()));
		level_num_file.WriteObject(level_num);
	}

public:

	CClusterGraphControl() {
		CHDFSFile::Initialize();
	}

	// This is the entry function
	// @param prev_client_num - this is the number of link sets
	// @param wave_pass_cycles - the number of wave pass cycles
	// @param wave_pass_inst - the number of wave pass instances
	// @param wave_pass_class_num - the number of wave pass classes
	// @param top_level_node_num - this is the maximum number of nodes that
	//                           - can exist on the top level	 
	// @param max_child_num - this is the maximum number of nodes that can
	//                      - be grouped in a given cluster group
	void ClusterGraphControl(int prev_client_num, int inst_id, int inst_num, 
		int hash_div_num, bool is_test, int wave_pass_cycles, 
		int wave_pass_inst, int wave_pass_class_num, int top_level_node_num, 
		int max_child_num, int max_process_num, const char *work_dir) {

		CNodeStat::SetClientNum(1);
		CNodeStat::SetInstID(inst_id);
		CNodeStat::SetInstNum(inst_num);
		CNodeStat::SetHashDivNum(hash_div_num);
		ProcessSet().SetPort(5555 + inst_id);
		ProcessSet().SetMaximumClientNum(max_process_num);
		CMapReduce::SetMaximumProcessNum(max_process_num);
		CNodeStat::LoadNodeLinkStat();

		wave_pass_class_num = 1;
		wave_pass_cycles = 5;
		CCommunication::SetWavePass(wave_pass_inst, wave_pass_class_num);

		ProcessSet().SetWorkingDirectory(CUtility::ExtendString
			(DFS_ROOT, "DyableWebGraph/DyableClusterGraph/DyableCommand/"));

		//m_link_cluster.InitializeLinkSet(is_test, prev_client_num);
		//m_hiearchy.InitializeHiearchy(CNodeStat::GetBaseNodeNum() / top_level_node_num);

		m_hiearchy.SetMaxClusterNodeNum(CNodeStat::GetBaseNodeNum() / top_level_node_num);

		CNodeStat::SetGlobalNodeNum(CNodeStat::GetBaseNodeNum());

		CNodeStat::SetHiearchyLevelNum(5);
		CClusterHiearchy::AssignNodeNum();
		CLinkCluster::AssignLinkNum();

		CNodeStat::SetBaseNodeNum(CNodeStat::GetCurrNodeNum());
		

		CreateHiearchy(wave_pass_cycles, max_child_num, top_level_node_num, is_test);
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int prev_client_num = atoi(argv[1]);
	int inst_id = atoi(argv[2]);
	int inst_num = atoi(argv[3]);
	int hash_div_num = atoi(argv[4]);
	int wave_pass_cycles = atoi(argv[5]);
	bool is_test = atoi(argv[6]);
	int wave_pass_inst = atoi(argv[7]);
	int wave_pass_class_num = atoi(argv[8]);
	int top_level_node_num = atoi(argv[9]);
	int max_child_num = atoi(argv[10]);
	int max_process_num = atoi(argv[11]);
	const char *work_dir = argv[12];

	CBeacon::InitializeBeacon(inst_id, 2222);
	CMemoryElement<CClusterGraphControl> command;
	command->ClusterGraphControl(prev_client_num, inst_id, inst_num, 
		hash_div_num, is_test, wave_pass_cycles, wave_pass_inst, wave_pass_class_num,
		top_level_node_num, max_child_num, max_process_num, work_dir);

	command.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();

	return 0;
}
