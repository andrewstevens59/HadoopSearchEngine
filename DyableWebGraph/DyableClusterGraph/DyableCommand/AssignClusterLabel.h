#include "./ClusterHiearchy.h"

// This is responsible for assigning the cluster labels to each node once
// Wavepass is complete. This is done by selecting the top N links with the
// highet similarity and loading them into memory. Only links with a majority
// class weight that belong to a specific client are handled. Once they have
// been loaded into memory they are sorted internally. Groups of nodes are 
// then merged together using a linked list structure that requires a constant
// merging time. A limit on the accumulalative probability mass of merged nodes
// is placed on each cluster. No nodes can be merged with a node that exceeds
// it's maximum probability mass on that level. This stops nodes all being 
// merged in the same cluster. 

// Links with higher traversal probability are given higher priority when 
// merging, this may possibly prempting links with lower weight. Once cluster
// assignments have been made for each node a cluster map file is created and 
// handed off to ClusterHiearchy for further processing,
class CAssignClusterLabel : public CCommunication {

	// This stores the set of nodes that make up
	// the clustered link set
	CSegFile m_clus_node_set_file;
	// This stores the set of wave pass labels
	CSegFile m_wave_pass_set_file;

	// This takes a node set for all the join links and generates a corresponding
	// list of traversal probability for all the nodes in the set as well as a 
	// list of wave pass distributions for every node.
	void ProcessNodeList() {

		CSegFile mapped_wave_file(CUtility::ExtendString
			("LocalData/mapped_wave_pass", GetInstID(), ".set"));

		CMapReduce::ExternalHashMap(NULL, m_clus_node_set_file, m_wave_pass_set_file,
			mapped_wave_file, CUtility::ExtendString("LocalData/node2_map", GetInstID()),
			sizeof(S5Byte), sizeof(float) + sizeof(u_short));
	}

	// This passes the cluster label for each src node to all of the neighbours
	void DistributeClusterNodes() {
		
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			CString arg("Index ");
			arg += i;
			arg += " ";
			arg += CNodeStat::GetHashDivNum();

			ProcessSet().CreateRemoteProcess("../MergeClusterNodes/DistributeClusterNodes/Debug/"
				"DistributeClusterNodes.exe", arg.Buffer(), i);
		}

		ProcessSet().WaitForPendingProcesses();
	}

	// This updates the cluster label in order to merge nodes together
	void UpdateClusterMapping(bool is_last_cycle) {

		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			CString arg("Index ");
			arg += i;
			arg += " ";
			arg += CNodeStat::GetHashDivNum();
			arg += " ";
			arg += is_last_cycle;

			ProcessSet().CreateRemoteProcess("../MergeClusterNodes/UpdateClusterMapping/Debug/"
				"UpdateClusterMapping.exe", arg.Buffer(), i);
		}

		ProcessSet().WaitForPendingProcesses();

	}

	// This updates the cluster label for each node based upon the neighbouring
	// nodes cluster labels that have been passed forward.
	void AccumulateClusterNodes() {

		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			CString arg("Index ");
			arg += i;
			arg += " ";
			arg += CNodeStat::GetHashDivNum();

			ProcessSet().CreateRemoteProcess("../MergeClusterNodes/AccumulateClusterNodes/Debug/"
				"AccumulateClusterNodes.exe", arg.Buffer(), i);
		}

		ProcessSet().WaitForPendingProcesses();
	}

	// This updates the cluster label for all of the p_links
	// @param max_child_num - this is the maximum number of nodes that 
	//                      - can be grouped together during any cycle
	void AssignClusterLabel(int max_child_num) {

		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {

			CString arg("Index ");
			arg += i;
			arg += " ";
			arg += CNodeStat::GetHashDivNum();
			arg += " ";
			arg += max_child_num;

			ProcessSet().CreateRemoteProcess("../MergeClusterNodes/AssignClusterLabel/Debug/"
				"AssignClusterLabel.exe", arg.Buffer(), i);
		}

		ProcessSet().WaitForPendingProcesses();
	}

	// This creates the set of join links
	void CreateWavePassJoinLinks(int set_num) {

		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			CString arg("Index ");
			arg += i;
			arg += " ";
			arg += CNodeStat::GetHashDivNum();
			arg += " ";
			arg += set_num;
			arg += " ";
			arg += CCommunication::WavePassClasses();
			arg += " ";
			arg += CCommunication::WavePassInstNum();

			ProcessSet().CreateRemoteProcess("../CreateJoinLinks/Debug/"
				"CreateJoinLinks.exe", arg.Buffer(), i);
		}

		ProcessSet().WaitForPendingProcesses();
	}

	// This creates the clustered link set
	void CreateClusteredLinkSet() {

		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			CString arg("Index ");
			arg += i;
			arg += " ";
			arg += CNodeStat::GetHashDivNum();

			ProcessSet().CreateRemoteProcess("../MergeClusterNodes/CreateClusteredLinkSet/Debug/"
				"CreateClusteredLinkSet.exe", arg.Buffer(), i);
		}

		ProcessSet().WaitForPendingProcesses();
	}

	// This recreates the clustered link set with only maximum links included
	void UpdateClusteredLinkSet() {

		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			CString arg("Index ");
			arg += i;
			arg += " ";
			arg += CNodeStat::GetHashDivNum();

			ProcessSet().CreateRemoteProcess("../MergeClusterNodes/UpdateClusteredLinksSet/Debug/"
				"UpdateClusteredLinksSet.exe", arg.Buffer(), i);
		}

		ProcessSet().WaitForPendingProcesses();
	}

	// This creates the final clustered link set with reversed maximal links
	void CreateFinalClusteredLinkSet() {

		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			CString arg("Index ");
			arg += i;
			arg += " ";
			arg += CNodeStat::GetHashDivNum();

			ProcessSet().CreateRemoteProcess("../MergeClusterNodes/CreateFinalClusteredLinkSet/Debug/"
				"CreateFinalClusteredLinkSet.exe", arg.Buffer(), i);
		}

		ProcessSet().WaitForPendingProcesses();
	}

	// This finds the maximum link set
	void FindMaximumLinkSet() {

		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			CString arg("Index ");
			arg += i;
			arg += " ";
			arg += CNodeStat::GetHashDivNum();

			ProcessSet().CreateRemoteProcess("../MergeClusterNodes/FindMaximumLinks/Debug/"
				"FindMaximumLinks.exe", arg.Buffer(), i);
		}

		ProcessSet().WaitForPendingProcesses();
	}

public:

	CAssignClusterLabel() {
	}

	// This joins nodes together based upon a combination of the wave
	// pass distribution and the link weight. This is done when the 
	// graph is of smaller size and there is a greater link density.
	// @param max_child_num - this is the maximum number of nodes that 
	//                      - can be grouped together during any cycle
	void AssignWavePassClusterLabel(int max_child_num) {
	}

	// This join nodes together based upon the link weight only.
	// @param max_child_num - this is the maximum number of nodes that 
	//                      - can be grouped together during any cycle
	void AssignLinkWeightClusterLabel(int cycle_num, int max_child_num) {

		CreateClusteredLinkSet();
		FindMaximumLinkSet();
		UpdateClusteredLinkSet();
		CreateFinalClusteredLinkSet();

		for(int i=0; i<cycle_num; i++) {
			cout<<"Cycle "<<i<<" Out Of "<<cycle_num<<endl;
			DistributeClusterNodes();
			AccumulateClusterNodes();
			UpdateClusterMapping(i == (cycle_num - 1));
		}

		AssignClusterLabel(max_child_num);
	}
};

// This tests that the cluster mapping does actually join links together
class CTestJoinLinks {
public:

	CTestJoinLinks() {
	}

	// This is the entry function
	void TestJoinLinks(int set_num) {

		CTrie base_map(4);
		CTrie clus_map1(4);
		CArrayList<SClusterMap> clus_set(4);
	
		SClusterMap clus_map;
		CHDFSFile local_clus_file;
		for(int i=0; i<set_num; i++) {

			local_clus_file.OpenReadFile(CUtility::ExtendString
				("LocalData/local_clus", i));

			while(clus_map.ReadClusterMap(local_clus_file)) {

				clus_map1.AddWord((char *)&clus_map.cluster, sizeof(S5Byte));
				base_map.AddWord((char *)&clus_map.base_node, sizeof(S5Byte));
				if(base_map.AskFoundWord()) {
					cout<<"duplicate mapping";getchar();
				} else {
					clus_set.PushBack(clus_map);
				}
			}
		}
		
		int join_num = 0;
		int total_num = 0;
		SWLink w_link;
		CFileComp join_link_file;
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			join_link_file.OpenReadFile(CUtility::ExtendString("LocalData/join_links", i));
			cout<<i<<endl;
			while(w_link.ReadWLink(join_link_file)) {
				int src_id = base_map.FindWord((char *)&w_link.src, sizeof(S5Byte));
				int dst_id = base_map.FindWord((char *)&w_link.dst, sizeof(S5Byte));
				total_num++;

				if(src_id < 0 || dst_id < 0) {
					continue;
				}

				if(clus_set[src_id].cluster == clus_set[dst_id].cluster) {
					join_num++;
				}
			}
		}

		cout<<clus_map1.Size()<<" "<<base_map.Size();getchar();
		cout<<join_num<<" Out of "<<total_num;getchar();
	}
};

// This is a test framework used to test for the correct functionality
// of assign cluster label.
class CTestAssignClusterLabel : public CNodeStat {

	// This stores the pulse node map
	CTrie m_pulse_node_map;
	// This stores the pulse node map
	CTrie m_wave_node_map;
	// This stores the set of pulse maps
	CArrayList<SPulseMap> m_pulse_buff;
	// This stores the set of wave pass dists
	CArrayList<SWaveDist> m_wave_pass_buff;

	// This stores the set of mapped pulse ranks
	CArrayList<SPulseMap> m_mapped_pulse_buff;
	// This stores the set of mapped wave pass dists
	CArrayList<SWaveDist> m_mapped_wave_buff;

	// This stores the set of nodes that make up
	// the clustered link set
	CSegFile m_clus_node_set_file;
	// This stores the set of pulse nodes
	CSegFile m_pulse_set_file;
	// This stores the set of wave pass labels
	CSegFile m_wave_pass_set_file;

	// This loads the set of pulse ranks
	void LoadPulseRankSet() {

		SPulseMap map;
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			m_pulse_set_file.OpenReadFile(CUtility::ExtendString
				("LocalData/pulse_set", GetInstID(), ".set", i));

			while(map.ReadPulseMap(m_pulse_set_file)) {
				m_pulse_node_map.AddWord((char *)&map.node, sizeof(S5Byte));
				if(!m_pulse_node_map.AskFoundWord()) {
					m_pulse_buff.PushBack(map);
				}
			}
		}
	}

	// This loads the set of wave pass dists
	void LoadWavePassSet() {

		SWaveDist dist;
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			m_wave_pass_set_file.OpenReadFile(CUtility::ExtendString
				("LocalData/label_file", GetInstID(), ".set", i));

			while(dist.ReadWaveDist(m_wave_pass_set_file)) {
				m_wave_node_map.AddWord((char *)&dist.node, sizeof(S5Byte));
				if(!m_wave_node_map.AskFoundWord()) {
					m_wave_pass_buff.PushBack(dist);
				}
			}
		}
	}

	// This maps the pulse rank to the node set
	void MapPulseRankToNodeSet() {

		S5Byte node;
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			m_clus_node_set_file.OpenReadFile(CUtility::ExtendString
				("LocalData/clus_node_set", CNodeStat::GetInstID(), ".set", i));

			while(m_clus_node_set_file.ReadCompObject(node)) {
				int id = m_pulse_node_map.FindWord((char *)&node, sizeof(S5Byte));
				m_mapped_pulse_buff.PushBack(m_pulse_buff[id]);
			}
		}
	}

	// This maps the wave pass to the node set
	void MapWavePassToNodeSet() {

		S5Byte node;
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			m_clus_node_set_file.OpenReadFile(CUtility::ExtendString
				("LocalData/cluster_node_set", CNodeStat::GetInstID(), ".set", i));

			while(m_clus_node_set_file.ReadCompObject(node)) {
				int id = m_wave_node_map.FindWord((char *)&node, sizeof(S5Byte));
				m_mapped_wave_buff.PushBack(m_wave_pass_buff[id]);
			}
		}
	}


public:

	CTestAssignClusterLabel() {
		m_pulse_node_map.Initialize(4);
		m_wave_node_map.Initialize(4);
		m_pulse_buff.Initialize(4);
		m_wave_pass_buff.Initialize(4);

		m_mapped_pulse_buff.Initialize(4);
		m_mapped_wave_buff.Initialize(4);
	}

	// This is the entry function
	void TestAssignClusterLabel() {

		LoadWavePassSet();
		MapWavePassToNodeSet();

		SPulseMap map;
		SWaveDist dist;
		CSegFile mapped_pulse_file;
		CSegFile mapped_wave_file;
		int offset1 = 0;
		int offset2 = 0;
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			/*mapped_pulse_file.OpenReadFile(CUtility::ExtendString
				("LocalData/mapped_pulse", GetInstID(), ".set", i));

			while(map.ReadPulseMap(mapped_pulse_file)) {
				if(m_mapped_pulse_buff[offset1].node != map.node) {
					cout<<"Node Mismatch";getchar();
				}

				if(m_mapped_pulse_buff[offset1].pulse_score != map.pulse_score) {
					cout<<"Pulse Mismatch "<<m_mapped_pulse_buff[offset1].pulse_score<<" "<<map.pulse_score;getchar();
				}

				offset1++;
			}*/

			mapped_wave_file.OpenReadFile(CUtility::ExtendString
				("LocalData/mapped_wave_pass", GetInstID(), ".set", i));

			while(dist.ReadWaveDist(mapped_wave_file)) {
				if(m_mapped_wave_buff[offset2].node != dist.node) {
					cout<<"Node Mismatch";getchar();
				}

				if(m_mapped_wave_buff[offset2].cluster != dist.cluster) {
					cout<<"Clus Mismatch";getchar();
				}

				if(m_mapped_wave_buff[offset2].majority_weight != dist.majority_weight) {
					cout<<"Majority Weight Mismatch";getchar();
				}
				offset2++;
			}
		}

		if(offset2 != m_mapped_wave_buff.Size()) {
			cout<<"Wave Size Mismatch";getchar();
		}
	}
};