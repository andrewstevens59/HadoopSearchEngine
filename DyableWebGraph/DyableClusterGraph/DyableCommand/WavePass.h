#include "./AssignClusterLabel.h"

// This class is responsible for performing wave pass on a 
// given cluster link set. This has already been implemented
// so each individual executable must be spawned
class CWavePass : public CCommunication {

	// This distribute the wave pass distribution to neighbours
	void DistributeWavePass() {

		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			CString arg("Index ");
			arg += i;
			arg += " ";
			arg += CNodeStat::GetHashDivNum();

			ProcessSet().CreateRemoteProcess("../WavePass/DistributeWavePass/Debug/"
				"DistributeWavePass.exe", arg.Buffer(), i);
		}

		ProcessSet().WaitForPendingProcesses();
	}

	// This updates the back buffer
	void UpdateBackBuffer(bool is_add_external) {

		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			CString arg("Index ");
			arg += i;
			arg += " ";
			arg += CNodeStat::GetHashDivNum();
			arg += " ";
			arg += is_add_external;
			arg += " ";
			arg += CNodeStat::GetBaseNodeNum();

			ProcessSet().CreateRemoteProcess("../WavePass/AccumulateHashDivision/Debug/"
				"AccumulateHashDivision.exe", arg.Buffer(), i);
		}

		ProcessSet().WaitForPendingProcesses();
	}

	// This updates wave pass history
	void UpdateWavePassHistory(bool update_label, int class_bit_width) {

		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			CString arg("Index ");
			arg += i;
			arg += " ";
			arg += CNodeStat::GetHashDivNum();
			arg += " ";
			arg += class_bit_width;
			arg += " ";
			arg += update_label;
			arg += " ";
			arg += CCommunication::WavePassClasses();
			arg += " ";
			arg += CNodeStat::GetBaseNodeNum();

			ProcessSet().CreateRemoteProcess("../WavePass/WavePassHistory/Debug/"
				"WavePassHistory.exe", arg.Buffer(), i);
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
			arg += " ";
			arg += CCommunication::WavePassClasses();

			ProcessSet().CreateRemoteProcess("../CreateClusteredLinkSet/Debug/"
				"CreateClusteredLinkSet.exe", arg.Buffer(), i);
		}

		ProcessSet().WaitForPendingProcesses();
	}

	// This finds the net class distribution
	void FindNetClassDist() {

		CMemoryChunk<float> final_class_weight(CCommunication::WavePassClasses(), 0);
		CMemoryChunk<float> temp_class_weight;
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			temp_class_weight.ReadMemoryChunkFromFile(CUtility::ExtendString
				("LocalData/class_weight", i));

			for(int j=0; j<CCommunication::WavePassClasses(); j++) {
				final_class_weight[j] += temp_class_weight[j];
			}
		}

		CMath::NormalizeVector(final_class_weight.Buffer(), CCommunication::WavePassClasses());
		for(int j=0; j<CCommunication::WavePassClasses(); j++) {
			final_class_weight[j] = (1 + final_class_weight[j]) * 
				(1 + final_class_weight[j]) * (1 + final_class_weight[j]);
		}

		CMath::NormalizeVector(final_class_weight.Buffer(), CCommunication::WavePassClasses());
		for(int j=0; j<CCommunication::WavePassClasses(); j++) {
			final_class_weight[j] = 1 - final_class_weight[j];
		}

		CMath::NormalizeVector(final_class_weight.Buffer(), CCommunication::WavePassClasses());
		final_class_weight.WriteMemoryChunkToFile(CUtility::ExtendString
			("LocalData/fin_class_weight", CNodeStat::GetInstID()));
	}

public:

	CWavePass() {
	}

	// This is the entry function
	void WavePass(int wave_pass_cycles) {

		CreateClusteredLinkSet();

		int class_bit_width = 0;
		for(int j=0; j<CCommunication::WavePassInstNum(); j++) {
			FindNetClassDist();
			cout<<"Pass "<<j<<endl;
			for(int i=0; i<wave_pass_cycles; i++) {
				cout<<"Distributing"<<endl;
				DistributeWavePass();
				cout<<"Accumulating"<<endl;
				UpdateBackBuffer(i == (wave_pass_cycles - 1));
				FindNetClassDist();
			}

			cout<<"Update Wave Pass History"<<endl;
			UpdateWavePassHistory(j != 0, class_bit_width);
			class_bit_width += CMath::LogBase2(CCommunication::WavePassClasses());
		}
	}
};

// This tests that the reverse w_links are merged correctly so a clustered
// link set can be created.
class CTestMergeBinaryLinks : public CNodeStat {

	// This stores the set of keys
	CTrie m_key_map;
	// This stores the set of links
	CTrie m_link_map;

	// This loads the key set 
	void LoadKeySet() {

		SWLink w_link;
		m_link_map.Initialize(4);
		m_key_map.Initialize(4);
		CHDFSFile bin_link_file;
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			bin_link_file.OpenReadFile(CUtility::ExtendString
				("LocalData/reverse_link_set", CNodeStat::GetInstID(), ".set", i));

			while(w_link.ReadWLink(bin_link_file)) {
				m_key_map.AddWord((char *)&w_link.src, sizeof(S5Byte));
				m_link_map.AddWord((char *)&w_link, sizeof(S5Byte) << 1);
			}
		}
	}

public:

	CTestMergeBinaryLinks() {
	}

	// This is the entry function
	void TestMergeBinaryLinks() {

		int id;
		int bytes = 12;
		SWLink w_link;
		CHDFSFile map_file;
		int num = 0;
		LoadKeySet();
		CMemoryChunk<bool> key_used(m_key_map.Size(), false);
		CMemoryChunk<bool> set_used(m_link_map.Size(), false);
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			map_file.OpenReadFile(CUtility::ExtendString
				("LocalData/reverse_link_set", CNodeStat::GetInstID(), ".set", i));

			_int64 prev_id = -1;
			while(w_link.ReadWLink(map_file)) {

				int id = m_key_map.FindWord((char *)&w_link.src, sizeof(S5Byte));

				if(id < 0) {
					cout<<"Could Not Find ";
					getchar();
				}

				if(w_link.src != prev_id) {
					prev_id = w_link.src.Value();
					if(key_used[id] == true) {
						cout<<"node already used";getchar();
					}
					key_used[id] = true;
				}

				id = m_link_map.FindWord((char *)&w_link, sizeof(S5Byte) << 1);
				
				if(set_used[id] == false) {
					set_used[id] = true;
					num++;
				}
			}
		}

		if(num != m_link_map.Size()) {
			cout<<"Link Num Mismatch";getchar();
		}
	}
};

// This class is used to test the correctness of wave pass, implemented
// as a multiple client process. Each just passes the current wave
// pass distribution to it's neighbours multiple times.
class CTestWavePass : public CNodeStat {

	// This stores a wave pass distribution
	struct SWaveScore {
		float class_percent[3];
		int majority_class;
	};

	// used to store the class weights done 
	// to stop one species from taking over
	CMemoryChunk<float> m_class_weight;
	// This stores the current back buffer
	CMemoryChunk<SWaveScore> m_back_buffer;
	// This stores the number of classes
	int m_class_num;

	// This stores the final waved pass distribution
	CMemoryChunk<SWaveDist> m_wave_dist;
	// This stores the local link set
	CArrayList<SClusterLink> m_local_link_set;
	// This stores a predicate specifying whether a node is a base node
	CMemoryChunk<bool> m_base_node_set;
	// This stores all the nodes in the link set
	CTrie m_node_set;

	// This stores the test distribution
	CArrayList<float> m_test_dist;
	// This stores the distribution offset
	int m_test_dist_offset;
	// This stores the class number bit width
	int m_class_bit_width;


	// this normalizes class percentages accross a given node
	// so the different nodes are competing for ownership of a node
	void NormalizeNodeClassPercentage() {

		for(int i=0; i<m_back_buffer.OverflowSize(); i++) {
			CMath::NormalizeVector(m_back_buffer[i].class_percent, m_class_num);
		}
	}

	// this normalizes class perctages for a given node over each
	// respective class to find weight in class membership
	void NormalizeClassPercentage() {

		for(int j=0; j<m_class_num; j++) {
			float sum = 0;
			for(int i=0; i<m_back_buffer.OverflowSize(); i++) {
				if(!m_base_node_set[i]) {
					continue;
				}

				sum += m_back_buffer[i].class_percent[j];
			}

			for(int i=0; i<m_back_buffer.OverflowSize(); i++) {
				if(!m_base_node_set[i]) {
					continue;
				}
				m_back_buffer[i].class_percent[j] /= sum;
			}
		}
	}

	// this is used to update the species weight
	void UpdateClassWeight() {

		for(int j=0; j<m_class_num; j++) {
			float sum = 0;
			for(int i=0; i<m_back_buffer.OverflowSize(); i++) {
				if(!m_base_node_set[i]) {
					continue;
				}
				sum += m_back_buffer[i].class_percent[j];
			}

			m_class_weight[j] = sum;
		}

		CMath::NormalizeVector(m_class_weight.Buffer(), m_class_num);

		for(int j=0; j<m_class_num; j++) {
			m_class_weight[j] = (1 + m_class_weight[j]) * 
				(1 + m_class_weight[j]) * (1 + m_class_weight[j]);
		}

		CMath::NormalizeVector(m_class_weight.Buffer(), m_class_num);
		for(int j=0; j<m_class_num; j++) {
			m_class_weight[j] = 1 - m_class_weight[j];
		}

		CMath::NormalizeVector(m_class_weight.Buffer(), m_class_num);
	}

	// This assigns the majority class to each node
	void AssignMajorityClass() {

		for(int i=0; i<m_back_buffer.OverflowSize(); i++) {
			float max = 0;

			u_short majority_class = 0;
			for(int j=0; j<m_class_num; j++) {
				if(m_back_buffer[i].class_percent[j] > max) {
					max = m_back_buffer[i].class_percent[j];
					m_back_buffer[i].majority_class = j;
					m_wave_dist[i].node = 0;
					majority_class = (u_short)j;
					memcpy((char *)&m_wave_dist.LastElement().node, m_node_set.GetWord(i), 4);

				}
			}
			m_wave_dist[i].cluster |= majority_class << m_class_bit_width;
			m_wave_dist[i].majority_weight += max;
		}
	}

	// This creates a local link set that mapes all nodes locally
	void CreateLocalLinkSet(CArrayList<SClusterLink> &local_link_set,
		CHDFSFile &clus_link_file) {

		uLong cluster_size;
		SWaveDist src_wave_dist;
		SWaveDist dst_wave_dist;
		SWLink w_link;

		while(clus_link_file.GetEscapedItem(cluster_size) >= 0) {
			clus_link_file.ReadCompObject(w_link.src);
			m_node_set.AddWord((char *)&w_link.src, 4);

			for(uLong i=0; i<cluster_size; i++) {
				clus_link_file.ReadCompObject(w_link.dst);
				clus_link_file.ReadCompObject(w_link.link_weight);

				local_link_set.ExtendBuffer(1);
				local_link_set.LastElement().base_link = w_link;

				m_node_set.AddWord((char *)&w_link.dst, 4);

				local_link_set.LastElement().cluster_link.src = 
					m_node_set.FindWord((char *)&w_link.src, 4);
				local_link_set.LastElement().cluster_link.dst = 
					m_node_set.FindWord((char *)&w_link.dst, 4);
				local_link_set.LastElement().link_weight = w_link.link_weight;
			}
		}

		m_local_link_set.WriteArrayListToFile("local_wave_link_set");
	}

	// This creates the initial distribution by loading in the test file
	// that is used to copy the distribution created for each client
	void LoadDistribution() {

		S5Byte node;
		while(true) {
			float node1 = m_test_dist[m_test_dist_offset++];
			if(node1 < 0) {
				// this indicates the initial distribution for the next pass
				break;
			}
			int node = node1;
			int id = m_node_set.FindWord((char *)&node, 4);
			m_base_node_set[id] = true;
			for(int i=0; i<m_class_num; i++) {
				m_back_buffer[id].class_percent[i] = m_test_dist[m_test_dist_offset++];
			}
		}
	}

	// This is the entry function that performs wave pass
	void PerformWavePass(int cycle_num) {

		for(int c=0; c<m_class_num; c++) {
			for(int j=0; j<m_back_buffer.OverflowSize(); j++) {
				m_back_buffer[j].class_percent[c] = 0;
			}
		}

		m_base_node_set.AllocateMemory(CNodeStat::GetGlobalNodeNum(), false);
		LoadDistribution();
		UpdateClassWeight();

		for(int k=0; k<cycle_num; k++) {
			CMemoryChunk<SWaveScore> forward_buffer(m_back_buffer);

			for(int c=0; c<m_class_num; c++) {
				for(int j=0; j<forward_buffer.OverflowSize(); j++) {
					forward_buffer[j].class_percent[c] = 0;
				}
			}
cout<<k<<endl;
			
			for(int j=0; j<m_local_link_set.Size(); j++) {
				SBLink &c_link = m_local_link_set[j].cluster_link;

				int src = c_link.src.Value();
				int dst = c_link.dst.Value();
				int id = *(int *)m_node_set.GetWord(dst);

				m_base_node_set[src] = true;

				for(int c=0; c<m_class_num; c++) {
					forward_buffer[dst].class_percent[c] += 
						m_back_buffer[src].class_percent[c] * m_local_link_set[j].link_weight;
				}
			}

			for(int i=0; i<forward_buffer.OverflowSize(); i++) {	

				float sum = CMath::NormalizeVector(forward_buffer[i].class_percent, m_class_num);
				if(!m_base_node_set[i]) {
					for(int c=0; c<m_class_num; c++) {
						m_back_buffer[i].class_percent[c] = forward_buffer[i].class_percent[c];
					}
					continue;
				}

				int id = *(int *)m_node_set.GetWord(i);

				for(int c=0; c<m_class_num; c++) {

					forward_buffer[i].class_percent[c] *= m_class_weight[c] *
						m_back_buffer[i].class_percent[c];
				}

				sum = CMath::NormalizeVector(forward_buffer[i].class_percent, m_class_num);

				for(int c=0; c<m_class_num; c++) {
					if(sum != 0) {
						m_back_buffer[i].class_percent[c] += forward_buffer[i].class_percent[c];
					}
				}
			}

			NormalizeNodeClassPercentage();
			UpdateClassWeight();
		}

		AssignMajorityClass();
	}

public:

	CTestWavePass() {
	}

	// This controls multiple passes of wave pass to create the final cluster assignment
	void PrduceClusterAssignment(int cycle_num, int wave_pass_num, int hash_div_num) {

		m_test_dist_offset = 0;
		m_test_dist.ReadArrayListFromFile("LocalData/test_dist");

		m_back_buffer.AllocateMemory(GetCurrNodeNum());
		m_class_num = 3;
		m_class_weight.AllocateMemory(m_class_num);

		m_wave_dist.AllocateMemory(m_back_buffer.OverflowSize());

		for(int i=0; i<m_wave_dist.OverflowSize(); i++) {
			m_wave_dist[i].cluster = 0;
			m_wave_dist[i].majority_weight = 0;
		}

		m_node_set.Initialize(4);
		m_local_link_set.Initialize(4);

		CHDFSFile clus_link_set;
		for(int j=0; j<hash_div_num; j++) {
			for(int i=0; i<CNodeStat::GetClientNum(); i++) {
				clus_link_set.OpenReadFile(CUtility::ExtendString
					(SUBM_SET_DIR, GetInstID(), ".client", i, ".set", j));
				CreateLocalLinkSet(m_local_link_set, clus_link_set);
			}
		}

		m_class_bit_width = 0;

		float assign_thresh = 1.0f;
		while(assign_thresh > 0.0001) {

			if(cycle_num < 1) {
				break;
			}

			PerformWavePass(wave_pass_num);
			m_class_bit_width += CMath::LogBase2(m_class_num);
			assign_thresh /= m_class_num;
			cycle_num--;
		}
	}

	// This checks the cluster label assignment
	void CheckClusterLabelAssign(int hash_div_num) {

		int count = 0;
		SWaveDist wave_dist;
		CHDFSFile wave_assn_file;
		for(int i=0; i<hash_div_num; i++) {
			wave_assn_file.OpenReadFile(CUtility::ExtendString
				("LocalData/label_file", GetInstID(), ".set", i));

			while(wave_dist.ReadWaveDist(wave_assn_file)) {
				count++;

				int id = m_node_set.FindWord((char *)&wave_dist.node, 4);
				m_wave_dist[id].node = wave_dist.node;
				if(m_wave_dist[id].cluster != wave_dist.cluster) {
					cout<<"Cluster Assign Mismatch "<<
						m_wave_dist[id].cluster<<" "<<wave_dist.cluster
						<<" "<<m_wave_dist[id].majority_weight<<" "
						<<wave_dist.majority_weight<<" "
						<<wave_dist.node.Value();getchar();
				}

				if(abs(m_wave_dist[id].majority_weight - wave_dist.majority_weight) > 0.01) {
					cout<<"majority weight mismatch";getchar();
				}
			}
		}

		if(count != m_node_set.Size()) {
			cout<<"Wave Pass Node Num Mismatch11 "<<count<<" "<<m_node_set.Size();getchar();
		}

		m_wave_dist.WriteMemoryChunkToFile("wave_dist");
	}

	// This tests the output of wave pass
	void TestWavePass(int hash_div_num) {

		CHDFSFile test_file;

		CMemoryChunk<bool> set(m_node_set.Size(), false);

		float dist;
		S5Byte node;
		int count = 0;
		for(int i=0; i<hash_div_num; i++) {
			test_file.OpenReadFile(CUtility::ExtendString("LocalData/wave_pass_test", i));

			while(test_file.ReadCompObject(node)) {
				count++;
				int match = m_node_set.FindWord((char *)&node, 4);
				if(match < 0) {
					cout<<"Node Not Found";getchar();
				}

				if(set[match]) {
					cout<<"Node	Already Stored "<<node.Value();getchar();
				}

				set[match] = true;
				int id = m_node_set.FindWord((char *)&node, 4);
				for(int j=0; j<m_class_num; j++) {
					test_file.ReadCompObject(dist);
					if(abs(dist - m_back_buffer[id].class_percent[j]) > 0.01) {
						cout<<"error   "<<endl;
						cout<<dist<<" "<<m_back_buffer[id].class_percent[j]<<"  "<<node.Value()<<" "<<id;getchar();
					}
				}
			}
		}


		if(count != m_node_set.Size()) {
			cout<<"Wave Pass Node Num Mismatch "<<count<<" "<<m_node_set.Size();getchar();
		}
	}
};
