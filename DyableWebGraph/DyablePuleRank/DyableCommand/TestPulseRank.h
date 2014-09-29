#include "../../../ProcessSet.h"

// This stores the directory where the pulse scores are stored
const char *FIN_PULSE_SCORE_DIR = "GlobalData/PulseRank/fin_pulse_score";

// This defines the directory where the final hashed cluster
// link is stored after processing
const char *FIN_LINK_SET_DIR = "GlobalData/LinkSet/fin_link_set";


// This is used for testing the pulse rank implemenation
class CTestPulseRank : public CNodeStat {

	// This stores the back buffer
	CMemoryChunk<float> m_back_buffer;
	// This stores a predicate indicating whether a node is used
	CMemoryChunk<bool> m_node_used;

	// This loads in an existing link set generated from the indexed documents.
	void LoadLinkSet(CArrayList<SWLink> &w_link_set,
		CArrayList<int> &cluster_size, bool is_keyword) {

		SWLink w_link;
		uLong clus_size;
		CHDFSFile link_set_file;
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			for(int j=0; j<CNodeStat::GetHashDivNum(); j++) {

				if(is_keyword == true) {
					link_set_file.OpenReadFile(CUtility::ExtendString
						("GlobalData/LinkSet/keyword_hash_link_set", i, ".set", j));
				} else {
					link_set_file.OpenReadFile(CUtility::ExtendString
						("GlobalData/LinkSet/webgraph_hash_link_set", i, ".set", j));
				}

				while(link_set_file.GetEscapedItem(clus_size) >= 0) {
					link_set_file.ReadCompObject(w_link.src);

					cluster_size.PushBack(clus_size);
					for(uLong k=0; k<clus_size; k++) {
						link_set_file.ReadCompObject(w_link.dst);
						link_set_file.ReadCompObject(w_link.link_weight);

						w_link_set.PushBack(w_link);
					}
				}
			}
		}
	}

public:

	// This loads in statistics relating to the 
	// total number of nodes and links
	CTestPulseRank() {
	}

	// This loads the back buffer pulse score
	void LoadBackBuffer() {

		m_node_used.AllocateMemory(CNodeStat::GetCurrNodeNum(), false);
		m_back_buffer.AllocateMemory(CNodeStat::GetCurrNodeNum(), 0);

		SPulseMap pulse_map;
		CHDFSFile back_pulse_file;
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			for(int j=0; j<CNodeStat::GetHashDivNum(); j++) {

				back_pulse_file.OpenReadFile(CUtility::ExtendString
					("LocalData/back_wave_pass", i, ".set", j));

				while(pulse_map.ReadPulseMap(back_pulse_file)) {
					m_back_buffer[pulse_map.node.Value()] = pulse_map.pulse_score;
					m_node_used[pulse_map.node.Value()] = true;
				}
			}
		}
	}

	// This is just a test framework
	void TestPulseRank(bool is_keyword, int cycle_num) {

		int offset = 0;
		CArrayList<SWLink> w_link_set(4);
		CArrayList<int> cluster_size(100);

		LoadLinkSet(w_link_set, cluster_size, is_keyword);

		CMemoryChunk<float> forward_buffer(CNodeStat::GetCurrNodeNum());
		_int64 cutoff = CNodeStat::GetBaseNodeNum();

		cout<<cycle_num<<" Cycle"<<endl;
		for(int k=0; k<cycle_num+1; k++) {
			cout<<"Pulse Cycle: "<<k<<endl;
			forward_buffer.InitializeMemoryChunk(0);

			float sum = 0;
			int offset = 0;
			for(int i=0; i<cluster_size.Size(); i++) {
				sum += m_back_buffer[w_link_set[offset].src.Value()];

				for(int j=offset; j<offset+cluster_size[i]; j++) {
					SWLink &w_link = w_link_set[j];

					if(w_link.link_weight < 0) {
						// this is a dummy link
						continue;
					}

					if(w_link.dst >= cutoff) {
						continue;
					}

					// remember the breadth is the cluster size minus dummy links
					forward_buffer[w_link.dst.Value()] += m_back_buffer[w_link.src.Value()] * w_link.link_weight;
					
					sum += forward_buffer[w_link.dst.Value()];
				}
				offset += cluster_size[i];
			}

			if(k == cycle_num - 1) {
				cutoff = CNodeStat::GetGlobalNodeNum();
			}

			if(k < cycle_num) {
				for(int i=0; i<CNodeStat::GetBaseNodeNum(); i++) {
					forward_buffer[i] += m_back_buffer[i];
				}
				m_back_buffer.MakeMemoryChunkEqualTo(forward_buffer);
				for(int i=0; i<CNodeStat::GetBaseNodeNum(); i++) {
					m_back_buffer[i] /= sum;
				}
			} else {

				for(int i=0; i<CNodeStat::GetBaseNodeNum(); i++) {
					forward_buffer[i] += m_back_buffer[i];
				}

				CMath::NormalizeVector(forward_buffer.Buffer(), CNodeStat::GetGlobalNodeNum());
			}
		}


		CMemoryChunk<bool> set(GetGlobalNodeNum(), false);
		if(is_keyword == true) {
			set.AllocateMemory(GetBaseNodeNum(), false);
		}

		SPulseMap pulse_map;
		CHDFSFile pulse_score_file;
		// compare the output of the internal and external approach
		for(int j=0; j<CNodeStat::GetHashDivNum(); j++) {

			if(is_keyword == true) {
				pulse_score_file.OpenReadFile(CUtility::
					ExtendString(KEYWORD_PULSE_SCORE_DIR, j));
			} else {
				pulse_score_file.OpenReadFile(CUtility::
					ExtendString(WEBGRAPH_PULSE_SCORE_DIR, j));
			}

			while(pulse_map.ReadPulseMap(pulse_score_file)) {

				if(set[pulse_map.node.Value()] == true) {
					cout<<"map already used";getchar();
				}

				set[pulse_map.node.Value()] = true;
				if(pulse_map.node.Value() < GetBaseNodeNum()) {
					//cout<<pulse_map.node.Value()<<" "<<pulse_map.pulse_score<<" **"<<endl;

					if(abs(pulse_map.pulse_score - forward_buffer[pulse_map.node.Value()]) > 0.00001f) {
						cout<<"error1 "<<pulse_map.node.Value()<<" "<<pulse_map.pulse_score<<" "<<forward_buffer[pulse_map.node.Value()];getchar();
					}
				} else {
					//cout<<pulse_map.node.Value()<<" "<<pulse_map.pulse_score<<" ##"<<endl;

					if(abs(pulse_map.pulse_score - forward_buffer[pulse_map.node.Value()]) > 0.00001f) {
						cout<<"error2 "<<pulse_map.node.Value()<<" "<<pulse_map.pulse_score<<" "<<forward_buffer[pulse_map.node.Value()];getchar();
					}
				}
			}
		}

		for(int i=0; i<set.OverflowSize(); i++) {
			if(!set[i] && m_node_used[i] == true) {
				cout<<"Node Not Found "<<i;getchar();
			}
		}
	}
};