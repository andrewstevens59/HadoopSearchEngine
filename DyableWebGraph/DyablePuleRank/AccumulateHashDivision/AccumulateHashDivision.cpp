#include "../../../MapReduce.h"

// This class is responsible for accumulating the pulse scores
// in a given hash divison. That is pulse scores are grouped
// by their node id and added together. Once summed all the pulse
// scores are normalized to find the pulse score for the next pass.
class CAccumulateHashDivision : public CNodeStat {

	// This maps global dst indexes to local indexes 
	CObjectHashMap<S5Byte> m_node_map;
	// This stores the stationary distribution for every 
	// node in the local hash division
	CArrayList<float> m_pulse_dist;
	// This stoers the new back buffer distribution
	CArrayList<SPulseMap> m_back_buff_dist;
	// This stores all the forward pulse rank scores
	CFileSet<CHDFSFile> m_forward_set;
	// This stores all the back pulse rank scores
	CFileSet<CHDFSFile> m_back_set;
	// This stores the net updated pulse score across all nodes
	float m_net_pulse_score;

	// This is an equality handle used to compare two 5-byte numbers
	static bool Equal(const S5Byte &arg1, const S5Byte &arg2) {
		return S5Byte::Value(arg1) == S5Byte::Value(arg2);
	}

	// This returns a hash code for a 5-byte number 
	static int HashCode(const S5Byte &node) {
		return (int)S5Byte::Value(node);
	}

	// This returns the back wave pass file
	inline const char *WavePassFile(int hash_div) {
		strcpy(CUtility::SecondTempBuffer(), CUtility::ExtendString
			("LocalData/forward_wave_pass", hash_div, ".set"));
		return CUtility::SecondTempBuffer();
	}

	// This updates the wave pass distribution for all nodes in a given hash 
	// division. This simply means accumulating the wave pass distribution
	// score for every dst node in a division. This is normalized before being
	// added to the existing back buffer.
	// @param hash_div - this is the current hash division being processed
	// @param forward_set_num - this is the number of forward sets created
	void AccumulateDistribution(int hash_div, int forward_set_num) {

		SPulseMap pulse_map;
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			m_forward_set.SetDirectory(WavePassFile(i));
			for(int j=0; j<forward_set_num; j++) {
				m_forward_set.OpenReadFile(hash_div, j);

				CHDFSFile &node_file = m_forward_set.File(hash_div);
				while(pulse_map.ReadPulseMap(node_file)) {
					int id = m_node_map.Put(pulse_map.node);

					if(!m_node_map.AskFoundWord()) {
						m_pulse_dist.PushBack(pulse_map.pulse_score);
					} else {
						m_pulse_dist[id] += pulse_map.pulse_score;
					}
				}

				node_file.CloseFile();
			}
		}
	}

	// This writes the new back buffer by adding the accumulated wave pass 
	// distribution to the existing back buffer distribution.
	// @param hash_div - this stores the current bucket being processed
	void WriteBackBuffer(int hash_div) {

		float new_pulse_dist;
		SPulseMap pulse_map;

		for(int j=0; j<GetClientNum(); j++) {
			m_back_set.OpenReadFile(hash_div, j);
			CHDFSFile &back_file = m_back_set.File(hash_div);

			m_back_buff_dist.Resize(0);
			while(pulse_map.ReadPulseMap(back_file)) {
				int id = m_node_map.Get(pulse_map.node);

				if(id >= 0) {
					new_pulse_dist = m_pulse_dist[id] + pulse_map.pulse_score;
				} else {
					// nothing was passed to this node 
					// (i.e. zero forward buffer)
					new_pulse_dist = pulse_map.pulse_score;
				}

				pulse_map.pulse_score = new_pulse_dist / m_net_pulse_score;
				m_back_buff_dist.PushBack(pulse_map);
			}

			m_back_set.OpenWriteFile(hash_div, j);
			for(int i=0; i<m_back_buff_dist.Size(); i++) {
				m_back_buff_dist[i].WritePulseMap(back_file);
			}

			back_file.CloseFile();
		}
	}

	// This preloads the set of base nodes into memory so that
	// they can be processed in the correct order. This allows
	// pulse scores to be matched up the src node in the link set.
	void PreLoadBaseNodes(int hash_div) {

		SPulseMap pulse_map;
		for(int j=0; j<GetClientNum(); j++) {
			m_back_set.OpenReadFile(hash_div, j);
			CHDFSFile &back_file = m_back_set.File(hash_div);

			while(pulse_map.ReadPulseMap(back_file)) {
				m_node_map.Put(pulse_map.node);
				if(!m_node_map.AskFoundWord()) {
					m_pulse_dist.PushBack(pulse_map.pulse_score);
				}
			}
		}
	}

	// This takes the existing pulse scores and writes them out externally.
	// In order for the pulse scores to be matched up with their corresponding
	// src node in the link set they are written out in a specified order. This
	// specified order matches up with each src node in this client's link set.
	// @param hash_div - this is the current hash division being processed 
	// @param is_keyword_set - true if the keyword link set is being used
	void WriteFinalPulseScores(int hash_div, bool is_keyword_set) {

		CSegFile fin_pulse_file;
		if(is_keyword_set == true) {
			fin_pulse_file.OpenWriteFile(CUtility::ExtendString
				(KEYWORD_PULSE_SCORE_DIR, hash_div));
		} else {
			fin_pulse_file.OpenWriteFile(CUtility::ExtendString
				(WEBGRAPH_PULSE_SCORE_DIR, hash_div));
		}

		SPulseMap pulse_map;
		m_node_map.ResetNextObject();
		for(int i=0; i<m_node_map.Size(); i++) {
			pulse_map.node = m_node_map.NextSeqObject();
			int id = m_node_map.Get(pulse_map.node);

			pulse_map.pulse_score = m_pulse_dist[id] / m_net_pulse_score;
			pulse_map.WritePulseMap(fin_pulse_file);
		}
	}

public:

	CAccumulateHashDivision() {
	}

	// This is called after all link sets have passed their wave pass distribution
	// to their neighbours and it's now time to update the back buffer so the next
	// pass through the link set can begin.
	// @param forward_set_num - this is the number of forward sets that have been created
	void Initialize(int client_id, int client_num, int forward_set_num) {

		CHDFSFile::Initialize();
		CNodeStat::SetClientID(client_id);
		CNodeStat::SetClientNum(client_num);
		CNodeStat::SetHashDivNum(client_num);
		CNodeStat::LoadNodeLinkStat();

		m_net_pulse_score = 0;
		double temp_pulse_score;
		CHDFSFile net_pulse_file;
		for(int j=0; j<CNodeStat::GetHashDivNum(); j++) {
			for(int i=0; i<forward_set_num; i++) {
				net_pulse_file.OpenReadFile(CUtility::ExtendString
					("LocalData/net_pulse", j, ".client", i));
				net_pulse_file.ReadObject(temp_pulse_score);

				m_net_pulse_score += temp_pulse_score;
			}
		}
	}

	// This is called after all link sets have passed their wave pass distribution
	// to their neighbours and it's now time to update the back buffer so the next
	// pass through the link set can begin.
	// @param hash_div - this is the hash division being processed
	// @param hash_div_num - this is the number of hash divisions
	// @param forward_set_num - this is the number of forward sets created
	// @param is_process_external - true if the external nodes are being added
	// @param is_keyword_set - true if the keyword link set is being used
	void AccumulateHashDivision(int hash_div, int hash_div_num, 
		int forward_set_num, bool is_process_external, bool is_keyword_set) {

		m_back_set.SetFileName("LocalData/back_wave_pass");
		m_back_set.AllocateFileSet(hash_div_num);
		m_forward_set.SetFileName("LocalData/forward_wave_pass");
		m_forward_set.AllocateFileSet(hash_div_num);
		CNodeStat::SetHashDivNum(hash_div_num);

		m_pulse_dist.Initialize((int)(GetBaseNodeNum() / 
			m_forward_set.SetNum()) + 1000);

		m_node_map.Initialize(HashCode, Equal, 3333333);

		if(is_process_external == true) {
			PreLoadBaseNodes(hash_div);
		}

		AccumulateDistribution(hash_div, forward_set_num);

		if(is_process_external == false) {
			m_back_buff_dist.Initialize((int)(GetBaseNodeNum() / 
				m_forward_set.SetNum()) + 1000);
			WriteBackBuffer(hash_div);
		} else {
			WriteFinalPulseScores(hash_div, is_keyword_set);
		}
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);
	int client_num = atoi(argv[2]);
	int forward_set_num = atoi(argv[3]);
	bool is_process_external = atoi(argv[4]);
	bool is_keyword_set = atoi(argv[5]);

	CBeacon::InitializeBeacon(client_id);
	CMemoryElement<CAccumulateHashDivision> acc;
	acc->Initialize(client_id, client_num, forward_set_num);
	acc->AccumulateHashDivision(client_id, client_num,
		forward_set_num, is_process_external, is_keyword_set);

	acc.DeleteMemoryElement();

	CBeacon::SendTerminationSignal();

	return 0;
}

