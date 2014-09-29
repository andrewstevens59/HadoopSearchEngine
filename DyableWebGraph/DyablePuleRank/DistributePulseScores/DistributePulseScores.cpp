#include "../../../MapReduce.h"

// This class is responsible for distributing pulse scores to 
// the respective hash division for futher processing. This 
// is done by parsing a given hashed cluster link set and 
// distributing the src pulse score to all of the dst nodes.
class CDistributePulseScores : public CNodeStat {

	// This stores all the forward pulse rank scores
	CFileSet<CHDFSFile> m_forward_set;
	// This stores all the back pulse rank scores
	CFileSet<CHDFSFile> m_back_set;
	// This stores the hashed link set once all link clusters
	// have been hashed to seperate bins
	CFileSet<CHDFSFile> m_hash_link_set;
	// This stores the net updated pulse score across all nodes
	double m_net_pulse_score;

	// This returns the back wave pass file
	inline const char *WavePassFile(int hash_div) {
		strcpy(CUtility::SecondTempBuffer(), CUtility::ExtendString
			("LocalData/forward_wave_pass", hash_div, ".set"));
		return CUtility::SecondTempBuffer();
	}

	// This performs a single pass of pulse rank. This means passing the
	// current back buffer to all the neighbours and storing the results
	// externally to be processed on the update stage.
	// @param clus_link_file - this stores all the clustered link sets
	// @param back_file - this stores all the current stationary distributions
	//                  - for all nodes in this link set
	// @param min_node_num - this stores the lower end of the node spectrum
	// @param max_node_num - this stores the upper end of the node spectrum
	void ProcessLinkSet(CHDFSFile &clus_link_file, CHDFSFile &back_file, 
		_int64 min_node_num, _int64 max_node_num) {

		SWLink w_link;
		uLong cluster_size;
		_int64 dst_index = 0;
		SPulseMap back_pulse_map;
		SPulseMap forward_pulse_map;

		while(clus_link_file.GetEscapedItem(cluster_size) >= 0) {
			clus_link_file.ReadCompObject(w_link.src);

			back_pulse_map.ReadPulseMap(back_file);
			m_net_pulse_score += back_pulse_map.pulse_score;

			for(uLong i=0; i<cluster_size; i++) {
				clus_link_file.ReadCompObject((char *)&dst_index, 5);
				clus_link_file.ReadCompObject(w_link.link_weight);

				if(dst_index >= max_node_num) {
					continue;
				}

				int hash = (int)(dst_index % m_forward_set.SetNum());
				forward_pulse_map.node = dst_index;
				forward_pulse_map.pulse_score = back_pulse_map.pulse_score * w_link.link_weight;

				m_net_pulse_score += forward_pulse_map.pulse_score;
				forward_pulse_map.WritePulseMap(m_forward_set.File(hash));
			}
		}
	}

public:

	CDistributePulseScores() {
		CHDFSFile::Initialize();
	}

	// This performs a single iteration of pulse rank. Here all the 
	// hash links are processed and the nodes are stored in their
	// respective hash division.
	// @param hash_div_num - this is the number of hash divisions
	// @param hash_div - this is the current has division being processed
	// @param client_start - this is the lower bound of hashed link sets for
	//                     - which this client is responsible
	// @param client_end - this is the upper bound of hashed link sets for
	//                   - which this client is responsible
	// @param min_node_num - this stores the lower end of the node spectrum
	// @param max_node_num - this stores the upper end of the node spectrum
	void PerformPulseRankPass(int hash_div_num, int hash_div, int client_id, int client_start,
		int client_end, bool is_keyword_set, _int64 min_node_num, _int64 max_node_num) {

		CNodeStat::SetClientID(client_id);
		m_back_set.SetFileName("LocalData/back_wave_pass");
		m_back_set.AllocateFileSet(hash_div_num);
		m_hash_link_set.AllocateFileSet(hash_div_num);

		if(is_keyword_set == true) {
			m_hash_link_set.SetFileName("GlobalData/LinkSet/keyword_hash_link_set");
		} else {
			m_hash_link_set.SetFileName("GlobalData/LinkSet/webgraph_hash_link_set");
		}

		m_net_pulse_score = 0;
		m_forward_set.OpenWriteFileSet(WavePassFile(hash_div),
			hash_div_num, GetClientID());

		for(int i=client_start; i<client_end; i++) {
			m_back_set.OpenReadFile(hash_div, i);
			m_hash_link_set.OpenReadFile(hash_div, i);

			ProcessLinkSet(m_hash_link_set.File(hash_div), 
				m_back_set.File(hash_div), min_node_num, max_node_num);
		}

		CHDFSFile net_pulse_file;
		net_pulse_file.OpenWriteFile(CUtility::ExtendString
			("LocalData/net_pulse", hash_div, ".client", GetClientID()));
		net_pulse_file.WriteObject(m_net_pulse_score);
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);
	int client_start = atoi(argv[2]);
	int client_end = atoi(argv[3]);
	int hash_div = atoi(argv[4]);
	int hash_div_num = atoi(argv[5]);
	int process_id = atoi(argv[6]);
	bool is_keyword_set = atoi(argv[7]);
	_int64 min_node_num = CANConvert::AlphaToNumericLong(argv[8], strlen(argv[8]));
	_int64 max_node_num = CANConvert::AlphaToNumericLong(argv[9], strlen(argv[9]));

	CBeacon::InitializeBeacon(process_id);
	CMemoryElement<CDistributePulseScores> map;
	map->PerformPulseRankPass(hash_div_num, hash_div, client_id,
		client_start, client_end, is_keyword_set, min_node_num, max_node_num);
	map.DeleteMemoryElement();

	CBeacon::SendTerminationSignal();
}
