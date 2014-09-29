#include "../../../../MapReduce.h"

// This class parses the link set in each hash division and distributes
// the src wave pass distribution to all of the neighbours. These wave
// pass distributions are later accumulated in a given hash division.
class CDistributeWavePass : public CNodeStat {

	// This defines the number of classes to use for wave pass,
	// this will most usually be set to the the number of clients
	int m_class_num;
	// This stores all the forward class distribution
	CFileSet<CHDFSFile> m_forward_set;
	// This stores the hashed link set once all link clusters
	// have been hashed to seperate bins
	CHDFSFile m_clus_link_file;
	// This stores all the back pulse rank scores
	CHDFSFile m_back_file;

	// This runs through all the link sets in a given hash division. 
	// It passes the wave pass distribution to all the neighbours.
	void ProcessLinkSet() {

		SWLink w_link;
		uLong cluster_size;
		CMemoryChunk<float> dist(m_class_num);

		while(m_clus_link_file.GetEscapedItem(cluster_size) >= 0) {
			m_clus_link_file.ReadCompObject(w_link.src);

			m_back_file.ReadCompObject(w_link.dst);
			for(int j=0; j<m_class_num; j++) {
				m_back_file.ReadCompObject(dist[j]);
			}

			if(w_link.src != w_link.dst) {
				cout<<"Node Mismatch "<<w_link.src.Value()<<" "<<w_link.dst.Value();getchar();
			}

			for(uLong i=0; i<cluster_size; i++) {
				m_clus_link_file.ReadCompObject(w_link.dst);
				m_clus_link_file.ReadCompObject(w_link.link_weight);

				int hash = w_link.dst.Value() % m_forward_set.SetNum();
				m_forward_set.File(hash).WriteCompObject(w_link.dst);

				for(int j=0; j<m_class_num; j++) {
					float weight = w_link.link_weight * dist[j];
					m_forward_set.File(hash).WriteCompObject(weight);
				}
			}
		}
	}

public:

	CDistributeWavePass() {
	}

	// This is the entry function
	void DistributeWavePass(int client_id, int client_num) {

		CNodeStat::SetInstID(0);
		CNodeStat::SetClientID(client_id);
		CNodeStat::SetClientNum(client_num);
		CHDFSFile::Initialize();
		m_class_num = 3;

		m_forward_set.OpenWriteFileSet("LocalData/forward_wave_pass",
			CNodeStat::GetClientNum(), GetClientID());

		m_clus_link_file.OpenReadFile(CUtility::ExtendString
			(SUBM_SET_DIR, CNodeStat::GetClientID()));
		
		m_back_file.OpenReadFile(CUtility::ExtendString
			("LocalData/back_wave_pass", CNodeStat::GetClientID()));
	
		ProcessLinkSet();
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);
	int client_num = atoi(argv[2]);

	CBeacon::InitializeBeacon(client_id, 5555);
	CMemoryElement<CDistributeWavePass> dist;
	dist->DistributeWavePass(client_id, client_num);
	dist.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();
	CBeacon::SendTerminationSignal();

	return 0;
}