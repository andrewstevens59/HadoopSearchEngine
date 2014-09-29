#include "../../../../MapReduce.h"

// This recreates the clustered link set with minimal links removed.
// Minimal links refers to links that are not part of the minimum
// spanning graph and hence will not be used.
class CUpdateClusteredLinkSet {

	// This stores the clustered link set 
	CHDFSFile m_clus_link_file;
	// This stores all the forward cluster labels
	CFileSet<CHDFSFile> m_forward_set;
	// This stores the distributed links
	CFileSet<CHDFSFile> m_link_set;

	// This recreates the clustered link set
	void CreateClusteredLinkSet() {

		SWLink w_link;
		SWLink max_w_link;
		uLong cluster_size;
		S5Byte src;
		int hash;

		while(m_clus_link_file.GetEscapedItem(cluster_size) >= 0) {
			m_clus_link_file.ReadCompObject(src);

			uLong neighbour_num = 0;
			float max_link_weight = 0;
			for(uLong i=0; i<cluster_size; i++) {
				m_clus_link_file.ReadCompObject(hash);
				m_forward_set.File(hash).ReadCompObject(w_link.dst);
				m_forward_set.File(hash).ReadCompObject(w_link.link_weight);
				w_link.src = src;

				if(w_link.link_weight > 0) {

					hash = w_link.src.Value() % CNodeStat::GetClientNum();
					w_link.WriteWLink(m_link_set.File(hash));

					CSort<char>::Swap(w_link.src, w_link.dst);
					hash = w_link.src.Value() % CNodeStat::GetClientNum();
					w_link.WriteWLink(m_link_set.File(hash));
					neighbour_num++;
				} 

				w_link.link_weight = abs(w_link.link_weight);
				if(w_link.link_weight > max_link_weight) {
					max_link_weight = w_link.link_weight;
					max_w_link = w_link;
				}
			}

			if(neighbour_num == 0) {
				// make sure to add in the reverse link because
				// nothing else connects to this node
				CSort<char>::Swap(max_w_link.src, max_w_link.dst);
				hash = max_w_link.src.Value() % CNodeStat::GetClientNum();
				max_w_link.WriteWLink(m_link_set.File(hash));

				max_w_link.src = src;
				// make sure to link to self in order to have a record of the src node
				hash = src.Value() % CNodeStat::GetClientNum();
				max_w_link.WriteWLink(m_link_set.File(hash));
			}
		}
	}

public:

	CUpdateClusteredLinkSet() {
	}

	// This is then entry function
	void UpdateClusteredLinkSet(int client_id, int client_num) {

		CNodeStat::SetClientID(client_id);
		CNodeStat::SetClientNum(client_num);

		m_clus_link_file.OpenReadFile(CUtility::ExtendString
			("LocalData/local_clus", CNodeStat::GetClientID()));

		m_forward_set.OpenReadFileSet("LocalData/forward_wave_pass",
			CNodeStat::GetClientNum(), CNodeStat::GetClientID());

		m_link_set.OpenWriteFileSet("LocalData/label_set", 
			CNodeStat::GetClientNum(), CNodeStat::GetClientID());

		CreateClusteredLinkSet();
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);
	int client_num = atoi(argv[2]);

	CBeacon::InitializeBeacon(client_id, 5555);
	CMemoryElement<CUpdateClusteredLinkSet> acc;
	acc->UpdateClusteredLinkSet(client_id, client_num);

	acc.DeleteMemoryElement();

	CBeacon::SendTerminationSignal();

	return 0;
}