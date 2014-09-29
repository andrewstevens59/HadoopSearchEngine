#include "../../../../MapReduce.h"

// This class is responsible for distributing the class label for
// each src node to neighbouring nodes. The cluster label for src
// nodes will only be distributed if it has been assigned. Another
// component of DistributeClusterNodes is to remap cluster labels
// from nodes that have been recently joined.
class CDistributeClusterNodes {

	// This stores all the forward class labels
	CFileSet<CHDFSFile> m_forward_set;
	// This stores the cluster map relabelling
	CFileSet<CHDFSFile> m_label_set;
	// This maps global dst indexes to local indexes 
	CObjectHashMap<S5Byte> m_node_map;
	// This stores the cluster label file
	CArrayList<SWClusterMap> m_clus_label_buff;
	// This stores the hashed link set once all link clusters
	// have been hashed to seperate bins
	CHDFSFile m_clus_link_file;
	// This stores the cluster label for each src node
	CFileSet<CHDFSFile> m_clus_label_set;

	// This is an equality handle used to compare two 5-byte numbers
	static bool Equal(const S5Byte &arg1, const S5Byte &arg2) {
		return S5Byte::Value(arg1) == S5Byte::Value(arg2);
	}

	// This returns a hash code for a 5-byte number 
	static int HashCode(const S5Byte &node) {
		return (int)S5Byte::Value(node);
	}

	// This loads the current cluster label file 
	void LoadClusterLabelFile() {

		SWClusterMap clus_map;
		for(int i=0; i<CNodeStat::GetClientNum(); i++) {
			m_clus_label_set.OpenReadFile(CNodeStat::GetClientID(), i);
			CHDFSFile &clus_file = m_clus_label_set.File(CNodeStat::GetClientID());

			while(clus_map.ReadClusterMap(clus_file)) {

				if((clus_map.base_node.Value() % CNodeStat::GetClientNum()) != CNodeStat::GetClientID()) {
					cout<<"div mis";getchar();
				}

				m_node_map.Put(clus_map.base_node);
				if(m_node_map.AskFoundWord() == false) {
					m_clus_label_buff.PushBack(clus_map);
				}
			}

			clus_file.CloseFile();
		}
	}

	// This runs through all the link sets in a given hash division. 
	// It passes cluster label of the src to all the neighbours.
	void ProcessLinkSet() {

		SWLink w_link;
		uLong cluster_size;

		while(m_clus_link_file.GetEscapedItem(cluster_size) >= 0) {
			m_clus_link_file.ReadCompObject(w_link.src);

			int id = m_node_map.Get(w_link.src);
			SWClusterMap &clus_map = m_clus_label_buff[id];
			int hash = clus_map.base_node.Value() % m_forward_set.SetNum(); 
			clus_map.WriteClusterMap(m_forward_set.File(hash));

			if(clus_map.base_node != w_link.src) {
				cout<<"src mismatch "<<clus_map.base_node.Value()<<" "<<w_link.src.Value();getchar();
			}

			for(uLong i=0; i<cluster_size; i++) {
				m_clus_link_file.ReadCompObject(w_link.dst);

				clus_map.base_node = w_link.dst;
				int hash = w_link.dst.Value() % m_forward_set.SetNum(); 
				clus_map.WriteClusterMap(m_forward_set.File(hash));
			}
		}
	}

public:

	CDistributeClusterNodes() {
	}

	// This is the entry function
	void DistributeClusterNodes(int client_id, int client_num) {

		CNodeStat::SetClientID(client_id);
		CNodeStat::SetClientNum(client_num);

		m_clus_link_file.OpenReadFile(CUtility::ExtendString
			(SUBM_SET_DIR, CNodeStat::GetClientID()));

		m_forward_set.OpenWriteFileSet("LocalData/forward_wave_pass",
			CNodeStat::GetClientNum(), CNodeStat::GetClientID());

		m_clus_label_set.AllocateFileSet(CNodeStat::GetClientNum());
		m_clus_label_set.SetDirectory("LocalData/clus_label");
		m_node_map.Initialize(HashCode, Equal);
		m_clus_label_buff.Initialize(1024);

		LoadClusterLabelFile();
		ProcessLinkSet();
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);
	int client_num = atoi(argv[2]);

	CBeacon::InitializeBeacon(client_id, 5555);
	CMemoryElement<CDistributeClusterNodes> dist;
	dist->DistributeClusterNodes(client_id, client_num);
	dist.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();

	return 0;
}