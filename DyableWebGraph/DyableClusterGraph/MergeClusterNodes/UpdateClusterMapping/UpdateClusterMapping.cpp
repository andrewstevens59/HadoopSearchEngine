#include "../../../../MapReduce.h"

// This updates the cluster mapping to handle cluster 
// groups that have been merged.
class CUpdateClusterMapping {

	// This stores the maximum cluster label for a given node
	struct SMaxClusterLabel {
		// This stores the max label weight
		float weight;
		// This stores the max label
		S5Byte clus_id;
	};

	// This maps global dst indexes to local indexes 
	CObjectHashMap<S5Byte> m_node_map;
	// This stores the cluster label remapping
	CArrayList<SMaxClusterLabel> m_clus_remap_buff;

	// This stores the cluster map relabelling
	CFileSet<CHDFSFile> m_label_set;
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

	// This loads the cluster node remapping
	void LoadClusterNodeRemapping() {

		SRemapClusterMap clus_map;
		for(int i=0; i<CNodeStat::GetClientNum(); i++) {
			m_label_set.OpenReadFile(CNodeStat::GetClientID(), i);

			CHDFSFile &file = m_label_set.File(CNodeStat::GetClientID());
			while(clus_map.ReadClusterMap(file)) {
				int id = m_node_map.Put(clus_map.cluster);

				if((clus_map.cluster.Value() % CNodeStat::GetClientNum()) != CNodeStat::GetClientID()) {
					cout<<"div mis";getchar();
				}

				if(m_node_map.AskFoundWord() == false) {
					m_clus_remap_buff.ExtendSize(1);
					m_clus_remap_buff.LastElement().weight =  0;
				}

				SMaxClusterLabel &label = m_clus_remap_buff[id];
				if(clus_map.clus_weight >= label.weight) {
					if(clus_map.weight != label.weight || clus_map.cluster_remap != label.clus_id) {
						label.weight = clus_map.weight;
						label.clus_id = clus_map.cluster_remap;
					}
				}
			}
		}
	}

	// This updates the cluster mapping
	void UpdateClusterMapping() {

		SWClusterMap clus_map;
		SRemapClusterMap remap;
		for(int i=0; i<CNodeStat::GetClientNum(); i++) {
			m_label_set.OpenReadFile(CNodeStat::GetClientID(), i);

			CHDFSFile &file = m_label_set.File(CNodeStat::GetClientID());
			while(remap.ReadClusterMap(file)) {

				clus_map.weight = remap.weight;
				clus_map.base_node = remap.base_node;
				int id = m_node_map.Get(remap.cluster);
				clus_map.cluster = m_clus_remap_buff[id].clus_id;

				int hash = remap.base_node.Value() % CNodeStat::GetClientNum();
				clus_map.WriteClusterMap(m_clus_label_set.File(hash));
			}
		}
	}

	// This creates the final cluster mapping
	void AssignClusterMapping() {

		SWClusterMap clus_map;
		SRemapClusterMap remap;
		for(int i=0; i<CNodeStat::GetClientNum(); i++) {
			m_label_set.OpenReadFile(CNodeStat::GetClientID(), i);

			CHDFSFile &file = m_label_set.File(CNodeStat::GetClientID());
			while(remap.ReadClusterMap(file)) {

				clus_map.weight = remap.clus_weight;
				clus_map.base_node = remap.base_node;
				int id = m_node_map.Get(remap.cluster);
				clus_map.cluster = m_clus_remap_buff[id].clus_id;

				int hash = clus_map.cluster.Value() % CNodeStat::GetClientNum();
				clus_map.WriteClusterMap(m_clus_label_set.File(hash));
			}
		}
	}

public:

	CUpdateClusterMapping() {
	}

	// This is the entry function
	void UpdateClusterMapping(int client_id, int client_num, bool is_last_cycle) {
		CNodeStat::SetClientID(client_id);
		CNodeStat::SetClientNum(client_num);

		m_label_set.AllocateFileSet(CNodeStat::GetClientNum());
		m_label_set.SetDirectory("LocalData/label_set");

		m_node_map.Initialize(HashCode, Equal);
		m_clus_remap_buff.Initialize(1024);

		LoadClusterNodeRemapping();

		m_clus_label_set.OpenWriteFileSet("LocalData/clus_label", 
			CNodeStat::GetClientNum(), CNodeStat::GetClientID());

		if(is_last_cycle == false) {
			UpdateClusterMapping(); 
		} else {
			AssignClusterMapping();
		}
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);
	int client_num = atoi(argv[2]);
	bool is_last_cycle = atoi(argv[3]);

	CBeacon::InitializeBeacon(client_id, 5555);
	CMemoryElement<CUpdateClusterMapping> dist;
	dist->UpdateClusterMapping(client_id, client_num, is_last_cycle);
	dist.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();

	return 0;
}