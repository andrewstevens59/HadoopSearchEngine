#include "../../../../MapReduce.h"

// This class assigns the maximum cluster label to each node. The
// maximum cluster labels refers to the cluster label that has
// the maximum label weight. If a node has already been assigned
// a cluster label than it is not reassigned. Initially there are
// a number of nodes that have been assigned a cluster label.
class CAccumulateClusterNodes {

	// This stores the maximum cluster label for a given node
	struct SMaxClusterLabel {
		// This stores the max label weight
		float weight;
		// This stores the max label
		S5Byte clus_id;
	};

	// This maps global dst indexes to local indexes 
	CObjectHashMap<S5Byte> m_node_map;
	// This stores the cluster label for a given node
	CArrayList<SMaxClusterLabel> m_label_buff;

	// This stores the assigned cluster labels
	CFileSet<CHDFSFile> m_label_set;
	// This stores all the forward cluster labels
	CFileSet<CHDFSFile> m_forward_set;
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

	// This finds the maximum cluster label for a given node based upon
	// the forward cluster labels.
	void AssignMaxClusterLabel() {

		SWClusterMap clus_map;
		for(int i=0; i<CNodeStat::GetClientNum(); i++) {
			m_forward_set.OpenReadFile(CNodeStat::GetClientID(), i);

			CHDFSFile &file = m_forward_set.File(CNodeStat::GetClientID());
			while(clus_map.ReadClusterMap(file)) {
				int id = m_node_map.Put(clus_map.base_node);

				if((clus_map.base_node.Value() % CNodeStat::GetClientNum()) != CNodeStat::GetClientID()) {
					cout<<"div mis2";getchar();
				}

				if(m_node_map.AskFoundWord() == false) {
					m_label_buff.ExtendSize(1);
					m_label_buff.LastElement().weight =  0;
				}

				SMaxClusterLabel &label = m_label_buff[id];
				if(clus_map.weight >= label.weight) {
					if(clus_map.weight != label.weight || clus_map.cluster != label.clus_id) {
						label.weight = clus_map.weight;
						label.clus_id = clus_map.cluster;
					}
				}
			}

			file.CloseFile();
		}
	}

	// This updates the cluster label for each node in the back buffer
	void UpdateBackBuffer() {

		SWClusterMap clus_map;
		SRemapClusterMap remap;
		for(int i=0; i<CNodeStat::GetClientNum(); i++) {
			m_clus_label_set.OpenReadFile(CNodeStat::GetClientID(), i);
			CHDFSFile &clus_file = m_clus_label_set.File(CNodeStat::GetClientID());

			while(clus_map.ReadClusterMap(clus_file)) {

				if((clus_map.base_node.Value() % CNodeStat::GetClientNum()) != CNodeStat::GetClientID()) {
					cout<<"div mis1";getchar();
				}

				remap.base_node = clus_map.base_node;
				remap.cluster = clus_map.cluster;
				remap.weight = clus_map.weight;

				int id = m_node_map.Get(clus_map.base_node);
				if(id >= 0) {
					SMaxClusterLabel &label = m_label_buff[id];
					remap.cluster_remap = label.clus_id;
					remap.clus_weight = label.weight;
				} else {
					remap.clus_weight = clus_map.weight;
					remap.cluster_remap = clus_map.cluster;
				}
			
				int hash = clus_map.cluster.Value() % CNodeStat::GetClientNum();
				remap.WriteClusterMap(m_label_set.File(hash));
			}

			clus_file.CloseFile();
		}
	}

public:

	CAccumulateClusterNodes() {
	}

	// This is the entry function
	void AccumulateClusterNodes(int client_id, int client_num) {

		CNodeStat::SetClientID(client_id);
		CNodeStat::SetClientNum(client_num);

		m_forward_set.SetFileName("LocalData/forward_wave_pass");
		m_forward_set.AllocateFileSet(CNodeStat::GetClientNum());

		m_clus_label_set.AllocateFileSet(CNodeStat::GetClientNum());
		m_clus_label_set.SetDirectory("LocalData/clus_label");

		m_node_map.Initialize(HashCode, Equal);
		m_label_buff.Initialize(1024);

		AssignMaxClusterLabel();

		m_label_set.OpenWriteFileSet("LocalData/label_set", 
			CNodeStat::GetClientNum(), CNodeStat::GetClientID());

		UpdateBackBuffer();
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);
	int client_num = atoi(argv[2]);

	CBeacon::InitializeBeacon(client_id, 5555);
	CMemoryElement<CAccumulateClusterNodes> acc;
	acc->AccumulateClusterNodes(client_id, client_num);

	acc.DeleteMemoryElement();

	CBeacon::SendTerminationSignal();

	return 0;
}