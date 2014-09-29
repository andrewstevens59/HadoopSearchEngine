#include "../../../../MapReduce.h"

// This class assigns a unique cluster label to each base node.
// A limit is placed on the maximum number of node allowed to
// exist in a cluster. If the limit is reached neighbouring nodes
// are merged together instead to create auxillary cluster groups.
// To decide on the order in which nodes are assigned a cluster
// label they are sorted within their cluster division.
class CAssignClusterLabel : public CNodeStat {

	// This stores the cluster label for a given node
	struct SLabel {
		// This is the maximum weight of the label
		float weight;
		// This is the base node
		S5Byte base_node;
		// This stores a ptr to the next label
		SLabel *next_ptr;
	};

	// This is used for sorting
	struct SLabelPtr {
		SLabel *ptr;
	};

	// This stores the local mapping for each cluster
	CObjectHashMap<S5Byte> m_clus_map;
	// This stores the maximum cluster label for each node
	CLinkedBuffer<SLabel> m_label_buff;
	// This stores the set of labels for each node
	CArrayList<SLabel *> m_label_set;
	//This is used to stores the set of sorted cluster maps
	CArrayList<SLabelPtr> m_sort_buff;

	// This stores the final cluster label
	CSegFile m_clus_label_file;
	// This stores all the join links for each client
	CFileSet<CHDFSFile> m_cluster_set;
	// This stores the maximum number of children allowed in a cluster
	int m_max_child_num;

	// This defines a function that computes the hash map for an object
	static int HashCode(const S5Byte &item) {
		return (int)S5Byte::Value(item);
	}

	// This defines a function to compare two nodes for equality
	static bool Equal(const S5Byte &arg1, const S5Byte &arg2) {
		return S5Byte::Value(arg1) == S5Byte::Value(arg2);
	}

	// This is used to compare cluster labels
	static int CompareClusterLabels(const SLabelPtr &arg1, const SLabelPtr &arg2) {

		SLabel &label1 = *arg1.ptr;
		SLabel &label2 = *arg2.ptr;

		if(label1.weight < label2.weight) {
			return -1;
		}

		if(label1.weight > label2.weight) {
			return 1;
		}

		return 0;
	}

	// This assigns the final cluster label for each node in a cluster
	void AssignFinalClusterLabel(S5Byte &clus_id) {

		int offset = 0;
		SClusterMap clus_map;
		clus_map.cluster = clus_id;
		for(int i=0; i<m_sort_buff.Size(); i++) {
			SLabel &label = *m_sort_buff[i].ptr;

			if(++offset >= m_max_child_num) {
				clus_map.cluster = label.base_node;
				offset = 0;
			}

			clus_map.base_node = label.base_node;
			clus_map.WriteClusterMap(m_clus_label_file);
		}
	}


	// This parses each cluster division and sorts each cluster map by the weight
	void ParseClusterSet() {

		m_clus_map.ResetNextObject();
		for(int i=0; i<m_clus_map.Size(); i++) {
			S5Byte &clus_id = m_clus_map.NextSeqObject();
			int id = m_clus_map.Get(clus_id);

			SLabel *curr_ptr = m_label_set[id];
			m_sort_buff.Resize(0);

			while(curr_ptr != NULL) {
				m_sort_buff.ExtendSize(1);
				m_sort_buff.LastElement().ptr = curr_ptr;
				curr_ptr = curr_ptr->next_ptr;
			}

			CSort<SLabelPtr> sort(m_sort_buff.Size(), CompareClusterLabels);
			sort.HybridSort(m_sort_buff.Buffer());

			AssignFinalClusterLabel(clus_id);
		}
	}

public:

	CAssignClusterLabel() {

		m_clus_map.Initialize(HashCode, Equal);
		m_label_buff.Initialize(1024);
		m_label_set.Initialize(1024);
		m_sort_buff.Initialize(1024);
	}

	// This is the entry function
	void AssignClusterLabel(int client_id, int client_num, int max_child_num) {

		CNodeStat::SetClientID(client_id);
		CNodeStat::SetClientNum(client_num);
		m_max_child_num = max_child_num;

		m_cluster_set.SetDirectory("LocalData/clus_label");
		m_cluster_set.AllocateFileSet(CNodeStat::GetClientNum());

		m_clus_label_file.OpenWriteFile(CUtility::ExtendString
			("LocalData/local_clus", CNodeStat::GetClientID()));

		SWClusterMap clus_map;
		for(int i=0; i<CNodeStat::GetClientNum(); i++) {
			m_cluster_set.OpenReadFile(CNodeStat::GetClientID(), i);
			CHDFSFile &label_file = m_cluster_set.File(CNodeStat::GetClientID());

			while(clus_map.ReadClusterMap(label_file)) {
				int id = m_clus_map.Put(clus_map.cluster);
				if(m_clus_map.AskFoundWord() == false) {
					m_label_set.PushBack(NULL);
				}

				if((clus_map.cluster.Value() % CNodeStat::GetClientNum()) != CNodeStat::GetClientID()) {
					cout<<"div mis";getchar();
				}

				SLabel *prev_ptr = m_label_set[id];
				m_label_set[id] = m_label_buff.ExtendSize(1);
				SLabel *curr_ptr = m_label_set[id];
				curr_ptr->next_ptr = prev_ptr;
				curr_ptr->base_node = clus_map.base_node;
				curr_ptr->weight = clus_map.weight;
			}
		}

		ParseClusterSet();
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);
	int client_num = atoi(argv[2]);
	int max_child_num = atoi(argv[3]);

	CBeacon::InitializeBeacon(client_id, 5555);
	CMemoryElement<CAssignClusterLabel> command;
	command->AssignClusterLabel(client_id, client_num, max_child_num);
	command.DeleteMemoryElement();

	CBeacon::SendTerminationSignal();

	return 0;
}