#include "../../../../MapReduce.h"

// This creates a final clustered link set from the set of reduced 
// maximum links that have also been reversed for the best
// graph connectivity.
class CCreateFinalClusteredLinkSet {

	// This stores one of the nodes in a given link cluster
	struct SClusterNode {
		// This stores the dst node
		S5Byte dst;
		// This stores the link weight
		float link_weight;
		// This stores a ptr to the next cluster node
		SClusterNode *next_ptr;
	};

	// This stores the cluster label for each src node
	CFileSet<CHDFSFile> m_clus_label_set;
	// This stores the distributed links
	CFileSet<CHDFSFile> m_link_set;
	// This stores the clustered link set 
	CHDFSFile m_clus_link_file;

	// This stores the src node map
	CObjectHashMap<S5Byte> m_src_map;
	// This stores the set of cluster nodes
	CArrayList<SClusterNode *> m_clus_set;
	// This stores the set of cluster nodes
	CLinkedBuffer<SClusterNode> m_clus_buff;

	// This stores the set of dst nodes
	CArrayList<S5Byte> m_dst_buff;

	// This defines a function that computes the hash map for an object
	static int HashCode(const S5Byte &item) {
		return (int)S5Byte::Value(item);
	}

	// This defines a function to compare two nodes for equality
	static bool Equal(const S5Byte &arg1, const S5Byte &arg2) {
		return S5Byte::Value(arg1) == S5Byte::Value(arg2);
	}

	// This adds a set of links to a cluster link set that all have the 
	// same src node. All the dst node and link weight are written.
	void CreateClusterLinkSet() {

		S5Byte src;
		SWClusterMap clus_map;
		m_src_map.ResetNextObject();
		for(int j=0; j<m_src_map.Size(); j++) {
			src = m_src_map.NextSeqObject();
			int id = m_src_map.Get(src);

			float link_weight = 0;
			SClusterNode *curr_ptr = m_clus_set[id];
			while(curr_ptr != NULL) {
				m_dst_buff.PushBack(curr_ptr->dst);
				link_weight += curr_ptr->link_weight;
				curr_ptr = curr_ptr->next_ptr;
			}

			m_clus_link_file.AddEscapedItem(m_dst_buff.Size());
			m_clus_link_file.WriteCompObject(src);
			clus_map.base_node = src;
			clus_map.cluster = src;
			clus_map.weight = link_weight;

			if(m_dst_buff.Size() == 1 && m_dst_buff[0] > src) {
				clus_map.cluster = m_dst_buff[0];
			} 

			int hash = clus_map.base_node.Value() % CNodeStat::GetClientNum();
			clus_map.WriteClusterMap(m_clus_label_set.File(hash));

			for(int i=0; i<m_dst_buff.Size(); i++) {
				m_clus_link_file.WriteCompObject(m_dst_buff[i]);
			}

			m_dst_buff.Resize(0);
		}
	}


	// This loads the final clustered link set
	void LoadClusteredLinkSet() {

		SWLink w_link;
		for(int i=0; i<CNodeStat::GetClientNum(); i++) {
			m_link_set.OpenReadFile(CNodeStat::GetClientID(), i);
			CHDFSFile &link_file = m_link_set.File(CNodeStat::GetClientID());

			while(w_link.ReadWLink(link_file)) {
				if((w_link.src.Value() % CNodeStat::GetClientNum()) != CNodeStat::GetClientID()) {
					cout<<"div mis";getchar();
				}

				int id = m_src_map.Put(w_link.src);
				if(m_src_map.AskFoundWord() == false) {
					m_clus_set.PushBack(NULL);
				}

				if(w_link.src == w_link.dst) {
					// used as a placeholder for src do not add
					continue;
				}

				SClusterNode *prev_ptr = m_clus_set[id];
				m_clus_set[id] = m_clus_buff.ExtendSize(1);
				m_clus_set[id]->dst = w_link.dst;
				m_clus_set[id]->link_weight = w_link.link_weight;
				m_clus_set[id]->next_ptr = prev_ptr;
			}
		}
	}

public:

	CCreateFinalClusteredLinkSet() {
	}

	// This is the entry function
	void CreateFinalClusteredLinkSet(int client_id, int client_num) {

		CNodeStat::SetClientID(client_id);
		CNodeStat::SetClientNum(client_num);

		m_clus_link_file.OpenWriteFile(CUtility::ExtendString
			(SUBM_SET_DIR, CNodeStat::GetClientID()));

		m_clus_label_set.OpenWriteFileSet("LocalData/clus_label", 
			CNodeStat::GetClientNum(), CNodeStat::GetClientID());

		m_link_set.SetDirectory("LocalData/label_set");
		m_link_set.AllocateFileSet(CNodeStat::GetClientNum());

		m_clus_buff.Initialize();
		m_dst_buff.Initialize(1000);
		m_clus_set.Initialize(4000000);
		m_src_map.Initialize(HashCode, Equal);

		LoadClusteredLinkSet();
		CreateClusterLinkSet();
	}

};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);
	int client_num = atoi(argv[2]);

	CBeacon::InitializeBeacon(client_id, 5555);
	CMemoryElement<CCreateFinalClusteredLinkSet> acc;
	acc->CreateFinalClusteredLinkSet(client_id, client_num);

	acc.DeleteMemoryElement();

	CBeacon::SendTerminationSignal();

	return 0;
}