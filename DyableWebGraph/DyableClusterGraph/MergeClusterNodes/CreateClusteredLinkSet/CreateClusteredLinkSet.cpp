#include "../../../../MapReduce.h"

// This class takes the set of binary links created 
// while updating the link set and produces a clustered
// version of the link set which is needed by WavePass.
// In addition the initial back buffer WavePass distribution
// must be generated.
class CCreateClusteredLinkSet : public CNodeStat {

	// This stores one of the nodes in a given link cluster
	struct SClusterNode {
		// This stores the dst node
		S5Byte dst;
		// This stores the link weight
		float link_weight;
		// This stores a ptr to the next cluster node
		SClusterNode *next_ptr;
	};

	// This stores the binary link set
	CHDFSFile m_bin_link_set_file;
	// This stores the clustered link set 
	CHDFSFile m_clus_link_file;
	// This stores all the forward class labels
	CFileSet<CHDFSFile> m_forward_set;

	// This stores the src node map
	CObjectHashMap<S5Byte> m_src_map;
	// This stores the set of cluster nodes
	CArrayList<SClusterNode *> m_clus_set;
	// This stores the set of cluster nodes
	CLinkedBuffer<SClusterNode> m_clus_buff;

	// This stores the set of dst nodes
	CArrayList<S5Byte> m_dst_buff;
	// This stores the set of link weights
	CArrayList<float> m_link_weight_buff;

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
		SWLink w_link;
		int count = 0;
		m_src_map.ResetNextObject();
		for(int j=0; j<m_src_map.Size(); j++) {
			src = m_src_map.NextSeqObject();
			int id = m_src_map.Get(src);

			if(++count >= 400000) {
				cout<<j<<" Out Of "<<m_src_map.Size()<<endl;
				count = 0;
			}

			int link_weight = 0;
			SClusterNode *curr_ptr = m_clus_set[id];
			while(curr_ptr != NULL) {
				m_dst_buff.PushBack(curr_ptr->dst);
				link_weight += curr_ptr->link_weight;
				m_link_weight_buff.PushBack(curr_ptr->link_weight);
				curr_ptr = curr_ptr->next_ptr;
			}

			m_clus_link_file.AddEscapedItem(m_dst_buff.Size());
			m_clus_link_file.WriteCompObject(src);

			for(int i=0; i<m_dst_buff.Size(); i++) {
				w_link.dst = m_dst_buff[i];
				int hash = w_link.dst.Value() % CNodeStat::GetClientNum();
				m_clus_link_file.WriteCompObject(hash);

				w_link.link_weight = m_link_weight_buff[i];
				m_forward_set.File(hash).WriteCompObject(w_link.dst);
				m_forward_set.File(hash).WriteCompObject(w_link.link_weight);
			}

			m_dst_buff.Resize(0);
			m_link_weight_buff.Resize(0);
		}
	}

	// This hashes the cluster links into buckets to be processed by Wavepass.
	// Once hashed clusters of links are created for faster processing.	This 
	// set just stores the src node and dst nodes along with the link weight.
	void LoadClusterLinkSet() {

		SClusterLink c_link;
		m_clus_buff.Initialize();
		m_dst_buff.Initialize(1000);
		m_link_weight_buff.Initialize(1024);
		m_clus_set.Initialize(4000000);
		m_src_map.Initialize(HashCode, Equal);

		while(c_link.ReadLink(m_bin_link_set_file)) {
			SBLink &b_link = c_link.cluster_link;

			int id = m_src_map.Put(b_link.src);
			if(m_src_map.AskFoundWord() == false) {
				m_clus_set.PushBack(NULL);
			}

			SClusterNode *prev_ptr = m_clus_set[id];
			m_clus_set[id] = m_clus_buff.ExtendSize(1);
			m_clus_set[id]->dst = b_link.dst;
			m_clus_set[id]->link_weight = c_link.link_weight;
			m_clus_set[id]->next_ptr = prev_ptr;
		}
	}

public:

	CCreateClusteredLinkSet() {
	}

	// This is the entry function
	void CreateClusteredLinkSet(int client_id, int client_num) {

		CNodeStat::SetClientID(client_id);
		CNodeStat::SetClientNum(client_num);
		CNodeStat::SetInstID(0);

		m_forward_set.OpenWriteFileSet("LocalData/forward_wave_pass",
			CNodeStat::GetClientNum(), CNodeStat::GetClientID());

		m_bin_link_set_file.OpenReadFile(CUtility::ExtendString
			("GlobalData/LinkSet/bin_link_set0.set", CNodeStat::GetClientID()));

		m_clus_link_file.OpenWriteFile(CUtility::ExtendString
			("LocalData/local_clus", CNodeStat::GetClientID()));

		LoadClusterLinkSet();
		CreateClusterLinkSet();
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);
	int client_num = atoi(argv[2]);

	CBeacon::InitializeBeacon(client_id, 5555);

	CMemoryElement<CCreateClusteredLinkSet> set;
	set->CreateClusteredLinkSet(client_id, client_num);
	set.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();
}