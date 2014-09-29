#include "../../../MapReduce.h"

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
	// This stores all the back class distribution
	CHDFSFile m_back_set_file;

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

	// This stores the global class weight used to keep wave pass balanced
	CMemoryChunk<float> m_class_weight;
	// This stores the net link weight
	float m_net_link_weight;
	// This stores the number of wave pass classes
	int m_class_num;

	// This defines a function that computes the hash map for an object
	static int HashCode(const S5Byte &item) {
		return (int)S5Byte::Value(item);
	}

	// This defines a function to compare two nodes for equality
	static bool Equal(const S5Byte &arg1, const S5Byte &arg2) {
		return S5Byte::Value(arg1) == S5Byte::Value(arg2);
	}

	// This assigns a random distribution to one of the back buffer nodes.
	// This is done during start up before wave pass commences.
	// @param node - this is the current back buffer node being processed
	void AssignBackBuffDist(S5Byte &node) {

		static CMemoryChunk<float> dist(m_class_num);

		m_back_set_file.WriteCompObject(node);

		float sum = 0;
		for(int i=0; i<m_class_num; i++) {
			dist[i] = CMath::RandGauss(0.2f, 1.0f);
			sum += dist[i];
		}
		if(sum == 0) {cout<<"zero sum";getchar();}
		CMath::NormalizeVector(dist.Buffer(), m_class_num);

		for(int i=0; i<m_class_num; i++) {
			m_class_weight[i] += dist[i];
			m_back_set_file.WriteCompObject(dist[i]);
		}
	}

	// This adds a set of links to a cluster link set that all have the 
	// same src node. All the dst node and link weight are written.
	inline void CreateClusterLinkSet() {

		S5Byte src;
		m_src_map.ResetNextObject();
		for(int j=0; j<m_src_map.Size(); j++) {
			src = m_src_map.NextSeqObject();
			int id = m_src_map.Get(src);

			m_net_link_weight = 0;
			SClusterNode *curr_ptr = m_clus_set[id];
			while(curr_ptr != NULL) {
				m_dst_buff.PushBack(curr_ptr->dst);
				m_link_weight_buff.PushBack(curr_ptr->link_weight);
				m_net_link_weight += curr_ptr->link_weight;
				curr_ptr = curr_ptr->next_ptr;
			}

			m_clus_link_file.AddEscapedItem(m_dst_buff.Size());
			m_clus_link_file.WriteCompObject(src);
			AssignBackBuffDist(src);

			for(int i=0; i<m_dst_buff.Size(); i++) {
				m_clus_link_file.WriteCompObject(m_dst_buff[i]);
				m_link_weight_buff[i] /= m_net_link_weight;

				// larger clusters have greater influence
				m_link_weight_buff[i] *= m_dst_buff.Size();
				m_clus_link_file.WriteCompObject(m_link_weight_buff[i]);
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
		m_link_weight_buff.Initialize(1000);
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

	// This returns the directory for the wave pass set
	inline const char *Directory(const char dir[]) {
		strcpy(CUtility::ThirdTempBuffer(), CUtility::ExtendString
			(dir, GetInstID(), ".client0.set"));

		return CUtility::ThirdTempBuffer();
	}

public:

	CCreateClusteredLinkSet() {
		CHDFSFile::Initialize();
	}

	// This is the entry function
	void CreateClusteredLinkSet(int client_id, int client_num, int class_num) {

		CNodeStat::SetClientID(client_id);
		CNodeStat::SetClientNum(client_num);
		CNodeStat::SetInstID(0);
		
		CUtility::RandomizeSeed();
		m_class_num = class_num;

		m_class_weight.AllocateMemory(m_class_num, 0);
		m_back_set_file.OpenWriteFile(CUtility::ExtendString
			("LocalData/back_wave_pass", CNodeStat::GetClientID()));

		m_bin_link_set_file.OpenReadFile(CUtility::ExtendString
			("GlobalData/LinkSet/bin_link_set0.set", CNodeStat::GetClientID()));

		m_clus_link_file.OpenWriteFile(CUtility::ExtendString
			(SUBM_SET_DIR, CNodeStat::GetClientID()));

		LoadClusterLinkSet();
		CreateClusterLinkSet();

		m_class_weight.WriteMemoryChunkToFile(CUtility::ExtendString
			("LocalData/class_weight", CNodeStat::GetClientID()));
	}
};

int main(int argc, char *argv[]) {


	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);
	int client_num = atoi(argv[2]);
	int class_num = atoi(argv[3]);

	CBeacon::InitializeBeacon(client_id, 5555);

	CMemoryElement<CCreateClusteredLinkSet> set;
	set->CreateClusteredLinkSet(client_id, client_num, class_num);
	set.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();
}