#include "../../../MapReduce.h"

// This is responsible for hashing the set of join links that are created 
// from the cluster lable stage into the set of appropriate cluster sets. 
class CCreateJoinLink : public CNodeStat {

	// This stores all the join links for each client
	CFileSet<CHDFSFile> m_join_link_set;
	// This stores the set of mapped wave pass nodes 
	CHDFSFile m_mapped_wave_file;
	// This stores the set of cluster links
	CHDFSFile m_clus_link_set_file;
	// This stores the link weight file
	CFileComp m_bin_link_file;

	// This stores the total number of cluster levels
	int m_clus_level;
	// This stores the number of bits that compose a label segment
	int m_label_bit_width;
	// This stores the number of classes in each wave pass cycle
	int m_class_num;
	// This stores the number of different sets to which links
	// are partitioned into based upon their cluster label
	int m_set_num;

	// This finds the cluster group for a particular cluster label
	inline int ClusterLabel(uLong label) {

		int set = 0;
		int factor = 1;
		uLong mask = 0xFF >> (8 - m_label_bit_width);

		for(int i=0; i<m_clus_level; i++) {
			set += (label & mask) * factor;
			factor *= m_class_num;
			label >>= m_label_bit_width;
		}

		return set;
	}

	// This takes all the join links and creates different client sets
	// based upon the majority cluster assignment. A link is only
	// valid if the two connecting nodes have the same majority cluster.
	void CreateClientSets() {

		SWLink w_link;
		SClusterLink c_link;
		SWaveDist src_wave_dist;
		SWaveDist dst_wave_dist;
		
		while(c_link.ReadLink(m_clus_link_set_file)) {
			src_wave_dist.ReadWaveDist(m_mapped_wave_file);
			dst_wave_dist.ReadWaveDist(m_mapped_wave_file);

			if(src_wave_dist.cluster != dst_wave_dist.cluster) {
				continue;
			}

			w_link.link_weight = c_link.link_weight;
			w_link.link_weight *= (1 + dst_wave_dist.majority_weight * 5);
			w_link.link_weight *= (1 + src_wave_dist.majority_weight * 5);
			w_link.src = c_link.cluster_link.src;
			w_link.dst = c_link.cluster_link.dst;
	
			int set = ClusterLabel(src_wave_dist.cluster);
			w_link.WriteWLink(m_join_link_set.File(set));
		}
	}

	// This is used to sort w_links
	static int CompareWLinks(const SExternalSort &arg1, const SExternalSort &arg2) {

		float weight1 = *(float *)(arg1.hit_item + (sizeof(S5Byte) << 1));
		float weight2 = *(float *)(arg2.hit_item + (sizeof(S5Byte) << 1));

		if(weight1 < weight2) {
			return -1;
		}

		if(weight1 > weight2) {
			return 1;
		}

		return 0;
	}

public:

	CCreateJoinLink() {
		CHDFSFile::Initialize();
	}

	// This is the entry function for wave pass links
	void CreateJoinLink(int client_id, int set_num, int class_num, int clus_lev_num) {

		CNodeStat::SetInstID(0);
		CNodeStat::SetClientID(client_id);

		m_set_num = set_num;
		m_class_num = class_num;
		m_clus_level = clus_lev_num;
		m_label_bit_width = CMath::LogBase2(m_class_num);

		m_join_link_set.OpenWriteFileSet("LocalData/wave_join_links", m_set_num, GetClientID());

		m_mapped_wave_file.OpenReadFile(CUtility::ExtendString
			("LocalData/mapped_wave_pass", GetInstID(), ".set", GetClientID()));

		m_clus_link_set_file.OpenReadFile(CUtility::ExtendString
			("GlobalData/LinkSet/bin_link_set", CNodeStat::GetInstID(), ".set", GetClientID()));

		CreateClientSets();
	}

	// This is the entry function for link weights
	void CreateJoinLink(int client_id) {

		CNodeStat::SetClientID(client_id);

		m_bin_link_file.OpenWriteFile(CUtility::ExtendString
			("LocalData/join_links", GetClientID()));

		m_clus_link_set_file.OpenReadFile(CUtility::ExtendString
			("GlobalData/LinkSet/bin_link_set", CNodeStat::GetInstID(), ".set", GetClientID()));

		SWLink w_link;
		SClusterLink c_link;
		while(c_link.ReadLink(m_clus_link_set_file)) {
			w_link.link_weight = c_link.link_weight;
			w_link.src = c_link.cluster_link.src;
			w_link.dst = c_link.cluster_link.dst;
			w_link.WriteWLink(m_bin_link_file);
		}

		CExternalQuickSort sort((sizeof(S5Byte) << 1) + sizeof(float),
			m_bin_link_file.CompBuffer(), CompareWLinks);
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);

	CBeacon::InitializeBeacon(client_id, 5555);
	CMemoryElement<CCreateJoinLink> set;

	if(argc < 4) {
		set->CreateJoinLink(client_id);
	} else {
		int set_num = atoi(argv[2]);
		int class_num = atoi(argv[3]);
		int clus_lev_num = atoi(argv[4]);
	
		set->CreateJoinLink(client_id, set_num, class_num, clus_lev_num);
	}

	set.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();
}