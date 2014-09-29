#include "../../../../MapReduce.h"

// This stores all the accumulated s_links that are remapped
// at every level of the hiearchy
const char *ACC_S_LINK_DIR = "GlobalData/SummaryLinks/acc_s_links";
// This stores all the mapped s_link nodes
const char *SLINK_NODE_DIR = "GlobalData/SummaryLinks/s_link_node_set";

// This class distributes each of the link clusters 
// based upon the cluster assignment of the src node.
class CDistributeLinkClusters : public CNodeStat {

	// This stores the set of base binary links
	CSegFile m_base_bin_links;
	// This stores the set of cluster binary links
	CSegFile m_clus_bin_links;

	// This stores the new link set
	CHDFSFile m_curr_link_set;
	// This stores the current links that are being added on
	// a given level of the hiearchy
	CHDFSFile m_curr_s_link_file;
	// This stores the s_link node set on a given level of the hiearchy
	CSegFile m_s_link_node_file;
	// This stores the distributed set of cluster links
	CFileSet<CHDFSFile> m_link_clus_set;
	// This stores the current level being processed in the hiearchy
	int m_curr_level;

	// This adds a s_link when it is subsumed. This happens 
	// when the two nodes it joins are merged. 
	// @param w_link - this is one of the summary links 
	//               - for a merged node
	void AddSLink(SClusterLink &c_link) {

		static SSummaryLink s_link;
		s_link.src = c_link.base_link.src;
		s_link.dst = c_link.base_link.dst;
		s_link.create_level = c_link.c_level;
		s_link.subsume_level = (uChar)m_curr_level;
		s_link.trav_prob = c_link.link_weight;

		s_link.WriteSLink(m_curr_s_link_file);
		m_s_link_node_file.WriteCompObject(s_link.src);
		m_s_link_node_file.WriteCompObject(s_link.dst);
	}

	// This creates the new link clusters by first updating the 
	// current link set and then merging link clusters.
	void UpdateLinkSetNodes() {

		SClusterLink c_link;
		while(c_link.ReadLink(m_curr_link_set)) {

			if(!SClusterLink::ReadBaseLink(m_base_bin_links, c_link)) {
				cout<<"none1";getchar();
			}
			if(!SClusterLink::ReadClusterLink(m_clus_bin_links, c_link)) {
				cout<<"none2";getchar();
			}

			if(c_link.cluster_link.src == c_link.cluster_link.dst) {

				// the link has been subsumed
				AddSLink(c_link);
				continue;
			}

			int hash = c_link.cluster_link.src.Value()
				% CNodeStat::GetClientNum();

			c_link.WriteLink(m_link_clus_set.File(hash));
		}
	}

	// This returns one of the partitioned directories
	char *PartitionedDir(const char dir[]) {
		strcpy(CUtility::SecondTempBuffer(), CUtility::ExtendString
			(dir, CNodeStat::GetInstID(), ".client"));
		return CUtility::SecondTempBuffer();
	}

public:

	CDistributeLinkClusters() {
		CHDFSFile::Initialize();
	}

	// This is the entry function
	void DistributeLinkClusters(int client_id, int client_num,
		int inst_id, int curr_level) {

		CNodeStat::SetClientID(client_id);
		CNodeStat::SetClientNum(client_num);
		CNodeStat::SetInstID(inst_id);
		CBeacon::InitializeBeacon(client_id, 5555 + inst_id);

		m_curr_level = curr_level;
		m_curr_s_link_file.OpenWriteFile(CUtility::ExtendString
			(ACC_S_LINK_DIR, curr_level, ".set", CNodeStat::GetClientID()));
		m_s_link_node_file.OpenWriteFile(CUtility::ExtendString
			(SLINK_NODE_DIR, curr_level, ".set", CNodeStat::GetClientID()));

		m_link_clus_set.OpenWriteFileSet(PartitionedDir("LocalData/link_clus"),
			CNodeStat::GetClientNum(), CNodeStat::GetClientID());

		m_curr_link_set.OpenReadFile(CUtility::ExtendString
			("GlobalData/LinkSet/bin_link_set", CNodeStat::GetInstID(),
			".set", CNodeStat::GetClientID()));

		m_base_bin_links.OpenReadFile(CUtility::ExtendString
			("LocalData/base_node_set", GetInstID(), ".set",
			CNodeStat::GetClientID()));
		m_clus_bin_links.OpenReadFile(CUtility::ExtendString
			("LocalData/cluster_node_set", GetInstID(), ".set",
			CNodeStat::GetClientID()));

		UpdateLinkSetNodes();
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);
	int client_num = atoi(argv[2]);
	int inst_id = atoi(argv[3]);
	int curr_level = atoi(argv[4]);

	CMemoryElement<CDistributeLinkClusters> dist;
	dist->DistributeLinkClusters(client_id, client_num, inst_id, curr_level);
	dist.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();
}