#include "../../../MapReduce.h"

// This stores all the accumulated s_links that are remapped
// at every level of the hiearchy
const char *ACC_S_LINK_DIR = "GlobalData/SummaryLinks/acc_s_links";
// This stores the directory where the mapping between 
// base nodes and their cluster mapping is stored
const char *BACKWARD_CLUS_MAP_DIR = "GlobalData/ClusterHiearchy/backward_clus_map";

// This class takes the set of cut links and s_links and
// distributes them based upon their node boundary location
// in the ab_tree. This is done using a binary search.
class CDistributeLinks : public CNodeStat {

	// This stores the backward mapping
	CSegFile m_backward_map_file;
	// This stores the cut link set for all of the different clients
	CFileSet<CHDFSFile> m_client_set;
	// This stores the root level boundary used to
	// partition the link set between individual ab_trees
	CMemoryChunk<_int64> m_client_boundary;

	// This finds the client set a s_link belongs to.
	// @param node - the node that belongs to the s_link 
	//             - that is being attatched to a client
	int ClientSetID(S5Byte &node) {

		int start = 0; 
		int end = m_client_boundary.OverflowSize() - 1;
		// does a binary search to find the right client
		while(end - start > 1) {
			int mid = (start + end) >> 1;
			if(node.Value() >= m_client_boundary[mid]) {
				start = mid;
				continue;
			}
			end = mid;
		}

		if(node < m_client_boundary[start] || node >= m_client_boundary[start+1]) {
			cout<<"Bound Align Error "<<node.Value();getchar();
		}

		return start;
	}

	// This adds a forward and back link to the appropriate client set.
	// @param s_link - this is the summary link that's being added
	inline void AddSLink(SSummaryLink &s_link) {
		int src_set = ClientSetID(s_link.src);
		s_link.is_forward_link = true;
		s_link.WriteSLink(m_client_set.File(src_set));
	}

	// This creates the s_links corresponding to the cut links
	// on the top level that were never subsumed
	void DistributeCutLinks() {
		
		SClusterLink c_link;
		SSummaryLink s_link;
		CHDFSFile cut_link_file;

		cut_link_file.OpenReadFile(CUtility::ExtendString
			("GlobalData/LinkSet/bin_link_set0.set", CNodeStat::GetClientID()));

		while(c_link.ReadLink(cut_link_file)) {
			s_link.create_level = c_link.c_level;
			s_link.src = c_link.base_link.src;
			s_link.dst = c_link.base_link.dst;
			// just make it an arbitrary high level so
			// it is compatible across multiple hiearchies
			s_link.subsume_level = 120;
			s_link.trav_prob = c_link.link_weight;
			AddSLink(s_link);
		}
	}

	// This distributes the set of s_links belonging to each webgraph instance
	// @param level - this is the current level in the hiearchy being processed
	// @param set - this is the s_link set being processed
	void DistributeSLinks(int level, int set) {

		static SSummaryLink s_link;
		static CHDFSFile s_link_file;
		static CHDFSFile s_link_node_file;

		s_link_file.OpenReadFile(CUtility::ExtendString
			(ACC_S_LINK_DIR, level, ".set", set));

		s_link_node_file.OpenReadFile(CUtility::ExtendString
			("LocalData/s_link_node_set", level, ".set", set));

		while(s_link.ReadSLink(s_link_file)) {
			s_link_node_file.ReadCompObject(s_link.src);
			s_link_node_file.ReadCompObject(s_link.dst);
			AddSLink(s_link);
		}
	}


public:

	CDistributeLinks() {
	}


	// This is the entry function
	void DistributeLinks(int client_id, int client_num, int level_num) {

		CNodeStat::SetInstID(0);
		CNodeStat::SetClientID(client_id);
		CNodeStat::SetClientNum(client_num);
		CNodeStat::SetHiearchyLevelNum(level_num);
		CHDFSFile::Initialize();

		m_client_boundary.ReadMemoryChunkFromFile
			("GlobalData/ClusterHiearchy/cluster_boundary");

		m_client_set.OpenWriteFileSet("LocalData/s_link_set",
			m_client_boundary.OverflowSize() - 1, GetClientID());

		DistributeCutLinks();

		for(int i=0; i<CNodeStat::GetHiearchyLevelNum(); i++) {
			DistributeSLinks(i, GetClientID());
			DistributeSLinks(i, GetClientNum() + GetClientID());
		}
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);
	int client_num = atoi(argv[2]);
	int level_num = atoi(argv[3]);

	CBeacon::InitializeBeacon(client_id);
	CMemoryElement<CDistributeLinks> map;
	map->DistributeLinks(client_id, client_num, level_num);
	map.DeleteMemoryElement();

	CBeacon::SendTerminationSignal();
}
