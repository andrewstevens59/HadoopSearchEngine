#include "./FloodSummaryLinks.h"

// This takes the hiearchy once it is complete and seperates the cluster
// segments and the hiearchy subtrees out into two seperate files. The
// hiearchy is transformed from a depth first order into a breadth first
// order and the cluster segments are sorted by base nodes. It's important
// that the final node cluster mapping is sorted by base node since this
// must be merged with hits which are ordered by doc id.
class CTransformHiearchy : public CNodeStat {

	// This is used to embedd all the summary links in the hiearchy
	CFloodSummaryLinks m_flood_s_links;
	// This is used to create the ab_tree once s_links have been
	// corrctly positioned in the hiearchy
	CCreateABTree m_create_ab_tree; 

	// This sorts s_links by src node, so they can be grouped
	// with their appropriate node in the hiearchy. 
	void SortSLinksBySrc() {

		CFloodSummaryLinks::SLinkSet().CloseFileSet();

		for(int i=0; i<GetHiearchyLevelNum(); i++) {
			CFileComp &sort_file = CFloodSummaryLinks::SLinkSet().File(i);

			if(sort_file.CompBuffer().BytesStored() > 0) {
				CExternalRadixSort sort(15, sort_file.CompBuffer(), sizeof(S5Byte));

				sort_file.OpenReadFile();
				SSummaryLink s_link;
				_int64 start = 0;
				while(s_link.ReadReducedSLink(sort_file)) {
					if(s_link.src < start) {
						cout<<"sort error "<<s_link.src.Value()<<" "<<s_link.dst.Value();getchar();
					}
				
					start  = s_link.src.Value();
				}
			}
		}

		CFloodSummaryLinks::SLinkSet().OpenReadFileSet(GetHiearchyLevelNum(), 
			CNodeStat::GetClientID());

		CCreateABTree::AB_S_LINK_SET().OpenWriteFileSet
			("LocalData/ab_tree_fin", GetHiearchyLevelNum(), GetClientID());
	}

	// This creates the set of sorted base node cluster mappings for this client.
	// In actual fact two sets need to be created a forward mapping and a backward
	// mapping. The forward mapping maps doc id's to cluster id's and the backward
	// mapping does the opposite. The forward mapping is sorted. This sorted set
	// is later merged with the other sorted sets in the command server.
	// @param hiearchy_file - this stores all the subtrees and base node segments
	// @param client_node_offset - this stores the node bound offset for this client
	// @param is_keyword_set - this is true when keyword set is being built
	// @return true if the number of hiearchies are greater than zero
	bool ProcessSubtrees(CHDFSFile &hiearchy_file) {

		SHiearchyStat stat;
		SClusterMap clus_map;
		_int64 hiearchy_num = 0;
		while(stat.ReadHiearchyStat(hiearchy_file)) {
			for(int i=0; i<stat.total_node_num; i++) {
				hiearchy_file.ReadCompObject(clus_map.base_node);
			}

			// this takes the subtrees in this cluster and processes them
			m_flood_s_links.ProcessSubtreeSet(hiearchy_file, 30, 1500);
			hiearchy_num++;
		}

		return hiearchy_num > 0;
	}

	// This is the entry function that takes the s_links generated
	// for a particular client (both forward and back links) and
	// floods the hiearchy. S_links with higher traversal probability
	// are placed higher up in the hiearchy. Note that first different 
	// client sets must be merged. 
	void LoadSLinks(_int64 client_node_start, _int64 client_node_end) {

		SSummaryLink s_link;
		CHDFSFile s_link_file;

		CFloodSummaryLinks::SLinkSet().OpenWriteFileSet
			("LocalData/s_link_level", GetHiearchyLevelNum(), CNodeStat::GetClientID());

		for(int i=0; i<CNodeStat::GetClientNum(); i++) {
			s_link_file.OpenReadFile(CUtility::ExtendString
				("LocalData/s_link_set", i, ".set", CNodeStat::GetClientID()));

			while(s_link.ReadSLink(s_link_file)) {

				if(s_link.src < client_node_start) {
					cout<<"start bound error";getchar();
				}

				if(s_link.src >= client_node_end) {
					cout<<"end bound error "<<s_link.src.Value();getchar();
				}

				if(s_link.subsume_level >= 5) {
					s_link.subsume_level++;
				}

				if(s_link.create_level >= 5) {
					s_link.create_level++;
				}

				int level = max(GetHiearchyLevelNum() - s_link.subsume_level - 1, (int)0);
				s_link.WriteReducedSLink(CFloodSummaryLinks::SLinkSet().File(level));
			}
		}

		SortSLinksBySrc();
	}

public:

	CTransformHiearchy() {
		CHDFSFile::Initialize();
	}

	// This is the entry function
	// @param level_num - this is the number of levels in the hiearchy
	// @param max_slink_num - this is the maximum number of s_links
	//                      - that can be grouped under a give node
	// @param client_node_start - this is the node offset for this
	//                          - segment of the global ab_tree
	bool TransformHiearchy(int client_id, int client_num, int level_num, 
		int max_slink_num, _int64 client_node_start, _int64 client_node_end) {

		CNodeStat::SetInstID(0);
		CNodeStat::SetClientID(client_id);
		CNodeStat::SetClientNum(client_num);
		CNodeStat::SetHiearchyLevelNum(level_num + 1);

		CNodeStat::SetHiearchyLevelNum(level_num + 2);

		m_create_ab_tree.SetupBaseLevelNodeNum();

		LoadSLinks(client_node_start, client_node_end);
		m_flood_s_links.Initialize(&m_create_ab_tree, client_node_start);

		CHDFSFile hiearchy_file;
		hiearchy_file.OpenReadFile(CUtility::ExtendString
			(CLUSTER_HIEARCHY_DIR, GetInstID(), ".set", CNodeStat::GetClientID()));

		if(!ProcessSubtrees(hiearchy_file)) {
			m_create_ab_tree.DestroyABTree();
			return false;
		}

		m_create_ab_tree.FinishABTreeNodeBound();
		m_create_ab_tree.CreateABTrees(client_node_start);
		m_create_ab_tree.DestroyABTree();
		return true;
	}

	// This is a test framework
	void TestTransformHiearchy(int hash_div, _int64 client_node_offset) {

		/*CTestFloodSummaryLinks test1;
		test1.Initialize(hash_div);
		test1.TestFloodSLinks(client_node_offset);*/

		CTestCreateABTree test2;
		test2.TestABTrees(client_node_offset);
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);
	int client_num = atoi(argv[2]);
	int level_num = atoi(argv[3]);
	int max_slink_num = atoi(argv[4]);
	_int64 client_node_start = CANConvert::AlphaToNumericLong
		(argv[5], strlen(argv[5]));
	_int64 client_node_end = CANConvert::AlphaToNumericLong
		(argv[6], strlen(argv[6]));

	CBeacon::InitializeBeacon(client_id);
	CMemoryElement<CTransformHiearchy> create;

	bool is_avail = create->TransformHiearchy(client_id, client_num,
		level_num, max_slink_num, client_node_start, client_node_end);

	if(is_avail == true) {
		create->TestTransformHiearchy(client_id, client_node_start);
	}

	create.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();
}