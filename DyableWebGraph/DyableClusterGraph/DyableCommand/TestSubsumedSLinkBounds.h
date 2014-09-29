#include "../../Communication.h"

// This stores all the accumulated s_links that are remapped
// at every level of the hiearchy
const char *ACC_S_LINK_DIR = "GlobalData/SummaryLinks/acc_s_links";
// This stores all the mapped s_link nodes
const char *SLINK_NODE_DIR = "GlobalData/SummaryLinks/s_link_node_set";

// This defines a level on the depth first search 
// it stores the total number of children left to
// be processed on the level and also the number 
// of nodes added on the level.
struct SLevel {
	// This stores the number of children left on the level
	int child_num;
	// This stores the weight percentage of s_links
	// to allow to be added to the current level
	float level_weight;
	// This stores a predicate stating if s_links have 
	// been flushed externally yet
	bool flushed;
	// This is used to open the overflow s_link for reading
	// only once at the start of the level
	bool is_ovf_s_link_open;

	SLevel() {
		level_weight = 1.0f;
	}

	void Set(int child_num, float level_weight) {
		flushed = false;
		is_ovf_s_link_open = false;
		this->child_num = child_num;
		this->level_weight = level_weight;
	}
};

// This is used to check that all subsumed s_links have src
// and dst nodes that fall within the same hiearchy boundary.
class CTestSubsumedSLinkBounds : public CNodeStat {

	// This stores the total number of nodes that are grouped
	// under a given level of the hiearchy
	CMemoryChunk<CArrayList<int> > m_hiearchy_node_num;
	// This stores the node boundary for nodes in the hiearchy
	CMemoryChunk<CArrayList<S64BitBound> > m_hiearchy_node_bound;
	// This stores the number of children still to be processed
	// on a given level of the tree
	CArray<SLevel> m_path;

	// This parses the hieachy under a given node
	void TraverseHiearchy(CHDFSFile &hiearchy_file) {

		SSubTree subtree;
		subtree.ReadSubTree(hiearchy_file);

		m_path.Resize(1);
		m_path.LastElement().Set(subtree.child_num, subtree.net_trav_prob);
		m_hiearchy_node_num[m_path.Size() - 1].PushBack(subtree.node_num);
		
		while(true) {
			while(m_path.LastElement().child_num == 0) {
				m_path.PopBack();

				if(m_path.Size() == 0) {
					return;
				}
			}

			m_path.LastElement().child_num--;
			if(m_path.Size() < m_path.OverflowSize()) {

				subtree.ReadSubTree(hiearchy_file);

				m_path.ExtendSize(1);
				m_path.LastElement().Set(subtree.child_num, subtree.net_trav_prob);
				m_hiearchy_node_num[m_path.Size() - 1].PushBack(subtree.node_num);
			}
		}
	}

	// This loads the hiearchy into memory
	void LoadHiearchy() {

		CHDFSFile hiearchy_file;
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			hiearchy_file.OpenReadFile(CUtility::ExtendString
				(CLUSTER_HIEARCHY_DIR, GetInstID(), ".set", i));

			SHiearchyStat stat;
			SClusterMap clus_map;
			while(stat.ReadHiearchyStat(hiearchy_file)) {
				for(int i=0; i<stat.total_node_num; i++) {
					hiearchy_file.ReadCompObject(clus_map.base_node);
				}

				TraverseHiearchy(hiearchy_file);
			}
		}

		for(int i=0; i<m_hiearchy_node_num.OverflowSize(); i++) {

			_int64 curr_offset = 0;
			for(int j=0; j<m_hiearchy_node_num[i].Size(); j++) {
				m_hiearchy_node_bound[i].ExtendSize(1);

				m_hiearchy_node_bound[i].LastElement().Set(curr_offset, curr_offset + m_hiearchy_node_num[i][j]);
				curr_offset += m_hiearchy_node_num[i][j];
			}
		}
	}

	// This prints out the hiearchy
	void PrintHiearchy() {

		for(int i=0; i<m_hiearchy_node_bound.OverflowSize(); i++) {

			for(int j=0; j<m_hiearchy_node_bound[i].Size(); j++) {
				cout<<m_hiearchy_node_bound[i][j].start<<"-"<<m_hiearchy_node_bound[i][j].end<<" ";
			}
			cout<<endl<<endl;
		}
	}

public:

	CTestSubsumedSLinkBounds() {
	}

	// This is the entry function
	void TestSubsumedSLinkBounds() {

		m_hiearchy_node_num.AllocateMemory(GetHiearchyLevelNum() + 1);
		m_hiearchy_node_bound.AllocateMemory(GetHiearchyLevelNum() + 1);
		for(int i=0; i<m_hiearchy_node_bound.OverflowSize(); i++) {
			m_hiearchy_node_num[i].Initialize();
			m_hiearchy_node_bound[i].Initialize();
		}

		m_path.Initialize(GetHiearchyLevelNum() + 1);
		LoadHiearchy();

		S5Byte src;
		S5Byte dst;
		SSummaryLink s_link;
		CHDFSFile acc_s_link_file;
		CSegFile s_link_node_file;
		for(int i=0; i<CNodeStat::GetHiearchyLevelNum(); i++) {
			s_link_node_file.OpenReadFile(CUtility::ExtendString
				(SLINK_NODE_DIR, GetInstID(), ".set", i));
			acc_s_link_file.OpenReadFile(CUtility::ExtendString
				(ACC_S_LINK_DIR, GetInstID(), ".set", i));

			while(s_link.ReadSLink(acc_s_link_file)) {
				s_link_node_file.ReadCompObject(src);
				s_link_node_file.ReadCompObject(dst);

				if(s_link.is_forward_link == false) {
					continue;
				}

				bool found = false;
				for(int k=0; k<m_hiearchy_node_bound.ElementFromEnd(i + 1).Size(); k++) {
					S64BitBound &node = m_hiearchy_node_bound.ElementFromEnd(i + 1)[k];

					if(src >= node.start && src < node.end) {

						if(dst < node.start || dst >= node.end) {
							cout<<"SLink not subsumed "<<src.Value()<<" "<<dst.Value();getchar();
						} else {
							found = true;
							break;
						}
					}
				}

				if(found == false) {
					cout<<"SLink Not Found "<<src.Value()<<" "<<dst.Value();getchar();
				}
			}
		}
	}
};