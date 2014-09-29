#include "./TestFloodSummaryLinks.h"

// This class is responsible for creating the ABTrees once all s_links
// have been embedded in the hiearchy. ABTrees are stored in a limited
// depth first manner. The s_links are stored in the hiearchy in a breadth
// first manner. Each ABTree is constructed such that it has a constant 
// height. The tree itself is traversed using a postorder traversal. This
// allows the byte offsets of each child node to be know in advance to allow
// a parent node to link to each respective child. Child linking is done by
// specifying a backwards byte offset from the parent node to each of the 
// child nodes. In particular to jump to a child node the byte offset must 
// be subtracted away from the parent node. Every node in an ABTree contains
// all the s_links have already been created aswell as the total node number,
// traversal probability and child byte offsets.

// Also due to the way the ab_tree was initially constructed during the 
// cluster phase, a large portion of the nodes have only a single child and
// no s_links. These nodes need to be left out when consructing the final
// ab_tree for compactness. This is handled by simply leaving them out of 
// the postorder traversal.
class CCreateABTree : public CNodeStat {

	// This defines the height of the ABTree that each tree.
	// This must be stated ahead of time so the child nodes
	// can be correctly determined
	static const int AB_TREE_HIEGHT = 4;

	// This defines the bound on a given level that specifies the number
	// of base nodes for a given ABTree
	struct SBaseNodeNum {
		// This stores the total number of base nodes
		int total_base_node;
		// This stores the current number of base nodes
		// processed for this level of the hiearchy
		int curr_base_node;

		SBaseNodeNum() {
			total_base_node = 0;
			curr_base_node = 0;
		}

		// This returns the number of nodes left to be processed
		inline int NodesLeft() {
			return total_base_node - curr_base_node;
		}

		// This writes a base node to file
		inline void WriteBaseNode(CHDFSFile &file) {
			file.WriteCompObject(total_base_node);
		}

		// This reads a base node from file
		inline bool ReadBaseNode(CHDFSFile &file) {
			curr_base_node = 0;
			return file.ReadCompObject(total_base_node);
		}
	};

	// This stores all the abtree nodes for each level of 
	// the hiearchy this includes all the high priority s_links
	static CFileSet<CHDFSFile> m_ab_s_link_set;
	// This stores the current ab_node being processed on 
	// each level of the hiearchy
	CMemoryChunk<SABNode> m_curr_ab_node;
	// This stores the ab node byte offsets for all nodes
	// on a given level of the hiearchy
	CMemoryChunk<CArrayList<_int64> > m_ab_byte_offset;

	// This stores the current byte offset that is being processed
	CMemoryChunk<int> m_curr_byte_offset;
	// This is the number of child left to be processed on 
	// the given level of the hiearchy
	CMemoryChunk<int> m_child_left;

	// This stores the base level node number on each ABTree 
	// aligned level of the hiearchy
	CMemoryChunk<SBaseNodeNum> m_base_level_num;
	// This stores the number of nodes on each base level of a
	// ABTree on a given level externally
	CFileSet<CHDFSFile> m_base_level_set;

	// This stores all the ab_trees upon completion
	CFileComp m_ab_tree_comp;
	// This stores all of the excerpt data
	CHDFSFile m_excerpt_file;

	// This stores the root byte offset for this tree
	CArrayList<SABRootByteOffset> m_root_node_buff;
	// This stores the current root node offset for this tree
	_int64 m_curr_root_node;

	// This stores the total number of root level nodes
	int m_root_lev_node_num;
	// This stores the current number of processed root nodes
	int m_curr_root_node_num;

	// This writes an ab_node child byte offsets out to file. Child byte 
	// offsets are not written if the node is an external node. Instead
	// the doc id mapping is written. Note also that the s_links must be
	// written after the child child byte offsets. This is important in 
	// retrieval as it allows s_links to be skiped over if necessary.
	// @param curr_byte_offset - this is the byte offset of the current node
	//                         - being processed
	// @param ab_node - this stores the current ab_node being processed
	// @param ab_file - this stores all the ab_node headers on the current level
	// @param lower_level - this is the index of the lower level, taking into 
	//                    - consideration that some nodes may have been removed
	void WriteABNodeChildOffset(_int64 &curr_byte_offset, 
		SABNode &ab_node, CHDFSFile &ab_file, int lower_level) {

		int &offset = m_curr_byte_offset[lower_level];

		for(uLong i=0; i<ab_node.child_num; i++) {
			_int64 &child_offset = m_ab_byte_offset[lower_level][offset++];

			int byte_diff = (int)(curr_byte_offset - child_offset);
			m_ab_tree_comp.AddEscapedItem(byte_diff);
		}

		if(offset >= m_ab_byte_offset[lower_level].Size()) {
			m_ab_byte_offset[lower_level].Resize(0);
			offset = 0;
		}
	}

	// This transfers the keyword set for the current document being processed.
	void StoreKeywordSet() {

		static S5Byte doc_id;
		static uLong byte_num;
		static CMemoryChunk<char> buff(1024);

		m_excerpt_file.ReadCompObject(doc_id);
		m_excerpt_file.GetEscapedItem(byte_num);
		m_excerpt_file.ReadCompObject(buff.Buffer(), byte_num);

		m_ab_tree_comp.WriteCompObject(doc_id);
		m_ab_tree_comp.AddEscapedItem(byte_num);

		if(byte_num != 0) {
			m_ab_tree_comp.WriteCompObject(buff.Buffer(), byte_num);
		}
	}

	// This writes the ab_node header for the current level and loads
	// in the next sequential ab_node on the current level.
	// @param lower_level - this is the level below in the hiearchy
	//                    - being traversed for the ab_node
	// @param ab_node - this stores statistics relating to the current
	//                - node (child number and s_link number)
	// @param ab_file - this stores all the ab_node headers on the current level
	void WriteCurrLevelABNode(int lower_level, SABNode &ab_node, CHDFSFile &ab_file) {

		static SClusterMap clus_map;
		_int64 curr_byte_offset = m_ab_tree_comp.CompBuffer().BytesStored();
		ab_node.WriteABNode(m_ab_tree_comp);
		_int64 se = curr_byte_offset;
		
		if(lower_level < 0) {
			// external node - read in the doc id mapping and the keyword set
			StoreKeywordSet();
		} else {
			WriteABNodeChildOffset(curr_byte_offset, ab_node, ab_file, lower_level);
		} 

		static SSummaryLink s_link;
		// now write each of the s_links
		for(uLong i=0; i<ab_node.s_link_num; i++) {
			s_link.ReadReducedSLink(ab_file);
			s_link.WriteReducedSLink(m_ab_tree_comp);
		}

		ab_node.ReadABNode(ab_file);
		if(lower_level >= 0) {
			m_child_left[lower_level] = ab_node.child_num;
		}
	}

	// This handles a vacant node in the tree. A vacant node has zero or one 
	// valid children and no s_links. All of the vacant nodes along with the 
	// doc id mapping from the children below must be linked into the current
	// node. This is so it can be propagated up to a valid node.
	// @param lower_level - this is the index of the lower level, taking into 
	//                    - consideration that some nodes may have been removed
	// @param curr_byte_offset - this is a ptr to the byte offset on the 
	//                         - current level of the ab_tree
	// @param ab_node - this stores statistics relating to the current
	//                - node (child number and s_link number)
	// @param ab_file - this stores all the ab_node headers on the current level
	void HandleVacantABNode(int lower_level, _int64 &curr_byte_offset,
		SABNode &ab_node, CHDFSFile &ab_file) {

		int &offset = m_curr_byte_offset[lower_level];
		// transfers the byte offset of the single child to the parent
		curr_byte_offset = m_ab_byte_offset[lower_level][offset++];

		if(offset >= m_ab_byte_offset[lower_level].Size()) {
			m_ab_byte_offset[lower_level].Resize(0);
			offset = 0;
		}

		ab_node.ReadABNode(ab_file);
		m_child_left[lower_level] = ab_node.child_num;
	}

	// This writes one of the ab_nodes to file once it has been
	// accessed during the postorder traversal.
	// @param curr_level - this is the current level in the hiearchy
	//                   - being traversed for the ab_node
	void ProcessABNode(int curr_level) {

		SABNode &ab_node = m_curr_ab_node[curr_level];

		if(curr_level < GetHiearchyLevelNum() - 1) {
			m_ab_byte_offset[curr_level].PushBack
				(m_ab_tree_comp.CompBuffer().BytesStored());
		} else {
			m_root_node_buff.ExtendSize(1);
			m_root_node_buff.LastElement().byte_offset = 
				m_ab_tree_comp.CompBuffer().BytesStored();
			m_root_node_buff.LastElement().node_offset = m_curr_root_node;
			m_curr_root_node += ab_node.total_node_num;
		}

		// this transfers the s_links
		CHDFSFile &ab_file = AB_S_LINK_SET().File(GetHiearchyLevelNum() - curr_level - 1);

		int lower_level = curr_level - 1;
		if(curr_level < GetHiearchyLevelNum() - 1 && curr_level > 0) {

			_int64 &byte_offset = m_ab_byte_offset[curr_level].LastElement();
			if(ab_node.child_num == 1 && ab_node.s_link_num == 0) {
				// remove this node from the tree, it carries no information
				HandleVacantABNode(lower_level, byte_offset, ab_node, ab_file);
				return;
			}
		}

		ab_node.level = curr_level;
		WriteCurrLevelABNode(lower_level, ab_node, ab_file);
	}

	// This does a postorder traversal of the tree, which is used to construct
	// the ABTree on a given level of the hiearchy.
	// @param curr_level - this is the current level in the hiearchy being processed
	// @param curr_ab_level - this is the current level in the ab_tree being processed
	bool PostOrderTraversal(int curr_level, int curr_ab_level) {

		curr_level = min(curr_level, GetHiearchyLevelNum() - 1);
		int base_level = curr_level;
		int child_processed = 0;
		int top_level = min(curr_level + AB_TREE_HIEGHT - 1, GetHiearchyLevelNum() - 1);
		m_child_left[top_level] = 1;

		while(true) {

			while(m_child_left[curr_level] == 0) {
				if(curr_level < top_level) {
					curr_level++;
					continue;
				}

				if(child_processed == m_base_level_num[curr_ab_level].total_base_node) {
					if(curr_ab_level == m_base_level_num.OverflowSize() - 1) {
						m_curr_root_node_num++;
						float percent = (float)m_curr_root_node_num / m_root_lev_node_num;
						cout<<(percent * 100)<<" % Done"<<endl;
					}
					return m_base_level_num[curr_ab_level].ReadBaseNode
						(m_base_level_set.File(curr_ab_level));
				}

				m_child_left[top_level] = 1;
				curr_level = base_level;
			}

			ProcessABNode(curr_level);
			m_child_left[curr_level]--;

			if(curr_level == base_level) {
				child_processed++;
			}

			if(m_child_left[curr_level] > 0) {
				curr_level = curr_ab_level * AB_TREE_HIEGHT;
			}
		}

		return true;
	}

	// This is called to initialize the LocalData structures in this class
	void Initialize() {

		int set_num = GetHiearchyLevelNum() / AB_TREE_HIEGHT;
		if(GetHiearchyLevelNum() % AB_TREE_HIEGHT) {
			set_num++;
		}
		m_base_level_num.AllocateMemory(set_num);

		m_base_level_set.OpenReadFileSet("LocalData/ab_base_level", set_num, GetClientID());
		for(int i=0; i<m_base_level_set.SetNum(); i++) {
			m_base_level_num[i].ReadBaseNode(m_base_level_set.File(i));
		}

		m_ab_tree_comp.OpenWriteFile(CUtility::ExtendString
			(AB_TREE_DIR, GetClientID()), 30000);

		AB_S_LINK_SET().OpenReadFileSet("LocalData/ab_tree_fin", 
			GetHiearchyLevelNum(), GetClientID());

		m_child_left.AllocateMemory(GetHiearchyLevelNum());
		m_curr_ab_node.AllocateMemory(GetHiearchyLevelNum() + 1);

		m_curr_ab_node.LastElement().child_num = 
			m_base_level_num.LastElement().total_base_node;

		for(int i=0; i<GetHiearchyLevelNum(); i++) {
			if(!m_curr_ab_node[i].ReadABNode(AB_S_LINK_SET().File
				(GetHiearchyLevelNum() - i - 1))) {
					cout<<"noe";getchar();
			}
		}

		for(int i=0; i<GetHiearchyLevelNum(); i++) {
			m_child_left[i] = m_curr_ab_node[i+1].child_num;
		}

		m_curr_byte_offset.AllocateMemory(GetHiearchyLevelNum(), 0);
		m_ab_byte_offset.AllocateMemory(GetHiearchyLevelNum());

		for(int i=0; i<GetHiearchyLevelNum(); i++) {
			m_ab_byte_offset[i].Initialize(100);
		}
	}

public:

	CCreateABTree() {
		m_root_lev_node_num = 0;
		m_curr_root_node_num = 0;
		m_root_node_buff.Initialize(1024);
	}

	// This is called to initialize the base level set number regarding
	// the number of base nodes on each level of the ABTree.
	void SetupBaseLevelNodeNum() {
		int set_num = GetHiearchyLevelNum() / AB_TREE_HIEGHT;
		if(GetHiearchyLevelNum() % AB_TREE_HIEGHT) {
			set_num++;
		}

		m_base_level_num.AllocateMemory(set_num);
		m_base_level_set.OpenWriteFileSet("LocalData/ab_base_level",
			set_num, GetClientID());

		m_excerpt_file.OpenReadFile(CUtility::ExtendString
			("LocalData/excerpt_mapped", CNodeStat::GetClientID()));
	}

	//This stores all the abtree nodes for each level of the hiearchy
	// this includes all the high priority s_links
	inline static CFileSet<CHDFSFile> &AB_S_LINK_SET() {
		return m_ab_s_link_set;
	}

	// This takes the generated anchored s_links generated for each
	// node in the hiearchy along with the node bounds and groups
	// them into an ABTree. An ABTree has to be of a fixed size if
	// it overflows a fixed size than a new ABTree must be created.
	// ABTrees are created by doing a limited depth first search.
	// @param node_offset - this store the global node offset for this tree
	// @param excerpt_title_file - this stores all of the compiled 
	//                           - excerpt titles
	void CreateABTrees(_int64 &node_offset) {

		Initialize();
		m_curr_root_node = node_offset;
		int curr_ab_level = 0;
		int curr_level = 0;

		while(true) {

			if(!PostOrderTraversal(curr_level, curr_ab_level)) {
				if(curr_ab_level == m_base_level_num.OverflowSize() - 1) {
					break;
				}

				curr_level += AB_TREE_HIEGHT;	
				curr_ab_level++;
				continue;
			}

			if(curr_ab_level < m_base_level_num.OverflowSize() - 1) {
				m_base_level_num[curr_ab_level + 1].curr_base_node++;
				if(m_base_level_num[curr_ab_level + 1].NodesLeft() == 0) {
					curr_level += AB_TREE_HIEGHT;	
					curr_ab_level++;
					continue;
				}
			}

			curr_level = 0;
			curr_ab_level = 0;
		}
	}

	// This adds a node to the hiearchy on a given level in depth first order.
	// This is used to determine the base level node bounds for every ABTree.
	// This information is needed when constructing the ABTrees using a 
	// postorder traversal.
	// @param level - this is the level at which a node bound is being added
	void AddABTreeNodeBound(int level) {

		level = GetHiearchyLevelNum() - level - 1;

		if(level == GetHiearchyLevelNum() - 1) {
			if(m_base_level_num.LastElement().total_base_node > 0) {
				m_base_level_num.LastElement().WriteBaseNode
					(m_base_level_set.File(m_base_level_num.OverflowSize()-1));
				m_base_level_num.LastElement().total_base_node = 0;
				m_root_lev_node_num++;
			}
		}

		if((level % AB_TREE_HIEGHT) != 0) {
			// not aligned to one of the ABTree levels
			return;
		}

		level /= AB_TREE_HIEGHT;
		m_base_level_num[level].total_base_node++;

		if(level > 0 && m_base_level_num[level-1].total_base_node > 0) {
			m_base_level_num[level-1].WriteBaseNode
				(m_base_level_set.File(level - 1));

			m_base_level_num[level-1].total_base_node = 0;
		}
	}

	// This finishes the node bounds for each ABTree on each level
	void FinishABTreeNodeBound() {
		for(int i=0; i<m_base_level_num.OverflowSize(); i++) {
			if(m_base_level_num[i].total_base_node == 0) {
				continue;
			}

			m_base_level_num[i].WriteBaseNode(m_base_level_set.File(i));
		}
	}

	// This closes all of the files associated with ab_tree
	void DestroyABTree() {
		m_ab_tree_comp.CloseFile();
		m_root_node_buff.WriteArrayListToFile(CUtility::ExtendString
			(AB_ROOT_DIR, GetClientID()));
	}

};

CFileSet<CHDFSFile> CCreateABTree::m_ab_s_link_set;