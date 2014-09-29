#include "../../../MapReduce.h"

// This stores the directory where all the ABTrees are stored
const char *AB_TREE_DIR = "GlobalData/ABTrees/ab_tree";
// This stores the directory where pointers to the root
// nodes of all the ab_trees are stored
const char *AB_ROOT_DIR = "GlobalData/ABTrees/ab_root";

// This stores statistics relating to the hiearchy. This 
// includes the number of subtrees below the current node
// and the working cluster id of the subtree.
struct SHiearchyStat1 {
	// Stores the total number stats in the tree
	int total_subtrees;
	// This is the number of nodes in the cluster segment
	int total_node_num;
	// This stores the pulse score of all the subsumed nodes
	float pulse_score;
	// Stores the cluster id
	S5Byte clus_id;

	SHiearchyStat1() {
		total_subtrees = 0;
		total_node_num = 0;
		pulse_score = 0;
	}

	// Reads a hiearchy stat from file
	bool ReadHiearchyStat(CHDFSFile &file) {
		return ReadHiearchyStat(file, *this);
	}

	// Reads a hiearchy stat from file
	static bool ReadHiearchyStat(CHDFSFile &file,
		SHiearchyStat1 &stat) {

		if(!file.ReadCompObject(stat.clus_id)) {
			return false;
		}

		file.ReadCompObject(stat.total_subtrees);
		file.ReadCompObject(stat.total_node_num);
		file.ReadCompObject(stat.pulse_score);
		return true;
	}

	// Writes a hiearchy stat to file
	void WriteHiearchyStat(CHDFSFile &file) {
		WriteHiearchyStat(file, *this);
	}

	// Writes a hiearchy stat to file
	static void WriteHiearchyStat(CHDFSFile &file,
		SHiearchyStat1 &stat) {

		file.WriteCompObject(stat.clus_id);
		file.WriteCompObject(stat.total_subtrees);
		file.WriteCompObject(stat.total_node_num);
		file.WriteCompObject(stat.pulse_score);
	}

	// This transfers a hiearchy stat from one file to another
	static bool TransferHiearchyStat(CHDFSFile &from_file, CHDFSFile &to_file) {
		static SHiearchyStat1 stat;
		if(!ReadHiearchyStat(from_file, stat)) {
			return false;
		}

		WriteHiearchyStat(to_file, stat);
		return true;
	}
};

// This is the directory of the exceprt keyword map
const char *EXCERPT_MAP_FILE = "../../../Data/ABTrees/excerpt_map";
// This stores the association map file
const char *ASSOC_MAP_FILE = "../../../Data/Lexon/assoc_map";

// This is responsible for loading in the keyword set and checking that 
// all titles are placed next to the corresponding external node. This class
// also handles the storing of keywords associated with each word id when
// in keyword mode.
class CTestTitleSet : public CNodeStat {

	// This storse one of the titles in the set 
	struct SKeyword {
		// This stores the number of tokens that compose the set
		int byte_num;
		// stores a ptr to the next keyword
		SKeyword *next_ptr;
	};

	// This stores the mapping of a doc id to the keyword byte_num
	CTrie m_doc_map;
	// This stores the set of doc ids and their titles
	CArrayList<SKeyword *> m_title_list;
	// This stores the set of titles
	CLinkedBuffer<SKeyword> m_title_buff;


public:

	// This loads the set of titles into memory
	void LoadTitleSet() {

		CHDFSFile map_file;
		CMemoryChunk<char> buff(150);
		_int64 doc_id;
		uChar token_num;
		S5Byte token_id;
		uLong check_sum;
		uChar word_pos;

		m_doc_map.Initialize(4);
		m_title_list.Initialize(4);
		m_title_buff.Initialize();

		map_file.OpenReadFile(EXCERPT_MAP_FILE);
		map_file.InitializeCompression();

		while(map_file.Get5ByteCompItem(doc_id)) {
			map_file.ReadCompObject(check_sum);
			map_file.ReadCompObject(token_num);

			int id = m_doc_map.AddWord((char *)&doc_id, 4);
			if(!m_doc_map.AskFoundWord()) {
				m_title_list.PushBack(NULL);
			}

			// this is check_sum + keyword num
			int byte_num = 5;
			for(int j=0; j<token_num; j++) {
				map_file.ReadCompObject(token_id);
				map_file.ReadCompObject(word_pos);
				byte_num += 6;
			}

			SKeyword *prev_ptr = m_title_list[id];
			SKeyword *keyword = m_title_buff.ExtendSize(1);
			m_title_list[id] = keyword;
			keyword->byte_num = byte_num;
			keyword->next_ptr = prev_ptr;
		}
	}

	// This loads the keyword list associated with each word id
	void LoadKeywordList() {

		CHDFSFile map_file;
		S5Byte word_id;
		uChar word_length;
		m_doc_map.Initialize(4);
		m_title_list.Initialize(4);
		m_title_buff.Initialize();

		map_file.OpenReadFile(ASSOC_MAP_FILE);
		map_file.InitializeCompression();

		while(map_file.ReadCompObject(word_id)) { 
			map_file.ReadCompObject(word_length);
			map_file.ReadCompObject(CUtility::SecondTempBuffer(), word_length);

			int id = m_doc_map.AddWord((char *)&word_id, 4);
			if(!m_doc_map.AskFoundWord()) {
				m_title_list.PushBack(NULL);
			}

			SKeyword *prev_ptr = m_title_list[id];
			SKeyword *keyword = m_title_buff.ExtendSize(1);
			m_title_list[id] = keyword;
			keyword->byte_num = word_length;
			keyword->next_ptr = prev_ptr;
		}
	}

	// This writes the set of titles associated with a particular doc id
	void WriteTitlesForDocument(S5Byte &doc_id, CFileComp &ab_file) {

		ab_file.WriteCompObject(doc_id);
		int id = m_doc_map.FindWord((char *)&doc_id, 4);

		if(id < 0) {
			ab_file.AddEscapedItem((int)0);
			return;
		}
		
		SKeyword *keyword = m_title_list[id];
		ab_file.AddEscapedItem(keyword->byte_num);
		ab_file.WriteCompObject(CUtility::SecondTempBuffer(), keyword->byte_num);
	}
};

// This stores a grouping of the hiearchy
struct SHiearchy {
	// This stores the list of subtrees
	SSubTree subtree;
	// This stores the list of segnodes attatched to this hiearchy
	CArrayList<S5Byte> seg_node;
	// This stores the list of s_links embedded at
	// this node in the hiearchy
	CArrayList<SSummaryLink> s_links;

	SHiearchy() {
		seg_node.Initialize(4);
		s_links.Initialize(4);
	}
};

// This stores the current hiearchy
CArray<CVector<SHiearchy> > m_hiearchy;

// This stores the directory where the mapping between 
// base nodes and their cluster mapping is stored
const char *FORWARD_CLUS_MAP_DIR = "../../../Data/ClusterHiearchy/forward_clus_map";
// This stores the directory where the mapping between 
// base nodes and their cluster mapping is stored
const char *BACKWARD_CLUS_MAP_DIR = "../../../Data/ClusterHiearchy/backward_clus_map";

// This tests the ab_tree for pointer integrity. That is the entire tree is 
// traversed by following the byte offsets and checking each node against
// the original node set on each level.
class CTestCreateABTree : public CNodeStat {

	struct SHiearchyNode {
		// This stores the node bound
		S64BitBound node_bound;
		// This is the root node 
		SABNode ab_node;
		// This stores the doc id
		S5Byte doc_id;
		// This stores the jump ptr, NULL if no jump
		SHiearchyNode *jump_ptr;
		// This stores the start of the child nodes
		int child_start;
		// This stores the start of the s_links
		int s_link_start;

		SHiearchyNode() {
			jump_ptr = NULL;
		}
	};

	// This stores the node set for this ab_tree
	CHDFSFile m_node_set_file;
	// This is used to write the final set of s_links 
	// that exist globally for this ab_tree
	CHDFSFile m_s_link_file;

	// This stores the set of root byte offsets
	CArrayList<SABRootByteOffset> m_root_byte_offset;
	// This stores the byte offset on a given level
	CMemoryChunk<CArrayList<_int64> > m_child_byte_offset;
	// This stores the hiearchy
	CMemoryChunk<CVector<SHiearchyNode> > m_hiearchy_set;
	// This stores the final reduces set
	CMemoryChunk<CVector<SHiearchyNode *> > m_red_hiearchy;
	// This stores the set of child nodes
	CArrayList<SHiearchyNode *> m_child_set;
	// This stores the set of s_links
	CArrayList<SSummaryLink> m_s_link_set;

	// This loads the hiearchy into memory
	void LoadHiearchy() {

		m_hiearchy_set.AllocateMemory(GetHiearchyLevelNum());

		m_child_set.Initialize();
		m_s_link_set.Initialize();

		SHiearchyNode ab_node;
		SSummaryLink s_link;
		CHDFSFile level_node_file;
		for(int i=0; i<GetHiearchyLevelNum(); i++) {
			level_node_file.OpenReadFile(CUtility::ExtendString
				("LocalData/ab_tree_fin", GetClientID(), ".set", i));

			int level_size = 0;
			while(ab_node.ab_node.ReadABNode(level_node_file)) {
				m_hiearchy_set[i].PushBack(ab_node);
				SHiearchyNode &node = m_hiearchy_set[i].LastElement();
				node.s_link_start = m_s_link_set.Size();

				if(i == (GetHiearchyLevelNum() - 1)) {
					m_node_set_file.ReadCompObject(node.doc_id);
				}

				for(uLong k=0; k<node.ab_node.s_link_num; k++) {
					s_link.ReadReducedSLink(level_node_file);
					m_s_link_set.PushBack(s_link);
				}
			}
		}
	}

	// This adds the child nodes for each level
	void LoadChildNodes() {

		for(int i=0; i<GetHiearchyLevelNum(); i++) {
			int offset = 0;
			for(int j=0; j<m_hiearchy_set[i].Size(); j++) {
				SHiearchyNode &node = m_hiearchy_set[i][j];
				node.child_start = m_child_set.Size();
				for(uLong k=0; k<node.ab_node.child_num; k++) {
					m_child_set.PushBack(&m_hiearchy_set[i+1][offset++]);
				}
			}
		}
	}

	// This assigns the jump ptrs
	void AssignJumpPtrs() {

		for(int i=0; i<GetHiearchyLevelNum(); i++) {
			for(int j=0; j<m_hiearchy_set[i].Size(); j++) {
				SHiearchyNode &node = m_hiearchy_set[i][j];
				SHiearchyNode *curr_ptr = &node;
				while(curr_ptr->ab_node.child_num == 1 && curr_ptr->ab_node.s_link_num == 0) {
					curr_ptr = m_child_set[curr_ptr->child_start];
				}

				node.jump_ptr = curr_ptr;
			}
		}
	}

	// This creates the reduced hiearchy
	void CreateReducedHiearchy(_int64 client_node_offset) {

		m_red_hiearchy.AllocateMemory(GetHiearchyLevelNum());

		for(int j=0; j<m_hiearchy_set[0].Size(); j++) {
			m_red_hiearchy[0].PushBack(&m_hiearchy_set[0][j]);

			m_hiearchy_set[0][j].node_bound.start = client_node_offset;
			client_node_offset += m_hiearchy_set[0][j].ab_node.total_node_num;
			m_hiearchy_set[0][j].node_bound.end = client_node_offset;
		}

		for(int i=0; i<GetHiearchyLevelNum(); i++) {
			for(int j=0; j<m_red_hiearchy[i].Size(); j++) {
				SHiearchyNode *node = m_red_hiearchy[i][j];
				_int64 node_offset = node->node_bound.start;

				for(int k=0; k<node->ab_node.child_num; k++) {
					SHiearchyNode *child_ptr = m_child_set[node->child_start+k]->jump_ptr;
					m_red_hiearchy[i+1].PushBack(child_ptr);
					child_ptr->node_bound.start = node_offset;
					node_offset += child_ptr->ab_node.total_node_num;
					child_ptr->node_bound.end = node_offset;
				}
			}
		}
	}

	// This checks the s_link set
	void CheckSLinkSet(SSummaryLink &s_link1, SSummaryLink &s_link2, S64BitBound &bound) {

		if(s_link1.src != s_link2.src) {
			cout<<"Src Mismatch "<<s_link1.src.Value()<<" "<<s_link2.src.Value();getchar();
		}

		if(s_link1.dst != s_link2.dst) {
			cout<<"Dst Mismatch";getchar();
		}

		if(s_link1.trav_prob != s_link2.trav_prob) {
			cout<<"Traversal Mismatch";getchar();
		}

		if(s_link1.is_forward_link != s_link2.is_forward_link) {
			cout<<"Forward Link Mismatch";getchar();
		}

		if(s_link1.create_level != s_link2.create_level) {
			cout<<"Create Level Mismatch ";getchar();
		}

		if(s_link1.src < bound.start) {
			cout<<"Node Bound Mismatch "<<CNodeStat::GetClientID()<<endl;getchar();
		}

		if(s_link1.src >= bound.end) {
			cout<<"Node Bound Mismatch "<<CNodeStat::GetClientID()<<endl;getchar();
		}
	}

	// This checks the ab_node
	void CheckABNode(SABNode &ab_node1, SABNode &ab_node2) {

		if(ab_node1.child_num != ab_node2.child_num) {
			cout<<"Child Num Mismatch "<<ab_node1.child_num<<" "<<ab_node2.child_num;getchar();
		}

		if(ab_node1.s_link_num != ab_node2.s_link_num) {
			cout<<"SLink Num Mismatch";getchar();
		}

		if(ab_node1.total_node_num != ab_node2.total_node_num) {
			cout<<"Node Num Mismatch";getchar();
		}

		if(ab_node1.trav_prob != ab_node2.trav_prob) {
			cout<<"Trav Prob Mismatch";getchar();
		}
	}

	// This process a given level in the tree
	void ProcessLevel(int level, CHitItemBlock &ab_file) {

		int enc_bytes;
		SABNode ab_node1;
		_int64 byte_offset;

		S5Byte node;
		SSummaryLink s_link1;

		cout<<"Level "<<level<<endl;

	    int offset = 0;
		for(int k=0; k<m_child_byte_offset[level].Size(); k++) {
			CArrayList<_int64> &curr_level = m_child_byte_offset[level];

			ab_file.RetrieveByteSet(curr_level[k], sizeof(SABNode) + 5,
				CUtility::SecondTempBuffer());

			int bytes = ab_node1.ReadABNode(CUtility::SecondTempBuffer());

			SHiearchyNode *hiearchy_ptr = m_red_hiearchy[level][offset];
			CheckABNode(ab_node1, hiearchy_ptr->ab_node);

			if(ab_node1.child_num == 0) {
				// an external node, retrieve the external doc id mapping
				memcpy((char *)&node, CUtility::SecondTempBuffer() + bytes, 5);

				if(node != hiearchy_ptr->doc_id) {
					cout<<"EXT node mismatch ";getchar();
				}

				bytes += 5;
				char *buff = CUtility::SecondTempBuffer() + bytes;
				bytes += CArray<char>::GetEscapedItem(buff, enc_bytes);
				bytes += enc_bytes;
			}

			_int64 curr_byte_offset = curr_level[k] + bytes;
			for(uLong j=0; j<ab_node1.child_num; j++) {

				ab_file.RetrieveByteSet(curr_byte_offset, 7,
					CUtility::SecondTempBuffer());

				curr_byte_offset += CArray<char>::GetEscapedItem
					(CUtility::SecondTempBuffer(), byte_offset);

				byte_offset = curr_level[k] - byte_offset;
				m_child_byte_offset[level+1].PushBack(byte_offset);
			}

			for(uLong j=0; j<ab_node1.s_link_num; j++) {
				ab_file.RetrieveByteSet(curr_byte_offset, 20,
					CUtility::SecondTempBuffer());

				curr_byte_offset += s_link1.ReadReducedSLink
					(CUtility::SecondTempBuffer());

				s_link1.subsume_level = level;
				s_link1.WriteSLink(m_s_link_file);

				CheckSLinkSet(s_link1, m_s_link_set[hiearchy_ptr->s_link_start+j],
					hiearchy_ptr->node_bound);
			}

			offset++;
		}
	}

	// This prints out the hiearchy
	void PrintHiearchy() {

		for(int i=0; i<m_hiearchy_set.OverflowSize(); i++) {

			for(int j=0; j<m_hiearchy_set[i].Size(); j++) {
				cout<<m_hiearchy_set[i][j].node_bound.start<<"-"<<m_hiearchy_set[i][j].node_bound.end<<" ";
			}
			cout<<endl<<endl;
		}
	}

public:

	CTestCreateABTree() {
	}

	// This compares the output generated by the external hiearchy to the 
	// internal hiearchy. This is done for every single ABTree.
	void TestABTrees(_int64 client_node_offset) {

		CHitItemBlock ab_file;
		m_root_byte_offset.ReadArrayListFromFile(CUtility::ExtendString
			(AB_ROOT_DIR, GetClientID()));
		ab_file.Initialize(CUtility::ExtendString
			(AB_TREE_DIR, GetClientID()), 999999);

		m_node_set_file.OpenReadFile(CUtility::ExtendString
			("LocalData/node_set", CNodeStat::GetClientID()));

		m_s_link_file.OpenWriteFile(CUtility::ExtendString
			("GlobalData/SummaryLinks/test_s_link_set", CNodeStat::GetClientID()));

		LoadHiearchy();
		LoadChildNodes();
		AssignJumpPtrs();
		CreateReducedHiearchy(client_node_offset);

		m_child_byte_offset.AllocateMemory(CNodeStat::GetHiearchyLevelNum());
		for(int i=0; i<m_child_byte_offset.OverflowSize(); i++) {
			m_child_byte_offset[i].Initialize(4);
		}

		CHitItemBlock::InitializeLRUQueue();
		for(int i=0; i<m_root_byte_offset.Size(); i++) {
			m_child_byte_offset[0].PushBack(m_root_byte_offset[i].byte_offset);
		}

		for(int j=0; j<GetHiearchyLevelNum(); j++) {
			ProcessLevel(j, ab_file);
		}
	}
};
