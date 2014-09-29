#include "../../../ProcessSet.h"

// This stores the association map file
const char *ASSOC_MAP_FILE = "GlobalData/Lexon/assoc_map";
// This stores the directory where the mapping between 
// base nodes and their cluster mapping is stored
const char *BACKWARD_CLUS_MAP_DIR = "GlobalData/ClusterHiearchy/backward_clus_map";
// This stores all the accumulated s_links that are remapped
// at every level of the hiearchy
const char *ACC_S_LINK_DIR = "GlobalData/SummaryLinks/acc_s_links";
// This stores all the mapped s_link nodes
const char *SLINK_NODE_DIR = "GlobalData/SummaryLinks/s_link_node_set";

// This class takes the set of keywords that have been generated
// for each excerpt and prepares them to be incorporated in to
// the ABTree so that they can be retrieved during lookup. Each
// keyword must be ordered by the doc ids as they appear in the 
// based nodes of the hiearchy. This is so they can be added
// straight to the ABTree external nodes when building the ab_tree.
class CMapExcerptKeywords : public CNodeStat {

	// This stores the base level node set 
	CSegFile m_node_set_file;
	// This stores the set of excerpt keywords for each excerpt
	CSegFile m_excerpt_keyword_map;
	// This stores the set of mapped excerpt keywords or associations
	CSegFile m_mapped_file;

public:

	CMapExcerptKeywords() {
		m_node_set_file.SetFileName("LocalData/node_set");
		m_excerpt_keyword_map.SetFileName("GlobalData/Excerpts/final_excerpt_set");
		m_mapped_file.SetFileName("LocalData/excerpt_mapped");
	}

	// This is the entry function that takes the set set of excerpt
	// keywords and processes them as required.
	void ProcessExcerptKeywords() {

		CMapReduce::ExternalHashMap("WriteExcerptKeywordSet", m_node_set_file, 
			m_excerpt_keyword_map, m_mapped_file, 
			"LocalData/keyword_map", sizeof(S5Byte), 240);
	}

};

// This is a test framework to ensure that everything is mapped correctly
class CTestMapExcerptKeywords : public CNodeStat {

	// This stores the node map
	CTrie m_node_map;
	// This stores the excerpt keyword buffer
	CArrayList<char> m_excerpt_key_buff;
	// This stores the excerpt keyword offset
	CArrayList<int> m_excerpt_key_offset;

	// This reads the set of keywords associated to excerpts
	bool ReadExcerptKeyword(CHDFSFile &file) {

		char *map_buff = CUtility::SecondTempBuffer();
		static uChar keyword_num;
		// reads in the word id
		if(!file.ReadCompObject(map_buff, 5)) {
			return false;
		}

		m_node_map.AddWord(map_buff, sizeof(S5Byte));
		if(m_node_map.AskFoundWord()) {
			cout<<"Map Already Found";getchar();
		}

		// read in the keyword number and checksum
		char *prev_ptr = map_buff;
		file.ReadCompObject(map_buff, 5);
		keyword_num = *((uChar *)map_buff + 4);
		map_buff += 5;

		// reads the word id and the word pos
		for(uChar i=0; i<keyword_num; i++) {
			file.ReadCompObject(map_buff, 5);
			map_buff += 5;
		}

		int map_bytes = (int)(map_buff - prev_ptr);
		m_excerpt_key_buff.CopyBufferToArrayList(prev_ptr, 
			map_bytes, m_excerpt_key_buff.Size());
		m_excerpt_key_offset.PushBack(m_excerpt_key_offset.LastElement()
			+ map_bytes);
		
		return true;
	}

public:

	CTestMapExcerptKeywords() {
	}

	// This loads the excerpt keyword set
	void LoadExcerptKeywords() {

		m_node_map.Initialize(4);
		m_excerpt_key_buff.Initialize();
		m_excerpt_key_offset.Initialize();
		m_excerpt_key_offset.PushBack(0);

		CHDFSFile excerpt_map_file;
		for(int i=0; i<CNodeStat::GetClientNum(); i++) {
			excerpt_map_file.OpenReadFile(CUtility::ExtendString
				("GlobalData/Excerpts/final_excerpt_set", i));

			while(ReadExcerptKeyword(excerpt_map_file));
		}
	}

	// This is the entry function
	void TestMapExcerptKeywords() {

		char buff;
		S5Byte node1;
		S5Byte node2;
		uLong length1;
		CHDFSFile node_file;
		CSegFile mapped_file;
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			node_file.OpenReadFile(CUtility::ExtendString
				("LocalData/node_set", i));

			mapped_file.OpenReadFile(CUtility::ExtendString
				("LocalData/excerpt_mapped", i));

			while(node_file.ReadCompObject(node1)) {
				int id = m_node_map.FindWord((char *)&node1, sizeof(S5Byte));

				mapped_file.ReadCompObject(node2);

				if(node1 != node2) {
					cout<<"Node Mismatch "<<node1.Value()<<" "<<node2.Value();getchar();
				}

				mapped_file.GetEscapedItem(length1);
				if(length1 == 0) {
					continue;
				}

				int offset = m_excerpt_key_offset[id];
				int length2 = m_excerpt_key_offset[id+1] - m_excerpt_key_offset[id];

				if(length2 != length1) {
					cout<<"length mismatch "<<length2<<" "<<(int)length1;getchar();
				}
				for(int j=0; j<length2 - 1; j++) {
					mapped_file.ReadCompObject(buff);
					if(buff != m_excerpt_key_buff[offset++]) {
						cout<<"Excerpt Map Mismatch";getchar();
					}
				}

				mapped_file.ReadCompObject(buff);
			}
		}
	}
};


// This is used to test that the backward cluster mapping is applied
// correctly to all of the cut links
class CTestMapBackwardClusMap : public CNodeStat {

	// This stores the node map
	CTrie m_node_map;
	// This stores the set of cluster maps
	CArrayList<SClusterMap> m_clus_map;
	// This stores the backward mapping
	CSegFile m_backward_map_file;

	// This loads the bacward cluster mapping 
	void LoadClusterMapping() {

		m_node_map.Initialize(4);
		m_clus_map.Initialize(4);

		SClusterMap clus_map;
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			m_backward_map_file.OpenReadFile(CUtility::ExtendString
				(BACKWARD_CLUS_MAP_DIR, i));

			while(clus_map.ReadClusterMap(m_backward_map_file)) {
				m_node_map.AddWord((char *)&clus_map.base_node, sizeof(S5Byte));
				if(m_node_map.AskFoundWord()) {
					cout<<"Map Already Exists";getchar();
				}

				m_clus_map.PushBack(clus_map);
			}
		}
	}

public:

	CTestMapBackwardClusMap() {
	}

	// This is the entry function
	void TestMapBackwardClusMap() {

		LoadClusterMapping();

		S5Byte node1;
		S5Byte node2;
		CHDFSFile mapped_s_link_node_file;
		CHDFSFile s_link_node_file;
		
		for(int i=0; i<CNodeStat::GetHiearchyLevelNum(); i++) {
			for(int j=0; j<CNodeStat::GetHashDivNum() << 1; j++) {
				s_link_node_file.OpenReadFile(CUtility::ExtendString
					(SLINK_NODE_DIR, i, ".set", j));
				mapped_s_link_node_file.OpenReadFile(CUtility::ExtendString
					("LocalData/s_link_node_set", i, ".set", j));

				while(s_link_node_file.ReadCompObject(node1)) {
					int id = m_node_map.FindWord((char *)&node1, sizeof(S5Byte));

					mapped_s_link_node_file.ReadCompObject(node2);
					if(m_clus_map[id].cluster != node2) {
						cout<<"Node Mismatch";getchar();
					}
				}
			}
		}
	}
};