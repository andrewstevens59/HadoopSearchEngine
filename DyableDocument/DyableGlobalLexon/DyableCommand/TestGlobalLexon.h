#include "../../../ProcessSet.h"

// This stores the association map file
const char *ASSOC_MAP_FILE = "GlobalData/Lexon/assoc_map";
// This stores the directory where the mapping between 
// base nodes and their cluster mapping is stored
const char *BACKWARD_CLUS_MAP_DIR = "GlobalData/ClusterHiearchy/backward_clus_map";

// This is a test framework to ensure that everything is mapped correctly
class CTestGlobalLexon : public CNodeStat {

	// This stores the node map
	CTrie m_node_map;
	// This stores the excerpt keyword buffer
	CArrayList<char> m_excerpt_key_buff;
	// This stores the excerpt keyword offset
	CArrayList<int> m_excerpt_key_offset;

	// This reads the keyword text string
	bool ReadKeywordTextStrings(CHDFSFile &file) {

		static S5Byte node;
		char *map_buff = CUtility::SecondTempBuffer();
		if(!file.ReadCompObject(map_buff, 5)) {
			return false;
		}

		m_node_map.AddWord(map_buff, sizeof(S5Byte));
		if(m_node_map.AskFoundWord()) {
			cout<<"Map Already Found";getchar();
		}

		// read in the text string corresponding to the keyword
		static uChar word_length;
		file.ReadCompObject(map_buff, sizeof(float));
		file.ReadCompObject(map_buff + sizeof(float), 1);
		word_length = (uChar)map_buff[4];
		file.ReadCompObject(map_buff + sizeof(float) + 1, word_length);
		int map_bytes = word_length + sizeof(float) + 1;

		m_excerpt_key_buff.CopyBufferToArrayList(map_buff, 
			map_bytes, m_excerpt_key_buff.Size());
		m_excerpt_key_offset.PushBack(m_excerpt_key_offset.LastElement()
			+ map_bytes);

		return true;
	}

	// This is the entry function
	void LoadMapAssocSet() {

		m_node_map.Initialize(4);
		m_excerpt_key_buff.Initialize();
		m_excerpt_key_offset.Initialize();
		m_excerpt_key_offset.PushBack(0);

		CHDFSFile excerpt_map_file;
		for(int i=0; i<1; i++) {
			excerpt_map_file.OpenReadFile(CUtility::ExtendString
				(ASSOC_MAP_FILE, i));

			while(ReadKeywordTextStrings(excerpt_map_file));
		}
	}

public:

	CTestGlobalLexon() {
	}

	// This is the entry function
	void TestMapExcerptKeywords() {

		char buff;
		S5Byte node1;
		S5Byte node2;
		uLong length1;
		CHDFSFile node_file;
		CSegFile mapped_file;

		LoadMapAssocSet();
		node_file.OpenReadFile("LocalData/word_id0");
		mapped_file.OpenReadFile("LocalData/word_map0");

		while(node_file.ReadCompObject(node1)) {
			int id = m_node_map.FindWord((char *)&node1, sizeof(S5Byte));
			mapped_file.ReadCompObject(node2);

			if(node1 != node2) {
				cout<<"Node Mismatch "<<node1.Value()<<" "<<node2.Value();getchar();
			}

			int offset = m_excerpt_key_offset[id];
			int length2 = m_excerpt_key_offset[id+1] - m_excerpt_key_offset[id];

			for(int j=0; j<length2; j++) {
				mapped_file.ReadCompObject(buff);
				if(buff != m_excerpt_key_buff[offset++]) {
					cout<<"Excerpt Map Mismatch";getchar();
				}
			}
		}
	}
};

// This is used to test the cluster mapping 
class CTestClusterMapping {

	// This stores the node map
	CTrie m_node_map;
	// This stores the cluster mapping
	CArrayList<SClusterMap> m_clus_map_buff;

	// This loads the cluster mapping
	void LoadClusterMapping() {

		m_node_map.Initialize(4);
		m_clus_map_buff.Initialize();

		SClusterMap clus_map;
		CHDFSFile map_file;
		for(int i=0; i<CNodeStat::GetInstNum(); i++) {
			map_file.OpenReadFile(CUtility::ExtendString
				(BACKWARD_CLUS_MAP_DIR, i));

			while(clus_map.ReadClusterMap(map_file)) {
				m_node_map.AddWord((char *)&clus_map.base_node, sizeof(S5Byte));
				if(m_node_map.AskFoundWord()) {
					cout<<"Clus Map Already Exists";getchar();
				}

				m_clus_map_buff.PushBack(clus_map);
			}
		}
	}

public:

	CTestClusterMapping() {
	}

	// This is used to test the cluster mapping 
	void TestClusterMapping() {

		LoadClusterMapping();

		CHDFSFile word_id_file;
		word_id_file.OpenReadFile("LocalData/word_id0");
		CHDFSFile mapped_file;
		mapped_file.OpenReadFile("LocalData/clus_map0");

		S5Byte cluster;
		S5Byte word_id;
		while(word_id_file.ReadCompObject(word_id)) {
			int id = m_node_map.FindWord((char *)&word_id, sizeof(S5Byte));
			mapped_file.ReadCompObject(cluster);
			if(id < 0) {

				if(cluster.IsMaxValueSet() == false) {
					cout<<"mismatch1 "<<cluster.Value();getchar();
				}
				continue;
			}

			if(cluster != m_clus_map_buff[id].cluster) {
				cout<<"mismatch2";getchar();
			}
		}
	}
};