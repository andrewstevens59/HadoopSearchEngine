#include "../../../ProcessSet.h"

// This is used to test that urls are mapped correctly
class CTestSpiderMapping : public CNodeStat {

	// This stores the node map
	CTrie m_node_map;
	// This stores the set of urls
	CArrayList<char> map_buff;
	// This stores the url offset
	CArrayList<int> map_buff_offset;
	// This stores the node set
	CArrayList<S5Byte> m_node_set;

public:

	CTestSpiderMapping() {
	}

	// This loads the url mapping
	void LoadURLMapping() {

		m_node_map.Initialize(4);
		map_buff.Initialize();
		map_buff_offset.Initialize();
		map_buff_offset.PushBack(0);
		m_node_set.Initialize();

		S5Byte node;
		int length;
		CHDFSFile node_file;
		CHDFSFile url_map_file;
		CMemoryChunk<char> buff(1024);
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			url_map_file.OpenReadFile(CUtility::ExtendString
				("LocalData/url_map", i));

			while(url_map_file.ReadCompObject(node)) {
				url_map_file.ReadCompObject(length);
				url_map_file.ReadCompObject(buff.Buffer(), length);
				m_node_map.AddWord((char *)&node, sizeof(S5Byte));
				if(m_node_map.AskFoundWord()) {
					cout<<"URL Already Exists";getchar();
				}

				map_buff.CopyBufferToArrayList(buff.Buffer(), length, map_buff.Size());
				map_buff_offset.PushBack(map_buff_offset.LastElement() + length);
			}

			node_file.OpenReadFile(CUtility::ExtendString
				("LocalData/pulse_node_set", i));

			while(node_file.ReadCompObject(node)) {
				m_node_set.PushBack(node);
			}
		}
	}

	// This tests the url mapping
	void TestURLMapping() {

		int length;
		S5Byte doc_id;
		int offset = 0;

		CHDFSFile node_file;
		CMemoryChunk<char> buff(1024);
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			node_file.OpenReadFile(CUtility::ExtendString
				("LocalData/fin_spider_list", i));

			while(node_file.ReadCompObject(doc_id)) {
				node_file.ReadCompObject(length);
				node_file.ReadCompObject(buff.Buffer(), length);

				S5Byte &node = m_node_set[offset++];

				if(node != doc_id) {
					cout<<"doc id mismach";getchar();
				}

				int id = m_node_map.FindWord((char *)&node, sizeof(S5Byte));

				int length2 = map_buff_offset[id+1] - map_buff_offset[id];

				if(length2 != length) {
					cout<<"length mismatch "<<length2<<" "<<length;getchar();
				}

				int space = map_buff_offset[id];
				for(int j=0; j<length; j++) {
					if(buff[j] != map_buff[space++]) {
						cout<<"mismatch";getchar();
					}
				}
			}
		}

		if(offset != m_node_set.Size()) {
			cout<<"map size mismatch";getchar();
		}
	}
};