#include "./MapAssociationsToDocument.h"

// This finds the keyword link occurrence of each each link
// present in the document set. This is used to create the 
// final set of keyword links that are culled.
class CFindKeywordLinkWeight : public CNodeStat {

	// This stores the set of keyword links
	CSegFile m_keyword_link_file;
	// This stores the reduced keyword link and the occurrence
	CSegFile m_red_keyword_weight;

public:

	CFindKeywordLinkWeight() {
	}

	// This is the entry function
	void FindKeywordLinkWeight() {

		m_keyword_link_file.SetFileName("LocalData/fin_link_set");
		m_red_keyword_weight.SetFileName("LocalData/fin_link_set_occur");

		CMapReduce::KeyWeightFloat(NULL, m_keyword_link_file, 
			m_red_keyword_weight, "LocalData/keyword_link", 
			sizeof(S5Byte) << 1, CNodeStat::GetHashDivNum());
	}
};

// This is used to test that keyword link occurrence is functioning correctly.
class CTestFindKeywordLinkWeight {

	// This stores the set of keyword links 
	CTrie m_keyword_link_map;
	// This stores the occurrence of each link set
	CArrayList<float> m_key_link_weight;

public:

	CTestFindKeywordLinkWeight() {
	}

	// This loads the keyword link set
	void LoadKeywordLinkSet(int client_num) {

		m_keyword_link_map.Initialize(4);
		m_key_link_weight.Initialize(4);

		float weight;
		CHDFSFile keyword_link_file;
		CMemoryChunk<S5Byte> buff(2);
		for(int i=0; i<client_num; i++) {
			keyword_link_file.OpenReadFile(CUtility::ExtendString
				("LocalData/fin_link_set", i));

			cout<<i<<" "<<client_num<<endl;
			while(keyword_link_file.ReadCompObject(buff.Buffer(), 2)) {
				keyword_link_file.ReadCompObject(weight);

				int id = m_keyword_link_map.AddWord((char *)buff.Buffer(), sizeof(S5Byte) * 2);
				if(!m_keyword_link_map.AskFoundWord()) {
					m_key_link_weight.PushBack(weight);
				} else {
					m_key_link_weight[id] += weight;
				}
			}
		}
	}

	// This is the entry function
	void TestFindKeywordLinkWeight(int client_num, int hash_div_num) {

		float weight;
		CHDFSFile red_keyword_weight;
		CMemoryChunk<S5Byte> buff(2);

		int offset = 0;
		for(int i=0; i<hash_div_num; i++) {
			red_keyword_weight.OpenReadFile(CUtility::ExtendString
				("LocalData/fin_link_set_occur", i));

			while(red_keyword_weight.ReadCompObject(buff.Buffer(), 2)) {

				offset++;
				red_keyword_weight.ReadCompObject(weight);
				int id = m_keyword_link_map.FindWord((char *)buff.Buffer(), sizeof(S5Byte) * 2);
				if(m_key_link_weight[id] != weight) {
					cout<<"weight mis "<<m_key_link_weight[id]<<" "<<weight;getchar();
				}
			}
		}

		if(offset != m_keyword_link_map.Size()) {
			cout<<"Keyword Link Num Mismatch";getchar();
		}
	}
};