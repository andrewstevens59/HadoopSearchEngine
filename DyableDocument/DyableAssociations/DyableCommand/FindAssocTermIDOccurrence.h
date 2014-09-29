#include "./CreateWordOccurrenceMap.h"

// This finds the occurrence of each individual word id that 
// eixsts in the set of associations.This is used to find 
// the unqiueness of a given term.
class CFindAssocTermIDOccurrence : public CNodeStat {

	// This stores the set of association word ids
	CSegFile m_assoc_id_file;
	// This stores the set of word ids that make up the excerpt documents
	CSegFile m_word_id_set_file;
	// This stores the mapped association id occurrnece
	CSegFile m_mapped_assoc_id_file;

public:

	// This is the entry function
	void FindAssocTermIDOccurrence() {

		m_assoc_id_file.SetFileName("LocalData/assoc_word_id_set2.client");
		CMapReduce::KeyOccurrenceLong(NULL, m_assoc_id_file, m_assoc_id_file, 
			"LocalData/assoc_id_occur", sizeof(uLong), CNodeStat::GetHashDivNum());
	}

	// This maps the association word id occurrence to each of the word ids that 
	// make up the excerpt document set
	void MapAssocTermIDToDocument() {
		
		m_word_id_set_file.SetFileName("LocalData/excerpt_id");
		m_mapped_assoc_id_file.SetFileName("LocalData/mapped_assoc_word_id");
		CMapReduce::ExternalHashMap("MapAssociationsToDocument", m_word_id_set_file, m_assoc_id_file, 
			m_mapped_assoc_id_file, "LocalData/map_assoc_id_set", sizeof(uLong), sizeof(uLong));
	}

};

// This class checks that the occurrence of each association is assigned correctly
class CTestFindAssocTermIDOccurrence {

	// This stores each of the association sets for keywords
	CMemoryChunk<CSegFile> m_keyword_assoc_set_file;
	// This stores each of the associations 
	CTrie m_assoc_map;
	// This stores the occurrence of each association
	CArrayList<int> m_assoc_occur;

public:

	CTestFindAssocTermIDOccurrence() {
		m_assoc_map.Initialize(4);
		m_assoc_occur.Initialize(4);
	}

	// This is the entry function
	void TestFindAssocTermIDOccurrence() {

		int occur;
		uLong word_id1;
		uLong word_id2;
		CSegFile assoc_occur_file;
		m_keyword_assoc_set_file.AllocateMemory(ASSOC_SET_NUM);

		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {

			m_keyword_assoc_set_file[0].OpenReadFile(CUtility::ExtendString
				("LocalData/assoc_occur2.client", i));

			CHDFSFile &keyword_file = m_keyword_assoc_set_file[0];
			while(keyword_file.ReadCompObject(word_id1)) {
				keyword_file.ReadCompObject(word_id2);
				keyword_file.ReadCompObject(occur);

				int id = m_assoc_map.AddWord((char *)&word_id1, sizeof(uLong));
				if(!m_assoc_map.AskFoundWord()) {
					m_assoc_occur.PushBack(1);
				} else {
					m_assoc_occur[id]++;
				}

				id = m_assoc_map.AddWord((char *)&word_id2, sizeof(uLong));
				if(!m_assoc_map.AskFoundWord()) {
					m_assoc_occur.PushBack(1);
				} else {
					m_assoc_occur[id]++;
				}
			}
		}

		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			for(int j=0; j<ASSOC_SET_NUM; j++) {
				assoc_occur_file.OpenReadFile(CUtility::ExtendString
					("LocalData/assoc_word_id_set", j + 2, ".client", i));

				while(assoc_occur_file.ReadCompObject(word_id1)) {
					assoc_occur_file.ReadCompObject(occur);
					int id = m_assoc_map.FindWord((char *)&word_id1, sizeof(uLong));

					if(occur != m_assoc_occur[id]) {
						cout<<"occurrence mis "<<occur<<" "<<m_assoc_occur[id];getchar();
					}
				}
			}
		}
	}
};