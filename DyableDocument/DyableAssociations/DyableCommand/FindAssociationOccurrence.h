#include "./FindAssocTermIDOccurrence.h"

// This class finds the occurrence of each unique association.
// This is done for each of the association classes. Associations
// with low occurrence are culled from the set.
class CFindAssociationOccurrence : public CNodeStat {

	// This stores each of the association sets for excerpt keywords
	CMemoryChunk<CSegFile> m_excerpt_assoc_set_file;

public:

	CFindAssociationOccurrence() {
		CHDFSFile::Initialize();
	}

	// This is the entry function
	void FindAssociationOccurrence(int assoc_set_size) {

		CSegFile assoc_occur_file(CUtility::ExtendString
			("LocalData/assoc_occur", assoc_set_size, ".client"));

		m_excerpt_assoc_set_file.AllocateMemory(ASSOC_SET_NUM);
		m_excerpt_assoc_set_file[assoc_set_size-2].SetFileName(CUtility::ExtendString
			("LocalData/excerpt_assoc_file", assoc_set_size-2, ".client"));

		CMapReduce::KeyOccurrenceLong(NULL, m_excerpt_assoc_set_file[assoc_set_size-2], 
			assoc_occur_file, "LocalData/assoc", (sizeof(uLong) * assoc_set_size),
			CNodeStat::GetHashDivNum());
	}
};

// This class checks that the occurrence of each association is assigned correctly
class CTestAssociationOccurrence {

	// This stores each of the association sets for keywords
	CMemoryChunk<CSegFile> m_keyword_assoc_set_file;
	// This stores each of the associations 
	CTrie m_assoc_map;
	// This stores the occurrence of each association
	CArrayList<int> m_assoc_occur;

public:

	CTestAssociationOccurrence() {
		m_assoc_map.Initialize(4);
		m_assoc_occur.Initialize(4);
	}

	// This is the entry function
	void TestAssociationOccurrence() {

		int occur;
		CSegFile assoc_occur_file;
		m_keyword_assoc_set_file.AllocateMemory(ASSOC_SET_NUM);

		for(int i=0; i<CNodeStat::GetClientNum(); i++) {

			for(int j=0; j<ASSOC_SET_NUM; j++) {
				CMemoryChunk<uLong> term_id(j + 2);
				m_keyword_assoc_set_file[j].OpenReadFile(CUtility::ExtendString
					("LocalData/excerpt_assoc_file", j, ".client", i));

				CHDFSFile &keyword_file = m_keyword_assoc_set_file[j];
				while(keyword_file.ReadCompObject(term_id.Buffer(), j + 2)) {
					int id = m_assoc_map.AddWord((char *)term_id.Buffer(), sizeof(uLong) * (j + 2));
					if(!m_assoc_map.AskFoundWord()) {
						m_assoc_occur.PushBack(1);
					} else {
						m_assoc_occur[id]++;
					}
				}
			}
		}

		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			for(int j=0; j<ASSOC_SET_NUM; j++) {
				CMemoryChunk<uLong> term_id(j + 2);
				assoc_occur_file.OpenReadFile(CUtility::ExtendString
					("LocalData/assoc_occur", j + 2, ".client", i));

				while(assoc_occur_file.ReadCompObject(term_id.Buffer(), j + 2)) {
					assoc_occur_file.ReadCompObject(occur);
					int id = m_assoc_map.AddWord((char *)term_id.Buffer(), sizeof(uLong) * (j + 2));

					if(occur != m_assoc_occur[id]) {
						cout<<"occurrence mis "<<occur<<" "<<m_assoc_occur[id];getchar();
					}
				}
			}
		}
	}
};
