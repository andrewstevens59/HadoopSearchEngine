#include "./MapGroupedTerms.h"

// This finds the occurrence of all of the grouped terms.
// This is used to update the keyword set.
class CFindGroupedTermOccurrence {

	// This stores the set of grouped terms
	CSegFile m_grouped_term_file;
	// This stores the occurrence of each grouped set
	CSegFile m_grouped_term_occur_file;

public:

	CFindGroupedTermOccurrence() {
	}

	// This is the entry function
	void FindGroupedTermOccurrence() {

		m_grouped_term_file.SetFileName("LocalData/grouped_terms");
		m_grouped_term_occur_file.SetFileName("LocalData/grouped_term_occur");
		CMapReduce::KeyOccurrenceLong(NULL, m_grouped_term_file, 
			m_grouped_term_occur_file, "LocalData/group_occur", 
			sizeof(S5Byte) << 1, CNodeStat::GetHashDivNum());
	}
};

// This is a test framework for FindGroupedTermOccurrence
class CTestFindGroupedTermOccurrence : public CNodeStat {

	// This stores one of the grouped terms in the set
	struct SGroupedTerm {
		S5Byte keyword1;
		S5Byte keyword2;
		uLong occur;
	};

	// This stores the grouped term map
	CTrie m_grouped_term_map;
	// This stores the set of of occurrence for each grouped term
	CArrayList<SGroupedTerm> m_grouped_term_occur;

	// This loads the set of grouped terms to find the occurrence
	void LoadGroupedTerms() {

		m_grouped_term_map.Initialize(4);
		CMemoryChunk<S5Byte> term_set(2);

		CHDFSFile group_term_file;
		for(int i=0; i<CNodeStat::GetClientNum(); i++) {
			group_term_file.OpenReadFile(CUtility::ExtendString("LocalData/grouped_terms", i));

			while(group_term_file.ReadCompObject(term_set.Buffer(), 2)) {
				int id = m_grouped_term_map.AddWord((char *)term_set.Buffer(), sizeof(S5Byte) << 1);

				if(!m_grouped_term_map.AskFoundWord()) {
					m_grouped_term_occur.ExtendSize(1);
					m_grouped_term_occur.LastElement().keyword1 = term_set[0];
					m_grouped_term_occur.LastElement().keyword2 = term_set[1];
					m_grouped_term_occur.LastElement().occur = 1;
				} else {
					m_grouped_term_occur[id].occur++;
				}
			}
		}
	}

public:

	CTestFindGroupedTermOccurrence() {
		m_grouped_term_map.Initialize(4);
		m_grouped_term_occur.Initialize(4);
	}

	// This is the entry function
	void TestFindGroupedTermOccurrence() {

        LoadGroupedTerms();
	
		uLong occur;
		CMemoryChunk<S5Byte> term_set(2);
		CHDFSFile grouped_term_file;
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {

			grouped_term_file.OpenReadFile(CUtility::ExtendString("LocalData/grouped_term_occur", i));
			while(grouped_term_file.ReadCompObject(term_set.Buffer(), 2)) {
				grouped_term_file.ReadCompObject(occur);

				int id = m_grouped_term_map.FindWord((char *)term_set.Buffer(), sizeof(S5Byte) << 1);
				if(id < 0) {
					cout<<"Term Not Found "<<term_set[0].Value()<<" "<<term_set[1].Value();getchar();
				}
				if(m_grouped_term_occur[id].occur != occur) {
					cout<<"Term Occurrence Mismatch "<<m_grouped_term_occur[id].occur<<" "<<occur;getchar();
					cout<<term_set[0].Value()<<" "<<term_set[1].Value();getchar();
				}
			}
		}
	}
};