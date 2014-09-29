#include "../../../ProcessSet.h"

// This class takes the final set of grouped terms along with their 
// id and maps them to their corresponding grouped terms that exist
// in the excerpt document set. 
class CMapGroupedTerms {

	// This stores the final set of grouped terms
	CSegFile m_group_term_file;
	// This stores the mapping for each grouped term
	CSegFile m_group_term_map_file;
	// This stores the mapped set of grouped terms
	CSegFile m_mapped_group_term_file;

public:

	CMapGroupedTerms() {
	}

	// This is the entry function
	// @param level - this is the current level in the hiearchy
	void MapGroupedTerms(int level) {

		m_group_term_file.SetFileName("LocalData/grouped_terms");

		m_group_term_map_file.SetFileName(CUtility::ExtendString
			("GlobalData/Keywords/fin_group_term_set", level + 1, ".set"));

		m_mapped_group_term_file.SetFileName("LocalData/mapped_group_terms");

		CMapReduce::ExternalHashMap("MapAssociationsToDocument", m_group_term_file,
			m_group_term_map_file, m_mapped_group_term_file, "LocalData/map_group_term", 
			sizeof(S5Byte) << 1, sizeof(uLong) + sizeof(S5Byte));
	}
};

// This is used to test the mapping of grouped terms to the set of document grouped terms
class CTestMapGroupedTermsToDocument : public CNodeStat {

	// This stores the association id and weight
	struct SKeyword {
		_int64 id;
		uLong occur;
	};

	// The stores the association map
	CTrie m_group_term_map;
	// This stores the set of association stats
	CArrayList<SKeyword> m_group_term_set;
	// This stores the set of excerpt associations
	CArrayList<SKeyword> m_excerpt_set;

	// This stores the mapping between associations and their id
	CSegFile m_group_term_map_file;
	// This stores each of the association sets for all words
	CSegFile m_group_term_file;
	// This stores the set of mapped associations 
	CSegFile m_mapped_group_term_file;

	// This loads the association set'
	void LoadGroupTermSet(int hash_div_num, int level) {

		for(int j=0; j<hash_div_num; j++) {

			CMemoryChunk<S5Byte> term_id(2);
			m_group_term_map_file.OpenReadFile(CUtility::ExtendString
				("GlobalData/Keywords/fin_group_term_set", level, ".set", j));

			uLong occur;
			S5Byte assoc_id;
			while(m_group_term_map_file.ReadCompObject(term_id.Buffer(), 2)) {
				m_group_term_map_file.ReadCompObject(assoc_id);
				m_group_term_map_file.ReadCompObject(occur);
				int id = m_group_term_map.AddWord((char *)term_id.Buffer(), sizeof(S5Byte) << 1);
				if(!m_group_term_map.AskFoundWord()) {
					m_group_term_set.ExtendSize(1);
					m_group_term_set.LastElement().id = assoc_id.Value();
					m_group_term_set.LastElement().occur = occur;
				}
			}
		}
	}

	// This creates the set of excerpt associations
	void CreateExcerptAssociations() {

		S5Byte assoc_id;
		for(int j=0; j<CNodeStat::GetClientNum(); j++) {

			CMemoryChunk<S5Byte> buff1(2);
			m_group_term_file.OpenReadFile(CUtility::ExtendString
				("LocalData/grouped_terms", j));

			while(m_group_term_file.ReadCompObject(buff1.Buffer(), 2)) {
				int id = m_group_term_map.FindWord((char *)buff1.Buffer(), sizeof(S5Byte) << 1);
				m_excerpt_set.ExtendSize(1);
				m_excerpt_set.LastElement().occur = INFINITE;
				if(id < 0)continue;
				m_excerpt_set.LastElement() = m_group_term_set[id];
			}
		}
	}

public:

	CTestMapGroupedTermsToDocument() {
		m_group_term_map.Initialize(4);
		m_group_term_set.Initialize(4);
		m_excerpt_set.Initialize(4);
	}

	// This is the entry function
	void TestMapGroupedTermsToDocument(int hash_div_num, int level) {

		LoadGroupTermSet(hash_div_num, level);
		CreateExcerptAssociations();

		S5Byte assoc_id;
		uLong occur;
		int offset = 0;
		for(int j=0; j<CNodeStat::GetClientNum(); j++) {

			CMemoryChunk<S5Byte> buff1(2);
			m_mapped_group_term_file.OpenReadFile(CUtility::ExtendString
				("LocalData/mapped_group_terms", j));

			uChar map_bytes;
			while(m_mapped_group_term_file.ReadCompObject(buff1.Buffer(), buff1.OverflowSize())) {
				m_mapped_group_term_file.ReadCompObject(map_bytes);
				if(map_bytes == 0) {
					offset++;
					continue;
				}
	
				m_mapped_group_term_file.ReadCompObject(assoc_id);
				m_mapped_group_term_file.ReadCompObject(occur);

				if(m_excerpt_set[offset].id != assoc_id.Value()) {
					cout<<"Assoc ID Miss "<<m_excerpt_set[offset].id<<" "<<assoc_id.Value();getchar();
				}

				if(m_excerpt_set[offset].occur != occur) {
					cout<<"Weight Miss";getchar();
				}
				offset++;
			}
		}

		if(offset != m_excerpt_set.Size()) {
			cout<<"Excerpt Size Mismatch";getchar();
		}
	}
};