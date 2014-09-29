#include "./FindAssociationOccurrence.h"

// This class maps the association ids to the set set of 
// association terms that are presen in the document. This
// is so each association can be uniquely identified.
class CMapAssociationsToDocument : public CNodeStat {

	// This stores each of the association sets for all words
	CSegFile m_excerpt_assoc_set_file;
	// This stores the mapping between associations and their id
	CSegFile m_assoc_map_file;
	// This stores the set of mapped associations 
	CSegFile m_mapped_assoc_set_file;

public:

	CMapAssociationsToDocument() {
		CHDFSFile::Initialize();
	}

	// THis is the entry function
	void MapAssociationsToDocument() {

		m_assoc_map_file.SetFileName("LocalData/perm_assoc_file");
		m_excerpt_assoc_set_file.SetFileName("LocalData/excerpt_assoc_file0.client");
		m_mapped_assoc_set_file.SetFileName("LocalData/mapped_assoc_file0.client");

		CMapReduce::ExternalHashMap("MapAssociationsToDocument", m_excerpt_assoc_set_file, 
			m_assoc_map_file, m_mapped_assoc_set_file, "LocalData/map_assoc",
			sizeof(uLong) << 1, sizeof(S5Byte) + sizeof(uLong));
	}
};

// This class is used to test that associations are correctly mapped to every
// term in the document. This includes the association weight and the association id.
class CTestMapAssociationsToDocument : public CNodeStat {

	// This stores the association id and weight
	struct SAssoc {
		_int64 id;
		uLong occur;
	};

	// The stores the association map
	CTrie m_assoc_map;
	// This stores the set of association stats
	CArrayList<SAssoc> m_assoc_set;
	// This stores the set of excerpt associations
	CArrayList<SAssoc> m_excerpt_set;

	// This stores the mapping between associations and their id
	CSegFile m_assoc_map_file;
	// This stores each of the association sets for all words
	CSegFile m_excerpt_assoc_set_file;
	// This stores the set of mapped associations 
	CSegFile m_mapped_assoc_set_file;

	// This loads the association set'
	void LoadAssocSet(int hash_div_num) {

		for(int j=0; j<hash_div_num; j++) {

			CMemoryChunk<uLong> term_id(2);
			m_assoc_map_file.OpenReadFile(CUtility::ExtendString
				("LocalData/perm_assoc_file", j));

			S5Byte assoc_id;
			uLong occur;
			while(m_assoc_map_file.ReadCompObject(term_id.Buffer(), 2)) {
				m_assoc_map_file.ReadCompObject(assoc_id);
				m_assoc_map_file.ReadCompObject(occur);

				int id = m_assoc_map.AddWord((char *)term_id.Buffer(), sizeof(uLong) << 1);
				if(!m_assoc_map.AskFoundWord()) {
					m_assoc_set.ExtendSize(1);
					m_assoc_set.LastElement().id = assoc_id.Value();
					m_assoc_set.LastElement().occur = occur;
				}
			}
		}
	}

	// This creates the set of excerpt associations
	void CreateExcerptAssociations() {

		S5Byte assoc_id;
		float weight;
		for(int j=0; j<CNodeStat::GetClientNum(); j++) {
			for(int i=0; i<ASSOC_SET_NUM; i++) {

				CMemoryChunk<uLong> buff1(i + 2);
				CMemoryChunk<uLong> buff2(i + 2);
				m_excerpt_assoc_set_file.OpenReadFile(CUtility::ExtendString
					("LocalData/excerpt_assoc_file", i, ".client", j));

				uChar map_bytes;
				while(m_excerpt_assoc_set_file.ReadCompObject(buff1.Buffer(), i + 2)) {
					int id = m_assoc_map.FindWord((char *)buff1.Buffer(), sizeof(uLong) * (i + 2));
					m_excerpt_set.ExtendSize(1);
					m_excerpt_set.LastElement().occur = INFINITE;
					if(id < 0)continue;
					m_excerpt_set.LastElement() = m_assoc_set[id];
				}
			}
		}
	}

public:

	CTestMapAssociationsToDocument() {
		m_assoc_map.Initialize(4);
		m_assoc_set.Initialize(4);
		m_excerpt_set.Initialize(4);
	}

	// This is the entry function
	void TestMapAssociationsToDocument(int client_id, int client_num, int hash_div_num) {

		LoadAssocSet(hash_div_num);
		CreateExcerptAssociations();

		S5Byte assoc_id;
		uLong occur;
		int offset = 0;
		for(int j=0; j<CNodeStat::GetClientNum(); j++) {
			for(int i=0; i<ASSOC_SET_NUM; i++) {

				CMemoryChunk<uLong> buff1(i + 2);
				CMemoryChunk<uLong> buff2(i + 2);
				m_mapped_assoc_set_file.OpenReadFile(CUtility::ExtendString
					("LocalData/mapped_assoc_file", i, ".client", j));

				uChar map_bytes;
				while(m_mapped_assoc_set_file.ReadCompObject(buff1.Buffer(), buff1.OverflowSize())) {
					m_mapped_assoc_set_file.ReadCompObject(map_bytes);
					if(map_bytes == 0) {
						offset++;
						continue;
					}
	
					m_mapped_assoc_set_file.ReadCompObject(assoc_id);
					m_mapped_assoc_set_file.ReadCompObject(occur);

					if(m_excerpt_set[offset].id != assoc_id.Value()) {
						cout<<"Assoc ID Miss "<<m_excerpt_set[offset].id<<" "<<assoc_id.Value();getchar();
					}

					if(m_excerpt_set[offset].occur != occur) {
						cout<<"Weight Miss";getchar();
					}
					offset++;
				}
			}
		}

		if(offset != m_excerpt_set.Size()) {
			cout<<"Excerpt Size Mismatch";getchar();
		}

	}
};