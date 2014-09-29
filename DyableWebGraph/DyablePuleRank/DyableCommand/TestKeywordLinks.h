#include "./TestPulseRank.h"

// This class tests that the keywords links produced matches the 
// the set of excerpts and the word ids that they contain
class CTestKeywordLinks : public CNodeStat {

	// This stores the set of doc ids that belong to a particular word div
	struct SWordSet {
		// This stores the doc id
		S5Byte doc_id;
		// This stores a ptr to the next word set
		SWordSet *next_ptr;
	};

	// This stores each word division
	CArrayList<SWordSet *> m_word_set;
	// This stores the set of doc ids
	CLinkedBuffer<SWordSet> m_word_set_buff;
	// This stores the word id map
	CTrie m_word_map;


	// This stores the doc id map
	CTrie m_doc_map;
	// This stores the set of doc ids that are linked to the current document
	CArrayList<S5Byte> m_doc_id_set;

	// This creates the doc id set for a given src document
	void CreateDocSet() {

		SWLink w_link;
		for(int i=0; i<m_word_set.Size(); i++) {

			m_doc_id_set.Resize(0);
			SWordSet *curr_ptr = m_word_set[i];
			while(curr_ptr != NULL) {
				m_doc_id_set.PushBack(curr_ptr->doc_id);
				curr_ptr = curr_ptr->next_ptr;
			}

			for(int j=0; j<m_doc_id_set.Size(); j++) {
				for(int k=0; k<m_doc_id_set.Size(); k++) {

					if(j == k) {
						continue;
					}

					w_link.src = m_doc_id_set[j];
					w_link.dst = m_doc_id_set[k];
					m_doc_map.AddWord((char *)&w_link, sizeof(S5Byte) << 1);
				}
			}
		}
	}
	
public:

	CTestKeywordLinks() {
		m_word_map.Initialize(4);
		m_word_set.Initialize();
		m_word_set_buff.Initialize();
		m_doc_map.Initialize(4);
		m_doc_id_set.Initialize(400);
	}

	// This creates the reverse word id mapping
	void LoadExcerpts() {

		CHDFSFile hit_file;
		SKeywordHitItem hit_item;
		for(int i=0; i<CNodeStat::GetClientNum(); i++) {
			hit_file.OpenReadFile(CUtility::ExtendString
				("LocalData/keyword_hit_file", i));

			while(hit_item.ReadKeywordHit(hit_file)) {

				int id = m_word_map.AddWord((char *)&hit_item.keyword_id, sizeof(S5Byte));
				if(!m_word_map.AskFoundWord()) {
					m_word_set.PushBack(NULL);
				}

				SWordSet *prev_ptr = m_word_set[id];
				m_word_set[id] = m_word_set_buff.ExtendSize(1);
				m_word_set[id]->doc_id = hit_item.doc_id;
				m_word_set[id]->next_ptr = prev_ptr;
			}
		}
	}

	// This checks the set of keyword links for exitence
	void CheckKeywordLinks() {

		SWLink w_link;
		CHDFSFile link_set_file;
		CreateDocSet();
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			link_set_file.OpenReadFile(CUtility::ExtendString
				("LocalData/bin_link_file", i));
			
			cout<<"Set "<<i<<endl;
			_int64 curr_src = -1;
			while(w_link.ReadWLink(link_set_file)) {

				m_doc_map.FindWord((char *)&w_link, sizeof(S5Byte) << 1);

				if(!m_doc_map.AskFoundWord()) {
					cout<<"Dst Node Not Found "<<w_link.src.Value()
						<<" "<<w_link.dst.Value();getchar();
				}
			}
		}
	}

};