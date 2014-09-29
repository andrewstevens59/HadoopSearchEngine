#include "../../../ProcessSet.h"

// This is the directory of the keyword text strings
const char *WORD_TEXT_STRING_FILE = "GlobalData/WordDictionary/word_map";
// This stores the directory of the association text
const char *ASSOC_TEXT_STRING_FILE = "GlobalData/Lexon/assoc_text_str";
// This stores the association map file
const char *ASSOC_MAP_FILE = "GlobalData/Lexon/assoc_map";

// This class creates the set of associations manually
// and then compares the output of the created assciations
class CTestCreateAssociationsText : public CNodeStat {

	// This stores the reduced set of excerpt ids
	CSegFile m_red_excerpt_map_file;
	// This stores the number of word ids that compose each document
	CHDFSFile m_red_doc_size_file;
	// This stores the word text string map
	CSegFile m_word_text_map_file;
	// This stores the final set of associations text strings along
	// with the association id
	CSegFile m_fin_assoc_file;

	// This stores the word id map
	CTrie m_word_map;
	// This stores the word text map
	CArrayList<char> m_word_text;
	// This stores the word offset start
	CArrayList<int> m_word_offset;

	// This stores the set of associations
	CTrie m_assoc_set;
	// This stores the association ids
	CArrayList<int> m_assoc_id;
	// This stores the occurrence of each association
	CArrayList<int> m_assoc_occur;

	// This loads the word text map
	void LoadWordTextMap() {

		m_word_map.Initialize(4);
		m_word_text.Initialize();
		m_word_offset.Initialize();
		m_word_offset.PushBack(0);

		uLong word_id;
		CHDFSFile word_text_file;
		word_text_file.OpenReadFile(CUtility::ExtendString
			(WORD_TEXT_STRING_FILE, (int)0));

		uChar word_length;
		while(word_text_file.ReadCompObject(word_id)) {
			m_word_map.AddWord((char *)&word_id, sizeof(uLong));
			if(m_word_map.AskFoundWord()) {
				cout<<"Word Already Exists";getchar();
			}

			word_text_file.ReadCompObject(word_length);
			word_text_file.ReadCompObject(CUtility::SecondTempBuffer(), word_length);

			m_word_text.CopyBufferToArrayList(CUtility::SecondTempBuffer(),
				word_length, m_word_text.Size());

			m_word_offset.PushBack(m_word_offset.LastElement() + word_length);
		}

	}

	// This adds one of the associations 
	void AddAssociation(CCyclicArray<uLong> &set) {

		int space = 0;
		int id = set[0];
		int length = m_word_offset[id+1] - m_word_offset[id];
		int offset = m_word_offset[id];
		for(int j=0; j<length; j++) {
			CUtility::SecondTempBuffer()[space++] = m_word_text[offset++];
		}

		CUtility::SecondTempBuffer()[space++] = '^';

		id = set[1];
		length = m_word_offset[id+1] - m_word_offset[id];
		offset = m_word_offset[id];
		for(int j=0; j<length; j++) {
			CUtility::SecondTempBuffer()[space++] = m_word_text[offset++];
		}

		CUtility::SecondTempBuffer()[space++] = '^';
		id = m_assoc_set.AddWord(CUtility::SecondTempBuffer(), space);
		if(!m_assoc_set.AskFoundWord()) {
			m_assoc_occur.PushBack(1);
			m_assoc_id.PushBack(set[0]);
			m_assoc_id.PushBack(set[1]);
		} else {
			m_assoc_occur[id]++;
		}
	}

public:

	CTestCreateAssociationsText() {
		CHDFSFile::Initialize();
		m_assoc_set.Initialize(4);
		m_assoc_occur.Initialize(4);
		m_assoc_id.Initialize();
	}
	
	// This is the entry function
	void CreateAssociations() {

		LoadWordTextMap();

		uLong doc_size;
		S5Byte doc_id;
		
		uLong word_id;
		uChar length;
		CHDFSFile assoc_id_file;
		CHDFSFile m_mapped_assoc_file;
		for(int i=0; i<CNodeStat::GetClientNum(); i++) {
			m_red_excerpt_map_file.OpenReadFile(CUtility::ExtendString
				("LocalData/excerpt_id", i));
			m_red_doc_size_file.OpenReadFile(CUtility::ExtendString
				("GlobalData/DocumentDatabase/doc_size", i));

			CCyclicArray<uLong> set(2);
			while(m_red_doc_size_file.GetEscapedItem(doc_size) >= 0) {
				m_red_doc_size_file.ReadCompObject(doc_id);
				set.Reset();

				for(uLong j=0; j<doc_size; j++) {
					m_red_excerpt_map_file.ReadCompObject(word_id);
					int id = m_word_map.FindWord((char *)&word_id, sizeof(uLong));
					set.PushBack(word_id);

					if(id < 0) {
						cout<<"Word Not Found "<<word_id;getchar();
					}

					if(set.Size() < 2) {
						continue;
					}

					if(set[0] == set[1]) {
						continue;
					}

					if(set[0] == 0 || set[1] == 0) {
						continue;
					}

					AddAssociation(set);
				}
			}
		}

		m_assoc_set.WriteTrieToFile("LocalData/assoc_set");
		m_assoc_occur.WriteArrayListToFile("LocalData/assoc_occur");
		m_assoc_id.WriteArrayListToFile("LocalData/assoc_id");

	}

	// This test the set of associations for existence
	void CheckAssociations() {

		m_assoc_set.ReadTrieFromFile("LocalData/assoc_set");
		m_assoc_occur.ReadArrayListFromFile("LocalData/assoc_occur");
		m_assoc_id.ReadArrayListFromFile("LocalData/assoc_id");

		m_fin_assoc_file.OpenReadFile(CUtility::ExtendString
			(ASSOC_MAP_FILE, (int)0));

		S5Byte word_id;
		uLong occur;
		uChar word_length;
		int num1 = 0;
		int num2 = 0;

		for(int i=0; i<m_assoc_occur.Size(); i++) {
			if(m_assoc_occur[i] >= 3) {
				num2++;
			}
		}

		while(m_fin_assoc_file.ReadCompObject(word_id)) {
			m_fin_assoc_file.ReadCompObject(occur);
			m_fin_assoc_file.ReadCompObject(word_length);
			m_fin_assoc_file.ReadCompObject(CUtility::SecondTempBuffer(), word_length);
			if(CUtility::SecondTempBuffer()[word_length-1] != '^') {
				continue;
			}

			/*if(CUtility::FindFragment(CUtility::SecondTempBuffer(), "hillary^clinton^")){
				cout<<"found rain "<<occur<<" "<<word_id.Value();getchar();
			}*/

			num1++;
			int id = m_assoc_set.FindWord(CUtility::SecondTempBuffer(), word_length);
			if(id < 0) {
				cout<<word_id.Value();getchar();
				CUtility::SecondTempBuffer()[word_length] = '\0';
				cout<<"Assoc Not Found "<<CUtility::SecondTempBuffer();getchar();
			}

			if(m_assoc_occur[id] < 2) {
				cout<<"Assoc Occur und";getchar();
			}

			if(occur != m_assoc_occur[id]) {
				cout<<"occur mismatch "<<occur<<" "<<m_assoc_occur[id];getchar();
			}
		}

		if(num1 != num2) {
			cout<<"Assoc Num Mismatch";getchar();
		}
	}
};