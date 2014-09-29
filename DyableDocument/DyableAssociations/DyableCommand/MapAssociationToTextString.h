#include "./MapAssociationsToDocument.h"

// This takes the set of word ids that make up each
// association and maps the set of text strings to
// each of the word ids. This is used when constructing
// the lexon so an association can be given assigned a
// text string.
class CMapAssociationToTextString : public CNodeStat {

	// This stores the set of association ids
	CSegFile m_assoc_id_file;
	// This stores the word text string map
	CSegFile m_word_text_map_file;
	// This stores the output of the association text string
	CSegFile m_mapped_assoc_file;

public:

	CMapAssociationToTextString() {
	}

	// This is the entry function
	void MapAssociationToTextString() {

		m_word_text_map_file.SetFileName(WORD_TEXT_STRING_FILE);
		m_mapped_assoc_file.SetFileName(ASSOC_TEXT_STRING_FILE);

		for(int i=0; i<ASSOC_SET_NUM; i++) {
			m_assoc_id_file.SetFileName(CUtility::ExtendString
				("LocalData/assoc_id_set", i + 2, ".client"));

			CMapReduce::ExternalHashMap("WriteAssocTextStringMap", m_assoc_id_file, 
				m_word_text_map_file, m_mapped_assoc_file, 
				"LocalData/assoc_text", sizeof(uLong), 40);
		}
	}
};

// This is a test framework for CMapAssociationToTextString it checks 
// that all word text strings are mapped correctly
class CTestMapAssociationToTextString : public CNodeStat {

	// This stores the word id map
	CTrie m_word_map;
	// This stores the word text map
	CArrayList<char> m_word_text;
	// This stores the word offset start
	CArrayList<int> m_word_offset;

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

public:

	CTestMapAssociationToTextString() {
	}

	// This is the entry function
	void TestMapAssociationToTextString() {

		LoadWordTextMap();
		
		uLong word_id;
		uChar length;
		CHDFSFile assoc_id_file;
		CHDFSFile m_mapped_assoc_file;
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			assoc_id_file.OpenReadFile(CUtility::ExtendString
				("LocalData/assoc_id_set", 2, ".client", i));
			m_mapped_assoc_file.OpenReadFile(CUtility::ExtendString
				(ASSOC_TEXT_STRING_FILE, i));

			while(assoc_id_file.ReadCompObject(word_id)) {
				int id = m_word_map.FindWord((char *)&word_id, sizeof(uLong));
				if(id < 0) {
					cout<<word_id;getchar();
				}

				m_mapped_assoc_file.ReadCompObject(word_id);
				m_mapped_assoc_file.ReadCompObject(length);
				m_mapped_assoc_file.ReadCompObject(CUtility::SecondTempBuffer(), length);

				int offset = m_word_offset[id];
				for(int j=0; j<length; j++) {
					if(CUtility::SecondTempBuffer()[j] != m_word_text[offset++]) {
						cout<<"Mismatch";getchar();
					}
				}
			}
		}
	}
};