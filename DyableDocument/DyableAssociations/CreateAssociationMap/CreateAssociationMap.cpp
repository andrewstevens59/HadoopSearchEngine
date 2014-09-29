#include "../../../MapReduce.h"

// This stores the directory of the association text
const char *ASSOC_TEXT_STRING_FILE = "GlobalData/Lexon/assoc_text_str";
// This stores the association map file
const char *ASSOC_MAP_FILE = "GlobalData/Lexon/assoc_map";
// This is the directory of the keyword text strings
const char *WORD_TEXT_STRING_FILE = "GlobalData/WordDictionary/word_map";
// This stores the directory of the keyword occurrence
const char *KEYWORD_OCCURRENCE_FILE = "GlobalData/WordDictionary/occurrence_map";

// This is used to create the mapping between an association id and 
// an association text string. This is used later in GlobalLexon.
class CCreateAssociationMap {

	// This stores the set of association ids and tokens that make up the association
	CHDFSFile m_lexon_assoc_id_file;
	// This stores the word text map of mapped associations
	CHDFSFile m_word_text_map_file;
	// This stores the final set of associations text strings along
	// with the association id
	CSegFile m_fin_assoc_file;
	// This stores the set of word text strings
	CHDFSFile m_word_text_file;
	// This stores the set of word occurrences
	CHDFSFile m_word_occur_file;

	// This loads in the association terms and updates the map file
	// @param map_file - this stores the mapping between a word id
	//                 - and the corresponding text string
	// @param term_num - this is the number of terms that make up the association
	// @param client_id - the current client set being processed
	void LoadAssociations(int term_num) {

		S5Byte word_id;
		uChar word_length;
		uLong occur;

		while(true) {
			char *buff = CUtility::SecondTempBuffer();

			for(int i=0; i<term_num; i++) {
				if(!m_word_text_map_file.ReadCompObject((char *)&word_id, 4)) {
					return;
				}

				m_word_text_map_file.ReadCompObject(word_length);
				m_word_text_map_file.ReadCompObject(buff, word_length);
				buff += word_length;
				*buff = '^';
				buff++;
			}

			m_lexon_assoc_id_file.ReadCompObject(word_id);
			m_lexon_assoc_id_file.ReadCompObject(occur);

			word_length = buff - CUtility::SecondTempBuffer();
			int ovf_size = sizeof(S5Byte) + sizeof(float) + 
				sizeof(uChar) + word_length;

			m_fin_assoc_file.AskBufferOverflow(ovf_size);
			m_fin_assoc_file.WriteCompObject(word_id); 
			m_fin_assoc_file.WriteCompObject(occur);
			m_fin_assoc_file.WriteCompObject(word_length);
			m_fin_assoc_file.WriteCompObject(CUtility::SecondTempBuffer(), word_length);
		}
	}

	// This appends the set of singular word ids in addition to their occurrence
	void AppendSingularTerms() {

		S5Byte word_id1 = 0;
		S5Byte word_id2 = 0;
		uChar word_length;
		uLong occur;

		m_word_text_file.OpenReadFile(CUtility::ExtendString
			(WORD_TEXT_STRING_FILE, CNodeStat::GetClientID()));
		m_word_occur_file.OpenReadFile(CUtility::ExtendString
			(KEYWORD_OCCURRENCE_FILE, CNodeStat::GetClientID()));

		char *buff = CUtility::SecondTempBuffer();
		while(m_word_occur_file.ReadCompObject((char *)&word_id1, 4)) {
			m_word_occur_file.ReadCompObject(occur);

			m_word_text_file.ReadCompObject((char *)&word_id2, 4);
			m_word_text_file.ReadCompObject(word_length);
			m_word_text_file.ReadCompObject(CUtility::SecondTempBuffer(), word_length);

			if(word_id1 != word_id2) {
				cout<<"Word Mismatch";getchar();
			}

			int ovf_size = sizeof(S5Byte) + sizeof(float) 
				+ sizeof(uChar) + word_length;

			m_fin_assoc_file.AskBufferOverflow(ovf_size);
			m_fin_assoc_file.WriteCompObject(word_id1); 
			m_fin_assoc_file.WriteCompObject(occur);
			m_fin_assoc_file.WriteCompObject(word_length);
			m_fin_assoc_file.WriteCompObject(CUtility::SecondTempBuffer(), word_length);
		}
	}

public:

	CCreateAssociationMap() {
		CHDFSFile::Initialize();
	}

	// This is the entry function
	void CreateAssociationMap(int client_id, int client_num) {

		CNodeStat::SetClientID(client_id);
		CNodeStat::SetClientNum(client_num);

		m_fin_assoc_file.OpenWriteFile(CUtility::ExtendString
			(ASSOC_MAP_FILE, client_id));

		if(CNodeStat::GetClientID() < CNodeStat::GetClientNum()) {
			AppendSingularTerms();
			return;
		} 

		m_lexon_assoc_id_file.OpenReadFile(CUtility::ExtendString
			("GlobalData/Lexon/assoc_id_file", CNodeStat::GetClientID() - CNodeStat::GetClientNum()));
		m_word_text_map_file.OpenReadFile(CUtility::ExtendString
			(ASSOC_TEXT_STRING_FILE, CNodeStat::GetClientID() - CNodeStat::GetClientNum()));

		LoadAssociations(2);
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);
	int client_num = atoi(argv[2]);

	CBeacon::InitializeBeacon(client_id, 2222);

	CMemoryElement<CCreateAssociationMap> set;
	set->CreateAssociationMap(client_id, client_num);
	set.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();
}