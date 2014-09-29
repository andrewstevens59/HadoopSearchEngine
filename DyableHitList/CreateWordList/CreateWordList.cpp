#include "../../MapReduce.h"

// This is the directory of the keyword text strings
const char *WORD_TEXT_STRING_FILE = "GlobalData/WordDictionary/word_map";
// This stores the directory of the keyword occurrence
const char *KEYWORD_OCCURRENCE_FILE = "GlobalData/WordDictionary/occurrence_map";

// This class takes the set of excerpt and keywords generated
// in HitList and maps each term id to it's corresponding occurrence.
class CCreateWordList : public CNodeStat {

	// This stores the entire word set used by other processes
	CSegFile m_word_set_file;
	// This stores the word occurrence map
	CSegFile m_occur_map_file;
	// stores the global dictionary offset for each of the log types
	CArray<SGlobalIndexOffset> m_dict_offset;
	// This stores the lexon
	CLexon m_lexon;

	// This function stores tbe mapping between a word id and it's occurrence
	void CreateWordOccurrenceMap() {

		int length;
		CHDFSFile file;
		CMemoryChunk<uLong> word_occurr;
		CHashDictionary<int> dict;
		SWordIDOccurrMap map;

		file.OpenReadFile(CUtility::ExtendString
			("GlobalData/WordDictionary/word_occurrence", CNodeStat::GetClientID()));
		dict.ReadHashDictionaryFromFile((CUtility::ExtendString
			("GlobalData/WordDictionary/word_dictionary", CNodeStat::GetClientID())));

		word_occurr.ReadMemoryChunkFromFile(file);
		for(int j=0; j<word_occurr.OverflowSize(); j++) {
			map.word_id = m_dict_offset[CNodeStat::GetClientID()].word_offset + j;
			map.word_occurr = word_occurr[j];

			m_occur_map_file.AskBufferOverflow(sizeof(uLong) << 1);
			m_occur_map_file.WriteCompObject(map.word_id);
			m_occur_map_file.WriteCompObject(map.word_occurr);

			char *word = dict.GetWord(j, length);
			m_word_set_file.AskBufferOverflow(sizeof(uLong) + length + 1);
			m_word_set_file.WriteCompObject(map.word_id);
			m_word_set_file.WriteCompObject((uChar)length);
			m_word_set_file.WriteCompObject(word, length);
		}
	}

	// This loads the word id mapping and occurrence from the lexon]
	void LoadLexonWordOccurMap() {

		m_lexon.LoadStopWordList();
		CHashDictionary<int> &dict = m_lexon.Dictionary();
		SWordIDOccurrMap map;
		int length;

		for(int i=0; i<dict.Size(); i++) {
			map.word_id = i;
			map.word_occurr = m_lexon.WordOccurrence(i);

			m_occur_map_file.AskBufferOverflow(sizeof(uLong) << 1);
			m_occur_map_file.WriteCompObject(map.word_id);
			m_occur_map_file.WriteCompObject(map.word_occurr);

			char *word = dict.GetWord(i, length);
			m_word_set_file.AskBufferOverflow(sizeof(uLong) + length + 1);
			m_word_set_file.WriteCompObject(map.word_id);
			m_word_set_file.WriteCompObject((uChar)length);
			m_word_set_file.WriteCompObject(word, length);
		}
	}

public:

	CCreateWordList() {

		uLong word_num;
		_int64 link_num;
		CHDFSFile::Initialize();

		CHDFSFile file;
		file.OpenReadFile("GlobalData/WordDictionary/dictionary_offset");
		// writes the number of base nodes
		file.ReadCompObject(link_num);
		// writes the total number of nodes
		file.ReadCompObject(link_num);
		file.ReadCompObject(word_num);
		m_dict_offset.ReadArrayFromFile(file);
	}

	// This is the entry function 
	void CreateWordList(int client_id) {

		CNodeStat::SetClientID(client_id);
		m_occur_map_file.OpenWriteFile(CUtility::ExtendString
			(KEYWORD_OCCURRENCE_FILE, GetClientID()));

		m_word_set_file.OpenWriteFile(CUtility::ExtendString
			(WORD_TEXT_STRING_FILE, CNodeStat::GetClientID()));

		if(CNodeStat::GetClientID() == 0) {
			LoadLexonWordOccurMap();
		}

		CreateWordOccurrenceMap();
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);

	CBeacon::InitializeBeacon(client_id, 2222);
	CMemoryElement<CCreateWordList> command;
	command->CreateWordList(client_id);
	command.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();

	return 0;
}