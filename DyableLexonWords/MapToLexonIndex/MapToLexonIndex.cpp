#include "../../DyableHitList/DyableLogFile/WordLogEntry.h"

// This class is responsible for mapping the existing word indexes
// to lexon indexes if they are high occurring words and exist in 
// the lexon. Note that any words that have base words will be 
// stored as there equivalent base word. Any extension words will
// have to be converted to the base form before lookup.

// Note that the first bit in a local word id signifies whether 
// it's been mapped to a lexon index or not. A 1 represents a lexon
// index and a 0 represents the local dictionary index. Also because
// new words are being placed in the lexon each of the word dictionaries 
// will have to be rebuilt with all lexon words removed. This includes
// rebuilding word occurrence also.
class CMapWordIndexes {

	// stores the clients boundary divisions
	SBoundary m_bound;
	// stores the mapping between local word
	// indexes and global lexon indexes
	CMemoryChunk<int> m_lexon_map;
	// used to store the existing word dictionary
	// for a given word log division
	CHashDictionary<int> m_curr_dict;
	// used to store the replacement dictionary
	// where all lexon words have been removed
	CHashDictionary<int> m_replace_dict;
	// used to store the current word occurrence 
	CMemoryChunk<uLong> m_curr_word_occurr;
	// used to store the replacement word occurrence
	CMemoryChunk<uLong> m_replace_occurr;

	// This is responsible for recompiling a word dictionary
	// by only keeping words that are not lexon words
	// @param dict_size - the current dictionary size for given division
	// @param queue_size - the number of high occurring words in the given division
	void RecompileWordDictionary(SDictionarySize &dict_size, int queue_size) {

		m_replace_occurr.AllocateMemory(dict_size.word_dict_size - queue_size);

		int length;
		for(int i=0; i<dict_size.word_dict_size; i++) {
			if(m_lexon_map[i] >= 0) {
				m_lexon_map[i] <<= 1;
				m_lexon_map[i] |= 0x01;
				continue;
			}

			// keeps the word if not a lexon word
			char *word = m_curr_dict.GetWord(i, length);
			int index = m_replace_dict.AddWord(word, length);

			m_replace_occurr[index] = m_curr_word_occurr[i];
			m_lexon_map[i] = index << 1;
		}

		// reassigns the new dictionary size with lexon words removed
		dict_size.word_dict_size = m_replace_dict.Size();
	}

	// This compiles each word index set in particular 
	// division by mapping local indexes to lexon indexes. 
	// This means taking an external dictionary index and 
	// rewriting to a local lexon index if it has been added to 
	// the lexon, otherwise maps it to the newly created
	// replace dictionary that has all lexon words removed.
	// @param div - the word division
	// @param set_num - the number of client word sets
	void MapWordDivision(int div, int set_num) {

		uLong word_id;
		CHDFSFile word_set_file;
		CHDFSFile replace_set_file;

		for(int i=0; i<set_num; i++) {
			cout<<"Compiling Set "<<i<<endl;
			replace_set_file.OpenWriteFile(CUtility::ExtendString
				("GlobalData/NewLogFile/new_word_log", i, ".div", div));
 
			word_set_file.OpenReadFile(CUtility::ExtendString
				("GlobalData/ProcessedLogFile/word_log", i, ".div", div));

			while(word_set_file.GetEscapedItem(word_id) >= 0) {
				uLong word_remap = m_lexon_map[(int)word_id];
				replace_set_file.AddEscapedItem(word_remap);
			}
		}
	}

	// Reads in the current dictionary size set
	// @param new_dict_size - stores the dictionary size for each division
	void ReadDictionarySizeSet(CArray<SDictionarySize> &new_dict_size) {

		int div_width;
		CHDFSFile file; 
		// first reads the current dictionary size set
		file.OpenReadFile(CUtility::ExtendString
			("GlobalData/LogFile/dictionary_size", CNodeStat::GetClientID())); 

		file.ReadObject(div_width);

		for(int j=0; j<div_width; j++) {
			new_dict_size.ExtendSize(1);
			file.ReadObject(new_dict_size.LastElement());
		}
	}

	// Writes in the current dictionary size set with modification
	// @param new_dict_size - stores the dictionary size for each division
	void WriteDictionarySizeSet(CArray<SDictionarySize> &new_dict_size) {

		CHDFSFile file; 
		// first reads the current dictionary size set
		file.OpenWriteFile(CUtility::ExtendString
			("GlobalData/LogFile/dictionary_size", CNodeStat::GetClientID())); 

		int width = m_bound.Width();
		// skips past the clients div's width
		file.WriteObject(width); 
		for(int j=0; j<m_bound.Width(); j++) {
			file.WriteObject(new_dict_size[j]);
		}

		// this is just used to store space for the net
		// offset dict offset when performing update
		// this is zero since compiling lexon for first time
		file.WriteObject(width); 
	}

	// Finds the maximum and average word occurrence for
	// all words in a given division of the log set
	// and writes the new word occurence to file
	// @param log_div - the word occcurence log division
	void WriteNewWordOccurrence(int log_div) {

		CHDFSFile file;
		file.OpenWriteFile(CUtility::ExtendString
			("GlobalData/WordDictionary/word_occurrence", log_div));

		m_replace_occurr.WriteMemoryChunkToFile(file);

		int max_occurrence = 0;
		_int64 net_occurrence = 0;
		// finds word occurrence statistics
		for(int i=0; i<m_replace_occurr.OverflowSize(); i++) {
			if(m_replace_occurr[i] > max_occurrence) {
				max_occurrence = m_replace_occurr[i];
			}

			net_occurrence += m_replace_occurr[i];
		}

		file.WriteCompObject(max_occurrence);
		file.WriteCompObject(net_occurrence);

		m_replace_dict.WriteHashDictionaryToFile(CUtility::ExtendString
			("GlobalData/NewLogFile/word_dictionary", log_div));

		if(m_replace_dict.Size() != m_replace_occurr.OverflowSize()) {
			cout<<"Size Mismatch "<<m_replace_dict.Size()<<" "<<
				m_replace_occurr.OverflowSize();getchar();
		}
	}

public:

	CMapWordIndexes() {
	}

	// This initializes the various variables 
	void Initialize(SBoundary bound, int client_id) {
		m_bound = bound;
		m_replace_dict.Initialize(0xFFFFF, 16); 
		CNodeStat::SetClientID(client_id);
		CHDFSFile::Initialize();
	}

	// This is the main entry point. Reads in existing
	// word indexes lists and writes them out again 
	// as there equivalent lexon indexes.
	// @param set_num - the number of client log sets
	void MapWordToLexonIndexes(int set_num) {

		CMemoryChunk<int> queue_size("GlobalData/Lexon/queue_size");
		CArray<SDictionarySize> new_dict_size(m_bound.Width());
		CMemoryChunk<int> div_width(set_num);
		ReadDictionarySizeSet(new_dict_size);

		CHDFSFile file;
		for(int i=m_bound.start; i<m_bound.end; i++) {

			cout<<"Compiling Division "<<i<<endl;
			m_lexon_map.ReadMemoryChunkFromFile(CUtility::ExtendString
				("GlobalData/WordDictionary/lexon_map", i)); 

			m_curr_dict.ReadHashDictionaryFromFile(CUtility::ExtendString
				("GlobalData/WordDictionary/word_dictionary", i));

			file.OpenReadFile(CUtility::ExtendString
				("GlobalData/WordDictionary/word_occurrence", i));

			CVector<uLong>::ReadVectorFromFile(file, m_curr_word_occurr);
			RecompileWordDictionary(new_dict_size[i - m_bound.start], queue_size[i]);

			MapWordDivision(i, set_num);
			WriteNewWordOccurrence(i);
			m_replace_dict.Reset();
		}

		WriteDictionarySizeSet(new_dict_size);
	}
};

int main(int argc, char *argv[]) {

	if(argc < 3)return 0;
	int client_id = atoi(argv[1]);
	int client_num = atoi(argv[2]);
	int log_div_size = atoi(argv[3]);

	CBeacon::InitializeBeacon(client_id, 2222);
	if(client_id >= log_div_size) {
		CBeacon::SendTerminationSignal();
		return 0;
	}

	SBoundary bound;
	bound.start = 0;
	bound.end = log_div_size;

	CHashFunction::BoundaryPartion(client_id, 
		client_num, bound.end, bound.start);

	CMemoryElement<CMapWordIndexes> map;
	map->Initialize(bound, client_id); 
	map->MapWordToLexonIndexes(client_num);
	map.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();
}
