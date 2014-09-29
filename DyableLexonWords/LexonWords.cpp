#include "../MapReduce.h"

// This class is responsible for processing the word
// log and specifically counting the number of times
// a given word occurres. This is done in order to 
// weight the term and also create a list of high
// occurring words to add to the lexon so a stemming
// dictionary can be created.
class CWordLogEntry : public CLexon {

	// used to store the high occurring words in
	// a given log division
	CLimitedPQ<SWord> m_word_queue; 
	// stores the global dictionary offset for each of the log types
	CArray<SGlobalIndexOffset> m_dict_offset;
	// This stores the number of documents that have been
	// spidered (base url size)
	_int64 m_base_url_size;
	// This stores the number of added words 
	uLong m_word_num;

	// This stores the set of word weights
	CHDFSFile m_word_occur_file;

	// used to store the word occurrence for each
	// log file division
	CVector<uLong> m_word_occurrence; 
	// used to store each of the local division indexes
	CHashDictionary<int> m_dictionary; 
	// stores the start and end of the log entry divisions 
	// for which this process is responsible
	SBoundary m_div_bound; 

	// the comparison function used to sort the 
	// high occurring words
	static inline int CompareWordOccur(const SWord &arg1, const SWord &arg2) {

		if(arg1.occurrence < arg2.occurrence) {
			return -1; 
		}

		if(arg1.occurrence  > arg2.occurrence) {
			return 1; 
		}

		return 0; 
	}

	// This reads a virtual weight from file
	static bool ReadWordWeight(CHDFSFile &file, uLong &weight) {
		return file.ReadCompObject(weight);
	}

	// This writes a virtual weight to file
	static void WriteWordWeight(CHDFSFile &file, uLong &weight) {
		file.WriteCompObject(weight);
	}

	// Adds the words from the priority queue to the lexon
	// @return the number of high priority words in the queue that don't
	//         contain any non english characters
	void AddHighOccuringWords() {

		int length;
		CArray<SWord> buff(m_word_queue.Size());
		m_word_queue.CopyNodesToBuffer(buff);

		for(int i=0; i<buff.Size(); i++) {

			SWord &word = buff[i]; 
			char *str = m_dictionary.GetWord(word.index, length); 
			// only adds words that contain all english characters
			if(CUtility::AskBufferNotContainEnglishCharacter(str, length)) {
				continue;
			}

			m_word_occur_file.WriteCompObject(word.occurrence);
			uLong id = m_dict_offset.LastElement().word_offset + word.index;
			AddStemWord(id, word.occurrence, str, length); 
			m_word_num++;
		}
	}

	// This finds the cutoff word occurrence for a given percentage of terms
	// @param percentage - this is the percentage of terms that
	//                   - are kept when compiling excerpt keywords
	void FindOccurrenceThreshold(int percentage) {

		float perc = (float)percentage / 100;
		_int64 kth_stat = m_word_num * perc;

		CKthOrderStat<uLong, CHDFSFile> cutoff;
		cutoff.Initialize(9999999, "LocalData/thresh",
			ReadWordWeight, WriteWordWeight);

		uLong threshold = cutoff.FindKthOrderStat
			(m_word_occur_file, m_word_num - 1024);

		CHDFSFile occur_thresh_file;
		occur_thresh_file.OpenWriteFile("GlobalData/HitList/word_occur_thresh");
		occur_thresh_file.WriteObject(threshold);
	}

	// Calculates the dictionary offset for each of the different
	// log division. This is done so a local dictionary index
	// belonging to a particular log division can be converted
	// into a global index.
	// @param div - this stores the current log division being processed
	// @param word_dict_size - this stores the size of the word dictionary
	void CalculateDictionaryOffset(int div, int word_dict_size) {

		SDictionarySize dict_size;

		CHDFSFile file;
		file.OpenReadFile(CUtility::ExtendString
			("GlobalData/LogFile/dictionary_size", div));
		file.ReadObject(dict_size);

		m_dict_offset.ExtendSize(1);
		// stores the base url size for later lookup
		m_dict_offset.SecondLastElement().base_url_size = 
			dict_size.url_dict_size.base_url_size;
		m_base_url_size += dict_size.url_dict_size.base_url_size;

		// finds the size of the dictionary minus base urls
		int non_spidered_num = dict_size.url_dict_size.dict_size 
			- dict_size.url_dict_size.base_url_size;

		// stores the size of the dictionary with
		// urls belonging to documents that have been spidered
		m_dict_offset.LastElement().Increment
			(m_dict_offset.SecondLastElement(),
			non_spidered_num, word_dict_size);
	}

	// This creates the dictionary offset
	void WriteDictionaryOffset() {

		// places all of the non spidered documents indexes
		// after all the spidered document indexes
		for(int i=0; i<m_dict_offset.Size(); i++) {
			m_dict_offset[i].link_offset += m_base_url_size;
		}

		CHDFSFile file;
		file.OpenWriteFile("GlobalData/WordDictionary/dictionary_offset");
		// writes the number of base nodes
		file.WriteCompObject(m_base_url_size);
		// writes the total number of nodes
		file.WriteCompObject(m_dict_offset.LastElement().link_offset);
		file.WriteCompObject(m_dict_offset.LastElement().word_offset);
		m_dict_offset.WriteArrayToFile(file);
	}

public:

	CWordLogEntry() {
		CHDFSFile::Initialize();
	}

	// This sets up the word log entry
	void Initialize(int log_div_size, int client_num) {
		
		CNodeStat::SetHashDivNum(log_div_size);
		CNodeStat::SetClientNum(client_num);

		LoadStopWordList();
		InitializeStemWords(MAX_LEXON_SIZE); 
		m_word_queue.Initialize(MAX_LEXON_SIZE / log_div_size, CompareWordOccur);

		m_word_occur_file.OpenWriteFile("LocalData/word_occur");

		m_dict_offset.Initialize(CNodeStat::GetHashDivNum() + 1);
		m_dict_offset.ExtendSize(1);
		m_dict_offset.LastElement().Reset();
		m_dict_offset.LastElement().word_offset = LexonSize();
		m_base_url_size = 0;
		m_word_num = 0;
	}

	// This function adds all the high occurring words
	// found earlier and adds them to the lexon. Note
	// this function is perform in a serial fashion
	// since only one process can modify the lexon at 
	// a time (the priority queues have already been created
	void AddHighOccurrenceWordsToLexon(int percentage) {

		_int64 net_occurr;
		CHDFSFile word_queue_file;
		CompileClientWordOccurr(CNodeStat::GetClientNum());

		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			cout<<"Adding High Occurrence Words For Div "<<i<<endl;

			m_dictionary.ReadHashDictionaryFromFile(CUtility::ExtendString
				("GlobalData/WordDictionary/word_dictionary", i)); 

			word_queue_file.OpenReadFile(CUtility::ExtendString
				("GlobalData/WordDictionary/word_priority", i)); 

			m_word_queue.ReadLimitedPQFromFile(word_queue_file);

			CalculateDictionaryOffset(i, m_dictionary.Size());
			AddHighOccuringWords();
		}

		WriteDictionaryOffset();
		FindOccurrenceThreshold(percentage);
		WriteLexonToFile();
	}

}; 

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int percentage = atoi(argv[1]);
	int client_num = atoi(argv[2]);
	int log_div_size = atoi(argv[3]);

	CBeacon::InitializeBeacon(0, 2222);
	CMemoryElement<CWordLogEntry> word_dict; 
	word_dict->Initialize(log_div_size, client_num);
	word_dict->AddHighOccurrenceWordsToLexon(percentage);
	word_dict.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();

	return 0;
}