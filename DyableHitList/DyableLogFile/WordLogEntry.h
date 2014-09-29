#include "../../DyableDocument/DyableIndex/Webpage.h"

// This class is responsible for processing the word
// log and specifically counting the number of times
// a given word occurres. This is done in order to 
// weight the term and also create a list of high
// occurring words to add to the lexon so a stemming
// dictionary can be created.
class CWordLogEntry : public CLogFile {

	// Used to store a reference to a word
	// in the priority queue
	struct SWord {
		// the word index
		int index;
		// the occurrence of the word
		uLong occurrence;
	};

	// stores the maximum number of high occurrence
	// words that can be stored for any word dictionary
	int m_max_word_size; 

protected:

	// used to store the high occurring words in
	// a given log division
	CLimitedPQ<SWord> m_word_queue; 
	// used to store the word occurrence for each
	// log file division
	CVector<uLong> m_word_occurrence; 
	// used to store each of the local division indexes
	CHashDictionary<int> m_dictionary; 

	// The comparison function used to sort the high occurring words
	static int CompareWordOccur(const SWord &arg1, const SWord &arg2) {

		if(arg1.occurrence < arg2.occurrence) {
			return -1; 
		}

		if(arg1.occurrence  > arg2.occurrence) {
			return 1; 
		}

		return 0; 
	}

public:

	// This is only used to add the high occurring words 
	// to the lexon once all log entries have been indexed.
	void Initialize() {
		// stores the maximum size of word priority queue
		m_max_word_size = MAX_LEXON_SIZE / CNodeStat::GetHashDivNum(); 

		m_word_queue.Initialize(m_max_word_size, CompareWordOccur); 
		m_dictionary.Initialize(0xFFFFFF, 64); 
	}

	// Compiles the high ranking words and hands them 
	// off to the lexon to be stored.
	// @param word_div - the division corresponding to some word dictionary
	//                 - that is being compiled and added to the lexon
	// @param net_occurr - the net word occurrence in a given word division
	// @returns the size of the priority queue
	int CompilePriorityWordOccurrence(int word_div, _int64 net_occurr) {

		SWord word;
		for(int i=0; i<m_word_occurrence.Size(); i++) {
			word.index = i;
			word.occurrence = m_word_occurrence[i];
			m_word_queue.AddItem(word); 
		}

		CHDFSFile file;
		int queue_size = m_word_queue.Size();
		file.OpenWriteFile(CUtility::ExtendString
			("GlobalData/WordDictionary/word_priority", word_div)); 

		// writes the priority queue to file
		m_word_queue.WriteLimitedPQToFile(file);
		file.WriteCompObject(net_occurr);

		m_word_queue.Reset();
		return queue_size;
	}

	// Compiles one of the word log divisions - this
	// must be done separately since the word occurrence
	// of each word is counted.
	// @param div - the log file division
	// @param next_hit - a function pointer that
	//                 - retrieves the next text string
	//                 - from one of the log files
	// @param log_id - the id for the log file that 
	//               - belongs to a particular processs
	//               - (ie one process compiles the log file of many
	//               - several other process log files found earlier)
	void CompileWordLogFileDivision(int div, int log_id) {

		uLong length; 
		char *text = NULL; 
		
		cout<<"Word Log Set "<<log_id<<" Out Of "<<CNodeStat::GetClientNum()<<endl;

		LoadLogFile("GlobalData/LogFile/word_log", div, log_id); 

		CHDFSFile index_set_file;
		index_set_file.OpenWriteFile(CUtility::ExtendString
			("GlobalData/ProcessedLogFile/word_log", log_id, ".div", div)); 
		index_set_file.InitializeCompression(10000);

		while((text = RetrieveNextLogEntry(length)) != NULL) {

			int index = m_dictionary.AddWord(text, length); 
			index_set_file.AddEscapedItem(index); 

			if(!m_dictionary.AskFoundWord()) {
				m_word_occurrence.PushBack(1); 
			} else if(m_word_occurrence[index] < INFINITE) {
				m_word_occurrence[index]++; 
			}
		}
	}

}; 
