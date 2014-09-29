#include "./WordLogEntry.h"

// This class is responsible for assigning a unique 
// local index to all the text strings stored in 
// the log files for the given divisions supplied
// by the command line
class CIndexLogEntries : public CWordLogEntry {

	// used to store the mapping between the base url
	// local dictionary index and the global document set id
	CMemoryChunk<S5Byte> m_doc_set_map;
	// stores true if high occurring words need to be added 
	// to the lexon for stemming purposes, false otherwise
	bool m_high_priority_term;
	// used to store base dictionary sizes from previous
	// log file entries -> for updating dictionaries
	CMemoryChunk<SDictionarySize> m_dict_size;
	// used to store the net base dictionary offset from
	// all the recent base urls that have been added
	// in the current log pass (only needed for updating).
	_int64 m_net_base_dict_offset;
	// This is used to assign duplicate base urls a unique url
	int m_dup_url_id;

	// Compiles one division of a base log file. Note that
	// each base url needs to be mapped to it's respective
	// document set id, so it can be looked up correctly later
	// @param div - the log file division
	// @param log_id - the id for the log file that 
	//        - belongs to a particular processs
	//        - (ie one process compiles the log file of many
	//        - several other process log files found earlier)
	void CompileBaseLogFileDivision(int div, int log_id) {

		uLong length; 
		char *text = NULL; 

		cout<<"Base Log Set "<<log_id<<" Out Of "<<CNodeStat::GetClientNum()<<endl;

		LoadLogFile("GlobalData/LogFile/base_doc_url_log", div, log_id); 

		_int64 doc_set_id = 0;
		// just needs to map to base log entries don't
		// need to store the index since already known
		while((text = RetrieveNextLogEntry(length)) != NULL) {
			int index = m_dictionary.AddWord(text, length); 

			if(m_dictionary.AskFoundWord()) {
				text[length] = '\0';
				text = CUtility::ExtendString(text, "#", m_dup_url_id);
				index = m_dictionary.AddWord(text, strlen(text)); 
				m_dup_url_id++;
			}

			RetrieveNextDocSetID(doc_set_id);
			m_doc_set_map[index] = doc_set_id;
		}
	}

	// Compiles one division for the link log file
	// @param str - a buffer containing the name of the 
	//            - log file to load
	// @param base_url_size - the size of the url dictionary
	//                      - only including base urls
	// @param div - the log file division
	// @param log_id - the id for the log file that 
	//        - belongs to a particular processs
	//        - (ie one process compiles the log file of many
	//        - several other process log files found earlier)
	void CompileLinkLogFileDivision(int base_url_size, int div, int log_id) {

		uLong length; 
		char *text = NULL; 

		cout<<"Link Log Set "<<log_id<<" Out Of "<<CNodeStat::GetClientNum()<<endl;

		LoadLogFile("GlobalData/LogFile/link_url_log", div, log_id); 

		CHDFSFile index_file;
		index_file.OpenWriteFile(CUtility::ExtendString
			("GlobalData/ProcessedLogFile/link_url_log", log_id, ".div", div)); 
		index_file.InitializeCompression(10000);

		while((text = RetrieveNextLogEntry(length)) != NULL) {
			// remember could be encoding a base url therefore
			// always need to encode a local index as 8-bytes
			_int64 index = (_int64)m_dictionary.AddWord(text, length); 

			if(index < base_url_size) {
				index = m_doc_set_map[(int)index].Value();
				index <<= 1;
			} else {
				// stores the index as the base offset
				index -= base_url_size;
				// not a base url
				index <<= 1;
				index |= 0x01;
			}

			index_file.AddEscapedItem(index);
		}
	}

	// Indexes the link urls - the base document urls
	// have to be added to the dictionary first before
	// the link urls can be added so any link set generated
	// is ordered by the src node.
	// @param log_set_num - the number of log sets that
	//                    - have been created by several 
	//                    - processes at once
	// @param log_div - the hash log division - this is constant
	//                - for all the different log sets so that
	//                - different log set divisions can be merged with other
	//                - other log set divisions from different processes
	// @param dict_size - stores the dictionary size for all the differnt log files
	void ProcessLinkURLS(int log_set_num, int log_div, SDictionarySize &dict_size) { 


		int total_base_num = 0;
		for(int i=0; i<log_set_num; i++) {
			total_base_num += RetrieveBaseURLNum(i, log_div);
		}
		
		m_doc_set_map.Resize(total_base_num);

		for(int i=0; i<log_set_num; i++) {
			// first compiles the base urls
			try {
				LoadBaseURLLogFile(log_div, i); 
				CompileBaseLogFileDivision(log_div, i); 
			} catch(EFileException e) {
				e.PrintException();
			}

			dict_size.url_dict_size.base_url_size = m_dictionary.Size(); 
		}

		int base_url_size = m_dictionary.Size();

		for(int i=0; i<log_set_num; i++) {
			// next compiles the link urls
			try {
				CompileLinkLogFileDivision(base_url_size, log_div, i); 
			} catch(EFileException e) {
				e.PrintException();
			}
			dict_size.url_dict_size.dict_size = m_dictionary.Size();
		}

		m_dictionary.WriteHashDictionaryToFile(CUtility::ExtendString
			("GlobalData/URLDictionary/url_dictionary", log_div));

		m_doc_set_map.WriteMemoryChunkToFile(CUtility::ExtendString
			("GlobalData/URLDictionary/doc_set_id", log_div));

		m_dictionary.Reset(); 
	}

	// Finds the maximum and average word occurrence for
	// all words in a given division of the log set
	// @param file - file to store the statistics
	// @return the net word occurrence
	_int64 CalculateWordStatistics(CHDFSFile &file) {

		int max_occurrence = 0;
		_int64 net_occurrence = 0;
		// finds wrod occurrence statistics
		for(int i=0; i<m_word_occurrence.Size(); i++) {
			if(m_word_occurrence[i] > max_occurrence) {
				max_occurrence = m_word_occurrence[i];
			}

			net_occurrence += m_word_occurrence[i];
		}

		file.WriteCompObject(net_occurrence);
		file.WriteCompObject(max_occurrence);
		return net_occurrence;
	}
  
	// Creates the indexes associated with the word log file
	// @param log_set_num - the number of log sets that.
	//          - have been created by several processes at once
	// @param log_div - the current log division
	// @param dict_size - the current dictionary size file 
	void ProcessWordLogFile(int log_set_num, int log_div, SDictionarySize &dict_size) {

		for(int i=0; i<log_set_num; i++) {
			// next processes the word log
			CompileWordLogFileDivision(log_div, i); 
		}

		dict_size.word_dict_size = m_dictionary.Size(); 
		// writes the word dictionary to file
		m_dictionary.WriteHashDictionaryToFile(CUtility::ExtendString
			("GlobalData/WordDictionary/word_dictionary", log_div));

		CHDFSFile file;
		// writes the word occurrence to file
		file.OpenWriteFile(CUtility::ExtendString
			("GlobalData/WordDictionary/word_occurrence", log_div));

		m_word_occurrence.WriteVectorToFile(file); 
		_int64 net_occurr = CalculateWordStatistics(file);

		if(m_high_priority_term == true) {
			// writes the high occurring word priority queue
			// for future compilation of the lexon (if required)
			CompilePriorityWordOccurrence(log_div, net_occurr);
		}

		m_dictionary.Reset(); 
		m_word_occurrence.Resize(0);
	}

public:

	// Sets the start and end log division
	// for which this process is responsible for
	void Initialize(int client_id) {
		m_high_priority_term = false;

		CNodeStat::SetClientID(client_id);
		CHDFSFile::Initialize();
		CWordLogEntry::Initialize();
	}

	// Called if high occuring words should be found
	// and added to the lexon for stemming purposes.
	inline void ActivateHighPrioritySearchTerms() {
		m_high_priority_term = true;
	}

	// Compiles all the log entries in the given boundary
	// @param log_set_num - the number of log sets that
	//                    - have been created by several 
	//                    - processes at once
	void CompileLogEntries(int log_set_num) {

		m_net_base_dict_offset = 0; 
		SDictionarySize dict_size; 
		m_dup_url_id = 0;

		cout<<"Compiling Log Division "<<CNodeStat::GetClientID()<<endl; 
		// processes the word log files for the curr division set
		ProcessWordLogFile(log_set_num, CNodeStat::GetClientID(), dict_size); 
		cout<<"Finished Processing Words - Division "<<CNodeStat::GetClientID()<<endl;

		ProcessLinkURLS(log_set_num, CNodeStat::GetClientID(), dict_size); 
		cout<<"Finished Processing Links - Division "<<CNodeStat::GetClientID()<<endl;

		CHDFSFile file; 
		file.OpenWriteFile(CUtility::ExtendString
			("GlobalData/LogFile/dictionary_size", CNodeStat::GetClientID())); 
		file.WriteObject(dict_size);
		file.WriteObject(m_net_base_dict_offset);
	}

}; 
