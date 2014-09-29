#include "./ImageStorage.h"

// This class is responsible for storing all the 
// different types of log hits such as url, word,
// image hits. These are hashed into appropriate
// divisions so that they can then be later merged
// with other log files from foreign hosts.
class CLogFile {

	// Stores the log file associated with the document base url
	CMemoryChunk<CHDFSFile> m_doc_base_url_file; 
	// Stores the log file associated with the link url
	CMemoryChunk<CHDFSFile> m_link_url_file; 
	// Stores the log file associated with the non stopwords
	CMemoryChunk<CHDFSFile> m_word_file; 
	// Stores the corresponding doc set id for each base url
	CMemoryChunk<CHDFSFile> m_doc_set_id_file;
	// Stores the number of base urls in each log division
	CMemoryChunk<int> m_doc_set_num;
	// Stores the log set id
	int m_log_set_id;

	// Adds a text string to a log file.
	// @param log_file - the comp buffer to add the text string to
	// @param str - the text string buffer
	// @param length - the length of the text string in bytes
	inline void AddToLogFile(CHDFSFile &log_file, const char str[], int length) {
		log_file.AddEscapedItem(length);
		log_file.WriteCompObject(str, length); 
	}

	// Initilizes one of the log files.
	// @param log_set - the comp buffer to add the text string to
	// @param str - the name of the comp buffer
	// @param div_size - the number of divisions
	// @param log_id - the client log file set
	// @param buff_size - the size of the comp buffer
	void InitializeLogFile(CMemoryChunk<CHDFSFile> &log_set, 
		const char str[], int div_size, int log_id, int buff_size) {

		log_set.AllocateMemory(div_size); 

		for(int i=0; i<div_size; i++) {
			cout<<"Creating Set "<<i<<" Out Of "<<div_size<<endl;
			log_set[i].OpenWriteFile(CUtility::ExtendString
				(str, log_id, ".div", i));
			log_set[i].InitializeCompression(100000); 
		}
	}

	// This removes illigal characters from a url
	inline char *CleanURL(char *url, int &url_len) {

		static CMemoryChunk<char> temp_buff(1024);
		if(url_len >= temp_buff.OverflowSize()) {	
			temp_buff.AllocateMemory(url_len + 5);
		}

		int offset = 0;
		for(int i=0; i<url_len; i++) {
			if(url[i] ==' ' || url[i] == '\0') {
				continue;
			}

			temp_buff[offset++] = url[i];
		}

		url_len = offset;
		return temp_buff.Buffer();
	}

public:

	CLogFile() {
	}

	// This creates all the different log files
	// @param log_set - the client log file set
	void InitializeLogFile(int log_set) {

		m_doc_set_num.AllocateMemory(CNodeStat::GetHashDivNum(), 0);
		m_log_set_id = log_set;

		InitializeLogFile(m_word_file, "GlobalData/LogFile/word_log", 
			CNodeStat::GetHashDivNum(), log_set, 1000000); 

		InitializeLogFile(m_doc_set_id_file, "GlobalData/LogFile/doc_set_id_log", 
			CNodeStat::GetHashDivNum(), log_set, 10000); 

		InitializeLogFile(m_doc_base_url_file, "GlobalData/LogFile/base_doc_url_log", 
			CNodeStat::GetHashDivNum(), log_set, 100000); 

		InitializeLogFile(m_link_url_file, "GlobalData/LogFile/link_url_log", 
			CNodeStat::GetHashDivNum(), log_set, 400000); 
	}

	// This loads one of the log files from memory
	// @param comp - the comp buffer to add the text string to
	// @param str - the name of the comp buffer
	// @param log_set - the client log file set
	// @param log_div - the division corresponding to some
	//                - log file in a particular log set (client set)
	void LoadLogFile(const char str[], int log_div, int log_set) {

		m_word_file.AllocateMemory(1); 
		m_word_file[0].OpenReadFile(CUtility::ExtendString
			(str, log_set, ".div", log_div));
	}

	// Loads the base url index log file
	// @param log_set - the client log file set
	// @param log_div - the division corresponding to some
	//                - log file in a particular log set (client set)
	void LoadBaseURLLogFile(int log_div, int log_set) {

		m_doc_set_id_file.AllocateMemory(1);
		m_doc_set_id_file[0].OpenReadFile(CUtility::ExtendString
			("GlobalData/LogFile/doc_set_id_log", log_set, ".div", log_div)); 
	}

	// Loads the base url index log file, this
	// function is used in the hit list final indexing
	// @param log_set - the client log file set
	void LoadBaseURLLogFile(int log_set) {

		m_doc_set_id_file.AllocateMemory(CNodeStat::GetHashDivNum());

		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {

			try {
				m_doc_set_id_file[i].OpenReadFile(CUtility::ExtendString
					("GlobalData/LogFile/doc_set_id_log", log_set, ".div", i)); 

			} catch(EFileException e) {
				e.PrintException();
			}
		}
	}

	// Retrieves the number of base urls in a given division for a given
	// client log set file - this has been stored previously
	// @param log_set - the log file set belonging to a client
	// @param log_div - the division in the log file set
	// @return the number of base url in the log division
	int RetrieveBaseURLNum(int log_set, int log_div) {

		int doc_set_num;
		m_doc_set_num.ReadMemoryElementFromFile(CUtility::ExtendString
			("GlobalData/LogFile/doc_set_num", log_set),
			log_div, doc_set_num);

		return doc_set_num;
	}

	// Adds the url belonging to the spidered document
	// @param doc_set_id - the id of the document that has
	//                   - been retrieved from the document set
	// @return the hash division
	int LogBaseURL(const _int64 &doc_set_id, char url[], int length) {

		url = CleanURL(url, length);

		int div = CHashFunction::SimpleHash
			(m_doc_base_url_file.OverflowSize(), url, length); 

		AddToLogFile(m_doc_base_url_file[div], url, length); 
		// increments the number of base urls in the current log div
		m_doc_set_num[div]++;

		// adds the doc set id 
		m_doc_set_id_file[div].AddEscapedItem(doc_set_id);
		return div; 
	}

	// Adds the url that exists on a webpage
	// @return the hash division
	int LogLinkURL(char url[], int length) {

		url = CleanURL(url, length);

		int div = CHashFunction::SimpleHash
			(m_link_url_file.OverflowSize(), url, length); 

		AddToLogFile(m_link_url_file[div], url, length); 
		return div; 
	}

	// Adds the url that exists on a webpage
	// @return the hash division
	int LogWord(const char str[], int length, int start = 0) {

		str += start; 
		length -= start; 

		int div = CHashFunction::SimpleHash
			(m_word_file.OverflowSize(), str, length); 

		AddToLogFile(m_word_file[div], str, length); 
		return div; 
	}

	// Retrieves the next doc set id used for the base urls
	// @param doc_set_id - stores the retrieved doc set id
	inline void RetrieveNextDocSetID(_int64 &doc_set_id) {
		m_doc_set_id_file[0].GetEscapedItem(doc_set_id);
	}

	// Retrieves the next doc set id used for the base urls
	// @param div - the log file division for the loaded log set
	// @param doc_set_id - stores the retrieved doc set id
	inline void RetrieveNextDocSetID(int div, _int64 &doc_set_id) {
		m_doc_set_id_file[div].GetEscapedItem(doc_set_id);
	}

	// Retrieves the next text string from one of the log files
	// that is currently loaded
	// @param length - stores the length of the text string
	//        - retrieved
	// @return a ptr to the text string retrieved, NULL if no
	//     more hits available
	char *RetrieveNextLogEntry(uLong &length) {

		length = 0; 
		static CMemoryChunk<char> buff(1024);

		if(m_word_file[0].GetEscapedItem(length) < 0) {
			return NULL; 
		}

		if(length >= buff.OverflowSize()) {
			buff.AllocateMemory(length + 5);
		}

		m_word_file[0].ReadCompObject(buff.Buffer(), length);

		return buff.Buffer(); 
	}


	~CLogFile() {
		if(m_doc_set_num.OverflowSize() <= 0) {
			return;
		}

		m_doc_set_num.WriteMemoryChunkToFile(CUtility::ExtendString
			("GlobalData/LogFile/doc_set_num", m_log_set_id));
	}
}; 
