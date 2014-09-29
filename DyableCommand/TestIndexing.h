#include "./TestLogFile.h"

// This class is responsible for storing all the image
// information. This relates to storing the image url
// as a text string. Note that each image url is not
// checked for redundancy. It's not likely that the
// same image with the same caption is repeated on many 
// pages. Any redundancy will be removed during lookup.
class CImageStorage {

	// stores the total number of images added
	_int64 m_image_num; 

	// indexes the current image url comp position
	void IndexCurrentImagePosition() {
	}

	// this reads a one of the text strings from the image buffer
	// @param text - a buffer to store the text string
	// @param text_length - stores the length of the text string
	void RetrieveTextString(char text[], int &text_length) {
	}

public:
	
	CImageStorage() {
	}

	// starts the file storage to start writing to
	void Initialize(int image_set) {
		m_image_num = 0; 
	}

	// returns the total number of images stored
	inline _int64 ImagesStored() {
		return m_image_num; 
	}

	// retrieves a particular image url from storage
	// @param doc_url - a buffer to store the document url
	// @param doc_length - the length of the document url
	// @param image_url - a buffer to store the image url
	// @param image_length - the length of the image url
	// @param index - the index of the image to retrieve
	// @param caption - a buffer to store the image caption
	// @param caption_length - the length of the image caption
	void RetrieveImage(char doc_url[], int &doc_length,
		char image_url[], int &image_length, const _int64 index,
		char caption[], int &caption_length) {

	}

	// adds a image url to the set
	// @param doc_url - the buffer containing the document url
	// @param doc_length - the length of the document url in bytes
	// @param image_url - the buffer containing the image url
	// @param image_length - the length of the image url in bytes
	// @param caption - the buffer containing the caption associated with the image
	// @param caption_length - the size of the caption in bytes
	void AddImageURL(char doc_url[], int doc_length,
		char image_url[], int image_length, char caption[],
		int caption_length) {

		m_image_num++;

	}

}; 

// This class is responsible for storing all the 
// different types of log hits such as url, word,
// image hits. These are hashed into appropriate
// divisions so that they can then be later merged
// with other log files from foreign hosts.
class CLogFile {

	// stores the log file associated with the document base url
	CMemoryChunk<CCompression> m_doc_base_url_comp; 
	// stores the log file associated with the link url
	CMemoryChunk<CCompression> m_link_url_comp; 
	// stores the log file associated with the non stopwords
	CMemoryChunk<CCompression> m_word_comp; 
	// stores the corresponding doc set id for each base url
	CMemoryChunk<CCompression> m_doc_set_id_comp;
	// stores the number of base urls in each log division
	CMemoryChunk<int> m_doc_set_num;
	// stores the log set id
	int m_log_set_id;

	// adds a text string to a log file
	// @param comp - the comp buffer to add the text string to
	// @param str - the text string buffer
	// @param length - the length of the text string in bytes
	inline void AddToLogFile(CCompression &comp, char str[], int length) {
	}

	// initilizes one of the log files
	// @param comp - the comp buffer to add the text string to
	// @param str - the name of the comp buffer
	// @param div_size - the number of divisions
	// @param log_set - the client log file set
	// @param buff_size - the size of the comp buffer
	inline void InitilizeLogFile(CMemoryChunk<CCompression> &comp, 
		const char str[], int div_size, int log_set, int buff_size) {

		comp.AllocateMemory(div_size); 
	}

public:

	// starts the log file
	// @param log_set - the client log file set
	void InitilizeLogFile(int log_set) {

		m_doc_set_num.AllocateMemory(CNodeStat::GetHashDivNum(), 0);
		m_log_set_id = log_set;

		InitilizeLogFile(m_doc_set_id_comp, "GlobalData/LogFile/doc_set_id_log", 
			CNodeStat::GetHashDivNum(), log_set, 10000); 

		InitilizeLogFile(m_doc_base_url_comp, "GlobalData/LogFile/base_doc_url_log", 
			CNodeStat::GetHashDivNum(), log_set, 100000); 

		InitilizeLogFile(m_link_url_comp, "GlobalData/LogFile/link_url_log", 
			CNodeStat::GetHashDivNum(), log_set, 400000); 

		InitilizeLogFile(m_word_comp, "GlobalData/LogFile/word_log", 
			CNodeStat::GetHashDivNum(), log_set, 1000000); 
	}

	// loads one of the log files
	// @param comp - the comp buffer to add the text string to
	// @param str - the name of the comp buffer
	// @param log_set - the client log file set
	// @param log_div - the division corresponding to some
	//                - log file in a particular log set (client set)
	void LoadLogFile(char str[], int log_div, int log_set) {
	}

	// loads the base url index log file
	// @param log_set - the client log file set
	// @param log_div - the division corresponding to some
	//                - log file in a particular log set (client set)
	void LoadBaseURLLogFile(int log_div, int log_set) {
	}

	// loads the base url index log file - this
	// function is used in the hit list final indexing
	// @param log_set - the client log file set
	void LoadBaseURLLogFile(int log_set) {
	}

	// retrieves the doc_set id number for a particular log divisions
	// @param doc_set_num - stores the number of base url in the log division
	// @param log_set - the log file set belonging to a client
	// @param log_div - the division in the log file set
	// @return the number of base urls in the given log division
	void RetrieveBaseURLNum(int &doc_set_num, int log_set, int log_div) {
	}

	// adds the url belonging to the spidered document
	// @param doc_set_id - the id of the document that has
	//                   - been retrieved from the document set
	// @return the hash division
	int LogBaseURL(const _int64 &doc_set_id, char url[], int length) {
		return 0;
	}

	// adds the url that exists on a webpage
	// @return the hash division
	int LogLinkURL(char url[], int length) {
		return 0; 
	}

	// adds the url that exists on a webpage
	// @return the hash division
	int LogWord(char str[], int length, int start = 0) {

		for(int i=start; i<length; i++) {
			if(str[i] == ' ' || str[i] == ' ') {
				cout<<"ill";getchar();
				for(int j=start; j<length; j++) {
					cout<<str[j];
				}
				getchar();
				return -1;
			}
		}

		return 0; 
	}

	// retrieves the next doc set id used for the base urls
	// @param doc_set_id - stores the retrieved doc set id
	inline void RetrieveNextDocSetID(_int64 &doc_set_id) {
	}

	// retrieves the next doc set id used for the base urls
	// @param div - the log file division for the loaded log set
	// @param doc_set_id - stores the retrieved doc set id
	inline void RetrieveNextDocSetID(int div, _int64 &doc_set_id) {
	}

	// retrieves the next text string from one of the log files
	// that is currently loaded
	// @param length - stores the length of the text string
	//        - retrieved
	// @return a ptr to the text string retrieved, NULL if no
	//     more hits available
	inline char *RetrieveNextLogEntry(int &length) {

		return NULL;
	}

	~CLogFile() {
	}

}; 


// Creates a webpage instance to store attributes
// specific to the webpage
class CWebpage : public CLexon, public CLogFile {
	// stores the url for the webpage
	CArray<char> m_base_url; 
	// stores the client id
	int m_client_id; 

	// stores the start of the server in the base url
	int m_server_start;
	// this defines one of the test hits
	STestHit m_test_hit;
	// This stores the current image being processed
	int m_curr_image;
	// This stores the current offset in the url buffer
	int m_url_offset;

	// This stores an instance of the log file
	CLogFile m_log_file;
	// This stores an instance of the lexon
	CLexon m_lexon;

protected:

	// stores all the image url collected
	CImageStorage m_image_storage;


	// returns the number of directory levels to which
	// a given url matches the base url
	// @param url - the url being compared with the base url
	// @param length - the length of the url being compared
	int BaseServerMatch(char url[], int length) {

		length = min(length , m_base_url.Size());
		int server_match = 0;

		int offset = m_server_start;
		while(offset < length) {
			if(m_base_url[offset] == '/')server_match++;
			if(m_base_url[offset] != url[offset])
				break; 

			offset++;
		}

		if(offset >= length && !server_match)
			server_match = 1;

		return (server_match > 3) ? 3 : server_match; 
	}

	// Checks if the word hit is a stop word and makes a note
	// of this so an index is not retrieved for it later in the log file
	// @param word_hit - the given word hit in the word hit buffer
	// @return the term weight the reflects the stop word status
	inline void AddWordIndex(SWordHit &word_hit) {
	}

	// This handles one of the title hits that make up a 
	// title that are used later for creating a list of 
	// statistically significant titles.
	// @param curr_word_hit - this is the current word hit being processed
	// @param title_num - this is the current number of title hits
	inline void ProcessTitleHit(SWordHit &curr_word_hit, uChar &title_num) {

		if(curr_word_hit.AskFirstHitType()) {

			if(curr_word_hit.AskTitleHit()) {
				if(title_num > 0) {
				}
				title_num = 0;
			}
		}

		if(curr_word_hit.AskTitleHit()) {
			title_num++;
		}
	}

public:


	CFastHashDictionary<int> m_test_word_dict;
	CFastHashDictionary<int> m_url_dict;

	// This stores all the base url indices
	CTrie m_base_url_id;
	CVector<int> word_index;
	CVector<int> word_hit_size;
	CVector<int> base_url_index;
	CVector<int> link_url_index;
	CVector<int> link_strength;
	CVector<STestHit> hit_list;
	CVector<int> cluster_size;

	void InitializeWebpage() {
		m_base_url.Initialize(1000); 
		m_test_word_dict.Initialize(9999);
		m_url_dict.Initialize(9999);
		m_base_url_id.Initialize(4);

		m_curr_image = -1;
		m_url_offset = -1;
	}

	// sets the base url for the webpage
	// @param url - the url text string
	// @param length - the length of the url
	void SetBaseURL(char url[], int length) {
		int start = 0; 
		while(*url == ' ' || *url == '\0') {
			url++;
			length--;
		}
		while(url[length-1] == ' ' || url[length-1] == '\0') 
			length--;

		CUtility::FindFragment(url, "http:// ", start); 
		memcpy(m_base_url.Buffer(), url + start, length - start); 
		m_base_url.Resize(length - start); 

		m_server_start = 0;
		while(m_base_url[m_server_start++] != '.');
	}

	// retrieves the base url for the webpage
	// @param url - the buffer to store the url
	// @return a buffer containing the base url
	inline CArray<char> &BaseURL() {
		return m_base_url; 
	}

	// used to set the client id
	// @param client_id - the id of the current client
	void SetClientId(int client_id) {
		m_client_id = client_id; 
	}

	// gets the client id
	// @return the client id
	inline int GetClientId() {
		return m_client_id; 
	}

	// This returns an instance of the lexon that is stored 
	// in webpage this is used by DocumentInstance.
	inline CLexon &Lexon() {
		return m_lexon;
	}

	// This returns an instance of the logfile that is
	// used to store text strings from the document.
	inline CLogFile &LogFile() {
		return m_log_file;
	}

	// Compiles the link set for the document. Note that each url 
	// has already been stored in the log file, but the log division
	// needs to be stored for each url along with the link set size.
	// @param link_set_div - the buffer containing all the log file 
	//           - divisions that have been added to the log file
	void CompileLinkSet(CArrayList<int> &link_set_div) {

		cluster_size.LastElement() += link_set_div.Size();
	}

	// This creates the links to other excerpts that belong to a document stub.
	// This is done by simply adding links with sequential indexes to each 
	// of the excerpts from the current document id.
	// @param doc_set_id - the id of the document that has
	//                   - been retrieved from the document set
	// @param excerpt_num - this is the number of excerpts in the current document
	void CreateExcerptLinksForStub(_int64 doc_set_id, int excerpt_num) {

		cluster_size.PushBack(excerpt_num);
		m_url_offset += excerpt_num;

		doc_set_id++;

		for(int i=0; i<excerpt_num; i++) {
			char *url = CUtility::ExtendString("ext", doc_set_id);
			link_url_index.PushBack(m_url_dict.AddWord(url, strlen(url)));
			if(m_url_dict.AskFoundWord()) {
				cout<<"excerpt url already exists";getchar();
			}
			link_strength.PushBack(0x03);
			doc_set_id++;
		}
	}

	// This creates the links from the stub to other excerpts for the document
	// This is done by simply adding links with sequential indexes to each 
	// of the excerpts from the current document id.
	// @param excerpt_doc_id - this is the doc id of the current excerpt
	// @param stub_doc_id - this is the doc id of the stub
	void CreateStubLinksForExcerpt(_int64 &excerpt_doc_id, _int64 &stub_doc_id) {
		cluster_size.PushBack(1);

		m_url_offset++;
		link_url_index.PushBack(m_url_dict.AddWord(BaseURL().Buffer(), BaseURL().Size()));
		link_strength.PushBack(0x03);
	}

	// Compiles the document hit list. This mostly breaks down to recording 
	// the log file division from which a text string is stored. The final 
	// word weight is also assigned and stored.	Along with this hits are classified
	// as image hits or excerpt hits or regular hits. An excerpt hit is defined by
	// a given excerpt bound between all excerpt hits lie. Image hits are also 
	// defined using a predefined bound when parsing the document.
	// @param word_hit - a buffer that stores all the word hits in the document
	void CompileDocumentHitList(CArrayList<SWordHit> &word_hit) {

		// stores the number of hits in the document
		word_hit_size.PushBack(word_hit.Size()); 
		uChar title_num = 0;

		for(int i=0; i<word_hit.Size(); i++) {
			SWordHit &curr_word_hit = word_hit[i];
			m_test_hit.title_segment = false;
			m_test_hit.pos = i;
			m_test_hit.exclude_word = false;
			m_test_hit.link_anchor = false;

			if(curr_word_hit.AskLinkHit()) {
				if(curr_word_hit.AskFirstHitType()) {
					m_url_offset++;
				}
				m_test_hit.link_anchor = true;
				m_test_hit.anchor_doc_id = link_url_index[m_url_offset];
			}

			if(curr_word_hit.AskStopWord()) {
				curr_word_hit.SetStopHitType();
				if(Lexon().AskExcludeWord(curr_word_hit.word_id)) {
					curr_word_hit.SetExcludeHitType();
					m_test_hit.term_weight = curr_word_hit.term_type;
					m_test_hit.exclude_word = true;
					hit_list.PushBack(m_test_hit);
					continue;
				}
			}

			ProcessTitleHit(curr_word_hit, title_num);

			m_test_hit.term_weight = curr_word_hit.term_type;

			AddWordIndex(curr_word_hit); 
			hit_list.PushBack(m_test_hit);

		}

		if(title_num > 0) {
		}
	}

	// called when all webpages have been indexed for
	// this given process
	void FinishWebpage() {

		for(int i=0; i<hit_list.Size(); i++) {

			int id = hit_list[i].anchor_doc_id;
			if(m_base_url_id.FindWord((char *)&id, 4) >= 0) {
				// anchor hits cannot be attatched to base documents
				hit_list[i].link_anchor = false;
			}
		}

		m_test_word_dict.WriteFastHashDictionaryToFile("test_word_dictionary");
		word_index.WriteVectorToFile("test_hit_index");
		base_url_index.WriteVectorToFile("test_base_url_index");
		link_url_index.WriteVectorToFile("test_link_url_index");
		hit_list.WriteVectorToFile("test_hit_list");
		cluster_size.WriteVectorToFile("test_cluster_size");
		word_hit_size.WriteVectorToFile("word_hit_size");
		link_strength.WriteVectorToFile("link_strength");
	}
}; 

// this class is responsible for tokenising the text strings
// and placing them in the log files. Some of the terms are may
// already exist in the lexon as stop words as so can be stored
// straight away as a global index. There are other utility 
// functions such as checking links are valid and indexing
// image hits in the log file. Note the lexon should be
// initilized before entering this stage.
class CDocumentInstance : public CWebpage {

	// stores a word hit in the document
	CArrayList<SWordHit> m_word_hit;
	// store the log division for each link in 
	// the document link set
	CArrayList<int> m_link_set_div;
	// This stores the number of exclude words
	int m_exclude_word_num;
	// This stores the current doc id
	_int64 doc_id;
	// This stores the number of excerpts in the current set
	int m_excerpt_num;
	// This stores all of the base urls
	CTrie m_base_url;
	// This stores the current duplicate url id
	int m_dup_url_id;


	// This adds a text string to the document that only contains 
	// alphabet characters. If the word happens to exist in the 
	// lexon then it can be assigned a word id immediately, otherwise
	// @param str - this is the buffer that contains the text passage
	// it has to be processed later in the logfile.
	// @param word_hit - the same word hit that is used to store
	//                 - in the word hit buffer
	// @param word_start - this stores the byte offset into the buffer where
	//                   - the word in the passage starts
	// @param word_end - this stores the byte offset into the buffer where
	//                 - the word in the passage ends
	inline void AddFullTextString(char str[], SWordHit &word_hit,
		int word_start, int word_end) {

		if(word_hit.word_id < 0) {
			// adds the word to the log file if not available in the lexon
			word_hit.word_div = LogFile().LogWord(str, word_end, word_start); 
		} else {
			Lexon().IncrementWordOccurr(word_hit.word_id);
			if(Lexon().AskExcludeWord(word_hit.word_id)) {
				m_exclude_word_num++;
			}
		}

		m_word_hit.PushBack(word_hit);
	}

	// This is called when a new word in the text passage is being processed.
	// Specifically it checks the word length is in normal bounds. It then 
	// checks if the word contains any non alphabet characters and handles
	// appropriately. Any words with non alphabet characters are added straight
	// to the logfile and are not checked against the lexon.
	// @param str - this is the buffer that contains the text passage
	// @param word_hit - the same word hit that is used to store
	//                 - in the word hit buffer
	// @param word_start - this stores the byte offset into the buffer where
	//                   - the word in the passage starts
	// @param word_end - this stores the byte offset into the buffer where
	//                 - the word in the passage ends
	// @param capital_set - this is a predicate specifying whether a word 
	//                    - contains a capital letter at the start
	// @param english_character_num - this specifies the number of english
	//                              - characters in the word
	// @param numeric_char_num - this specifies the number of numeric characters
	//                         - in the word
	inline void ProcessNextWord(char str[], SWordHit &word_hit,
		int word_start, int word_end, bool capital_set, 
		int english_char_num, int numeric_char_num) {

		int word_length = word_end - word_start;

		if(word_length >= 17) {
			return;
		}


		// checks if the word is a stop word
		if(english_char_num == word_length) {
			word_index.PushBack(m_test_word_dict.AddWord(str, word_end, word_start));
			word_hit.word_id = Lexon().WordIndex(str, word_end, word_start);
			AddFullTextString(str, word_hit, word_start, word_end);
			return;
		}

		if(english_char_num == 0) {
			if(numeric_char_num  <= 3 || numeric_char_num >= 6) {
				return;
			}
		}

		word_hit.word_id = -1;
		word_hit.word_div = LogFile().LogWord(str, word_end, word_start);

		word_index.PushBack(m_test_word_dict.AddWord(str, word_end, word_start));

		m_word_hit.PushBack(word_hit);
	}

	// Scans a text string embedded in the document and extracts
	// any valid words. It also checks for stopwords and indexes
	// appropriately, aswell as converting to lower case and checking
	// for capital letters at the start of the word.
	// @param str - this is the buffer that contains the text passage
	// @param word_hit - the same word hit that is used to store
	//                 - in the word hit buffer
	void ScanTextSegment(SWordHit &word_hit, 
		char str[], int length, int start = 0) {
			
		int english_char_num = 0;
		int numeric_char_num = 0;
		int word_end = 0, word_start = 0;
		bool capital_set = false;

		CTokeniser::ResetTextPassage(start);

		for(int i=start; i<length; i++) {

			if(CUtility::AskEnglishCharacter(str[i])) {
				if(CUtility::AskCapital(str[i])) {
					CUtility::ConvertToLowerCaseCharacter(str[i]);
					if(capital_set == false) {
						capital_set = true;
					} 
				}
				english_char_num++;
			} else if(CUtility::AskNumericCharacter(str[i])) {
				numeric_char_num++;
			}

			if(CTokeniser::GetNextWordFromPassage(word_end, word_start, i, str, length)) {

				ProcessNextWord(str, word_hit, word_start, word_end, 
					capital_set, english_char_num, numeric_char_num);

				capital_set = false;
				english_char_num = 0;
				numeric_char_num = 0;
			} 
		}
	}

	// This scans a text fragment and looks for html tags embedded in 
	// the text, all html tags are not index.
	// @param str - this is the buffer that contains the text passage
	// @param word_hit - the same word hit that is used to store
	//                 - in the word hit buffer
	void RemoveHTMLTag(SWordHit &word_hit, 
		char str[], int length, int start = 0) {

		bool html_tag = false; 
		for(int i=start; i<length; i++) {

			if(str[i] == '<') {
				if(html_tag == false) {
					ScanTextSegment(word_hit, str, i, start);	
				}

				start = i + 1; 
				if(start < length && str[start] != ' ') {
					html_tag = true;
				}
				continue;
			} 
			if(str[i] == '>') {
				html_tag = false;
				start = i + 1; 
			} 
		}

		if(html_tag == false) {
			ScanTextSegment(word_hit, str, length, start);	
		}
	}

	// Tokenises a string of text provided by HTMLAttribute, 
	// the lexon is consulted to check for stop words that 
	// have already been indexed and do not need to be put 
	// in the log file. All invisible words go in the log file.
	// @param term_type - this is an attribute belonging to the term type
	// @return true if hits added, false otherwise
	int AddTextToDocument(uChar term_type, char str[], int length) {

		// skips empty space
		int start = 0;
		while(!CUtility::AskOkCharacter(str[start]) && (start < length)) {
			start++;
		}

		if(start >= length) {
			return false;
		}

		static SWordHit word_hit;
		// assigns the font size and type (anchor, keyword etc)
		word_hit.term_type = term_type;

		m_exclude_word_num = 0;
		// scans the given text fragment
		int prev_size = m_word_hit.Size();
		RemoveHTMLTag(word_hit, str, length, start);

		return m_word_hit.Size() - prev_size;
	}

	// This indexes one of the attribute types
	// @param doc - this stores a pointer to the document
	void HandleMetaAttributes(char * &doc) {

		int attr_size = *(int *)doc;
		doc += 4;

		for(int i=0; i<attr_size; i++) {
			int frag_size = *(int *)doc;
			doc += 4;
			AddTextToDocument(META_TYPE_HIT, doc, frag_size);
			doc += frag_size;
		}
	}

	// This indexes one of the attribute types
	// @param doc - this stores a pointer to the document
	void HandleTitleAttributes(char * &doc) {

		int attr_size = *(int *)doc;
		doc += 4;

		for(int i=0; i<attr_size; i++) {
			int frag_size = *(int *)doc;
			doc += 4;

			if(frag_size < 65) {
				int prev_hit_size = m_word_hit.Size();
				int terms_added = AddTextToDocument(TITLE_TYPE_HIT, doc, frag_size);
				if(terms_added > 0) {
					terms_added -= m_exclude_word_num;
					m_word_hit[prev_hit_size].SetFirstHitType();

					if(terms_added > 0) {
					}
				}
			}
			doc += frag_size;
		}
	}

	// This handles all of the image attributes
	// @param doc - this stores a pointer to the document
	void HandleImageAttributes(char * &doc) {

		int attr_size = *(int *)doc;
		doc += 4;

		for(int i=0; i<attr_size; i++) {
			int url_size = *(int *)doc;
			char *url = doc + 4;
			doc += 4 + url_size;

			int text_size = *(int *)doc;
			char *text = doc + 4;

			int prev_hit_size = m_word_hit.Size();
			if(AddTextToDocument(IMAGE_TYPE_HIT, text, text_size) > 0) {
				m_word_hit[prev_hit_size].SetFirstHitType();

				// adds the image url to file storage
				m_image_storage.AddImageURL(BaseURL().Buffer(), 
					BaseURL().Size(), url, url_size, text, text_size);
			}

			doc += 4 + text_size;
		}
	}

	// This handles all of the link attributes
	// @param doc - this stores a pointer to the document
	void HandleLinkAttributes(char * &doc) {

		int attr_size = *(int *)doc;
		doc += 4;

		for(int i=0; i<attr_size; i++) {
			int url_size = *(int *)doc;
			char *url = doc + 4;
			doc += 4 + url_size;

			int text_size = *(int *)doc;
			char *text = doc + 4;

			if(m_link_set_div.Size() < 100) {

				int prev_hit_size = m_word_hit.Size();
				if(AddTextToDocument(LINK_TYPE_HIT, text, text_size) > 0) {
					m_word_hit[prev_hit_size].SetFirstHitType();

					int match_num = BaseServerMatch(url, url_size);
					int div = LogFile().LogLinkURL(url, url_size);
					div |= match_num << 6;

					int id = m_url_dict.AddWord(url, url_size);
					link_url_index.PushBack(id);

					m_link_set_div.PushBack(div);
					link_strength.PushBack(match_num);
				}
			}

			doc += 4 + text_size;
		}
	}

	// This handles all of the excerpt attributes
	// @param doc_set_id - the id of the document that has
	//                   - been retrieved from the document set
	// @param doc - this stores a pointer to the document
	void HandleExcerptAttributes(_int64 &doc_set_id, char * &doc) {

		int excerpt_num = *(int *)doc;
		doc += 4;
		int excerpt_index = *(int *)doc;
		doc += 4;
		int excerpt_length = *(int *)doc;
		doc += 4;

		m_excerpt_num--;
		char *url = CUtility::ExtendString("ext", doc_set_id);
		strcpy(CUtility::ThirdTempBuffer(), url);
		base_url_index.PushBack(m_url_dict.AddWord(CUtility::ThirdTempBuffer(), 
			strlen(CUtility::ThirdTempBuffer())));

		m_base_url_id.AddWord((char *)&base_url_index.LastElement(), 4);

		_int64 stub_doc_id = doc_set_id - excerpt_index - 1;
		AddTextToDocument(EXCERPT_TYPE_HIT, doc, excerpt_length);
		CreateStubLinksForExcerpt(doc_set_id, stub_doc_id);
	}

	// This parses all of the attributes belonging to the stub
	// @param doc - this stores a pointer to the document
	void ParseStubAttributes(char * &doc) {

		if(m_excerpt_num != 0) {
			cout<<"excerpt mismatch "<<m_excerpt_num;getchar();
		}

		if(CUtility::FindFragment(doc, "<m>", 3, 0)) {
			doc += 3;
			HandleMetaAttributes(doc);
		}

		if(CUtility::FindFragment(doc, "<l>", 3, 0)) {
			doc += 3;
			HandleLinkAttributes(doc);
		}

		if(CUtility::FindFragment(doc, "<t>", 3, 0)) {
			doc += 3;
			HandleTitleAttributes(doc);
		}

		if(CUtility::FindFragment(doc, "<i>", 3, 0)) {
			doc += 3;
			HandleImageAttributes(doc);
		}
	}

public:

	CDocumentInstance() {
		m_base_url.Initialize(4);
		m_dup_url_id = 0;
	}

	// Initilizes all the member variables
	// belonging to document instance 
	void InitializeDocumentInstance() {

		m_word_hit.Initialize(500000);
		m_link_set_div.Initialize(5000);

		InitializeWebpage();

		Lexon().LoadLexon();
		m_excerpt_num = 0;
	}

	// This is the entry function that partses a document type
	// @param doc_set_id - the id of the document that has
	//                   - been retrieved from the document set
	// @param doc - this is a pointer to the document
	void ParseDocumentType(_int64 &doc_set_id, char doc[]) {

		doc_id = doc_set_id;
		m_word_hit.Resize(0);
		m_link_set_div.Resize(0);
		BaseURL().Buffer()[BaseURL().Size()] = '\0';

		if(CUtility::FindFragment(doc, "<e>", 3, 0)) {
			doc += 3;
			HandleExcerptAttributes(doc_set_id, doc);
			return;
		}

		m_base_url.AddWord(BaseURL().Buffer(), BaseURL().Size());
		if(m_base_url.AskFoundWord()) {
			char *url = CUtility::ExtendString(BaseURL().Buffer(), "#", m_dup_url_id);
			m_dup_url_id++;

			BaseURL().Resize(strlen(url));
			strcpy(BaseURL().Buffer(), url);
			
			m_base_url.AddWord(BaseURL().Buffer(), BaseURL().Size());
			if(m_base_url.AskFoundWord()) {
				cout<<"duplicate base url";getchar();
			}
		}

		int id = m_url_dict.AddWord(BaseURL().Buffer(), BaseURL().Size());
		base_url_index.PushBack(id);

		m_base_url_id.AddWord((char *)&base_url_index.LastElement(), 4);

		doc += 3;
		int excerpt_num = *(int *)doc;	
		CreateExcerptLinksForStub(doc_set_id, *(int *)doc);
		doc += 4;

		ParseStubAttributes(doc);

		m_excerpt_num = excerpt_num;
	}


	// Called when the entire document has been processed
	// and the buffered information needs to be handled
	void FinishDocument() {

		CompileLinkSet(m_link_set_div);


		// compiles the final hit list for this document
		CompileDocumentHitList(m_word_hit);
	}
};

// This class is responsible for retrieve documents
// from the document server so that they can be indexed.
// It is also responsible for adding new resources that
// can be used later such as excerpts that extracted 
// during the index stage
class CDocumentServer : public CDocumentInstance {
	// stores the retrieved document
	CArray<char> m_document;
	// This stores the document index
	CDocumentDatabase m_doc_set;


public:

	CDocumentServer() {

		CUtility::Initialize();
		LoadLexon();
	}

	// returns the length of the document
	inline int DocumentLength() {
		return m_document.OverflowSize();
	}

	// indexes each sequential document until finished
	void IndexDocuments() {
		int document = 0;
		int client_id = 0;
		_int64 doc_set_id;

		SetClientId(client_id);

		if(m_doc_set.LoadCompiledAttrDatabase
			("GlobalData/CompiledAttributes/comp_doc", 0, 1) == false) {
				return;
		}

		InitializeDocumentInstance();
		InitilizeLogFile(0);

		CMemoryChunk<char> url_buff;
		m_document.Initialize(1000000);
		while(m_doc_set.RetrieveNextDocument(doc_set_id, m_document)) {

			int url_length = CFileStorage::ExtractURL
				(m_document.Buffer(), url_buff);

			SetBaseURL(url_buff.Buffer(), url_length);

			cout<<"Indexing Document "<<document++<<endl;
			// starts a new document instance
			ParseDocumentType(doc_set_id, 
				m_document.Buffer() + url_length + 9);

			FinishDocument();
		}
		FinishWebpage();
	}
};
