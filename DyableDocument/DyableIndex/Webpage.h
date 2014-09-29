#include "./LogFile.h"


// Defines a webpage to store attributes specific to the webpage. This 
// includes the URL of the webpage. Also this checks valid and invalid
// URL types that are to be indexed in the webgraph. Principally this
// class is responsible for creating the hit information that is used
// later by HitList to create the final hit items. Before this can 
// happen though all text strings need to be assigned unique id's. Each
// hit item is broken down into a title hit, excerpt hit, normal hit and
// image hit. This information is defined by the hypertext on the webpage.
class CWebpage {

	// stores the url for the webpage
	CArray<char> m_base_url; 
	// stores the client id
	static int m_client_id; 

	// This stores the set of domain names along with
	// their associated weight used when calculating pulse rank
	CHashDictionary<short> m_domain_name_set;
	// This stores the set of weights for a given domain
	CArrayList<uChar> m_domain_weight;
	// This stores the domain name
	CArray<char> m_domain_name;

	// stores the hit list for all the documents
	CHDFSFile m_hit_file; 
	// stores the link set for all the documents
	CHDFSFile m_link_set_file;

	// This stores the size of each title segment
	// that is parsed by this particular client
	CHDFSFile m_title_size_file;
	// This stores the meta text that makes up each title
	CHDFSFile m_meta_title_file;
	// This stores the current title being processed
	_int64 m_curr_title_id;

	// stores the start of the server in the base url
	int m_server_start;
	// This stores the local title offset
	int m_title_offset;

	// This stores an instance of the log file
	CLogFile m_log_file;
	// This stores an instance of the lexon
	CLexon m_lexon;

	// This stores the set of title anchor tags
	CHashDictionary<short> m_html_title_set;

	// Checks if the word hit is a stop word and makes a note
	// of this so an index is not retrieved for it later in the log file
	// @param word_hit - the given word hit in the word hit buffer
	// @return the term weight the reflects the stop word status
	inline void AddWordIndex(SWordHit &word_hit) {

		// indicates that the word has already been indexed
		if(word_hit.AskStopWord()) {
			// adds the global stop word index
			m_hit_file.AddEscapedItem(word_hit.word_id); 
		} else {
			// adds the log file word division
			m_hit_file.WriteCompObject(word_hit.word_div); 
		}
	}

	// This handles one of the title hits that make up a 
	// title that are used later for creating a list of 
	// statistically significant titles.
	// @param curr_word_hit - this is the current word hit being processed
	// @param title_header - this is the header of the current title being processed
	inline void ProcessTitleHit(SWordHit &curr_word_hit, STitleHeader &title_header) {

		if(curr_word_hit.AskFirstHitType()) {
			if(curr_word_hit.AskTitleHit()) {

				if(title_header.token_num > 0) {
					// flushes the title hit number from the previous title set
					title_header.WriteTitleHeader(m_title_size_file);
				}
				title_header.token_num = 0;
			}
		}
	}

	// This sets up the domain weight for a predefines set of domains
	void SetupDomainNameSet() {

		m_domain_name_set.Initialize(60, 7); 
		m_domain_weight.Initialize();

		const char *domain_name[] = {"wikipedia.com", "5", "howstuffworks.com",
			 "5", "theaustralian.com", "5", "cnn.com", "5", "//"};

		int index = 0; 
		while(!CUtility::FindFragment(domain_name[index], "//")) {
			int length = (int)strlen(domain_name[index]); 
			m_domain_name_set.AddWord(domain_name[index++], length); 
			uChar weight = atoi(domain_name[index]);
			m_domain_weight.PushBack(weight);
		} 
	}

	// This finds the domain name for this url
	void FindDomainName() {

		int offset = m_server_start;
		while(offset < m_base_url.Size()) {
			if(m_base_url[offset] == '/') {
				break;
			}

			offset++;
		}

		offset = min(offset, m_base_url.Size());
		m_domain_name.Resize(offset - m_server_start);
		memcpy(m_domain_name.Buffer(), m_base_url.Buffer() + 
			m_server_start, m_domain_name.Size());
	}

public:

	CWebpage() {
	}

	// This initializes the webpage at the very start. This includes loading
	// in the set of illegal web page type extensions.
	void InitializeWebpage() {

		m_base_url.Initialize(2048);
		m_domain_name.Initialize(2048);
		SetupDomainNameSet();
		m_curr_title_id = 0;

		m_log_file.InitializeLogFile(GetClientID()); 

		m_hit_file.OpenWriteFile(CUtility::ExtendString
			("GlobalData/HitList/meta_hit_list", GetClientID())); 

		m_title_size_file.OpenWriteFile(CUtility::ExtendString
			("GlobalData/Title/title_size", GetClientID())); 

		m_meta_title_file.OpenWriteFile(CUtility::ExtendString
			("GlobalData/Title/meta_title_text", GetClientID())); 

		m_link_set_file.OpenWriteFile(CUtility::ExtendString
			("GlobalData/HitList/meta_link_set", GetClientID())); 

		m_html_title_set.Initialize(60, 7); 
		const char *html_title[] = {"title ", "a", "b", "h1", "h2", "h3", "h4", "h5", "h6", "strong", "//"};

		int index = 0; 
		while(!CUtility::FindFragment(html_title[index], "//")) {
			int length = (int)strlen(html_title[index]); 
			m_html_title_set.AddWord(html_title[index++], length); 
		} 
	}

	// This returns true if a html tag indicates a title hit
	inline bool IsTitleHit(const char str[], int length) {
		return m_html_title_set.FindWord(str, length) >= 0;
	}

	// This returns a predicate indicating whether an 
	// outgoing link is an affiliated link or not
	// @param url - the url being compared with the base url
	// @param length - the length of the url being compared
	bool BaseServerMatch(char url[], int length) {

		length = min(length , m_base_url.Size());
		int server_match = 0;

		int offset = m_server_start;
		while(offset < length) {
			if(m_base_url[offset] == '/') {
				return true;
			}

			if(m_base_url[offset] != url[offset]) {
				break; 
			}

			offset++;
		}

		return (offset >= length) && (server_match == false);
	}

	// Sets the base url for the webpage
	// @param url - the url text string
	// @param length - the length of the url
	void SetBaseURL(char url[], int length) {

		m_title_offset = 0;
		if(CUtility::FindFragment(url, "http://")) {
			length -= 7;
			url += 7;
		}

		if(length > m_base_url.OverflowSize()) {
			m_base_url.Initialize(length);
		}

		memcpy(m_base_url.Buffer(), url, length); 
		m_base_url.Resize(length); 

		m_server_start = 0;
		while(m_base_url[m_server_start++] != '.') {
			if(m_server_start >= m_base_url.Size()) {
				m_server_start = 0;
				break;
			}
		}

		FindDomainName();
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

	// Retrieves the base url for the webpage
	// @param url - the buffer to store the url
	// @return a buffer containing the base url
	inline CArray<char> &BaseURL() {
		return m_base_url; 
	}

	// Used to set the client id
	// @param client_id - the id of the current client
	static inline void SetClientID(int client_id) {
		m_client_id = client_id; 
	}

	// Gets the client id
	// @return the client id
	inline int GetClientID() {
		return m_client_id; 
	}

	// Compiles the link set for the document. Note that each url 
	// has already been stored in the log file, but the log division
	// needs to be stored for each url along with the link set size.
	// @param link_set_div - the buffer containing all the log file 
	//           - divisions that have been added to the log file
	void CompileLinkSet(CArray<u_short> &link_set_div) {

		uChar weight = 1;
		int id = m_domain_name_set.FindWord(m_domain_name.Buffer(), m_domain_name.Size());
		if(m_domain_name_set.AskFoundWord()) {
			weight = m_domain_weight[id];
		}

		m_link_set_file.WriteCompObject(weight);
		m_link_set_file.AddEscapedItem(link_set_div.Size()); 
		
		// the base url and server division are listed
		// before all the link log divisions are listed
		for(int i=0; i<link_set_div.Size(); i++) {
			m_link_set_file.WriteCompObject(link_set_div[i]); 
		}
	}

	// This adds a meta title text to the set for later association
	inline void AddMetaTitleText(char str[], int length) { 

		_int64 title_id = m_curr_title_id << 3;
		title_id |= GetClientID();

		m_meta_title_file.WriteCompObject5Byte(title_id);
		m_meta_title_file.WriteCompObject(length);
		m_meta_title_file.WriteCompObject(str, length);
		m_curr_title_id++;
	}

	// This creates the links to other excerpts that belong to a document stub.
	// This is done by simply adding links with sequential indexes to each 
	// of the excerpts from the current document id.
	// @param doc_set_id - the id of the document that has
	//                   - been retrieved from the document set
	// @param excerpt_num - this is the number of excerpts in the current document
	void CreateExcerptLinksForStub(_int64 doc_set_id, int excerpt_num) {

		m_link_set_file.WriteCompObject((char *)&doc_set_id, 5);
		m_link_set_file.AddEscapedItem(excerpt_num); 
		doc_set_id++;

		for(int i=0; i<excerpt_num; i++) {
			m_link_set_file.WriteCompObject((char *)&doc_set_id, 5);
			doc_set_id++;
		}
	}

	// This creates the links from the stub to other excerpts for the document
	// This is done by simply adding links with sequential indexes to each 
	// of the excerpts from the current document id.
	// @param excerpt_doc_id - this is the doc id of the current excerpt
	// @param stub_doc_id - this is the doc id of the stub
	void CreateStubLinksForExcerpt(_int64 &excerpt_doc_id, _int64 &stub_doc_id) {

		m_link_set_file.WriteCompObject((char *)&excerpt_doc_id, 5);
		m_link_set_file.AddEscapedItem((int)1); 
		m_link_set_file.WriteCompObject((char *)&stub_doc_id, 5);
	}

	// Compiles the document hit list. This mostly breaks down to recording 
	// the log file division from which a text string is stored. The final 
	// word weight is also assigned and stored.	Along with this hits are classified
	// as image hits or excerpt hits or regular hits. An excerpt hit is defined by
	// a given excerpt bound between all excerpt hits lie. Image hits are also 
	// defined using a predefined bound when parsing the document.
	// @param doc_id - this is the doc id of the current document being processed
	// @param word_hit - a buffer that stores all the word hits in the document
	// @param is_excerpt_doc - true if an excerpt document is being parsed
	void CompileDocumentHitList(_int64 &doc_id, CArrayList<SWordHit> &word_hit, bool is_excerpt_doc) {

		m_hit_file.AddEscapedItem(word_hit.Size()); 
		m_hit_file.WriteCompObject((uChar)is_excerpt_doc);

		STitleHeader title_header;
		title_header.doc_id = doc_id;

		for(int i=0; i<word_hit.Size(); i++) {
			SWordHit &curr_word_hit = word_hit[i];

			if(curr_word_hit.AskStopWord()) {
				curr_word_hit.SetStopHitType();
				if(Lexon().AskExcludeWord(curr_word_hit.word_id)) {
					curr_word_hit.SetExcludeHitType();
					ProcessTitleHit(curr_word_hit, title_header);
					// adds the term weight
					m_hit_file.WriteCompObject(curr_word_hit.term_type); 
					continue;
				}
			}
			
			ProcessTitleHit(curr_word_hit, title_header);
			if(curr_word_hit.AskTitleHit()) {
				title_header.token_num++;
			}

			// adds the term weight
			m_hit_file.WriteCompObject(curr_word_hit.term_type); 
			AddWordIndex(curr_word_hit); 
		}

		if(title_header.token_num > 0) {
			title_header.WriteTitleHeader(m_title_size_file);
		}
	}

	~CWebpage() {
		m_lexon.WriteClientLexonWordOccurr(GetClientID(), 
			"GlobalData/WordDictionary/lexon");
	}	
}; 
int CWebpage::m_client_id;
