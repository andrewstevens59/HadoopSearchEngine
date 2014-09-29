#include "./Webpage.h"

// This class is responsible for tokenising the text strings
// and placing them in the log files. Some of the terms are may
// already exist in the lexon as stop words as so can be stored
// straight away as a global index. Hit item bounds are used for
// excerpt and image hits. These bounds are used when assembling
// the final hit list in Webpage. Another feature of this class
// is it assigns word weights to each of the hit items. This is
// done by processing the HTML text and looking for HTML tags.
// The other important aspect is the processing and storing of 
// links embedded in the HTML text. Hits embedded in anchor text
// are given higher priority. All urls are stored in a log file.

// There are other utility functions such as checking links are 
// valid and indexing image hits in the log file. Note the lexon 
// should be initilized before entering this stage.
class CDocumentInstance : public CWebpage {

	// stores a word hit in the document
	CArrayList<SWordHit> m_word_hit;
	// store the log division for each link in 
	// the document link set
	CArray<u_short> m_link_set_div;
	// stores all the image url collected
	CImageStorage m_image_storage;
	// This is used to find words with illegal suffixes
	CStemWord m_stem;

	// This stores the number of terms excluding title hits
	int m_normal_term_num;
	// This stores the number of stop words
	int m_stop_word_num;
	// This stores the current document id
	_int64 m_doc_id;
	// This is a predicate indicating an excerpt document
	bool m_is_excerpt_doc;

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

		uChar length = word_end - word_start;

		if(CStemWord::GetStem(str + word_start, length) >= 3) {
			word_hit.term_type |= SUFFIX_TYPE_HIT;
		}

		if(word_hit.word_id < 0) {
			// adds the word to the log file if not available in the lexon
			word_hit.word_div = LogFile().LogWord(str, word_end, word_start); 
		} else {
			Lexon().IncrementWordOccurr(word_hit.word_id);
			if(Lexon().AskStopWord(word_hit.word_id)) {
				m_stop_word_num++;
			} 
		}

		m_word_hit.PushBack(word_hit);
		word_hit.term_type &= ~SUFFIX_TYPE_HIT;
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
	// @param english_character_num - this specifies the number of english
	//                              - characters in the word
	// @param numeric_char_num - this specifies the number of numeric characters
	//                         - in the word
	inline void ProcessNextWord(char str[], SWordHit &word_hit,
		int word_start, int word_end, int english_char_num, int numeric_char_num) {

		int word_length = word_end - word_start;

		if(word_length >= 17 || word_length < 2) {
			return;
		}

		// checks if the word is a stop word
		if(english_char_num == word_length) {
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

		m_word_hit.PushBack(word_hit);
	}

	// Scans a text string embedded in the document and extracts
	// any valid words. It also checks for stopwords and indexes
	// appropriately, aswell as converting to lower case and checking
	// for capital letters at the start of the word.
	// @param str - this is the buffer that contains the text passage
	// @param word_hit - the same word hit that is used to store
	//                 - in the word hit buffer
	void ScanTextSegment(uChar term_type, char str[], int length, int start = 0) {
			
		int english_char_num = 0;
		int numeric_char_num = 0;
		int word_end = 0, word_start = 0;

		static SWordHit word_hit;
		word_hit.term_type = term_type;
		CTokeniser::ResetTextPassage(start);
		int prev_size = m_word_hit.Size();

		for(int i=start; i<length; i++) {

			if(CUtility::AskEnglishCharacter(str[i])) {
				if(CUtility::AskCapital(str[i])) {
					CUtility::ConvertToLowerCaseCharacter(str[i]);
				}
				english_char_num++;
			} else if(CUtility::AskNumericCharacter(str[i])) {
				numeric_char_num++;
			}

			if(CTokeniser::GetNextWordFromPassage(word_end, word_start, i, str, length)) {
				ProcessNextWord(str, word_hit, word_start, word_end, 
					english_char_num, numeric_char_num);

				english_char_num = 0;
				numeric_char_num = 0;
			} 
		}

		if(term_type != TITLE_TYPE_HIT) {
			m_normal_term_num += m_word_hit.Size() - prev_size;
		}
	}

	// This scans a text fragment and looks for html tags embedded in 
	// the text, all html tags are not index.
	// @param str - this is the buffer that contains the text passage
	// @param word_hit - the same word hit that is used to store
	//                 - in the word hit buffer
	void RemoveHTMLTag(uChar term_type, char str[], int length, int start = 0) {

		int html_length;
		bool html_tag = false; 
		bool is_title_hit = false;
		for(int i=start; i<length; i++) {
			if(str[i] == '<') {

				if(html_tag == false) {
					if(is_title_hit == true) {
						ScanTextSegment(TITLE_TYPE_HIT, str, i, start);	
					} else {
						ScanTextSegment(term_type, str, i, start);	
					}
				}

				html_length = i;
				while(CUtility::AskOkCharacter(str[++html_length]));
				is_title_hit = IsTitleHit(str + i + 1, html_length - i - 1);

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
			if(is_title_hit == true) {
				ScanTextSegment(TITLE_TYPE_HIT, str, length, start);	
			} else {
				ScanTextSegment(term_type, str, length, start);	
			}
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

		// scans the given text fragment
		int prev_size = m_word_hit.Size();
		RemoveHTMLTag(term_type, str, length, start);

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
			//AddTextToDocument(TITLE_TYPE_HIT, doc, frag_size);
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
				/*if(terms_added > 0) {
					terms_added -= m_exclude_word_num;
					m_word_hit[prev_hit_size].SetFirstHitType();

					if(terms_added > 0) {
						CWebpage::AddMetaTitleText(doc, frag_size);
					}
				}*/
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

			if(m_link_set_div.Size() < m_link_set_div.OverflowSize()) {
				int prev_hit_size = m_word_hit.Size();
				if(AddTextToDocument(LINK_TYPE_HIT, text, text_size) > 0) {
					m_word_hit[prev_hit_size].SetFirstHitType();

					int div = LogFile().LogLinkURL(url, url_size);
					if(BaseServerMatch(url, url_size)) {
						div |= 0x8000;
					}

					m_link_set_div.PushBack(div);
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
		u_short title_length = *(u_short *)doc;

		if(title_length > 2) {
			AddTextToDocument(TITLE_TYPE_HIT, doc + 2, title_length);
		}

		doc += title_length + 2;

		// adds the base url for the given document
		char *url = CUtility::ExtendString("ext", doc_set_id);
		LogFile().LogBaseURL(doc_set_id, url, strlen(url));

		_int64 stub_doc_id = doc_set_id - excerpt_index - 1;
		AddTextToDocument(EXCERPT_TYPE_HIT, doc, excerpt_length);

		CreateStubLinksForExcerpt(doc_set_id, stub_doc_id);
	}

	// This parses all of the attributes belonging to the stub
	// @param doc - this stores a pointer to the document
	// @param doc_end_ptr - this is a ptr to the last character
	void ParseStubAttributes(char * &doc_start_ptr, char *doc_end_ptr) {

		if(doc_start_ptr >= doc_end_ptr) {
			return;
		}

		if(CUtility::FindFragment(doc_start_ptr, "<m>", 3, 0)) {
			doc_start_ptr += 3;
			HandleMetaAttributes(doc_start_ptr);
		}

		if(doc_start_ptr >= doc_end_ptr) {
			return;
		}

		if(CUtility::FindFragment(doc_start_ptr, "<l>", 3, 0)) {
			doc_start_ptr += 3;
			HandleLinkAttributes(doc_start_ptr);
		}

		if(doc_start_ptr >= doc_end_ptr) {
			return;
		}

		if(CUtility::FindFragment(doc_start_ptr, "<t>", 3, 0)) {
			doc_start_ptr += 3;
			HandleTitleAttributes(doc_start_ptr);
		}

		if(doc_start_ptr >= doc_end_ptr) {
			return;
		}

		if(CUtility::FindFragment(doc_start_ptr, "<i>", 3, 0)) {
			doc_start_ptr += 3;
			HandleImageAttributes(doc_start_ptr);
		}
	}

public:

	CDocumentInstance() {
	}

	// Initilizes all the member variables
	// belonging to document instance 
	void InitializeDocumentInstance() {

		m_word_hit.Initialize(500000);
		m_link_set_div.Initialize(100);
		m_image_storage.Initialize(GetClientID());

		InitializeWebpage();

		Lexon().LoadLexon();
		CStemWord::Initialize("GlobalData/Lists/IllegalSuffixes"); 	
	}

	// This is the entry function that partses a document type
	// @param doc_set_id - the id of the document that has
	//                   - been retrieved from the document set
	// @param doc - this is a pointer to the document
	// @param 
	void ParseDocumentType(_int64 &doc_set_id, char doc[], int doc_length) {

		m_word_hit.Resize(0);
		m_link_set_div.Resize(0);
		m_doc_id = doc_set_id;
		m_normal_term_num = 0;
		m_stop_word_num = 0;

		if(CUtility::FindFragment(doc, "<e>", 3, 0)) {
			doc += 3;
			m_is_excerpt_doc = true;
			HandleExcerptAttributes(doc_set_id, doc);
			return;
		}

		m_is_excerpt_doc = false;
		char *doc_end_ptr = doc + doc_length;
		// adds the base url for the given document
		LogFile().LogBaseURL(doc_set_id, 
			BaseURL().Buffer(), BaseURL().Size());
		
		doc += 3;
		CreateExcerptLinksForStub(doc_set_id, *(int *)doc);
		doc += 4;
		
		if(doc >= doc_end_ptr) {
			return;
		}

		ParseStubAttributes(doc, doc_end_ptr);
	}

	// Called when the entire document has been processed
	// and the buffered information needs to be handled
	void FinishDocument() {

		CompileLinkSet(m_link_set_div);

		if(m_stop_word_num < 8 || m_normal_term_num < 225) {
			m_is_excerpt_doc = false;
		}

		// compiles the final hit list for this document
		CompileDocumentHitList(m_doc_id, m_word_hit, m_is_excerpt_doc);
	}
};
