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

	// This defines the minimum number of sentences allowed to be considered an excerpt
	static const int MIN_SENTENCE_NUM = 2;
	// This defines the minimum number of characters allowed to be considered an excerpt
	static const int MIN_CHAR_NUM = 512;

	// Stores statistics relating to the current
	// document font characteristics
	struct STextStat {
		// stores the current font type encoding
		// such as anchor text, keyword text, title hits
		uChar font_type;
		// stores the current font size
		int curr_font_size;
		// stores the previous font size
		int prev_font_size;
		// stores the average font size
		int avg_font_size;
		// stores ht max and average font size
		SFont font_stat;

		void Initialize() {
			curr_font_size = 1;
			font_stat.max_font = 1;
			font_stat.avg_font = 0;
			prev_font_size = curr_font_size;
			font_type = 0;
		}

		// updates the current font states
		inline void UpdateTextStats() {
			if(curr_font_size > font_stat.max_font)
				font_stat.max_font = curr_font_size;
		}
	};

	// Statistics relating to the current 
	// excerpt such as word number, capital number, stopword number
	struct SExcerptStat {
		// stores the number of characters in the excerpt
		int char_num;
		// This stores the number of sentences in the excerpt
		int sentence_num;
		// stores the number of illegal characters
		int illegal_char_num;
		// This stores the number of links in the excerpt
		int link_num;	
		// This stores the number of characters in a paragraph
		int paragraph_char_num;	
		// This stores the number of consecutive null paragraphs
		int null_paragraph_num;	

		void Initialize() {
			char_num = 0;
			null_paragraph_num = 0;
			paragraph_char_num = 0;
			illegal_char_num = 0;
			sentence_num = 0;
			link_num = 0;
		}
	};

	// This stores the set of paragraph bounds
	CArrayList<STextBound> m_paragraph_bound;
	// Stores the word position for which an exceprt
	// begins and ends
	CArray<STextBound> m_excerpt_bound;
	// Stores the word position for which an image
	// begins and ends
	CArray<SLinkBound> m_image_bound;
	// Stores the word position for which a link
	// begins and ends
	CArray<SLinkBound> m_link_bound;
	// Stores the word position for which a link
	// begins and ends
	CArray<STextBound> m_title_bound;
	// Stores the word position for which a meta text
	// begins and ends
	CArray<STextBound> m_meta_bound;
	// This is a temporary buffer used to store modified urls
	CArray<char> m_mod_url_buff;
	// This returns the set of stop characters
	bool m_is_stop_character[256];

	// Stores text stats
	STextStat m_text;
	// Stores the excerpt stats
	SExcerptStat m_excerpt;
	// This is a flag that indicates when a title 
	// is being parsed in the document
	bool m_is_title_parse;

	// A predicate indicating when a link is being processed
	bool m_link_active;
	STextBound m_anchor_text;

	// Stores a ptr to the last occurrence of a url
	char *m_url_ptr;
	// Stores the length of the last url
	int m_url_length;
	// Stores a ptr to the end of the last text fragment
	char *m_end_text_ptr;
	// This stores the start of the current html tag being processed
	char *m_html_text_tag;

	// Scans a text string embedded in the document and extracts
	// any valid words. It also checks for stopwords and indexes
	// appropriately, aswell as converting to lower case and checking
	// for capital letters at the start of the word.
	// @param str - this is the buffer that contains the text passage
	void ScanTextSegment(char str[], int length, int start = 0) {

		bool is_capital = false;
		m_excerpt.paragraph_char_num += length - start;
		int char_num = 0;
		for(int i=start; i<length; i++) {

			if((str[i] == '.' || str[i] == ';' || str[i] == '?') && str[i+1] == ' ') {
				if(char_num > 20 && is_capital == true) { 
					m_excerpt.sentence_num++;
					is_capital = false;
				}

				char_num = 0;
			}

			char_num++;
			if(CUtility::AskEnglishCharacter(str[i])) {
				m_excerpt.char_num++;
				if(CUtility::AskCapital(str[i])) {
					is_capital = true;
				}
				continue;
			}

			if(m_is_stop_character[(uChar)(str[i])]) {
				continue;
			}

			m_excerpt.illegal_char_num++;
		}
	}

	// This adds a link stored in the temp buffer to the log
	// file if anchor text was found and is a valid url.
	void AddLinkToSet() {

		if(m_url_ptr == NULL) {
			return;
		}

		if(m_link_bound.Size() + 1 >= m_link_bound.OverflowSize()) {
			return;
		}

		// disregards capitals in url
		CUtility::ConvertBufferToLowerCase(m_url_ptr, m_url_length);

		if(CUtility::FindFragment(m_url_ptr, "http://") == false) {

			int new_size = m_url_length + BaseURL().Size() + 2;
			if(m_mod_url_buff.Size() + new_size > m_mod_url_buff.OverflowSize()) {
				return;
			}

			int prev_size = m_mod_url_buff.Size();
			m_mod_url_buff.ExtendSize(new_size);
			char *mod_url_ptr = m_mod_url_buff.Buffer() + prev_size;

			// attatches an indirect url to the base url if necessary
			COpenConnection::CompileURL(BaseURL().Buffer(), BaseURL().Size(), m_url_ptr, 
				m_url_length, m_url_length, mod_url_ptr);

			m_url_ptr = mod_url_ptr;

		} else {
			// skips past the http:// 
			m_url_length -= 7;
			m_url_ptr += 7;
		}

		m_excerpt.link_num++;
		m_url_ptr = COpenConnection::CheckURLPrefix(m_url_ptr, m_url_length);

		m_link_bound.ExtendSize(1);
		m_link_bound.LastElement().url.Set(m_url_ptr, m_url_ptr + m_url_length);
		m_link_bound.LastElement().anchor.Set(m_anchor_text.start_ptr, m_anchor_text.end_ptr);
		m_url_ptr = NULL;
	}

	// This creates a new excerpt 
	// @param paragraph_start - an index of the first paragraph
	// @param paragraph_end - an index of the last paragraph
	inline bool CreateExcerpt(int paragraph_start, int paragraph_end) {

		char *start_ptr = m_paragraph_bound[paragraph_start].start_ptr;
		char *end_ptr = m_paragraph_bound[paragraph_end].end_ptr;

		if(start_ptr == NULL) {
			return false;
		}

		if(end_ptr - start_ptr < 1600) {
			return false;
		}

		m_excerpt_bound.LastElement().Set(start_ptr, end_ptr);
		m_excerpt_bound.ExtendSize(1);

		return true;
	
	}

	// This compiles the set of paragraphs to create an excerpt
	void CompileParagraphSet() {

		if(m_excerpt.sentence_num >= MIN_SENTENCE_NUM && m_paragraph_bound.Size() > 0
			&& m_excerpt.char_num > MIN_CHAR_NUM) {

			int curr_set = 0;
			int prev_size = m_excerpt_bound.Size();
			for(int i=0; i<m_paragraph_bound.Size(); i++) {
				int width = m_paragraph_bound[i].end_ptr - 
					m_paragraph_bound[curr_set].start_ptr;

				if(width >= 3750 && (i - curr_set) > 0) {
					CreateExcerpt(curr_set, i - 1);
					curr_set = i;
				}
			}

			if(m_excerpt_bound.Size() > prev_size) {
				m_excerpt_bound.SecondLastElement().end_ptr = 
					m_paragraph_bound.LastElement().end_ptr;
			} else if(CreateExcerpt(0, m_paragraph_bound.Size() - 1) == false) {
				return;
			}
		}

		m_paragraph_bound.Resize(1);
		m_paragraph_bound.LastElement().Set(NULL, NULL);
		m_excerpt_bound.LastElement().Set(NULL, NULL);
		m_excerpt.Initialize();
	}

public:

	CDocumentInstance() {
	}

	// Initilizes all the member variables
	// belonging to document instance 
	// @param set_id - this is the set id for the current
	//               - file storage for this client
	void InitializeDocumentInstance(int set_id) {
		m_excerpt_bound.Initialize(6000);
		m_paragraph_bound.Initialize(6000);
		m_image_bound.Initialize(1000);
		m_link_bound.Initialize(5000);
		m_title_bound.Initialize(5000);
		m_meta_bound.Initialize(1000);
		m_mod_url_buff.Initialize(0xFFFFF);

		for(int i=0; i<256; i++) {
			m_is_stop_character[i] = false;
		}

		m_is_stop_character[(uChar)(',')] = true;
		m_is_stop_character[(uChar)(' ')] = true;
		m_is_stop_character[(uChar)('.')] = true;
		m_is_stop_character[(uChar)('\'')] = true;

		InitializeWebpage(set_id);
	}

	// Starts a new document alerting the log file and webgraph
	bool CreateNewDocument() {

		m_is_title_parse = false;
		m_url_ptr = NULL;
		m_html_text_tag = NULL;
		m_link_active = false;

		m_text.Initialize();
		m_excerpt.Initialize();

		m_image_bound.Resize(0);
		m_link_bound.Resize(0);
		m_excerpt_bound.Resize(0);
		m_title_bound.Resize(0);
		m_meta_bound.Resize(0);
		m_mod_url_buff.Resize(0);
		m_paragraph_bound.Resize(0);

		// starts the first excerpt bound
		m_excerpt_bound.ExtendSize(1);
		m_excerpt_bound.LastElement().Set(NULL, NULL);

		m_paragraph_bound.ExtendSize(1);
		m_paragraph_bound.LastElement().Set(NULL, NULL);

		return true;
	}

	// This sets a ptr to the current html tag being processed
	// @param html_tag_ptr - this stores a ptr to the html 
	//                     - being processed
	inline void SetHTMLTag(char *html_tag_ptr) {

		if(m_html_text_tag != NULL) {
			return;
		}

		m_html_text_tag = html_tag_ptr;
	}

	// This indicates that a title is being parsed
	inline void SetTitleActive() {
		m_is_title_parse = true;
		HandleNewParagraph();
	}

	// This indicates that a title has finished being parsed
	inline void SetTitleInActive() {
		m_is_title_parse = false;
	}

	// Adjusts the current font size by some factor
	// @param factor - the factor for which to multiply
	//        - the current font size
	void RelativeFont(float factor) {
		if(factor > 10.0f)factor = 10.0f;

		m_text.prev_font_size = m_text.curr_font_size;
		int font_size = (int)(m_text.curr_font_size * factor);
		if(font_size <= 0)font_size = 1;
		if(font_size > 30)font_size = 30;

		m_text.curr_font_size = font_size;
		m_text.UpdateTextStats();
	}

	// Adds a link to the document instance - first checks
	// it's a valid url before adding it to the collection
	void AddLinkURL(char url[], int length, int start = 0) {

		// finishes any previous link
		if(m_text.font_type & LINK_TYPE_HIT) {
			FinishLinkURL();	
		}

		int url_length = start;
		// first checks the length of the url
		while(url[url_length] != '\'' && url[url_length] != '"' && url_length < length) {
			if(AskIllegalURLCharacter(url[url_length++])) {
				return;
			}
		}

		if((url_length - start) > 4096)return;
		if((url_length - start) < 3)return;

		if(CheckLinkType(url, url_length, start)) {
			m_url_length = url_length - start;
			m_url_ptr = url + start;
			m_link_active = true;

			m_anchor_text.start_ptr = url + length + 1;
			m_anchor_text.end_ptr = url + length + 1;
		}
	}

	// Adds an image to the document if it has
	// been deemed to be of appropriate dimension
	// an there is caption information available for indexing
	// @param url - the url of the image
	// @param url_length - the length of the url in bytes
	// @param image_text - a ptr to the image caption
	// @param text_length - the length of the caption in bytes
	// @param segment_size - the total size of html tag size in bytes
	void AddImage(char url[], int url_start, char image_text[], 
		int text_start, int segment_size) {

		m_anchor_text.end_ptr = m_anchor_text.start_ptr;
		if(m_image_bound.Size() + 2 >= m_image_bound.OverflowSize()) {
			return;
		}

		int url_length = url_start;
		while((url[url_length] != '"' && url[url_length] != '\'') && (url_length < segment_size)) {
			url_length++;
		}

		if((url_length - url_start) > 4096)return;
		if((url_length - url_start) < 3)return;

		int text_length = text_start;
		while((image_text[text_length] != '"' && image_text[text_length] != '\'') && (text_length < segment_size)) {
			text_length++;
		}

		// checks the number of english characters in the caption
		int english_char = CUtility::CountEnglishCharacters
			(image_text + text_start, text_length - text_start);

		if(english_char < 5)return;

		m_image_bound.ExtendSize(1);
		m_image_bound.LastElement().url.Set
			(url + url_start, url + url_length);
		m_image_bound.LastElement().anchor.Set
			(image_text + text_start, image_text + text_length);
	}

	// Finishes processing the current link url when 
	// the </A> link anchor is encountered
	void FinishLinkURL() {

		AddLinkToSet();
		m_link_active = false;	
		m_url_ptr = NULL;
	}

	// Called when a new paragraph handler is encountered such as <P> or <BR>
	// this is so a exerpt word can be indexed appropriately.
	// @param end_ptr - ptr to the last character in the end paragraph tag
	void HandleNewParagraph(char *end_ptr = NULL) {

		if(m_excerpt.illegal_char_num >= 20) {
			m_excerpt.illegal_char_num = 0;
			return;
		}

		STextBound &paragraph = m_paragraph_bound.LastElement();

		if(paragraph.start_ptr == NULL) {
			paragraph.start_ptr = m_html_text_tag;
			if(paragraph.start_ptr == NULL) {
				return;
			}
		}

		if(end_ptr != NULL) {
			m_end_text_ptr = end_ptr;
		}

		paragraph.end_ptr = m_end_text_ptr;
		m_html_text_tag = NULL;

		m_excerpt.paragraph_char_num = 0;
		if(m_excerpt.paragraph_char_num < 10) {
			if(++m_excerpt.null_paragraph_num > 20) {
				CompileParagraphSet();
				m_paragraph_bound.Resize(0);
			}
		} else {
			m_excerpt.null_paragraph_num = 0;
		}

		m_paragraph_bound.ExtendSize(1);
		m_paragraph_bound.LastElement().Set(NULL, NULL);
	}

	// This handles a new section in the text which is broken
	// irrespective of the current number of sentences parsed
	// @param end_ptr - ptr to the last character in the end paragraph tag
	void HandleNewSection(char *end_ptr = NULL) {

		if(m_link_active == true) {
			return;	
		}

		HandleNewParagraph(end_ptr);

		m_paragraph_bound.PopBack();
		CompileParagraphSet();
	}

	// Sets the current font size explicitly 
	void SetFont() {

		if(m_paragraph_bound.LastElement().start_ptr == NULL) {
			// parsed a title
			HandleNewSection();
		}
	}

	// This sets the ptr to the last character in an paragraph
	// @param end_ptr - this is the last character in the paragraph
	inline void SetEndHTMLTag(char *end_ptr) {
		m_end_text_ptr = end_ptr;
	}

	// Tokenises a string of text provided by HTMLAttribute, 
	// the lexon is consulted to check for stop words that 
	// have already been indexed and do not need to be put 
	// in the log file. All invisible words go in the log file.
	void AddTextToDocument(char str[], int length, int start = 0) {

		while(CUtility::AskOkCharacter(str[start]) && (start < length) == false) {
			start++;
		}

		if(start >= length) {
			return;
		}

		ScanTextSegment(str, length, start);
		m_end_text_ptr = str + length;
		m_anchor_text.end_ptr = str + length;

		if(m_paragraph_bound.LastElement().start_ptr == NULL) {
			m_paragraph_bound.LastElement().start_ptr = m_html_text_tag;
		}
	}

	// Adds higher priority words in the meta tag such as
	// webpage keywords and description handlers
	void HandleKeywords(char str[], int length, int start = 0) {

		if((length - start) > 100000) {
			return;
		}

		if(m_meta_bound.Size() < m_meta_bound.OverflowSize()) {
			m_meta_bound.ExtendSize(1);
			m_meta_bound.LastElement().Set(str + start, str + length);
		}
	}

	// Called when the entire document has been processed
	// and the buffered information needs to be handled
	bool FinishDocument() {

		FinishLinkURL();

		HandleNewSection();
		m_excerpt_bound.PopBack();

		// compiles the final hit list for this document
		return ProcessDocument(m_excerpt_bound, m_image_bound, 
			m_link_bound, m_title_bound, m_meta_bound);
	}
};
