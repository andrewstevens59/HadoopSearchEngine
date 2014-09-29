#include "./DocumentInstance.h"

// This class is responsible for examining the HTML attribute tags on
// each of the webpages and extracting the characteristics of the 
// page such as links, images, text formating, keywords, line breaks
// and paragraphs. 
class CHTMLAttribute {

	// indicates whether the current text segment should be ignored
	bool m_ignore;
	// indicate whether the document has been flagged as no spidering
	bool m_no_spider;

	// stores the html tags used in looking up the handler functions
	CHashDictionary<short> m_html_tag;
	// stores the current image index found
	int m_curr_image_index;
	// This stores the current instance of a document
	CDocumentInstance m_doc_inst;
	// This stores a ptr ot the title of the document
	STextBound m_title;

public:

	// this initilizes the html tag dictionary by adding 
	// each term type - html tag type so that they can be
	// looked up later 
	CHTMLAttribute() {

		m_ignore = false;
		m_no_spider = false;
		m_curr_image_index = 0;

		m_html_tag.Initialize(200, 8);
		char html[][20]={"a", "img", "meta", "p", "/p", "/a", 
			"title", "/title", "li", "font", "pre", "/pre", "center",  
			"/center", "tt", "/tt", "h", "/h", "hr", "table", "/table", "br", "script", "/script", 
			"form", "/form", "dl", "/dl", "spacer", "style", "/style", 
			"td", "/td", "tr", "/tr", "div", "/div", "dd", "dt", "//"};

		int index=0;
		while(!CUtility::FindFragment(html[index], "//")) {
			int length = (int)strlen(html[index]);
			m_html_tag.AddWord(html[index++], length);
		}

	}

	// This returns a reference to the current document instance
	inline CDocumentInstance &DocInstance() {
		return m_doc_inst;
	}

	// resets the current html document instance
	void Reset() {
		m_ignore = false;
		m_no_spider = false;
		m_curr_image_index = 0;
		m_title.Initialize();
	}

	// This is responsible for calling a particular html handler
	// by looking up the html tag in the local dictionary
	void AddAttribute(char str[], int length, int start) {

		static void (CHTMLAttribute::*html_handler[])(char str[], int start, int length) = {
			&CHTMLAttribute::HandleLink, &CHTMLAttribute::HandleImage, &CHTMLAttribute::HandleMetaData, 
			&CHTMLAttribute::HandleParagraph, &CHTMLAttribute::HandleParagraph, &CHTMLAttribute::HandleLinkAnchor, 
			&CHTMLAttribute::HandleTitle, &CHTMLAttribute::HandleTitleAnchor, &CHTMLAttribute::HandleParagraph, 
			&CHTMLAttribute::HandleFontChange, &CHTMLAttribute::HandleParagraph, &CHTMLAttribute::HandleParagraph,
			&CHTMLAttribute::HandleParagraph, &CHTMLAttribute::HandleParagraph, &CHTMLAttribute::HandleParagraph, 
			&CHTMLAttribute::HandleParagraph, &CHTMLAttribute::HandleHeaderText, 
			&CHTMLAttribute::HandleHeaderTextAnchor, &CHTMLAttribute::HandleNewSection, 
			&CHTMLAttribute::HandleParagraph, &CHTMLAttribute::HandleParagraph, 
			&CHTMLAttribute::HandleParagraph, &CHTMLAttribute::HandleIgnore, &CHTMLAttribute::HandleIgnoreAnchor, 
			&CHTMLAttribute::HandleParagraph, &CHTMLAttribute::HandleParagraph, 
			&CHTMLAttribute::HandleParagraph, &CHTMLAttribute::HandleParagraph, 
			&CHTMLAttribute::HandleParagraph, &CHTMLAttribute::HandleIgnore, &CHTMLAttribute::HandleIgnoreAnchor,
			&CHTMLAttribute::HandleParagraph, &CHTMLAttribute::HandleParagraph,	
			&CHTMLAttribute::HandleParagraph, &CHTMLAttribute::HandleParagraph, &CHTMLAttribute::HandleParagraph,
			&CHTMLAttribute::HandleParagraph, &CHTMLAttribute::HandleParagraph, &CHTMLAttribute::HandleParagraph
		};

		m_doc_inst.SetHTMLTag(str + start - 1);

		int html_length = start;
		if(str[start] == '/') {
			html_length++;
		}

		while(CUtility::AskEnglishCharacter(str[html_length])) {
			if(CUtility::AskCapital(str[html_length])) {
				CUtility::ConvertToLowerCaseCharacter(str[html_length]);
			}

			if(++html_length >= length) {
				break;
			}
		}

		if((html_length - start) > 10) {
			return;
		}

		int index = m_html_tag.FindWord(str, html_length, start);
		//if the appropriate index is found
		if(m_html_tag.AskFoundWord()) {	
			//call the appropriate handler through the function pointer
			(this->*html_handler[index])(str, length, start);	
		}
	}

	// called when processing a string of text that is outside a 
	// HTML tag - processed in document instance
	inline void AddData(char str[], int length, int start) {

		if(m_ignore == true) {
			return;
		}

		m_doc_inst.AddTextToDocument(str, length, start);
	}

	// called when processing a hyperlink this is handled higher
	// up in document instance and added to the webgraph logfile
	inline void HandleLink(char str[], int length, int start) {

		CUtility::ConvertBufferToLowerCase(str + start, length - start);
		int url_start = CUtility::FindSubFragment
			(str, " href", length, start);

		if(url_start >= 0) {

			url_start += 5;
			m_doc_inst.HandleNewSection(str + start - 1);
			while(CUtility::AskOkCharacter(str[url_start]) == false && (url_start < length)) {
				url_start++;
			}

			m_doc_inst.AddLinkURL(str, length, url_start);
		}
	}

	// handles an image attribute tag - compares the image index
	// to the preprocessed image list index belonging to the 
	// webpage to see if it is a valid image to index
	inline void HandleImage(char str[], int length, int start) {

		CUtility::ConvertBufferToLowerCase(str + start, length - start);
		int text_start = CUtility::FindSubFragment
			(str, " alt", length, start);

		if(text_start < 0)return;
		text_start += 4;
		while(str[text_start] != '"' && str[text_start] != '\'') {
			if(++text_start >= length) {
				return;
			}
		}

		text_start++;
		int url_start = CUtility::FindSubFragment
			(str, " src", length, start);

		if(url_start < 0)return;
		url_start += 4;
		while(str[url_start] != '"' && str[url_start] != '\'') {
			if(++url_start >= length) {
				return;
			}
		}

		url_start++;
		m_curr_image_index++;
		m_doc_inst.HandleNewParagraph(str + length + 1);
		m_doc_inst.AddImage(str, url_start, str, text_start, length);
	}

	// handles the meta html keyword - this deals with the keywords
	// attribute, the description attribute and the robot exclusion protocol
	void HandleMetaData(char str[], int length, int start) {

		char fragment1[] = "name keywords content ";
		CUtility::ConvertBufferToLowerCase(str + start, length - start);
		int key_pos = CTokeniser::TermMatch(str, fragment1, length, start, 3);
		
		if(key_pos >= 0) {
			while(str[key_pos++] != '"' && (key_pos < length));
			int end_pos = key_pos;
			while(str[++end_pos] != '"' && (end_pos < length));
			m_doc_inst.HandleKeywords(str, end_pos, key_pos);
		}

		m_doc_inst.HandleNewSection(str + length + 1);
	}

	// returns true if the page has been marked with the spider 
	// exclusion protocol
	inline bool AskSpiderExclusion() {
		return m_no_spider;
	}

	// this handles a change in the font size
	inline void HandleFontChange(char str[], int length, int start) {

		if(CUtility::FindSubFragment(str + start, " size", length - start) < 0) {
			return;
		}

		m_doc_inst.SetFont();
		m_doc_inst.SetHTMLTag(str + start - 1);
	}

	// this handles the paragraph attribute indicated by tags like <P>
	inline void HandleParagraph(char str[], int length, int start) {
		m_doc_inst.HandleNewParagraph(str + length + 1);
	}

	// this handles any line break attribute indicated by tags like <BR>
	inline void HandleNewSection(char str[], int length, int start) {
		m_doc_inst.HandleNewSection(str + length + 1);
	}

	// this handles the html attribute corresponding to the end of 
	// a link ie </a> attribute tag triggers this
	inline void HandleLinkAnchor(char str[], int length, int start) {
		m_doc_inst.FinishLinkURL();
		m_doc_inst.SetEndHTMLTag(str + length + 1);
	}

	// the ignore handler may be used for things like tables that 
	// should not be indexed or text that is irralivent like a disclaimer
	inline void HandleIgnore(char str[], int length, int start) {
		m_ignore = true;
	}

	// this diables ignore so any text is again recognised as indexable
	// note that this attribute does not apply if the page has been flagged
	// as no spider 
	inline void HandleIgnoreAnchor(char str[], int length, int start) {
		m_ignore = false;
	}

	// this is used for header text like H1, H2, H3 etc.
	// it is used to signify important text that should be indexed
	// as title hits - increases the relative font size
	inline void HandleHeaderText(char str[], int length, int start) {

		if(CUtility::AskNumericCharacter(str[start+1])) {
			m_doc_inst.SetTitleActive();
			m_doc_inst.HandleNewSection(str + length + 1);
			m_doc_inst.SetHTMLTag(str + start - 1);
		}
	}

	// this handles the end of the header text like </H1> </H2> </H3> etc
	inline void HandleHeaderTextAnchor(char str[], int length, int start) {

		if(CUtility::AskNumericCharacter(str[start+2])) {
			m_doc_inst.SetTitleInActive();
		}
	}

	// this handles the title attribute embedded in the html tag
	// useful for finding title hits and retrieving the documents name
	// corresponds to <TITLE>
	inline void HandleTitle(char str[], int length, int start) {
		m_doc_inst.HandleNewSection(str + length + 1);
		m_doc_inst.SetTitleActive();
		m_title.start_ptr = str + length + 1;
		m_doc_inst.SetHTMLTag(str + start - 1);
	}

	// this deactivates the title tag corresponding to </TITLE>
	inline void HandleTitleAnchor(char str[], int length, int start) {

		m_doc_inst.SetTitleInActive();
		m_title.end_ptr = str + start - 1;
		m_doc_inst.SetTitle(m_title.start_ptr, m_title.end_ptr);
	}

	// destroys the current document instance
	inline void FinishDocument() {
		m_doc_inst.FinishDocument();
	}
};
