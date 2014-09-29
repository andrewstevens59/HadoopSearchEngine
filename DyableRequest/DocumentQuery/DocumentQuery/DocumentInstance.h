#include "./CompileSummary.h"

// This stores an instance of a retrieved document. It is spawned by 
// a number of parallel threads to retrieve a specifc docuemnt. It
// also parses the document and highlight keywords.
class CDocumentInstance {

	// This stores the capital predicate and length of each word
	struct SWordType {
		// This stores the length of the word
		uChar word_len;
		// This is a predicate indicating a capital word
		bool is_capital;
	};

	// This stores the highlighted text passage
	CArrayList<char> m_keyword_pass;
	// This stores the html tag dictionary
	CHashDictionary<short> m_html_tag;
	// This stores the active html tags
	CMemoryChunk<bool> m_html_tag_set;
	// This stores each word type
	CArrayList<SWordType> m_word_type_buff;

	// This is true when text is being ignored
	bool m_ignore_text;
	// This stores the number of continous breaks
	int m_line_break_num;
	// This marks the end of a paragraph
	bool m_is_paragraph_end;
	// This marks the end of a sentence
	bool m_is_sentence_end;
	// This stores the number of query terms
	int m_query_term_num;
	// This stores the number of keyword terms
	int m_keyword_term_num;
	// This indicates a header text is being parsed
	bool m_is_header_text;

	// This is used to compile the excerpt summary
	CCompileSummary m_summary;
	// This stores the highlighted terms
	CTrie m_keyword_highlight;
	// This stores a predicate indicating whether
	// a keyword should be displayed all in capitals or not
	CArrayList<char> m_is_keyword_capt;

	// This stores the set of colors
	const char *m_highlight_color[10];
	// This stores the url
	CString m_url;
	// This stores the domain name
	CString m_domain_name;
	// This stores the ptr to the title
	char *m_title_ptr;

	// This is looks for a term match for a given query term
	// @param capital_num - this is the number of capital letters
	// @return the word index of the term if a priority term, -1 otherwise
	int AskPriorityTerm(char text[], int length, int start, int capital_num) {

		StemWord(text, length, start);
		int word_length = length - start;
		int term_match = -1;
		static int dict_length;

		if(word_length > 3) {
			int closest_match = 0;
			term_match = m_keyword_highlight.FindWord
				(closest_match, text, length, start);

			if(term_match < 0 && closest_match >= 0) {
	
				char *word = m_keyword_highlight.GetWord(closest_match, dict_length);
				if(CUtility::FindFragment(text + start, word, 6, 0)) {
					term_match = closest_match;
				} else if(CUtility::FindFragment(text + start, word, 5, 0) && word_length < 6) {
					term_match = closest_match;
				}
			}

		} else {
			term_match = m_keyword_highlight.FindWord(text, length, start);
		}


		if(term_match >= 0 && term_match < m_keyword_term_num) {
			return term_match;
		}

		if(word_length == capital_num && word_length >= 3) {
			int id = m_keyword_highlight.AddWord(text, length, start);
			if(m_keyword_highlight.AskFoundWord() == false) {
				m_is_keyword_capt.PushBack(0);
			}

			return id;
		}


		if(capital_num > 0 && word_length >= 4 && m_summary.IsStopWord(text, start, length) == false) {
			int id = m_keyword_highlight.AddWord(text, length, start);
			if(m_keyword_highlight.AskFoundWord() == false) {
				m_is_keyword_capt.PushBack(0);
			}

			return id;
		}

		return -1;
	}

	// This handles a given word in the passage
	// @param keyword_id - this is the id of the current keyword being processed
	// @param capital_num - this is the number of capital letters in the word
	void ProcessWord(char text[], int word_start, int word_end, int keyword_id, int capital_num) {

		static int word_start_offset;
		static int word_end_offset;
		bool is_paragraph_start = false;

		if(keyword_id >= 0) {
			if(capital_num == (word_end - word_start)) {
				m_is_keyword_capt[keyword_id]++;
			} else {
				m_is_keyword_capt[keyword_id]--;
			}
		}

		word_start_offset = m_keyword_pass.Size();

		if(keyword_id >= 0 && keyword_id < m_query_term_num) {
			m_keyword_pass.CopyBufferToArrayList("<FONT COLOR=\"red\"><B>");
		}

		bool is_sentence_start = CUtility::AskCapital(text[word_start]) && m_is_sentence_end == true;
		if(is_sentence_start == true) {
			m_is_sentence_end = false;
		}

		if(m_is_paragraph_end == true) {
			m_is_paragraph_end = false;
			is_paragraph_start = true;
		}

		m_keyword_pass.CopyBufferToArrayList(text + word_start, 
			word_end - word_start, m_keyword_pass.Size());

		if(keyword_id >= 0 && keyword_id < m_query_term_num) {
			m_keyword_pass.CopyBufferToArrayList("</B></FONT>");
		}

		word_end_offset = m_keyword_pass.Size();

		m_is_sentence_end = (text[word_end] == '.' || text[word_end] == '?') && text[word_end + 1] == ' ';

		if(text[word_start-1] == '(') {
			m_summary.SetComma();
		}

		if(text[word_start-1] == '&' || text[word_start-1] == '#') {
			return;
		}

		m_summary.AddTerm(word_start_offset, word_end_offset, keyword_id,
			is_sentence_start, m_is_sentence_end, is_paragraph_start, 
			m_is_paragraph_end, text[word_end] == ',' || text[word_end] == '"' || 
			text[word_end] == ')' || text[word_end] == ';' || text[word_end] == ':'
			|| text[word_end] == '-', capital_num, word_end - word_start);

	}

	// This checks to see if a particular character is a sentence end
	bool IsSentenceEnd(char text[], int word_pos) {

		bool delim = text[word_pos] == '.' || text[word_pos] == '!' || text[word_pos] == '?';

		return (delim == true) && (CUtility::AskOkCharacter(text[word_pos+1]) == true);
	}

	// This is responsible for printing a string of text 
	// with highlighting and exclusion of carriage 
	// returns, line feeds etc
	// @param text - ptr to the text segment
	void PrintHighlightedPassage(char text[], int text_length, bool is_bold = false) {

		static CArrayList<int> capital_pos(30);
		capital_pos.Resize(0);
		int word_end = 0;
		int word_start = 0;

		CTokeniser::ResetTextPassage(0);
		for(int i=0; i<text_length; i++) {

			if(CUtility::AskCapital(text[i])) {
				CUtility::ConvertToLowerCaseCharacter(text[i]);
				capital_pos.PushBack(i);
			}

			if(CTokeniser::GetNextWordFromPassage(word_end, word_start, i, text, text_length)) {

				if(IsSentenceEnd(text, word_end) == true) {
					continue;
				}

				int keyword_id = AskPriorityTerm(text, word_end, word_start, capital_pos.Size());
				if(is_bold == true && keyword_id >= m_query_term_num) {
					keyword_id = -1;
				}

				m_line_break_num = 0;
				if(capital_pos.Size() > 0) {

					for(int j=0; j<capital_pos.Size(); j++) {
						CUtility::ConvertToUpperCaseCharacter(text[capital_pos[j]]);
					}
				}

				if(is_bold == false) {
					ProcessWord(text, word_start, word_end, keyword_id, capital_pos.Size());
				} else {
					if(keyword_id >= 0) {
						cout<<"<b>";
					}

					for(int j=word_start; j<word_end; j++) {
						cout<<text[j];
					}

					if(keyword_id >= 0) {
						cout<<"</b>";
					}
				}

				capital_pos.Resize(0);
			} 

			if(is_bold == false) {
				if(text[i] == 25) {
					m_keyword_pass.CopyBufferToArrayList("&quot;");
				} else if(text[i] > 0 && CUtility::AskOkCharacter(text[i]) == false) {
					m_keyword_pass.PushBack(text[i]);
				}
			} else if(CUtility::AskOkCharacter(text[i]) == false) {
				cout<<text[i];
			}
		}
	}

	// Called to indicate that an anchor is being parsed
	void ParseAnchor(char str[], int length) {
		PrinteRelativeLink(str, length, "href");
	}

	// Called to print arbitrary text
	void PrintText(char str[], int length) {
		m_keyword_pass.CopyBufferToArrayList(str, 
			length, m_keyword_pass.Size());
	}

	// This replaces a text following a relative link
	void PrinteRelativeLink(char str[], int length, const char tag[]) {

		int pos = CUtility::FindSubFragment(str, tag, length);
		if(pos < 0) {
			m_keyword_pass.CopyBufferToArrayList(str, 
				length, m_keyword_pass.Size());
			return;
		}

		pos += strlen(tag);
		m_keyword_pass.CopyBufferToArrayList(str, 
			pos, m_keyword_pass.Size());

		int offset = pos;
		while(CUtility::AskOkCharacter(str[pos]) == false && str[pos] != '\'' && str[pos++] != '"') {
			if(pos >= length) {
				break;
			}
		}

		int tag_length = pos;
		while(str[pos] != '\'' && str[tag_length] != '"' && str[tag_length] != ' ' && str[tag_length] != '>') {
			if(++tag_length >= length) {
				break;
			}
		}

		if(tag_length > length) {
			m_keyword_pass.CopyBufferToArrayList(str, 
				length, m_keyword_pass.Size());
			return;
		}

		m_keyword_pass.CopyBufferToArrayList(str + offset, 
			pos - offset, m_keyword_pass.Size());

		if(CUtility::FindFragment(str + pos, "http://")) {
			m_keyword_pass.CopyBufferToArrayList(str + pos, tag_length - pos);
			m_keyword_pass.CopyBufferToArrayList(str + tag_length, length - tag_length);
			return;
		}
		
		m_keyword_pass.CopyBufferToArrayList("http://");

		m_keyword_pass.CopyBufferToArrayList(m_domain_name.Buffer(), 
			m_domain_name.Size(), m_keyword_pass.Size());

		if(str[pos] != '/') {
			m_keyword_pass.CopyBufferToArrayList("/");
		}

		m_keyword_pass.CopyBufferToArrayList(str + pos, 
			length - pos, m_keyword_pass.Size());
	}

	// This checks for the src tag for images
	void PrintImage(char str[], int length) {
		PrinteRelativeLink(str, length, "src");
	}

	// This prints the paragraph symbol
	void PrintParagraph(char str[], int length) {

		if(m_is_header_text == true) {
			m_keyword_pass.CopyBufferToArrayList("</h2>");
			m_is_header_text = false;
		}

		m_summary.Reset();
		m_keyword_pass.CopyBufferToArrayList("<p>");
		m_summary.SetSentenceEnd();
		m_summary.SetParagraphEnd();
		m_is_paragraph_end = true;
		m_is_sentence_end = true;
	}

	// This prints the paragraph anchor symbol
	void PrintParagraphAnchor(char str[], int length) {

		if(m_is_header_text == true) {
			m_keyword_pass.CopyBufferToArrayList("</h2>");
			m_is_header_text = false;
		}

		m_summary.Reset();
		m_keyword_pass.CopyBufferToArrayList("</p>");
		m_summary.SetSentenceEnd();
		m_summary.SetParagraphEnd();
		m_is_paragraph_end = true;
		m_is_sentence_end = true;
	}

	// Called to print header text
	void PrintHeaderText(char str[], int length) {
		/*if(!CUtility::AskNumericCharacter(str[2])) {
			return;
		}

		m_summary.Reset();
		m_keyword_pass.CopyBufferToArrayList("<h2>");
		m_is_header_text = true;*/
	}

	// Called to print anchor header text
	void PrintAnchorHeaderText(char str[], int length) {
		/*if(!CUtility::AskNumericCharacter(str[3])) {
			return;
		}

		m_summary.Reset();
		m_keyword_pass.CopyBufferToArrayList("</h2>");
		m_is_header_text = false;*/
	}

	// This ignores sections of text
	void HandleIgnore(char str[], int length) {
		m_ignore_text = true;
	}

	// This ignores sections of text
	void HandleIgnoreAnchor(char str[], int length) {
		m_ignore_text = false;
	}

	// This handles a specific line break
	void HandleLineBreak(char str[], int length) {

		m_summary.Reset();
		m_is_sentence_end = true;
		m_summary.SetSentenceEnd();
		if(++m_line_break_num > 2) {
			return;
		}

		if(m_is_header_text == true) {
			m_keyword_pass.CopyBufferToArrayList("</h2>");
			m_is_header_text = false;
		}

		m_keyword_pass.CopyBufferToArrayList("<br>");
		m_summary.SetParagraphEnd();
		m_is_paragraph_end = true;
	}

	// This processes a html attribute that has been parsed in the text 
	// @param text - ptr to the text segment
	void ParseHTMLAttribute(char text[], int text_length) {

		static void (CDocumentInstance::*html_handler[])(char str[], int length) = {
			&CDocumentInstance::ParseAnchor, &CDocumentInstance::ParseAnchor, 
			&CDocumentInstance::HandleLineBreak, &CDocumentInstance::PrintHeaderText,
			&CDocumentInstance::PrintAnchorHeaderText, &CDocumentInstance::PrintParagraph, 
			&CDocumentInstance::PrintParagraphAnchor, &CDocumentInstance::PrintText, 
			&CDocumentInstance::PrintText, &CDocumentInstance::PrintText,
			&CDocumentInstance::PrintText, &CDocumentInstance::PrintText,
			&CDocumentInstance::PrintText, &CDocumentInstance::HandleIgnore,
			&CDocumentInstance::HandleIgnoreAnchor, &CDocumentInstance::PrintParagraph, 
			&CDocumentInstance::PrintParagraphAnchor
		};

		int html_length = 0;
		if(text[0] == '/') {
			html_length++;
		}

		while(CUtility::AskEnglishCharacter(text[html_length])) {
			if(CUtility::AskCapital(text[html_length])) {
				CUtility::ConvertToLowerCaseCharacter(text[html_length]);
			}

			if(++html_length >= text_length) {
				break;
			}
		}

		int index = m_html_tag.FindWord(text, html_length);
		// if the appropriate index is found
		if(m_html_tag.AskFoundWord()) {
			// call the appropriate handler through the function pointer
			(this->*html_handler[index])(text - 1, text_length + 2);	
		}
	}

	// This breaks the text document up into individual html tag 
	// segments and text string segments. It then feeds each 
	// segment into HTMLAttribute for further processing.
	// @param offset - the offset in the document buffer
	void CompileWebDocument(char text[], int text_length) {

		bool html_tag = false; 
		int start = 0;

		for(int i=0; i<text_length; i++) {
			if(text[i] == '<') {
				if(html_tag == false && m_ignore_text == false) {
					m_keyword_pass.PushBack(' ');
					PrintHighlightedPassage(text + start, i - start);
				}

				start = i + 1; 
				if(start < text_length && text[start] != ' ') {
					html_tag = true;
				}
				continue;
			} 
			if(text[i] == '>') {
				if(html_tag == true) {
					ParseHTMLAttribute(text + start, i - start);	
				}
				html_tag = false;
				start = i + 1; 
				continue;
			} 
		}

		if(html_tag == false) {
			PrintHighlightedPassage(text + start, text_length - start);	
		}
	}

	// This finds the domain name
	void FindDomainName(char url[], int url_length) {

		int stem_length = -1;
		for(int i=url_length-1; i>=0; i--) {
			if(url[i] =='/') {
				stem_length = i;
			}
		}

		if(stem_length < 0) {
			stem_length = url_length;
		}

		url[stem_length] = '\0';
		m_domain_name = url;
	}

	// This renders the title for the document
	// @param title_ptr - this stores the ptr to the first character
	//                  - in the title
	// @return the number of bytes that make up the title
	int DrawTitle(char *title_ptr) {

		u_short title_length = *(u_short *)title_ptr;

		return title_length + 2;

		title_ptr += 2;
		m_keyword_pass.CopyBufferToArrayList("<center><H1><a href=\"http://");
		m_keyword_pass.CopyBufferToArrayList(m_url.Buffer());
		m_keyword_pass.CopyBufferToArrayList("\">");

		if(title_length > 0) {
			PrintHighlightedPassage(title_ptr, title_length);
		} else {
			PrintHighlightedPassage(m_url.Buffer(), m_url.Size());
		}

		m_keyword_pass.CopyBufferToArrayList("</a></H1></center>");

		return title_length + 2;
	}

	// This returns the stemmed word 
	inline static void StemWord(const char *word, int &word_end, int word_start) {

		if(word[word_end-1] != 's' || word[word_end-2] == 'a' || word_end - word_start < 8 ||
			word[word_end-2] == 'o' || word[word_end-2] == 'u' || word[word_end-2] == 's' || word[word_end-2] == 'e') {
			return;
		}

		word_end--;
	}

	// This displays the title of the document
	void PrintTitleDocument(u_short title_length) {

		cout<<"<center><font size=\"4\"><a href=\"http://"<<m_url.Buffer()<<"\">";

		if(title_length > 0) {
			PrintHighlightedPassage(m_title_ptr, title_length, true);
		} else {
			cout<<m_url.Buffer();
		}

		cout<<"</a></font></center><br>";
	}

public:

	CDocumentInstance() {

		m_highlight_color[0] = "<b style=\"color:black;background-color:#ffff66\">";
		m_highlight_color[1] = "<b style=\"color:black;background-color:#ff9999\">";
		m_highlight_color[2] = "<b style=\"color:black;background-color:#99ff99\">";
		m_highlight_color[3] = "<b style=\"color:black;background-color:#a0ffff\">";
		m_highlight_color[4] = "<b style=\"color:white;background-color:#880000\">";
		m_highlight_color[5] = "<b style=\"color:black;background-color:#ff66ff\">";
		m_highlight_color[6] = "<b style=\"color:white;background-color:#990099\">";
		m_highlight_color[7] = "<b style=\"color:white;background-color:#00aa00\">";
		m_highlight_color[8] = "<b style=\"color:white;background-color:#886800\">";
		m_highlight_color[9] = "<b style=\"color:white;background-color:#004699\">";

		m_word_type_buff.Initialize(2048);
		m_keyword_highlight.Initialize(4);
		m_is_keyword_capt.Initialize(64);
		m_html_tag.Initialize(200, 8);

		m_is_paragraph_end = true;
		m_is_sentence_end = true;
		m_is_header_text = false;
		m_ignore_text = false;

		char html[][20]={"a", "/a", "br", "h", "/h", 
			"p", "/p", "ul", "/ul", "ol", "/ol", 
			"center", "/center", "script", "/script", "pre", "/pre", "//"};

		int index=0;
		while(!CUtility::FindFragment(html[index], "//")) {
			int length = (int)strlen(html[index]);
			m_html_tag.AddWord(html[index++], length);
		}

	}

	// This adds the query terms to the set of highlighted terms
	// @return the number of terms
	int AddQueryTerms(const char str[], int length) {

		int term_num = 0;
		int word_end;
		int word_start;

		CTokeniser::ResetTextPassage(0);

		for(int i=0; i<length; i++) {

			if(CTokeniser::GetNextWordFromPassage(word_end, word_start, i, str, length)) {
				StemWord(str, word_end, word_start);
				m_keyword_highlight.AddWord(str, word_end, word_start);
				if(!m_keyword_highlight.AskFoundWord()) {
					m_is_keyword_capt.PushBack(0);
					term_num++;
				}
			}
		}

		m_query_term_num = term_num;

		return term_num;
	}

	// This adds the keyword set which is used to cluster a given excerpt
	void AddKeywordTerms(const char str[], int length) {

		int word_end;
		int word_start;

		CTokeniser::ResetTextPassage(0);

		for(int i=0; i<length; i++) {

			if(CTokeniser::GetNextWordFromPassage(word_end, word_start, i, str, length)) {
				StemWord(str, word_end, word_start);
				m_keyword_highlight.AddWord(str, word_end, word_start);
				if(!m_keyword_highlight.AskFoundWord()) {
					m_is_keyword_capt.PushBack(0);
				}
			}
		}

		m_keyword_term_num = m_keyword_highlight.Size();
	}

	// This prints the compiled text
	void DisplayDocument(_int64 doc_id, CMemoryChunk<char> &keyword_buff, int excerpt_id) {

		u_short title_length = *(u_short *)m_title_ptr;
		m_title_ptr += 2;

		PrintTitleDocument(title_length);

		for(int i=0; i<m_keyword_pass.Size(); i++) {
			cout<<m_keyword_pass[i];
		}

		cout<<"`";

		m_summary.GenerateKeyPhrases(m_keyword_pass, m_keyword_highlight,
			m_keyword_term_num, m_is_keyword_capt);

		PrintTitleDocument(title_length);

		m_summary.CompileSummary(m_keyword_pass);

		cout<<"<center>";
		m_summary.RenderClusterLabel(m_keyword_highlight, excerpt_id, m_is_keyword_capt);
		cout<<"</center>";

		//cout<<"<center><H4><a href=\"http://localhost/cgi-bin/FindSimilarQuery?doc="
		//	<<doc_id<<"&q="<<query.Buffer()<<"&node="<<node_id<<"\">Explore</a>";
	}

	// This retrieves a particular document from the set and highlights keywords
	// @param doc_id - this is the id of the document being retrieved
	void Compile(CMemoryChunk<char> &doc_buff, bool is_draw_title = true) {

		m_line_break_num = 0;
		m_summary.Initialize(m_keyword_highlight.Size(), m_query_term_num);
		m_keyword_pass.Initialize(doc_buff.OverflowSize() + 100);
		static CMemoryChunk<char> url_buff(1024);

		int url_length = CFileStorage::ExtractURL
			(doc_buff.Buffer(), url_buff);

		url_buff[url_length] = '\0';
		m_url = url_buff.Buffer();
		//cout<<"<BR>"<<m_url.Buffer()<<"<BR>";

		FindDomainName(url_buff.Buffer(), url_length);

		url_length = CFileStorage::DocumentBegOffset(doc_buff.Buffer()) + 15;

		m_title_ptr = doc_buff.Buffer() + url_length;
		url_length += DrawTitle(doc_buff.Buffer() + url_length);

		CompileWebDocument(doc_buff.Buffer() + url_length,
			doc_buff.OverflowSize() - url_length);

		m_keyword_pass.CopyBufferToArrayList("</blockquote>");
		m_keyword_pass.CopyBufferToArrayList("</ul>");
		m_keyword_pass.CopyBufferToArrayList("</ol>");
		m_keyword_pass.CopyBufferToArrayList("</i>");
		m_keyword_pass.CopyBufferToArrayList("</pre>");

		m_summary.Initialize(m_keyword_highlight.Size(), m_query_term_num);
	}

};