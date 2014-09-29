#include "./ClusterPhrase.h"

const char *EXCLUDE_WORD = "nbsp midot amp s shtml quot and a amp www com of on or that the this to was what when where who will with the for"
	"from how in is for fromhow is it by  are as at i we'll we'll went were  weren't weren weve what whatever what'll whats whatve when whence"
	"whenever where whereafter whereas whereby wherein wheres whereupon great wherever whether which whichever while whilst whither who whod "
	"whoever whole who'll whom whomever who's whose why will willing wish with within without wonder wont would wouldn't wouldn want wants was"
	"wasn't wasn way we us use used useful uses using usually very than thank thanks thanx that that'll thats thats thatve the their their's"
	"them  themselves then thence there thereafter thereby thered therefore therein there'll therere theres  thereupon thereve these they theyd "
	"they'll they're they've thing things think third thirty this thorough thoroughly those though three through throughout thru  till to together "
	"too take taken taking some somebody  somehow  something sometime sometimes somewhat somewhere or other others otherwise ought oughtn't oughtn"
	"our ours ourselves much must mustn't mustn my might mightn't mightn mine make makes many may maybe mayn't mayn lest let lets is isn't isn it "
	"however itd it'll its its itself ive j k keep keeps kept know known knows had hadn't hadn has hasn't hasn have haven't haven having get gets "
	"getting given gives go goes going gone got gotten followed following follows for forever former formerly even ever evermore every everybody "
	"everyone everything everywhere do does doesn't doesn doing done don't don did didn't didn daren't daren could  couldn't couldn com come comes "
	"cause causes can't can but by be became because become becomes becoming been before beforehand are aren't aren around as as an and another "
	"any anybody anyhow anyone anything anyway anyway's anywhere ain't ain all allow allows almost also already again against about above a able "
	"occupy occupying original originally although essentially therefore however more meaning";

// This is used to compile the summary of the excerpt that is 
// displayed to the user before displaying the entire excerpt.
// This is accomplished using a cyclic array of some finite 
// window. Each instance of the window is ranked by the numer
// of keyword matches. The heterogenity of the keywords is 
// taken into account when ranking a given window instance.
class CCompileSummary {

	// This defines the maximum size of the window
	static const int WINDOW_SIZE = 20;

	// This stores a given instance of the window. 
	// It defines the number of keywords in the window
	// which is used to rank the window.
	struct SWindow {
		// This stores the keyword score of the window
		uLong keyword_score;
		// This stores the ptr to the start of the text segment
		int word_start;
		// This stores the ptr to the end of the text segment
		int word_end;
		// This stores the number of terms in the window
		int term_num;
		// This stores the space misalignment either side of a keyword bound
		int keyword_align_offset;
		// This stores the start offset in the keyword buffer
		int keyword_start;
		// This stores the end offset in the keyword buffer
		int keyword_end;
		// This is a predicate indicating if the window
		// is aligned to a sentence beginning or end
		bool sentence_align;
		// This is a predicate indicating if the
		// window is aligned to a paragraph 
		bool paragraph_align;
		// This stores a ptr to the next window
		SWindow *next_ptr;
	};

	// This is used for sorting
	struct SWindowPtr {
		SWindow *ptr;
	};

	// This stores the set of words that make up the window
	CCyclicArray<SIndvWord *> m_window;
	// This stores the set of keywords in a window
	CQueue<SIndvWord *> m_keyword_set;
	// This stores the number of occurrences of a given keyword
	CMemoryChunk<int> m_keyword_occur;
	// This stores the set of window instances
	CLinkedBuffer<SWindow> m_window_inst;
	// This stores the set of word
	CVector<SIndvWord> m_word_buff;
	// This stores the final set of summary positions
	CArray<SBoundary> m_summary_bound;
	// This stores the set of stop words which should not be used
	// as singular keywords
	CHashDictionary<int> m_stop_word_dict;
	
	// This stoers the complete set of keywords in the order
	// they appear in the document
	CVector<int> m_keyword_buff;
	// This stores the current keyword boundary for the set
	// of keyword that make up the window
	SBoundary m_keyword_bound;

	// This is used to check for summary overlap
	CSummaryOverlap m_overlap;
	// This is used to generate individual key phrases
	CClusterPhrase m_phrase;
	// This stores the number of query terms
	int m_query_term_num;

	// This is used to compare windows
	static int CompareWindows(const SWindow &arg1, const SWindow &arg2) {

		if(arg1.keyword_score < arg2.keyword_score) {
			return -1;
		}

		if(arg1.keyword_score > arg2.keyword_score) {
			return 1;
		}

		if(arg1.keyword_align_offset < arg2.keyword_align_offset) {
			return 1;
		}

		if(arg1.keyword_align_offset > arg2.keyword_align_offset) {
			return -1;
		}

		if(arg1.paragraph_align < arg2.paragraph_align) {
			return -1;
		}

		if(arg1.paragraph_align > arg2.paragraph_align) {
			return 1;
		}

		if(arg1.sentence_align < arg2.sentence_align) {
			return -1;
		}

		if(arg1.sentence_align > arg2.sentence_align) {
			return 1;
		}

		if(arg1.term_num < arg2.term_num) {
			return -1;
		}

		if(arg1.term_num > arg2.term_num) {
			return 1;
		}

		return 0;
	}

	// This updates the window and the keyword score. In particular
	// any keywords that are removed from the window are decremented
	// from the keyword score. Any keywords that are added to the 
	// window are added to the keyword score.
	void UpdateWindow() {

		if(m_window.Size() < m_window.OverflowSize()) {
			return;
		}

		SIndvWord *ptr = m_window[0];
		if(ptr->keyword_id >= 0) {
			m_keyword_bound.start++;
			if(ptr == m_keyword_set.PeekFirst()) {
				m_keyword_set.PopItem();
			}
		}
	}

	// This adds the window the set of window instances
	void AddWindowInst() {
		if(m_keyword_bound.Width() == 0) {
			return;
		}

		if(m_window.Size() < 6) {
			return;
		}

		SWindow *ptr = m_window_inst.ExtendSize(1);
		ptr->word_start = m_window[0]->pos;
		ptr->word_end = m_window.LastElement()->pos;
		ptr->term_num = m_window.Size();
		ptr->keyword_start = m_keyword_bound.start;
		ptr->keyword_end = m_keyword_bound.end;

		ptr->keyword_align_offset = abs(m_keyword_set.PeekFirst()->pos - m_window[0]->pos);
		ptr->keyword_align_offset -= abs(m_window.LastElement()->pos - m_keyword_set.PeekLast()->pos);
		ptr->keyword_align_offset = abs(ptr->keyword_align_offset);

		ptr->sentence_align = m_window[0]->is_sentence_start |
			m_window.LastElement()->is_sentence_end;
		ptr->paragraph_align = m_window[0]->is_paragraph_start | 
			m_window.LastElement()->is_paragraph_end;
	}

	// This searches for the start of a sentence
	int SentenceStart(int pos) {

		int text_start = m_word_buff[pos].word_start_ptr;

		if(m_word_buff[pos].is_sentence_start || m_word_buff[pos].is_paragraph_start) {
			return text_start;
		}

		for(int i=0; i<10; i++) {
			if(pos == 0) {
				return text_start;
			}

			SIndvWord &word = m_word_buff[--pos];
			if(word.is_sentence_start == true) {
				return word.word_start_ptr;
			}

			if(word.keyword_id >= 0) {
				return text_start;
			}
		}

		return text_start;
	}

	// This searches fo the end of a sentence
	int SentenceEnd(SWindow &window) {

		int pos = window.word_end;
		int width = window.word_end - window.word_start;
		
		for(int i=0; i<35 - width; i++) {

			SIndvWord &word = m_word_buff[pos++];

			if(word.is_paragraph_end == true) {
				return word.word_end_ptr;
			}

			if(pos >= m_word_buff.Size()) {
				return m_word_buff.LastElement().word_end_ptr;
			}
		}

		int text_end = m_word_buff[pos].word_end_ptr;

		for(int i=0; i<15; i++) {

			if(pos >= m_word_buff.Size()) {
				return text_end;
			}

			SIndvWord &word = m_word_buff[pos++];
			if(word.is_sentence_end == true || word.is_paragraph_end == true) {
				return word.word_end_ptr;
			}

			if(word.keyword_id >= 0) {
				return text_end;
			}
		}

		return text_end;
	}

	// This prints the window set
	void PrintWindow() {

		for(int i=0; i<m_window.Size(); i++) {
			cout<<(int)m_window[i]<<" ";
		}

		getchar();
	}

	// This prints the summary
	void PrintSummary(char *text_start, char *text_end) {

		while(text_start < text_end) {
			if(CUtility::FindFragment(text_start, "<") && 
				CUtility::FindFragment(text_start, "<FONT") == false && 
				CUtility::FindFragment(text_start, "</FONT") == false && 
				CUtility::FindFragment(text_start, "<B>") == false && 
				CUtility::FindFragment(text_start, "</B>") == false) {

				text_start++;
				while(*text_start != '>') {
					if(text_start >= text_end) {
						break;
					}
					text_start++;
				}
				text_start++;
			}

			cout<<*text_start;
			text_start++;
		}
	}

	// This calculates the keyword score for a window
	void CalculateKeywordScore(SWindow &window) {

		int query_term_num = 0;
		window.keyword_score = 0;
		for(int i=window.keyword_start; i<window.keyword_end; i++) {
			int offset = m_keyword_occur[m_keyword_buff[i]] + 1;
			offset = 32 - offset;

			if(m_keyword_buff[i] < m_query_term_num) {
				query_term_num++;
			}

			window.keyword_score += 1 << offset;
		}

		window.keyword_score |= query_term_num << 29;
	}

	// This updates the keyword occurrence based upon the selected window
	void UpdateKeywordOccurence(SWindow &window) {

		for(int i=window.keyword_start; i<window.keyword_end; i++) {
			m_keyword_occur[m_keyword_buff[i]]++;
		}
	}

	// This checks to see if a singular term is in close proximity to another 
	// keyword to be allowed as a valid keyword.
	// @param global_keyword_num - this is the size of keywords originally supplied
	// @param buff - this stores the character set for the document
	bool IsValidSingularTerm(int offset, int global_term_num, CArrayList<char> &buff) {

		SIndvWord &word = m_word_buff[offset];

		if(word.capital_num == 0) {
			return false;
		}

		char *str = CUtility::SecondTempBuffer();
		memcpy(str, buff.Buffer() + word.word_start_ptr, word.word_len);
		CUtility::ConvertBufferToLowerCase(str, word.word_len);

		if(m_stop_word_dict.FindWord(str, word.word_len) >= 0) {
			return false;
		}

		if(word.keyword_id < global_term_num && word.word_len >= 3) {
			return true;
		}

		if(word.capital_num > 0 && word.word_len >= 4 && word.is_sentence_start == false) {
			return true;
		}

		return false;
	}

	// This determines if a give word is a stop word
	int IsStopWord(CArrayList<char> &buff, int offset) {

		SIndvWord &word = m_word_buff[offset];

		char *str = CUtility::SecondTempBuffer();
		memcpy(str, buff.Buffer() + word.word_start_ptr, word.word_len);
		CUtility::ConvertBufferToLowerCase(str, word.word_len);

		if(m_stop_word_dict.FindWord(str, word.word_len) < 0) {
			return -1;
		}

		return word.word_len;
	}

	// This adds a phrase to the set, first checks a few conditions 
	// @param keyword_windows - the phrase that is being added
	// @param global_keyword_num - this is the size of keywords originally supplied
	// @param phrase_len - this is the number of characters that make up the phrase
	// @param word_pos - this is the current position of the word being processed
	// @param buff - this stores the character set for the document
	void AddPhraseToSet(CCyclicArray<int> &keyword_window, int global_keyword_num,
		int phrase_len, int word_pos, CArrayList<char> &buff) {

		if(keyword_window.Size() == 0) {
			return;
		}

		if(keyword_window.Size() >= 2) {
			if(keyword_window[0] == keyword_window[1]) {
				return;
			}
		}

		if(keyword_window.Size() >= 3) {
			if(keyword_window[0] == keyword_window[2]) {
				return;
			}

			if(keyword_window[1] == keyword_window[2]) {
				return;
			}
		}

		if(keyword_window.Size() == 1 && IsValidSingularTerm(word_pos - 1, global_keyword_num, buff) == false) {
			return;
		}

		int word_len;
		int left = word_pos - keyword_window.Size();
		int right = word_pos - 1;

		if(keyword_window.Size() == 2) {

			if((word_len = IsStopWord(buff, left)) >= 0) {
				keyword_window.PushBack(-1);
				phrase_len -= word_len;
				left++;
			} else if((word_len = IsStopWord(buff, right)) >= 0) {
				keyword_window[1] = -1;
				phrase_len -= word_len;
				right--;
			}
		}

		m_phrase.AddPhrase(keyword_window, left, right, global_keyword_num, phrase_len);
	}

	// This splits a long phrase into two smaller phrases if necessary
	// @param keyword_windows - the phrase that is being added
	// @param word_pos - this stores the current word being processed
	// @param global_keyword_num - this is the size of keywords originally supplied
	// @param phrase_len - this stores the number of characters in the phrase
	void SplitPhrase(CCyclicArray<int> &keyword_window, 
		int word_pos, int global_keyword_num, int &phrase_len) {

		int prev_phrase_len = phrase_len;
		phrase_len = 0; 
		for(int j=0; j<keyword_window.Size(); j++) {

			int id = word_pos + 1 - keyword_window.Size() + j;

			prev_phrase_len -= m_word_buff[id].word_len; 
			if(m_word_buff[id].is_new_keyword == true && (j < keyword_window.Size() - 2) && j > 1) {
				int prev_size = keyword_window.Size();
				keyword_window[j] = -1;

				m_phrase.AddPhrase(keyword_window, id - j, id - 1, global_keyword_num, phrase_len);
				keyword_window.Reset();

				phrase_len = prev_phrase_len;
				for(int k=j+1; k<prev_size; k++) {
					keyword_window.PushBack(m_word_buff[++id].keyword_id);
				}

				break;
			}

			phrase_len += m_word_buff[id].word_len;
		}
	}

public:

	CCompileSummary() {

		CUtility::Initialize();
		m_window.Initialize(WINDOW_SIZE);
		m_window_inst.Initialize(100);
		m_keyword_bound.Set(0, 0);

		int word_end;
		int word_start;
		CTokeniser::ResetTextPassage(0);
		int length = strlen(EXCLUDE_WORD);
		m_stop_word_dict.Initialize(1024, 32);

		for(int i=0; i<length; i++) {
			if(CTokeniser::GetNextWordFromPassage(word_end, word_start, i, EXCLUDE_WORD, length)) {
				m_stop_word_dict.AddWord(EXCLUDE_WORD, word_end, word_start);
			}
		}
	}

	// This sets up the various buffers
	void Initialize(int keyword_num, int query_num) {
		m_keyword_occur.AllocateMemory(keyword_num, 1);
		m_summary_bound.Initialize(5);
		m_query_term_num = query_num;
	}

	// This returns true if a given word is a stopword
	inline bool IsStopWord(char *word, int start, int length) {
		return m_stop_word_dict.FindWord(word, length, start) >= 0;
	}

	// This resets the window for a new paragraph
	inline void Reset() {
		m_window.Reset();
		m_keyword_occur.InitializeMemoryChunk(0);
		m_keyword_bound.start = m_keyword_bound.end;
		m_keyword_set.Reset();
	}

	// This sets the current word as the sentence end
	inline void SetSentenceEnd() {

		if(m_word_buff.Size() == 0) {
			return;
		}

		m_word_buff.LastElement().is_sentence_end = true;
	}

	// This sets the current word as the paragraph end
	inline void SetParagraphEnd() {

		if(m_word_buff.Size() == 0) {
			return;
		}

		m_word_buff.LastElement().is_paragraph_end = true;
	}

	// This sets the current word as a comma type
	inline void SetComma() {

		if(m_word_buff.Size() == 0) {
			return;
		}

		m_word_buff.LastElement().is_comma = true;
	}

	// This adds a term to the set
	// @param word_start - this is the ptr to the first character
	//                   - in the word
	// @param word_end - this is the ptr to the last character
	//                 - in the word
	// @param keyword_id - this is keyword id, -1 if not a keyword
	// @param is_sentence_start - predicate indicating the start of a sentence
	// @param is_sentence_end - predicate indicating the end of a sentence
	// @param is_paragraph_start - predicate indicating the start of a paragraph
	// @param is_paragraph_end - predicate indicating the end of a paragraph
	// @param is_comma - true if the word contains a comma
	// @param capital_num - number of capital letters in the word
	// @param word_len - the length of the word in characters
	void AddTerm(int word_start, int word_end, int keyword_id, 
		bool is_sentence_start, bool is_sentence_end, bool is_paragraph_start, 
		bool is_paragraph_end, bool is_comma, int capital_num, int word_len) {

		m_word_buff.ExtendSize(1);
		SIndvWord *ptr = &m_word_buff.LastElement();
		ptr->word_start_ptr = word_start;
		ptr->word_end_ptr = word_end;
		ptr->keyword_id = keyword_id;
		ptr->is_sentence_start = is_sentence_start;
		ptr->is_sentence_end = is_sentence_end;
		ptr->is_paragraph_start = is_paragraph_start;
		ptr->is_paragraph_end = is_paragraph_end;
		ptr->pos = m_word_buff.Size() - 1;
		ptr->is_new_keyword = false;
		ptr->word_len = word_len;
		ptr->capital_num = capital_num;
		ptr->is_comma = is_comma;
		ptr->is_comma |= is_sentence_end;
		ptr->is_comma |= is_paragraph_end;

		UpdateWindow();

		if(keyword_id >= 0) {
			m_keyword_bound.end++;
			m_keyword_buff.PushBack(keyword_id);
			m_keyword_set.AddItem(ptr);
		}

		m_window.PushBack(ptr);
		AddWindowInst();
	}

	// This generates the list of possible key phrases that are composed 
	// of multiple individual keywords. These key phrases are used for clustering.
	// @param buff - this stores the character buffer
	// @param keyword_dict - this stores the set of keywords
	// @param global_keyword_num - this is the number of keywords originally supplied
	// @param is_keyword_capt - predicating indicating whether a word should 
	//                        - be dispalayed all in capitals
	void GenerateKeyPhrases(CArrayList<char> &buff, CTrie &keyword_dict,
		int global_keyword_num, CArrayList<char> &is_keyword_capt) {

		int phrase_len = 0;
		CCyclicArray<int> keyword_window(6);
		for(int i=0; i<m_word_buff.Size() - 1; i++) {

			SIndvWord &word = m_word_buff[i];
			bool delim = (word.is_sentence_end == true || word.is_paragraph_end == true || word.is_comma == true);

			if(word.keyword_id < 0 && (keyword_window.Size() == 0 || word.word_len >= 4 || 
				m_word_buff[i+1].keyword_id < 0)) {

				AddPhraseToSet(keyword_window, global_keyword_num, phrase_len, i, buff);
				phrase_len = 0;
				keyword_window.Reset();
				continue;
			}

			if(word.keyword_id < 0 && word.word_len < 4) {
				word.keyword_id = keyword_dict.AddWord(buff.Buffer(), word.word_end_ptr, word.word_start_ptr);
				word.is_new_keyword = true;
			}

			keyword_window.PushBack(word.keyword_id);
			phrase_len += word.word_len;

			if(keyword_window.Size() > 4) {
				SplitPhrase(keyword_window, i, global_keyword_num, phrase_len);
			}

			if(delim == true) {
				AddPhraseToSet(keyword_window, global_keyword_num, phrase_len, i + 1, buff);
				phrase_len = 0;
				keyword_window.Reset();
			}
		}

		AddPhraseToSet(keyword_window, global_keyword_num, phrase_len, m_word_buff.Size() - 1, buff);
		m_phrase.DisplayKeywordPhrase(buff, m_word_buff, keyword_dict, is_keyword_capt);
	}

	// This outputs the final cluster labels. This includes each cluster label
	// along with the weight of the cluster label for the given excerpt.
	// @param keyword_dict - this stores the keyword dictionary for individual terms
	// @param excerpt_id - the current excerpt being processed
	// @param is_keyword_capt - predicating indicating whether a word should 
	//                        - be dispalayed all in capitals
	inline void RenderClusterLabel(CTrie &keyword_dict, int excerpt_id,
		CArrayList<char> &is_keyword_capt) {

		cout<<"<b>";
		m_phrase.RenderClusterLabel(keyword_dict, excerpt_id, is_keyword_capt);
		cout<<"</b>";
	}

	// This compiles the set of windows
	void CompileSummary(CArrayList<char> &buff) {

		if(m_window_inst.Size() == 0) {
			return;
		}

		SWindow *ptr;
		SWindow *max_window;
		SWindow dummy;
		SBoundary bound;
		dummy.keyword_score = 0;

		for(int i=0; i<1; i++) {
			m_window_inst.ResetPath();
			max_window = &dummy;
			while((ptr = m_window_inst.NextNode()) != NULL) {

				bound.start = SentenceStart(ptr->word_start);
				bound.end = SentenceEnd(*ptr);
				if(m_overlap.SummaryOverlap(bound.start, bound.end)) {
					continue;
				}
				
				CalculateKeywordScore(*ptr);
				if(CompareWindows(*max_window, *ptr) < 0) {
					max_window = ptr;
				}
			}

			if(max_window == &dummy) {
				return;
			}

			UpdateKeywordOccurence(*max_window);
			bound.start = SentenceStart(max_window->word_start);
			bound.end = SentenceEnd(*max_window);

			m_summary_bound.PushBack(bound);
			m_overlap.AddBound(bound.start, bound.end);
			PrintSummary(bound.start + buff.Buffer(), bound.end + buff.Buffer());
			cout<<"</a></i><P>";
		}
	}
};