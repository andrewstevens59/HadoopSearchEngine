#include "./SummaryOverlap.h"

// This defines a given instance of a word
struct SIndvWord {
	// This stores a ptr to the start of the word
	int word_start_ptr;
	// This stores a ptr to the end of the word
	int word_end_ptr;
	// This stores the keyword id
	int keyword_id;
	// This stores the length of the word
	int word_len;
	// This is the number of capital letters in the word
	int capital_num;
	// This stores the position of the word
	int pos;

	// This is a predicating for a sentence
	bool is_sentence_start;
	// This is a predicating for a sentence
	bool is_sentence_end;
	// This is a predicating for a paragraph
	bool is_paragraph_start;
	// This is a predicating for a paragraph
	bool is_paragraph_end;
	// This indicates a comma
	bool is_comma;
	// This indicates that a new keyword has been added
	// superficially in order to assign it a keyword
	bool is_new_keyword;

};	

// This class is responsible for generating a set of cluster phrases that 
// are used to summarize the excerpt. Cluster phrases are used as labels
// to describe a given excerpt so an excerpt can be grouped in some way.
class CClusterPhrase {

	// This defines the maximum allowable words in a sentence
	static const int MAX_SENTENCE_NUM = 24;

	// This stores the positions of a given key phrase
	struct SKeyPhrase {
		// This stores the position of the key phrase
		u_short left_pos;
		// This stores the position of the key phrase
		u_short right_pos;
		// This store the next key phrase
		SKeyPhrase *next_ptr;
	};

	// This stores the positions of a given key phrase
	struct SKeyPhrasePtr {
		// This stores the number of terms
		uChar term_num;
		// This stores the number supplied keyword matches
		uChar keyword_match_num;
		// This stores the phrase id
		uChar phrase_id;
		// This stores the length of the phrase
		uChar phrase_len;
		// This store the next key phrase
		SKeyPhrase *ptr;
	};

	// This is responsible for storing each cluster phrase
	CHashDictionary<int> m_phrase_map; 
	// This stores the occurrence of each cluster phrase
	CArrayList<int> m_phrase_occur; 

	// This stores the key phrase buffer
	CLinkedBuffer<SKeyPhrase> m_key_phrase_buff;
	// This stores the key phrase set
	CArrayList<SKeyPhrasePtr> m_key_phrase;
	// This is used to eliminate duplicate sentences
	CSummaryOverlap m_overlap;
	// This stores the number of global keywords
	int m_global_keyword_num;

	// This is used to compare key phrases
	static int CompareKeyPhrases(const SKeyPhrasePtr &arg1, const SKeyPhrasePtr &arg2) {
	
		if(arg1.term_num < arg2.term_num) {
			return -1;
		}

		if(arg1.term_num > arg2.term_num) {
			return 1;
		}

		if(arg1.keyword_match_num < arg2.keyword_match_num) {
			return -1;
		}

		if(arg1.keyword_match_num > arg2.keyword_match_num) {
			return 1;
		}

		if(arg1.phrase_len < arg2.phrase_len) {
			return -1;
		}

		if(arg1.phrase_len > arg2.phrase_len) {
			return 1;
		}

		return 0;
	}	

	// This adds a key phrase
	// @param num - this is the number of keywords in the phrase
	// @param phrase_id - this is the id for the key phrase
	// @left_pos - this is the position of the key phrase
	// @right_pos - this is the position of the key phrase
	// @param keyword_match_num - this is the original number of 
	//                          - terms matched to the original keyword set
	// @param phrase_len - number of characters that make up the phrase
	void AddKeyPhrase(int num, _int64 phrase_id, int left_pos, 
		int right_pos, int keyword_match_num, int phrase_len) {

		int id1 = m_phrase_map.AddWord((char *)&phrase_id, 6);

		if(m_phrase_map.AskFoundWord() == false) {
			m_phrase_occur.PushBack(0);
			m_key_phrase.ExtendSize(1);
			m_key_phrase.LastElement().ptr = NULL;
		} 

		m_phrase_occur[id1]++;
		SKeyPhrase *prev_ptr = m_key_phrase[id1].ptr;
		m_key_phrase[id1].ptr = m_key_phrase_buff.ExtendSize(1);
		m_key_phrase[id1].ptr->next_ptr = prev_ptr;
		m_key_phrase[id1].ptr->left_pos = left_pos;
		m_key_phrase[id1].ptr->right_pos = right_pos;
		m_key_phrase[id1].term_num = num;
		m_key_phrase[id1].phrase_id = id1;
		m_key_phrase[id1].keyword_match_num = keyword_match_num;
		m_key_phrase[id1].phrase_len = phrase_len;
	}

	// This removes the html tags from a given buffer
	void RemoveHTMLTags(CArrayList<char> &buff, char str[], int length, int start) {
	
		bool is_html_tag = false;

		for(int i=start; i<length; i++) {

			if(str[i] == '<') {
				is_html_tag = true;
				continue;
			}

			if(str[i] == '>') {
				is_html_tag = false;
				continue;
			}

			if(is_html_tag == false) {
				buff.PushBack(str[i]);
			}
		}
	}

	// This checks to see if the sentence is valid
	// @param buff - this stores the character buffer
	// @param sentence_start - this is the position of the beginning sentence
	// @param sentence_end - this is the position of the end of the sentence
	// @param word_buff - this stores the set of keyword terms
	bool IsValidSentence(CArrayList<char> &buff, int &sentence_start, 
		int &sentence_end, CVector<SIndvWord> &word_buff, int &keyword_num) {

		if(sentence_end - sentence_start < 10) {
			return false;
		}

		static CTrie word_map(4);
		static CArrayList<uChar> occur(40);
		word_map.Reset();
		occur.Resize(0);

		int capital_num = 0;
		for(int i=sentence_start; i<sentence_end; i++) {

			SIndvWord &word = word_buff[i];

			if(word.word_len < 4) {
				if(word.keyword_id >= 0 && word.keyword_id < m_global_keyword_num) {
					keyword_num++;
				}
				continue;
			}

			if(word.capital_num > 0 && ++capital_num >= 12) {
				return false;
			}

			int id = word_map.AddWord(buff.Buffer(), word.word_end_ptr, word.word_start_ptr);
			if(word_map.AskFoundWord() == false) {
				if(word.keyword_id >= 0 && word.keyword_id < m_global_keyword_num) {
					keyword_num++;
				}
				occur.PushBack(0);
			}

			if(++occur[id] >= 4) {
				return false;
			}
		}

		sentence_start = word_buff[sentence_start].word_start_ptr;
		sentence_end = word_buff[sentence_end].word_end_ptr;

		static CArrayList<char> temp_buff(200);
		temp_buff.Resize(0);
		RemoveHTMLTags(temp_buff, buff.Buffer(), sentence_end, sentence_start);

		int illegal_num = 0;
		int numeric_char = 0;
		for(int i=0; i<temp_buff.Size(); i++) {
			if((temp_buff[i] == '/' || temp_buff[i] == '_' || temp_buff[i] == '(' || temp_buff[i] == ':' || 
				temp_buff[i] == ')' || temp_buff[i] == '[' || temp_buff[i] == ']') && ++illegal_num >= 4) {
				return false;
			}

			if(CUtility::AskNumericCharacter(temp_buff[i]) == true && ++numeric_char >= 12) {
				return false;
			}
		}

		return true;
	}

	// This creates the sentence bounds
	// @param buff - this stores the character buffer
	// @param sentence_start - this is the position of the beginning sentence
	// @param sentence_end - this is the position of the end of the sentence
	// @param word_buff - this stores the set of keyword terms
	// @param term_num - this is the number of individual terms in the phrase
	// @param phrase - this stores the keyword phrase
	// @param phrase_bound - this is the bound of the keyword phrase
	void CreateSentenceBounds(CArrayList<char> &buff, int sentence_start, 
		int sentence_end, CVector<SIndvWord> &word_buff, int term_num,
		CString &phrase, SBoundary &phrase_bound) {

		int check_sum = CHashFunction::UniversalHash(phrase.Buffer(), phrase.Size());
		if(m_overlap.SentenceOverlap(sentence_start, sentence_end, check_sum) == false) {
			// rendundant sentence
			return;
		}

		int keyword_num = 0;
		if(IsValidSentence(buff, sentence_start, sentence_end, word_buff, keyword_num) == false) {
			sentence_end = sentence_start + 1;
		}

		if(phrase_bound.start > 0 && buff[phrase_bound.start-1] == ' ') {
			phrase_bound.start--;
		}

		if(phrase_bound.end < buff.Size() && buff[phrase_bound.end] == ' ') {
			phrase_bound.end++;
		}

		for(int i=phrase_bound.start; i<phrase_bound.end; i++) {
			cout<<buff[i];
		}

		cout<<"`";
		cout<<phrase.Buffer();
		cout<<"`";
		cout<<term_num;
		cout<<"`";

		for(int i=sentence_start; i<sentence_end; i++) {
			cout<<buff[i];
		}

		cout<<"`"<<keyword_num<<"`";
	}

	// This draws a sentence containing a key phrase
	// @param buff - this stores the character buffer
	// @param id - this is the phrase_id
	// @param left_pos - this is the position of the keyword
	// @param right_pos - this is the position of the keyword
	// @param term_num - this is the number of individual terms in the phrase
	// @param word_buff - this stores the set of keyword terms
	// @param phrase - this stores the keyword phrase
	void DrawSententce(CArrayList<char> &buff, _int64 phrase_id, int left_pos, 
		int right_pos, int term_num, CVector<SIndvWord> &word_buff, CString &phrase) {

		int left = left_pos;
		int right = right_pos;
		int word_num = 0;

		int sentence_start = left;
		int sentence_end = right;
		SBoundary phrase_bound(word_buff[left].word_start_ptr,
			word_buff[right].word_end_ptr);

		for(int i=0; i<MAX_SENTENCE_NUM; i++) {

			if(left >= 0) {
				sentence_start = left;
				word_num++;

				if((word_buff[left].is_sentence_start == true)
					|| word_buff[left].is_paragraph_start == true) {
					left = -1;
				}
			}

			if(right < word_buff.Size() - 1) {
				sentence_end = right;
				word_num++;

				if((word_buff[right].is_sentence_end == true)
					|| word_buff[right].is_paragraph_end == true) {
					right = word_buff.Size();
				}
			}

			left--;
			right++;
		}

		CreateSentenceBounds(buff, sentence_start, sentence_end, 
			word_buff, term_num, phrase, phrase_bound);
	}

public:

	CClusterPhrase() {
		m_phrase_map.Initialize(20);
		m_phrase_occur.Initialize(20);

		m_key_phrase_buff.Initialize(20);
		m_key_phrase.Initialize(20);
	}

	// This adds a potential phrase to the set
	// @phrase - this is the phrase that is being added
	// @left_pos - this is the position of the key phrase
	// @right_pos - this is the position of the key phrase
	// @param global_keyword_num - this is the size of keywords originally supplied
	// @param phrase_len - this is the number of characters that make up the phrase
	void AddPhrase(CCyclicArray<int> &phrase, int left_pos,
		int right_pos, int global_keyword_num, int phrase_len) {

		uChar num = 0;
		int keyword_match_num = 0;
		_int64 phrase_id = 0xFFFFFFFFFFFFFFFF;
		m_global_keyword_num = global_keyword_num;
		uChar *id = (uChar *)&phrase_id;

		for(int i=0; i<phrase.Size(); i++) {

			if(phrase[i] < 0) {
				break;
			}

			if(phrase[i] < global_keyword_num) {
				keyword_match_num++;
			}

			*id = phrase[i];
			num++;
			id++;
		}

		if(num > 0) {
			AddKeyPhrase(num, phrase_id, left_pos, right_pos, keyword_match_num, phrase_len);
		}
	}

	// This displays the set of summaries for a given keyword phrase
	// @param buff - this stores the character buffer
	// @param word_buff - this stores the set of keyword terms
	// @param keyword_dict - this stores the keyword dictionary for individual terms
	// @param is_keyword_capt - predicating indicating whether a word should 
	//                        - be dispalayed all in capitals
	void DisplayKeywordPhrase(CArrayList<char> &buff, 
		CVector<SIndvWord> &word_buff, CTrie &keyword_dict,
		CArrayList<char> &is_keyword_capt) {

		int len;
		CString phrase;
		_int64 phrase_id = 0xFFFFFFFFFFFFFFFF;
		static CArray<int> keyword_buff(10);

		for(int i=0; i<min(30, m_phrase_map.Size()); i++) {

			SKeyPhrase *curr_ptr = m_key_phrase[i].ptr;
			int id1 = m_key_phrase[i].phrase_id;
			keyword_buff.Resize(0);

			phrase.Reset();
			memcpy((char *)&phrase_id, m_phrase_map.GetWord(id1), 6);
			uChar *id = (uChar *)&phrase_id;
			int num = 0;

			int max = 0;
			int max_offset = 0;

			while(*id != 0xFF) {
				char *word = keyword_dict.GetWord(*id, len);

				if(len == 1 && *word == 's' && max > 0) {
					id++;
					continue;
				}

				keyword_buff.PushBack(len);
				id++;

				len = 0;
				for(int j=max(0, keyword_buff.Size() - 3); j<keyword_buff.Size(); j++) {
					len += keyword_buff[j];
				}

				if(len > max) {
					max = len;
					max_offset = max(0, keyword_buff.Size() - 3);
				}
			}

			int offset = 0;
			id = (uChar *)&phrase_id;
			while(*id != 0xFF) {

				char *word = keyword_dict.GetWord(*id, len);
				if(len == 1 && *word == 's' && offset > 0) {
					id++;
					continue;
				}

				if(offset++ == max_offset) {
					phrase += "[";
				}

				word[len] = '\0';
				for(int j=0; j<len; j++) {
					if(CUtility::AskEnglishCharacter(word[j])) {
						CUtility::ConvertToUpperCaseCharacter(word[j]);
					}

					if(*id >= is_keyword_capt.Size() || is_keyword_capt[*id] <= 0) {
						break;
					}
				}

				phrase += word;
				if(offset > max_offset) {
					num++;
				}

				if(num == 3) {
					phrase += "]";
				}

				phrase += " ";
				id++;
			}

			if(num < 3) {
				phrase += "]";
			}

			while(curr_ptr != NULL) {
				DrawSententce(buff, i, curr_ptr->left_pos, curr_ptr->right_pos, num, word_buff, phrase);
				curr_ptr = curr_ptr->next_ptr;
			}
		}
	}

	// This outputs the final cluster labels. This includes each cluster label
	// along with the weight of the cluster label for the given excerpt.
	// @param keyword_dict - this stores the keyword dictionary for individual terms
	// @param excerpt_id - the current excerpt being processed
	// @param is_keyword_capt - predicating indicating whether a word should 
	//                        - be dispalayed all in capitals
	void RenderClusterLabel(CTrie &keyword_dict, int excerpt_id,
		CArrayList<char> &is_keyword_capt) {

		CSort<SKeyPhrasePtr> sort(m_key_phrase.Size(), CompareKeyPhrases);
		sort.HybridSort(m_key_phrase.Buffer());

		int len;
		int phrase_len = 0;
		_int64 phrase_id = 0xFFFFFFFFFFFFFFFF;
		for(int i=0; i<m_phrase_map.Size(); i++) {

			if(m_key_phrase[i].term_num > 3) {
				continue;
			}

			SKeyPhrase *curr_ptr = m_key_phrase[i].ptr;
			int id1 = m_key_phrase[i].phrase_id;

			memcpy((char *)&phrase_id, m_phrase_map.GetWord(id1), 6);
			uChar *id = (uChar *)&phrase_id;
			uChar num = 0;

			cout<<"<font size=2><b>";
			while(*id != 0xFF) {

				char *word = keyword_dict.GetWord(*id, len);
				phrase_len += len + 1;

				word[len] = '\0';
				for(int j=0; j<len; j++) {
					if(CUtility::AskEnglishCharacter(word[j])) {
						CUtility::ConvertToUpperCaseCharacter(word[j]);
					}

					if(*id >= is_keyword_capt.Size() || is_keyword_capt[*id] <= 0) {
						break;
					}
				}

				cout<<word<<" ";
				num++;
				id++;
			}
			cout<<"</b></font>";

			m_phrase_occur[i] = min(m_phrase_occur[i], 2);
			m_phrase_occur[i] *= num;

			cout<<"&nbsp;&nbsp;&nbsp;";

			if(++phrase_len >= 140) {
				break;
			}
		}
	}
};