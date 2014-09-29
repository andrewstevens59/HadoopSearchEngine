#include "./RenderResults.h"

// This class is responsible for compiling a set of phrases
// that are similar to the query for which terms in the query
// have been altered to match a more common phrase. This is 
// done for the purposes of spell checking to try and interpret
// what the user really meant. The results returned to the user
// is one or more phrases where certain terms have been replaced
// with more common terms.
class CDidYouMean {

	// This stores one of the possible spellings for a word
	struct STerm {
		// This stores the offset in the word buffer
		int word_offset;
		// This stores the occurrence of the word
		uLong word_occur;
		// This stores the length of the word
		u_short word_length;
		// This stores a ptr to the next possible spelling
		STerm *next_ptr;
		// This stores the word_id
		S5Byte word_id;
		// This stores the match score
		uChar match_score;
		// This is a predicate indicating a synonym
		uChar is_synom;
	};

	// This stores the focus terms
	CString m_focus_terms;
	// This stores the number of mismatched words
	int m_mismatch_num;

	// This stores the set of possible spellings for words
	CArrayList<char> m_word_buff;
	// This stores the set of terms
	CMemoryChunk<STerm *> m_word_set;
	// This stores all of the different terms
	CLinkedBuffer<STerm> m_term_buff;

	// This is the ranking function used to compare two terms
	inline int CompareTerms(STerm *arg1, STerm *arg2) {

		if(arg1->is_synom < arg2->is_synom) {
			return 1;
		}

		if(arg1->is_synom > arg2->is_synom) {
			return -1;
		}

		if(arg1->match_score < arg2->match_score) {
			return -1;
		}

		if(arg1->match_score > arg2->match_score) {
			return 1;
		}

		if(arg1->word_occur < arg2->word_occur) {
			return -1;
		}

		if(arg1->word_occur > arg2->word_occur) {
			return 1;
		}

		return 0;
	}

	// This returns true if the two terms match, false otherwise
	bool CompareSpelling(STerm *arg1, STerm *arg2) {
		char *str1 = arg1->word_offset + m_word_buff.Buffer();
		char *str2 = arg2->word_offset + m_word_buff.Buffer();

		if(arg1->word_length != arg2->word_length) {
			return false;
		}

		for(int i=0; i<arg1->word_length; i++) {
			if(str1[i] != str2[i]) {
				return false;
			}
		}

		return true;
	}

	// This adds the search phrase to the list
	// @param is_same - this is a predicate indicating whether
	//                - the search term matches the retrieved term
	// @param max_ptr - this stores a ptr to the retrieved term
	// @param term_id - this is the id of the current term being processed
	void AddSearchPhrase(bool is_same, STerm *max_ptr, int term_id) {

		char *str = max_ptr->word_offset + m_word_buff.Buffer();

		CRender::DYMURL().AddTextSegment(str, max_ptr->word_length);
		CRender::DYMURL() += "+";

		CRender::QueryText() += " ";
		CRender::QueryText().AddTextSegment(str, max_ptr->word_length);

		if(is_same == false) {
			m_mismatch_num++;
			CRender::DYMPhrase() += "<B><i>";
		}

		CRender::DYMPhrase().AddTextSegment(str, max_ptr->word_length);

		if(is_same == false) {
			CRender::DYMPhrase() += "</i></B>";
		}

		CRender::DYMPhrase() += " ";
	}

public:

	CDidYouMean() {
		m_mismatch_num = 0;
	}

	// This initializes the total number of words in the set
	void Initialize(int total_word_num) {

		m_word_set.AllocateMemory(total_word_num, NULL);
		m_term_buff.Initialize();
		m_word_buff.Initialize(1024);

		for(int i=0; i<total_word_num; i++) {
			m_word_set[i] = m_term_buff.ExtendSize(1);	
			m_word_set[i]->match_score = 0;
			m_word_set[i]->word_occur = 0;
			m_word_set[i]->next_ptr = NULL;		
		}
	}

	// This adds one of the root terms to the set
	// @param term_id - this is the current id of the term being processed
	void AddQueryTerm(char str[], int length, int term_id) {
		m_word_set[term_id]->word_offset = m_word_buff.Size();
		m_word_set[term_id]->word_length = length;
		m_word_set[term_id]->next_ptr = NULL;
		m_word_buff.CopyBufferToArrayList(str, length);
	}

	// This adds a possible spelling of the root term
	// @param term_id - this is the current id of the term being processed
	// @param word_occur - this stores the occurrence of the term
	// @param assoc_weight - this stores the weight given adjacent terms
	// @param match_score - this is the match score of the term
	// @param word_id - this is the word id of the term
	// @param is_synom - true if the word is a synonym for the one supplied
	void AddPossibleSpelling(char str[], int length, int term_id,
		uLong word_occur, uChar match_score, S5Byte &word_id, bool is_synom) {

		if(term_id >= m_word_set.OverflowSize()) {
			return;
		}

		STerm *prev_ptr = m_word_set[term_id]->next_ptr;
		STerm *curr_ptr = m_word_set[term_id];
		curr_ptr->next_ptr = m_term_buff.ExtendSize(1);
		curr_ptr = curr_ptr->next_ptr;
		curr_ptr->next_ptr = prev_ptr;

		curr_ptr->word_offset = m_word_buff.Size();
		curr_ptr->word_length = length;
		curr_ptr->word_occur = word_occur;
		curr_ptr->match_score = match_score;
		curr_ptr->word_id = word_id;
		curr_ptr->is_synom = is_synom;

		m_word_buff.CopyBufferToArrayList(str, length);
	}

	// This ranks all of the possible spellings of a particular term
	void SelectSpelling(CArrayList<SWordItem> &word_id_set) {

		m_mismatch_num = 0;
		int offset = word_id_set.Size();
		for(int i=0; i<m_word_set.OverflowSize(); i++) {
			STerm *curr_ptr = m_word_set[i];

			STerm *max_ptr = curr_ptr;
			curr_ptr = curr_ptr->next_ptr;
			while(curr_ptr != NULL) {

				char *str = curr_ptr->word_offset + m_word_buff.Buffer();

				if(curr_ptr->is_synom == true) {
					word_id_set.ExtendSize(1);
					word_id_set.LastElement().word_id = curr_ptr->word_id;
					word_id_set.LastElement().local_id = offset + i;

					CRender::AdditionalQueryText() += " ";
					CRender::AdditionalQueryText().AddTextSegment(str, curr_ptr->word_length);
				}
				
				if(CompareTerms(max_ptr, curr_ptr) < 0) {
					max_ptr = curr_ptr;
				}
				curr_ptr = curr_ptr->next_ptr;
			}

			if(max_ptr != m_word_set[i]) {
				bool is_same = CompareSpelling(m_word_set[i], max_ptr);
				AddSearchPhrase(is_same, max_ptr, i);
			}
			
			if(word_id_set.Size() > 0 && word_id_set.LastElement().word_id == max_ptr->word_id) {
				continue;
			}
			
			if(max_ptr != m_word_set[i]) {
				word_id_set.ExtendSize(1);
				word_id_set.LastElement().word_id = max_ptr->word_id;
				word_id_set.LastElement().local_id = offset + i;
			}
		}
	}

	// This renders the did you mean phrase
	void CreateDidYouMeanPhrase() {

		if(m_mismatch_num == 0) {
			CRender::DYMPhrase().Reset();
			return;
		}
	}
};