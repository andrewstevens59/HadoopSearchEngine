#include "./DidYouMean.h"

// This is responsible for converting a text string provided 
// by thhe user into a set of word ids. These word ids are 
// used throughout to initiate the search process. Multiple
// requests are made if there is more than one text string
// server available to issue a request to.
class CTextStringServer {

	// This stores one of the parallel connections to the text string server
	struct SInstConn {
		// This stores a this ptr
		CTextStringServer *this_ptr;
		// This stores a ptr to the text string being retrieved
		char *text_ptr;
		// This stores the length of the string being retrieved
		int text_length;
		// This stores the local id of the term
		int id;
		// This stores the handle for the thread
		HANDLE handle;
	};

	// This stores the number of query terms
	int m_query_term_num;
	// This stores the set of connections to the text string server
	CArray<SInstConn> m_inst_conn;
	// This stores the set of associations
	CAssociations m_assoc;
	// This is the did you mean phrase
	CDidYouMean m_spell;
	// This is a predicate indicating whether spell check should
	// be used or not when retrieving search terms
	uChar m_is_spell_check;
	// This protects access to shared resources
	static CMutex m_mutex;

	// This normalises all the word weights 
	// @param word_id - contains the word id's and wieghts
	void NormaliseWordWeight() {

		float sum = 0;
		for(int i=0; i<CRetrieveServer::WordIDSet().Size(); i++) {
			sum += CRetrieveServer::WordIDSet()[i].factor;
		}

		CArray<SWordItem> temp_set(CRetrieveServer::WordIDSet().Size());

		for(int i=0; i<CRetrieveServer::WordIDSet().Size(); i++) {
			CRetrieveServer::WordIDSet()[i].factor /= sum;
		}
	}

	// This tokenizes the closest matched text string returned by
	// the TextStringServer. It places each of the matched terms 
	// in each of the respective term buckets.
	// @param conn - this is the connection to the text string server
	// @param word_id - this is the current word id being processed
	// @param local_id - this stores the current local id of the word
	// @param match_score - this is the match score of the term
	// @param is_synom - true if the word is a synonym for the one supplied
	void TokenizeClosetMatchString(COpenConnection &conn, S5Byte &word_id,
		uLong word_occur, int local_id, uChar match_score, bool is_synom) {

		int word_end;
		int word_start;
		uChar word_len;
		char *buff = CUtility::SecondTempBuffer();
		conn.Receive((char *)&word_len, sizeof(uChar));
		conn.Receive(buff, word_len);
		m_assoc.AddAssociations(conn);

		int offset = 0;
		CTokeniser::ResetTextPassage(0);
		for(int j=0; j<word_len; j++) {
			if(CTokeniser::GetNextWordFromPassage(word_end,
				word_start, j, buff, word_len)) {
					
				int id = CRender::QueryTermID(buff + word_start, 
					word_end - word_start);

				if(id < 0) {
					id = local_id + offset;
				}

				offset++;
				m_spell.AddPossibleSpelling(buff + word_start, word_end - word_start, 
					id, word_occur, match_score, word_id, is_synom);
			}
		}
	}

	// This processes terms that may be similar but not the same as
	// the search term that has been entered by the user.
	// @param conn - this is the connection to the text string server
	// @param match_num - this is the number of term matches
	// @param local_id - this stores the current local id of the word
	void ProcessSimilarTerm(COpenConnection &conn, int match_num, int local_id) {

		S5Byte word_id;
		uChar term_match;
		uLong word_occur;
		bool is_synom;

		for(int i=0; i<match_num; i++) {
			conn.Receive((char *)&word_id, 5);
			conn.Receive((char *)&word_occur, 4);
			conn.Receive((char *)&term_match, 1);
			conn.Receive((char *)&is_synom, 1);

			m_mutex.Acquire();
			TokenizeClosetMatchString(conn, word_id, word_occur, 
				local_id, term_match, is_synom);
			m_mutex.Release();
		}
	}

	// This issues a request to the TextString Server to find the word id for a given 
	// text string. It may return an association id if the query terms match the 
	// combined terms in the association.
	void IssueRequest(SInstConn &inst) {
	
		COpenConnection conn;
		CNameServer::TextStringServerInst(conn);
	
		int match_num;
		static char request[20] = "SimilarWord";

		conn.Send(request, 20);
		conn.Send((char *)&m_is_spell_check, 1);
		conn.Send((char *)&inst.text_length, 2);
		conn.Send(inst.text_ptr, inst.text_length);
		conn.Receive((char *)&match_num, sizeof(int));

		ProcessSimilarTerm(conn, match_num, inst.id);
		conn.CloseConnection();
	}

	// This spawns a connnection to a text string server
	static THREAD_RETURN1 THREAD_RETURN2 TextStringServerThread(void *ptr) {
		SInstConn *call = (SInstConn *)ptr;
		call->this_ptr->IssueRequest(*call);
		return 0;
	}

	// This retrieves the word id for each term in the string
	// @param term_num - the maximum number of terms allowed
	// @param passage_length - the length of the excerpt passage
	// @param passage - buffer containing the passage being parsed
	void FindWordIds(int term_num, int passage_length, char passage[]) {

		int word_end;
		int word_start;
		static uLong occurr;
		bool new_word = true;
		int first_word = 0;

		int offset = 0;
		unsigned int uiThread1ID;
		for(int i=0; i<passage_length; i++) {
			if(CTokeniser::GetNextWordFromPassage(new_word, first_word, 
				word_end, word_start, i, passage, passage_length)) {

				int id = CRender::QueryTermID(passage + word_start, 
						word_end - word_start);

				if(id < 0) {
					continue;
				}

				if(offset >= term_num) {
					return;
				}

				if(CRender::IsDuplicateTerm(offset++)) {
					continue;
				}

				m_spell.AddQueryTerm(passage + word_start, word_end - word_start, id);

				m_inst_conn.ExtendSize(1);
				SInstConn *ptr = &m_inst_conn.LastElement();
				ptr->id = m_inst_conn.Size() - 1;
				ptr->text_ptr = passage + word_start;
				ptr->text_length = passage_length - word_start;
				ptr->this_ptr = this;

				ptr->handle = (HANDLE)_beginthreadex(NULL, 0, 
					TextStringServerThread, ptr, NULL, &uiThread1ID);
			}
		}
	}

	// This selects the closest matching terms in the dictionary
	// to those supplied by the user.
	void SelectWordSet() {

		m_query_term_num = 0;
		CRender::QueryText().Reset();
		m_spell.SelectSpelling(CRetrieveServer::WordIDSet());
		m_query_term_num = CRetrieveServer::WordIDSet().Size();
	}

public:

	CTextStringServer() {
	}

	// This returns the number of query terms
	inline int QueryTermNum() {
		return m_query_term_num;
	}

	// This creates the did you mean phrase
	inline void CreateDidYouMeanPhrase() {
		m_spell.CreateDidYouMeanPhrase();
	}

	// This is the main entry point for a query that has an associated
	// passage that needs to be culled and max terms found.
	// @param str - contains the query buffer
	void ParseQuery(char str[]) {

		int len;
		const char *query = CUtility::ExtractText(str, "q=", len);
		if(query != NULL) {
			CRender::QueryText().AddTextSegment(query, len);
		}

		m_is_spell_check = (CUtility::ExtractText(str, "spell=FALSE", len) == NULL);
		int query_length = CRender::QueryText().Size();

		CRender::QueryText().ConvertToLowerCase();
		CRender::OriginalText() = CRender::QueryText();

		m_query_term_num = CRender::AddQueryTerms(CRender::QueryText().Buffer(), query_length);

		m_spell.Initialize(CRender::QueryTermNum());
		CRetrieveServer::WordIDSet().Initialize(CRender::QueryTermNum() << 2);
		m_inst_conn.Initialize(CRender::QueryTermNum());

		if(m_query_term_num > 0) {
			FindWordIds(m_query_term_num, query_length, CRender::QueryText().Buffer());
		}

		for(int i=0; i<m_inst_conn.Size(); i++) {
			WaitForThread(m_inst_conn[i].handle, INFINITE);
		}

		SelectWordSet();
		NormaliseWordWeight();
	}
};
CMutex CTextStringServer::m_mutex;