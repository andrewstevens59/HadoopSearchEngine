#include "./AddKeywords.h"

// This class is responsible for compiling the set of documents associated
// with a collection of hits. It also assigns a keyword match score to 
// each document by using the set of hits to assign the score. Once the 
// set of docuements has been compiled than they must be ranked. A document
// is ranked in priority order. Each document is ranked first by it's keyword
// match score. Secondly each document is ranked by its spatial score. Finally
// its ranked on its pulse rank score. Both the hit score and spatial score
// are assigned externally to this class. 
class CCompileRankedList {

	// This defines the maximum number of documents to send
	static const int MAX_DOC_NUM = 30000;

	// This is just a pointer to the document used for sorting
	struct SDocumentPtr {
		SDocument *doc_ptr;
	};

	// This stores all of the documents that have been discoversed so far
	CLinkedBuffer<SDocument> m_excerpt_doc_buff;
	// This stores the set of ranked documents
	CLimitedPQ<SDocumentPtr> m_doc_queue;
	// This is used to classify the type of hits in each search result
	CMemoryChunk<u_short> m_word_type_num;
	// This stores the existence of a particular term
	CMemoryChunk<bool> m_is_word_found;
	// This stores the set of document hits
	CArrayList<SDocHitItem> m_doc_hit_buff;

	// This stores the set of document matches
	CHashDictionary<int> m_doc_map;
	// This stores the set of document matches
	CArrayList<SDocument *> m_doc_buff;
	// This is used to attach additional keywords
	CMemoryChunk<CAddKeywords> m_keywords;

	// This is used for performance profiling
	CStopWatch m_timer;

	// This is the final sorting function used to sort documents. This is 
	// done on a combination of hit score, spatial score and pulse score. 
	// Note that all of this scores have been preprocessed to break them
	// into different priority divisions.
	static int CompareFinDoc(const SDocumentPtr &arg1, const SDocumentPtr &arg2) {

		SDocument &doc1 = *arg1.doc_ptr;
		SDocument &doc2 = *arg2.doc_ptr;

		if(doc1.word_div_num < doc2.word_div_num) {
			return -1;
		}

		if(doc1.word_div_num > doc2.word_div_num) {
			return 1;
		}

		if(doc1.rank < doc2.rank) {
			return 1;
		}

		if(doc1.rank > doc2.rank) {
			return -1;
		}

		return 0;
	}

	// Used to sort hit items based on position in document
	static int SortHitItem(const SDocHitItem &arg1, const SDocHitItem &arg2) {

		if(arg1.pos < arg2.pos) {
			return -1;
		}

		if(arg1.pos > arg2.pos) {
			return 1; 
		}

		return 0; 
	}

	// This finds the number of title hits
	void FindTitleHitNum(SDocument &doc) {

		doc.title_div_num = 0;
		m_word_type_num.InitializeMemoryChunk(0);
		SDocHitItem *curr_ptr = doc.hit_ptr;

		while(curr_ptr != NULL) {

			if(curr_ptr->enc == TITLE_HIT_INDEX && curr_ptr->pos < 12) {
				if(m_word_type_num[curr_ptr->word_id] == false) {
					m_word_type_num[curr_ptr->word_id] = true;
					doc.title_div_num++;
				}
			}
			
			curr_ptr = curr_ptr->next_ptr;
		}
	}

	// This calculates the hit score for a given document
	int HitScore(int max_gap) {

		m_word_type_num.InitializeMemoryChunk(0);
		m_is_word_found.InitializeMemoryChunk(false);

		int term_num = 0;
		int max_term_num = 0;
		for(int i=0; i<m_doc_hit_buff.Size() - 1; i++) {

			if(m_is_word_found[m_doc_hit_buff[i].word_id] == false) {
				m_is_word_found[m_doc_hit_buff[i].word_id] = true;
				max_term_num = max(max_term_num, term_num);
				m_word_type_num[term_num++]++;
			}

			int gap = m_doc_hit_buff[i+1].pos - m_doc_hit_buff[i].pos;
			if(gap > max_gap) {
				m_is_word_found.InitializeMemoryChunk(false);
				term_num = 0;
			}
		}

		if(m_is_word_found[m_doc_hit_buff.LastElement().word_id] == false) {
			m_is_word_found[m_doc_hit_buff.LastElement().word_id] = true;
			max_term_num = max(max_term_num, term_num);
			m_word_type_num[term_num++]++;
		}

		return max_term_num;
	}

	// This calculates the checksum for a particular document
	void CalculateCheckSum(SDocument &doc) {

		doc.check_sum = 0;
		m_doc_hit_buff.Resize(0);
		SDocHitItem *curr_ptr = doc.hit_ptr;
		int max_occur = 0;

		while(curr_ptr != NULL) {
			if(curr_ptr->enc == EXCERPT_HIT_INDEX) {
				doc.check_sum += (curr_ptr->word_id + 1) << (curr_ptr->pos >> 1);
				m_doc_hit_buff.PushBack(*curr_ptr);
				m_word_type_num[curr_ptr->word_id]++;

				max_occur = max(max_occur, m_word_type_num[curr_ptr->word_id]);
			}

			curr_ptr = curr_ptr->next_ptr;
		}

		if(max_occur > 45) {
			doc.check_sum = 0;
			return;
		}

		if(m_doc_hit_buff.Size() == 0) {
			doc.hit_score = 0;
			return;
		}

		CSort<SDocHitItem> sort(m_doc_hit_buff.Size(), SortHitItem);
		sort.ShellSort(m_doc_hit_buff.Buffer());

		int hit_score1 = HitScore(3);
		int hit_score2 = HitScore(20);
		
		doc.hit_score = min(3, hit_score2);
		doc.hit_score |= min(3, hit_score1) << 2;
	}

	// This sends one of the document results to the query server
	inline void SendDocumentRes(COpenConnection &conn, SDocument &doc) {

		CalculateCheckSum(doc);
		FindTitleHitNum(doc);

		conn.Send((char *)&doc.word_div_num, sizeof(uChar));
		conn.Send((char *)&doc.title_div_num, sizeof(uChar));
		conn.Send((char *)&doc.node_id, sizeof(S5Byte));
		conn.Send((char *)&doc.hit_score, sizeof(uChar));
		conn.Send((char *)&doc.check_sum, sizeof(uLong));
	}


public:

	CCompileRankedList() {
		m_excerpt_doc_buff.Initialize(2048);
		m_doc_hit_buff.Initialize(1024);
		m_doc_map.Initialize(100000);
		m_doc_queue.Initialize(MAX_DOC_NUM, CompareFinDoc);
		m_doc_buff.Initialize(20000);
		m_keywords.AllocateMemory(20);
	}

	// This returns a new excerpt document instance 
	// @param node_id - this is the id of the document being added
	// @param word_div_num - this stores the number of unique terms
	// @param hit_type - this stores the current hit type
	inline SDocument *NewExcerptDocument(_int64 &node_id, uChar word_div_num, uChar hit_type) {

		int id = m_doc_map.AddWord((char *)&node_id, sizeof(S5Byte));
		if(m_doc_map.AskFoundWord() == false) {
			SDocument *ptr = m_excerpt_doc_buff.ExtendSize(1);
			ptr->hit_ptr = NULL;
			ptr->prev_hit_type = hit_type;
			ptr->word_div_num = 0;
			m_doc_buff.PushBack(ptr);
			return ptr;
		} 

		if(hit_type != m_doc_buff[id]->prev_hit_type) {
			m_doc_buff[id]->prev_hit_type = hit_type;
			return m_doc_buff[id];
		}
		
		if(word_div_num >= m_doc_buff[id]->word_div_num) {
			return m_doc_buff[id];
		}

		return NULL;
	}

	// This returns the number of documents
	inline int DocumentNum() {
		return m_excerpt_doc_buff.Size();
	}

	// This attaches additional excerpt hits for the set of title hits
	void AttachExcerptHits() {

		CAddKeywords::ResetQueue();
		for(int i=0; i<CWordDiv::WordIDSet().Size(); i++) {
			SWordItem &word = CWordDiv::WordIDSet()[i];
			m_keywords[i].AddDocumentSet(m_excerpt_doc_buff, word.word_id, word.local_id, EXCERPT_HIT_INDEX, 0);
		}

		for(int i=0; i<1000000; i++) {

			if(CAddKeywords::ExpandRegion() == false) {
				return;
			}
		}
	}

	// This attaches additional keyword hits
	void AttachKeywordHits() {

		CAddKeywords::ResetQueue();
		static _int64 word_id[] = {10807637, 10229273, 7751938};

		m_word_type_num.AllocateMemory(CWordDiv::ClusterTermNum() + 1);
		m_is_word_found.AllocateMemory(CWordDiv::ClusterTermNum() + 1);

		SDocument *doc_ptr;
		uChar max_title_div_num = 0;
		m_excerpt_doc_buff.ResetPath();
		while((doc_ptr = m_excerpt_doc_buff.NextNode()) != NULL) {
			FindTitleHitNum(*doc_ptr);
			max_title_div_num = max(max_title_div_num, doc_ptr->title_div_num);
		}

		for(int i=0; i<3; i++) {
			S5Byte id = word_id[i];
			m_keywords[CWordDiv::ClusterTermNum() + i].AddDocumentSet(m_excerpt_doc_buff,
				id, CWordDiv::ClusterTermNum(), TITLE_HIT_INDEX, max_title_div_num);
		}

		for(int i=0; i<100000; i++) {

			if(CAddKeywords::ExpandRegion() == false) {
				return;
			}
		}
	}

	// This sends the search results to the query server
	// @param max_unique_words - this is the maximum number of hits in a doucment
	void SendSearchResults(COpenConnection &conn, int max_unique_words) {

		SQueryRes res;
		SDocumentPtr doc_ptr;
		m_excerpt_doc_buff.ResetPath();
		m_word_type_num.AllocateMemory(CWordDiv::ClusterTermNum() + 1);
		m_is_word_found.AllocateMemory(CWordDiv::ClusterTermNum() + 1);

		cout<<"Sending------------------ "<<m_doc_buff.Size()<<endl;


		if(m_doc_buff.Size() > MAX_DOC_NUM) {
		} else {
			int doc_num = m_doc_buff.Size();
			conn.Send((char *)&doc_num, sizeof(int));
		}

		for(int i=0; i<m_doc_buff.Size(); i++) {

			if(m_excerpt_doc_buff.Size() > MAX_DOC_NUM) {
				doc_ptr.doc_ptr = m_doc_buff[i];
				m_doc_queue.AddItem(doc_ptr);
			} else {
				SendDocumentRes(conn, *m_doc_buff[i]);
			}
		}

		if(m_doc_buff.Size() < MAX_DOC_NUM) {
			return;
		}

		int doc_num = m_doc_queue.Size();
		conn.Send((char *)&doc_num, sizeof(int));

		while(m_doc_queue.Size() > 0) {
			doc_ptr = m_doc_queue.PopItem();
			SendDocumentRes(conn, *doc_ptr.doc_ptr);
		}
	}

	// This is calle when processing each new query
	void Reset() {
		m_excerpt_doc_buff.Restart();
		m_doc_map.Reset();
		m_doc_queue.Reset();
		m_doc_buff.Resize(0);
		CAddKeywords::Reset();
	}
};