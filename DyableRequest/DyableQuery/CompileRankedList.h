#include "./KeywordServer.h"

// This stores the set of ranked documents that have been retrieved from each
// of the retrieve servers. It is responsible for creating the final ranked list
// of documents. It also removes duplicate documents.
class CCompileRankedList {

	// This stores the reverse keyword map
	struct SKeywordMap {
		// This stores the doc id
		int doc_id;
		// This stores the next ptr
		SKeywordMap *next_ptr;
	};

	// This is used to sort keywords based on variance
	struct SKeywordInst {
		// This stores the keyword id
		int id;
		// This stores the variance
		float var;
	};

	// This defines the maximum number of documents to use
	// for the final ranking
	static const int MAX_DOCUMENT_SIZE = 100;
	// This defines the number of top matches used to generate
	// the keyword distribution which is used to generate results
	static const int KEYWORD_DOCUMENT_SIZE = 200;

	// This stores the set of keywords
	CMemoryChunk<SKeywordMap *> m_reverse_map;
	// This stores the set of keyword maps
	CLinkedBuffer<SKeywordMap> m_keyword_buff;
	// This stores all of the documents that have been discoversed so far
	CLinkedBuffer<SQueryRes> m_excerpt_doc_buff;

	// This stores the set of keywords for each for each excerpt
	CMemoryChunk<int> m_keyword_set;
	// This is used to the top N documents that have a high hit score 
	// in relation to the query
	CLimitedPQ<SQueryResPtr> m_hit_score_queue;
	// This stores the ptr to the final set
	static CArray<SQueryResPtr> *m_fin_set_ptr;

	// This is used to calculate the expected reward of each node
	CExpRewServer m_exp_rew;
	// This stores is used to resolve the set of keywords
	CKeywordServer m_keyword_server;

	// This is used to compare keyword by variance 
	static int CompareByVariance(const SKeywordInst &arg1, const SKeywordInst &arg2) {

		if(arg1.var < arg2.var) {
			return 1;
		}

		if(arg1.var > arg2.var) {
			return -1;
		}

		return 0;
	}

	// This is used to sort documents by the node id
	static int CompareByNodeID(const SQueryResPtr &arg1, const SQueryResPtr &arg2) {

		static SQueryRes *ptr1;
		static SQueryRes *ptr2;
		ptr1 = arg1.ptr;
		ptr2 = arg2.ptr;

		if(ptr1->node_id < ptr2->node_id) {
			return 1;
		}

		if(ptr1->node_id > ptr2->node_id) {
			return -1;
		}

		return 0;
	}

	// This is used to compare documents by checksum
	static int CompareByChecksum(const SQueryResPtr &arg1, const SQueryResPtr &arg2) {

		static SQueryRes *ptr1;
		static SQueryRes *ptr2;
		ptr1 = arg1.ptr;
		ptr2 = arg2.ptr;

		if(ptr1->keyword_score < ptr2->keyword_score) {
			return -1;
		}

		if(ptr1->keyword_score > ptr2->keyword_score) {
			return 1;
		}

		if(ptr1->keyword_check_sum < ptr2->keyword_check_sum) {
			return -1;
		}

		if(ptr1->keyword_check_sum > ptr2->keyword_check_sum) {
			return 1;
		}

		if(ptr1->check_sum < ptr2->check_sum) {
			return -1;
		}

		if(ptr1->check_sum > ptr2->check_sum) {
			return 1;
		}

		return 0;
	}

	// This is the final sorting function used to sort documents. This is 
	// done on a combination of hit score, spatial score and pulse score. 
	// Note that all of this scores have been preprocessed to break them
	// into different priority divisions.
	static int CompareTitleDoc(const SQueryResPtr &arg1, const SQueryResPtr &arg2) {

		static SQueryRes *ptr1;
		static SQueryRes *ptr2;
		ptr1 = arg1.ptr;
		ptr2 = arg2.ptr;

		if(ptr1->is_red < ptr2->is_red) {
			return 1;
		}

		if(ptr1->is_red > ptr2->is_red) {
			return -1;
		}

		if(arg1.ptr->hit_score < arg2.ptr->hit_score) {
			return -1;
		}

		if(arg1.ptr->hit_score > arg2.ptr->hit_score) {
			return 1;
		}

		if(arg1.ptr->title_div_num < arg2.ptr->title_div_num) {
			return -1;
		}

		if(arg1.ptr->title_div_num > arg2.ptr->title_div_num) {
			return 1;
		}

		if(ptr1->keyword_score < ptr2->keyword_score) {
			return -1;
		}

		if(ptr1->keyword_score > ptr2->keyword_score) {
			return 1;
		}

		return 0;
	}

	// This is the final sorting function used to sort documents. This is 
	// done on a combination of hit score, spatial score and pulse score. 
	// Note that all of this scores have been preprocessed to break them
	// into different priority divisions.
	static int CompareFinDoc(const SQueryResPtr &arg1, const SQueryResPtr &arg2) {

		static SQueryRes *ptr1;
		static SQueryRes *ptr2;
		ptr1 = arg1.ptr;
		ptr2 = arg2.ptr;

		if(ptr1->is_red < ptr2->is_red) {
			return 1;
		}

		if(ptr1->is_red > ptr2->is_red) {
			return -1;
		}

		if(arg1.ptr->hit_score < arg2.ptr->hit_score) {
			return -1;
		}

		if(arg1.ptr->hit_score > arg2.ptr->hit_score) {
			return 1;
		}

		if(ptr1->keyword_score < ptr2->keyword_score) {
			return -1;
		}

		if(ptr1->keyword_score > ptr2->keyword_score) {
			return 1;
		}

		return 0;
	}

	// This removes duplicate excerpts from the set by looking for excerpts 
	// that contain a large number of keywords in common.
	// @param set1 - the input set
	void RemoveDuplicateExcerpt(CArray<SQueryResPtr> * &set1) {

		m_keyword_set.InitializeMemoryChunk(-100);

		CSort<SQueryResPtr> sort(set1->Size(), CompareByChecksum);
		sort.HybridSort(set1->Buffer());

		for(int i=0; i<set1->Size(); i++) {

			SQueryRes &res1 = *set1->operator[](i).ptr;
			res1.duplicate_num = 0;

			while(i + 1 < set1->Size()) {

				SQueryRes &res2 = *set1->operator[](i + 1).ptr;

				if(res1.check_sum != res2.check_sum) {
					break;
				} 

				if(CKeywordSet::IsDuplicateExcerpt(m_keyword_set, res1.local_doc_id, res2.local_doc_id) == false) {
					break;
				} 

				i++;
				res2.duplicate_num = 0xFF;
				if(abs(res1.node_id.Value() - res2.node_id.Value()) > 2000) {
					res1.duplicate_num = min(res1.duplicate_num + 1, 8);
				}
			}
		}

		for(int i=0; i<set1->Size(); i++) {
			SQueryRes &res = *set1->operator[](i).ptr;	
			res.is_red = (res.duplicate_num == 0xFF);
		}
	}

	// This resolves the doc ids for the top ranking documents
	void ResolveDocIDs() {

		SOCKET sock;
		int num = m_fin_set_ptr->Size();
		CNameServer::ExpectedRewardServerInst(sock);

		static char request[20] = "DocIDLookup";
		COpenConnection::Send(sock, request, 20);
		COpenConnection::Send(sock, (char *)&num, sizeof(int));

		for(int i=0; i<m_fin_set_ptr->Size(); i++) {
			SQueryRes &res = *m_fin_set_ptr->operator[](i).ptr;
			COpenConnection::Send(sock, (char *)&res.node_id, sizeof(S5Byte));
			COpenConnection::Receive(sock, (char *)&res.doc_id, sizeof(S5Byte));
		}

		COpenConnection::CloseConnection(sock);
	}

public:

	CCompileRankedList() {
		m_excerpt_doc_buff.Initialize();
		m_keyword_set.AllocateMemory(100, -100);
	}

	// This adds a document to the set
	inline void AddDocument(SQueryRes &res) {

		if(res.check_sum == 0) {
			return;
		}

		m_excerpt_doc_buff.PushBack(res);
	}

	// This returns a reference to the last document
	inline SQueryRes &LastDoc() {
		return m_excerpt_doc_buff.LastElement();
	}

	// This returns one of the document id's for a sorted document
	// @param rank - this is the rank of the sorted document to retrieve
	// @param res - this is the doc id and score
	inline static SQueryRes &Doc(int rank) {
		return *m_fin_set_ptr->operator[](rank).ptr;
	}

	// This stores the number of results in the set 
	inline static int ResultNum() {
		return m_fin_set_ptr->Size();
	}

	// This ranks the final set of documents
	// @param max_word_div_num - this is the maximum number of query matches
	//                         - in a given document
	void RankFinalDocumentSet(int max_word_div_num) {

		uChar max_title_div = 0;
		cout<<"Found "<<m_excerpt_doc_buff.Size()<<"<BR>";
		uChar max_hit_score = m_exp_rew.CalculateExpectedReward(m_excerpt_doc_buff, max_word_div_num, max_title_div);

		//CKeywordSet::FilterKeywords();

		CArray<SKeyword> keyword_buff(90);
		CKeywordSet::AssignKeywordScoreByRank(keyword_buff);

		for(int i=0; i<keyword_buff.Size(); i++) {
			m_keyword_server.AddKeyword(keyword_buff[i]);
		}

		m_fin_set_ptr = m_keyword_server.ResolveKeywords(max_word_div_num, max_hit_score, m_excerpt_doc_buff);

		for(int i=0; i<m_fin_set_ptr->Size(); i++) {
			SQueryRes &res = *m_fin_set_ptr->operator[](i).ptr;	
			res.keyword_score = CKeywordSet::CalculateOccurKeywordScore(res.local_doc_id, 8);
		}

		RemoveDuplicateExcerpt(m_fin_set_ptr);

		CSort<SQueryResPtr> sort(m_fin_set_ptr->Size(), CompareFinDoc);
		sort.HybridSort(m_fin_set_ptr->Buffer());

		for(int i=0; i<m_fin_set_ptr->Size(); i++) {
			SQueryRes &res = *m_fin_set_ptr->operator[](i).ptr;	

			if(res.is_red == true) {
				m_fin_set_ptr->Resize(i);
				break;
			}
		}

		CSort<SQueryResPtr> sort1(m_fin_set_ptr->Size(), CompareTitleDoc);
		sort1.HybridSort(m_fin_set_ptr->Buffer());

		CKeywordSet::ResetStatistics();
		for(int i=0; i<min(KEYWORD_DOCUMENT_SIZE, m_fin_set_ptr->Size()); i++) {
			SQueryRes &res = *m_fin_set_ptr->operator[](i).ptr;
			if(res.title_div_num < max_title_div && i > 70) {
				break;
			}

			res.check_sum = CKeywordSet::UpdateGlobalKeywordOccur(res.local_doc_id, 1.0f);
		}

		RemoveDuplicateExcerpt(m_fin_set_ptr);
		
		float max_score = 0;
		for(int i=0; i<m_fin_set_ptr->Size(); i++) {
			SQueryRes &res = *m_fin_set_ptr->operator[](i).ptr;
			res.keyword_score = CKeywordSet::CalculateOccurKeywordScore(res.local_doc_id, 8);
			max_score = max(max_score, res.keyword_score);
		}

		for(int i=0; i<m_fin_set_ptr->Size(); i++) {
			SQueryRes &res = *m_fin_set_ptr->operator[](i).ptr;
			if(res.keyword_score < max_score * 0.1f) {
				res.is_red = true;
			}
		}

		CSort<SQueryResPtr> sort2(m_fin_set_ptr->Size(), CompareTitleDoc);
		sort2.HybridSort(m_fin_set_ptr->Buffer());

		for(int i=0; i<m_fin_set_ptr->Size(); i++) {
			SQueryRes &res = *m_fin_set_ptr->operator[](i).ptr;	

			if(res.is_red == true) {
				m_fin_set_ptr->Resize(i);
				break;
			}
		}

		m_fin_set_ptr->Resize(min(m_fin_set_ptr->Size(), 500));

		CSort<SQueryResPtr> sort4(m_fin_set_ptr->Size(), CompareFinDoc);
		sort4.HybridSort(m_fin_set_ptr->Buffer());

		CKeywordSet::SortKeywordsByOccurrence();
		m_fin_set_ptr->Resize(min(m_fin_set_ptr->Size(), MAX_DOCUMENT_SIZE));

		ResolveDocIDs();
	}

};
CArray<SQueryResPtr> *CCompileRankedList::m_fin_set_ptr;