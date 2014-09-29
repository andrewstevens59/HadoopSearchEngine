#include "./Associations.h"

// This stores a ptr to one of the documents
struct SQueryResPtr {
	SQueryRes *ptr;
};

// This class is responsible for creating the expected reward for a given
// set of nodes. This is accomplished by sending seed nodes to a number 
// of parallel servers and allow each individual server to calculate the
// expected reward for a subset of nodes.
class CExpRewServer {

	// This defines the number of parallel servers to use
	static const int SERVER_NUM = 4;
	// This stores the maximum number of search results
	// to send to the exp rew server
	static const int DOCUMENT_NUM = 5000;

	// This stores one of the parallel connections to the text string server
	struct SInstConn {
		// This stores a this ptr
		CExpRewServer *this_ptr;
		// This stores the local id of the term
		int id;
		// This stores the client socket
		SOCKET sock;
		// This stores the handle for the thread
		HANDLE handle;
	};

	// This stores the document gap to it's neighbour for each document 
	struct SDocumentGap {
		// This stores the ptr to the document
		SQueryRes *ptr;
		// This stores the number of documents in a give gap div
		uLong doc_num;
		// This stores the document gap
		_int64 gap;

		SDocumentGap() {
			doc_num = 0;
		}
	};

	// This stores the maximum hit score
	uChar m_max_hit_score;
	// This stores the maximum title hit score
	uChar m_max_title_div_num;
	// This stores the number of query terms
	static int m_query_term_num;

	// This stores the set of associated query terms
	CArray<SAssocPtr> m_assoc_buff;
	// This stores the final set or ranked documents
	CArray<SQueryResPtr> m_subset_buff;
	// This stores the set of connections to the text string server
	CArray<SInstConn> m_inst_conn;
	// This stores the set of final documents from which the 
	// the keyword score has been calculated
	CArray<SQueryResPtr> m_res_buff;
	// This is a general mutex
	CMutex m_mutex;

	// This is used to sort documents based on document gap
	static int CompareDocumentGap(const SDocumentGap &arg1, const SDocumentGap &arg2) {

		if(arg1.gap < arg2.gap) {
			return -1;
		}

		if(arg1.gap > arg2.gap) {
			return 1;
		}

		return 0;
	}

	// This sorts gap division by the document number
	static int CompareDocumentNum(const SDocumentGap &arg1, const SDocumentGap &arg2) {

		if(arg1.doc_num < arg2.doc_num) {
			return 1;
		}

		if(arg1.doc_num > arg2.doc_num) {
			return -1;
		}

		return 0;
	}

	// This is used to sort documents based on document gap
	static int CompareNodeID(const SDocumentGap &arg1, const SDocumentGap &arg2) {

		_int64 node1 = S5Byte::Value(arg1.ptr->node_id);
		_int64 node2 = S5Byte::Value(arg2.ptr->node_id);

		if(node1 < node2) {
			return 1;
		}

		if(node1 > node2) {
			return -1;
		}

		return 0;

		return 0;
	}

	// This is used to compare search results based on the search rank
	// this is used to reduce the final set of hits
	static int CompareSearchRank(const SQueryResPtr &arg1, const SQueryResPtr &arg2) {

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

		if(arg1.ptr->rank < arg2.ptr->rank) {
			return 1;
		}

		if(arg1.ptr->rank > arg2.ptr->rank) {
			return -1;
		}

		return 0;
	}

	// This is used to compare nodes based on the node value. Used to remove
	// nodes that are heavily clustered in one location
	static int CompareNodeID(const SQueryResPtr &arg1, const SQueryResPtr &arg2) {

		_int64 node1 = S5Byte::Value(arg1.ptr->node_id);
		_int64 node2 = S5Byte::Value(arg2.ptr->node_id);

		if(node1 < node2) {
			return 1;
		}

		if(node1 > node2) {
			return -1;
		}

		if(arg1.ptr < arg2.ptr) {
			return 1;
		}

		if(arg1.ptr > arg2.ptr) {
			return -1;
		}

		return 0;
	}

	// This initializes a client ready to process
	void InitializeClient(SOCKET &sock, int id) {

		CNameServer::ExpectedRewardServerInst(sock);

		m_mutex.Acquire();
		int node_num = m_subset_buff.Size();
		int server_num = SERVER_NUM;
		int assoc_num = m_assoc_buff.Size();
		m_mutex.Release();

		static char request[20] = "Query";
		COpenConnection::Send(sock, request, 20);
		COpenConnection::Send(sock, (char *)&node_num, sizeof(int));
		COpenConnection::Send(sock, (char *)&server_num, sizeof(int));
		COpenConnection::Send(sock, (char *)&id, sizeof(int));


		COpenConnection::Send(sock, (char *)&assoc_num, sizeof(int));
		for(int i=0; i<m_assoc_buff.Size(); i++) {
			m_mutex.Acquire();
			SAssoc *ptr = m_assoc_buff[i].ptr;
			m_mutex.Release();

			COpenConnection::Send(sock, (char *)&ptr->id, sizeof(S5Byte));
			COpenConnection::Send(sock, (char *)&ptr->weight, sizeof(float));
		}

		for(int i=0; i<m_subset_buff.Size(); i++) {

			m_mutex.Acquire();
			SQueryRes *ptr = m_subset_buff[i].ptr;
			m_mutex.Release();

			COpenConnection::Send(sock, (char *)&ptr->node_id, sizeof(S5Byte));
			COpenConnection::Send(sock, (char *)&ptr->rank, sizeof(float));
			COpenConnection::Send(sock, (char *)&ptr->title_div_num, sizeof(uChar));
			COpenConnection::Send(sock, (char *)&ptr->hit_score, sizeof(uChar));
		}
	}

	// This issues a request to the ExpRew Server.
	void IssueRequest(SOCKET &sock, int id) {

		int doc_size;
		InitializeClient(sock, id);
		COpenConnection::Receive(sock, (char *)&doc_size, sizeof(int));

		for(int i=0; i<doc_size; i++) {
			m_mutex.Acquire();
			CKeywordSet::AssignAnchorNodeKeywords(sock);
			m_mutex.Release();
		}

		m_mutex.Acquire();
		CKeywordSet::AssignSpatialKeywords(sock);
		m_mutex.Release();

		COpenConnection::CloseConnection(sock);
	}

	// This spawns a connnection to a text string server
	static THREAD_RETURN1 THREAD_RETURN2 ExpRewServerThread(void *ptr) {
		SInstConn *call = (SInstConn *)ptr;
		call->this_ptr->IssueRequest(call->sock, call->id);
		return 0;
	}

	// This parses the gap boundaries in order to assign the number of documents
	// to each gap division
	// @param buff - this stores the set of documents
	// @param temp - this stores the set of gap boundaries
	void AssignSpacedDocuments(CArray<SQueryResPtr> &buff, uLong max_div_num,
		CMemoryChunk<SDocumentGap> &temp, uChar max_hit_score) {

		int offset = 0;
		SQueryResPtr max_ptr = buff[0];
		_int64 node_bound = temp[offset++].ptr->node_id.Value();
		for(int i=0; i<buff.Size(); i++) {
			SQueryRes &res = *buff[i].ptr;

			if(res.node_id >= node_bound) {

				if(offset < min(DOCUMENT_NUM, temp.OverflowSize()) - 1) {
					node_bound = temp[offset++].ptr->node_id.Value();
				} else {
					node_bound = buff.LastElement().ptr->node_id.Value() + 1;
				}

				max_ptr = buff[i];
			}

			if(max_hit_score == 0xFF) {
				temp[offset-1].doc_num++;
			} else if(temp[offset-1].doc_num < max_div_num) {
				m_subset_buff.PushBack(buff[i]);
			}

			if(CompareSearchRank(buff[i], max_ptr) > 0) {
				max_ptr.ptr = &res;
			}
		}

		if(max_hit_score == 0xFF) {
			temp[offset-1].doc_num++;
		}
	}

	// This find the set of appropriately spaced documents to use
	// in the expected reward server.
	// @param buff - this stores the set of documents
	void FindSpacedDocuments(CArray<SQueryResPtr> &buff) {

		CMemoryChunk<SDocumentGap> temp(buff.Size());
		CSort<SQueryResPtr> sort(buff.Size(), CompareNodeID);
		sort.HybridSort(buff.Buffer());

		for(int i=0; i<buff.Size() - 1; i++) {
			temp[i].gap = abs(buff[i].ptr->node_id.Value() - buff[i+1].ptr->node_id.Value());
			temp[i].ptr = buff[i].ptr;
		}

		temp.LastElement().gap = temp.SecondLastElement().gap;
		temp.LastElement().ptr = buff.LastElement().ptr;

		CSort<SDocumentGap> sort3(temp.OverflowSize(), CompareDocumentGap);
		sort3.HybridSort(temp.Buffer());

		int num = min(DOCUMENT_NUM, temp.OverflowSize());
		CSort<SDocumentGap> sort1(num, CompareNodeID);
		sort1.HybridSort(temp.Buffer());

		AssignSpacedDocuments(buff, 0xFFFF, temp, 0xFF);

		CSort<SDocumentGap> sort2(num, CompareDocumentNum);
		sort2.HybridSort(temp.Buffer());

		uLong max_doc_num = 0;
		if(num > 100) {
			max_doc_num = temp[num - 20].doc_num;
		}

		max_doc_num = min(max_doc_num, buff.Size() / DOCUMENT_NUM);
		max_doc_num = max(max_doc_num, 25);

		sort1.HybridSort(temp.Buffer());

		AssignSpacedDocuments(buff, max_doc_num, temp, m_max_hit_score);

	}

	// This calculates the spatial rank for each document
	void CalculateSpatialRank(int start, int end, float spatial_score) {

		if(end - start < 3) {

			for(int i=start; i<end; i++) {
				m_subset_buff[i].ptr->rank = spatial_score;
			}

			return;
		}

		_int64 offset1 = m_subset_buff[start].ptr->node_id.Value();
		_int64 offset2 = m_subset_buff[end-1].ptr->node_id.Value();

		_int64 middle = (offset1 + offset2) >> 1;

		for(int i=start; i<end; i++) {

			if(m_subset_buff[i].ptr->node_id.Value() > middle) {
				int left_doc_num = i - start;
				int right_doc_num = end - i;
				spatial_score /= 10;
				CalculateSpatialRank(start, i, spatial_score + left_doc_num);
				CalculateSpatialRank(i, end, spatial_score + right_doc_num);
				return;
			}
		}
	}

public:

	CExpRewServer() {
	}

	// This sets the number of query terms
	static inline void SetQueryTermNum(int query_term_num) {
		m_query_term_num = query_term_num;
	}

	// This finds the expected reward of each document in the top ranked set
	// @param max_word_div_num - this is the maximum number of unique terms in a document
	// @return max_hit_score - this stores the maximum hit score
	uChar CalculateExpectedReward(CLinkedBuffer<SQueryRes> &excerpt_buff, int max_term_num, uChar &max_title_div) {

		SQueryResPtr ptr;
		m_max_hit_score = 0;
		m_max_title_div_num = 0;
		excerpt_buff.ResetPath();

		CArray<SQueryResPtr> buff(excerpt_buff.Size());
		while((ptr.ptr = excerpt_buff.NextNode()) != NULL) {

			if(ptr.ptr->unique_term_num >= max_term_num) {
				m_max_hit_score = max(m_max_hit_score, ptr.ptr->hit_score & 0x03);
				m_max_title_div_num = max(m_max_title_div_num, ptr.ptr->title_div_num);
				buff.PushBack(ptr);
				ptr.ptr->is_red = false;
			}
		}

		m_max_hit_score = min(m_max_hit_score, 1);
		m_max_title_div_num = min(m_max_title_div_num, 2);
		max_title_div = min(m_query_term_num, m_max_title_div_num);

		if(buff.Size() < 10) {
			return m_max_hit_score;
		}

		CAssociations::CompileAssociations(m_assoc_buff);
		m_subset_buff.Initialize(buff.Size());

		FindSpacedDocuments(buff);

		CSort<SQueryResPtr> sort(m_subset_buff.Size(), CompareNodeID);
		sort.HybridSort(m_subset_buff.Buffer());

		CalculateSpatialRank(0, m_subset_buff.Size(), 0.0f);

		CSort<SQueryResPtr> sort1(m_subset_buff.Size(), CompareSearchRank);
		sort1.HybridSort(m_subset_buff.Buffer());

		for(int i=150; i<m_subset_buff.Size(); i++) {
			SQueryRes &res = *m_subset_buff[i].ptr;
			if(res.title_div_num < m_max_title_div_num) {
				m_subset_buff.Resize(i);
				break;
			}
		}

		CSort<SQueryResPtr> sort2(m_subset_buff.Size(), CompareNodeID);
		sort2.HybridSort(m_subset_buff.Buffer());

		m_res_buff.Initialize(m_subset_buff.Size());
		m_inst_conn.Initialize(SERVER_NUM);

		unsigned int uiThread1ID;
		for(int i=0; i<m_inst_conn.OverflowSize(); i++) {
			SInstConn *ptr = m_inst_conn.ExtendSize(1);
			ptr->this_ptr = this;
			ptr->id = i;

			ptr->handle = (HANDLE)_beginthreadex(NULL, 0, 
				ExpRewServerThread, ptr, NULL, &uiThread1ID);
		}

		for(int i=0; i<m_inst_conn.Size(); i++) {
			WaitForThread(m_inst_conn[i].handle, INFINITE);
		}

		return m_max_hit_score;
	}
};
int CExpRewServer::m_query_term_num;