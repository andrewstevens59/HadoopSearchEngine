#include "./ExpRewServer.h"

// This is used to contact the keyword server in order to resolve the set
// of keywords for each document. Once the keyword set has been resolved,
class CKeywordServer {

	// This defines the number of parallel servers to use
	static const int SERVER_NUM = 4;

	// This stores one of the parallel connections to the text string server
	struct SInstConn {
		// This stores a this ptr
		CKeywordServer *this_ptr;
		// This stores the local id of the term
		int id;
		// This stores the client socket
		SOCKET sock;
		// This stores the handle for the thread
		HANDLE handle;
	};

	// This stores the set of connections to the text string server
	CArray<SInstConn> m_inst_conn;
	// This is used to store the result set sent to the server
	CArray<SQueryResPtr> m_res_buff;
	// This stores the set of keywords
	CArrayList<SKeyword> m_keyword_buff;
	// This stores the current doc id
	int m_doc_id;
	// This is a general purpose mutex
	CMutex m_mutex;

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

		return 0;
	}

	// This issues a request to the Keyword Server.
	void IssueRequest(SOCKET &sock, int id) {

		CNameServer::KeywordServerInst(sock);

		m_mutex.Acquire();

		SBoundary bound(0, m_res_buff.Size());
		CHashFunction::BoundaryPartion(id, SERVER_NUM, bound.end, bound.start);

		int width = bound.Width();
		COpenConnection::Send(sock, (char *)&width, sizeof(int));

		for(int i=bound.start; i<bound.end; i++) {
			COpenConnection::Send(sock, (char *)&m_res_buff[i].ptr->node_id, sizeof(S5Byte));
		}

		int num = m_keyword_buff.Size();
		COpenConnection::Send(sock, (char *)&num, sizeof(int));
		for(int i=0; i<m_keyword_buff.Size(); i++) {
			uChar occur = m_keyword_buff[i].occur;
			COpenConnection::Send(sock, (char *)&m_keyword_buff[i].keyword_id, sizeof(S5Byte));
			COpenConnection::Send(sock, (char *)&occur, sizeof(uChar));
		}

		m_mutex.Release();

		for(int i=bound.start; i<bound.end; i++) {

			SQueryRes &res = *m_res_buff[i].ptr;
			COpenConnection::Receive(sock, (char *)&res.node_id, sizeof(S5Byte));

			m_mutex.Acquire();
			res.keyword_check_sum = CKeywordSet::AssignKeywordSet(sock, res.node_id);
			res.local_doc_id = m_doc_id++;
			res.is_red = false;
			m_mutex.Release();
		}
	}

	// This spawns a connnection to a text string server
	static THREAD_RETURN1 THREAD_RETURN2 KeywordServerThread(void *ptr) {
		SInstConn *call = (SInstConn *)ptr;
		call->this_ptr->IssueRequest(call->sock, call->id);
		return 0;
	}

public:

	CKeywordServer() {
		m_keyword_buff.Initialize(20);
		m_doc_id = 0;
	}

	// This adds a high ranking keyword to the set
	inline void AddKeyword(SKeyword &keyword) {
		m_keyword_buff.PushBack(keyword);
	}

	// This finds the expected reward of each document in the top ranked set
	// @param max_word_div_num - this is the maximum number of unique terms in a document
	// @param max_hit_score - this stores the maximum hit score
	CArray<SQueryResPtr> *ResolveKeywords(int max_word_div_num,
		uChar max_hit_score, CLinkedBuffer<SQueryRes> &excerpt_buff) {

		CKeywordSet::ResetKeywordSet();
		m_res_buff.Initialize(excerpt_buff.Size());

		excerpt_buff.ResetPath();
		for(int i=0; i<excerpt_buff.Size(); i++) {
			SQueryRes *ptr = excerpt_buff.NextNode();
			if(ptr->unique_term_num >= max_word_div_num) {
				m_res_buff.ExtendSize(1);
				m_res_buff.LastElement().ptr = ptr;
			}
		}

		CSort<SQueryResPtr> sort(m_res_buff.Size(), CompareNodeID);
		sort.HybridSort(m_res_buff.Buffer());

		m_inst_conn.Initialize(SERVER_NUM);

		unsigned int uiThread1ID;
		for(int i=0; i<m_inst_conn.OverflowSize(); i++) {
			SInstConn *ptr = m_inst_conn.ExtendSize(1);
			ptr->this_ptr = this;
			ptr->id = i;

			ptr->handle = (HANDLE)_beginthreadex(NULL, 0, 
				KeywordServerThread, ptr, NULL, &uiThread1ID);
		}

		for(int i=0; i<m_inst_conn.Size(); i++) {
			WaitForThread(m_inst_conn[i].handle, INFINITE);
		}

		return &m_res_buff;
	}
};