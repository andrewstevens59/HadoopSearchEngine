#include "./CompileRankedList.h"

// This class is responsible for contacting a number of the retrieve servers 
// in parllel in order to process a given request made by the user. The
// retrieve servers can only be contacted once the word id set is known.
class CRetrieveServer : public CNodeStat {

	// This stores the maximum number of retrieve servers allowed per query
	static const int MAX_SERVER_NUM = 0;
	// This stores the maximum number of iterations allowed in between search passes
	static const int MAX_SEARCH_IT = 200000;
	// This defines the maximum number of search passes
	static const int MAX_SEARCH_PASS_NUM = 1;
	// This defines the number of retrieve servers
	static const int RETRIEVE_SERVER_NUM = 5;

	// This stores one of the parallel connections to the retrieve server
	struct SRetrieveConn {
		// This stores a this ptr
		CRetrieveServer *this_ptr;
		// This stores the retrieve id
		int id;
		// This stores the handle for the thread
		HANDLE handle;
		// This stores the socket of the child server
		// if it is not the parent
		SOCKET child_sock;
	};

	// This stores the set of word ids
	static CArrayList<SWordItem> m_word_id_set;
	// This stores the set of active retrieve servers
	static CLinkedBuffer<SRetrieveConn> m_pend_server_set;
	// This stores the set of retrieved documents
	static CCompileRankedList m_ranked_list;
	// This protects access to the result queue
	CMutex m_mutex;

	// This stores the maximum number of terms found 
	// belonging to a particular document
	int m_max_word_div_num;
	// This is the number of query terms
	int m_query_term_num;

	// This sends the query terms to the server
	void SendQueryTerms(SOCKET &socket) {


		int query_term_num = CRetrieveServer::WordIDSet().Size();
		COpenConnection::Send(socket, (char *)&query_term_num, 4);

		for(int j=0; j<CRetrieveServer::WordIDSet().Size(); j++) {
			SWordItem &word = CRetrieveServer::WordIDSet()[j];
			COpenConnection::Send(socket, (char *)&word.word_id, sizeof(S5Byte));
			COpenConnection::Send(socket, (char *)&word.factor, sizeof(float));
			COpenConnection::Send(socket, (char *)&word.local_id, sizeof(uChar));
		}
	}

	// This is called to subdivide a retrieve server to increase the level
	// of parallelism in the search strategy.
	void ServerInst(SOCKET &socket, int client_id) {

		char is_divide;
		int max_word_div_num;
		int search_pass_num = 0;
		int active_serv_num = 0;

		COpenConnection::Send(socket, (char *)&MAX_SEARCH_IT, sizeof(int));
		
		while(true) {
			COpenConnection::Receive(socket, (char *)&max_word_div_num, sizeof(int));
			if(max_word_div_num < 0) {
				COpenConnection::Receive(socket, (char *)&max_word_div_num, sizeof(int));
				m_mutex.Acquire();
				m_max_word_div_num = max(m_max_word_div_num, max_word_div_num);
				m_mutex.Release();
				break;
			}

			m_mutex.Acquire();
			m_max_word_div_num = max(m_max_word_div_num, max_word_div_num);
			m_mutex.Release();
			
			if(++search_pass_num >= MAX_SEARCH_PASS_NUM) {
				max_word_div_num = -1;
				COpenConnection::Send(socket, (char *)&max_word_div_num, sizeof(int));
				break;
			}

			m_mutex.Acquire();
			COpenConnection::Send(socket, (char *)&m_max_word_div_num, sizeof(int));
			m_mutex.Release();

			is_divide = (active_serv_num < MAX_SERVER_NUM) ? 'd' : 'f';
			COpenConnection::Send(socket, &is_divide, sizeof(char));

			if(is_divide == 'd') {
				active_serv_num++;
				CreateChildServerInst(socket, client_id);
			} 
		}

		RetrieveDocuments(socket, client_id);

		COpenConnection::CloseConnection(socket);
	}

	// This is used to spawn one of the document instances
	// to retreive a document
	static THREAD_RETURN1 THREAD_RETURN2 SubdividServersThread(void *ptr) {
		SRetrieveConn *call = (SRetrieveConn *)ptr;

		call->this_ptr->ServerInst(call->child_sock, call->id);

		return 0;
	}

	// This transfers data from the parent search server to the child search server
	void TransferData(SOCKET &parent_sock, SOCKET &child_sock) {

		uChar hit_seg_num;
		SHitSegment hit_seg;
		u_short tree_id;
		uChar tree_level;

		SABNode header;
		_int64 byte_offset;
		S64BitBound node_bound;

		int node_num;
		COpenConnection::Receive(parent_sock, (char *)&node_num, sizeof(int));
		COpenConnection::Send(child_sock, (char *)&node_num, sizeof(int));

		for(int i=0; i<node_num; i++) {
	
			COpenConnection::Receive(parent_sock, (char *)&tree_id, sizeof(u_short));
			COpenConnection::Receive(parent_sock, (char *)&tree_level, sizeof(uChar));
			COpenConnection::Receive(parent_sock, (char *)&byte_offset, sizeof(_int64));
			COpenConnection::Receive(parent_sock, (char *)&header, sizeof(SABNode));
			COpenConnection::Receive(parent_sock, (char *)&node_bound, sizeof(S64BitBound));
	
			COpenConnection::Send(child_sock, (char *)&tree_id, sizeof(u_short));
			COpenConnection::Send(child_sock, (char *)&byte_offset, sizeof(_int64));
			COpenConnection::Send(child_sock, (char *)&header, sizeof(SABNode));
			COpenConnection::Send(child_sock, (char *)&node_bound, sizeof(S64BitBound));
			COpenConnection::Send(child_sock, (char *)&tree_level, sizeof(uChar));

			COpenConnection::Receive(parent_sock, (char *)&hit_seg_num, sizeof(uChar));
			COpenConnection::Send(child_sock, (char *)&hit_seg_num, sizeof(uChar));
				
			for(uChar k=0; k<hit_seg_num; k++) {
				COpenConnection::Receive(parent_sock, (char *)&hit_seg, sizeof(SHitSegment));
				COpenConnection::Send(child_sock, (char *)&hit_seg, sizeof(SHitSegment));
			}
		}
	}

	// This is used to create a new server instance
	void CreateChildServerInst(SOCKET &socket, int client_id) {

		m_mutex.Acquire();
		SRetrieveConn *ptr = m_pend_server_set.ExtendSize(1);
		ptr->this_ptr = this;
		ptr->id = client_id;
		m_mutex.Release();

		CNameServer::RetrieveServerInst(ptr->child_sock);

		unsigned int uiThread1ID;
		const char command[20] = "Seed";
		COpenConnection::Send(ptr->child_sock, command, 20);

		m_mutex.Acquire();
		COpenConnection::Send(ptr->child_sock, (char *)&client_id, sizeof(int));
		COpenConnection::Send(ptr->child_sock, (char *)&RETRIEVE_SERVER_NUM, sizeof(int));
		SendQueryTerms(ptr->child_sock);
		COpenConnection::Send(ptr->child_sock, (char *)&m_max_word_div_num, sizeof(int));

		m_mutex.Release();

		TransferData(socket, ptr->child_sock);

		ptr->handle = (HANDLE)_beginthreadex(NULL, 0, 
			SubdividServersThread, ptr, NULL, &uiThread1ID);
	}

	// This retrieves the set of documents and keywords from a retrieve server
	void RetrieveDocuments(SOCKET &socket, int client_id) {

		SQueryRes res;	
		int set_num = 0;
		COpenConnection::Receive(socket, (char *)&set_num, sizeof(int));
		res.is_red = false;

		for(int j=0; j<set_num; j++) {
			COpenConnection::Receive(socket, (char *)&res.unique_term_num, sizeof(uChar));
			COpenConnection::Receive(socket, (char *)&res.title_div_num, sizeof(uChar));
			COpenConnection::Receive(socket, (char *)&res.node_id, sizeof(S5Byte));
			COpenConnection::Receive(socket, (char *)&res.hit_score, sizeof(uChar));
			COpenConnection::Receive(socket, (char *)&res.check_sum, sizeof(uLong));

			m_mutex.Acquire();
			m_ranked_list.AddDocument(res);
			m_mutex.Release();
		}
	}

	// This sends a query to the retrieve server for a particular 
	// client. The results are processed and stored in the priority queue.
	// @param client_id - this is the id of the parallel client
	// @param word_id_set - this stores the set of query terms
	void SendQuery(SRetrieveConn *ptr) {

		CNameServer::RetrieveServerInst(ptr->child_sock);
		
		const char command[20] = "Query";
		COpenConnection::Send(ptr->child_sock, command, 20);
		COpenConnection::Send(ptr->child_sock, (char *)&ptr->id, sizeof(int));
		COpenConnection::Send(ptr->child_sock, (char *)&RETRIEVE_SERVER_NUM, sizeof(int));

		SendQueryTerms(ptr->child_sock);
		ServerInst(ptr->child_sock, ptr->id);
	}

	// This is used to spawn one of the document instances
	// to retreive a document
	static THREAD_RETURN1 THREAD_RETURN2 RetrieveResThread(void *ptr) {
		SRetrieveConn *call = (SRetrieveConn *)ptr;
		call->this_ptr->SendQuery(call);

		return 0;
	}

public:

	CRetrieveServer() {
		m_max_word_div_num = 0;
	}

	// This returns the set of query terms
	static CArrayList<SWordItem> &WordIDSet() {
		return m_word_id_set;
	}

	// This returns one of the document id's for a sorted document
	// @param rank - this is the rank of the sorted document to retrieve
	// @param res - this is the doc id and score
	inline static SQueryRes &Doc(int rank) {
		return m_ranked_list.Doc(rank);
	}

	// This returns one of the document id's for a sorted document
	// @param rank - this is the rank of the sorted document to retrieve
	// @param res - this is the doc id and score
	inline static int ResultNum() {
		return m_ranked_list.ResultNum();
	}

	//	This hands off a request to each of the parallel retrieve servers
	void ProcessQuery(int query_term_num) {

		unsigned int uiThread1ID;
		m_query_term_num = query_term_num;
		m_pend_server_set.Initialize(30);
		CKeywordSet::Initialize();

		for(int i=0; i<RETRIEVE_SERVER_NUM; i++) {
			SRetrieveConn *ptr = m_pend_server_set.ExtendSize(1);
			ptr->id = i;
			ptr->this_ptr = this;

			ptr->handle = (HANDLE)_beginthreadex(NULL, 0, 
				RetrieveResThread, ptr, NULL, &uiThread1ID);
		}

		SRetrieveConn *ptr;
		m_pend_server_set.ResetPath();
		while((ptr = m_pend_server_set.NextNode()) != NULL) {
			WaitForThread(ptr->handle, INFINITE);
		}

		m_ranked_list.RankFinalDocumentSet(m_max_word_div_num);
	}

};
const int CRetrieveServer::MAX_SERVER_NUM;
const int CRetrieveServer::MAX_SEARCH_IT;
const int CRetrieveServer::MAX_SEARCH_PASS_NUM;
const int CRetrieveServer::RETRIEVE_SERVER_NUM;

CArrayList<SWordItem> CRetrieveServer::m_word_id_set;
CLinkedBuffer<CRetrieveServer::SRetrieveConn> CRetrieveServer::m_pend_server_set;
CCompileRankedList CRetrieveServer::m_ranked_list;