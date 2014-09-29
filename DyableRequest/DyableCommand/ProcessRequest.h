#include "./ServerType.h"

// This class handles the processing of a connecting client's request.
// It returns the connection details of the various servers to a client
// so the client can contact the server directly. If there is no available
// server for which to service the request than it is created. This must
// be done before the connection details can be handed back to the client.
class CProcessRequest : public CNameServerStat {

	// This defines the number of retrieve servers to run in parallel
	static const int RETRIEVE_SERVER_NUM = 5;
	// This defines the number of expected reward servers to run in parallel
	static const int EXPECTED_SERVER_NUM = 4;
	// This defines the number of document servers to run in parallel
	static const int DOCUMENT_SERVER_NUM = 2;
	// This defines the number of document servers to run in parallel
	static const int TEXT_STRING_SERVER_NUM = 2;
	// This defines the number of keyword servers to run in parallel
	static const int KEYWORD_SERVER_NUM = 4;
	// This defines the number of name servers
	static const int NAME_SERVER_NUM = 1;

	// This stores the last N number of allocated servers
	CCyclicArray<int> m_server_stat;
	// This stores the number of allocated servers
	int m_allocated_server_num;
	// This stores the number of existing servers
	int m_total_server_num;

	// This stores a ptr to each of the different server types
	CMemoryChunk<CServerType *> m_server_type;
	// This stores the set of servers for TextStringServer
	CServerType m_text_string_server;
	// This stores the set of servers for DocumentServer
	CServerType m_doc_server;
	// This stores the set of expected reward servers
	CServerType m_exp_rew_server;
	// This stores the set of servers for RetrieveServer
	CServerType m_retrieve_server;
	// This stores the set of servers for KeywordServer
	CServerType m_keyword_server;
	// This stores the name server timeout
	int m_ns_timeout;
	// This protects access to shared variables
	CMutex m_mutex;

	// This stores the keep alive conn
	COpenConnection m_alive_conn;

	// This checks for notifications given by the NameServer 
	// to verify that the NameServer is still alive. It
	// will terminate if the name server is not alive
	static THREAD_RETURN1 THREAD_RETURN2 NotifyThread(void *ptr1) {

		CMemoryChunk<char> buff(20);
		CProcessRequest *this_ptr = (CProcessRequest *)ptr1;
		sprintf(buff.Buffer(), "a 0 100");

		COpenConnection conn;

		while(true) {

			conn.OpenClientUDPConnection(40000, LOCAL_NETWORK);
			conn.SendUDP(buff.Buffer(), 20);
			conn.CloseConnection();

			this_ptr->m_mutex.Acquire();
			this_ptr->m_ns_timeout++;
			this_ptr->m_text_string_server.SendNotification();
			this_ptr->m_doc_server.SendNotification();
			this_ptr->m_exp_rew_server.SendNotification();
			this_ptr->m_retrieve_server.SendNotification();
			this_ptr->m_keyword_server.SendNotification();

			this_ptr->m_mutex.Release();

			Sleep(100);
		}

		return 0;
	}

public:

	CProcessRequest() {
	}

	void Initialize() {

		int offset = 0;
		m_ns_timeout = 0;
		CNodeStat::SetClientNum(NAME_SERVER_NUM);
		m_server_type.AllocateMemory(5);

		m_text_string_server.Initialize("../DyableTextStringServer/"
			"Debug/DyableTextStringServer.exe", offset, TEXT_STRING_SERVER_NUM);
		m_server_type[offset++] = &m_text_string_server;

		m_doc_server.Initialize("../DyableDocumentRetrieve/"
			"Debug/DyableDocumentRetrieve.exe", offset, DOCUMENT_SERVER_NUM);
		m_server_type[offset++] = &m_doc_server;

		m_exp_rew_server.Initialize("../ExpectedReward/"
			"Debug/ExpectedReward.exe", offset, EXPECTED_SERVER_NUM);
		m_server_type[offset++] = &m_exp_rew_server;

		m_retrieve_server.Initialize("../SearchHitItems/"
			"Debug/SearchHitItems.exe", offset, RETRIEVE_SERVER_NUM);
		m_server_type[offset++] = &m_retrieve_server;

		m_keyword_server.Initialize("../SearchKeywords/"
			"Debug/SearchKeywords.exe", offset, KEYWORD_SERVER_NUM);
		m_server_type[offset++] = &m_keyword_server;
	}

	// This process a request issued by a query
	void ProcessRequest(SOCKET &socket) {

		int server_type_id;
		CMemoryChunk<char> buff(20);
		COpenConnection::Receive(socket, buff.Buffer(), 20);

		cout<<buff.Buffer()<<endl;
		if(CUtility::FindFragment(buff.Buffer(), "RetrieveServer")) {
			m_retrieve_server.ProcessRequest(socket);
		}

		if(CUtility::FindFragment(buff.Buffer(), "TextStringServer")) {
			m_text_string_server.ProcessRequest(socket);
		}

		if(CUtility::FindFragment(buff.Buffer(), "DocumentServer")) {
			m_doc_server.ProcessRequest(socket);
		}

		if(CUtility::FindFragment(buff.Buffer(), "ExpRewServer")) {
			m_exp_rew_server.ProcessRequest(socket);
		}

		if(CUtility::FindFragment(buff.Buffer(), "KeywordServer")) {
			m_keyword_server.ProcessRequest(socket);
		}

		if(CUtility::FindFragment(buff.Buffer(), "FreeServer")) {
			COpenConnection::Receive(socket, (char *)&server_type_id, sizeof(int));
			m_server_type[server_type_id]->FreeServer(socket);
		}

		if(CUtility::FindFragment(buff.Buffer(), "DestroyServer")) {
			COpenConnection::Receive(socket, (char *)&server_type_id, sizeof(int));
			m_server_type[server_type_id]->DestroyServer(socket);
		}
	}

};