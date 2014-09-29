#include "../NameServer.h"

// This stores one of the retrieved results
struct SFindSimRes {
	// This stores the doc id
	S5Byte doc_id;
	// This stores the node id
	S5Byte node_id;
	// This stores the number of retrieved word ids
	uChar word_id_num;
	// This stores the word id set
	S5Byte word_id_set[30];
};

// This class is responsible for contacting a number of the retrieve servers 
// in parllel in order to process a given request made by the user. The
// retrieve servers can only be contacted once the word id set is known.
class CRetrieveServer : public CNodeStat {

	// This stores one of the parallel connections to the retrieve server
	struct SRetrieveConn {
		// This stores a this ptr
		CRetrieveServer *this_ptr;
		// This stores the retrieve id
		_int64 doc_id;
		// This stores the retrieve id
		int id;
		// This stores the handle for the thread
		HANDLE handle;
	};

	// This stores the set of results
	CArray<SFindSimRes> m_res_buff;
	// This protects access to the result queue
	CMutex m_mutex;

	// This sends a query to the retrieve server for a particular 
	// client. The results are processed and stored in the priority queue.
	// @param client_id - this is the id of the parallel client
	// @param word_id_set - this stores the set of query terms
	void SendQuery(_int64 doc_id, int id) {

		COpenConnection conn;
		CNameServer::RetrieveServerInst(conn, id);

		const char command[20] = "FindSimilar";
		conn.Send(command, 20);
		conn.Send((char *)&doc_id, sizeof(S5Byte));
		
		int set_num;
		conn.Receive((char *)&set_num, sizeof(int));

		for(int j=0; j<set_num; j++) {
			m_mutex.Acquire();
			SFindSimRes *ptr = m_res_buff.ExtendSize(1);
			conn.Receive((char *)&ptr->doc_id, sizeof(S5Byte));
			conn.Receive((char *)&ptr->node_id, sizeof(S5Byte));
			conn.Receive((char *)&ptr->word_id_num, sizeof(uChar));

			for(int i=0; i<ptr->word_id_num; i++) {
				conn.Receive((char *)&ptr->word_id_set[i], sizeof(S5Byte));
			}

			m_mutex.Release();
		}

		conn.CloseConnection();
	}

	// This is used to spawn one of the document instances
	// to retreive a document
	static unsigned __stdcall RetrieveResThread(void *ptr) {
		SRetrieveConn *call = (SRetrieveConn *)ptr;
		call->this_ptr->SendQuery(call->doc_id, call->id);

		return 0;
	}

public:

	CRetrieveServer() {
	}

	//	This hands off a request to each of the parallel retrieve servers
	void ProcessQuery(_int64 doc_id) {

		unsigned int uiThread1ID;
		CMemoryChunk<SRetrieveConn> conn_buff(GetClientNum());
		m_res_buff.Initialize(GetClientNum() * 100);

		for(int i=0; i<GetClientNum(); i++) {
			conn_buff[i].doc_id = doc_id;
			conn_buff[i].id = i;
			conn_buff[i].this_ptr = this;

			conn_buff[i].handle = (HANDLE)_beginthreadex(NULL, 0, 
				RetrieveResThread, &conn_buff[i], NULL, &uiThread1ID);
		}

		for(int i=0; i<GetClientNum(); i++) {
			WaitForSingleObject(conn_buff[i].handle, INFINITE);
		}
	}

	// This returns a reference to the result buffer
	CArray<SFindSimRes> &ResBuff() {
		return m_res_buff;
	}

};