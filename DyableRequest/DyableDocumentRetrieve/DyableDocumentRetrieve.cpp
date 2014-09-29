#include "../NameServer.h"

// This stores the association map file
const char *ASSOC_MAP_FILE = "GlobalData/Lexon/assoc_map";

// This class handles the retreival of a document for a particular
// document id that is supplied as part of the query process.
class CParseQuery : public CNameServerStat {

	// This stores all of the document instances when performing a lookup
	CRetrieveDocument m_doc_set;
	// This stores the tcp connection
	COpenConnection m_conn;

	// This predicate indicates whether the server is in use or not
	bool m_in_use;
	// This is a general purpose mutex
	CMutex m_mutex;

	// writes the a success code back to the client
	// @param connect - send data to connection
	static inline void WriteSuccessResponse(COpenConnection &connect) {
		char success = 's';
		connect.Send(&success, 1);
	}

	// writes the a failure code back to the client
	// @param connect - send data to connection
	static inline void WriteFailureResponse(COpenConnection &connect) {
		char failure = 'f';
		connect.Send(&failure, 1);
	}

	// Retrieves a particular document from the document
	// set with a given document id
	// @param connect - send data to connection
	void HandleQueryCase(COpenConnection &connect) {

		_int64 doc_id = 0;
		CMemoryChunk<char> doc_buff;
		connect.Receive((char *)&doc_id, 5);

		if(!m_doc_set.RetrieveDocument(doc_id, doc_buff)) {
			WriteFailureResponse(connect);
			return;
		}

		cout<<"Returning Documents"<<endl;
		if(doc_buff.OverflowSize() == 0) {
			WriteFailureResponse(connect);
		}

		WriteSuccessResponse(connect);

		int length = doc_buff.OverflowSize();
		connect.Send((char *)&length, 4);
		connect.Send(doc_buff.Buffer(), length);
	}

public:

	// This creates a document server
	CParseQuery(int server_type_id) {


		CUtility::Initialize();
		m_doc_set.Initialize("GlobalData/CoalesceDocumentSets/html_text");

		CNameServerStat::SetServerTypeID(server_type_id);
		CNameServerStat::InitializeBeacon();

		CUtility::RandomizeSeed();
		u_short listen_port = (rand() % 40000) + 10000;
		while(m_conn.OpenServerConnection(listen_port, LOCAL_NETWORK) == false) {
			listen_port = (rand() % 40000) + 10000;
		}

		CNameServer::Initialize();
		CNameServer::FreeServer(listen_port);
		m_conn.ListenOnServerConnection(INFINITE);
		
		for(int i=0; i<10000; i++) {
			try {
				COpenConnection conn(m_conn.ServerAcceptConnection());
				CNameServer::SetServerActive(conn);
				HandleQueryCase(conn);
			} catch(...) {
			}

			CNameServer::FreeServer(listen_port);
		}

		CNameServer::DestroyServer(listen_port);
	}
};

int main(int argc, char *argv[]) {


	if(argc < 2) return 0;
	int server_type_id = atoi(argv[1]);
	CParseQuery retrieve(server_type_id);

	return 0;
}