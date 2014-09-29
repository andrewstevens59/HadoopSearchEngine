#include "./SearchKeywords.h"

// This is the main entry class for a query.
// It's responsible for parsing a text string
// and breaking it down into individual word ids.
class CParseQuery : public CNodeStat {

	// This stores the tcp connection
	COpenConnection m_conn;
	// This is used to compile the keyword set for each document
	CSearchKeywords m_keywords;
	// This is used for performance profiling
	CStopWatch m_timer;

	// This predicate indicates whether the server is in use or not
	bool m_in_use;
	// This is a general purpose mutex
	CMutex m_mutex;

	// This performs a complete query.
	// @param inst_conn - this stores the connection tot the client
	void HandleQueryCase(COpenConnection &inst_conn) {


		m_keywords.Initialize(inst_conn);

		m_keywords.PerformSearch();
		m_keywords.CompileSearchResults(inst_conn);
	}

public:

	CParseQuery() {
	}

	// This is the entry function that processes requests from
	// connecting clients.
	void ProcessClientRequests(int server_type_id) {

		CNameServerStat::SetServerTypeID(server_type_id);
		CNameServerStat::InitializeBeacon();
		CByte::Initialize(256, CNodeStat::GetHashDivNum());

		CUtility::RandomizeSeed();
		u_short listen_port = (rand() % 40000) + 10000;
		while(m_conn.OpenServerConnection(listen_port, LOCAL_NETWORK) == false) {
			listen_port = (rand() % 40000) + 10000;
		}

		CNameServer::Initialize();
		CNameServer::InitializeServerThread();

		cout<<"Search Keywords Listening "<<listen_port<<endl;
		m_conn.ListenOnServerConnection(INFINITE);

		CNameServer::FreeServer(listen_port);
		
		for(int i=0; i<CNameServer::KeepAliveTime(); i++) {
			try {
				COpenConnection conn(m_conn.ServerAcceptConnection());
				cout<<"Search Keywords Accepted A Connection "<<listen_port<<endl;
				CNameServer::SetServerActive(conn);
				HandleQueryCase(conn);
			} catch(...) {
			}

			CNameServer::FreeServer(listen_port);
		}

		CNameServer::DestroyServer(listen_port);
	}

	// This resets the entire system ready for the next query
	void Reset() {
		cout<<"Query "<<m_timer.GetElapsedTime()<<endl;
		CHitItemBlock::BeginNewQuery();
	}
};

// Main program entry point
int main(int argc, char* argv[])
{

	if(argc < 2) {
		return 0;
	} 

	if(argc < 2) return 0;
	int server_type_id = atoi(argv[1]);
	CNodeStat::SetHashDivNum(256);

	CParseQuery query;
	query.ProcessClientRequests (server_type_id);

    return 0;
}