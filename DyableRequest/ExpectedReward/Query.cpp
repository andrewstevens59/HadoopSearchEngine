#include "AssignDocumentScore.h"

// This is the main entry class for a query.
// It's responsible for parsing a text string
// and breaking it down into individual word ids.
class CParseQuery : public CNodeStat {

	// This stores the tcp connection
	COpenConnection m_conn;
	// This is used to to assign a document score to 
	// each of the documents that need resolving
	CAssignDocumentScore m_doc_score;

	// This begins the query 
	void FullQuery(COpenConnection &conn) {

		CStopWatch stop;
		stop.StartTimer();

		m_doc_score.Initialize(conn);

		stop.StopTimer();
		cout<<"-------------------  Exp Reward "<<stop.GetElapsedTime()<<endl;
		conn.CloseConnection();
	}

	// This handles the query case that is conn requested
	void HandleQueryCase(COpenConnection &conn) {

		conn.Receive(CUtility::SecondTempBuffer(), 20);

		if(CUtility::FindFragment(CUtility::SecondTempBuffer(), "Query")) {
			Reset();
			FullQuery(conn);
			return;
		}

		if(CUtility::FindFragment(CUtility::SecondTempBuffer(), "DocIDLookup")) {
			m_doc_score.RetrieveDocIDs(conn);
			return;
		}
	}

public:

	CParseQuery() {
		CHDFSFile::Initialize();

		/*CByte::Initialize(256, 256);
		CNodeStat::SetClientID(0);
		CNodeStat::SetClientNum(6);

		COpenConnection conn;
		m_doc_score.Initialize(conn);*/
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

		cout<<"Expected Reward Listening "<<listen_port<<endl;
		m_conn.ListenOnServerConnection(INFINITE);

		CNameServer::FreeServer(listen_port);
		
		for(int i=0; i<CNameServer::KeepAliveTime(); i++) {
			try {
				COpenConnection conn(m_conn.ServerAcceptConnection());
				cout<<"Expected Reward Accepted A Connection "<<listen_port<<endl;
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

	CNodeStat::SetClientID(0);
	CNodeStat::SetClientNum(1);

	CParseQuery query;
	query.ProcessClientRequests (server_type_id);

    return 0;
}