#include "./SearchHitItems.h"

// This is the main entry class for a query.
// It's responsible for parsing a text string
// and breaking it down into individual word ids.
class CParseQuery : public CNodeStat {

	// This is used to start searching for the different 
	// documents that have high relevance to the query
	CSearchHitItems m_search_hit_item;
	// This stores the tcp connection
	COpenConnection m_conn;
	
	// This is used for performance profiling
	CStopWatch m_timer;


	// This retrieves the word id set from the user and searches for hits that 
	// that match the query. This is done using the heuristic search.
	// @param inst_conn - this stores the connection tot the client
	void SearchForQuery(COpenConnection &inst_conn) {

		SWordItem word;
		int query_term_num;
		int unique_term_num = 0;
		inst_conn.Receive((char *)&query_term_num, sizeof(int));
		CWordDiv::WordIDSet().Initialize(query_term_num + 3);
	
		for(int j=0; j<query_term_num; j++) {
			inst_conn.Receive((char *)&word.word_id, 5);
			inst_conn.Receive((char *)&word.factor, sizeof(float));
			inst_conn.Receive((char *)&word.local_id, sizeof(uChar));
			cout<<word.word_id.Value()<<" "<<(int)word.local_id<<"   ####################"<<endl;

			CWordDiv::WordIDSet().PushBack(word);
			CWordDiv::WordIDSet().LastElement().factor = word.factor;
			unique_term_num = max(unique_term_num, word.local_id);
		}

		CWordDiv::SetTermNum(0, unique_term_num + 1);
	}

	// This performs a complete query.
	// @param inst_conn - this stores the connection tot the client
	void FullQuery(COpenConnection &inst_conn) {

		int it_num;
		int client_id = 0;
		int client_num = 1;
		inst_conn.Receive((char *)&client_id, sizeof(int));
		inst_conn.Receive((char *)&client_num, sizeof(int));

		CNodeStat::SetClientID(client_id);
		CNodeStat::SetClientNum(client_num);
		cout<<"Client ID "<<client_id<<"  ClientNum "<<client_num<<endl;

		SearchForQuery(inst_conn);


		inst_conn.Receive((char *)&it_num, sizeof(int));

		m_timer.StartTimer();
		m_search_hit_item.PerformSearch(inst_conn, it_num);
		m_timer.StopTimer();

		cout<<m_timer.GetElapsedTime()<<"  Hit Search Time "<<endl;
	}

public:

	CParseQuery() {

		/*CNodeStat::SetClientID(0);
		CNodeStat::SetClientNum(1);
		CWordDiv::WordIDSet().Initialize(6);

		CWordDiv::WordIDSet().ExtendSize(1);
		CWordDiv::WordIDSet().LastElement().word_id = 22760854 ;
		CWordDiv::WordIDSet().LastElement().factor = 1.0f;
		CWordDiv::WordIDSet().LastElement().local_id = 0;

		CWordDiv::WordIDSet().ExtendSize(1);
		CWordDiv::WordIDSet().LastElement().word_id = 111384505 ;
		CWordDiv::WordIDSet().LastElement().factor = 1.0f;
		CWordDiv::WordIDSet().LastElement().local_id = 0;

		CWordDiv::WordIDSet().ExtendSize(1);
		CWordDiv::WordIDSet().LastElement().word_id = 142225194   ;
		CWordDiv::WordIDSet().LastElement().factor = 1.0f;
		CWordDiv::WordIDSet().LastElement().local_id = 1;

		CWordDiv::WordIDSet().ExtendSize(1);
		CWordDiv::WordIDSet().LastElement().word_id = 69112264    ;
		CWordDiv::WordIDSet().LastElement().factor = 1.0f;
		CWordDiv::WordIDSet().LastElement().local_id = 1;

		CWordDiv::WordIDSet().ExtendSize(1);
		CWordDiv::WordIDSet().LastElement().word_id = 10807727   ;
		CWordDiv::WordIDSet().LastElement().factor = 1.0f;
		CWordDiv::WordIDSet().LastElement().local_id = 2;

		CWordDiv::WordIDSet().ExtendSize(1);
		CWordDiv::WordIDSet().LastElement().word_id = 98223607   ;
		CWordDiv::WordIDSet().LastElement().factor = 1.0f;
		CWordDiv::WordIDSet().LastElement().local_id = 2;

		CSpatialScore::Initialize();

		CWordDiv::SetTermNum(0, 2);

		CByte::Initialize(256, 256);

		COpenConnection conn;
		int it_num = 100000;

		m_timer.StartTimer();
		m_search_hit_item.PerformSearch(conn, it_num);
		m_timer.StopTimer();*/
	}

	// This handles the query case that is being requested
	void HandleQueryCase(COpenConnection &inst_conn) {

		inst_conn.Receive(CUtility::SecondTempBuffer(), 20);

		if(CUtility::FindFragment(CUtility::SecondTempBuffer(), "Query")) {
			cout<<"Query-------------------"<<endl;
			Reset();
			FullQuery(inst_conn);
		}
		
		inst_conn.CloseConnection();
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
		
		cout<<"Search Hit Items Listening "<<listen_port<<endl;
		m_conn.ListenOnServerConnection(INFINITE);

		CNameServer::FreeServer(listen_port);

		for(int i=0; i<CNameServer::KeepAliveTime(); i++) {
			try {
				COpenConnection conn(m_conn.ServerAcceptConnection());
				cout<<"Search Hit Items Accepted A Connection "<<listen_port<<endl;
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
		m_search_hit_item.Reset();
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
	query.ProcessClientRequests(server_type_id);

    return 0;
}