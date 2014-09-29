#include "./TextIDLookup.h"

// This class takes request from connecting clients
// to perform a lookup request for a particular
// word or url text string or id request.
class CParseQuery : public CNameServerStat {

	// This finds the word id or a set of similar words
	CRetrieveSimilarWordID m_sim_word;
	// This finds the text string for a given word id
	CRetrieveTextString m_text_string;
	// This stores the tcp connection
	COpenConnection m_conn;

	// This process the request for the connecting client
	void HandleQueryCase(COpenConnection &accept) {

		accept.Receive(CUtility::SecondTempBuffer(), 20);
		cout<<"Request "<<CUtility::SecondTempBuffer()<<endl;
		if(CUtility::FindFragment(CUtility::SecondTempBuffer(), "SimilarWord")) {
			m_sim_word.WordIDRequest(accept);
		}

		if(CUtility::FindFragment(CUtility::SecondTempBuffer(), "WordIDRequest")) {
			m_text_string.ProcessRequest(accept);
		}

		accept.CloseConnection();
	}

public:

	// This starts the server
	CParseQuery(int server_type_id) {

		m_sim_word.Initialize();
		m_text_string.Initialize();

		CNameServerStat::SetServerTypeID(server_type_id);
		CNameServerStat::InitializeBeacon();

		CUtility::RandomizeSeed();
		u_short listen_port = (rand() % 40000) + 10000;
		while(m_conn.OpenServerConnection(listen_port, LOCAL_NETWORK) == false) {
			listen_port = (rand() % 40000) + 10000;
		}

		CNameServer::Initialize();
		CNameServer::InitializeServerThread();

		cout<<"Text String Server Listening "<<listen_port<<endl;
		m_conn.ListenOnServerConnection(INFINITE);

		CNameServer::FreeServer(listen_port);
		
		while(true) {

			try {
				COpenConnection conn(m_conn.ServerAcceptConnection());
				CNameServer::SetServerActive(conn);
				HandleQueryCase(conn);
			} catch(...) {
			}

			CNameServer::FreeServer(listen_port);
		}
	}
};

int main(int argc, char *argv[]) {

	/*freopen("move.txt", "w", stdout);

	CNodeStat::SetHashDivNum(256);
	CRetrieveSimilarWordID se;

	char *words[] = {"shockley", "facts", "biography", "geography", "education", "government", 
		"description", "summary", "overview", "generally", "definition", 
		"statistics", "development", "discussion"};

	for(int i=0; i<6; i++) {
		se.TestRetrieveSimilarWordID(words[i]);
	}getchar();*/

	if(argc < 2) return 0;
	int server_type_id = atoi(argv[1]);
	CNodeStat::SetHashDivNum(256);

	CParseQuery retrieve(server_type_id);
	
	return 0;
}
