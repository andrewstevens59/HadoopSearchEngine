#include "./ProcessRequest.h"

// This server is responsible for keeping track of each of the different servers
// used to perform various tasks needed to process a query. It associates a 
// listening port and an ip_address with each process. A process must first 
// publish itself to the name server so it can be contacted in fucture requests.
class CNameServer : public CProcessRequest {

	// This defines the keep alive time for a server before it is restarted
	static const int KEEP_ALIVE_TIME = 10000000;

	// This is used to process requests
	COpenConnection m_conn;

public:

	CNameServer() {
	}

	// This listens for incomming requests
	void ListenForConnections() {

		CUtility::RandomizeSeed();
		u_short listen_port = (rand() % 40000) + 10000;
		while(m_conn.OpenServerConnection(listen_port, LOCAL_NETWORK) == false) {
			listen_port = (rand() % 40000) + 10000;
		}

		CHDFSFile ns_port_file;
		ns_port_file.OpenWriteFile("GlobalData/PortInfo/ns");
		ns_port_file.WriteObject(listen_port);
		ns_port_file.WriteObject(KEEP_ALIVE_TIME);
		ns_port_file.CloseFile();

		SOCKET socket;
		m_conn.ListenOnServerConnection(INFINITE);
		cout<<"Name Server Listening For Connections"<<endl;

		Initialize();

		while(true) {
			socket = m_conn.ServerAcceptConnection();
cout<<"in&&&"<<endl;
			try {
				ProcessRequest(socket);
			} catch(...) {
			}

			COpenConnection::CloseConnection(socket);
cout<<"out&&&"<<endl;
		}
	}
};
const int CNameServer::KEEP_ALIVE_TIME;



int main(int argc, char *argv[]) {

	if(argc < 2) {
		CNameServerStat::SetServerID(0);
	} else {
		CNameServerStat::SetServerID(atoi(argv[1])); 
	}

	cout<<CNameServerStat::GetServerID()<<" ----------------------"<<endl;
	CNameServer name;
//	name.TestNameServer();
	name.ListenForConnections();
	return 0;
}