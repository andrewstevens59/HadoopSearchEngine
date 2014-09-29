#include "../../ProcessSet.h"

// This class is responsible for handling each instance of a 
// given server type. If a server is not currently available
// to handle a request, the request is placed in a queue. For
// each pending request a new server is created to handle the
// load. Once a server becomes available or an existing server
// is no longer used, a pending request is taken from the queue
// and handed the connection details of the free server.
class CServerType : public CNameServerStat {

	// This defines the set of available connections
	struct SAvailConn {
		// This stores the port
		u_short port;
		// This is a predicate indicating whether available
		bool is_avail;
	};

	// This store the set of available servers
	CMemoryChunk<SAvailConn> m_avail_set;
	// This stores the server dir
	CString m_server_dir;
	// This stores the server id 
	int m_server_id;

	// This creates a server
	void AllocateServer() {

		if(fork() == 0) {

			int pid;
			while(true) {
				if((pid = fork()) != 0) {
					waitpid(pid, NULL, 0);
				} else {

					CString arg("Index ");
					arg += m_server_id;

					cout<<"Spawn "<<m_server_dir.Buffer()<<endl;
					CProcessSet::ExecuteProcess(m_server_dir.Buffer(), arg.Buffer());
				}
			}
		}
	}

public:

	CServerType() {
	}

	// This initializes the server type. It creates a single instance
	// of a server to start with.
	// @param dir - this is the directory for the server
	void Initialize(const char dir[], int server_id, int num) {

		m_server_id = server_id;
		m_server_dir = dir;
		m_avail_set.AllocateMemory(num);
		for(int i=0; i<num; i++) {
			m_avail_set[i].is_avail = false;
			m_avail_set[i].port = 0;
		}

		for(int i=0; i<num; i++) {
			AllocateServer();
		}
	}

	// This processes an incoming request for a server
	void ProcessRequest(SOCKET &socket) {

		char is_avail = false;
		cout<<m_server_dir.Buffer()<<" ";
		for(int i=0; i<m_avail_set.OverflowSize(); i++) {
			cout<<m_avail_set[i].port<<" ";
		}
		cout<<endl;

		for(int i=0; i<m_avail_set.OverflowSize(); i++) {

			if(m_avail_set[i].is_avail == true) {
				is_avail = true;
				m_avail_set[i].is_avail = false;
				COpenConnection::Send(socket, &is_avail, sizeof(char));
				COpenConnection::Send(socket, (char *)&m_avail_set[i].port, sizeof(u_short));
				return;
			}
		}

		COpenConnection::Send(socket, &is_avail, sizeof(char));
	}

	// This sends a notification to all of the client servers
	// to indicate that the name server is still alive
	void SendNotification() {

		static COpenConnection conn;
		static CMemoryChunk<char> buff(20);
		strcpy(buff.Buffer(), "Notify");

		for(int i=0; i<m_avail_set.OverflowSize(); i++) {

			conn.OpenClientUDPConnection(m_avail_set[i].port, LOCAL_NETWORK);
			conn.SendUDP(buff.Buffer(), 20);
			conn.CloseConnection();
		}

	}
	
	// This destroys a given server and removes it's contact information from the table
	void DestroyServer(SOCKET &sock) {

		u_short port;
		COpenConnection::Receive(sock, (char *)&port, sizeof(u_short));

		for(int i=0; i<m_avail_set.OverflowSize(); i++) {

			if(m_avail_set[i].port == port) {
				m_avail_set[i].is_avail = false;
				m_avail_set[i].port = 0;
				return;
			}
		}

		cout<<"Cant Find Server (((((((((((((((((((("<<endl;
	}
	
	// This handles the freeing of the server from use. This could 
	// occur when the server is no longer being used or a new server
	// has just been created.
	void FreeServer(SOCKET &sock) {

		u_short port;
		COpenConnection::Receive(sock, (char *)&port, sizeof(u_short));
cout<<"here1"<<endl;
		for(int i=0; i<m_avail_set.OverflowSize(); i++) {

			if(m_avail_set[i].port == port || m_avail_set[i].port == 0) {
				m_avail_set[i].is_avail = true;
				m_avail_set[i].port = port;
				return;
			}
		}
cout<<"here2"<<endl;
		COpenConnection conn;
		for(int i=0; i<m_avail_set.OverflowSize(); i++) {

			if(conn.OpenClientConnection(m_avail_set[i].port, LOCAL_NETWORK) == true) {
				conn.CloseConnection();
				continue;
			}

			m_avail_set[i].port = port;
			m_avail_set[i].is_avail = true;
		}
cout<<"here"<<endl;
	}
};