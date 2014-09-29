#include "../DocumentDatabase.h"

// This class is responsible for finding the contact information for each of the 
// different servers in order to service a given request made by a user.
class CNameServer {

	// This defines the number of name servers
	static const int NAME_SERVER_NUM = 1;

	// This stores one of the connection to an available server
	struct SAvailConn {
		// This stores the port
		u_short port;
		// This stores the ip address
		char *ip_address;
	};

	// This stores one of the connections to the server
	struct SConn {
		// This is the connection to the server
		SAvailConn conn;
		// This stores the connection to the name server
		SOCKET name_socket;
	};

	// This stores the beacon connection
	static COpenConnection m_beacon_conn;
	// This stores the name server port
	static u_short m_ns_port;
	// This stores the general purpose mutex
	static CMutex m_mutex;
	// This stores the reset predicate
	static bool m_is_reset;
	// This stores the reset port
	static int m_reset_port;
	// This sets the timeout value
	static int m_time_out;
	// This stores the keep alive time
	static int m_keep_alive_time;

	// This prints the timeout message
	static void PrintTimeOut() {

		cout<<"<center><div style=\"border: 1px solid blue;\">";
		cout<<"Sorry due to server load, Synopsis is unable to "
					"process your request at this time.<p>Please understand that Synopsis"
					" is a shared resource and hardware restrictions means that"
					" only a certain number of queries can be processed at once";
		cout<<"</div></center>";
	}

	// This retrieves a given instance of a server
	// @param conn - this is used to store the connection to the server
	// @param server_name - this is the type of server
	// @param id - this is the id of one of the retrieve servers
	//           - if the retreive servers are being used
	static void ServerInst(SOCKET &socket, const char server_name[]) {

		u_short port;
		SOCKET ns_sock;

		CStopWatch timer;
		timer.StartTimer();
		char is_accept = false;
		unsigned int uiThread1ID;

		while(true) {

			timer.StopTimer();

			if(timer.GetElapsedTime() > 2.0f) {
				PrintTimeOut();
				exit(0);
			}

			if(!COpenConnection::OpenClientConnection(ns_sock, m_ns_port, LOCAL_NETWORK)) {
				exit(0);
			}

			strcpy(CUtility::SecondTempBuffer(), server_name);
			COpenConnection::Send(ns_sock, CUtility::SecondTempBuffer(), 20);
			COpenConnection::Receive(ns_sock, (char *)&is_accept, sizeof(char));

			if(is_accept == false) {
				COpenConnection::CloseConnection(ns_sock);
				Sleep(100);
				continue;
			}

			COpenConnection::Receive(ns_sock, (char *)&port, sizeof(u_short));
			COpenConnection::CloseConnection(ns_sock);
			
			if(COpenConnection::OpenClientConnection(socket, port, LOCAL_NETWORK)) {
				COpenConnection::Receive(socket, (char *)&m_reset_port, sizeof(int));
				_beginthreadex(NULL, 0, BeaconThread2, &m_reset_port, NULL, &uiThread1ID);
				break;
			}
		
			cout<<"Can't Connect "<<server_name<<" "<<port<<endl;
			PrintTimeOut();
			exit(0);
		}
	}

	// This spawns the server keep alive thread, to notify servers 
	// that query instance is still active
	static THREAD_RETURN1 THREAD_RETURN2 BeaconThread1(void *ptr) {

		COpenConnection conn;
		conn.OpenClientUDPConnection(m_reset_port, LOCAL_NETWORK);
		char buff = 'a';

		while(true) {
			conn.SendUDP(&buff, 1);
			Sleep(300);
		}

		return 0;
	}

	// This spawns the server keep alive thread, to notify servers 
	// that query instance is still active
	static THREAD_RETURN1 THREAD_RETURN2 BeaconThread2(void *ptr) {

		COpenConnection conn;
		conn.OpenClientUDPConnection(*(int *)ptr, LOCAL_NETWORK);
		char buff = 'b';

		while(true) {
			conn.SendUDP(&buff, 1);
			Sleep(100);
		}

		return 0;
	}

	// This spawns the server keep alive thread, to notify servers 
	// that query instance is still active
	static THREAD_RETURN1 THREAD_RETURN2 ServerThread(void *ptr) {

		char buff;
		while(m_beacon_conn.ReceiveUDP(&buff, 1) >= 0) {

			m_mutex.Acquire();

			if(buff == 'a') {
				if(m_is_reset == false && ++m_time_out >= 10) {
					CHitItemBlock::SetServerInactive();
					m_is_reset = true;
					m_time_out = 0;
				}
			} else {
				m_time_out = 0;
				m_is_reset = false;
			}
			m_mutex.Release();
		}

		return 0;
	}

public:

	CNameServer() {
	}

	// This initializes the name server
	static void Initialize() {

		CHDFSFile ns_port_file;
		ns_port_file.OpenReadFile("GlobalData/PortInfo/ns");
		ns_port_file.ReadObject(m_ns_port);
		ns_port_file.ReadObject(m_keep_alive_time);
		ns_port_file.CloseFile();
		m_is_reset = true;
	}

	// This returns the keep alive time
	static inline int KeepAliveTime() {
		return m_keep_alive_time;
	}

	// This creates the server thread
	static void InitializeServerThread() {

		CUtility::RandomizeSeed();
		m_reset_port = (rand() % 40000) + 10000;

		m_time_out = 0;
		while(m_beacon_conn.OpenServerUDPConnection(m_reset_port, LOCAL_NETWORK) == false) {
			m_reset_port = (rand() % 40000) + 10000;
		}

		unsigned int uiThread1ID;
		_beginthreadex(NULL, 0, ServerThread, NULL, NULL, &uiThread1ID);

		_beginthreadex(NULL, 0, BeaconThread1, NULL, NULL, &uiThread1ID);
	}

	// This sets the server as active
	static void SetServerActive(COpenConnection &conn) {
		conn.Send((char *)&m_reset_port, sizeof(int));
		CHitItemBlock::SetServerActive();

		m_mutex.Acquire();
		m_time_out = 0;
		m_is_reset = true;
		m_mutex.Release();
	}

	// This retrieves an instance of a keyword server
	static void KeywordServerInst(SOCKET &socket) {
		ServerInst(socket, "KeywordServer");
	}

	// This retrieves an instance of a retrieve server
	static void RetrieveServerInst(SOCKET &socket) {
		ServerInst(socket, "RetrieveServer");
	}

	// This retrieves an instance of a document server
	// @param id - this is the id of the retrieve server being request
	//           - this is one of the N parallel servers
	static void DocumentServerInst(SOCKET &socket) {
		ServerInst(socket, "DocumentServer");
	}

	// This retrieves an instance of a document server
	// @param id - this is the id of the retrieve server being request
	//           - this is one of the N parallel servers
	static inline void DocumentServerInst(COpenConnection &conn) {
		DocumentServerInst(conn.Socket());
	}

	// This retrieves an instance of a text string server
	static void TextStringServerInst(SOCKET &socket) {
		ServerInst(socket, "TextStringServer");
	}

	// This retrieves an instance of a text string server
	static inline void TextStringServerInst(COpenConnection &conn) {
		TextStringServerInst(conn.Socket());
	}

	// This retrieves an instance of a text string server
	static void ExpectedRewardServerInst(SOCKET &socket) {
		ServerInst(socket, "ExpRewServer");
	}

	// This retrieves an instance of a expected reward server
	static inline void ExpectedRewardServerInst(COpenConnection &conn) {
		ExpectedRewardServerInst(conn.Socket());
	}

	// This frees a given instance of a server back to the name server
	static inline void DestroyServer(u_short port) {
		FreeServer(port, true);
	}

	// This frees a given instance of a server back to the name server
	static void FreeServer(u_short port, bool is_destroy = false) {

		COpenConnection conn;
		if(conn.OpenClientConnection(m_ns_port, LOCAL_NETWORK) == false) {
			exit(0);
		}

		int server_type_id = CNameServerStat::GetServerTypeID();

		if(is_destroy == false) {
			strcpy(CUtility::SecondTempBuffer(), "FreeServer");
		} else {
			strcpy(CUtility::SecondTempBuffer(), "DestroyServer");
		}

		conn.Send(CUtility::SecondTempBuffer(), 20);
		conn.Send((char *)&server_type_id, sizeof(int));
		conn.Send((char *)&port, sizeof(u_short));
		conn.CloseConnection();
	}

};
u_short CNameServer::m_ns_port;
CMutex CNameServer::m_mutex;
bool CNameServer::m_is_reset;
int CNameServer::m_reset_port;
int CNameServer::m_time_out;
COpenConnection CNameServer::m_beacon_conn;
int CNameServer::m_keep_alive_time;
