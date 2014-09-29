#include <mpi.h>
#include <sys/time.h>
#include <sys/types.h>
#include <ifaddrs.h>
#include <netinet/in.h> 
#include <arpa/inet.h>

#include "../ProcessSet.h"

int tag = 42;

// This class acts as a slave process that accepts execution requests
// for a particular process and a set of command line arguments
class CSlave {

	// This stores the total elapsed time
	double m_elap_time;
	// This stores the server connection
	COpenConnection m_serv_conn;

	// This processes incominng requests from the master
	void ProcessMasterRequests(COpenConnection &conn) {

		pid_t pid;
		int rank;
		int len;
		int arg_offset;
	
		CMemoryChunk<char> buff(4096);
		MPI_Comm_rank(MPI_COMM_WORLD, &rank);
		struct timeval time1;
		struct timeval time2;

		while(true) {		
			conn.Receive((char *)&len, sizeof(int));

			if(len < 0) {
				if(len == -3) {
					exit(0);
				}
				conn.Send((char *)&m_elap_time, sizeof(double));
				continue;
			}

			if(len >= buff.OverflowSize()) {
				buff.AllocateMemory(len);
			}


			conn.Receive((char *)&arg_offset, sizeof(int));
			conn.Receive(buff.Buffer(), len);

			gettimeofday(&time1, NULL);

			if((pid = fork()) != 0) {
				int status;
				waitpid(pid, &status, 0);

			} else {
				CProcessSet::ExecuteProcess(buff.Buffer(), buff.Buffer() + arg_offset);
			}

			gettimeofday(&time2, NULL);

			m_elap_time += time2.tv_sec - time1.tv_sec;
			m_elap_time += (time2.tv_usec - time1.tv_usec) / 1000000;

			conn.Send((char *)&rank, sizeof(int));
		}
	}

public:

	CSlave() {

		m_elap_time = 0;
		char ip_addr[INET_ADDRSTRLEN];
		struct ifaddrs * ifAddrStruct = NULL;
		struct ifaddrs * ifa = NULL;
		void * tmpAddrPtr = NULL;
		int rank;
		int port;
		
		MPI_Comm_rank(MPI_COMM_WORLD, &rank);
		getifaddrs(&ifAddrStruct);

		for (ifa = ifAddrStruct; ifa != NULL; ifa = ifa->ifa_next) {
			if (ifa ->ifa_addr->sa_family==AF_INET) { // check it is IP4
				tmpAddrPtr=&((struct sockaddr_in *)ifa->ifa_addr)->sin_addr;
				inet_ntop(AF_INET, tmpAddrPtr, ip_addr, INET_ADDRSTRLEN);
		
				if(strcmp(ifa->ifa_name, "ib0") == 0) {
					break;
				}
			} 
		}
		if (ifAddrStruct!=NULL) {
			freeifaddrs(ifAddrStruct);
		}

		CUtility::RandomizeSeed();
		port = (rand() % (0xFF - 5000)) + 5000 + rank; 
		while(!m_serv_conn.OpenServerConnection(port, LOCAL_NETWORK)) {
			port = (rand() % (0xFF - 5000)) + 5000 + rank;
		}
		
		m_serv_conn.ListenOnServerConnection(INFINITE);

		CHDFSFile ip_file;
		int len = strlen(ip_addr);
		ip_file.OpenWriteFile(CUtility::ExtendString("DyableMPI/IPADDR/set", rank));
		ip_file.WriteObject(port);
		ip_file.WriteObject(len);
		ip_file.WriteObject(ip_addr, len);
		ip_file.CloseFile();
	}

	// This is the entry function
	void Slave() {

		int rank;
		MPI_Comm_rank(MPI_COMM_WORLD, &rank);

		while(true) {

			COpenConnection conn;
			conn.Socket() = m_serv_conn.ServerAcceptConnection();
			
			try {
				ProcessMasterRequests(conn);
			} catch(...) {
				conn.CloseConnection();
			}

			cout<<"Slave "<<rank<<"   Restarting-----------------------"<<endl;
		}
	}
};

// This class acts as a master to control the distribution of different
// processes. During the creation of a given process the master is contacted
// for a list of available processes. During the creation of a given 
// process the command line arguments and process name is sent to an 
// available process for execution.
class CMaster {

	// This stores the set of command line arguments
	CMemoryChunk<char> m_proc_buff;
	// This stores the offset for each process
	CMemoryChunk<int> m_proc_offset;
	// This stores the set of connections to the slaves
	CMemoryChunk<COpenConnection> m_conn;

	// This stores the read fd
	fd_set m_read_fd;

	// This stores the pipe id
	static COpenConnection m_master_conn;
	// This stores the connection to the master
	static COpenConnection m_slave_conn;
	// This stores the global number of processors available
	int m_tot_proc_num;
	// This stores the number of pending processes
	int m_pend_proc_num;
	// This stores the current process being processed
	int m_curr_proc;

	// This spawns the intital set of processes
	void InitializeProcessSet() {
			
		m_pend_proc_num = min(m_tot_proc_num, (m_proc_offset.OverflowSize() - 1) >> 1);
		m_curr_proc = 0;

		for(int i=0; i<m_pend_proc_num; i++) {
			int len = m_proc_offset[m_curr_proc+2] - m_proc_offset[m_curr_proc];
			int arg_offset = m_proc_offset[m_curr_proc+1] - m_proc_offset[m_curr_proc];

			m_conn[i].Send((char *)&len, sizeof(int));
			m_conn[i].Send((char *)&arg_offset, sizeof(int));
			m_conn[i].Send(m_proc_buff.Buffer() + m_proc_offset[m_curr_proc], len);
			m_curr_proc += 2;
		}
	}

	// This spawns a new process following the completion of an
	// existing process that has been previously spawned
	void RespawnProcessSet() {
		
		int rank;
		while(m_curr_proc < m_proc_offset.OverflowSize() - 1) {
			FD_ZERO(&m_read_fd);
			int max_fd = 0;
			for(int i=0; i<m_conn.OverflowSize(); i++) {
				FD_SET(m_conn[i].Socket(), &m_read_fd);
				max_fd = max(max_fd, m_conn[i].Socket());
			}

			fd_set read_copy = m_read_fd;
			int status = select(max_fd + 1, &read_copy, NULL, NULL, NULL);

			if(status < 0) {
				continue;
			}

			for(int i=0; i<m_pend_proc_num; i++) {
				if(FD_ISSET(m_conn[i].Socket(), &read_copy) == false) {
					continue;
				}

				COpenConnection &conn = m_conn[i];
				conn.Receive((char *)&rank, sizeof(int));

				cout<<"Start "<<(m_curr_proc >> 1)<<" Out Of "<<(m_proc_offset.OverflowSize() >> 1)<<endl;
				int len = m_proc_offset[m_curr_proc+2] - m_proc_offset[m_curr_proc];
				int arg_offset = m_proc_offset[m_curr_proc+1] - m_proc_offset[m_curr_proc];

				conn.Send((char *)&len, sizeof(int));
				conn.Send((char *)&arg_offset, sizeof(int));
				conn.Send(m_proc_buff.Buffer() + m_proc_offset[m_curr_proc], len);
				m_curr_proc += 2;
			}
		}

		// waits for all the pending processes to complete
		for(int i=0; i<m_pend_proc_num; i++) {
			cout<<"Wait for "<<i<<" Out Of "<<m_pend_proc_num<<endl;
			m_conn[i].Receive((char *)&rank, sizeof(int));
			m_curr_proc += 2;
		}
	}

	// This gathers statistics about the utilization of each machine
	void DisplayExecutionUtil() {

		CMemoryChunk<double> elap_time(m_tot_proc_num);
		int len = -1;

		for(int i=0; i<m_tot_proc_num; i++) {
			m_conn[i].Send((char *)&len, sizeof(int));
			m_conn[i].Receive((char *)&elap_time[i], sizeof(double));
		}

		CMath::NormalizeVector(elap_time.Buffer(), elap_time.OverflowSize());

		cout<<endl<<"***********************************************"<<endl;
		for(int i=0; i<m_tot_proc_num; i++) {
			cout<<"Node "<<i<<" Util "<<(elap_time[i] * 100)<<"%"<<endl;
		}	

		cout<<"***********************************************"<<endl;

		len = -3;
		for(int i=0; i<m_tot_proc_num; i++) {
			m_conn[i].Send((char *)&len, sizeof(int));
			m_conn[i].CloseConnection();
		}
	}

	// This initializes the connections to each of the slaves
	bool ConnectToSlaves() {

		int port;
		int len;
		char ip_addr[INET_ADDRSTRLEN];
		m_conn.AllocateMemory(m_tot_proc_num);
		int count = 0;

		FD_ZERO(&m_read_fd);
		
		CHDFSFile ip_file;
		for(int i=0; i<m_tot_proc_num; i++) {	
	
			while(true) {
				try {
					ip_file.OpenReadFile(CUtility::ExtendString
						("DyableMPI/IPADDR/set", i));
					ip_file.ReadObject(port);
					ip_file.ReadObject(len);
					ip_file.ReadObject(ip_addr, len);
					ip_addr[len] = '\0';
					break;
				} catch(...) {
					Sleep(100);
				}
			}

			while(m_conn[i].OpenClientConnection(port, ip_addr) == false) {
				if(++count >= 20) {
					return false;
				}
				cout<<"Could Not Contact Slave"<<endl;
				Sleep(200);
			}

			FD_SET(m_conn[i].Socket(), &m_read_fd);
		}

		return true;
	}

	// This reinitializes the clients if one of the connections is broken for some reason
	void RestartClients() {

		cout<<"Master Respawn-------------------------------------------------"<<endl;
		for(int i=0; i<m_conn.OverflowSize(); i++) {
			m_conn[i].CloseConnection();
		}

		Sleep(200);
		ConnectToSlaves();
	}

public:

	CMaster() {

		int set_num;
		MPI_Comm_size(MPI_COMM_WORLD, &set_num);
		m_tot_proc_num = set_num;
	}

	// This intialzes the master before commencing
	static int Initialize() {

		int port = 8888;
		CUtility::RandomizeSeed();
		while(m_master_conn.OpenServerConnection(port, LOCAL_NETWORK) == false) {
			port = (rand() % 100000) + 8888;
		}

		m_master_conn.ListenOnServerConnection(INFINITE);
		return port;
	}

	// This listens for the incomming connection from the slave
	static void ListenForConnections() {
		m_slave_conn.Socket() = m_master_conn.ServerAcceptConnection();
	}

	// This is used to terminate the mpi set
	static void Terminate(SOCKET &socket) {

		int status = -1;
		COpenConnection::Send(socket, (char *)&status, sizeof(int));
		m_slave_conn.CloseConnection();
		m_master_conn.CloseConnection();
		close(socket);
		exit(0);
	}

	// This is the entry function
	void Master() {	

		int proc_num;
		bool is_conn = false;


		while(true) {

			m_slave_conn.Receive((char *)&proc_num, sizeof(int));

			if(is_conn == false) {
				if(ConnectToSlaves() == false) {
					proc_num = -1;
				}

				is_conn = true;
			}

			if(proc_num < 0) {
				DisplayExecutionUtil();
				m_slave_conn.CloseConnection();
				m_master_conn.CloseConnection();
				exit(0);
			}

			m_proc_offset.AllocateMemory(proc_num);
			m_slave_conn.Receive((char *)m_proc_offset.Buffer(), sizeof(int) * proc_num);
			
			m_proc_buff.AllocateMemory(m_proc_offset.LastElement());
			m_slave_conn.Receive(m_proc_buff.Buffer(), m_proc_buff.OverflowSize());

			while(true) {
	
				try {
					InitializeProcessSet();
					RespawnProcessSet();
					break;
				} catch(...) {
					RestartClients();
				}
			}

			// notify command that finished
			m_slave_conn.Send((char *)&proc_num, sizeof(int));
		}
	}
};
COpenConnection CMaster::m_master_conn;
COpenConnection CMaster::m_slave_conn;


int main(int argc, char *argv[]) {

	int node;
	pid_t pid;
	SOCKET socket;
	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &node);
	int status;
	cout<<node<<" **"<<endl;
	if(node == 0) {

		int port = CMaster::Initialize();

		if(fork() == 0) {
		
			CMaster::ListenForConnections();

			if(fork() == 0) {
				CMaster master;
				master.Master();
			} else {
				CSlave slave;
				slave.Slave();
			}
		} else {
	
			Sleep(1000);
			while(COpenConnection::OpenClientConnection(socket, port, LOCAL_NETWORK) == false) {
				cout<<"Could not Connect"<<endl;
				Sleep(100);
			}

			CHDFSFile port_file;
			port_file.OpenWriteFile("DyableMPI/IPADDR/client_port");
			port_file.WriteObject(socket);
			port_file.CloseFile();

			if((pid = fork()) != 0) {
				waitpid(pid, &status, 0);
				CMaster::Terminate(socket);
			} else {
				

				CProcessSet::ExecuteProcess(CUtility::ExtendString
					(DFS_ROOT, "DyableCommand/DyableCommand"), "TestMaster");
			}
		}
	} else {
		CSlave slave;
		slave.Slave();
	}
	
	MPI_Finalize();
	return 0;
}
