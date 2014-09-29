#include "./MyStuff.h"

const char *LOCAL_NETWORK = "127.0.0.1";

// This is a general purpose class that allows opening
// a connection or accepting a connection from a foreign host.
// Once the connection has been established it's possible
// to write or reading character strings from the connection.
class COpenConnection {

	// This stores a socket object
	SOCKET m_socket;
	// Stores the different socket I/O types
	fd_set m_read_fd, m_write_fd, m_except_fd;
	// Stores the time out value
	timeval m_timeout;
	// Create a sockaddr_in object and set its values.
	sockaddr_in m_service;

	// This stores the server info
	struct sockaddr_in m_server;

	void CleanUp() {
		closesocket(m_socket);
	}

	// This is used to zero out a buffer
	void bzero(char *buff, int len) {
		for(int i=0; i<len; i++) {
			buff[i] = 0;
		}
	}

	// This is used to initialize the connection
	void Initialize() {

		#ifdef OS_WINDOWS
		WORD wVersionRequested;
		WSADATA wsaData;
		int wsaerr;

		// Using MAKEWORD macro, Winsock version request 2.2
		wVersionRequested = MAKEWORD(2, 2);
		wsaerr = WSAStartup(wVersionRequested, &wsaData);

		if(wsaerr != 0) {
			/* Tell the user that we could not find a usable */
			/* WinSock DLL.*/
			printf("Server: The Winsock dll not found!\n");
			return;
		}

		/* Confirm that the WinSock DLL supports 2.2.*/
		/* Note that if the DLL supports versions greater */
		/* than 2.2 in addition to 2.2, it will still return */
		/* 2.2 in wVersion since that is the version we */
		/* requested. */
		if(LOBYTE(wsaData.wVersion) != 2 || HIBYTE(wsaData.wVersion) != 2) {
			/* Tell the user that we could not find a usable */
			/* Winock DLL.*/
			printf("Server: The dll do not support the Winsock version %i.%i!\n", 
				LOBYTE(wsaData.wVersion), HIBYTE(wsaData.wVersion));
			return;
		}
		
		#endif

		m_timeout.tv_sec = 0;
		m_timeout.tv_usec = 0;
	}

public:

	// Sets up several DLL libraries
	COpenConnection() {
		Initialize();
		
	}

	COpenConnection(SOCKET socket) {
		Initialize();
		m_socket = socket;
	}

	// Opens a client connection to some server on a given port and ip address
	// @param port - the connecting port for the given process
	// @param ip_address - a buffer containing the server ip address
	// @return true if successful, false otherwise
	static bool OpenClientConnection(SOCKET &sock, u_short port, const char *ip_addr) {
		
		#ifdef OS_WINDOWS
		// Initialize Winsock.
		WSADATA wsaData;
		int iResult = WSAStartup(MAKEWORD(2, 2), &wsaData);

		if(iResult != NO_ERROR) {
			printf("Client: Error at WSAStartup().\n");
			return false;
		}
		
		#endif
		
		sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
		if(sock == INVALID_SOCKET) {
			printf("Client: socket() - Error at socket():\n");
			return false;
		}
		
		int nHostAddress;
		struct hostent* pHostInfo;
		/* get IP address from name */
	        pHostInfo = gethostbyname(ip_addr);
	        /* copy address into int */
	        memcpy(&nHostAddress, pHostInfo->h_addr, pHostInfo->h_length);
		
		// Connect to a server.
		sockaddr_in clientService;
		clientService.sin_family = AF_INET;
		clientService.sin_addr.s_addr = nHostAddress;
		clientService.sin_port = htons(port);

		if(connect(sock, (struct sockaddr *) &clientService, 
			sizeof(clientService)) == SOCKET_ERROR) {
			closesocket(sock);
			return false;
		}

		return true;
	}

	// Opens a client connection to some server on a given port and ip address
	// @param port - the connecting port for the given process
	// @param ip_address - a buffer containing the server ip address
	// @return true if successful, false otherwise
	bool OpenClientConnection(u_short port, const char *ip_addr) {
		return OpenClientConnection(m_socket, port, ip_addr);
	}
	
	// Opens a server connection to some server on a given port
	// and ip address.
	// @param port - the connecting port for the given process
	// @param ip_address - a buffer containing the server ip address
	// @return true if successful, false otherwise
	bool OpenServerConnection(u_short port, const char *ip_addr) {
		
		// Call the socket function and return its value to the
		// m_socket variable. For this application, use the Internet
		// address family, streaming sockets, and the TCP/IP protocol.
		// using AF_INET family, TCP socket type and protocol of the AF_INET - IPv4
		m_socket = socket(AF_INET, SOCK_STREAM, 0);

		// Check for errors to ensure that the socket is a valid socket
		if(m_socket == INVALID_SOCKET) {
			printf("Server: Error at socket():\n");
			return false;
		}

 
		// AF_INET is the Internet address family.
		m_service.sin_family = AF_INET;
		// LOCAL_NETWORK is the local IP address to 
		// which the socket will be bound.
		m_service.sin_addr.s_addr = INADDR_ANY;
		// port number to which the socket will be bound.
		m_service.sin_port = htons(port);

		// Call the bind function, passing the created 
		// socket and the sockaddr_in structure as parameters.
		// Check for general errors.
		if(bind(m_socket, (struct sockaddr *) &m_service, sizeof(m_service)) == SOCKET_ERROR) {
			return false;
		}

		return true;
	}

	// This formats the UDP connection before sending
	void FormatUDPClientConnection(u_short port, const char *ip_addr) {

		 /* Zero out server address - IMPORTANT! */
		memset((void *)&m_server, '\0', sizeof(struct sockaddr_in));

		/* Set address type to IPv4 */
		m_server.sin_family = AF_INET;
		/* Assign port number, using network byte order */
		m_server.sin_port = htons(port);

	        #ifdef WINDOWS
			struct hostent *hp =  gethostbyname("localhost");
			m_server.sin_addr.S_un.S_un_b.s_b1 = hp->h_addr_list[0][0];
			m_server.sin_addr.S_un.S_un_b.s_b2 = hp->h_addr_list[0][1];
			m_server.sin_addr.S_un.S_un_b.s_b3 = hp->h_addr_list[0][2];
			m_server.sin_addr.S_un.S_un_b.s_b4 = hp->h_addr_list[0][3];
		#else 
			if(inet_aton(ip_addr, &m_server.sin_addr) == 0) {
				perror("Invalid IP\n");
			}
		#endif
	}

	// This creates a UDP socket on the server side
	void OpenClientUDPConnection(u_short port, const char *ip_addr) {

		m_socket = socket(AF_INET, SOCK_DGRAM, 0);
		FormatUDPClientConnection(port, ip_addr);
	}

	// This creates a UDP socket on the server side
	bool OpenServerUDPConnection(u_short port, const char *ip_addr) {

		  if((m_socket = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
			  return false;
		  }

		  memset((void *)&m_server, '\0', sizeof(struct sockaddr_in));
		  m_server.sin_family = AF_INET;

		  m_server.sin_port = htons(port);

		  #ifdef WINDOWS
			struct hostent *hp =  gethostbyname("localhost");
			m_server.sin_addr.S_un.S_un_b.s_b1 = hp->h_addr_list[0][0];
			m_server.sin_addr.S_un.S_un_b.s_b2 = hp->h_addr_list[0][1];
			m_server.sin_addr.S_un.S_un_b.s_b3 = hp->h_addr_list[0][2];
			m_server.sin_addr.S_un.S_un_b.s_b4 = hp->h_addr_list[0][3];
	 	  #else 
			m_server.sin_addr.s_addr = htonl(INADDR_ANY);
		  #endif
 
		  if (bind(m_socket, (struct sockaddr *)&m_server, sizeof(m_server)) < 0) {
			  return false;
		  }

		  return true;
	}


	// Returns a reference to the socket
	SOCKET &Socket() {
		return m_socket;
	}

	// Sets the current fd associated with the given socket -
	// used for select in non-blocking sockets
	inline void SetupFDSet() {
		FD_ZERO(&m_read_fd);
		FD_ZERO(&m_write_fd);
		FD_ZERO(&m_except_fd);

		FD_SET(m_socket, &m_read_fd);
		FD_SET(m_socket, &m_write_fd);
		FD_SET(m_socket, &m_except_fd);
	}

	// Sets the time to wait before timeout occurs in using
	// non blocking sockets
	// @param seconds - the number of seconds to wait for
	inline void SetTimeOutForNonBlocking(int seconds) {
		m_timeout.tv_sec = seconds;
		m_timeout.tv_usec = 0;
	}

	// Starts the server listening on the given connection
	// @param max_connnection - the max number of connections to accept
	// @return true if successful, false otherwise
	bool ListenOnServerConnection(int max_connnection) {
		// Call the listen function, passing the created 
		// socket and the maximum number of allowed WEB_CONNECTIONS
		// to accept as parameters. Check for general errors.
		if(listen(m_socket, max_connnection) == SOCKET_ERROR) {
			printf("Server: listen(): Error listening on socket \n");
			CleanUp();
			return false;
		}
		return true;
	}

	// Checks to see if the start of the url contains www. - 
	// otherwise inserts it
	// @param url - a character string containing the url
	// @param url_lenth - the length of the url it is changed
	// - if www. is prepended to the url buffer
	// @param start - an offset into the url string
	// @returns the modified string
	static char *CheckURLPrefix(char url[], int &url_length, int start = 0) {

		url += start;
		url_length -= start;
		int dot = 0;
		for(int i=0;i<url_length;i++) {
			if(url[i] == '/')break;
			if(url[i] == '.')dot++;
		}

		if(dot < 2) {
			memcpy(CUtility::SecondTempBuffer(), "www.", 4);
			memcpy(CUtility::SecondTempBuffer() + 4, url, url_length);
			url_length += 4;
			url = CUtility::SecondTempBuffer();
		}
		return url;
	}

	// returns a socket that represents a connection between
	// the server and the connecting client
	// @return the socket handling the connection
	SOCKET ServerAcceptConnection() {
		// Create a temporary SOCKET object called 
		// AcceptSocket for accepting WEB_CONNECTIONS.
		SOCKET accept_socket = SOCKET_ERROR;
		int nAddressSize = sizeof(struct sockaddr_in);

		while(accept_socket == SOCKET_ERROR) {
			// wait for a connection
			accept_socket = accept(m_socket, NULL, NULL);
		}

		return accept_socket;
	}

	// Sends data using UDP
	void SendUDP(const char buff[], int length) {

		int client_length = (int)sizeof(struct sockaddr_in);
		if(sendto(m_socket, buff, length, NULL, (struct sockaddr *) &m_server, client_length) < 0) {
			printf("Could not send packet!");
		}
	}

	// Receives data using UDP
	// @return the number of bytes received
	int ReceiveUDP(char buff[], int buffer_size) {

		socklen_t client_length = (int)sizeof(struct sockaddr_in);

		int bytes_received = recvfrom(m_socket, buff, buffer_size,
			0, (struct sockaddr *)&m_server, &client_length);

		if (bytes_received < 0)
		{
			return -1;
		}
		

		return bytes_received;
	}

	// Sends data to the connected foreign host
	// @param buffer - the buffer containg the data to send
	// @param length - the number of bytes to send accross the connection
	// @return the status of the sent data, -1 if an error occured
	static int Send(SOCKET &socket, const char buffer[], int length) {
		
		#ifdef OS_WINDOWS
			int status = send(socket, buffer, length, 0);
		#else
			int status = write(socket, buffer, length);
		#endif

		if(status <= 0) {
			throw ENetworkException("Send Error");
		}

		return status;
	}

	// Sends data to the connected foreign host
	// @param buffer - the buffer containg the data to send
	// @param length - the number of bytes to send accross the connection
	// @return the status of the sent data, -1 if an error occured
	int Send(const char buffer[], int length) {
		return Send(m_socket, buffer, length);
	}

	// Recieves data from a given connection
	// @param bytes_recieved - the number of bytes recieved from the other size
	// @param buffer - a buffer to place the recieved bytes
	// @param buffer_size - the maximum number of bytes to recieve at any one time
	// @return the status code, -1 if the connection has closed
	static int Receive(SOCKET &sock, int &bytes_recieve, 
		char buffer[], int buffer_size) {

		bytes_recieve = SOCKET_ERROR;
		
		#ifdef OS_WINDOWS
			bytes_recieve = recv(sock, buffer, buffer_size, 0);
		#else
			bytes_recieve = read(sock, buffer, buffer_size);
		#endif


		if(bytes_recieve <= 0) {
			throw ENetworkException("Receive Error");
		}

		return 0;
	}

	// This is the non blocking version of receive
	// it uses select to wait for an available connection.
	// It will return after timeout or if an error occurred, if
	// the data does not arrive in the allowed time
	// @param buffer - the buffer containg the data to send
	// @param length - the number of bytes to send accross the connection
	// @return -1 if error occurred, 1 if connection closed, 0 otherwise
	int NonBlockingReceive(char buffer[], int length) {

		while(true) {
			fd_set read_fd = m_read_fd;
			FD_SET(m_socket, &read_fd);
			int status = select(m_socket + 1, &read_fd, NULL, NULL, &m_timeout);
			if(status <= 0)return -1;

			if(FD_ISSET(m_socket, &read_fd)) {
				// descriptor became readable
				return Receive(buffer, length);
			}
		}
	}

	// Removes the http:// prefix from the url if it exists
	// @param url - a buffer containing the url
	// @param length - stores the new length of the string
	// - considering the removal of the http:// prefix
	// @return a pointer to the start of the url
	static char *StandardizeURL(char url[], int &length) {
		int start = 0;
		if(CUtility::FindFragment(url, "http://", start)) {
			length -= 7;
		}
		return url + start;
	}

	// Retrieves the server part of a given url
	// @param url - a buffer containing the url
	// @param length - stores the new length of the string
	// - considering the removal of the http:// prefix
	// @return a pointer to the start of the url
	static char *GetServer(char url[], int &length) {
		int offset = 0;
		while((url[offset] != '/') && (++offset < length));
		length = offset;
		return url;
	}

	// Retrieves some data from the connecting process
	// @param buffer - buffer to place the recieved data
	// @param buffer_size - the maximum number of bytes to recieve at
	// - any one time
	// @return the status code, -1 if connection closed
	static int Receive(SOCKET &sock, char buffer[], int buffer_size) {
		int bytes_recieve = 0;
		int total_bytes = 0;

		while(total_bytes < buffer_size) {
			Receive(sock, bytes_recieve, 
				buffer + total_bytes, buffer_size - total_bytes);
		
			total_bytes += bytes_recieve;
		}

		return total_bytes;
	}

	// Retrieves some data from the connecting process
	// @param buffer - buffer to place the recieved data
	// @param buffer_size - the maximum number of bytes to recieve at
	// - any one time
	// @return the status code, -1 if connection closed
	int Receive(char buffer[], int buffer_size) {
		return Receive(m_socket, buffer, buffer_size);
	}

	// Retrieves some data from the connecting process
	// @param buffer - buffer to place the recieved data
	// @param buffer_size - the maximum number of bytes to recieve at
	// - any one time
	// @return the status code, -1 if connection closed
	int Receive1(int &bytes_recieve, char buffer[], int buffer_size) {

		return Receive(m_socket, bytes_recieve, buffer, buffer_size);
	}

	// Forms a new url by taking a document and appending
	// to the existing server in the current url
	// @param url - the existing url
	// @param url_length - the length of the existing url
	// @param document - the document file that is needed to create the new url
	// @param doc_length - the length of the document
	// @param new_length - the length of the new url
	// @return a pointer to the new url
	char *CompileURL(char url[], int url_length, 
		char document[], int doc_length, int &new_length) {

		CompileURL(url, url_length, document, 
			doc_length, new_length, CUtility::SecondTempBuffer());

		return CUtility::SecondTempBuffer();
	}

	// Forms a new url by taking a document and appending
	// to the existing server in the current url
	// @param url - the existing url
	// @param url_length - the length of the existing url
	// @param document - the document file that is needed to create the new url
	// @param doc_length - the length of the document
	// @param new_length - the length of the new url
	// @param final_url - used to store the final url
	// @return a pointer to the new url
	static void CompileURL(char url[], int url_length, char document[],
		int doc_length, int &new_length, char final_url[]) {

		if(document[0] == '/') {
			for(int i=0; i<url_length; i++) {
				if(url[i] == '/') {
					url_length = i;
					break;
				}
			}

			document++;
			doc_length--;
		} else {

			for(int i=url_length-1; i>=0; i--) {
				if(url[i] == '/') {
					url_length = i;
					break;
				}
			}
		}

		memcpy(final_url, url, url_length);
		final_url[url_length] = '/';
		url_length++;

		memcpy(final_url + url_length, document, doc_length);
		new_length = url_length + doc_length;
	}

	// Closes the current connection
	inline void CloseConnection() {
		closesocket(m_socket);
	}

	// Closes the current connection
	inline static void CloseConnection(SOCKET &socket) {
		closesocket(socket);
	}

	~COpenConnection() {
		CleanUp();
	}
};

// This is used to establish a pipe connection between two processes. This 
// allows to processes to communicate with one another without the use of sockets.
class CPipe {

	// This stores the pipe connection
	int m_pipe_id[2];

public:

	CPipe () {
	}

	// This initializes the pipe
	inline void InitializePipe(int pipe_id[]) {
		memcpy(m_pipe_id, pipe_id, sizeof(int) * 2);
	}

	// This creates the pipe
	inline void CreatePipe() {
		pipe(m_pipe_id);
	}

	// This writes the pipe connection to file
	void WritePipeToFile(CHDFSFile &file) {
		file.WriteObject(m_pipe_id, 2);
	}

	// This reads the pipe connection to file
	void ReadPipeFromFile(CHDFSFile &file) {
		file.ReadObject(m_pipe_id, 2);
	}

	// Retrieves some data from the connecting process
	// @param buffer - buffer to place the recieved data
	// @param buffer_size - the maximum number of bytes to recieve at
	// 			 - any one time
	// @return the status code, -1 if connection closed
	int Receive(char buffer[], int buffer_size) {

		int total_bytes = 0;
		while(total_bytes < buffer_size) {
			total_bytes += read(m_pipe_id[0], buffer + total_bytes, buffer_size - total_bytes);
		}

		return total_bytes;
	}

	// Sends some data to the connecting process
	// @param buffer - buffer to place the recieved data
	// @param buffer_size - the maximum number of bytes to recieve at
	// 			 - any one time
	// @return the status code, -1 if connection closed
	int Send(char buffer[], int buffer_size) {

		int total_bytes = 0;
		while(total_bytes < buffer_size) {
			total_bytes += write(m_pipe_id[1], buffer + total_bytes, buffer_size - total_bytes);
		}

		return total_bytes;
	}

	// This closes the pipe file
	void CloseConnection() {
		close(m_pipe_id[0]);
		close(m_pipe_id[1]);
	}

	~CPipe () {
		close(m_pipe_id[0]);
		close(m_pipe_id[1]);
	}

};
