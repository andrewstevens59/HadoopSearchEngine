#include "./DocumentDatabase.h"

#define ADDRESS     "mysocket"  /* addr to connect */

// This class is responsible for maintaining a set of processes.
// It also handles the renewable of a given process if one dies.
class CProcessSet {

	// This defines an instance of a thread
	struct SThread {
		CProcessSet *this_ptr;
		int client_id;
	};

	// This stores the set of process info
	struct SProcessInfo {
		// This stores the process directory
		CString process_dir;
		// This stores the process args
		CString process_args;
		// This stores the client id
		int client_id;
		// This stores the number of elapsed time checkpoints
		int elapsed_checkpoints;
		// This stores the handle of the process
		HANDLE handle;
		// This stores a ptr to the next info block
		SProcessInfo *next_ptr;
	};

	// This is used to protect access to the beacon response
	static CMutex m_mutex;
	// This stores the current working directory
	char m_work_dir[500];

	#ifdef OS_WINDOWS
	// This is used to SendUDP a status beacon to notify command
	// that it is still alive
	static COpenConnection m_conn;
	// This stores the client id of the slave
	static int m_client_id;
	// This stores the process info
	static CLinkedBuffer<SProcessInfo> m_process_state;
	// This stores each active process
	static CArray<SProcessInfo *> m_active_process;
	// This stores the free set 
	static SProcessInfo *m_free_ptr;
	// This stores the communication port
	static int m_port;
	// This stores the handle to the monitor thread
	static HANDLE m_monitor_handle;
	// This is the maximum number of spawned clients
	static int m_max_client_num;
	// This is the time out for the process set
	static int m_timeout;
	// This is a predicate indicating whether a process
	// should be restarted or terminated after timeout
	static bool m_is_restart_process;
	#else
	// This the set of command line arguments
	CArrayList<char>  m_proc_buff;
	// This stores the offset of each process in the buffer
	CArrayList<int> m_proc_offset;
	// This stores the pipe connections used to connect to the master
	COpenConnection m_conn;
	#endif

	// This creates a process used to initiate one of the 
	// stages in the pipeline.
	// @param process_name - the name of the process to start
	// @param command_str - the command line args separated by spaces
	HANDLE CreateDyableProcess(const char process_name[], const char command_str[]) {

		#ifdef OS_WINDOWS
		STARTUPINFO StartInfo;
		PROCESS_INFORMATION ProcessInfo;
	
		ZeroMemory(&StartInfo, sizeof(StartInfo));
		StartInfo.cb = sizeof(StartInfo);

		char temp_directory[500];
		int process_name_length = strlen(process_name);
		while(process_name[--process_name_length] != '/');
		strcpy(temp_directory, m_work_dir);
		strcat(temp_directory, process_name);

		while(!CreateProcess(temp_directory,
		    (LPSTR)command_str, NULL, NULL, FALSE,
			HIGH_PRIORITY_CLASS | CREATE_NEW_CONSOLE, NULL, "./", 
			&StartInfo, &ProcessInfo))Sleep(500);

		return ProcessInfo.hProcess;

                #else

		int pid;
		if((pid = fork()) == 0) {
                        strcpy(CUtility::SecondTempBuffer(), m_work_dir);
			strcat(CUtility::SecondTempBuffer(), process_name);
			strcpy(CUtility::ThirdTempBuffer(), command_str);

			int process_length = strlen(CUtility::SecondTempBuffer());

			if(CUtility::FindFragment(CUtility::SecondTempBuffer(), "exe", 
				process_length, process_length - 3)) {
		
				CUtility::SecondTempBuffer()[process_length - 4] = '\0';
				process_length -= 4;
			}

			int pos = CUtility::FindSubFragment(CUtility::SecondTempBuffer(), "Debug/");
			if(pos > 0) {
				memcpy(CUtility::SecondTempBuffer() + pos, 
					CUtility::SecondTempBuffer() + pos + 6, process_length - 6 - pos);
				CUtility::SecondTempBuffer()[process_length - 6] = '\0';
				process_length -= 6;
			}

   			char *args[20];
			int len = strlen(command_str);
			int back = strlen(CUtility::SecondTempBuffer());
			args[0] = CUtility::ThirdTempBuffer();
			int offset = 1;

			while(CUtility::SecondTempBuffer()[back - 1] != '/') {				
				back--;
			}

			CUtility::SecondTempBuffer()[back - 1] = '\0';
			chdir(CUtility::SecondTempBuffer());

			for(int i=0; i<len; i++) {				
				if(command_str[i] == ' ') {		
					args[offset++] = CUtility::ThirdTempBuffer() + i + 1;
					CUtility::ThirdTempBuffer()[i] = '\0';
				}
			}

			args[offset] = 0;
			execv(CUtility::SecondTempBuffer() + back, args);
		}
		
		return pid;
                #endif
	}

	// This checks for process updates of active clients
	void Monitor() {

		#ifdef OS_WINDOWS
		static CMemoryChunk<char> buff(20);

		while(true) {
			m_mutex.Acquire();
			for(int i=0; i<m_active_process.Size(); i++) {
				SProcessInfo *info = m_active_process[i];
				if(++info->elapsed_checkpoints >= m_timeout) {
					info->elapsed_checkpoints = 0;
					cout<<"Terminating Process "<<info->client_id<<" "<<m_port<<endl;

					if(m_is_restart_process == false) {
						sprintf(buff.Buffer(), "f %d", (int)info->client_id);
						m_conn.SendUDP(buff.Buffer(), 20);
					} else {
						strcpy(CUtility::SecondTempBuffer(), info->process_dir.Buffer());
						strcpy(CUtility::ThirdTempBuffer(), info->process_args.Buffer());
						CreateDyableProcess(CUtility::SecondTempBuffer(), CUtility::ThirdTempBuffer());
					}
				}
			}
			m_mutex.Release();
			Sleep(100);
		}
		#endif
	}

	// This checks for process updates of active clients
	static THREAD_RETURN1 THREAD_RETURN2 MonitorThread(void *ptr) {

		CProcessSet *this_ptr = (CProcessSet *)ptr;
		this_ptr->Monitor();

		return 0;
	}

	// This monitors the response sent back from a client
	bool ProcessClientResponse() {

		#ifdef OS_WINDOWS
		//cout<<"Listening"<<endl;
		CMemoryChunk<char> buff(20);
		if(m_conn.ReceiveUDP(buff.Buffer(), 20) < 0) {
			return false;
		}

		//cout<<"ReceiveUDP "<<buff.Buffer()<<endl;
		bool process_fin = (buff[0] == 'f');
		int client_id = CANConvert::AlphaToNumeric(buff.Buffer() + 2);

		m_mutex.Acquire();

		for(int i=0; i<m_active_process.Size(); i++) {
			SProcessInfo *info = m_active_process[i];
			if(info->client_id == client_id) {
				if(process_fin == false) {
					info->elapsed_checkpoints = 0;
					m_mutex.Release();
					return true;
				} 

				SProcessInfo *prev_ptr = m_free_ptr;
				sfsfdm_free_ptr = info;
	
				m_free_ptr->next_ptr = prev_ptr;
				m_active_process.DeleteElement(i);
				m_mutex.Release();
				return true;
			}
		}
		
		m_mutex.Release();
		#endif
		return true;
	}

	// This spawns a process 
	// @param command_str - this stores the directory of the process
	// @param args - this the command line arguments
	// @param client_id - this is the id of the client being created
	void SpawnProcess(const char process_name[], const char args[], int client_id) {

		#ifdef OS_WINDOWS
		SProcessInfo *info = NULL;


		if(m_free_ptr == NULL) {
			info = m_process_state.ExtendSize(1);
		} else {
			info = m_free_ptr;
			m_free_ptr = m_free_ptr->next_ptr;
		}

		m_active_process.PushBack(info);
		info->client_id = client_id;
		info->elapsed_checkpoints = 0;
		info->process_args = args;
		info->process_dir = process_name;
		info->handle = CreateDyableProcess(process_name, args);
		#endif
	}

	// This executes a child process and waits for the child to terminate
	bool SpawnChildProcess() { 

		if(m_proc_offset.Size() > 3) {
			return false;
		}
		
		pid_t pid;
		if((pid = fork()) != 0) {
			int status;
			waitpid(pid, &status, 0);
		} else {

			static CMemoryChunk<char> temp_buff(4096);
			if(m_proc_buff.Size() > temp_buff.OverflowSize()) {
				temp_buff.AllocateMemory(m_proc_buff.Size());
			}

			memcpy(temp_buff.Buffer(), m_proc_buff.Buffer(), m_proc_buff.Size());

			cout<<"Create Single "<<temp_buff.Buffer()<<" "<<(temp_buff.Buffer() + m_proc_offset[1])<<endl;
			CProcessSet::ExecuteProcess(temp_buff.Buffer(), temp_buff.Buffer() + m_proc_offset[1]);
		}

		return true;
	}

public:

	CProcessSet() {

		strcpy(m_work_dir, "");

		#ifdef OS_WINDOWS
		m_port = 5555;
		m_max_client_num = 8;
		m_timeout = 20;
		m_is_restart_process = false;
		#else
		m_proc_offset.Initialize();
		m_proc_buff.Initialize(1024);

		CHDFSFile port_file;
		port_file.OpenReadFile("DyableMPI/IPADDR/client_port");
		port_file.ReadObject(m_conn.Socket());

		#endif
	}

	// This will restart the process on timeout
	inline static void SetRestartProcess() {
		#ifdef OS_WINDOWS
		m_is_restart_process = true;
		#endif
	}

	// This will terminate the process on timeout
	inline static void SetTerminateProcess() {
		#ifdef OS_WINDOWS
		m_is_restart_process = false;
		#endif
	}

	// This sets the listening port
	inline static void SetPort(int port) {
		#ifdef OS_WINDOWS
		m_port = port;
		#endif
	}

	// This returns the listening port
	inline static int GetPort() {
		#ifdef OS_WINDOWS
		return m_port;
		#else
		return 0;
		#endif
	}

	// This sets the current working directory
	inline void SetWorkingDirectory(const char dir[]) {
		strcpy(m_work_dir, dir);
	}

	// This sets the timeout
	inline void SetTimeout(int units) {
		#ifdef OS_WINDOWS
		m_timeout = units;
		#endif
	}

	// This resets the set of pending processes
	inline void ResetProcessSet() {
		#ifdef OS_WINDOWS
		m_mutex.Acquire();
		m_active_process.Resize(0);
		m_mutex.Release();
		#endif
	}

	// This sets the maximum number of clients to initiate 
	// at any one time
	inline void SetMaximumClientNum(int client_num) {
		#ifdef OS_WINDOWS
		m_max_client_num = client_num;
		#endif
	}

	// This creates a standard process
	// @param process_name - the name of the process to start
	// @param command_str - the command line args separated by spaces
	HANDLE CreateLocalProcess(const char process_name[], const char command_str[]) {
		return CreateDyableProcess(process_name, command_str);
	}

	// This creates a given process
	static void ExecuteProcess(const char *process_name, const char *command_str) {

		static CMemoryChunk<char> temp_buff1(4096);
		static CMemoryChunk<char> temp_buff2(4096);
		int len1 = strlen(process_name);
		int len2 = strlen(command_str);

		if(len1 > temp_buff1.OverflowSize()) {
			temp_buff1.AllocateMemory(len1);
		}

		if(len2 > temp_buff2.OverflowSize()) {
			temp_buff2.AllocateMemory(len2);
		}

		strcpy(temp_buff1.Buffer(), process_name);
		strcpy(temp_buff2.Buffer(), command_str);
		
		int process_length = strlen(temp_buff1.Buffer());
		if(CUtility::FindFragment(temp_buff1.Buffer(), "exe", 
			process_length, process_length - 3)) {
		
			temp_buff1[process_length - 4] = '\0';
			process_length -= 4;
		}
		
		int pos = CUtility::FindSubFragment(temp_buff1.Buffer(), "Debug/");
		if(pos > 0) {
			memcpy(temp_buff1.Buffer() + pos, temp_buff1.Buffer()
				 + pos + 6, process_length - 6 - pos);
			temp_buff1[process_length - 6] = '\0';
			process_length -= 6;
		}
		
		static char *args[30];
		char *tok = strtok(temp_buff2.Buffer(), " ");
		int offset = 0;
                
		while(tok != NULL) {
			args[offset++] = tok;
			tok = strtok(NULL, " ");
		}	
	
		args[offset] = 0;
		execv(temp_buff1.Buffer(), args);
	}

	// This waits for all pending processes to terminate
	void WaitForPendingProcesses() {

		#ifdef OS_WINDOWS
		int count = 0;
		int curr_size = m_active_process.Size();
		while(curr_size > 0) {
			while(ProcessClientResponse() == false) {
				if(++count >= 3) {
					return;
				}

				Sleep(200 + (rand() % 300));
			}

			m_mutex.Acquire();
			curr_size = m_active_process.Size();
			m_mutex.Release();
			count = 0;
		}
		#else

		if(m_proc_offset.Size() == 0) {
			return;
		}

		m_proc_offset.PushBack(m_proc_buff.Size());

		if(SpawnChildProcess() == false) {
			int size = m_proc_offset.Size();
			m_conn.Send((char *)&size, sizeof(int));
			m_conn.Send((char *)m_proc_offset.Buffer(), m_proc_offset.Size() * sizeof(int));
			m_conn.Send(m_proc_buff.Buffer(), m_proc_buff.Size());
			m_conn.Receive((char *)&size, sizeof(int));
		}
		
		m_proc_buff.Resize(0);
		m_proc_offset.Resize(0);
		#endif
	}

	// This creates a renewable process
	// @param command_str - this stores the directory of the process
	// @param args - this the command line arguments
	// @param client_id - this is the id of the client being created
	void CreateRemoteProcess(const char process_name[], const char args[], int client_id) {
		
		#ifdef OS_WINDOWS
		m_mutex.Acquire();
		if(m_active_process.OverflowSize() == 0) {
			m_active_process.Initialize(m_max_client_num);
			m_process_state.Initialize(20);
			m_conn.OpenServerUDPConnection(m_port, LOCAL_NETWORK);
			m_free_ptr = NULL;

			unsigned int threadID;
			static SThread thread;
			thread.client_id = client_id;
			thread.this_ptr = this;

			m_mutex.Release();
			m_monitor_handle = (HANDLE)_beginthreadex(NULL, 0, 
				MonitorThread, &thread, NULL, &threadID);
		} else {
			m_mutex.Release();
		}

		if(m_active_process.Size() < m_active_process.OverflowSize()) {	
			SpawnProcess(process_name, args,  client_id);
			m_mutex.Release();
			return;
		}

		int curr_size = m_active_process.Size();
		m_mutex.Release();

		while(curr_size >= m_active_process.OverflowSize()) {
			while(ProcessClientResponse() == false) {
				Sleep(200 + (rand() % 300));
			}
			m_mutex.Acquire();
			curr_size = m_active_process.Size();
			m_mutex.Release();
		}

		m_mutex.Acquire();
		SpawnProcess(process_name, args,  client_id);
		m_mutex.Release();
	
		#else

		strcpy(CUtility::ThirdTempBuffer(), m_work_dir);
		strcat(CUtility::ThirdTempBuffer(), process_name);
		cout<<"Spawn "<<CUtility::ThirdTempBuffer()<<" "<<args<<endl;

		m_proc_offset.PushBack(m_proc_buff.Size());
		m_proc_buff.CopyBufferToArrayList(CUtility::ThirdTempBuffer());
		m_proc_buff.CopyBufferToArrayList('\0');

		m_proc_offset.PushBack(m_proc_buff.Size());
		m_proc_buff.CopyBufferToArrayList(args);
		m_proc_buff.CopyBufferToArrayList('\0');

		#endif
	}

	// This removes files from a particular directory
	static void RemoveFiles(const char dir[]) {
		return;		
	   #ifdef OS_WINDOWS
		   WIN32_FIND_DATA ffd;
		   TCHAR szDir[MAX_PATH];
		   HANDLE hFind = INVALID_HANDLE_VALUE;
		   DWORD dwError=0;

		   // Prepare string for use with FindFile functions.  First, copy the
		   // string to a buffer, then append '\*' to the directory name.

		   StringCchCopy(szDir, MAX_PATH, dir);
		   StringCchCat(szDir, MAX_PATH, TEXT("\\*"));

		   // Find the first file in the directory.

		   hFind = FindFirstFile(szDir, &ffd);

		   if(INVALID_HANDLE_VALUE == hFind) {
			  return;
		   } 
   
		   // List all the files in the directory with some info about them.

		   do
		   {
			  if (ffd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)
			  {
			  }
			  else
			  {
				 cout<<"Remove "<<CUtility::ExtendString(dir, "/", ffd.cFileName)<<endl;
				 remove(CUtility::ExtendString(dir, "/", ffd.cFileName));
			  }
		   }
		   while (FindNextFile(hFind, &ffd) != 0);

		   FindClose(hFind);
	   #else
	   
		struct dirent *file_ptr = NULL;
	        DIR *dir_ptr = NULL;

		strcpy(CUtility::SecondTempBuffer(), CUtility::ExtendString(DFS_ROOT, dir));
		
	        dir_ptr = opendir(CUtility::SecondTempBuffer());
	        if(dir_ptr == NULL) {
		      cout<<"Couldn't open directory "<<CUtility::SecondTempBuffer()<<endl;
		      return;
	        }

	        while(file_ptr = readdir(dir_ptr)) {
	            cout<<"Remove "<<CUtility::ExtendString(CUtility::SecondTempBuffer(), "/", file_ptr->d_name)<<endl;
		    remove(CUtility::ExtendString(CUtility::SecondTempBuffer(), "/", file_ptr->d_name));
	        }
		
	        closedir(dir_ptr);

  
	   #endif
	}

	~CProcessSet() {
		#ifdef OS_WINDOWS
		TerminateThread(m_monitor_handle);
		#endif
	}
};

CMutex CProcessSet::m_mutex;
#ifdef OS_WINDOWS
COpenConnection CProcessSet::m_conn;
int CProcessSet::m_client_id;
CArray<CProcessSet::SProcessInfo *> CProcessSet::m_active_process;
CLinkedBuffer<CProcessSet::SProcessInfo> CProcessSet::m_process_state;
CProcessSet::SProcessInfo *CProcessSet::m_free_ptr;
int CProcessSet::m_port;
HANDLE CProcessSet::m_monitor_handle;
int CProcessSet::m_max_client_num;
int CProcessSet::m_timeout;
bool CProcessSet::m_is_restart_process;
#endif

// The maximum number of map reduce tasks
int m_max_process_num = 8;

// This is used to initiate MapReduce procedures. 
class CMapReduce : public CNodeStat {

	// This stores the connection to the MapReduce server
	static COpenConnection m_conn;
	// This stores the process set
	static CProcessSet m_process_set;

	// This issues the request to the MapReduce server
	// @param data_handle_func - this is the name of the data handler function
	// @param map_file_dir - this is the directory of the map_file
	// @param key_file_dir - this is the directory of the key file
	// @param output_file_dir - this is the directory of the output 
	// @param work_dir - this is where the map reduce temp files are stored
	// @param max_key_bytes - this is the maximum possible number of bytes
	//                      - that make up a key
	// @param maxp_map_bytes - this is the maximum possible number of bytes 
	//                       - that make up a map
	// @param map_reduce_type - this is the type of map reduce command
	// @param data_type - this is the data type that is being processed
	// @param hash_div_num - this is the number of hash sets to create
	static void IssueRequestToProcess(const char *data_handle_func, const char *map_file_dir, 
		const char *key_file_dir, const char *output_file_dir, const char *work_dir,
		int max_key_bytes, int max_map_bytes, const char *map_reduce_type, 
		const char *data_type, int hash_div_num) {

		if(data_handle_func == NULL) {
			data_handle_func = "NULL";
		}
				
		CString arg("Index ");
		arg += data_handle_func;
		arg += " ";
		arg += map_reduce_type;
		arg += " ";
		arg += map_file_dir;
		arg += " ";
		arg += key_file_dir;
		arg += " ";
		arg += output_file_dir;
		arg += " ";
		arg += work_dir;
		arg += " ";
		arg += data_type;
		arg += " ";
		arg += max_key_bytes;
		arg += " ";
		arg += max_map_bytes;
		arg += " ";

		if(hash_div_num >= 0) {
			arg += hash_div_num;
		} else {
			arg += "-1";
		}

		arg += " ";
		arg += CNodeStat::GetInstID();
		arg += " ";
		arg += m_max_process_num;

		m_process_set.SetWorkingDirectory(DFS_ROOT);
		m_process_set.SetPort(5555 + CNodeStat::GetInstID());
		m_process_set.CreateRemoteProcess("DyableMapReduce/DyableCommand"
			"/Debug/DyableCommand.exe", arg.Buffer(), CNodeStat::GetInstID());

		m_process_set.WaitForPendingProcesses();
	}

	// This issues the request to the MapReduce server
	// @param data_handle_func - this is the name of the data handler function
	// @param map_file_dir - this is the directory of the map_file
	// @param key_file_dir - this is the directory of the key file
	// @param output_file_dir - this is the directory of the output 
	// @param work_dir - this is where the map reduce temp files are stored
	// @param max_key_bytes - this is the maximum possible number of bytes
	//                      - that make up a key
	// @param maxp_map_bytes - this is the maximum possible number of bytes 
	//                       - that make up a map
	// @param map_reduce_type - this is the type of map reduce command
	// @param data_type - this is the data type that is being processed
	// @param hash_div_num - this is the number of hash sets to create
	static void IssueRequestToServer(const char *data_handle_func, const char *map_file_dir, 
		const char *key_file_dir, const char *output_file_dir, const char *work_dir,
		int max_key_bytes, int max_map_bytes, const char *map_reduce_type, 
		const char *data_type, int hash_div_num) {

		if(!m_conn.OpenClientConnection(5555, LOCAL_NETWORK)) {
			throw ENetworkException("Could not contact server");
		}

		int data_handle_func_length = 0;
		if(data_handle_func != NULL) {
			data_handle_func_length = strlen(data_handle_func);
		}

		int map_reduce_type_length = strlen(map_reduce_type);
		int key_file_buff_length = strlen(key_file_dir);
		int map_file_buff_length = strlen(map_file_dir);
		int output_file_buff_length = strlen(output_file_dir);
		int work_dir_length = strlen(work_dir);
		int data_type_length = strlen(data_type);

		m_conn.SendUDP((char *)&data_handle_func_length, sizeof(int));
		m_conn.SendUDP((char *)&map_reduce_type_length, sizeof(int));
		m_conn.SendUDP((char *)&map_file_buff_length, sizeof(int));
		m_conn.SendUDP((char *)&key_file_buff_length, sizeof(int));
		m_conn.SendUDP((char *)&output_file_buff_length, sizeof(int));
		m_conn.SendUDP((char *)&work_dir_length, sizeof(int));

		m_conn.SendUDP((char *)&max_key_bytes, sizeof(int));
		m_conn.SendUDP((char *)&max_map_bytes, sizeof(int));
		m_conn.SendUDP((char *)&data_type_length, sizeof(int));
		m_conn.SendUDP((char *)&hash_div_num, sizeof(int));
		m_conn.SendUDP((char *)&m_max_process_num, sizeof(int));

		cout<<map_reduce_type<<endl;
		cout<<map_file_dir<<endl;
		cout<<key_file_dir<<endl;
		cout<<output_file_dir<<endl;
		cout<<work_dir<<endl;
		cout<<data_type<<endl;
		cout<<hash_div_num<<endl;

		m_conn.SendUDP(data_handle_func, data_handle_func_length);
		m_conn.SendUDP(map_reduce_type, map_reduce_type_length);
		m_conn.SendUDP(map_file_dir, map_file_buff_length);
		m_conn.SendUDP(key_file_dir, key_file_buff_length);
		m_conn.SendUDP(output_file_dir, output_file_buff_length);
		m_conn.SendUDP(work_dir, work_dir_length);
		m_conn.SendUDP(data_type, data_type_length);

		char success = 'f';
		m_conn.ReceiveUDP(&success, 1);
		m_conn.CloseConnection();
	}

	// This issues the request to the MapReduce server
	// @param data_handle_func - this is the name of the data handler function
	// @param map_file_dir - this is the directory of the map_file
	// @param key_file_dir - this is the directory of the key file
	// @param output_file_dir - this is the directory of the output 
	// @param work_dir - this is where the map reduce temp files are stored
	// @param max_key_bytes - this is the maximum possible number of bytes
	//                      - that make up a key
	// @param maxp_map_bytes - this is the maximum possible number of bytes 
	//                       - that make up a map
	// @param map_reduce_type - this is the type of map reduce command
	// @param data_type - this is the data type that is being processed
	// @param hash_div_num - this is the number of hash sets to create
	static inline void IssueRequest(const char *data_handle_func, const char *map_file_dir, 
		const char *key_file_dir, const char *output_file_dir, const char *work_dir,
		int max_key_bytes, int max_map_bytes, const char *map_reduce_type, 
		const char *data_type, int hash_div_num) {

		if(1) {
			IssueRequestToProcess(data_handle_func, map_file_dir, key_file_dir, 
				output_file_dir, work_dir, max_key_bytes, max_map_bytes, 
				map_reduce_type, data_type, hash_div_num);
		} else {
			IssueRequestToServer(data_handle_func, map_file_dir, key_file_dir, 
				output_file_dir, work_dir, max_key_bytes, max_map_bytes, 
				map_reduce_type, data_type, hash_div_num);
		}
	}

public:

	CMapReduce() {
	}

	// This sets the maximum number of map reduce tasks to spawn.
	static void SetMaximumProcessNum(int process_num) {
		m_max_process_num = process_num;
	}

	// This is used to perform radix sort 
	// @param data_handle_func - this is the name of the data handler function
	// @param key_file_dir - this is the directory of the key file
	// @param output_file_dir - this is the directory of the output 
	// @param work_dir - this is where the map reduce temp files are stored
	// @param sort_byte_size - this is the number of bytes that make up a sort item
	// @param comp_byte_size - this is the number of lower order bytes that are used
	//                       - to compare sort items
	static void ExternalRadixSort(CSegFile &key_file, CSegFile &output_file, 
		const char *work_dir, int sort_byte_size, int comp_byte_size) {

		key_file.CloseFile();
		output_file.CloseFile();

		IssueRequest(NULL, "NULL", key_file.GetRootFileName(),
			output_file.GetRootFileName(), work_dir, sort_byte_size,
			comp_byte_size, "ExternalRadixSort", "int", -1);
	}

	// This is used to perform radix sort 
	// @param data_handle_func - this is the name of the data handler function
	// @param key_file_dir - this is the directory of the key file
	// @param output_file_dir - this is the directory of the output 
	// @param work_dir - this is where the map reduce temp files are stored
	// @param sort_byte_size - this is the number of bytes that make up a sort item
	static void ExternalQuickSort(const char *data_handle_func, 
		CSegFile &key_file, CSegFile &output_file, 
		const char *work_dir, int sort_byte_size) {

		key_file.CloseFile();
		output_file.CloseFile();

		IssueRequest(data_handle_func, "NULL", key_file.GetRootFileName(),
			output_file.GetRootFileName(), work_dir, sort_byte_size,
			0, "ExternalQuickSort", "int", -1);
	}

	// This is the external hash map function that applies maps to keys
	// @param data_handle_func - this is the name of the data handler function
	// @param map_file_dir - this is the directory of the map_file
	// @param key_file_dir - this is the directory of the key file
	// @param output_file_dir - this is the directory of the output 
	// @param work_dir - this is where the map reduce temp files are stored
	// @param max_key_bytes - this is the maximum possible number of bytes
	//                      - that make up a key
	// @param maxp_map_bytes - this is the maximum possible number of bytes 
	//                       - that make up a map
	static void ExternalHashMap(const char *data_handle_func, const char *key_file_dir,
		const char *map_file_dir, const char *output_file_dir, const char *work_dir,
		int max_key_bytes, int max_map_bytes) {

		IssueRequest(data_handle_func, map_file_dir, key_file_dir, output_file_dir,
			work_dir, max_key_bytes, max_map_bytes, "ExternalHashMap", "int", -1);
	}

	// This is the external hash map function that applies maps to keys
	// @param data_handle_func - this is the name of the data handler function
	// @param map_file_dir - this is the directory of the map_file
	// @param key_file_dir - this is the directory of the key file
	// @param output_file_dir - this is the directory of the output 
	// @param work_dir - this is where the map reduce temp files are stored
	// @param max_key_bytes - this is the maximum possible number of bytes
	//                      - that make up a key
	// @param maxp_map_bytes - this is the maximum possible number of bytes 
	//                       - that make up a map
	static void ExternalHashMap(const char *data_handle_func, CSegFile &key_file,
		CSegFile &map_file, CSegFile &output_file, const char *work_dir,
		int max_key_bytes, int max_map_bytes) {

		key_file.CloseFile();
		map_file.CloseFile();
		output_file.CloseFile();

		IssueRequest(data_handle_func, map_file.GetRootFileName(), key_file.GetRootFileName(),
			output_file.GetRootFileName(), work_dir, max_key_bytes,
			max_map_bytes, "ExternalHashMap", "int", -1);
	}

	// This finds the occurrence of each unique key
	// @param data_handle_func - this is the name of the data handler function
	// @param key_file_dir - this is the directory of the key file
	// @param output_file_dir - this is the directory of the output 
	// @param work_dir - this is where the map reduce temp files are stored
	// @param max_key_bytes - this is the maximum possible number of bytes
	//                      - that make up a key
	// @param hash_div_num - this is the number of merged sets to create
	static void KeyOccurrenceLong(const char *data_handle_func, 
		CSegFile &key_file, CSegFile &output_file, const char *work_dir,
		int max_key_bytes, int hash_div_num = -1) {

		key_file.CloseFile();
		output_file.CloseFile();

		IssueRequest(data_handle_func, "NULL", key_file.GetRootFileName(),
			output_file.GetRootFileName(),
			work_dir, max_key_bytes, 0, "KeyOccurrence", "int", hash_div_num);
	}

	// This finds the occurrence of each unique key
	// @param data_handle_func - this is the name of the data handler function
	// @param key_file_dir - this is the directory of the key file
	// @param output_file_dir - this is the directory of the output 
	// @param work_dir - this is where the map reduce temp files are stored
	// @param max_key_bytes - this is the maximum possible number of bytes
	//                      - that make up a key
	// @param maxp_map_bytes - this is the maximum possible number of bytes 
	//                       - that make up a map
	// @param hash_div_num - this is the number of merged sets to create
	static void KeyOccurrenceFloat(const char *data_handle_func, 
		CSegFile &key_file, CSegFile &output_file, const char *work_dir,
		int max_key_bytes, int hash_div_num = -1) {

		key_file.CloseFile();
		output_file.CloseFile();

		IssueRequest(data_handle_func, "NULL", key_file.GetRootFileName(), 
			output_file.GetRootFileName(),
			work_dir, max_key_bytes, 0, "KeyOccurrence", "float", hash_div_num);
	}


	// This finds the occurrence of each unique key and applies the occurrence to each key
	// @param data_handle_func - this is the name of the data handler function
	// @param key_file_dir - this is the directory of the key file
	// @param output_file_dir - this is the directory of the output 
	// @param work_dir - this is where the map reduce temp files are stored
	// @param max_key_bytes - this is the maximum possible number of bytes
	//                      - that make up a key
	static void DuplicateKeyOccurrence(const char *data_handle_func, 
		const char *key_file_dir, const char *output_file_dir, const char *work_dir,
		int max_key_bytes) {

		IssueRequest(data_handle_func, "NULL", key_file_dir, output_file_dir,
			work_dir, max_key_bytes, 0, "DuplicateKeyOccurrence", "int", -1);
	}

	// This finds the occurrence of each unique key and applies the occurrence to each key
	// @param data_handle_func - this is the name of the data handler function
	// @param key_file_dir - this is the directory of the key file
	// @param output_file_dir - this is the directory of the output 
	// @param work_dir - this is where the map reduce temp files are stored
	// @param max_key_bytes - this is the maximum possible number of bytes
	//                      - that make up a key
	static void DuplicateKeyOccurrenceLong(const char *data_handle_func, 
		CSegFile &key_file, CSegFile &output_file, const char *work_dir,
		int max_key_bytes) {

		key_file.CloseFile();
		output_file.CloseFile();

		IssueRequest(data_handle_func, "NULL", key_file.GetRootFileName(),
			output_file.GetRootFileName(),
			work_dir, max_key_bytes, 0, "DuplicateKeyOccurrence", "int", -1);
	}

	// This finds the occurrence of each unique key and applies the occurrence to each key
	// @param data_handle_func - this is the name of the data handler function
	// @param key_file_dir - this is the directory of the key file
	// @param output_file_dir - this is the directory of the output 
	// @param work_dir - this is where the map reduce temp files are stored
	// @param max_key_bytes - this is the maximum possible number of bytes
	//                      - that make up a key
	static void DuplicateKeyOccurrenceFloat(const char *data_handle_func, 
		CSegFile &key_file, CSegFile &output_file, const char *work_dir,
		int max_key_bytes) {

		key_file.CloseFile();
		output_file.CloseFile();

		IssueRequest(data_handle_func, "NULL", key_file.GetRootFileName(),
			output_file.GetRootFileName(),
			work_dir, max_key_bytes, 0, "DuplicateKeyOccurrence", "float", -1);
	}

	// This finds the key weight for each unique key
	// @param data_handle_func - this is the name of the data handler function
	// @param key_file_dir - this is the directory of the key file
	// @param output_file_dir - this is the directory of the output 
	// @param work_dir - this is where the map reduce temp files are stored
	// @param max_key_bytes - this is the maximum possible number of bytes
	//                      - that make up a key
	// @param hash_div_num - this is the number of merged sets to create
	static void KeyWeight(const char *data_handle_func, 
		const char *key_file_dir, const char *output_file_dir, const char *work_dir,
		int max_key_bytes, int hash_div_num = -1) {

		IssueRequest(data_handle_func, "NULL", key_file_dir, output_file_dir,
			work_dir, max_key_bytes, 0, "KeyWeight", "int", hash_div_num);
	}

	// This finds the key weight for each unique key
	// @param data_handle_func - this is the name of the data handler function
	// @param key_file_dir - this is the directory of the key file
	// @param output_file_dir - this is the directory of the output 
	// @param work_dir - this is where the map reduce temp files are stored
	// @param max_key_bytes - this is the maximum possible number of bytes
	//                      - that make up a key
	// @param hash_div_num - this is the number of merged sets to create
	static void KeyWeightLong(const char *data_handle_func, 
		CSegFile &key_file, CSegFile &output_file, const char *work_dir,
		int max_key_bytes, int hash_div_num = -1) {

		key_file.CloseFile();
		output_file.CloseFile();

		IssueRequest(data_handle_func, "NULL", key_file.GetRootFileName(), 
			output_file.GetRootFileName(), work_dir, max_key_bytes, 0,
			"KeyWeight", "int", hash_div_num);
	}

	// This finds the key weight for each unique key
	// @param data_handle_func - this is the name of the data handler function
	// @param key_file_dir - this is the directory of the key file
	// @param output_file_dir - this is the directory of the output 
	// @param work_dir - this is where the map reduce temp files are stored
	// @param max_key_bytes - this is the maximum possible number of bytes
	//                      - that make up a key
	// @param hash_div_num - this is the number of merged sets to create
	static void KeyWeightFloat(const char *data_handle_func, 
		CSegFile &key_file, CSegFile &output_file, const char *work_dir,
		int max_key_bytes, int hash_div_num = -1) {

		key_file.CloseFile();
		output_file.CloseFile();

		IssueRequest(data_handle_func, "NULL", key_file.GetRootFileName(), 
			output_file.GetRootFileName(), work_dir, max_key_bytes, 0,
			"KeyWeight", "float", hash_div_num);
	}

	// This finds the key weight for each unique key and applies the weight to each key
	// @param data_handle_func - this is the name of the data handler function
	// @param key_file_dir - this is the directory of the key file
	// @param output_file_dir - this is the directory of the output 
	// @param work_dir - this is where the map reduce temp files are stored
	// @param max_key_bytes - this is the maximum possible number of bytes
	//                      - that make up a key
	static void DuplicateKeyWeight(const char *data_handle_func, 
		const char *key_file_dir, const char *output_file_dir, const char *work_dir,
		int max_key_bytes) {

		IssueRequest(data_handle_func, "NULL", key_file_dir, output_file_dir,
			work_dir, max_key_bytes, 0, "DuplicateKeyWeight", "int", -1);
	}

	// This finds the key weight for each unique key and applies the weight to each key
	// @param data_handle_func - this is the name of the data handler function
	// @param key_file_dir - this is the directory of the key file
	// @param output_file_dir - this is the directory of the output 
	// @param work_dir - this is where the map reduce temp files are stored
	// @param max_key_bytes - this is the maximum possible number of bytes
	//                      - that make up a key
	static void DuplicateKeyWeightLong(const char *data_handle_func, 
		CSegFile &key_file, CSegFile &output_file, const char *work_dir,
		int max_key_bytes) {

		IssueRequest(data_handle_func, "NULL", key_file.GetRootFileName(),
			output_file.GetRootFileName(), work_dir, max_key_bytes, 0,
			"DuplicateKeyWeight", "int", -1);
	}

	// This merges key value pairs by their key value in the same physical location
	// @param data_handle_func - this is the name of the data handler function
	// @param map_file_dir - this is the directory of the map_file
	// @param output_file_dir - this is the directory of the output 
	// @param work_dir - this is where the map reduce temp files are stored
	// @param max_key_bytes - this is the maximum possible number of bytes
	//                      - that make up a key
	// @param maxp_map_bytes - this is the maximum possible number of bytes 
	//                       - that make up a map
	// @param hash_div_num - this is the number of merged sets to create
	static void MergeSet(const char *data_handle_func, const char *map_file_dir, 
		const char *output_file_dir, const char *work_dir,
		int max_key_bytes, int max_map_bytes, int hash_div_num = -1) {

		IssueRequest(data_handle_func, map_file_dir, "NULL", output_file_dir,
			work_dir, max_key_bytes, max_map_bytes, "MergeSet", "int", hash_div_num);
	}

	// This merges key value pairs by their key value in the same physical location
	// @param data_handle_func - this is the name of the data handler function
	// @param map_file_dir - this is the directory of the map_file
	// @param output_file_dir - this is the directory of the output 
	// @param work_dir - this is where the map reduce temp files are stored
	// @param max_key_bytes - this is the maximum possible number of bytes
	//                      - that make up a key
	// @param maxp_map_bytes - this is the maximum possible number of bytes 
	//                       - that make up a map
	// @param hash_div_num - this is the number of merged sets to create
	static void MergeSet(const char *data_handle_func, CSegFile &map_file, 
		CSegFile &output_file, const char *work_dir,
		int max_key_bytes, int max_map_bytes, int hash_div_num = -1) {

		IssueRequest(data_handle_func, map_file.GetRootFileName(), "NULL",
			output_file.GetRootFileName(), work_dir, max_key_bytes, max_map_bytes,
			"MergeSet", "int", hash_div_num);
	}

	// This merges key value pairs by their key value in the same physical location
	// @param data_handle_func - this is the name of the data handler function
	// @param map_file_dir - this is the directory of the map_file
	// @param output_file_dir - this is the directory of the output 
	// @param work_dir - this is where the map reduce temp files are stored
	// @param max_key_bytes - this is the maximum possible number of bytes
	//                      - that make up a key
	// @param maxp_map_bytes - this is the maximum possible number of bytes 
	//                       - that make up a map
	// @param hash_div_num - this is the number of merged sets to create
	static void MergeSortedSet(const char *data_handle_func, const char *map_file_dir, 
		const char *output_file_dir, const char *work_dir,
		int max_key_bytes, int max_map_bytes, int hash_div_num = -1) {

		IssueRequest(data_handle_func, map_file_dir, "NULL", output_file_dir,
			work_dir, max_key_bytes, max_map_bytes, "MergeSortedSet", "int", hash_div_num);
	}

	// This merges key value pairs by their key value in the same physical location
	// @param data_handle_func - this is the name of the data handler function
	// @param map_file_dir - this is the directory of the map_file
	// @param output_file_dir - this is the directory of the output 
	// @param work_dir - this is where the map reduce temp files are stored
	// @param max_key_bytes - this is the maximum possible number of bytes
	//                      - that make up a key
	// @param maxp_map_bytes - this is the maximum possible number of bytes 
	//                       - that make up a map
	// @param hash_div_num - this is the number of merged sets to create
	static void MergeSortedSet(const char *data_handle_func, CSegFile &map_file, 
		CSegFile &output_file, const char *work_dir,
		int max_key_bytes, int max_map_bytes, int hash_div_num = -1) {

		IssueRequest(data_handle_func, map_file.GetRootFileName(), "NULL",
			output_file.GetRootFileName(), work_dir, max_key_bytes, max_map_bytes,
			"MergeSortedSet", "int", hash_div_num);
	}
};
COpenConnection CMapReduce::m_conn;
CProcessSet CMapReduce::m_process_set;
