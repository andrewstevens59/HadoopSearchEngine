#include "./NodeStat.h"

// This finds the kth order statistic for a set of elements. This uses
// the random binary partion where the median is approximated by the 
// average. The k-th order is found externally by hashing all elements
// into three buckets at each level. An equal bucket, a less than bucket
// and a greater than bucket. The number of elements in each bucket and
// the bucket containing the kth order statistic is recursed on. If the 
// kth order statistic belongs in the equal bucket than the search can
// terminate. Once all elements can fit into memory an internal version
// of the above procedure is used.

// @param X - the type of kth order statistic
// @param Y - this is either a File or a FileComp
template <class X, class Y> class CKthOrderStat {

	// This defines averages for the three different buckets
	// as well as the number of elements
	struct SBucketAvg {
		// this is the number of elements in the equal bucket
		_int64 equal_num;

		// this is the average for the equal bucket
		float greater_than_avg;
		// this is the number of elements in the equal bucket
		_int64 greater_than_num;

		// this is the average for the equal bucket
		float less_than_avg;
		// this is the number of elements in the equal bucket
		_int64 less_than_num;

		void Reset() {
			equal_num = 0;
			greater_than_avg = 0;
			greater_than_num = 0;
			less_than_avg = 0;
			less_than_num = 0;
		}
	};
	
	// This is used for testing 
	struct STestThread {
		int client_id;
		int client_num;
	};

	// This defines a LocalData structure that holds elements
	// less than greater than and equal to, externally
	template <class Z> struct SExternalCompare {
		// all elements less than the median
		Z less_than;
		// all elements greater than the median
		Z greater_than;

		// This creates a level for writing
		void OpenWriteFile(const char dir[], int level) {
			less_than.OpenWriteFile(CUtility::ExtendString
				(dir, ".less_than", level));
			less_than.InitializeCompression();

			greater_than.OpenWriteFile(CUtility::ExtendString
				(dir, ".greater_than", level));
			greater_than.InitializeCompression();
		}

		// This creates a level for reading
		void OpenReadFile(const char dir[], int level) { 
			less_than.OpenReadFile(CUtility::ExtendString
				(dir, ".less_than", level));
			less_than.InitializeCompression();

			greater_than.OpenReadFile(CUtility::ExtendString
				(dir, ".greater_than", level));
			greater_than.InitializeCompression();
		}

		// This removes a level
		static void RemoveLevel(const char dir[], int level) {

			if(level < 0) {
				return;
			}

			remove(CUtility::ExtendString
				(dir, ".less_than", level));
			remove(CUtility::ExtendString
				(dir, ".greater_than", level));
		}

		// This closes both the files
		void CloseFile() {
			less_than.CloseFile();
			greater_than.CloseFile();
		}
	};

	// This defines a LocalData structure that holds elements
	// less than greater than and equal to, internally
	template <class Z> struct SInternalCompare {
		// all elements less than the median
		CLinkedBuffer<Z> less_than;
		// all elements greater than the median
		CLinkedBuffer<Z> greater_than;
	};

	// This stores the directory of the kth order statistic
	char m_directory[500];
	// This stores the maximum number of bytes allowed
	// to be occupied internally
	int m_max_mem_size;
	// This stores the current level being processed
	int m_curr_level;


	// This reads an element from file
	bool (*m_read_element)(Y &from_file, X &element);
	// This writes an element to file
	void (*m_write_element)(Y &to_file, X &element);

	// This puts elements into their correct bucket externally
	// @param file - this is the file that holds the kth order statistic
	// @param median - this is the median of all three buckets
	//               - (i.e. <, >, =)

	// @param next_level - this is the next level bucket set
	// @param avg - this stores the averages and number of elements in
	//            - all three buckets for the next level
	void HashExternally(Y &file, float &median,
		SExternalCompare<Y> &next_level, SBucketAvg &avg) {

		X element;
		next_level.OpenWriteFile(m_directory, m_curr_level);
		avg.Reset();

		file.OpenReadFile();

		while(m_read_element(file, element)) {
			if(element > median) {
				m_write_element(next_level.greater_than, element);
				avg.greater_than_avg += element;
				avg.greater_than_num++;
				continue;
			}

			if(element < median) {
				m_write_element(next_level.less_than, element);
				avg.less_than_avg += element;
				avg.less_than_num++;
				continue;
			}
			avg.equal_num++;
		}

		next_level.CloseFile();
		file.CloseFile();
		SExternalCompare<Y>::RemoveLevel(m_directory, m_curr_level - 1);
	}

	// This puts elements into their correct bucket internally
	// @param file - this is the file that holds the kth order statistic
	// @param median - this is the median of all three buckets
	//               - (i.e. <, >, =)
	// @param next_level - this is the next level bucket set (internal)
	// @param avg - this stores the averages and number of elements in
	//            - all three buckets for the next level
	void HashInternally(Y &file, float &median,
		SInternalCompare<float> &next_level, SBucketAvg &avg) {

		X element;
		avg.Reset();
		next_level.greater_than.Initialize();
		next_level.less_than.Initialize();

		file.OpenReadFile();

		while(m_read_element(file, element)) {
			if(element > median) {
				next_level.greater_than.PushBack(element);
				avg.greater_than_avg += element;
				avg.greater_than_num++;
				continue;
			}

			if(element < median) {
				next_level.less_than.PushBack(element);
				avg.less_than_avg += element;
				avg.less_than_num++;
				continue;
			}

			avg.equal_num++;
		}

		file.CloseFile();
		SExternalCompare<Y>::RemoveLevel(m_directory, m_curr_level - 1);
	}

	// This continues to hash internally with an existing internal bucket set.
	// @param curr_level - this is the bucket set for the current level 
	//                   - it's stored internally
	// @param median - this is the median of all three buckets
	//               - (i.e. <, >, =)
	// @param next_level - this is the next level bucket set (internal)
	// @param avg - this stores the averages and number of elements in
	//            - all three buckets for the next level
	void HashInternally(CLinkedBuffer<float> &curr_level, float &median,
		SInternalCompare<float> &next_level, SBucketAvg &avg) {

		next_level.greater_than.Initialize();
		next_level.less_than.Initialize();
		avg.Reset();

		float *curr_element;
		curr_level.ResetPath();
		while((curr_element = curr_level.NextNode())) {

			if(*curr_element > median) {
				next_level.greater_than.PushBack(*curr_element);
				avg.greater_than_avg += *curr_element;
				avg.greater_than_num++;
				continue;
			}

			if(*curr_element < median) {
				next_level.less_than.PushBack(*curr_element);
				avg.less_than_avg += *curr_element;
				avg.less_than_num++;
				continue;
			}
			avg.equal_num++;
		}
	}

	// This is responsible for processing a set of of bucket sets on
	// the current level. These can either be external or internal
	// dependiing upon the amount of internal memory available.
	// @param curr_level_ptr - this is the current level internal bucket
	//                       - set that contains all three groups
	// @param next_level_ptr - this stores the internal bucket sets for the next
	//                       - level bucket sets
	// @param curr_file - this is the file on which to recurse for the next
	//                  - level, this is either the < than or > than
	// @param bucket_stat - this stores statistic relating to each bucket 
	//                    - on the current level like average and element number
	// @param curr_bucket - this is the internal bucket that is chosen to be 
	//                    - recursed on the next level
	// @param average - this is the median element used to place elements into 
	//                - their correct bucket
	// @param ext_bucket_set - this holds the external buckets for the next level
	// @param buff2 - this is the actual buffer used to store internally the 
	//              - next level bucket set
	// @param byte_size - this is the required internal memory occupancy
	void ProcessBucketSets(SInternalCompare<float> **curr_level_ptr,
		SInternalCompare<float> **next_level_ptr, Y &curr_file,
		SBucketAvg &bucket_stat, CLinkedBuffer<float> *curr_bucket,
		float &median, SExternalCompare<Y> &ext_bucket_set,
		SInternalCompare<float> &buff2, _int64 &byte_size) {

		if(*next_level_ptr == NULL) {
			if(byte_size < m_max_mem_size) {
				*next_level_ptr = &buff2;
				HashInternally(curr_file, median, 
					**next_level_ptr, bucket_stat);
			} else {
				HashExternally(curr_file, median,
					ext_bucket_set, bucket_stat);
			}

			return;
		}

		CSort<char>::Swap(*curr_level_ptr, *next_level_ptr);

		(*next_level_ptr)->greater_than.FreeMemory();
		(*next_level_ptr)->less_than.FreeMemory();

		HashInternally(*curr_bucket, median, 
			**next_level_ptr, bucket_stat);
	}

	// This chooses the bucket on which to recurse for the next level,
	// depending upon the location of the kth order statistic. The 
	// kth order statistic itself must also be modified if it is in
	// the greater than bucket to be the offset in this bucket.
	// @param kth_order - this is the rank of the kth order statistic
	// @param curr_file - this is the file on which to recurse for the next
	//                  - level, this is either the < than or > than
	// @param average - this is the average of all elements in the next 
	//                - recursion bucket
	// @param element_num - this stores the number of elments in the chosen
	//                    - recursion bucket
	// @param bucket_stat - this stores statistic relating to each bucket 
	//                    - on the current level like average and element number
	// @param curr_bucket - this is the internal bucket that is chosen to be 
	//                    - recursed on the next level
	// @param next_level_ptr - this stores the internal bucket sets for the next
	//                       - level bucket sets
	// @param ext_bucket_set - this holds the external buckets for the next level
	// @return false if cannot be recursed further, true otherwise
	bool ChooseRecursionBucket(_int64 &kth_order, Y **curr_file,
		float &average, _int64 &element_num, SBucketAvg &bucket_stat,
		CLinkedBuffer<float> **curr_bucket, SInternalCompare<float> &next_level_ptr,
		SExternalCompare<Y> &ext_bucket_set) {

		if(bucket_stat.less_than_num == 0) {
			return false;
		}

		if(bucket_stat.greater_than_num == 0) {
			return false;
		}

		if(kth_order < bucket_stat.less_than_num) {
			element_num = bucket_stat.less_than_num;
			average = bucket_stat.less_than_avg / element_num;
			*curr_file = &ext_bucket_set.less_than;
			*curr_bucket = &next_level_ptr.less_than;
			return true;
		}

		_int64 greater_than_num = bucket_stat.less_than_num 
			+ bucket_stat.equal_num;

		if(kth_order >= greater_than_num) {
			element_num = bucket_stat.greater_than_num;
			kth_order -= greater_than_num;

			average = bucket_stat.greater_than_avg / element_num;
			*curr_file = &ext_bucket_set.greater_than;
			*curr_bucket = &next_level_ptr.greater_than;
		}
		

		return true;
	}

	// This is a test thread
	static thread_return TestThread(void *ptr) {
		TestClient(*(STestThread *)ptr);

		return 0;
	}

	// This is used for testing
	static bool ReadElement(CHDFSFile &file, int &element) {
		return file.ReadCompObject(element);
	}

	// This is used for testing
	static void WriteElement(CHDFSFile &file, int &element) {
		file.WriteCompObject(element);
	}

	// This is used for testing
	static int ComparetTestElement(const int &arg1, const int &arg2) {

		if(arg1 < arg2) {
			return 1;
		}

		if(arg1 > arg2) {
			return -1;
		}

		return 0;
	}

public:

	CKthOrderStat() {
	}

	// This is called to initialize the class and set internal
	// variables needed for completion.
	// @param max_mem_size - This stores the maximum number of 
	//                     - bytes allowed to be occupied internally
	// @param dir - this is the directory of the kth order statistic
	// @param read_element - this reads an element from file
	// @param write_element - this writes an element to file
	CKthOrderStat(int max_mem_size, const char dir[], 
		bool (*read_element)(Y &from_file, float &element),
		void (*write_element)(Y &to_file, float &element)) {

		Initialize(max_mem_size, dir, read_element, write_element);
	}

	// This is called to initialize the class and set internal
	// variables needed for completion.
	// @param max_mem_size - This stores the maximum number of 
	//                     - bytes allowed to be occupied internally
	// @param dir - this is the directory of the kth order statistic
	// @param read_element - this reads an element from file
	// @param write_element - this writes an element to file
	void Initialize(int max_mem_size, const char dir[],
		bool (*read_element)(Y &from_file, X &element),
		void (*write_element)(Y &to_file, X &element)) {

		m_max_mem_size = max_mem_size;
		m_read_element = read_element;
		m_write_element = write_element;
		strcpy(m_directory, dir);
		m_curr_level = -1;
	}

	// This is the entry function that finds the kth order statistic.
	// @param file - this is where all the elements are stored
	// @param kth_order - this is the rank of the kth order statistic
	// @return float - this is the kth order statistic
	float FindKthOrderStat(Y &file, _int64 kth_order) {

		X element;
		float average = 0;
		file.OpenReadFile();

		// First finds the average
		_int64 element_num = 0;
		while(m_read_element(file, element)) {
			average += element;
			element_num++;
		}

		SInternalCompare<float> buff1;
		SInternalCompare<float> buff2;
		SInternalCompare<float> *curr_level_ptr = &buff1;
		SInternalCompare<float> *next_level_ptr = NULL;

		file.ResetReadFile();
		CLinkedBuffer<float> *curr_bucket = NULL;
		SExternalCompare<Y> ext_bucket_set1;
		SExternalCompare<Y> ext_bucket_set2;
		SExternalCompare<Y> *ext_bucket_ptr1 = &ext_bucket_set1;
		SExternalCompare<Y> *ext_bucket_ptr2 = &ext_bucket_set2;

		SBucketAvg bucket_stat;
		average /= element_num;
		Y *curr_file = &file;

		while(true) {
			_int64 byte_size = element_num * sizeof(float);
			if(++m_curr_level > 100) {
				throw EException("Corrupted LocalData");
			}

			ProcessBucketSets(&curr_level_ptr, &next_level_ptr,
				*curr_file, bucket_stat, curr_bucket, average, 
				*ext_bucket_ptr1, buff2, byte_size);

			if(bucket_stat.equal_num == 0) {
				if(bucket_stat.less_than_num == 0) {
					break;
				}

				if(bucket_stat.greater_than_num == 0) {
					break;
				}
			}

			if(kth_order >= bucket_stat.less_than_num && kth_order - 
				bucket_stat.less_than_num < bucket_stat.equal_num) {
				// kth order in the equal bucket
				break;
			}

			if(ChooseRecursionBucket(kth_order, &curr_file, average, element_num, bucket_stat, 
				&curr_bucket, *next_level_ptr, *ext_bucket_ptr1) == false) {
				break;
			}

			CSort<char>::Swap(ext_bucket_ptr1, ext_bucket_ptr2);
		}

		return average;
	}

	// This is just a test framework
	static void TestKthOrderStatistic() {

		CArrayList<int> test_buff(4);
		CHDFSFile test_file;
		test_file.OpenWriteFile("test_file");
		test_file.InitializeCompression();

		for(int i=0; i<5000; i++) {
			test_buff.PushBack(rand());
			test_file.WriteCompObject(test_buff.LastElement());
		}

		test_file.CloseFile();
		CArrayList<int> compare_buff(test_buff);

		CSort<int> sort(compare_buff.Size(), ComparetTestElement);
		sort.HybridSort(compare_buff.Buffer());

		for(int i=0; i<5000; i += 10) {
			CKthOrderStat<int, CHDFSFile> test(500, "test", ReadElement, WriteElement);
			int id = test.FindKthOrderStat(test_file, i);
			if(id != compare_buff[i]) {
				cout<<"error";getchar();
			}cout<<i<<endl;
		}
	}
};

// This is the becon class to notify the master that the client is still alive
class CBeacon {

	#ifdef OS_WINDOWS
	// This stores the client id of the slave
	static int m_client_id;
	// This stores the communication port
	static int m_port;
	// This is used to send a status beacon to notify command
	// that it is still alive
	static COpenConnection m_beacon;
	// This is used to protect access to the network connection
	static CMutex m_mutex;
	// This stores the handle of the beacon thread
	static HANDLE m_handle;
	#endif

	// This sends out a beacon to the command server to notify
	// that this client is still alive
	static THREAD_RETURN1 THREAD_RETURN2 BeaconThread(void *ptr1) {

		#ifdef OS_WINDOWS
		CMemoryChunk<char> buff(20);

		while(true) {
			m_mutex.Acquire();
			sprintf(buff.Buffer(), "a %d", (int)m_client_id);
			m_beacon.SendUDP(buff.Buffer(), 20);
			m_mutex.Release();
			Sleep(100);
		}

		#endif

		return 0;
	}

public:

	CBeacon() {
	}

	// This starts the beacon thread to indicate that a process 
	// is still alive. This is processed by the command server.
	static void InitializeBeacon(int client_id, int port = 5555) {
		#ifdef OS_WINDOWS
		m_client_id = client_id;
		m_beacon.OpenClientUDPConnection(port, LOCAL_NETWORK);

		unsigned int threadID;
		m_handle = (HANDLE)_beginthreadex(NULL, 0, BeaconThread, NULL, NULL, &threadID);
		#endif
	}

	// This sends the termination signal to the MapReduce command
	static void SendTerminationSignal() {

		#ifdef OS_WINDOWS
		CMemoryChunk<char> buff(20);

		for(int i=0; i<1; i++) {
			m_mutex.Acquire();
			sprintf(buff.Buffer(), "f %d", (int)m_client_id);
			m_beacon.SendUDP(buff.Buffer(), 20);
			m_mutex.Release();
		}
		#endif
	}

	~CBeacon() {
		#ifdef OS_WINDOWS
		m_mutex.Acquire();
		TerminateThread(m_handle, 0);
		#endif
	}
};
#ifdef OS_WINDOWS
int CBeacon::m_client_id;
int CBeacon::m_port;
COpenConnection CBeacon::m_beacon;
CMutex CBeacon::m_mutex;
HANDLE CBeacon::m_handle;
#endif

// This is used as a general class in query processing to 
// issue requests to each of the different server types
class CNameServerStat {

	// This stores the name server port
	static int m_name_server_port;
	// This stores the listening port
	static int m_listen_port;
	// This stores the id of the server
	static int m_server_id;
	// This stores the server type id
	static int m_server_type_id;
	// This is used to send a status beacon to notify command
	// that it is still alive
	static COpenConnection m_beacon;
	// This is used to receive incoming connections from the name server
	static COpenConnection m_name_server_conn;
	// This is used to protect access to the network connection
	static CMutex m_mutex;
	// This stores the handle of the beacon thread
	static HANDLE m_handle;
	// This stores the number of unchecked notifications
	static int m_timeout;

	// This sends out a beacon to the command server to notify
	// that this client is still alive
	static THREAD_RETURN1 THREAD_RETURN2 BeaconThread(void *ptr1) {

		CMemoryChunk<char> buff(20);

		while(true) {

			m_mutex.Acquire();
			sprintf(buff.Buffer(), "a %d %d", (int)m_server_id, (int)m_server_type_id);

			m_beacon.SendUDP(buff.Buffer(), 20);

			if(++m_timeout >= 10) {
				exit(0);
			}
			m_mutex.Release();

			Sleep(100);
		}
	}

	// This checks for notifications given by the NameServer 
	// to verify that the NameServer is still alive. It
	// will terminate if the name server is not alive
	static THREAD_RETURN1 THREAD_RETURN2 NameServerThread(void *ptr1) {

		CMemoryChunk<char> buff(20);

		while(m_name_server_conn.ReceiveUDP(buff.Buffer(), 20) >= 0) {
			if(CUtility::FindFragment(buff.Buffer(), "Notify")) {
				m_mutex.Acquire();
				m_timeout = 0;
				m_mutex.Release();
			}
		}

		exit(0);
	}

public:

	CNameServerStat() {
		m_timeout = 0;
	}

	// This sets the id of the server
	inline static void SetServerID(int server_id) {
		m_server_id = server_id;
	}

	// This sets the name server port
	inline static void SetNameServerPort(int port) {
		m_name_server_port = port;
	}
	
	// This sets the listen port
	inline static void SetListenPort(int port) {
		m_listen_port = port;
	}

	// This sets the server type id
	inline static void SetServerTypeID(int server_id) {
		m_server_type_id = server_id;
	}

	// This returns the name server port
	inline static int GetNameServerPort() {
		return m_name_server_port;
	}
	
	// This returns the listen port
	inline static int GetListenPort() {
		return m_listen_port;
	}

	// This returns the id of the server
	inline static int GetServerID() {
		return m_server_id;
	}

	// This returns the server type id
	inline static int GetServerTypeID() {
		return m_server_type_id;
	}

	// This sends a beacon response to a neighbouring 
	// name server to notify its still alive.
	static void InitializeNameServerBeacon(u_short port) {

		unsigned int threadID;
		m_beacon.OpenClientUDPConnection(port, LOCAL_NETWORK);
		m_handle = (HANDLE)_beginthreadex(NULL, 0, BeaconThread, NULL, NULL, &threadID);
	}

	// This sends a setup response back to the server
	// to indicate that it is ready to accept connections
	static void InitializeBeacon() {

		CMemoryChunk<char> buff(20);
		m_beacon.OpenClientUDPConnection(GetNameServerPort(), LOCAL_NETWORK);
		m_name_server_conn.OpenServerUDPConnection(GetListenPort(), LOCAL_NETWORK);
		cout<<GetListenPort()<<endl;

		sprintf(buff.Buffer(), "a %d %d", (int)m_server_id, (int)m_server_type_id);
		m_beacon.SendUDP(buff.Buffer(), 20);

		unsigned int threadID;
		m_handle = (HANDLE)_beginthreadex(NULL, 0, BeaconThread, NULL, NULL, &threadID);
		_beginthreadex(NULL, 0, NameServerThread, NULL, NULL, &threadID);
	}

	~CNameServerStat() {
		m_mutex.Acquire();
		TerminateThread(m_handle, 0);
	}

};

int CNameServerStat::m_name_server_port;
int CNameServerStat::m_listen_port;
int CNameServerStat::m_server_id;
int CNameServerStat::m_server_type_id;
COpenConnection CNameServerStat::m_beacon;
CMutex CNameServerStat::m_mutex;
HANDLE CNameServerStat::m_handle;
int CNameServerStat::m_timeout;
COpenConnection CNameServerStat::m_name_server_conn;
