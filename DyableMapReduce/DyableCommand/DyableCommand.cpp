#include "./DistributeCompBlocks.h"

// This is responsible for handling MapReduce requests being given 
// by connecting clients. It then executes the necessary MapReduce
// primatives in order to complete the entire MapReduce request.
class CMapReduceCommand {

	// This stores connections for listening for requests
	COpenConnection m_connect;
	// Stores the html tags used in looking up the handler functions
	CHashDictionary<short> m_meta_tag;
	// This is used to initiate different map reduce primatives
	CDistributeCompBlocks *m_mapred_prim_ptr;

	// This is the external hash map function that applies maps to keys
	// @param data_handle_func - this is the name of the data handler function
	// @param map_file_dir - this is the directory of the map_file
	// @param key_file_dir - this is the directory of the key file
	// @param output_file_dir - this is the directory of the output 
	// @param work_dir - this is where the map reduce temp files are stored
	// @param max_key_bytes - this is the maximum possible number of bytes
	//                      - that make up a key
	// @param max_map_bytes - this is the maximum possible number of bytes 
	//                       - that make up a map
	void ExternalHashMap(const char *data_handle_func, const char *map_file_dir, 
		const char *key_file_dir, const char *output_file_dir, const char *work_dir,
		int max_key_bytes, int max_map_bytes) {

		m_mapred_prim_ptr->SetAttributes(work_dir, map_file_dir, max_key_bytes, max_map_bytes);
		m_mapred_prim_ptr->DistributeMaps(data_handle_func);

		m_mapred_prim_ptr->SetAttributes(work_dir, key_file_dir, max_key_bytes, max_map_bytes);
		m_mapred_prim_ptr->DistributeKeys(data_handle_func);

		m_mapred_prim_ptr->ApplyMapsToKeys(data_handle_func);
		m_mapred_prim_ptr->SetAttributes(work_dir, output_file_dir, max_key_bytes, max_map_bytes);
		m_mapred_prim_ptr->OrderMappedSets(data_handle_func);
	}

	// This finds the occurrence of each unique key
	// @param data_handle_func - this is the name of the data handler function
	// @param map_file_dir - this is the directory of the map_file
	// @param key_file_dir - this is the directory of the key file
	// @param output_file_dir - this is the directory of the output 
	// @param work_dir - this is where the map reduce temp files are stored
	// @param max_key_bytes - this is the maximum possible number of bytes
	//                      - that make up a key
	// @param max_map_bytes - this is the maximum possible number of bytes 
	//                       - that make up a map
	void KeyOccurrence(const char *data_handle_func, const char *map_file_dir, 
		const char *key_file_dir, const char *output_file_dir, const char *work_dir,
		int max_key_bytes, int max_map_bytes) {

		m_mapred_prim_ptr->SetAttributes(work_dir, key_file_dir, max_key_bytes, max_map_bytes);
		m_mapred_prim_ptr->DistributeKeys(data_handle_func);

		m_mapred_prim_ptr->SetAttributes(work_dir, output_file_dir, max_key_bytes, max_map_bytes);
		m_mapred_prim_ptr->FindKeyOccurrence(data_handle_func);
	}

	// This finds the occurrence of each unique key and applies the occurrence to each key
	// @param data_handle_func - this is the name of the data handler function
	// @param map_file_dir - this is the directory of the map_file
	// @param key_file_dir - this is the directory of the key file
	// @param output_file_dir - this is the directory of the output 
	// @param work_dir - this is where the map reduce temp files are stored
	// @param max_key_bytes - this is the maximum possible number of bytes
	//                      - that make up a key
	// @param max_map_bytes - this is the maximum possible number of bytes 
	//                       - that make up a map
	void DuplicateKeyOccurrence(const char *data_handle_func, const char *map_file_dir, 
		const char *key_file_dir, const char *output_file_dir, const char *work_dir,
		int max_key_bytes, int max_map_bytes) {

		m_mapred_prim_ptr->SetAttributes(work_dir, key_file_dir, max_key_bytes, max_map_bytes);
		m_mapred_prim_ptr->DistributeKeys(data_handle_func);

		m_mapred_prim_ptr->FindDuplicateKeyOccurrence(data_handle_func);

		m_mapred_prim_ptr->SetAttributes(work_dir, output_file_dir, max_key_bytes, max_map_bytes);
		m_mapred_prim_ptr->OrderMappedOccurrences(data_handle_func);
	}

	// This finds the key weight for each unique key
	// @param data_handle_func - this is the name of the data handler function
	// @param map_file_dir - this is the directory of the map_file
	// @param key_file_dir - this is the directory of the key file
	// @param output_file_dir - this is the directory of the output 
	// @param work_dir - this is where the map reduce temp files are stored
	// @param max_key_bytes - this is the maximum possible number of bytes
	//                      - that make up a key
	// @param maxp_map_bytes - this is the maximum possible number of bytes 
	//                       - that make up a map
	void KeyWeight(const char *data_handle_func, const char *map_file_dir, 
		const char *key_file_dir, const char *output_file_dir, const char *work_dir,
		int max_key_bytes, int max_map_bytes) {

		m_mapred_prim_ptr->SetAttributes(work_dir, key_file_dir, max_key_bytes, max_map_bytes);
		m_mapred_prim_ptr->DistributeKeyWeight(data_handle_func);

		m_mapred_prim_ptr->SetAttributes(work_dir, output_file_dir, max_key_bytes, max_map_bytes);
		m_mapred_prim_ptr->FindKeyWeight(data_handle_func);
	}

	// This finds the key weight for each unique key and applies the weight to each key
	// @param data_handle_func - this is the name of the data handler function
	// @param map_file_dir - this is the directory of the map_file
	// @param key_file_dir - this is the directory of the key file
	// @param output_file_dir - this is the directory of the output 
	// @param work_dir - this is where the map reduce temp files are stored
	// @param max_key_bytes - this is the maximum possible number of bytes
	//                      - that make up a key
	// @param maxp_map_bytes - this is the maximum possible number of bytes 
	//                       - that make up a map
	void DuplicateKeyWeight(const char *data_handle_func, const char *map_file_dir, 
		const char *key_file_dir, const char *output_file_dir, const char *work_dir,
		int max_key_bytes, int max_map_bytes) {


		m_mapred_prim_ptr->SetAttributes(work_dir, key_file_dir, max_key_bytes, max_map_bytes);
		m_mapred_prim_ptr->DistributeKeyWeight(data_handle_func);

		m_mapred_prim_ptr->SetAttributes(work_dir, output_file_dir, max_key_bytes, max_map_bytes);
		m_mapred_prim_ptr->FindDuplicateKeyWeight(data_handle_func);

		m_mapred_prim_ptr->SetAttributes(work_dir, output_file_dir, max_key_bytes, max_map_bytes);
		m_mapred_prim_ptr->OrderMappedOccurrences(data_handle_func);
	}

	// This merges key value pairs by their key value in the same physical location
	// @param data_handle_func - this is the name of the data handler function
	// @param map_file_dir - this is the directory of the map_file
	// @param key_file_dir - this is the directory of the key file
	// @param output_file_dir - this is the directory of the output 
	// @param work_dir - this is where the map reduce temp files are stored
	// @param max_key_bytes - this is the maximum possible number of bytes
	//                      - that make up a key
	// @param max_map_bytes - this is the maximum possible number of bytes 
	//                       - that make up a map
	void MergeSet(const char *data_handle_func, const char *map_file_dir, 
		const char *key_file_dir, const char *output_file_dir, const char *work_dir,
		int max_key_bytes, int max_map_bytes) {

		m_mapred_prim_ptr->SetAttributes(work_dir, map_file_dir, max_key_bytes, max_map_bytes);
		m_mapred_prim_ptr->DistributeMaps(data_handle_func);

		m_mapred_prim_ptr->SetAttributes(work_dir, output_file_dir, max_key_bytes, max_map_bytes);
		m_mapred_prim_ptr->MergeSet(data_handle_func);
	}

	// This merges key value pairs by their key value in the same physical location
	// @param data_handle_func - this is the name of the data handler function
	// @param map_file_dir - this is the directory of the map_file
	// @param key_file_dir - this is the directory of the key file
	// @param output_file_dir - this is the directory of the output 
	// @param work_dir - this is where the map reduce temp files are stored
	// @param max_key_bytes - this is the maximum possible number of bytes
	//                      - that make up a key
	// @param max_map_bytes - this is the maximum possible number of bytes 
	//                       - that make up a map
	void MergeSortedSet(const char *data_handle_func, const char *map_file_dir, 
		const char *key_file_dir, const char *output_file_dir, const char *work_dir,
		int max_key_bytes, int max_map_bytes) {

		m_mapred_prim_ptr->SetAttributes(work_dir, map_file_dir, max_key_bytes, max_map_bytes);
		m_mapred_prim_ptr->DistributeMaps(data_handle_func);

		m_mapred_prim_ptr->SetAttributes(work_dir, output_file_dir, max_key_bytes, max_map_bytes);
		m_mapred_prim_ptr->MergeSortedSet(data_handle_func);
	}

	// This is used to perform an external radix sort
	// @param data_handle_func - this is the name of the data handler function
	// @param map_file_dir - this is the directory of the map_file
	// @param key_file_dir - this is the directory of the key file
	// @param output_file_dir - this is the directory of the output 
	// @param work_dir - this is where the map reduce temp files are stored
	// @param max_key_bytes - this is the maximum possible number of bytes
	//                      - that make up a key
	// @param max_map_bytes - this is the maximum possible number of bytes 
	//                       - that make up a map
	void ExternalRadixSort(const char *data_handle_func, const char *map_file_dir, 
		const char *key_file_dir, const char *output_file_dir, const char *work_dir,
		int max_key_bytes, int max_map_bytes) {

		m_mapred_prim_ptr->SetAttributes(work_dir, key_file_dir, max_key_bytes, max_map_bytes);
		m_mapred_prim_ptr->CreateRadixSortedBlocks(data_handle_func);

		m_mapred_prim_ptr->SetAttributes(work_dir, output_file_dir, max_key_bytes, max_map_bytes);
		m_mapred_prim_ptr->MergeRadixSortedBlocks(data_handle_func);
	}

	// This is used to perform an external quick sort
	// @param data_handle_func - this is the name of the data handler function
	// @param map_file_dir - this is the directory of the map_file
	// @param key_file_dir - this is the directory of the key file
	// @param output_file_dir - this is the directory of the output 
	// @param work_dir - this is where the map reduce temp files are stored
	// @param max_key_bytes - this is the maximum possible number of bytes
	//                      - that make up a key
	// @param max_map_bytes - this is the maximum possible number of bytes 
	//                       - that make up a map
	void ExternalQuickSort(const char *data_handle_func, const char *map_file_dir, 
		const char *key_file_dir, const char *output_file_dir, const char *work_dir,
		int max_key_bytes, int max_map_bytes) {

		m_mapred_prim_ptr->SetAttributes(work_dir, key_file_dir, max_key_bytes, max_map_bytes);
		m_mapred_prim_ptr->CreateQuickSortedBlocks(data_handle_func);

		m_mapred_prim_ptr->SetAttributes(work_dir, output_file_dir, max_key_bytes, max_map_bytes);
		m_mapred_prim_ptr->MergeQuickSortedBlocks(data_handle_func);
	}

	// This extracts the MapReduce arguments supplied by the client
	void ExtractArguments(COpenConnection &conn) {

		int data_handle_func_length;
		int map_reduce_type_length;
		int map_file_buff_length;
		int key_file_buff_length;
		int output_file_buff_length;
		int work_dir_length;

		int max_key_bytes;
		int max_map_bytes;
		int data_type_length;
		int hash_div_num;
		int max_process_num;

		conn.Receive((char *)&data_handle_func_length, sizeof(int));cout<<"here1"<<endl;
		conn.Receive((char *)&map_reduce_type_length, sizeof(int));cout<<"here2"<<endl;
		conn.Receive((char *)&map_file_buff_length, sizeof(int));cout<<"here3"<<endl;
		conn.Receive((char *)&key_file_buff_length, sizeof(int));cout<<"here4"<<endl;
		conn.Receive((char *)&output_file_buff_length, sizeof(int));cout<<"here5"<<endl;
		conn.Receive((char *)&work_dir_length, sizeof(int));cout<<"here6"<<endl;

		conn.Receive((char *)&max_key_bytes, sizeof(int));cout<<"here7"<<endl;
		conn.Receive((char *)&max_map_bytes, sizeof(int));cout<<"here8"<<endl;
		conn.Receive((char *)&data_type_length, sizeof(int));cout<<"here10"<<endl;
		conn.Receive((char *)&hash_div_num, sizeof(int));cout<<"here11"<<endl;
		conn.Receive((char *)&max_process_num, sizeof(int));cout<<"here11"<<endl;

		CMemoryChunk<char> data_handler_func(data_handle_func_length + 1);
		CMemoryChunk<char> map_reduce_type(map_reduce_type_length + 1);
		CMemoryChunk<char> map_file_buff(map_file_buff_length + 1);
		CMemoryChunk<char> key_file_buff(key_file_buff_length + 1);
		CMemoryChunk<char> output_file_buff(output_file_buff_length + 1);
		CMemoryChunk<char> work_dir_buff(work_dir_length + 1);
		CMemoryChunk<char> data_type_buff(data_type_length + 1);

		conn.Receive(data_handler_func.Buffer(), data_handle_func_length);cout<<"here1"<<endl;
		conn.Receive(map_reduce_type.Buffer(), map_reduce_type_length);cout<<"here2"<<endl;
		conn.Receive(map_file_buff.Buffer(), map_file_buff_length);cout<<"here3"<<endl;
		conn.Receive(key_file_buff.Buffer(), key_file_buff_length);cout<<"here4"<<endl;
		conn.Receive(output_file_buff.Buffer(), output_file_buff_length);cout<<"here5"<<endl;
		conn.Receive(work_dir_buff.Buffer(), work_dir_length);cout<<"here6"<<endl;
		conn.Receive(data_type_buff.Buffer(), data_type_length);cout<<"here9"<<endl;

		data_handler_func[data_handle_func_length] = '\0';
		map_reduce_type[map_reduce_type_length] = '\0';
		map_file_buff[map_file_buff_length] = '\0';
		key_file_buff[key_file_buff_length] = '\0';
		output_file_buff[output_file_buff_length] = '\0';
		work_dir_buff[work_dir_length] = '\0';
		data_type_buff[data_type_length] = '\0';

		cout<<data_handler_func.Buffer()<<endl;
		cout<<map_reduce_type.Buffer()<<endl;
		cout<<map_file_buff.Buffer()<<endl;
		cout<<key_file_buff.Buffer()<<endl;
		cout<<output_file_buff.Buffer()<<endl;
		cout<<work_dir_buff.Buffer()<<endl;
		cout<<data_type_buff.Buffer()<<endl;
		cout<<max_key_bytes<<endl;
		cout<<max_map_bytes<<endl;

		static void (CMapReduceCommand::*meta_handler[])(const char *data_handle_func,
			const char *map_file_dir, const char *key_file_dir, const char *output_file_dir,
			const char *work_dir, int max_key_bytes, int max_map_bytes) = {
			&CMapReduceCommand::ExternalHashMap, &CMapReduceCommand::KeyOccurrence, 
			&CMapReduceCommand::DuplicateKeyOccurrence, &CMapReduceCommand::KeyWeight, 
			&CMapReduceCommand::DuplicateKeyWeight, &CMapReduceCommand::MergeSet, 
			&CMapReduceCommand::MergeSortedSet, &CMapReduceCommand::ExternalRadixSort,
			&CMapReduceCommand::ExternalQuickSort
		};

		m_mapred_prim_ptr = new CDistributeCompBlocks;
		m_mapred_prim_ptr->SetDataType(data_type_buff.Buffer());
		m_mapred_prim_ptr->SetHashDivNum(hash_div_num);
		m_mapred_prim_ptr->ProcessSet().SetMaximumClientNum(max_process_num);

		int length = strlen(map_reduce_type.Buffer());
		int index = m_meta_tag.FindWord(map_reduce_type.Buffer(), length);
		//if the appropriate index is found
		if(m_meta_tag.AskFoundWord()) {	
			//call the appropriate handler through the function pointer
			(this->*meta_handler[index])(data_handler_func.Buffer(), map_file_buff.Buffer(), 
				key_file_buff.Buffer(), output_file_buff.Buffer(), work_dir_buff.Buffer(),
				max_key_bytes, max_map_bytes);	
		} else {
			cout<<"Command Not Found"<<endl;
		}
		
		delete m_mapred_prim_ptr;
		char success = 'f';
		conn.Send(&success, 1);
	}

public:

	CMapReduceCommand() {
		CHDFSFile::Initialize();

		m_meta_tag.Initialize(200, 8);
		char html[][40]={"ExternalHashMap", "KeyOccurrence", "DuplicateKeyOccurrence",
			"KeyWeight", "DuplicateKeyWeight", "MergeSet", "MergeSortedSet",
			"ExternalRadixSort", "ExternalQuickSort", "//"};

		int index=0;
		while(!CUtility::FindFragment(html[index], "//")) {
			int length = (int)strlen(html[index]);
			m_meta_tag.AddWord(html[index++], length);
		}
	}

	// This swans servers to fullfill a query requested by the user
	void ListenForConnections() {

		if(!m_connect.OpenServerConnection(5555, LOCAL_NETWORK)) {
			throw ENetworkException("Could Not Open Server Connection");
		}

		m_connect.ListenOnServerConnection(INFINITE);
		cout<<"Listening For Connections"<<endl;

		while(true) {
			COpenConnection client_conn;
			client_conn.Socket() = m_connect.ServerAcceptConnection();
			cout<<"Accepted a Connection"<<endl;
			ExtractArguments(client_conn);
			client_conn.CloseConnection();
		}
	}

	// This spawns a map reduce task
	void MapReduceTask(const char *data_handle_func, const char *map_reduce_type,
		const char *map_file, const char *key_file, const char *output_file, 
		const char *work_dir, const char *data_type, int max_key_bytes, 
		int max_map_bytes, int hash_div_num, int inst_id, int max_process_num) {

		static void (CMapReduceCommand::*meta_handler[])(const char *data_handle_func,
			const char *map_file_dir, const char *key_file_dir, const char *output_file_dir,
			const char *work_dir, int max_key_bytes, int max_map_bytes) = {
			&CMapReduceCommand::ExternalHashMap, &CMapReduceCommand::KeyOccurrence, 
			&CMapReduceCommand::DuplicateKeyOccurrence, &CMapReduceCommand::KeyWeight, 
			&CMapReduceCommand::DuplicateKeyWeight, &CMapReduceCommand::MergeSet, 
			&CMapReduceCommand::MergeSortedSet, &CMapReduceCommand::ExternalRadixSort,
			&CMapReduceCommand::ExternalQuickSort
		};

		m_mapred_prim_ptr = new CDistributeCompBlocks;
		m_mapred_prim_ptr->SetDataType(data_type);
		m_mapred_prim_ptr->SetHashDivNum(hash_div_num);
		m_mapred_prim_ptr->ProcessSet().SetPort(3333 + inst_id);
		m_mapred_prim_ptr->ProcessSet().SetMaximumClientNum(max_process_num);
		m_mapred_prim_ptr->ProcessSet().SetWorkingDirectory(CUtility::ExtendString
			(DFS_ROOT, "DyableMapReduce/DyableCommand/"));

		int length = strlen(map_reduce_type);
		int index = m_meta_tag.FindWord(map_reduce_type, length);

		//if the appropriate index is found
		if(index >= 0) {	
			//call the appropriate handler through the function pointer
			(this->*meta_handler[index])(data_handle_func, map_file, 
				key_file, output_file, work_dir,
				max_key_bytes, max_map_bytes);	
		} else {
			cout<<"Command Not Found"<<endl;
		}

		delete m_mapred_prim_ptr;
	}

	// This is just a test framework
	void TestExternalRadixSort() {

		for(int item_num = 5; item_num < 24; item_num++) {
			cout<<"Item Byte Num "<<item_num<<endl;

			_int64 num = 0;
			_int64 sum = 0;
			uChar buff[40];

			for(int k=0; k<10; k++) {

				CSegFile sort_file;
				sort_file.OpenWriteFile(CUtility::ExtendString
					("DyableMapReduce/DyableCommand/TestDir/key_file", k));

				for(int i=0; i<9999; i++) {
					num = rand();

					sort_file.AskBufferOverflow(item_num + 5);
					for(int j=0; j<item_num + 5; j++) {
						buff[j] = rand();
						sort_file.WriteCompObject((char *)&buff[j], 1);
					}

					for(int j=0; j<item_num; j++) {
						sum += buff[j];
					}
				}
			}

			ExternalRadixSort(NULL, "", "DyableMapReduce/DyableCommand/TestDir/key_file", 
				"DyableMapReduce/DyableCommand/TestDir/key_file", "DyableMapReduce/DyableCommand/TestDir/test", item_num + 5, item_num);

			uChar prev[40];
			for(int j=0; j<40; j++) {
				prev[j] = 0;
			}

			_int64 compare = 0;
			_int64 curr_num = 0;
			CSegFile output_file;
			output_file.OpenReadFile("DyableMapReduce/DyableCommand/TestDir/key_file");
			bool flag = true;

			num = 0;
			while(flag) {
				for(int j=0; j<item_num + 5; j++) {
					if(!output_file.ReadCompObject((char *)&buff[j], 1)) {
						flag = false;
						break;
					}
				}

				if(flag == false) {
					break;
				}

				for(int j=item_num-1; j>=0; j--) {
					if(prev[j] > buff[j]) {
						cout<<"error2";getchar();
					}
					if(buff[j] > prev[j])break;
				}

				for(int j=item_num-1; j>=0; j--) {
					prev[j] = buff[j];
					compare += prev[j];
				}
			}

			if(sum != compare) {
				cout<<"error1 "<<sum<<" "<<compare ;getchar();
			}

			delete m_mapred_prim_ptr;
			m_mapred_prim_ptr = new CDistributeCompBlocks;
		}

		delete m_mapred_prim_ptr;
	
	}

	// This is just a test framework
	void TestExternalHashMap() {
		
		int client_num =6;
		CTrie map(4);
		CArrayList<char> value(4);
		CArrayList<int> value_offset(4);
		value_offset.PushBack(0);
		CDistributeCompBlocks block;
		cout<<"ExternalHashMap"<<endl;

		for(int i=0; i<50000; i++) {
			int id = rand() % 1000000;
			map.AddWord((char *)&id, 4);
			if(!map.AskFoundWord()) {
				int length = (rand() % 7) + 2;
				length = 8;

				for(int j=0; j<length; j++) {
					value.PushBack((rand() % 26) + 'a');
				}
				value_offset.PushBack(value.Size());
			}

		}

		cout<<"Here"<<endl;
		CMemoryChunk<CArrayList<int> > key_set(client_num);
		for(int j=0; j<client_num; j++) {
			CSegFile key_file;
			CSegFile map_file;
			key_file.OpenWriteFile(CUtility::ExtendString
				("DyableMapReduce/DyableCommand/TestDir/key_file", j));
			map_file.OpenWriteFile(CUtility::ExtendString
				("DyableMapReduce/DyableCommand/TestDir/map_file", j));
			cout<<"Client "<<j<<endl;
			int length;
			key_set[j].Initialize(4);
			for(int i=0; i<50000; i++) {
				int id = rand() % map.Size();
				key_file.WriteCompObject(map.GetWord(id), 4);
				key_set[j].PushBack(*((int *)map.GetWord(id, length)));
			}

			for(int i=0; i<map.Size(); i++) {
				if((i % client_num) != j) {
					continue;
				}
				int size = value_offset[i+1] - value_offset[i];
				map_file.AskBufferOverflow(4 + size);
				map_file.WriteCompObject(map.GetWord(i), 4);
				map_file.WriteCompObject(value.Buffer() + value_offset[i], size);
			}
		}

		cout<<"Spawning"<<endl;
		ExternalHashMap(NULL, "DyableMapReduce/DyableCommand/TestDir/map_file", "DyableMapReduce/DyableCommand/TestDir/key_file", 
			"DyableMapReduce/DyableCommand/TestDir/output", "DyableMapReduce/DyableCommand/TestDir/test", 4, 8);

		cout<<"Testing "<<endl;

		int total = 0;
		CMemoryChunk<char> map_res(20);
		for(int j=0; j<client_num; j++) {

			cout<<"Client Set "<<j<<endl;
			CHDFSFile key_file;
			key_file.OpenReadFile(CUtility::ExtendString("DyableMapReduce/DyableCommand/TestDir/output", j));

			int key;
			int offset = 0;
			int key_offset = 0;
			while(key_file.ReadCompObject(key)) {
				if(key_set[j][key_offset++] != key) {
					cout<<"Key Mismatch "<<key_set[j][key_offset-1]<<" "<<key<<" "<<j;getchar();
				}

				total++;
				key_file.ReadCompObject(map_res.Buffer(), 8);
				int id = map.FindWord((char *)&key, 4);
				if(id < 0) {
					cout<<"Key Not Found";getchar();
				}

				int offset = value_offset[id];
				for(int k=0; k<8; k++) {
					if(map_res[k] != value[offset++]) {
						cout<<"error "<<offset<<"  "<<" "<<value[offset-1];
						getchar();
					}
				}
			}

			if(key_offset != key_set[j].Size()) {
				cout<<"Size Mismatch";getchar();
			}
		}

		if(total != 50000 * client_num) {
			cout<<"error2";getchar();
		}
	
	}

	// This is just a test framework
	void TestDuplicateKeyOccurrence() {

		CTrie map(4);
		CArrayList<int> key_offset(10);
		CArrayList<char> key_buff(10);
		CArrayList<int> key_id(4);
		CMemoryChunk<int> key_num(100, 0);
		int client_num = 6;
		key_offset.PushBack(0);
		CDistributeCompBlocks block;

		for(int i=0; i<100; i++) {
			int length = (rand() % 7) + 2;
			length = 8;
			int prev = key_offset.LastElement();

			for(int j=0; j<length; j++) {
				key_buff.PushBack('a' + (rand() % 26));
			}

			map.AddWord(key_buff.Buffer() + prev, length);
			key_offset.PushBack(key_buff.Size());
		}

		for(int i=0; i<client_num; i++) {
			CSegFile key_file;
			key_file.OpenWriteFile(CUtility::ExtendString
				("DyableMapReduce/DyableCommand/TestDir/key_file", i));

			int length;
			for(int k=0; k<10000; k++) {
				int id = rand() % map.Size();
				key_id.PushBack(id);
				
				char *key = map.GetWord(id, length);
				key_file.WriteCompObject(key, length);

				key_num[id]++;
			}

		}

		DuplicateKeyOccurrence(NULL, "DyableMapReduce/DyableCommand/TestDir/map_file", "DyableMapReduce/DyableCommand/TestDir/key_file", 
			"DyableMapReduce/DyableCommand/TestDir/output", "DyableMapReduce/DyableCommand/TestDir/test", 8, 8);

		cout<<"Testing "<<endl;

		CHDFSFile map_file;
		int num = 0;
		int offset = 0;
		for(int j=0; j<client_num; j++) {

			cout<<"Test Client: "<<j<<endl;
			CMemoryChunk<char> buff(20);
			map_file.OpenReadFile(CUtility::ExtendString("DyableMapReduce/DyableCommand/TestDir/output", j));

			int bytes = 8;
			int occurr;
			while(map_file.ReadCompObject(buff.Buffer(), bytes)) {

				map_file.ReadCompObject(occurr);
				int id = map.FindWord(buff.Buffer(), bytes);

				if(id != key_id[offset++]) {
					cout<<"Key error";getchar();
				}

				if(id < 0) {
					cout<<"Could Not Find ";
					for(int i=0; i<bytes; i++) {
						cout<<buff[i];
					}
					getchar();
				}
				num++;

				if(occurr != key_num[id]) {
					cout<<"error "<<occurr<<" "<<key_num[id];
					getchar();
				}
			}
		}

		if(client_num * 10000 != num) {
			cout<<"error2";getchar();
		}
	}

	// This is just a test framework
	void TestDuplicateKeyWeight() {

		CTrie map(4);
		CArrayList<int> key_offset(10);
		CArrayList<char> key_buff(10);
		CArrayList<int> key_id(4);
		CMemoryChunk<int> key_num(100, 0);
		int client_num = 6;
		key_offset.PushBack(0);
		CDistributeCompBlocks block;

		for(int i=0; i<100; i++) {
			int length = (rand() % 7) + 2;
			length = 8;
			int prev = key_offset.LastElement();

			for(int j=0; j<length; j++) {
				key_buff.PushBack('a' + (rand() % 26));
			}

			map.AddWord(key_buff.Buffer() + prev, length);
			key_offset.PushBack(key_buff.Size());
		}

		for(int i=0; i<client_num; i++) {
			CSegFile key_file;
			key_file.OpenWriteFile(CUtility::ExtendString
				("DyableMapReduce/DyableCommand/TestDir/key_file", i));

			int length;
			for(int k=0; k<10000; k++) {
				int id = rand() % map.Size();
				key_id.PushBack(id);
				
				key_file.AskBufferOverflow(8 + sizeof(int));
				int val = rand() % 10;
				char *key = map.GetWord(id, length);
				key_file.WriteCompObject(key, 8);
				key_file.WriteCompObject(val);

				key_num[id] += val;
			}

		}

		DuplicateKeyWeight(NULL, "DyableMapReduce/DyableCommand/TestDir/map_file", "DyableMapReduce/DyableCommand/TestDir/key_file", 
			"DyableMapReduce/DyableCommand/TestDir/output", "DyableMapReduce/DyableCommand/TestDir/test", 8, 8);

		cout<<"Testing "<<endl;

		CHDFSFile map_file;
		int num = 0;
		int offset = 0;
		for(int j=0; j<client_num; j++) {

			cout<<"Test Client: "<<j<<endl;
			CMemoryChunk<char> buff(20);
			map_file.OpenReadFile(CUtility::ExtendString("DyableMapReduce/DyableCommand/TestDir/output", j));

			int bytes = 8;
			int occurr;
			while(map_file.ReadCompObject(buff.Buffer(), bytes)) {

				map_file.ReadCompObject(occurr);
				int id = map.FindWord(buff.Buffer(), bytes);

				if(id != key_id[offset++]) {
					cout<<"Key error";getchar();
				}

				if(id < 0) {
					cout<<"Could Not Find ";
					for(int i=0; i<bytes; i++) {
						cout<<buff[i];
					}
					getchar();
				}
				num++;

				if(occurr != key_num[id]) {
					cout<<"error "<<occurr<<" "<<key_num[id];
					getchar();
				}
			}
		}

		if(client_num * 10000 != num) {
			cout<<"error2";getchar();
		}
	}

	// This is just a test framework
	void TestKeyOccurrence() {

		CTrie map(4);
		CArrayList<int> key_offset(10);
		CArrayList<char> key_buff(10);
		CMemoryChunk<int> key_num(100, 0);
		int client_num = 6;
		key_offset.PushBack(0);
		CDistributeCompBlocks block;

		for(int i=0; i<100; i++) {
			int length = (rand() % 7) + 2;
			int prev = key_offset.LastElement();
			length = 8;

			for(int j=0; j<length; j++) {
				key_buff.PushBack('a' + (rand() % 26));
			}

			map.AddWord(key_buff.Buffer() + prev, length);
			key_offset.PushBack(key_buff.Size());
		}

		for(int i=0; i<client_num; i++) {
			CSegFile key_file;
			key_file.OpenWriteFile(CUtility::ExtendString
				("DyableMapReduce/DyableCommand/TestDir/key_file", i));

			int length;
			for(int k=0; k<10000; k++) {
				int id = rand() % map.Size();
				
				char *key = map.GetWord(id, length);
				if(CUtility::FindFragment(key, "saxxmnsn", 8, 0)) {
					cout<<"go";getchar();
				}

				key_file.WriteCompObject(key, length);
				key_num[id]++;
			}
		}

		KeyOccurrence(NULL, "DyableMapReduce/DyableCommand/TestDir/map_file", "DyableMapReduce/DyableCommand/TestDir/key_file", 
			"DyableMapReduce/DyableCommand/TestDir/output", "DyableMapReduce/DyableCommand/TestDir/test", 8, 8);

		cout<<"Testing "<<endl;
		CHDFSFile map_file;
		int num = 0;
		int curr_set = 0;
		while(true) {

			CMemoryChunk<char> buff(20);

			try {
				map_file.OpenReadFile(CUtility::ExtendString
					("DyableMapReduce/DyableCommand/TestDir/output", curr_set++));
			} catch(...) {
				break;
			}

			int occurr;
			int bytes = 8;
			while(map_file.ReadCompObject(buff.Buffer(), bytes)) {
				map_file.ReadCompObject(occurr);
				int id = map.FindWord(buff.Buffer(), bytes);

				if(id < 0) {
					cout<<"Could Not Find ";
					for(int i=0; i<bytes; i++) {
						cout<<buff[i];
					}
					getchar();
				}
				num++;

				if(occurr != key_num[id]) {
					cout<<"error "<<occurr<<" "<<key_num[id];
					getchar();
				}
				cout<<"pass"<<endl;
			}
		}

		if(map.Size() != num) {
			cout<<"error2 "<<map.Size()<<" "<<num;getchar();
		}

	}

	// This is just a test framework
	void TestKeyWeight() {

		CTrie map(4);
		CArrayList<int> key_offset(10);
		CArrayList<char> key_buff(10);
		CMemoryChunk<int> key_weight(100, 0);
		int client_num = 6;
		key_offset.PushBack(0);
		CDistributeCompBlocks block;

		for(int i=0; i<100; i++) {
			int length = (rand() % 7) + 2;
			int prev = key_offset.LastElement();
			length = 8;

			for(int j=0; j<length; j++) {
				key_buff.PushBack('a' + (rand() % 26));
			}

			map.AddWord(key_buff.Buffer() + prev, length);
			key_offset.PushBack(key_buff.Size());
		}

		for(int i=0; i<client_num; i++) {
			CSegFile key_file;
			key_file.OpenWriteFile(CUtility::ExtendString
				("DyableMapReduce/DyableCommand/TestDir/key_file", i));

			int length;
			for(int k=0; k<10000; k++) {
				int id = rand() % map.Size();
				
				int weight = rand() % 10000;
				char *key = map.GetWord(id, length);
				key_file.WriteCompObject(key, length);

				key_file.WriteCompObject(weight);

				key_weight[id] += weight;
			}

		}

		KeyWeight(NULL, "DyableMapReduce/DyableCommand/TestDir/map_file", "DyableMapReduce/DyableCommand/TestDir/key_file", 
			"DyableMapReduce/DyableCommand/TestDir/output", "DyableMapReduce/DyableCommand/TestDir/test", 8, 8);

		cout<<"Testing "<<endl;
		CHDFSFile map_file;
		int num = 0;
		int curr_set = 0;
		while(true) {

			CMemoryChunk<char> buff(20);

			try {
				map_file.OpenReadFile(CUtility::ExtendString
					("DyableMapReduce/DyableCommand/TestDir/output", curr_set++));
			} catch(...) {
				break;
			}

			int bytes = 8;
			int weight;
			while(map_file.ReadCompObject(buff.Buffer(), bytes)) {
				map_file.ReadCompObject(weight);
				int id = map.FindWord(buff.Buffer(), bytes);

				if(id < 0) {
					cout<<"Could Not Find ";
					for(int i=0; i<bytes; i++) {
						cout<<buff[i];
					}
					getchar();
				}
				num++;

				if(weight != key_weight[id]) {
					cout<<"error "<<weight<<" "<<key_weight[id];
					getchar();
				}
				cout<<"pass"<<endl;
			}
		}

		if(map.Size() != num) {
			cout<<"error2 "<<map.Size()<<" "<<num;getchar();
		}
	}

	// This is just a test framework
	void TestMergeMap() {

		CTrie map(4);
		CMemoryChunk<bool> key_used(1000, false);
		int client_num = 6;
		CDistributeCompBlocks block;

		CMemoryChunk<char> buff(100);
		for(int i=0; i<client_num; i++) {
			CSegFile set_file;
			set_file.OpenWriteFile(CUtility::ExtendString
				("DyableMapReduce/DyableCommand/TestDir/map_file", i));

			for(int k=0; k<100000; k++) {
				int id = rand() % 1000;
				memcpy(buff.Buffer(), (char *)&id, 4);
				int length = (rand() % 10) + 2;
				length = 8;
				for(int j=0; j<length; j++) {
					buff[j+4] = (rand() % 26) + 'a';
				}

				map.AddWord(buff.Buffer(), length + 4);

				if(!map.AskFoundWord()) {
					set_file.WriteCompObject(buff.Buffer(), length + 4);
				}
			}
		}

		CMemoryChunk<bool> set_used(map.Size());
		MergeSet(NULL, "DyableMapReduce/DyableCommand/TestDir/map_file", "DyableMapReduce/DyableCommand/TestDir/key_file", 
			"DyableMapReduce/DyableCommand/TestDir/output", "DyableMapReduce/DyableCommand/TestDir/test", 4, 8);

		CHDFSFile map_file;
		int num = 0;
		int curr_set = 0;
		while(true) {

			CMemoryChunk<char> buff(20);
			try {
				map_file.OpenReadFile(CUtility::ExtendString
					("DyableMapReduce/DyableCommand/TestDir/output", curr_set++));
			} catch(...) {
				break;
			}

			int id;
			int bytes = 12;
			int prev_id = -1;
			while(map_file.ReadCompObject(buff.Buffer(), bytes)) {
				memcpy((char *)&id, buff.Buffer(), 4);

				if(id != prev_id) {
					prev_id = id;
					if(key_used[id] == true) {
						cout<<"node already used";getchar();
					}
					key_used[id] = true;
				}

				id = map.FindWord(buff.Buffer(), bytes);

				if(id < 0) {
					cout<<"Could Not Find ";
					for(int i=0; i<bytes; i++) {
						cout<<buff[i];
					}
					getchar();
				}
				
				if(set_used[id] == true) {
					cout<<"Set already used";getchar();
				}

				set_used[id] = true;
				num++;
			}
		}

		if(map.Size() != num) {
			cout<<"error2";getchar();
		}
	}
};

int main(int argc, char *argv[]) {

	CMemoryElement<CMapReduceCommand> command;

	if(argc < 2) {
		//command->TestExternalRadixSort();return 0;
		command->ListenForConnections();
	}

	const char *data_handle_func = argv[1];
	const char *map_reduce_type = argv[2];
	const char *map_file = argv[3];
	const char *key_file = argv[4];
	const char *output_file = argv[5];
	const char *work_dir = argv[6];
	const char *data_type = argv[7];
	int max_key_bytes = atoi(argv[8]);
	int max_map_bytes = atoi(argv[9]);
	int hash_div_num = atoi(argv[10]);
	int inst_id = atoi(argv[11]);
	int max_process_num = atoi(argv[12]);

	CBeacon::InitializeBeacon(inst_id, 5555 + inst_id);
	command->MapReduceTask(data_handle_func, map_reduce_type, map_file, 
		key_file, output_file, work_dir, data_type, max_key_bytes, 
		max_map_bytes, hash_div_num, inst_id, max_process_num);
	command.DeleteMemoryElement();

	CBeacon::SendTerminationSignal();

	return 0;
}
