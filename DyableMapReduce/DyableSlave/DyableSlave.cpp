#include "./DataHandleTag.h"

// This class is responsible for examining the processing of commands
// given to the slave by the master. From here it spawns off the appropriate
// processing function to handle the request
template <class X> class CProcessCommand : public CDataHandleTag<X> {

	// stores the html tags used in looking up the handler functions
	CHashDictionary<short> m_meta_tag;

	// This stores the working directory of the set
	const char *m_work_dir;
	// This stores the data directory of the set
	const char *m_data_dir;

	// This stores the set boundary for which a process is responsible
	SBoundary m_set_bound;
	// This stores the maximum number of key bytes
	int m_max_key_bytes;
	// This stores the maximum number of map bytes
	int m_max_map_bytes;
	// This stores the file byte offset for distribution of keys and maps
	_int64 m_file_byte_offset;
	// This stores the number of tuples to process in the set
	_int64 m_tuple_bytes;

public:

	CProcessCommand() {
	}

	// This initilizes the html tag dictionary by adding each term type, html tag type
	// so that they can be looked up later 
	// @param key_client_num - this is the total number of key client sets
	// @param map_client_num - this is the total number of map client sets
	void Initialize(int client_id, int key_client_num, int map_client_num) {

		CNodeStat::SetClientID(client_id);
		CSetNum::SetKeyClientNum(key_client_num);
		CSetNum::SetMapClientNum(map_client_num);
		CHDFSFile::Initialize();

		m_meta_tag.Initialize(200, 8);
		char html[][40]={"DistributeKeys", "DistributeMaps", "DistributeKeyWeight",
			"FindKeyWeight", "FindKeyOccurrence", "ApplyMapsToKeys", "MergeSet",
			"FindDuplicateKeyWeight", "FindDuplicateKeyOccurrence", "OrderMappedSets",
			"OrderMappedOccurrences", "MergeSortedSet", "CreateRadixSortedBlock",
			"CreateQuickSortedBlock", "MergeRadixSortedBlocks", "MergeQuickSortedBlocks", "//"};

		int index=0;
		while(!CUtility::FindFragment(html[index], "//")) {
			int length = (int)strlen(html[index]);
			m_meta_tag.AddWord(html[index++], length);
		}
	}
	
	// This is responsible for calling a particular map reduce handler
	// @param request - this is the request string being issued
	// @param max_key_bytes - this is the maximum number of bytes that make
	//                      - up a key
	// @param max_map_bytes - this is the maximum number of bytes that maka
	//                      - up a map
	// @param file_byte_offset - this is the byte offset in the file from 
	//                         - which to start reading
	// @param tuple_num - this is the number of sets to read from the file
	// @param work_dir - this stores the ptr to the work directory in which
	//                 - to undertake the mapreduce
	// @param data_dir - this stores the ptr to the file from which to process
	// @param data_div_start - this is the lower bound on the client sets to process
	// @param data_div_end - this is the upper bound on the client sets to process
	void HandleRequest(const char request[], int max_key_bytes,
		int max_map_bytes, _int64 file_byte_offset, _int64 tuple_bytes,
		const char work_dir[], const char data_dir[],
		int data_div_start, int data_div_end) {

		work_dir = "LocalData/map_red";

		m_max_key_bytes = max_key_bytes;
		m_max_map_bytes = max_map_bytes;
		m_file_byte_offset = file_byte_offset;
		m_tuple_bytes = tuple_bytes;
		m_work_dir = work_dir;
		m_data_dir = data_dir;

		m_set_bound.start = data_div_start;
		m_set_bound.end = data_div_end;

		static void (CProcessCommand::*meta_handler[])() = {
			&CProcessCommand::DistributeKeys, &CProcessCommand::DistributeMaps, 
			&CProcessCommand::DistributeKeyWeight, &CProcessCommand::FindKeyWeight, 
			&CProcessCommand::FindKeyOccurrence, &CProcessCommand::ApplyMapsToKeys,
			&CProcessCommand::MergeSet, &CProcessCommand::FindDuplicateKeyWeight,
			&CProcessCommand::FindDuplicateKeyOccurrence, &CProcessCommand::OrderMappedSets,
			&CProcessCommand::OrderMappedOccurrences, &CProcessCommand::MergeSortedSet,
			&CProcessCommand::CreateRadixSortedBlock, &CProcessCommand::CreateQuickSortedBlock,
			&CProcessCommand::MergeRadixSortedBlocks, &CProcessCommand::MergeQuickSortedBlocks
		};

		int length = strlen(request);
		int index = m_meta_tag.FindWord(request, length);
		//if the appropriate index is found
		if(m_meta_tag.AskFoundWord()) {	
			//call the appropriate handler through the function pointer
			(this->*meta_handler[index])();	
		} else {
			cout<<"Method Not Found";getchar();
		}
	}

	// This function handles the distribution of keys among multiple clients
	void DistributeKeys() {

		CDistributeKeys set;
		if(CDataHandleTag<X>::m_retrieve_key == NULL) {
			set.DistributeKeys(m_work_dir, m_data_dir, 
				m_file_byte_offset, m_tuple_bytes, m_max_key_bytes);
		} else {
			set.DistributeKeys(m_work_dir, m_data_dir, m_file_byte_offset, 
				m_tuple_bytes, m_max_key_bytes, CDataHandleTag<X>::m_retrieve_key);
		}
	}

	// This function handles the distribution of maps among multiple clients
	void DistributeMaps() {

		CDistributeMaps set;
		if(CDataHandleTag<X>::m_retrieve_map == NULL) {
			set.DistributeMaps(m_work_dir, m_data_dir, 
				m_file_byte_offset, m_tuple_bytes, m_max_key_bytes, m_max_map_bytes);
		} else {
			set.DistributeMaps(m_work_dir, m_data_dir, m_file_byte_offset, 
				m_tuple_bytes, m_max_key_bytes, m_max_map_bytes, CDataHandleTag<X>::m_retrieve_map);
		}
	}

	// This function handles the distribution of key weights among 
	// multiple clients
	void DistributeKeyWeight() {

		CDistributeKeyWeight<X> set;
		if(CDataHandleTag<X>::m_retrieve_key_weight == NULL) {
			set.DistributeKeys(m_work_dir, m_data_dir, 
				m_file_byte_offset, m_tuple_bytes, m_max_key_bytes);
		} else {
			set.DistributeKeys(m_work_dir, m_data_dir, m_file_byte_offset, 
				m_tuple_bytes, m_max_key_bytes, CDataHandleTag<X>::m_retrieve_key_weight);
		}
	}

	// This sorts one of the distributed blocks and writes it back to file
	void CreateRadixSortedBlock() {

		CRadixSortBlock set;
		set.SortBlock(m_work_dir, m_data_dir, m_file_byte_offset,
			m_tuple_bytes, m_max_key_bytes, m_max_map_bytes);
	}

	// This sorts one of the distributed blocks and writes it back to file
	void CreateQuickSortedBlock() {

		CQuickSortBlock set;
		set.SortBlock(m_work_dir, m_data_dir, m_file_byte_offset,
			m_tuple_bytes, m_max_key_bytes, CDataHandleTag<X>::m_compare_func);
	}

	// This merges the set of sorted blocks to create a single sorted block
	void MergeRadixSortedBlocks() {

		CMergeSortedBlocks set;
		set.MergeRadixSortedBlocks(m_work_dir, m_data_dir, m_max_key_bytes,
			m_max_map_bytes, m_set_bound.start, m_set_bound.end);
	}

	// This merges the set of sorted blocks to create a single sorted block
	void MergeQuickSortedBlocks() {

		CMergeSortedBlocks set;
		set.MergeQuickSortedBlocks(m_work_dir, m_data_dir, m_max_key_bytes,
			m_set_bound.start, m_set_bound.end, CDataHandleTag<X>::m_compare_func);
	}

	// This function merges keys and sums the weight
	void FindKeyWeight() {

		CFindKeyWeight<X> set;
		set.FindKeyWeight(m_work_dir, m_data_dir, 
			m_max_key_bytes, CDataHandleTag<X>::m_write_key_weight);
	}

	// This function merges keys and sums the occurrence
	void FindKeyOccurrence() {

		CFindKeyOccurrence<X> set;
		set.FindKeyOccurrence(m_work_dir, m_data_dir, 
			m_max_key_bytes, CDataHandleTag<X>::m_write_occur);
	}

	// This function applies a set of maps to a set of keys
	void ApplyMapsToKeys() {
		CApplyMapsToKeys set;
		set.PerformMapping(m_work_dir, m_set_bound, m_max_key_bytes, m_max_map_bytes);
	}

	// This function groups key value pairs by their key value
	void MergeSet() {

		CMergeMap set;
		set.MergeSet(m_work_dir, m_data_dir, 
			m_max_key_bytes, m_max_map_bytes, CDataHandleTag<X>::m_write_set);
	}

	// This function groups key value pairs by their key value
	// this is done in sorted order
	void MergeSortedSet() {

		CMergeMap set;
		set.MergeSortedSet(m_work_dir, m_data_dir, 
			m_max_key_bytes, m_max_map_bytes, CDataHandleTag<X>::m_write_set);
	}

	// This function applies the summed weight to each key
	void FindDuplicateKeyWeight() {

		CFindKeyWeight<X> set;
		set.FindDuplicateKeyWeight(m_work_dir, m_data_dir, m_max_key_bytes);
	}

	// This function applies the summed occurence to each key
	void FindDuplicateKeyOccurrence() {

		CFindKeyOccurrence<X> set;
		set.FindDuplicateKeyOccurrence(m_work_dir, m_data_dir, m_max_key_bytes);
	}

	// This function writes the mapped keys back to client sets
	// in the original order that they were present
	void OrderMappedSets() {
		COrderMappedSets set;
		set.OrderMappedSets(m_work_dir, m_data_dir, CDataHandleTag<X>::m_write_map, 
			m_set_bound, m_max_key_bytes, m_max_map_bytes);
	}

	// This function writes the mapped keys back to client sets
	// in the original order that they were present
	void OrderMappedOccurrences() {

		COrderMappedSets set;
		set.OrderMappedOccurrences(m_work_dir, m_data_dir, 
			m_max_key_bytes, m_set_bound, CDataHandleTag<X>::m_write_occur);
	}

	// This initiates the different components for testing
	void TestComponents() {

		_int64 byte_offset = 0;
		_int64 tuple_bytes = 0;

		CHDFSFile set_file;
		set_file.OpenReadFile("DyableMapReduce/DyableCommand/TestDir/key_file.comp_size");

		int comp_size;
		int norm_size;

		set_file.ReadCompObject(comp_size);
		set_file.ReadCompObject(norm_size);
		tuple_bytes = norm_size;

		HandleRequest("DistributeKeys", 4, 4, byte_offset, tuple_bytes,
			"DyableMapReduce/DyableSlave/TestDir/test",
			"DyableMapReduce/DyableSlave/TestDir/key", 0, 0);

		HandleRequest("DistributeKeys", 4, 4, byte_offset, tuple_bytes,
			"DyableMapReduce/DyableSlave/TestDir/test",
			"DyableMapReduce/DyableSlave/TestDir/key", 0, 0);
	}
};

int main(int argc, char *argv[]) {

	if(argc < 3)return 0;

	cout<<argv[1]<<endl;
	cout<<argv[2]<<endl;
	cout<<argv[3]<<endl;
	cout<<argv[4]<<endl;
	cout<<argv[5]<<endl;
	cout<<argv[6]<<endl;
	cout<<argv[7]<<endl;
	cout<<argv[8]<<endl;
	cout<<argv[9]<<endl;
	cout<<argv[10]<<endl;
	cout<<argv[11]<<endl;
	cout<<argv[12]<<endl;
	cout<<argv[13]<<endl;
	cout<<argv[14]<<endl;

	int client_id = atoi(argv[1]);
	int key_client_num = atoi(argv[2]);
	int map_client_num = atoi(argv[3]);
	const char *request = argv[4];
	const char *data_handle_tag = argv[5];
	const char *work_dir = argv[6];
	const char *data_dir = argv[7];

	int data_div_start = atoi(argv[8]);
	int data_div_end = atoi(argv[9]);

	int max_key_bytes = atoi(argv[10]);
	int max_map_bytes = atoi(argv[11]);
	int port = atoi(argv[12]);
	_int64 file_byte_offset = CANConvert::AlphaToNumericLong(argv[13], strlen(argv[13]));
	_int64 tuple_bytes = CANConvert::AlphaToNumericLong(argv[14], strlen(argv[14]));
	char *data_type = argv[15];

	CBeacon::InitializeBeacon(client_id, port);

	if(strcmp(data_type, "float") == 0) {
		CMemoryElement<CProcessCommand<float> > process;
		process->Initialize(client_id, key_client_num, map_client_num);
		process->SetDataHandleFunc(data_handle_tag);
		process->HandleRequest(request, max_key_bytes, max_map_bytes, 
			file_byte_offset, tuple_bytes, work_dir, data_dir,
			data_div_start, data_div_end);
		process.DeleteMemoryElement();
	} else if(strcmp(data_type, "int") == 0) {
		CMemoryElement<CProcessCommand<int> > process;
		process->Initialize(client_id, key_client_num, map_client_num);
		process->SetDataHandleFunc(data_handle_tag);
		process->HandleRequest(request, max_key_bytes, max_map_bytes, 
			file_byte_offset, tuple_bytes, work_dir, data_dir,
			data_div_start, data_div_end);
		process.DeleteMemoryElement();
	} else if(strcmp(data_type, "_int64") == 0) {
		CMemoryElement<CProcessCommand<_int64> > process;
		process->Initialize(client_id, key_client_num, map_client_num);
		process->SetDataHandleFunc(data_handle_tag);
		process->HandleRequest(request, max_key_bytes, max_map_bytes, 
			file_byte_offset, tuple_bytes, work_dir, data_dir,
			data_div_start, data_div_end);
		process.DeleteMemoryElement();
	}

	CBeacon::SendTerminationSignal();

	return 0;
}
