#include "./MapReducePrimatives.h"

// This class is responsible for distributing the workload for one
// of the MapReduce primatives among multiple clients. In particular
// it segments a file and distributes individual segments to each
// client. It also handles error failure of one or more of the parallel
// clients and restarts a client if necessary.
class CDistributeCompBlocks : public CMapReducePrimatives {

	// This defines the number of hash divisions
	int HASH_DIV_NUM;

	// This stores the comp block compressed and uncompressed size
	struct SCompBlock {
		// this stores the blocks compressed size
		int compress_size;
		// this stores the blocks uncompressed size
		int uncompress_size;
	};

	// This stores the set of sequential comp blocks 
	CArrayList<SCompBlock> m_seq_comp_blocks;
	// This stores the set of div bounds that belong to a client file
	CArrayList<SBoundary> m_file_div_bound;
	// This stores the comp block compressed size
	CHDFSFile m_comp_block_size;

	// This stores the current file byte offset 
	_int64 m_file_byte_offset;
	// This stores the current number of tuple bytes retrieved
	_int64 m_tuple_bytes;

	// This stores the current client id
	int m_curr_client_id;
	// This stores the total number of key client sets
	int m_key_client_set_num;
	// This stores the total number of map client sets
	int m_map_client_set_num;
	// This stores a ptr to either the map or key client set number
	int *m_client_set_num_ptr;
	// This is true when maps are being distributed
	bool m_distribute_maps;

	// This stores the data directory 
	const char *m_data_dir;
	// This stores the work directory
	const char *m_work_dir;
	// This stores a ptr to the data handler function
	const char *m_data_handler_func;
	// This stores the function ptr
	void (CMapReducePrimatives::*m_distribute_func)(int client_id, int key_client_num, 
		int map_client_num, const char *data_handle_func, _int64 file_byte_offset, 
		_int64 tuple_bytes, int div_start, int div_end, const char *data_dir);

	// This creates a set of clients to distribute the keys for a set of comp blocks
	void DistributeClients(const char *data_dir) {

		m_tuple_bytes = 0;
		for(int i=0; i<m_seq_comp_blocks.Size(); i++) {
			m_tuple_bytes += m_seq_comp_blocks[i].uncompress_size;
		}

		(*m_client_set_num_ptr)++;
		if(m_distribute_maps == false) {
			m_file_div_bound.LastElement().end = *m_client_set_num_ptr;
		}

		(this->*m_distribute_func)(m_curr_client_id, HASH_DIV_NUM, HASH_DIV_NUM, 
			m_data_handler_func, m_file_byte_offset, m_tuple_bytes, 0, 0, data_dir);

		m_curr_client_id++;
		for(int i=0; i<m_seq_comp_blocks.Size(); i++) {
			m_file_byte_offset += m_seq_comp_blocks[i].compress_size;
		}

		m_seq_comp_blocks.Resize(0);
	}

	// This finds the total number of comp blocks for a given file set
    	int CompBlockNum(const char *dir) {

		int temp;
		int curr_client_set = 0;
		int comp_block_num = 0;

		while(true) {

			try {
				m_comp_block_size.OpenReadFile(CUtility::ExtendString
					(m_data_dir, curr_client_set++, ".comp_size"));

				while(m_comp_block_size.ReadCompObject(temp)) {
					m_comp_block_size.ReadCompObject(temp);
					comp_block_num++;
				}

			} catch(...) {
				break;
			}
		}

		return comp_block_num;
	}

	// This function loads in sequential comp blocks to determine client boundaries 
	// for individual segments. It then spawns off a number of processes to handle a
	// partioned segment of the file.
	// @param distribute_func - this is the function used to distribute keys and maps
	// @param data_handle_func - this is a ptr to the data handling function
	// @param max_file_seg_num - defines the maximum number of file segments to process
	//                         - at any one time
	void LoadFileSegments(int max_file_seg_num = -1) {

		if(max_file_seg_num < 0) {
			//max_file_seg_num = CompBlockNum(m_data_dir);
			//max_file_seg_num /= HASH_DIV_NUM;
			//max_file_seg_num++;
			max_file_seg_num = 100000000;
		}

		SCompBlock comp_block;
		int curr_client_set = 0;
		m_comp_block_size.OpenReadFile(CUtility::ExtendString
			(m_data_dir, curr_client_set, ".comp_size"));

		while(true) {
			while(m_comp_block_size.ReadCompObject(comp_block.compress_size)) {
				m_comp_block_size.ReadCompObject(comp_block.uncompress_size); 
				if(m_seq_comp_blocks.Size() >= max_file_seg_num) {
					DistributeClients(CUtility::ExtendString(m_data_dir, curr_client_set));
				}

				m_seq_comp_blocks.PushBack(comp_block);
			}

			if(m_seq_comp_blocks.Size() > 0) {
				DistributeClients(CUtility::ExtendString(m_data_dir, curr_client_set));
			} 

			try {
				curr_client_set++;
				m_comp_block_size.OpenReadFile(CUtility::ExtendString
					(m_data_dir, curr_client_set, ".comp_size"));

				if(m_distribute_maps == false) {
					m_file_div_bound.ExtendSize(1);
					m_file_div_bound.LastElement().Set(*m_client_set_num_ptr, *m_client_set_num_ptr);
				}

				m_file_byte_offset = 0;
			} catch(...) {
				WaitForPendingProcesses();
				break;
			}
		}
	}

	// This function creates processes to handle the processing of different 
	// hash divisions. The correct number of processes will be spawned 
	// depending on the total number of hash divisions remaining.
	void ProcessHashDivision() {

		for(int i=0; i<HASH_DIV_NUM; i++) {
			(this->*m_distribute_func)(i, m_key_client_set_num, 
				m_map_client_set_num, m_data_handler_func, 0, 0, 0, 0, m_data_dir);
		}

		WaitForPendingProcesses();
	}

	// This function creates processes to handle the processing of different 
	// hash divisions. The correct number of processes will be spawned 
	// depending on the total number of hash divisions remaining.
	void ApplyMapsToKeys() {

		int file_set_num = m_key_client_set_num >> 2;
		file_set_num++;

		for(int i=0; i<HASH_DIV_NUM; i++) {

			int offset = 0;
			while(true) {

				(this->*m_distribute_func)(i, m_key_client_set_num, 
					m_map_client_set_num, m_data_handler_func, 0, 0, offset,
					min(m_key_client_set_num, offset + file_set_num), m_data_dir);

				offset += file_set_num;
				if(offset >= m_key_client_set_num) {
					break;
				}
			}
		}

		WaitForPendingProcesses();
	}

	// This function spawns process off processes to handle the processing of
	// ordering keys and merging them into a single file as given in the original file set. 
	void OrderAndMergeFileSegments() {

		for(int i=0; i<m_file_div_bound.Size(); i++) {
			SBoundary &bound = m_file_div_bound[i];
			(this->*m_distribute_func)(i, HASH_DIV_NUM, HASH_DIV_NUM, 
				m_data_handler_func, 0, 0, bound.start, bound.end, m_data_dir);
		}

		WaitForPendingProcesses();
	}

	// This is used to create a set of sorted blocks
	void CreateSortedBlocks(const char *data_handle_func) {

		m_curr_client_id = 0;
		m_file_byte_offset = 0;
		m_key_client_set_num = 0;
		m_client_set_num_ptr = &m_key_client_set_num;

		m_distribute_maps = false;
		m_data_handler_func = data_handle_func;

		LoadFileSegments(40);
		ResetProcessSet();
	}

	// This merges the set of sorted blocks to create a single sorted block
	void MergeSortedBlocks(const char *data_handle_func) {

		int offset = 0;
		m_data_handler_func = data_handle_func;
		int width = m_key_client_set_num;
		int block_size = 64;

		while(width > block_size) {

			int new_width = 0;
			int end = offset + width;
			for(int i=0; i<width; i+=block_size) {
				SBoundary bound(offset, min(end, offset + block_size));

				(this->*m_distribute_func)(1, HASH_DIV_NUM, HASH_DIV_NUM, 
					m_data_handler_func, 0, 0, bound.start, bound.end, 
					CUtility::ExtendString(m_work_dir, end + new_width));

				offset += block_size;
				new_width++;
			}

			WaitForPendingProcesses();
			offset = end;
			width = new_width;
		}

		(this->*m_distribute_func)(0, HASH_DIV_NUM, HASH_DIV_NUM, 
				m_data_handler_func, 0, 0, offset, offset + width, 
				CUtility::ExtendString(m_data_dir, (int)0));

		WaitForPendingProcesses();
	}

public:

	CDistributeCompBlocks() {
		m_seq_comp_blocks.Initialize(4);
		m_file_div_bound.Initialize(4);

		m_file_div_bound.ExtendSize(1);
		m_file_div_bound.LastElement().Set(0, 0);
		HASH_DIV_NUM = 128;

		m_key_client_set_num = 0;
		m_map_client_set_num = 0;
	}

	// This sets the working directory and data directory as well as the 
	// the max key bytes and max map bytes.
	void SetAttributes(const char *work_dir, const char *data_dir, 
		int max_key_bytes, int max_map_bytes) {
			
		m_work_dir = work_dir;
		m_data_dir = data_dir;
		SetClientAttributes(work_dir, max_key_bytes, max_map_bytes);
	}

	// This sets the hash division number
	inline void SetHashDivNum(int hash_div_num) {
		if(hash_div_num < 0) {
			return;
		}

		HASH_DIV_NUM = hash_div_num;
	}

	// This function handles the distribution of keys among multiple clients
	// @param data_handle_func - the data processing handle name
	void DistributeKeys(const char *data_handle_func) {

		m_file_byte_offset = 0;
		m_curr_client_id = 0;
		m_key_client_set_num = 0;
		m_client_set_num_ptr = &m_key_client_set_num;
		m_distribute_maps = false;

		m_distribute_func = &CMapReducePrimatives::DistributeKeys;
		m_data_handler_func = data_handle_func;

		LoadFileSegments();
	}

	// This function handles the distribution of maps among multiple clients
	// @param data_handle_func - the data processing handle name
	void DistributeMaps(const char *data_handle_func) {

		m_file_byte_offset = 0;
		m_curr_client_id = 0;
		m_distribute_maps = true;

		m_map_client_set_num = 0;
		m_client_set_num_ptr = &m_map_client_set_num;

		m_distribute_func = &CMapReducePrimatives::DistributeMaps;
		m_data_handler_func = data_handle_func;

		LoadFileSegments();
	}

	// This function handles the distribution of key weights among multiple clients.
	// @param data_handle_func - the data processing handle name
	void DistributeKeyWeight(const char *data_handle_func) {

		m_curr_client_id = 0;
		m_file_byte_offset = 0;
		m_key_client_set_num = 0;
		m_client_set_num_ptr = &m_key_client_set_num;

		m_distribute_maps = false;
		m_distribute_func = &CMapReducePrimatives::DistributeKeyWeight;
		m_data_handler_func = data_handle_func;

		LoadFileSegments();
		ResetProcessSet();
	}

	// This is used to create a set of sorted blocks
	void CreateRadixSortedBlocks(const char *data_handle_func) {
		m_distribute_func = &CMapReducePrimatives::CreateRadixSortedBlock;
		CreateSortedBlocks(data_handle_func);
	}

	// This is used to create a set of sorted blocks
	void CreateQuickSortedBlocks(const char *data_handle_func) {
		m_distribute_func = &CMapReducePrimatives::CreateQuickSortedBlock;
		CreateSortedBlocks(data_handle_func);
	}

	// This merges the set of sorted blocks to create a single sorted block
	void MergeRadixSortedBlocks(const char *data_handle_func) {
		m_distribute_func = &CMapReducePrimatives::MergeRadixSortedBlocks;
		MergeSortedBlocks(data_handle_func);
	}

	// This merges the set of sorted blocks to create a single sorted block
	void MergeQuickSortedBlocks(const char *data_handle_func) {
		m_distribute_func = &CMapReducePrimatives::MergeQuickSortedBlocks;
		MergeSortedBlocks(data_handle_func);
	}

	// This function merges keys and sums the weight.
	// @param data_handle_func - the data processing handle name
	void FindKeyWeight(const char *data_handle_func) {

		m_distribute_func = &CMapReducePrimatives::FindKeyWeight;
		m_data_handler_func = data_handle_func;
		ProcessHashDivision();
		ResetProcessSet();

		for(int j=0; j<HASH_DIV_NUM; j++) {
			CHDFSFile::Remove(CUtility::ExtendString
				(m_work_dir, ".hash_node_set", j));
		}
	}

	// This function merges keys and sums the occurrence
	// @param data_handle_func - the data processing handle name
	void FindKeyOccurrence(const char *data_handle_func) {

		m_distribute_func = &CMapReducePrimatives::FindKeyOccurrence;
		m_data_handler_func = data_handle_func;
		ProcessHashDivision();
		ResetProcessSet();

		for(int j=0; j<HASH_DIV_NUM; j++) {
			CHDFSFile::Remove(CUtility::ExtendString
				(m_work_dir, ".hash_node_set", j));
		}
	}

	// This function applies a set of maps to a set of keys.
	// @param data_handle_func - the data processing handle name
	void ApplyMapsToKeys(const char *data_handle_func) {

		m_distribute_func = &CMapReducePrimatives::ApplyMapsToKeys;
		m_data_handler_func = data_handle_func;
		ApplyMapsToKeys();
		ResetProcessSet();
	}

	// This function groups key value pairs by their key value.
	// @param data_handle_func - the data processing handle name
	void MergeSet(const char *data_handle_func) {

		m_distribute_func = &CMapReducePrimatives::MergeSet;
		m_data_handler_func = data_handle_func;
		ProcessHashDivision();
		ResetProcessSet();
	}

	// This function groups key value pairs by their key value.
	// @param data_handle_func - the data processing handle name
	void MergeSortedSet(const char *data_handle_func) {

		m_distribute_func = &CMapReducePrimatives::MergeSortedSet;
		m_data_handler_func = data_handle_func;
		ProcessHashDivision();
		ResetProcessSet();
	}

	// This function applies the summed weight to each key.
	// @param data_handle_func - the data processing handle name
	void FindDuplicateKeyWeight(const char *data_handle_func) { 

		m_distribute_func = &CMapReducePrimatives::FindDuplicateKeyWeight;
		m_data_handler_func = data_handle_func;
		ProcessHashDivision();
		ResetProcessSet();
	}

	// This function applies the summed occurence to each key.
	// @param data_handle_func - the data processing handle name
	void FindDuplicateKeyOccurrence(const char *data_handle_func) {

		m_distribute_func = &CMapReducePrimatives::FindDuplicateKeyOccurrence;
		m_data_handler_func = data_handle_func;
		ProcessHashDivision();
		ResetProcessSet();
	}

	// This function writes the mapped keys back to client sets
	// in the original order that they were present.
	// @param data_handle_func - the data processing handle name
	void OrderMappedSets(const char *data_handle_func) {

		m_distribute_func = &CMapReducePrimatives::OrderMappedSets;
		m_data_handler_func = data_handle_func;
		m_curr_client_id = 0;
		OrderAndMergeFileSegments();
		ResetProcessSet();
	}

	// This function writes the mapped keys back to client sets
	// in the original order that they were present.
	// @param data_handle_func - the data processing handle name
	void OrderMappedOccurrences(const char *data_handle_func) {

		m_distribute_func = &CMapReducePrimatives::OrderMappedOccurrences;
		m_data_handler_func = data_handle_func;
		m_curr_client_id = 0;
		OrderAndMergeFileSegments();
		ResetProcessSet();
	}

};
