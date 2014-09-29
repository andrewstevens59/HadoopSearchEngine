#include "../../ProcessSet.h"

// This composes the server side MapReduce primatives that are used to
// perform various MapReduce tasks by multiple clients.
class CMapReducePrimatives {

	// This is used to spawn the the set of processes
	static CProcessSet m_process_set;
	// This stores the maximum number of bytes that make up the key
	int m_max_key_bytes;
	// This stores the maximum number of bytes that make up the map
	int m_max_map_bytes;
	// This stores the working directory
	const char *m_work_dir;
	// This stores the data type for the MapReduce
	const char *m_data_type;

	// This function formats the request to one of the slave MapReduce primatives.
	// @param client_id - this is the current client set being processed
	// @param key_client_num - the number of key client sets
	// @param map_client_num - the number of map client sets
	// @param file_byte_offset - the byte offset in the client file
	// @param tuple_bytes - the number of bytes to process
	// @param div_start - the lower bound on the client division set
	// @param div_end - the upper bound on the client division set
	// @param request - the type of map reduce primative
	// @param data_handle_func - the data processing handle name
	// @param data_dir - this stores the directory of the data set
	void ProcessCommand(int client_id, int key_client_num, int map_client_num, 
		_int64 file_byte_offset, _int64 tuple_bytes, int div_start,
		int div_end, const char *request, const char *data_handle_func,
		const char *data_dir) {

		CString comand("Index ");

		comand += client_id;
		comand += " ";
		comand += key_client_num;
		comand += " ";
		comand += map_client_num;
		comand += " ";
		comand += request;
		comand += " ";

		if(data_handle_func == NULL || strlen(data_handle_func) == 0) {
			comand += "NULL";
		} else {
			comand += data_handle_func;
		}

		comand += " ";
		comand += m_work_dir;
		comand += " ";
		comand += data_dir;
		comand += " ";
		comand += div_start;
		comand += " ";
		comand += div_end;
		comand += " ";
		comand += m_max_key_bytes;
		comand += " ";
		comand += m_max_map_bytes;
		comand += " ";
		comand += m_process_set.GetPort();
		comand += " ";
		comand += file_byte_offset;
		comand += " ";
		comand += tuple_bytes;
		comand += " ";
		comand += m_data_type;

		m_process_set.CreateRemoteProcess
			("../DyableSlave/Debug/DyableSlave.exe", comand.Buffer(), client_id);
	}

public:

	CMapReducePrimatives() {
		m_data_type = "int";
		m_process_set.SetPort(3000);
	}

	// This sets the working directory and data directory as well as the 
	// the max key bytes and max map bytes.
	void SetClientAttributes(const char *work_dir,
		int max_key_bytes, int max_map_bytes) {

		m_work_dir = work_dir;
		m_max_key_bytes = max_key_bytes;
		m_max_map_bytes = max_map_bytes;
	}

	// This returns a reference to the process set
	inline CProcessSet &ProcessSet() {
		return m_process_set;
	}

	// This sets the data type
	inline void SetDataType(const char *data_type) {

		if(strlen(data_type) > 0) {
			m_data_type = data_type;
		}
	}

	// This function handles the distribution of keys among multiple clients
	// @param client_id - this is the current client set being processed
	// @param client_num - the number of client sets to distribute sets to
	// @param data_handle_func - the data processing handle name
	// @param file_byte_offset - the byte offset in the client file
	// @param tuple_bytes - the number of bytes to process
	// @param div_start - the lower bound on the client division set
	// @param div_end - the upper bound on the client division set
	// @param data_dir - this stores the directory of the data set
	void DistributeKeys(int client_id, int key_client_num, int map_client_num, 
		const char *data_handle_func, _int64 file_byte_offset, _int64 tuple_bytes, 
		int div_start, int div_end, const char *data_dir) {

		return ProcessCommand(client_id, key_client_num, map_client_num, file_byte_offset,
			tuple_bytes, 0, 0, "DistributeKeys", data_handle_func, data_dir);
	}

	// This function handles the distribution of maps among multiple clients
	// @param client_id - this is the current client set being processed
	// @param client_num - the number of client sets to distribute sets to
	// @param data_handle_func - the data processing handle name
	// @param file_byte_offset - the byte offset in the client file
	// @param tuple_bytes - the number of bytes to process
	// @param div_start - the lower bound on the client division set
	// @param div_end - the upper bound on the client division set
	void DistributeMaps(int client_id, int key_client_num, int map_client_num, 
		const char *data_handle_func, _int64 file_byte_offset, _int64 tuple_bytes, 
		int div_start, int div_end, const char *data_dir) {

		return ProcessCommand(client_id, key_client_num, map_client_num, file_byte_offset,
			tuple_bytes, 0, 0, "DistributeMaps", data_handle_func, data_dir);
	}

	// This function handles the distribution of key weights among 
	// multiple clients
	// @param client_id - this is the current client set being processed
	// @param client_num - the number of client sets to distribute sets to
	// @param data_handle_func - the data processing handle name
	// @param file_byte_offset - the byte offset in the client file
	// @param tuple_bytes - the number of bytes to process
	// @param div_start - the lower bound on the client division set
	// @param div_end - the upper bound on the client division set
	void DistributeKeyWeight(int client_id, int key_client_num, int map_client_num, 
		const char *data_handle_func, _int64 file_byte_offset, _int64 tuple_bytes, 
		int div_start, int div_end, const char *data_dir) {
		
		ProcessCommand(client_id, key_client_num, map_client_num, file_byte_offset,
			tuple_bytes, 0, 0, "DistributeKeyWeight", data_handle_func, data_dir);
	}

	// This is used to create a set of sorted blocks
	// @param client_id - this is the current client set being processed
	// @param client_num - the number of client sets to distribute sets to
	// @param data_handle_func - the data processing handle name
	// @param file_byte_offset - the byte offset in the client file
	// @param tuple_bytes - the number of bytes to process
	// @param div_start - the lower bound on the client division set
	// @param div_end - the upper bound on the client division set
	void CreateRadixSortedBlock(int client_id, int key_client_num, int map_client_num, 
		const char *data_handle_func, _int64 file_byte_offset, _int64 tuple_bytes, 
		int div_start, int div_end, const char *data_dir) {
		
		ProcessCommand(client_id, key_client_num, map_client_num, file_byte_offset,
			tuple_bytes, 0, 0, "CreateRadixSortedBlock", data_handle_func, data_dir);
	}

	// This is used to create a set of sorted blocks
	// @param client_id - this is the current client set being processed
	// @param client_num - the number of client sets to distribute sets to
	// @param data_handle_func - the data processing handle name
	// @param file_byte_offset - the byte offset in the client file
	// @param tuple_bytes - the number of bytes to process
	// @param div_start - the lower bound on the client division set
	// @param div_end - the upper bound on the client division set
	void CreateQuickSortedBlock(int client_id, int key_client_num, int map_client_num, 
		const char *data_handle_func, _int64 file_byte_offset, _int64 tuple_bytes, 
		int div_start, int div_end, const char *data_dir) {
		
		ProcessCommand(client_id, key_client_num, map_client_num, file_byte_offset,
			tuple_bytes, 0, 0, "CreateQuickSortedBlock", data_handle_func, data_dir);
	}

	// This merges the set of sorted blocks to create a single sorted block
	// @param client_id - this is the current client set being processed
	// @param client_num - the number of client sets to distribute sets to
	// @param data_handle_func - the data processing handle name
	// @param file_byte_offset - the byte offset in the client file
	// @param tuple_bytes - the number of bytes to process
	// @param div_start - the lower bound on the client division set
	// @param div_end - the upper bound on the client division set
	void MergeRadixSortedBlocks(int client_id, int key_client_num, int map_client_num, 
		const char *data_handle_func, _int64 file_byte_offset, _int64 tuple_bytes, 
		int div_start, int div_end, const char *data_dir) {
		
		ProcessCommand(client_id, key_client_num, map_client_num, file_byte_offset,
			tuple_bytes, div_start, div_end, "MergeRadixSortedBlocks", data_handle_func, data_dir);
	}

	// This merges the set of sorted blocks to create a single sorted block
	// @param client_id - this is the current client set being processed
	// @param client_num - the number of client sets to distribute sets to
	// @param data_handle_func - the data processing handle name
	// @param file_byte_offset - the byte offset in the client file
	// @param tuple_bytes - the number of bytes to process
	// @param div_start - the lower bound on the client division set
	// @param div_end - the upper bound on the client division set
	void MergeQuickSortedBlocks(int client_id, int key_client_num, int map_client_num, 
		const char *data_handle_func, _int64 file_byte_offset, _int64 tuple_bytes, 
		int div_start, int div_end, const char *data_dir) {
		
		ProcessCommand(client_id, key_client_num, map_client_num, file_byte_offset,
			tuple_bytes, div_start, div_end, "MergeQuickSortedBlocks", data_handle_func, data_dir);
	}

	// This function merges keys and sums the weight
	// @param client_id - this is the current client set being processed
	// @param client_num - the number of client sets to distribute sets to
	// @param data_handle_func - the data processing handle name
	// @param file_byte_offset - the byte offset in the client file
	// @param tuple_bytes - the number of bytes to process
	// @param div_start - the lower bound on the client division set
	// @param div_end - the upper bound on the client division set
	void FindKeyWeight(int client_id, int key_client_num, int map_client_num, 
		const char *data_handle_func, _int64 file_byte_offset, _int64 tuple_bytes, 
		int div_start, int div_end, const char *data_dir) {

		ProcessCommand(client_id, key_client_num, map_client_num, 0,
			0, 0, 0, "FindKeyWeight", data_handle_func, data_dir);
	}

	// This function merges keys and sums the occurrence
	// @param client_id - this is the current client set being processed
	// @param client_num - the number of client sets to distribute sets to
	// @param data_handle_func - the data processing handle name
	// @param file_byte_offset - the byte offset in the client file
	// @param tuple_bytes - the number of bytes to process
	// @param div_start - the lower bound on the client division set
	// @param div_end - the upper bound on the client division set
	void FindKeyOccurrence(int client_id, int key_client_num, int map_client_num, 
		const char *data_handle_func, _int64 file_byte_offset, _int64 tuple_bytes, 
		int div_start, int div_end, const char *data_dir) {

		ProcessCommand(client_id, key_client_num, map_client_num, 0,
			0, 0, 0, "FindKeyOccurrence", data_handle_func, data_dir);
	}

	// This function applies a set of maps to a set of keys
	// @param client_id - this is the current client set being processed
	// @param client_num - the number of client sets to distribute sets to
	// @param data_handle_func - the data processing handle name
	// @param file_byte_offset - the byte offset in the client file
	// @param tuple_bytes - the number of bytes to process
	// @param div_start - the lower bound on the client division set
	// @param div_end - the upper bound on the client division set
	void ApplyMapsToKeys(int client_id, int key_client_num, int map_client_num, 
		const char *data_handle_func, _int64 file_byte_offset, _int64 tuple_bytes, 
		int div_start, int div_end, const char *data_dir) {

		ProcessCommand(client_id, key_client_num, map_client_num, 0,
			0, div_start, div_end, "ApplyMapsToKeys", data_handle_func, data_dir);
	}

	// This function groups key value pairs by their key value
	// @param client_id - this is the current client set being processed
	// @param client_num - the number of client sets to distribute sets to
	// @param data_handle_func - the data processing handle name
	// @param file_byte_offset - the byte offset in the client file
	// @param tuple_bytes - the number of bytes to process
	// @param div_start - the lower bound on the client division set
	// @param div_end - the upper bound on the client division set
	void MergeSet(int client_id, int key_client_num, int map_client_num, 
		const char *data_handle_func, _int64 file_byte_offset, _int64 tuple_bytes, 
		int div_start, int div_end, const char *data_dir) {

		ProcessCommand(client_id, key_client_num, map_client_num, 0,
			0, 0, 0, "MergeSet", data_handle_func, data_dir);
	}

	// This function groups key value pairs by their key value
	// @param client_id - this is the current client set being processed
	// @param client_num - the number of client sets to distribute sets to
	// @param data_handle_func - the data processing handle name
	// @param file_byte_offset - the byte offset in the client file
	// @param tuple_bytes - the number of bytes to process
	// @param div_start - the lower bound on the client division set
	// @param div_end - the upper bound on the client division set
	void MergeSortedSet(int client_id, int key_client_num, int map_client_num, 
		const char *data_handle_func, _int64 file_byte_offset, _int64 tuple_bytes, 
		int div_start, int div_end, const char *data_dir) {

		ProcessCommand(client_id, key_client_num, map_client_num, 0,
			0, 0, 0, "MergeSortedSet", data_handle_func, data_dir);
	}

	// This function applies the summed weight to each key
	// @param client_id - this is the current client set being processed
	// @param client_num - the number of client sets to distribute sets to
	// @param data_handle_func - the data processing handle name
	// @param file_byte_offset - the byte offset in the client file
	// @param tuple_bytes - the number of bytes to process
	// @param div_start - the lower bound on the client division set
	// @param div_end - the upper bound on the client division set
	void FindDuplicateKeyWeight(int client_id, int key_client_num, int map_client_num, 
		const char *data_handle_func, _int64 file_byte_offset, _int64 tuple_bytes, 
		int div_start, int div_end, const char *data_dir) {

		ProcessCommand(client_id, key_client_num, map_client_num, 0,
			0, 0, 0, "FindDuplicateKeyWeight", data_handle_func, data_dir);
	}

	// This function applies the summed occurence to each key
	// @param client_id - this is the current client set being processed
	// @param client_num - the number of client sets to distribute sets to
	// @param data_handle_func - the data processing handle name
	// @param file_byte_offset - the byte offset in the client file
	// @param tuple_bytes - the number of bytes to process
	// @param div_start - the lower bound on the client division set
	// @param div_end - the upper bound on the client division set
	void FindDuplicateKeyOccurrence(int client_id, int key_client_num, int map_client_num, 
		const char *data_handle_func, _int64 file_byte_offset, _int64 tuple_bytes, 
		int div_start, int div_end, const char *data_dir) {

		ProcessCommand(client_id, key_client_num, map_client_num, 0,
			0, 0, 0, "FindDuplicateKeyOccurrence", data_handle_func, data_dir);
	}

	// This function writes the mapped keys back to client sets
	// in the original order that they were present
	// @param client_id - this is the current client set being processed
	// @param client_num - the number of client sets to distribute sets to
	// @param data_handle_func - the data processing handle name
	// @param file_byte_offset - the byte offset in the client file
	// @param tuple_bytes - the number of bytes to process
	// @param div_start - the lower bound on the client division set
	// @param div_end - the upper bound on the client division set
	void OrderMappedSets(int client_id, int key_client_num, int map_client_num, 
		const char *data_handle_func, _int64 file_byte_offset, _int64 tuple_bytes, 
		int div_start, int div_end, const char *data_dir) {

		ProcessCommand(client_id, key_client_num, map_client_num, 0,
			0, div_start, div_end, "OrderMappedSets", data_handle_func, data_dir);
	}

	// This function writes the mapped keys back to client sets
	// in the original order that they were present
	// @param client_id - this is the current client set being processed
	// @param client_num - the number of client sets to distribute sets to
	// @param data_handle_func - the data processing handle name
	// @param file_byte_offset - the byte offset in the client file
	// @param tuple_bytes - the number of bytes to process
	// @param div_start - the lower bound on the client division set
	// @param div_end - the upper bound on the client division set
	void OrderMappedOccurrences(int client_id, int key_client_num, int map_client_num, 
		const char *data_handle_func, _int64 file_byte_offset, _int64 tuple_bytes, 
		int div_start, int div_end, const char *data_dir) {

		ProcessCommand(client_id, key_client_num, map_client_num, 0,
			0, div_start, div_end, "OrderMappedOccurrences", data_handle_func, data_dir);
	}

	// This waits for pending processes to complete
	inline void WaitForPendingProcesses() {
		m_process_set.WaitForPendingProcesses();
	}

	// This resets the process set
	inline void ResetProcessSet() {
		m_process_set.ResetProcessSet();
	}
};
CProcessSet CMapReducePrimatives::m_process_set;
