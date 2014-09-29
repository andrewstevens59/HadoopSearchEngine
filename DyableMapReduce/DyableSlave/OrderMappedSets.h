#include "./FindKeyOccurrence.h"

// This class recombines the mapped keys to a complete file in the order that 
// they were supplied. There may have been multiple client files originally
// supplied that need to be recreated. A seperate process is used for each
// individual client file.
class COrderMappedSets : public CSetNum {

	// This file stores the hash set for which a node was
	// placed so that it can be retrieved in the correct order
	CHDFSFile m_node_hash_file;

	// This stores the directory for the node map
	char m_directory[500];
	// This stores the number of bytes allowed to load a 
	// bucket division for the comp buff
	int m_comp_buff_size;
	// This stores the maximum number of bytes that make up a key
	int m_max_key_bytes;
	// This stores the maximum number of bytes that make up a map value
	int m_max_map_bytes;

	// This takes the newly mapped keys and puts them back in
	// the same order that they were given. This is so they
	// can be matched up to the original LocalData structure supplied
	// by the user. This is done by reading the hash meta LocalData
	// file that stores the hash for each node in the order it
	// was supplied.
	// @param output_file - this is used to store the mapped 
	//                    - in the order that they were given
	// @param write_map - this is a function pointer that writes the 
	//                  - the final mapped value to file
	// @param retrieve_map - this method simply reads a map from a file
	// @param set_id - this is the current set being processed
	void MergeMapSets(CSegFile &output_file,
		void (*write_map)(CHDFSFile &to_file, char map[], int &map_bytes,
			char key[], int &key_bytes), int set_id) {

		CMemoryChunk<CHDFSFile> key_hash_set(CSetNum::GetKeyClientNum());
		for(int i=0; i<GetKeyClientNum(); i++) {
			key_hash_set[i].OpenReadFile(CUtility::ExtendString
				(m_directory, ".mapped_set", i, ".client", set_id));
		}

		m_node_hash_file.OpenReadFile(CUtility::ExtendString
			(m_directory, ".hash_node_set", set_id));

		uChar bucket;
		int key_bytes;
		int map_bytes;
		uLong bytes;
		CMemoryChunk<char> key_buff(m_max_key_bytes);
		CMemoryChunk<char> map_buff(m_max_map_bytes);
		while(m_node_hash_file.ReadCompObject(bucket)) {

			CHDFSFile &key_file = key_hash_set[bucket];
			key_file.GetEscapedItem(bytes);
			key_bytes = bytes;
			key_file.GetEscapedItem(bytes);
			map_bytes = bytes;

			key_file.ReadCompObject(key_buff.Buffer(), key_bytes);

			if(map_bytes != 0) {
				key_file.ReadCompObject(map_buff.Buffer(), map_bytes);
			}

			output_file.AskBufferOverflow(key_bytes + map_bytes + 10);

			if(write_map != NULL) {
				write_map(output_file, map_buff.Buffer(), map_bytes, 
					key_buff.Buffer(), key_bytes);

				continue;
			} 

			output_file.WriteCompObject(key_buff.Buffer(), key_bytes);
			output_file.WriteCompObject(map_buff.Buffer(), map_bytes);
		}
	}

	// This takes the newly mapped keys and puts them back in
	// the same order that they were given. This is so they
	// can be matched up to the original LocalData structure supplied
	// by the user. This is done by reading the hash meta LocalData
	// file that stores the hash for each node in the order it
	// was supplied.
	// @param output_file - this is used to store the mapped 
	//                    - in the order that they were given
	// @param write_map - this is a function pointer that writes the 
	//                  - the final mapped value to file
	// @param retrieve_map - this method simply reads a map from a file
	// @param set_id - this is the current set being processed
	template <class X>
	void MergeMapOccurrences(CSegFile &output_file,
		void (*write_occur)(CHDFSFile &map_file, char key[], 
			int bytes, const X &occurr), int set_id) {

		CMemoryChunk<CHDFSFile> key_hash_set(CSetNum::GetKeyClientNum());
		for(int i=0; i<GetKeyClientNum(); i++) {
			key_hash_set[i].OpenReadFile(CUtility::ExtendString
				(m_directory, ".mapped_set", i, ".client", set_id));
		}

		m_node_hash_file.OpenReadFile(CUtility::ExtendString
			(m_directory, ".hash_node_set", set_id));

		X occur;
		uLong bytes;
		uChar bucket;
		int key_bytes;
		CMemoryChunk<char> key_buff(m_max_key_bytes);
		while(m_node_hash_file.ReadCompObject(bucket)) {

			CHDFSFile &key_file = key_hash_set[bucket];
			key_file.GetEscapedItem(bytes);
			key_bytes = bytes;

			key_file.ReadCompObject(key_buff.Buffer(), key_bytes);
			key_file.ReadCompObject(occur);

			output_file.AskBufferOverflow(key_bytes + sizeof(X) + 10);

			if(write_occur != NULL) {
				write_occur(output_file, key_buff.Buffer(), key_bytes, occur);
				continue;
			} 

			output_file.WriteCompObject(key_buff.Buffer(), key_bytes);
			output_file.WriteCompObject(occur);
		}
	}

public:

	COrderMappedSets() {
	}

	// This takes the set of mapped keys belonging to a client and merges 
	// them into a single file in the order in which they were present
	// in the original set.
	// @param work_dir - this is the working directory of the MapReduce
	// @param output_dir - this is the output directory of the mapped keys
	// @param write_map - this is a function pointer that writes the 
	//                  - the final mapped value to file
	// @param set_bound - this is the number of individual sets that make up
	//                  - the original client key set
	// @param max_key_bytes - this stores the maximum number of bytes that 
	//                      - make up a key
	// @param max_map_bytes - this stores the maximum number of bytes that
	//                      - make up a map value
	void OrderMappedSets(const char work_dir[], const char output_dir[],
		void (*write_map)(CHDFSFile &to_file, char map[], int &map_bytes,
			char key[], int &key_bytes), SBoundary &set_bound, 
			int max_key_bytes, int max_map_bytes) {

		strcpy(m_directory, work_dir);
		m_max_key_bytes = max_key_bytes;
		m_max_map_bytes = max_map_bytes;

		m_comp_buff_size = min((int)960000, MAX_MAPRED_BYTES
			/ CSetNum::GetKeyClientNum());

		CSegFile output_file;
		output_file.OpenWriteFile(CUtility::ExtendString
			(output_dir, CSetNum::GetClientID()));

		for(int i=set_bound.start; i<set_bound.end; i++) {
			MergeMapSets(output_file, write_map, i);
		}
	}

	// This takes the set of keys and their occurrence and then merges
	// them into a single file in the same order they were present in
	// the original client set.
	// @param work_dir - this is the working directory of the MapReduce
	// @param output_dir - this is the output directory of the mapped keys
	// @param max_key_bytes - this stores the maximum number of bytes that 
	//                      - make up a key
	// @param write_occur - this is the function that writes the key occurrence
	template <class X>
	void OrderMappedOccurrences(const char work_dir[], const char output_dir[],
		int max_key_bytes, SBoundary &set_bound, 
		void (*write_occur)(CHDFSFile &map_file, char key[], 
			int bytes, const X &occurr)) {

		strcpy(m_directory, work_dir);
		m_max_key_bytes = max_key_bytes;

		m_comp_buff_size = min((int)960000, MAX_MAPRED_BYTES
			/ CSetNum::GetKeyClientNum());

		CSegFile output_file;
		output_file.OpenWriteFile(CUtility::ExtendString
			(output_dir, CSetNum::GetClientID()));

		for(int i=set_bound.start; i<set_bound.end; i++) {
			MergeMapOccurrences(output_file, write_occur, i);
		}
	}
};