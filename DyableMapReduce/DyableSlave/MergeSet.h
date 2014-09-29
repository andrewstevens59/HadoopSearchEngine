#include "../../MapReduce.h"

static const int MAX_MAPRED_BYTES = 1000000;

// This class is used to store the total number of key and map client sets
class CSetNum {

	// This stores the total number of key client sets
	static int m_key_client_set_num;
	// This stores the total number of map client sets
	static int m_map_client_set_num;

public:

	CSetNum() {
	}

	// Set the total number of key client sets
	inline static void SetKeyClientNum(int key_set_num) {
		m_key_client_set_num = key_set_num;
	}
	
	// Set the total number of map client sets
	inline static void SetMapClientNum(int map_set_num) {
		m_map_client_set_num = map_set_num;
	}

	// Returns the total number of key client sets
	inline static int GetKeyClientNum() {
		return m_key_client_set_num;
	}

	// Returns the total number of map client sets
	inline static int GetMapClientNum() {
		return m_map_client_set_num;
	}

	// This returns the client id
	inline static int GetClientID() {
		return CNodeStat::GetClientID();
	}
};
int CSetNum::m_key_client_set_num;
int CSetNum::m_map_client_set_num;

// This class takes an arbitrary set of key value pairs and groups 
// values by their key value, in the same physical location in the
// file. This class handles keys and values of an arbitrary byte 
// length. The usual parallel paradigm is used to distribute the 
// work load. Internally a linked list is used in each bucket 
// division to group values with the same key.
class CMergeMap : public CSetNum {
	
	// This is used to store a list of linked values 
	// that are grouped by a particular key
	struct SValueSet {
		// This stores the value byte offset
		uLong value_offset;
		// This stores the size of the map
		int map_bytes;
		// This stores as ptr to the next value
		SValueSet *next_ptr;
	};

	// This defines a sorted key value
	struct SSortedKey {
		// This is the id of the key
		int id;
		// This is the key value
		_int64 key;
	};

	// This stores the directory for the node map
	char m_directory[500];
	// This stores the number of bytes allowed to load a 
	// bucket division for the comp buff
	int m_comp_buff_size;

	// This stores the different bucket files that groups
	// similar clusters together based on their cluster id
	CMemoryChunk<CHDFSFile> m_bucket_set;
	// This stores the internal mapping between a key
	// and its respective value for a given bucket division
	// This stores the maximum number of bytes that make up a key
	int m_max_key_bytes;
	// This stores the maximum number of bytes that make up a map
	int m_max_map_bytes;

	CHashDictionary<int> m_key_map;
	// This stores the set of values for a key
	CArrayList<SValueSet *> m_value_set;
	// This stores the list of linkec value ptrs
	CLinkedBuffer<SValueSet> m_set_buff;
	// This stores the values in the current bucket division
	CArrayList<char> m_value_buff;

	// This is used to sort keys in ascending order
	static int CompareKeys(const SSortedKey &arg1, const SSortedKey &arg2) {

		if(arg1.key < arg2.key) {
			return 1;
		}

		if(arg1.key > arg2.key) {
			return -1;
		}

		return 0;
	}

	// This loads each key contained in a bucket set into memory.
	// It also keeps track of the number of times a key occurrs.
	// @param set_file - this stores the key value pairs
	void AssignMapValues(CHDFSFile &set_file, char key_buff[]) {

		uLong key_bytes;
		uLong map_bytes;
		while(set_file.GetEscapedItem(key_bytes) >= 0) {
			set_file.GetEscapedItem(map_bytes);
			set_file.ReadCompObject(key_buff, key_bytes);

			int id = m_key_map.AddWord(key_buff, key_bytes);
			if(!m_key_map.AskFoundWord()) {
				m_value_set.PushBack(NULL);
			} 

			SValueSet *prev_ptr = m_value_set[id];
			m_value_set[id] = m_set_buff.ExtendSize(1);

			SValueSet *curr_ptr = m_value_set[id];
			curr_ptr->map_bytes = map_bytes;
			curr_ptr->value_offset = m_value_buff.Size();
			curr_ptr->next_ptr = prev_ptr;

			m_value_buff.ExtendSize(map_bytes);
			set_file.ReadCompObject(m_value_buff.Buffer() + 
				curr_ptr->value_offset, map_bytes);
		}
	}

	// This writes the set of grouped key vaue pairs out to external storage.
	// @param output_file - this stores the sets grouped by the key
	// @param write_key_weight - this writes a unique key and the weight
	//						   - of each key
	void WriteGroupedSets(CSegFile &output_file, void (*write_set)(CHDFSFile &file, 
		int &key_bytes, char key_buff[], int &map_bytes, char map_buff[])) {

		int key_bytes;
		for(int i=0; i<m_key_map.Size(); i++) {
			SValueSet *curr_ptr = m_value_set[i];
			char *key = m_key_map.GetWord(i, key_bytes);

			while(curr_ptr != NULL) {
				if(write_set != NULL) {
					write_set(output_file, key_bytes, key, curr_ptr->map_bytes,
						m_value_buff.Buffer() + curr_ptr->value_offset);
				} else {
					output_file.WriteCompObject(key, key_bytes);
					output_file.WriteCompObject(m_value_buff.Buffer()
						+ curr_ptr->value_offset, curr_ptr->map_bytes);
				}
				curr_ptr = curr_ptr->next_ptr;
			}	
		}
	}

	// This writes the set of grouped key vaue pairs out to external storage.
	// @param output_file - this stores the sets grouped by the key
	// @param write_key_weight - this writes a unique key and the weight
	//						   - of each key
	void WriteSortedGroupedSets(CSegFile &output_file, void (*write_set)(CHDFSFile &file, 
		int &key_bytes, char key_buff[], int &map_bytes, char map_buff[])) {

		int key_bytes;
		CMemoryChunk<SSortedKey> sort_buff(m_key_map.Size());
		for(int i=0; i<m_key_map.Size(); i++) {
			SValueSet *curr_ptr = m_value_set[i];
			char *key = m_key_map.GetWord(i, key_bytes);
			SSortedKey &sort_key = sort_buff[i];

			if(key_bytes >= sizeof(_int64)) {
				cout<<"key bytes overflow";getchar();
			}

			sort_key.id = i;
			sort_key.key = 0;
			memcpy((char *)&sort_key.key, key, key_bytes);
		}

		CSort<SSortedKey> sort(sort_buff.OverflowSize(), CompareKeys);
		sort.HybridSort(sort_buff.Buffer());

		for(int i=0; i<sort_buff.OverflowSize(); i++) {
			int id = sort_buff[i].id;

			SValueSet *curr_ptr = m_value_set[id];
			char *key = m_key_map.GetWord(id, key_bytes);

			while(curr_ptr != NULL) {
				if(write_set != NULL) {
					write_set(output_file, key_bytes, key, curr_ptr->map_bytes,
						m_value_buff.Buffer() + curr_ptr->value_offset);
				} else {
					output_file.WriteCompObject(key, key_bytes);
					output_file.WriteCompObject(m_value_buff.Buffer()
						+ curr_ptr->value_offset, curr_ptr->map_bytes);
				}
				curr_ptr = curr_ptr->next_ptr;
			}	
		}
	}

	// This takes all the bucket sets that belong to a particular client
	// and loads the keys into memory. It then groups all the values in 
	// a bucket by it's key value. It then writes the grouped values 
	// out to an external file.
	// @param output_file - this stores the sets grouped by the key
	void PerformMapping(CSegFile &output_file) {

		output_file.OpenWriteFile();

		CHDFSFile curr_key_file;
		m_value_set.Initialize((MAX_MAPRED_BYTES / 
			max(m_max_key_bytes, m_max_map_bytes)) + 1000);

		m_set_buff.Initialize(2048);
		m_value_buff.Initialize(m_value_set.OverflowSize());
		m_key_map.Initialize();

		CMemoryChunk<char> buff(m_max_key_bytes);
		for(int j=0; j<CSetNum::GetMapClientNum(); j++) {
					
			// first load the explicit mapping for each key
			curr_key_file.OpenReadFile(CUtility::ExtendString
				(m_directory, ".map_set", CSetNum::GetClientID(), ".client", j));

			AssignMapValues(curr_key_file, buff.Buffer());
		}
	}

public:

	CMergeMap() {
	}

	// This takes the set of keys and reduces them to find the occurrence
	// of each unique key. It then writes the key and its occurrence out
	// to file to be processed later.
	// @param work_dir - this is the working directory of the MapReduce
	// @param output_dir - this is the output directory of the mapped keys
	// @param max_key_bytes - this stores the maximum number of bytes that 
	//                      - make up a key
	// @param max_map_bytes - this stores the maximum number of bytes that
	//                      - make up a map
	// @param write_set - this is the function that writes a set
	void MergeSet(const char work_dir[], const char output_dir[],
		int max_key_bytes, int max_map_bytes,
		void (*write_set)(CHDFSFile &file, int &key_bytes, 
			char key_buff[], int &map_bytes, char map_buff[])) {

		strcpy(m_directory, work_dir);
		m_max_key_bytes = max_key_bytes;
		m_max_map_bytes = max_map_bytes;

		CSegFile merge_file;
		merge_file.OpenWriteFile(CUtility::ExtendString
			(output_dir, GetClientID()));

		PerformMapping(merge_file);
		WriteGroupedSets(merge_file, write_set);
	}

	// This takes the set of keys and reduces them to find the occurrence
	// of each unique key. It then writes the key and its occurrence out
	// to file to be processed later.
	// @param work_dir - this is the working directory of the MapReduce
	// @param output_dir - this is the output directory of the mapped keys
	// @param max_key_bytes - this stores the maximum number of bytes that 
	//                      - make up a key
	// @param max_map_bytes - this stores the maximum number of bytes that
	//                      - make up a map
	// @param write_set - this is the function that writes a set
	void MergeSortedSet(const char work_dir[], const char output_dir[],
		int max_key_bytes, int max_map_bytes,
		void (*write_set)(CHDFSFile &file, int &key_bytes, 
			char key_buff[], int &map_bytes, char map_buff[])) {
	
		strcpy(m_directory, work_dir);
		m_max_key_bytes = max_key_bytes;
		m_max_map_bytes = max_map_bytes;

		CSegFile merge_file;
		merge_file.OpenWriteFile(CUtility::ExtendString
			(output_dir, GetClientID()));

		PerformMapping(merge_file);
		WriteSortedGroupedSets(merge_file, write_set);
	}
};