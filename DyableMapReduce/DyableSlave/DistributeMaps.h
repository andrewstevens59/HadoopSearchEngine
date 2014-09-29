#include "./DistributeKeyWeight.h"

// This class is responsible for distributing the maps for
// a map set based upon their hash value
class CDistributeMaps : public CSetNum {

	// This stores the hashed maps 
	CMemoryChunk<CHDFSFile> m_bucket_set;
	// This stores the map set directory
	CHDFSFile m_map_file;

	// This initializes the bucket set 
	// @param dir - this is the working directory to store the hashed
	//            - maps produced as output
	// @param map_set_dir - this is the directory of the map set
	// @param map_set_byte_offset - this stores the byte offset in the 
	//                            - map set file
	void Initialize(const char dir[], const char map_set_dir[],
		_int64 map_set_byte_offset) {

		int comp_buff_size = min(960000, MAX_MAPRED_BYTES / GetMapClientNum());
		m_bucket_set.AllocateMemory(GetMapClientNum());
		for(int i=0; i<GetMapClientNum(); i++) {
			this->m_bucket_set[i].OpenWriteFile(CUtility::ExtendString
				(dir, ".map_set", i, ".client", CSetNum::GetClientID()));
			this->m_bucket_set[i].InitializeCompression(comp_buff_size);
		}
                
		m_map_file.OpenReadFile(map_set_dir);
		m_map_file.SeekReadFileFromBeginning(map_set_byte_offset);
	}


public:

	CDistributeMaps() {
	}

	// This is the entry function that takes a map set and distribute
	// the maps appropriately, based upon their hash value.
	// @param work_dir - this is the working directory to store the hashed
	//                 - maps produced as output
	// @param map_set_dir - this is the directory of the map set
	// @param map_set_byte_offset - this stores the byte offset in the 
	//                            - map set file
	// @param tuple_bytes - this is the number of bytes to retrieve
	// @param max_key_bytes - this is the maximum number of bytes that 
	//                      - compose a key
	// @param max_map_bytes - this is the maximum number of bytes that 
	//                      - compose a map
	// @param retrieve_map - this is a function used to retrieve a map
	void DistributeMaps(const char work_dir[], const char map_set_dir[],
		_int64 map_set_byte_offset, _int64 tuple_bytes, int max_key_bytes, 
		int max_map_bytes, void (*retrieve_map)(CHDFSFile &map_file, char map[], 
			char key[], int &map_bytes, int &key_bytes)) {

		Initialize(work_dir, map_set_dir, map_set_byte_offset);

		int key_bytes;
		int map_bytes;
		CMemoryChunk<char> key_buff(max_key_bytes);
		CMemoryChunk<char> map_buff(max_map_bytes);

		while(tuple_bytes > 0) {

			retrieve_map(m_map_file, map_buff.Buffer(), 
				key_buff.Buffer(), map_bytes, key_bytes);

			int hash = CHashFunction::SimpleHash(GetMapClientNum(), 
				key_buff.Buffer(), key_bytes);

			if(key_bytes > max_key_bytes) {
				throw EIllegalArgumentException("Too many key bytes");
			}

			if(map_bytes > max_map_bytes) {
				throw EIllegalArgumentException("Too many map bytes");
			}

			tuple_bytes -= key_bytes + map_bytes;
			CHDFSFile &hash_file = m_bucket_set[hash];
			hash_file.AddEscapedItem(key_bytes);
			hash_file.AddEscapedItem(map_bytes);

			hash_file.WriteCompObject(key_buff.Buffer(), key_bytes);
			hash_file.WriteCompObject(map_buff.Buffer(), map_bytes);
		}
	}

	// This is the entry function that takes a map set and distribute
	// the maps appropriately, based upon their hash value.
	// @param work_dir - this is the working directory to store the hashed
	//                 - maps produced as output
	// @param map_set_dir - this is the directory of the map set
	// @param map_set_byte_offset - this stores the byte offset in the 
	//                            - map set file
	// @param tuple_bytes - this is the number of bytes to retrieve
	// @param map_bytes - this is the number of bytes that compose a map
	// @param key_bytes - this is the number of bytes that make up a key
	void DistributeMaps(const char work_dir[], const char map_set_dir[],
		_int64 map_set_byte_offset, _int64 tuple_bytes, int key_bytes, int map_bytes) {

		Initialize(work_dir, map_set_dir, map_set_byte_offset);

		CMemoryChunk<char> key_buff(key_bytes);
		CMemoryChunk<char> map_buff(map_bytes);
		while(tuple_bytes > 0) {

			m_map_file.ReadCompObject(key_buff.Buffer(), key_bytes);
			m_map_file.ReadCompObject(map_buff.Buffer(), map_bytes);
			tuple_bytes -= key_bytes + map_bytes;

			int hash = CHashFunction::SimpleHash
				(GetMapClientNum(), key_buff.Buffer(), key_bytes);

			CHDFSFile &bucket_file = this->m_bucket_set[hash];
			bucket_file.AddEscapedItem(key_bytes);
			bucket_file.AddEscapedItem(map_bytes);
			bucket_file.WriteCompObject(key_buff.Buffer(), key_bytes);
			bucket_file.WriteCompObject(map_buff.Buffer(), map_bytes);
		}
	}

};
