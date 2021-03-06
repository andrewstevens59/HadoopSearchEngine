#include "./DistributeMaps.h"

// This class is responsible for distributing the keys for
// a key set based upon their hash value
class CDistributeKeys : public CSetNum {

	// This stores the hashed keys 
	CMemoryChunk<CHDFSFile> m_bucket_set;
	// This stores the key set directory
	CHDFSFile m_key_file;
	// This stores the hash value of each key in the set
	CHDFSFile m_hash_node_file;

	// This initializes the bucket set 
	// @param dir - this is the working directory to store the hashed
	//            - keys produced as output
	// @param key_set_dir - this is the directory of the key set
	// @param key_set_byte_offset - this stores the byte offset in the 
	//                            - key set file
	void Initialize(const char dir[], const char key_set_dir[],
		_int64 key_set_byte_offset) {

		int comp_buff_size = min(960000, MAX_MAPRED_BYTES / GetKeyClientNum());
		m_bucket_set.AllocateMemory(GetKeyClientNum());
		for(int i=0; i<GetKeyClientNum(); i++) {
			this->m_bucket_set[i].OpenWriteFile(CUtility::ExtendString
				(dir, ".key_set", i, ".client", CSetNum::GetClientID()));
			this->m_bucket_set[i].InitializeCompression(comp_buff_size);
		}

		m_key_file.OpenReadFile(key_set_dir);
		m_key_file.SeekReadFileFromBeginning(key_set_byte_offset);

		m_hash_node_file.OpenWriteFile(CUtility::ExtendString
			(dir, ".hash_node_set", CSetNum::GetClientID()));
	}


public:

	CDistributeKeys() {
	}

	// This is the entry function that takes a key set and distribute
	// the keys appropriately, based upon their hash value.
	// @param work_dir - this is the working directory to store the hashed
	//                 - keys produced as output
	// @param key_set_dir - this is the directory of the key set
	// @param key_set_byte_offset - this stores the byte offset in the 
	//                            - key set file
	// @oaram tuple_bytes - this is the number of bytes to retreive
	// @param max_key_bytes - this is the maximum number of bytes that 
	//                      - compose a key
	// @param retrieve_key - this is a function used to retrieve a key
	void DistributeKeys(const char work_dir[], const char key_set_dir[],
		_int64 key_set_byte_offset, _int64 tuple_bytes,
		int max_key_bytes, void (*retrieve_key)
			(CHDFSFile &key_file, int &bytes, char buff[])) {

		Initialize(work_dir, key_set_dir, key_set_byte_offset);

		int bytes;
		CMemoryChunk<char> temp_buff(max_key_bytes);
		while(tuple_bytes > 0) {

			retrieve_key(m_key_file, bytes, temp_buff.Buffer());
			int hash = CHashFunction::SimpleHash
				(GetKeyClientNum(), temp_buff.Buffer(), bytes);

			if(bytes > max_key_bytes) {
				throw EIllegalArgumentException("Invalid Key Byte Num");
			}

			tuple_bytes -= bytes;
			m_hash_node_file.WriteCompObject((uChar)hash);
			CHDFSFile &bucket_file = this->m_bucket_set[hash];
			bucket_file.AddEscapedItem(bytes);
			bucket_file.WriteCompObject(temp_buff.Buffer(), bytes);
		}
	}

	// This is the entry function that takes a key set and distribute
	// the keys appropriately, based upon their hash value.
	// @param work_dir - this is the working directory to store the hashed
	//                 - keys produced as output
	// @param key_set_dir - this is the directory of the key set
	// @param key_set_byte_offset - this stores the byte offset in the 
	//                            - key set file
	// @oaram tuple_bytes - this is the number of bytes to retreive
	// @oaram key_num - this is the number of keys in the set
	// @param key_bytes - this is the number of bytes that compose a key
	void DistributeKeys(const char work_dir[], const char key_set_dir[],
		_int64 key_set_byte_offset, _int64 tuple_bytes, int key_bytes) {

		Initialize(work_dir, key_set_dir, key_set_byte_offset);
		CMemoryChunk<char> temp_buff(key_bytes);
		while(tuple_bytes > 0) {

			m_key_file.ReadCompObject(temp_buff.Buffer(), key_bytes);

			int hash = CHashFunction::SimpleHash
				(GetKeyClientNum(), temp_buff.Buffer(), key_bytes);

			tuple_bytes -= key_bytes;
			m_hash_node_file.WriteCompObject((uChar)hash);
			CHDFSFile &bucket_file = this->m_bucket_set[hash];
			bucket_file.AddEscapedItem(key_bytes);
			bucket_file.WriteCompObject(temp_buff.Buffer(), key_bytes);
		}
	}

};