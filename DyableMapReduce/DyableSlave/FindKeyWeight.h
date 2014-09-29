#include "./MergeSet.h"

// This is the general version of CVariableKeyOccurrence that takes an arbitrary
// set of variable length keys along with the weight of each key and sums them.
// The output of the class is a set of unique key along with its net weight.
template <class X>
class CFindKeyWeight : public CSetNum {

	// This stores the internal mapping between a key
	// and its respective value for a given bucket division
	CHashDictionary<int> m_key_map;
	// This stores the occurrence of each key
	CArrayList<X> m_occurr;
	// This stores the maximum number of bytes that make up a key
	int m_max_key_bytes;
	// This stores the directory for the node map
	char m_directory[500];

	// This loads each key contained in a bucket set into memory.
	// It also keeps track of the number of times a key occurrs.
	// @param key_file - this contains each key in a bucket division
	// @param buff - this is the place to store a key set 
	void LoadKeySet(CHDFSFile &key_file, char buff[]) {

		uLong bytes;
		X weight;

		while(key_file.GetEscapedItem(bytes) >= 0) {
			key_file.ReadCompObject(buff, bytes);
			key_file.ReadCompObject(weight);

			if(bytes > m_max_key_bytes) {
				cout<<"key mis";getchar();
			}

			int id = this->m_key_map.AddWord(buff, bytes);
			if(!this->m_key_map.AskFoundWord()) {
				this->m_occurr.PushBack(weight);
			} else {
				this->m_occurr[id] += weight;
			}
		}
	}

	// This writes the occurrence of each unique key out to a file
	// @param map_file - this stores the mapping between each unique key
	//                 - and the occurrence of that key
	// @param write_key_weight - this writes a unique key and the weight
	//						   - of each key
	void ApplyWeightToKeys(CSegFile &map_file, 
		void (*write_key_weight)(CHDFSFile &map_file, char *key, 
			int bytes, const X &weight)) {

		int length;
		for(int i=0; i<this->m_key_map.Size(); i++) {
			char *key = this->m_key_map.GetWord(i, length);

			map_file.AskBufferOverflow(length + sizeof(X));

			if(write_key_weight != NULL) {
				write_key_weight(map_file, key, length, this->m_occurr[i]);
			} else {
				map_file.WriteCompObject(key, length);
				map_file.WriteCompObject(this->m_occurr[i]);
			}
		}
	}

	// This takes all the bucket sets that belong to a particular client
	// and loads the keys into memory. It then counts the occurrence of
	// each key and then writes the key and the occurence out to a file.
	void PerformMapping() {

		CHDFSFile curr_key_file;
		m_occurr.Initialize((MAX_MAPRED_BYTES / m_max_key_bytes) + 1000);
		m_key_map.Initialize();

		CMemoryChunk<char> buff(m_max_key_bytes);
		for(int j=0; j<CSetNum::GetKeyClientNum(); j++) {
			// first load the explicit mapping for each key
			curr_key_file.OpenReadFile(CUtility::ExtendString
				(m_directory, ".key_set", CSetNum::GetClientID(), ".client", j));

			LoadKeySet(curr_key_file, buff.Buffer());
		}
	}

	// This function applies the weight to each key in the set 
	void ApplyWeightToDuplicateKeys() {

		CHDFSFile curr_key_file;
		CHDFSFile curr_map_file;

		CMemoryChunk<char> buff(m_max_key_bytes);
		for(int j=0; j<CSetNum::GetKeyClientNum(); j++) {
			// first load the explicit mapping for each key
			curr_key_file.OpenReadFile(CUtility::ExtendString
				(m_directory, ".key_set", CSetNum::GetClientID(), ".client", j));
			curr_map_file.OpenWriteFile(CUtility::ExtendString
				(m_directory, ".mapped_set", CSetNum::GetClientID(), ".client", j));

			uLong bytes;
			X weight;
			while(curr_key_file.GetEscapedItem(bytes) >= 0) {
				curr_key_file.ReadCompObject(buff.Buffer(), bytes);
				curr_key_file.ReadCompObject(weight);

				int id = m_key_map.FindWord(buff.Buffer(), bytes);
				curr_map_file.AddEscapedItem(bytes);
				curr_map_file.WriteCompObject(buff.Buffer(), bytes);
				curr_map_file.WriteCompObject(m_occurr[id]);
			}
		}
	}

public:

	CFindKeyWeight() {
	}

	// This takes the set of keys and reduces them to find the occurrence
	// of each unique key. It then writes the key and its occurrence out
	// to file to be processed later.
	// @param work_dir - this is the working directory of the MapReduce
	// @param output_dir - this is the output directory of the mapped keys
	// @param max_key_bytes - this stores the maximum number of bytes that 
	//                      - make up a key
	// @param write_key_weight - this is the function that writes the key weight
	void FindKeyWeight(const char work_dir[], const char output_dir[],
		int max_key_bytes, void (*write_key_weight)(CHDFSFile &map_file, char key[], 
			int bytes, const X &weight)) {

		strcpy(m_directory, work_dir);
		m_max_key_bytes = max_key_bytes;

		PerformMapping();

		CSegFile occur_file;
		occur_file.OpenWriteFile(CUtility::ExtendString
			(output_dir, GetClientID()));

		ApplyWeightToKeys(occur_file, write_key_weight);
	}

	// This finds the occurrence of keys but does not reduce the key set. It 
	// just assigns an occurrence to each duplicate key.
	// @param work_dir - this is the working directory of the MapReduce
	// @param output_dir - this is the output directory of the mapped keys
	// @param max_key_bytes - this stores the maximum number of bytes that 
	//                      - make up a key
	void FindDuplicateKeyWeight(const char work_dir[], 
		const char output_dir[], int max_key_bytes) {

		strcpy(m_directory, work_dir);
		m_max_key_bytes = max_key_bytes;

		PerformMapping();

		ApplyWeightToDuplicateKeys();
	}
};