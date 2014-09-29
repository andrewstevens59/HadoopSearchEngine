#include "../../../MapReduce.h"

// This is used to create the reverse lookup that maps word hash
// values to the corresponding word ids. This is done by creating
// an index over each hash value in the set.
class CCreateWordHashLookup {

	// This stores the word lookup index 
	CCompression m_lookup_index;
	// This stores the merged sorted hash values 
	CFileComp m_sort_hash_file;

	// This stores the set of mapped associations
	CSegFile m_assoc_map_file;
	// This stores the set of word hash values that have been sorted 
	CFileComp m_word_hash_file;
	// This stores the set of mapped words for a given hash value
	CSegFile m_word_map_file;

	// This fills in the word id gap used in the lookup index
	inline void FillInWordIdGap(uLong gap) {

		_int64 bytes = m_sort_hash_file.CompBuffer().BytesStored();

		for(uLong i=0; i<gap; i++) {
			m_lookup_index.AddToBuffer((char *)&bytes, 5);
		}
	}

	// This stores the set of associations 
	// @param word_id - this is the current word id being processed
	void StoreAssociationSet(S5Byte &word_id) {

		static S5Byte id;
		static uChar set_num;
		static u_short map_bytes;
		static CMemoryChunk<char> temp_buff(1024);
		static uChar assoc_length;

		m_assoc_map_file.ReadCompObject(id);
		m_assoc_map_file.ReadCompObject(map_bytes);

		m_sort_hash_file.WriteCompObject(map_bytes);

		if(id != word_id) {
			cout<<"word id mis";getchar();
		}

		if(map_bytes == 0) {
			return;
		}

		m_assoc_map_file.ReadCompObject(temp_buff.Buffer(), map_bytes);
		m_sort_hash_file.WriteCompObject(temp_buff.Buffer(), map_bytes);
	}

	// This parses the sorted words and creates the lookup index
	// as well as merging the word text strings with the hash value
	void ParseSortedHashValues() {

		S5Byte word_id1;
		S5Byte word_id2;
		uLong occur;
		uChar word_length;
		float assoc_weight;
		uLong hash;

		int count = 0;
		_int64 prev_word_hash = -1;
		CMemoryChunk<char> word_buff(1024);

		while(m_word_map_file.ReadCompObject(word_id1)) {
			m_word_map_file.ReadCompObject(occur);
			m_word_map_file.ReadCompObject(word_length);
			m_word_map_file.ReadCompObject(word_buff.Buffer(), word_length);

			m_word_hash_file.ReadCompObject(occur);
			m_word_hash_file.ReadCompObject(hash);
			m_word_hash_file.ReadCompObject(word_id2);

			if(word_id1 != word_id2) {
				cout<<"Word ID Mismatch";getchar();
			}

			if(++count >= 10000) {
				cout<<(m_word_map_file.PercentageFileRead() * 100)<<"% Done"<<endl;
				count = 0;
			}

			if(hash != prev_word_hash) {
				FillInWordIdGap(hash - prev_word_hash);
				prev_word_hash = hash;
			}

			m_sort_hash_file.WriteCompObject(word_id1);
			m_sort_hash_file.WriteCompObject(-occur);
			m_sort_hash_file.WriteCompObject(word_length);
			m_sort_hash_file.WriteCompObject(word_buff.Buffer(), word_length);
			StoreAssociationSet(word_id2);
		}

		FillInWordIdGap(1);
	}

public:

	CCreateWordHashLookup() {
	}

	// This is the entry function
	void CreateWordHashLookup(int client_id) {

		CNodeStat::SetClientID(client_id);
		m_lookup_index.Initialize(CUtility::ExtendString
			("GlobalData/Retrieve/word_lookup",
			CNodeStat::GetClientID()), 5 * 1000);

		m_sort_hash_file.OpenWriteFile(CUtility::ExtendString
			("GlobalData/Retrieve/sorted_words",
			CNodeStat::GetClientID()), 10000);

		m_assoc_map_file.OpenReadFile(CUtility::ExtendString
			("LocalData/assoc_set_mapped", CNodeStat::GetClientID()));

		m_word_hash_file.OpenReadFile(CUtility::ExtendString
			("LocalData/sort_hash_set", CNodeStat::GetClientID()));

		m_word_map_file.OpenReadFile(CUtility::ExtendString
			("LocalData/word_map", CNodeStat::GetClientID()));

		ParseSortedHashValues();	
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);

	CBeacon::InitializeBeacon(client_id);
	CMemoryElement<CCreateWordHashLookup> set;
	set->CreateWordHashLookup(client_id);
	set.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();
}