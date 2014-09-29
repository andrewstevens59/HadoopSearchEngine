#include "../../../MapReduce.h"

// This is responsible for creating the lookup used to map word ids
// to their equivalent text string representation. To accomplish this
// text string maps must first be mapped to their sequential word id
// counterpart. Finally the lookup index is created to index into 
// every word id that has been created.
class CCreateWordIDLookup {

	// This stores the word lookup index 
	CCompression m_lookup_index;
	// This stores the merged sorted hash values 
	CFileComp m_sort_hash_file;

	// This parses the sorted words and creates the lookup index
	// as well as merging the word text strings with the hash value
	// @param word_map_output - this stores the set of mapped associations
	void ParseSortedWordIDs(CSegFile &word_map_output) {

		uLong occur;
		S5Byte word_id1;
		uChar word_length;
		uChar map_bytes;

		int count = 0;
		_int64 curr_word_id = 0;
		CMemoryChunk<char> word_buff(1024);

		while(word_map_output.ReadCompObject(word_id1)) {

			word_id1 = word_id1.Value() / CNodeStat::GetClientNum();

			if(word_id1 != curr_word_id) {
				cout<<"Word ID Mismatch";getchar();
			}

			curr_word_id++;
			word_map_output.ReadCompObject(map_bytes);

			_int64 bytes = m_sort_hash_file.CompBuffer().BytesStored();
			m_lookup_index.AddToBuffer((char *)&bytes, 5);

			if(map_bytes == 0) {
				continue;
			}

			word_map_output.ReadCompObject(occur);
			word_map_output.ReadCompObject(word_length);
			word_map_output.ReadCompObject(word_buff.Buffer(), word_length);

			if(++count >= 10000) {
				cout<<(word_map_output.PercentageFileRead() * 100)<<"% Done"<<endl;
				count = 0;
			}

			m_sort_hash_file.WriteCompObject(word_length);
			m_sort_hash_file.WriteCompObject(word_buff.Buffer(), word_length);
		}
	}

public:

	CCreateWordIDLookup() {
	}

	// This is the entry function 
	void CreateWordIDLookup(int client_id, int client_num) {

		CNodeStat::SetClientID(client_id);
		CNodeStat::SetClientNum(client_num);

		m_lookup_index.Initialize(CUtility::ExtendString
			("GlobalData/Retrieve/reverse_word_lookup",
			CNodeStat::GetClientID()), 5 * 1000);

		m_sort_hash_file.OpenWriteFile(CUtility::ExtendString
			("GlobalData/Retrieve/reverse_sorted_words",
			CNodeStat::GetClientID()), 10000);

		CSegFile word_map_output;
		word_map_output.OpenReadFile(CUtility::ExtendString
			("LocalData/word_map", CNodeStat::GetClientID()));

		ParseSortedWordIDs(word_map_output);
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);
	int client_num = atoi(argv[2]);

	CBeacon::InitializeBeacon(client_id);
	CMemoryElement<CCreateWordIDLookup> set;
	set->CreateWordIDLookup(client_id, client_num);
	set.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();
}