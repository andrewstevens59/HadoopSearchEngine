#include "../../../MapReduce.h"

// This class is responsible for combining the set of hash values
// for each word in a given division and then sorting them by 
// the hash value. Once sorted the set of word ids for each hash
// value is created in sorted order. This is so the associations
// can be aligned to each sorted hash value.
class CSortWordHashSet {

	// This stores the set of word hash values
	CFileComp m_word_hash_file;
	// This stores the final word id file aligned to the sorted hash values
	CSegFile m_word_id_file;
	// This stores the set of hash values for each client
	CFileSet<CHDFSFile> m_word_hash_set;

	// This creates the sorted word id set
	void CreateWordIDSet() {

		uLong occur;
		uLong hash;
		S5Byte word_id;

		CExternalRadixSort sort(sizeof(uLong) * 2 + sizeof(S5Byte),
			m_word_hash_file.CompBuffer(), sizeof(uLong) * 2);

		m_word_id_file.OpenWriteFile(CUtility::ExtendString
			("LocalData/word_id", CNodeStat::GetClientID()));

		_int64 max_assoc_id = 0;
		m_word_hash_file.OpenReadFile();
		while(m_word_hash_file.ReadCompObject(occur)) {
			m_word_hash_file.ReadCompObject(hash);
			m_word_hash_file.ReadCompObject(word_id);
			m_word_id_file.WriteCompObject(word_id);
			max_assoc_id = max(max_assoc_id, word_id.Value());
		}

		CHDFSFile assoc_term_file;
		assoc_term_file.OpenWriteFile(CUtility::ExtendString
				("LocalData/assoc_num", CNodeStat::GetClientID()));
		assoc_term_file.WriteObject(max_assoc_id);
	}

public:

	CSortWordHashSet() {
	}

	// This is the entry function
	void SortWordHashSet(int client_id, int client_num) {

		CNodeStat::SetClientID(client_id);
		CNodeStat::SetClientNum(client_num);

		m_word_hash_set.SetFileName("LocalData/word_set_hash");
		m_word_hash_set.AllocateFileSet(CNodeStat::GetClientNum());

		m_word_hash_file.OpenWriteFile(CUtility::ExtendString
			("LocalData/sort_hash_set", CNodeStat::GetClientID()));

		uLong occur;
		uLong hash_value;
		S5Byte word_id;
		for(int j=0; j<CNodeStat::GetClientNum(); j++) {
			m_word_hash_set.OpenReadFile(CNodeStat::GetClientID(), j);

			CHDFSFile &file = m_word_hash_set.File(CNodeStat::GetClientID());
			while(file.ReadCompObject(occur)) {
				file.ReadCompObject(hash_value);
				file.ReadCompObject(word_id);

				m_word_hash_file.AskBufferOverflow(sizeof(S5Byte) + (sizeof(uLong) << 1));
				m_word_hash_file.WriteCompObject(occur);
				m_word_hash_file.WriteCompObject(hash_value);
				m_word_hash_file.WriteCompObject(word_id);
			}
		}

		CreateWordIDSet();
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);
	int client_num = atoi(argv[2]);

	CBeacon::InitializeBeacon(client_id);
	CMemoryElement<CSortWordHashSet> set;
	set->SortWordHashSet(client_id, client_num);
	set.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();
}