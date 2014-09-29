#include "../../../MapReduce.h"

// This stores the association map file
const char *ASSOC_MAP_FILE = "GlobalData/Lexon/assoc_map";

// This creates the set of hash maps for each word in the dictionary.
// Each hash map is used to sort a given word by its hash value.
class CCreateWordHashSet {

	// This defines the number of letters to use for indexing
	static const int LETTER_NUM = 6;

	// This stores the set of vowels
	CMemoryChunk<bool> m_vowel;
	// This stores the set of word indexes that follow vowels
	CArray<int> m_vowel_branch;
	// This stores the set of letters that make up
	// the word in expansion order
	CArray<char> m_letter_set;

	// This stores the set of associations and the word id
	CHDFSFile m_assoc_map_file;
	// This stores the set of hash values for each client
	CFileSet<CHDFSFile> m_word_hash_set;

	// This adds to the hash for the current letter index
	// @parma buff - this stores the word
	// @param length - this is the word length
	// @param i - this is the letter id
	void UpdateHash(char buff[], int length, int i) { 

		if(buff[i] == '*') {
			return;
		}

		m_letter_set.PushBack(buff[i]);

		buff[i] = '*';

		if(i > 0) {
			m_vowel_branch.PushBack(i - 1);
		}

		if(i < length - 1) {
			m_vowel_branch.PushBack(i + 1);
		}
	}

	// This find the N permuted hash values, it uses 
	// the leave one out heuristic
	// @param hash_value - this is used to store the set of hash values
	int PermuteHashValue(char buff[], int length, CMemoryChunk<uLong> &hash_value) {

		FindWordHash(buff, length);

		int hash_set_num = min(hash_value.OverflowSize(), m_letter_set.Size());
		for(int j=0; j<hash_set_num; j++) {

			hash_value[j] = CHashFunction::UniversalHash(m_letter_set.Buffer(), 
				min(m_letter_set.Size(), LETTER_NUM));
		}

		return hash_set_num;
	}

	// This returns the word hash for a particular word
	void FindWordHash(char buff[], int length) {

		m_vowel_branch.Initialize(20);
		m_letter_set.Initialize(20);

		for(int i=0; i<length; i++) {
			if(m_vowel[buff[i]] == true) {
				UpdateHash(buff, length, i);
			}

			if(m_letter_set.Size() >= LETTER_NUM) {
				return;
			}
		}

		for(int i=0; i<min(m_vowel_branch.Size(), LETTER_NUM); i++) {
			int index = m_vowel_branch[i];
			if(buff[index] == '*') {
				continue;
			}

			if(m_letter_set.Size() >= LETTER_NUM) {
				return;
			}

			m_letter_set.PushBack(buff[index]);
			buff[index] = '*';
		}

		for(int i=0; i<min(length, LETTER_NUM); i++) {
			m_letter_set.PushBack(buff[i]);
		}
	}

public:

	CCreateWordHashSet() {

		CHDFSFile::Initialize();
		m_vowel.AllocateMemory(256, false);

		m_vowel['a'] = true;
		m_vowel['e'] = true;
		m_vowel['i'] = true;
		m_vowel['o'] = true;
		m_vowel['u'] = true;
	}

	// This is the entry function
	void CreateWordHashSet(int client_id, int client_num) {

		CNodeStat::SetClientID(client_id);
		CNodeStat::SetClientNum(client_num);

		m_word_hash_set.OpenWriteFileSet("LocalData/word_set_hash", 
			CNodeStat::GetClientNum(), CNodeStat::GetClientID());

		m_assoc_map_file.OpenReadFile(CUtility::ExtendString
			(ASSOC_MAP_FILE, CNodeStat::GetClientID()));

		uLong occur;
		S5Byte word_id;
		uChar word_length;
		static CMemoryChunk<uLong> hash_value(4);
		while(m_assoc_map_file.ReadCompObject(word_id)) {
			m_assoc_map_file.ReadCompObject(occur);
			m_assoc_map_file.ReadCompObject(word_length);
			m_assoc_map_file.ReadCompObject(CUtility::SecondTempBuffer(), word_length);

			int hash_set_num = PermuteHashValue(CUtility::SecondTempBuffer(),
				word_length, hash_value);

			for(int i=0; i<hash_set_num; i++) {
				uLong hash = hash_value[i];
				int div = hash % CNodeStat::GetClientNum();

				hash /= CNodeStat::GetClientNum();

				CHDFSFile &file = m_word_hash_set.File(div);
				file.WriteCompObject(-occur);
				file.WriteCompObject(hash);
				file.WriteCompObject(word_id);
			}
		}
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);
	int client_num = atoi(argv[2]);

	CBeacon::InitializeBeacon(client_id);
	CMemoryElement<CCreateWordHashSet> set;
	set->CreateWordHashSet(client_id, client_num);
	set.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();
}