#include "./ResultCache.h"

// This is used to permute a supplied word to search for alternative
// spellings of that word. This is used as part of the spell checking process.
class CPermuteWord {

	// This defines the number of letters to use for indexing
	static const int LETTER_NUM = 6;

	// This stores one of the map values for a given letter combo
	struct SMapValue {
		// This stores the map value
		const char *map_ptr;
		// This stores the ptr to the next map value
		SMapValue *next_ptr;
	};

	// This stores the key character map
	CHashDictionary<int> m_key_map;
	// This stores the set of map values
	CArrayList<SMapValue *> m_map_set;
	// This stores each map
	CLinkedBuffer<SMapValue> m_map_buff;

	// This stores the letter buffer
	CArrayList<char> m_letter_buff;
	// This stores the word offset in the letter buffer
	CArrayList<int> m_word_offset;
	// This stores the length of the original word
	int m_word_len;

	// This stores the set of hash values
	CTrie m_hash_value_map;
	// This stores the set of vowels
	CMemoryChunk<bool> m_vowel;
	// This stores the set of word indexes that follow vowels
	CArray<int> m_vowel_branch;
	// This stores the set of letters that make up
	// the word in expansion order
	CArray<char> m_letter_set;

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

	// This finds the letter set in priority order
	void FindLetterSet(char buff[], int length) {

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

	// This is used to transpose adjacent letters in the word
	void PermuteLetters(char *word) {

		for(int i=0; i<m_word_len-1; i++) {
			m_letter_buff.CopyBufferToArrayList(word, m_word_len);
			char *perm = m_letter_buff.Buffer() + m_word_offset.LastElement();
			CSort<char>::Swap(perm[i], perm[i+1]);
			m_word_offset.PushBack(m_letter_buff.Size());
		}

		// removes a singular letter
		for(int i=0; i<m_word_len-1; i++) {
			m_letter_buff.CopyBufferToArrayList(word, i);
			m_letter_buff.CopyBufferToArrayList(word + i + 1, m_word_len - i - 1);
			m_word_offset.PushBack(m_letter_buff.Size());
		}
	}

	// This find the N permuted hash values, it uses 
	// the leave one out heuristic
	// @param hash_value - this is used to store the set of hash values
	void PermuteHashValue(CArrayList<uLong> &hash_value) {

		for(int i=0; i<m_word_offset.Size()-1; i++) {

			FindLetterSet(m_letter_buff.Buffer() + m_word_offset[i], m_word_offset[i+1] - m_word_offset[i]);

			int hash = CHashFunction::UniversalHash(m_letter_set.Buffer(), 
					min(m_letter_set.Size(), LETTER_NUM));

			m_hash_value_map.AddWord((char *)&hash, sizeof(int));
			if(m_hash_value_map.AskFoundWord() == false) {
				hash_value.PushBack(hash);
			}
		}
	}

	// This finds the matching subset string in a given word for a modified word
	// @return the matching id 
	int FindSubsetString(char *word, int offset, int &len) {

		for(int i=min(m_word_len, offset + 3); i>offset; i--) {

			int id = m_key_map.FindWord(word, i, offset);
			if(id >= 0) {
				len = i - offset;
				return id;
			}
		}

		return -1;
	}

	// This creates modifies the word by looking for common spelling alternatives
	void ModifyWord(char *word) {

		int len;

		for(int i=0; i<m_word_len; i++) {
			int match = FindSubsetString(word, i, len);
			if(match < 0) {
				continue;
			}

			SMapValue *curr_ptr = m_map_set[match];

			while(curr_ptr != NULL) {
				m_letter_buff.CopyBufferToArrayList(word, i);
				m_letter_buff.CopyBufferToArrayList(curr_ptr->map_ptr, strlen(curr_ptr->map_ptr));
				m_letter_buff.CopyBufferToArrayList(word + i + len, m_word_len - i - len);

				m_word_offset.PushBack(m_letter_buff.Size());
				curr_ptr = curr_ptr->next_ptr;
			}
		}
	}

	// This removes as single letter from the word
	void RemoveCharacters(char *word) {

		for(int i=0; i<m_word_len; i++) {
			m_letter_buff.CopyBufferToArrayList(word, i);
			m_letter_buff.CopyBufferToArrayList(word + i + 1, m_word_len - i - 1);
			m_word_offset.PushBack(m_letter_buff.Size());
		}
	}
	
	// This replaces the vowels with other vowels in the word
	void ReplaceVowels(char *word) {

		static char vowel[] = {'a', 'e', 'i', 'o', 'u'};

		for(int i=0; i<m_word_len; i++) {

			if(m_vowel[word[i]] == true) {

				for(int j=0; j<5; j++) {
					if(vowel[j] != word[i]) {
						m_letter_buff.CopyBufferToArrayList(word, i);
						m_letter_buff.PushBack(vowel[j]);
						m_letter_buff.CopyBufferToArrayList(word + i + 1, m_word_len - i - 1);
						m_word_offset.PushBack(m_letter_buff.Size());

					}
				}
			}
		}
	}

	// This inserts additional characters with a leave one out policy
	void InsertCharacters(char *word) {

		static char letter_set[] = {'e', 't', 'a', 'o', 'i', 'n', 's', 'h', 'r', 'd', 'l',
			'c', 'u', 'm', 'w', 'f', 'g', 'y', 'p', 'b', 'v', 'k', 'j', 'x', 'q', 'z'};

		for(int j=0; j<26; j++) {
			for(int i=0; i<=m_word_len; i++) {
				m_letter_buff.CopyBufferToArrayList(word, i);
				m_letter_buff.CopyBufferToArrayList(&(letter_set[j]), 1);
				m_letter_buff.CopyBufferToArrayList(word + i, m_word_len - i);
				m_word_offset.PushBack(m_letter_buff.Size());
			}
		}
	}

public:

	CPermuteWord() {
		m_letter_buff.Initialize(4);
		m_word_offset.Initialize(4);

		m_key_map.Initialize(1024);
		m_map_set.Initialize(4);
		m_map_buff.Initialize(30);
		m_hash_value_map.Initialize(4);

		m_vowel.AllocateMemory(256, false);
		m_vowel['a'] = true;
		m_vowel['e'] = true;
		m_vowel['i'] = true;
		m_vowel['o'] = true;
		m_vowel['u'] = true;
	}

	// This resets the class
	void Reset() {
		m_letter_buff.Resize(0);
		m_word_offset.Resize(0);
		m_key_map.Reset();
		m_map_set.Resize(0);
		m_map_buff.Restart();
		m_hash_value_map.Reset();
	}

	// This returns the hash value for a particular word
	uLong HashValue(char word[], int len) {

		FindLetterSet(word, len);

		return CHashFunction::UniversalHash(m_letter_set.Buffer(), 
				min(m_letter_set.Size(), LETTER_NUM));
	}

	// This permutes a given word and calculates a set of 
	// has values for each permutation of that word.
	// @param hash_value - this stores the set of hash values for each permutation
	void PermuteWord(char *buff, int len, CArrayList<uLong> &hash_value) {

		m_word_offset.PushBack(0);
		m_letter_buff.CopyBufferToArrayList(buff, len);
		m_word_offset.PushBack(m_letter_buff.Size());
		m_word_len = len;

		static const char *permute_char[] = {"ie", "y", "y", "ie", "e", "a", "a", "e", "a", "o", "o", "a",
			"us", "ous", "a", "er", "er", "a", "ie", "ee", "ee", "ie", "au", "o", "o", "au", "ey", "ee", "ee", "ey", 
			"i", "e", "e", "i", "i", "y", "a", "u", "u", "a", "gl", "gle", "ey", "ie", "ie", "ey",
			"er", "ur", "ur", "er", "ee", "i", "i", "ee", NULL};

		int offset = 0;
		while(permute_char[offset] != NULL) {
			int id = m_key_map.AddWord(permute_char[offset], strlen(permute_char[offset]));

			if(m_key_map.AskFoundWord() == false) {
				m_map_set.PushBack(NULL);
			}

			SMapValue *prev_ptr = m_map_set[id];
			m_map_set[id] = m_map_buff.ExtendSize(1);
			m_map_set[id]->map_ptr = permute_char[offset+1];
			m_map_set[id]->next_ptr = prev_ptr;
			offset += 2;
		}

		PermuteLetters(buff);
		RemoveCharacters(buff);
		ReplaceVowels(buff);

		ModifyWord(buff);

		// adds duplicate letters
		for(int i=0; i<m_word_len-1; i++) {
			if(buff[i] == buff[i+1]) {
				continue;
			}

			if(buff[i] != 's' && buff[i] != 'r' && buff[i] != 'l' && buff[i] != 'e' && buff[i] != 'o') {
				continue;
			}

			m_letter_buff.CopyBufferToArrayList(buff, i);
			m_letter_buff.PushBack(buff[i]);
			m_letter_buff.CopyBufferToArrayList(buff + i, m_word_len - i);

			m_word_offset.PushBack(m_letter_buff.Size());
		}

		InsertCharacters(buff);

		PermuteHashValue(hash_value);
	}
};