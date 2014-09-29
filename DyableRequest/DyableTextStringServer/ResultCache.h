#include "./RetrieveTextString.h"

// This stores the top N closes matching terms for a given query text
// for future use. The caching mechanism using a LRU scheme for 
// evicting entries from the cache.
class CResultCache {

	// This defines the size of a word block
	static const int WORD_BLOCK_SIZE = 5;
	// This defines the breadh of the hash map
	static const int HASH_BREADH = 1000000;
	// This defines the maximum number of entries 
	// allowed in memory at any one time
	static const int MAX_CACHE_ENTRIES = 10;

	// This stores one of the word text string blocsk
	struct SWordBlock {
		// This stores thd id of the block
		int block_id;
		// This stores a ptr to the next word block
		SWordBlock *next_ptr;
	};

	// This stores an instance of one of the retrieved words
	struct SPossibleMatch {
		// This stores the word id
		S5Byte word_id;
		// This stores the match score
		uChar match_score;
		// This stores the length of the word
		u_short word_length;
		// This stores the occurrence of the word
		uLong occur;
		// This stores a ptr to the word block
		SWordBlock *word_block_ptr;
		// This stores a ptr to the next possible match
		SPossibleMatch *next_ptr;
	};

	// This stores one of the entries in the cache
	struct SHashEntry {
		// This stores a ptr to the next cached node
		SHashEntry *next_cache_ptr;
		// This stores a ptr to the previous cached node
		SHashEntry *prev_cache_ptr;
		// This stores a ptr to the next cache entry
		SHashEntry *next_hash_ptr;
		// This stores a ptr to the word block set
		SWordBlock *word_block_ptr;
		// This stores a ptr to the set of possible matches
		SPossibleMatch *match_ptr;
		// This stores the hash id of an entry
		int hash_id;
		// This stores the number of match possible matches
		int match_num;
	};

	// This stores the hash map used to retrieve existing entries
	CMemoryChunk<SHashEntry *> m_hash_map;
	// This stores the word block buffer
	CArrayList<char> m_word_buff;
	// This stores the set of hash entries
	CLinkedBuffer<SHashEntry> m_hash_node_buff;
	// This stores the set of possible matches
	CLinkedBuffer<SPossibleMatch> m_match_buff;
	// This stores the set of word blocks
	CLinkedBuffer<SWordBlock> m_word_block_buff;

	// This stores the head cache ptr
	SHashEntry *m_head_cache_ptr;
	// This stores the tail cache ptr
	SHashEntry *m_tail_cache_ptr;
	// This stores the current number of cache entries
	int m_cache_entry_num;

	// This stores the set of free hash entries
	SHashEntry *m_hash_free_ptr;
	// This stores the set of free matching entries
	SPossibleMatch *m_free_match_ptr;
	// This stores the set of free word blocks
	SWordBlock *m_free_word_ptr;

	// This returns a free hash node entry
	SHashEntry *FreeHashNodeEntry() {

		SHashEntry *ptr = m_hash_free_ptr;
		if(ptr != NULL) {
			m_hash_free_ptr = m_hash_free_ptr->next_cache_ptr;
			return ptr;
		}

		return m_hash_node_buff.ExtendSize(1);
	}

	// This returns a free possible match entry
	SPossibleMatch *FreeMatchEntry() {

		SPossibleMatch *ptr = m_free_match_ptr;
		if(ptr != NULL) {
			m_free_match_ptr = m_free_match_ptr->next_ptr;
			return ptr;
		}

		return m_match_buff.ExtendSize(1);
	}

	// This returns a free word block entry
	SWordBlock *FreeWordBlockEntry() {

		SWordBlock *ptr = m_free_word_ptr;
		if(ptr != NULL) {
			m_free_word_ptr = m_free_word_ptr->next_ptr;
			return ptr;
		}

		ptr = m_word_block_buff.ExtendSize(1);
		ptr->block_id = m_word_block_buff.Size() - 1;
		m_word_buff.ExtendSize(WORD_BLOCK_SIZE);
		return ptr;
	}

	// This removes an entry in the hash map
	void RemoveHashMapEntry(int hash_id) {

		SHashEntry *prev_ptr = NULL;
		SHashEntry *curr_ptr = m_hash_map[hash_id];
		
		while(curr_ptr != NULL) {

			if(m_tail_cache_ptr == curr_ptr) {

				if(prev_ptr == NULL) {
					m_hash_map[hash_id] = curr_ptr->next_hash_ptr;
				} else {
					prev_ptr->next_hash_ptr = curr_ptr->next_hash_ptr;
				}
				break;
			}

			prev_ptr = curr_ptr;
			curr_ptr = curr_ptr->next_hash_ptr;
		}
	}

	// This frees all of the attributes associated with a hash node entry
	void VacateHashNodeEntry() {
	
		RemoveHashMapEntry(m_tail_cache_ptr->hash_id);

		SWordBlock *curr_ptr1 = m_tail_cache_ptr->word_block_ptr;
		while(curr_ptr1 != NULL) {
			SWordBlock *prev_ptr = m_free_word_ptr;
			m_free_word_ptr = curr_ptr1;
			curr_ptr1 = curr_ptr1->next_ptr;
			m_free_word_ptr->next_ptr = prev_ptr;
		}

		SPossibleMatch *curr_ptr2 = m_tail_cache_ptr->match_ptr;
		while(curr_ptr1 != NULL) {
			SPossibleMatch *prev_ptr = m_free_match_ptr;
			m_free_match_ptr = curr_ptr2;
			curr_ptr2 = curr_ptr2->next_ptr;
			m_free_match_ptr->next_ptr = prev_ptr;
		}

		m_cache_entry_num--;
		if(m_tail_cache_ptr == m_head_cache_ptr) {	
			m_head_cache_ptr = NULL;
		}
		m_tail_cache_ptr = m_tail_cache_ptr->prev_cache_ptr;
	}

	// This adds a word to a word block buffer
	void AddWordTextString(SWordBlock * &word_block_ptr, const char str[], int length) {

		word_block_ptr = FreeWordBlockEntry();
		SWordBlock *curr_ptr = word_block_ptr;

		while(length > 0) {
			char *buff = m_word_buff.Buffer() + (curr_ptr->block_id * WORD_BLOCK_SIZE);
			memcpy(buff, str, min(length, WORD_BLOCK_SIZE));

			length -= WORD_BLOCK_SIZE;
			str += WORD_BLOCK_SIZE;

			if(length > 0) {
				curr_ptr->next_ptr = FreeWordBlockEntry();
				curr_ptr = curr_ptr->next_ptr;
			}
		}

		if(length < 0) {
			length += WORD_BLOCK_SIZE;
			str -= WORD_BLOCK_SIZE;
			char *buff = m_word_buff.Buffer() + (curr_ptr->block_id * WORD_BLOCK_SIZE) + length;
			*buff = '\0';
		}

		curr_ptr->next_ptr = NULL;
	}

	// This adds an entry to the hash map
	// @param ptr - this is a ptr to the hash map entry
	// @return the hash id
	int AddToHashMap(SHashEntry *ptr, const char str[], int length) {

		int hash = CHashFunction::SimpleHash(m_hash_map.OverflowSize(), str, length);

		SHashEntry *prev_ptr = m_hash_map[hash];
		m_hash_map[hash] = ptr;
		m_hash_map[hash]->next_hash_ptr = prev_ptr;

		prev_ptr = m_head_cache_ptr;
		m_head_cache_ptr = ptr;

		if(prev_ptr == NULL) {
			m_tail_cache_ptr = m_head_cache_ptr;
			m_head_cache_ptr->next_cache_ptr = NULL;
		} else {
			m_head_cache_ptr->next_cache_ptr = prev_ptr;
			prev_ptr->prev_cache_ptr = m_head_cache_ptr;
		}

		return hash;
	}

	// This promotes a given cache entry once it has been used
	void PromoteCacheEntry(SHashEntry *ptr) {

		if(ptr == m_head_cache_ptr) {
			// already at the head of the list
			return;
		}

		if(ptr == m_tail_cache_ptr) {
			// reassigns the tail
			m_tail_cache_ptr = ptr->prev_cache_ptr;
			m_tail_cache_ptr->next_cache_ptr = NULL;
		} else {
			// links up the nodes in the list
			(ptr->prev_cache_ptr)->next_cache_ptr = ptr->next_cache_ptr;
			(ptr->next_cache_ptr)->prev_cache_ptr = ptr->prev_cache_ptr;
		}

		SHashEntry *prev_ptr = m_head_cache_ptr;
		m_head_cache_ptr = ptr;
		m_head_cache_ptr->next_cache_ptr = prev_ptr;
		m_head_cache_ptr->prev_cache_ptr = NULL;
		prev_ptr->prev_cache_ptr = m_head_cache_ptr;
	}

	// This compares a text string against another text string
	bool FindTextString(SWordBlock *ptr, const char str[], int length) {

		while(ptr != NULL) {
			char *buff = m_word_buff.Buffer() + (ptr->block_id * WORD_BLOCK_SIZE);
			for(int i=0; i<min(length + 1, WORD_BLOCK_SIZE); i++) {

				if(buff[i] == '\0') {
					return i == length;
				}

				if(buff[i] != str[i]) {
					return false;
				}
			}

			str += WORD_BLOCK_SIZE;
			length -= WORD_BLOCK_SIZE;
			ptr = ptr->next_ptr;
		}	

		return length == 0;
	}

	// This stores the text string in a given buffer
	// @param ptr - this is a ptr to the start of the word block set
	void RetrieveTextString(SWordBlock *ptr, char str[], u_short &length) {

		length = 0;
		while(ptr != NULL) {
			char *buff = m_word_buff.Buffer() + (ptr->block_id * WORD_BLOCK_SIZE);
			for(int i=0; i<WORD_BLOCK_SIZE; i++) {
				if(buff[i] == '\0') {
					return;
				}

				str[length++] = buff[i];
			}

			ptr = ptr->next_ptr;
		}	
	}

	// This searches for a given hash entry
	SHashEntry *FindEntry(const char str[], int length) {

		int hash = CHashFunction::SimpleHash(m_hash_map.OverflowSize(), str, length);
		SHashEntry *curr_ptr = m_hash_map[hash];

		while(curr_ptr != NULL) {
			if(FindTextString(curr_ptr->word_block_ptr, str, length)) {
				return curr_ptr;
			}

			curr_ptr = curr_ptr->next_hash_ptr;
		}

		return NULL;
	}

public:

	CResultCache() {
		m_hash_free_ptr = NULL;
		m_free_match_ptr = NULL;
		m_free_word_ptr = NULL;

		m_head_cache_ptr = NULL;
		m_tail_cache_ptr = NULL;

		m_hash_map.AllocateMemory(HASH_BREADH, NULL);
		m_word_buff.Initialize();
		m_hash_node_buff.Initialize();
		m_match_buff.Initialize();
		m_word_block_buff.Initialize();
		m_cache_entry_num = 0;
	}

	// This searches for an entry in the result cache and
	// returns the stored result back to the client.
	bool FindEntry(COpenConnection &conn, const char str[], int length) {

		SHashEntry *ptr = FindEntry(str, length);

		if(ptr == NULL) {
			return false;
		}

		PromoteCacheEntry(ptr);

		conn.Send((char *)&ptr->match_num, sizeof(int));

		SPossibleMatch *curr_ptr = ptr->match_ptr;

		while(curr_ptr != NULL) {

			RetrieveTextString(curr_ptr->word_block_ptr, 
				CUtility::SecondTempBuffer(), curr_ptr->word_length);
		
			conn.Send((char *)&curr_ptr->word_id, sizeof(S5Byte));
			conn.Send((char *)&curr_ptr->occur, 4);
			conn.Send((char *)&curr_ptr->match_score, 1);

			conn.Send((char *)&curr_ptr->word_length, 2);
			conn.Send(CUtility::SecondTempBuffer(), curr_ptr->word_length);

			curr_ptr = curr_ptr->next_ptr;
		}
	}

	// This adds an entry to the cache
	// @return a ptr to the hash node entry from which to
	//         add possible entries
	void *AddEntry(const char str[], int length) {

		if(++m_cache_entry_num >= MAX_CACHE_ENTRIES) {
			VacateHashNodeEntry();
		}

		SHashEntry *ptr = FreeHashNodeEntry();
		ptr->match_ptr = NULL;
		ptr->word_block_ptr = NULL;
		ptr->match_num = 0;
		ptr->hash_id = AddToHashMap(ptr, str, length);
		AddWordTextString(ptr->word_block_ptr, str, length);
		return ptr;
	}

	// This adds a possible match for an entry
	// @param entry - this is the hash map entry]
	// @param occurr - this is the occurrence of the term
	// @param word_id - the global id of the term
	// @param match_score - this is the match score a particular match
	void AddPossibleMatch(void *entry, const char str[], int length,
		uLong occur, S5Byte &word_id, uChar match_score) {

		SHashEntry *ptr = (SHashEntry *)entry;
		ptr->match_num++;

		SPossibleMatch *match_ptr = FreeMatchEntry();
		SPossibleMatch *prev_ptr = ptr->match_ptr;
		ptr->match_ptr = match_ptr;
		match_ptr->next_ptr = prev_ptr;

		match_ptr->occur = occur;
		match_ptr->word_id = word_id;
		match_ptr->word_length = length;
		match_ptr->match_score = match_score;

		AddWordTextString(match_ptr->word_block_ptr, str, length);
	}

	// This is just a test framework
	void TestResultCache() {

		S5Byte id;

		CMemoryChunk<CString> string_set(10000);

		for(int i=0; i<10000; i++) {
			int length = (rand() % 100) + 5;
			for(int j=0; j<length; j++) {
				CUtility::SecondTempBuffer()[j] = (rand() % 26) + 'a';
			}

			string_set[i].AddTextSegment(CUtility::SecondTempBuffer(), length);

			COpenConnection conn;
			for(int j=max(0, i-MAX_CACHE_ENTRIES + 1); j<i; j++) {
				if(FindEntry(conn, string_set[j].Buffer(), string_set[j].Size()) == false) {
					cout<<"not found";getchar();
				} 
			}

			if(i - MAX_CACHE_ENTRIES > 0) {
				for(int j=0; j<10; j++) {
					int id = rand() % (i - MAX_CACHE_ENTRIES);
					if(FindEntry(conn, string_set[id].Buffer(), string_set[id].Size()) == true) {
						cout<<"not evicted";getchar();
					} 
				}
			}

			void *ptr = AddEntry(CUtility::SecondTempBuffer(), length);
			AddPossibleMatch(ptr, "gooog", strlen("gooog"), 4, id, 5);
		}
	}
};