#include "./PermuteWord.h"

// This stores the association map file
const char *ASSOC_MAP_FILE = "GlobalData/Lexon/assoc_map";

// This class retrieves the word id for a particular text string.
// It is also responsible for creating a list of closely matched
// words if the word being searched for cannot be found or has
// a low word occurrecnce. This is used to offer alternative word
// suggestions to the user. 

// To retreive a word the hash is calculated for the word as a key
// into the index. The index then points to the list of closely 
// matched terms to the term being searched for. The linear search
// will stop after some finite number of words have been searched.
// From this point the word id is returned or a set of closely matched
// words are returned instead.
class CRetrieveSimilarWordID : public CNodeStat {

	// This defines the minimum allowed occurrence of a word
	// before spell check will look for an alternative
	static const int MIN_WORD_OCCUR = 40000;

	// This stores an instance of one of the retrieved words
	struct SWord {
		// This stores the word id
		S5Byte word_id;
		// This stores the term match
		uChar term_match;
		// This stores the length of the word
		uChar word_length;
		// This stores the length of the target term
		uChar target_term_length;
		// This stores the occurrence of the word
		uLong occur;
		// This stores the byte offset of the term
		S5Byte byte_offset;
		// This stores the cache division
		u_short div;
	};

	// This stores the lookup indexes
	CMemoryChunk<CHitItemBlock> m_lookup_index;
	// This stores the set of sorted terms
	CMemoryChunk<CHitItemBlock> m_sort_word;
	// This stores the set of associated terms
	CMemoryChunk<CHitItemBlock> m_assoc_word;

	// This is used to store previous query results
	CResultCache m_res_cache;
	// This stores a ptr to the current cache entry
	void *m_cache_entry_ptr;

	// This is used to compare words
	CLimitedPQ<SWord> m_word_queue;
	// This is used to store suffixes of the query term
	CTrie m_term_suffix;
	// This is used to generate a set of hash values
	CPermuteWord m_permute_word;

	// This stores the maximum match score
	uChar m_max_match_score;
	// This is a predicate indicating whether spell check should
	// be used or not when retrieving search terms
	uChar m_is_spell_check;

	// This is used to compare words
	static int CompareWords(const SWord &arg1, const SWord &arg2) {

		if(arg1.term_match < arg2.term_match) {
			return -1;
		}

		if(arg1.term_match > arg2.term_match) {
			return 1;
		}

		if(arg1.occur < arg2.occur) {
			return -1;
		}

		if(arg1.occur > arg2.occur) {
			return 1;
		}

		return 0;
	}


	// This assigns the match score for a particular word
	uChar MatchScore(char str[], int length) {

		if(m_term_suffix.FindWord(str, length) >= 0) {
			return 100 + length;
		}

		int score = 0;
		for(int i=0; i<length-1; i++) {
			m_term_suffix.FindWord(str + i, 2);

			if(m_term_suffix.AskFoundWord()) {
				score += 2;
			} else {
				score--;
			}
		}
		
		return max(score, 0);
	}

	// This scans forward searching for words that match the query term
	// @param byte_offset - this is the byte offset to start searching from
	// @param word_length - this is the length of hte word being searched for
	// @param word_cache - this stores all of the sorted words
	// @param div - this is the cache division
	void SearchForward(S64BitBound &byte_offset, int word_length, 
		CHitItemBlock &word_cache, int div) {

		static CMemoryChunk<char> buff(10);
		static S5Byte assoc_byte_offset;
		static SWord word;
		int offset = 0; 

		while(byte_offset.start < byte_offset.end || ++offset < 100000) {

			if(byte_offset.start >= word_cache.CompBuffer().BytesStored()) {
				return;
			}

			word_cache.RetrieveByteSet(byte_offset.start , 10, buff.Buffer());
			memcpy((char *)&word.word_id, buff.Buffer(), 5);
			word.occur = *(uLong *)(buff.Buffer() + 5);
			word.word_length = *(uChar *)(buff.Buffer() + 9);
			byte_offset.start += 10;

			word.byte_offset = byte_offset.start;
			char *word_buff = CUtility::SecondTempBuffer();
			word_cache.RetrieveByteSet(byte_offset.start, word.word_length, word_buff);
			byte_offset.start += word.word_length + sizeof(S5Byte);

			word.term_match = MatchScore(word_buff, word.word_length);
			m_max_match_score = max(m_max_match_score, word.term_match);

			word.target_term_length = word_length;
			word.div = div;

			m_word_queue.AddItem(word);
			if(word.term_match == 100 + word_length) {
				return;
			}
		}
	}

	// This creates the dictionary term match for a particular word
	void CreateLetterMatch(const char str[], int word_length) {

		m_term_suffix.Reset();
		m_term_suffix.AddWord(str, word_length);
		m_max_match_score = 0;

		for(int i=0; i<word_length-1; i++) {
			m_term_suffix.AddWord(str + i, 2);
		}
	}

	// This searches for a particular word corresponding to a given hash value
	// @param hash - this is the hash of the word
	// @param word_length - this is the length of the word
	void SearchForTextString(uLong hash, int word_length) {

		S64BitBound byte_offset(0, 0);
		int div = hash % CNodeStat::GetHashDivNum();
		hash /= CNodeStat::GetHashDivNum();
	
		_int64 index_byte_start = 5 * (_int64)hash;
		CHitItemBlock &lookup_cache = m_lookup_index[div];
		CHitItemBlock &word_cache = m_sort_word[div];

		if(index_byte_start >= lookup_cache.CompBuffer().BytesStored()) {
			return;
		}

		lookup_cache.RetrieveByteSet(index_byte_start, 5, (char *)&byte_offset.start);
		index_byte_start += 5;
		
		if(index_byte_start < lookup_cache.CompBuffer().BytesStored()) {
			lookup_cache.RetrieveByteSet(index_byte_start, 5, (char *)&byte_offset.end);
		} else {
			byte_offset.end = lookup_cache.CompBuffer().BytesStored();
		}

		SearchForward(byte_offset, word_length, word_cache, div);
	}

	// This searches for a particular text string in the list
	// and stores the closes match for the text string
	// @return true for an exact match, false for any inexact match
	bool SearchForTextString(const char str[], int word_length) {

		static CArrayList<uLong> hash_set(5);
		hash_set.Resize(0);

		CreateLetterMatch(str, word_length);
		memcpy(CUtility::SecondTempBuffer(), str, word_length);
 		m_permute_word.PermuteWord(CUtility::SecondTempBuffer(), word_length, hash_set);
		m_max_match_score = 0;

		CStopWatch timer;
		timer.StartTimer();
		for(int i=0; i<hash_set.Size(); i++) {
			SearchForTextString(hash_set[i], word_length);

			timer.StopTimer();
			if(timer.GetElapsedTime() > 2.5) {
				break;
			}

			CArray<SWord> buff(m_word_queue.Size());
			m_word_queue.CopyNodesToBuffer(buff);
			m_word_queue.Reset();
			uChar is_match = 0;

			for(int j=0; j<buff.Size(); j++) {
				SWord &item = buff[j];

				m_word_queue.AddItem(item);		

				if(item.term_match >= 100 + word_length) {
					if(item.occur > MIN_WORD_OCCUR || item.word_length < 5 || m_is_spell_check == false) {
						is_match |= 0x01;
						continue;
					}
				}

				uChar match = FinalTermMatch(str, false, item);

				if(item.occur > MIN_WORD_OCCUR && match < 2) { 
					is_match |= 0x02;
				}
			}

			if((is_match & 0x01) == 0x01) {
				return true;
			}

			if((is_match & 0x02) == 0x02) {
				return false;
			}

			if(word_length <= 4) {
				break;
			}
		}

		return true;
	}

	// This assigns the final term match for a given query term using the edit distance
	// @param search_str - the target search term
	// @param is_match - true if an exact match is found, false otherwise
	uChar FinalTermMatch(const char *search_str, bool is_match, SWord &item) {

		uChar match = 0;
		char *word_buff = CUtility::SecondTempBuffer();
		CHitItemBlock &word_cache = m_sort_word[item.div];
		word_cache.RetrieveByteSet(item.byte_offset.Value(), item.word_length, word_buff);
		item.byte_offset += item.word_length;

		if(item.target_term_length > item.word_length) {
			match = CUtility::EditDistance(search_str, 
				item.target_term_length, word_buff, item.word_length);
		} else {
			match = CUtility::EditDistance(word_buff, item.word_length,
				search_str, item.target_term_length);
		}

		if(match == 0 && is_match == true) {
			item.term_match = 100;
		} else if(match < 4 && is_match == false && item.occur > MIN_WORD_OCCUR) {
			item.term_match = 100 - match;
		} else {
			item.term_match = 50 - match;
		}

		return match;
	}

	// This sends the set of associated terms for a given query term
	// @param item - this is the particular word item
	// @param byte_offset - this is the association byte offset
	void SendAssociatedTerms(COpenConnection &conn, SWord &item, S5Byte &assoc_byte_offset) {

		CHitItemBlock &cache = m_assoc_word[item.word_id.Value() % CNodeStat::GetHashDivNum()];
		_int64 byte_offset = assoc_byte_offset.Value();

		u_short num = 0;
		if(assoc_byte_offset.IsMaxValueSet() == true) {
			conn.Send((char *)&num, sizeof(u_short));
			return;
		}

		cache.RetrieveByteSet(byte_offset, sizeof(u_short), (char *)&num);
		byte_offset += sizeof(u_short);

		CMemoryChunk<char> buff(num * (sizeof(S5Byte) + sizeof(float)));
		cache.RetrieveByteSet(byte_offset, buff.OverflowSize(), buff.Buffer());

		conn.Send((char *)&num, sizeof(u_short));
		conn.Send(buff.Buffer(), buff.OverflowSize());
	}

	// This sends a matching term back to the query instance 
	// @param item - this is the particular word item
	// @param conn - the open connection to the client
	// @param is_match - true if an exact match is found, false otherwise
	// @param is_synm - true if the word is a synonym for the one supplied
	void SendPossibleMatch(SWord &item, bool is_match, bool is_synm,
		const char *search_str, COpenConnection &conn) {

		static S5Byte assoc_byte_offset;

		CHitItemBlock &word_cache = m_sort_word[item.div];
		char *word_buff = CUtility::SecondTempBuffer();

		FinalTermMatch(search_str, is_match, item);

		word_cache.RetrieveByteSet(item.byte_offset.Value(), sizeof(S5Byte), (char *)&assoc_byte_offset);
		item.byte_offset += sizeof(S5Byte);

		conn.Send((char *)&item.word_id, 5);
		conn.Send((char *)&item.occur, 4);
		conn.Send((char *)&item.term_match, 1);
		conn.Send((char *)&is_synm, 1);

		conn.Send((char *)&item.word_length, 1);
		conn.Send(word_buff, item.word_length);

		SendAssociatedTerms(conn, item, assoc_byte_offset);

		for(int i=0; i<item.word_length; i++) {
			cout<<word_buff[i];
		}
		cout<<" "<<(int)item.term_match<<" "<<item.occur<<" "<<item.word_id.Value()<<" "<<is_synm<<endl;
	}

	// This sends the discovered words back to the user
	// @param is_match - true if an exact match is found, false otherwise
	void ResolveDiscoveredTerms(const char *search_str, bool is_match) {

		CArray<SWord> word_buff(m_word_queue.Size());
		m_word_queue.CopyNodesToBuffer(word_buff);
		m_word_queue.Reset();

		for(int i=0; i<word_buff.Size(); i++) {
			SWord &item = word_buff[i];

			_int64 prev_byte_offset = item.byte_offset.Value();
			FinalTermMatch(search_str, is_match, item);
			item.byte_offset = prev_byte_offset;

			m_word_queue.AddItem(item);
		}
	}

	// This is the entry function that performs a search
	// for a particular word text string and returns the word id.
	// @param conn - the open connection to the client
	// @param phrase_length - this is the length of the total phrase sent
	// @param - this is the length of the first word in the phrase
	void FindWordID1(COpenConnection &conn, char str[], 
		int phrase_length, int word_length) {
		
		bool is_match = SearchForTextString(str, word_length);

		int num = 0;
		if(m_is_spell_check == false && m_max_match_score < 100) {
			return;
		}

		ResolveDiscoveredTerms(str, is_match);

		SWord item = m_word_queue.PopItem();

		static S5Byte assoc_byte_offset;

		CHitItemBlock &word_cache = m_sort_word[item.div];
		char *word_buff = CUtility::SecondTempBuffer();

		FinalTermMatch(str, is_match, item);

		word_cache.RetrieveByteSet(item.byte_offset.Value(), sizeof(S5Byte), (char *)&assoc_byte_offset);
		item.byte_offset += sizeof(S5Byte);

		for(int i=0; i<item.word_length; i++) {
			cout<<word_buff[i];
		}
		cout<<" "<<item.occur<<endl;
		
	}

	// This is the entry function that performs a search
	// for a particular word text string and returns the word id.
	// @param conn - the open connection to the client
	// @param phrase_length - this is the length of the total phrase sent
	// @param - this is the length of the first word in the phrase
	void FindWordID(COpenConnection &conn, char str[], 
		int phrase_length, int word_length) {
		
		bool is_match = SearchForTextString(str, word_length);

		int num = 0;
		if(m_is_spell_check == false && m_max_match_score < 100) {
			conn.Send((char *)&num, sizeof(int));
			return;
		}

		num = 2;
		ResolveDiscoveredTerms(str, is_match);
		conn.Send((char *)&num, sizeof(int));

		SWord item = m_word_queue.PopItem();
		SendPossibleMatch(item, is_match, false, str, conn);
		char *word_buff = CUtility::SecondTempBuffer();

		m_word_queue.Reset();
		m_max_match_score = 0;

		if(word_buff[item.word_length - 1] == 's') {
			CreateLetterMatch(word_buff, item.word_length - 1);
			uLong hash = m_permute_word.HashValue(word_buff, item.word_length - 1);
			SearchForTextString(hash, item.word_length - 1);
		} else {
			word_buff[item.word_length] = 's';
			CreateLetterMatch(word_buff, item.word_length + 1); 
			uLong hash = m_permute_word.HashValue(word_buff, item.word_length + 1);
			SearchForTextString(hash, item.word_length + 1);
		}

		item = m_word_queue.PopItem();
		bool is_synm = m_max_match_score >= 10;//item.occur > MIN_WORD_OCCUR;
		cout<<item.occur<<" ((("<<endl;
		item.occur = 0;

		SendPossibleMatch(item, true, is_synm, word_buff, conn);
		
	}

public:

	CRetrieveSimilarWordID() {

		CHitItemBlock::InitializeLRUQueue(100000);
	}

	// This initializes the set of hit blocks
	void Initialize() {

		if(m_assoc_word.OverflowSize() > 0) {
			return;
		}

		CUtility::Initialize();
		m_word_queue.Initialize(16, CompareWords);
		m_term_suffix.Initialize(4);

		m_assoc_word.AllocateMemory(CNodeStat::GetHashDivNum());
		m_lookup_index.AllocateMemory(CNodeStat::GetHashDivNum());
		m_sort_word.AllocateMemory(CNodeStat::GetHashDivNum());
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			m_lookup_index[i].Initialize(CUtility::ExtendString
				("GlobalData/Lexicon/word_lookup", i), 100);
			m_sort_word[i].Initialize(CUtility::ExtendString
				("GlobalData/Lexicon/sorted_words", i), 100);
			m_assoc_word[i].Initialize(CUtility::ExtendString
				("GlobalData/Lexicon/assoc_term", i), 100);
		}
	}

	// This resets the query terms
	void Reset() {
		m_word_queue.Reset();
		m_term_suffix.Reset();
		m_permute_word.Reset();
	}

	// Returns the word id belonging to some word text string
	// @param conn - the open connection to the client
	void WordIDRequest(COpenConnection &conn) {
	
		int phrase_length = 0;
		int word_length = -1;
		int word_num = 0;

		Reset();
		// next gets the word length
		conn.Receive((char *)&m_is_spell_check, 1);
		conn.Receive((char *)&phrase_length, 2);
		conn.Receive(CUtility::ThirdTempBuffer(), phrase_length);
		char *word_buff = CUtility::ThirdTempBuffer();

		for(int i=0; i<phrase_length; i++) {
			if(!CUtility::AskOkCharacter(word_buff[i])) {
				word_buff[i] = '^';
				if(word_length < 0) {
					word_length = i;
				}

				if(++word_num >= 2) {
					phrase_length = i;
					break;
				}
			}
		}

		if(word_length < 0) {
			word_length = phrase_length;
		} else if(CUtility::AskOkCharacter(word_buff[phrase_length-1])) {
			word_buff[phrase_length++] = '^';
		}

		for(int i=0; i<phrase_length; i++) {
			cout<<word_buff[i];
		}cout<<endl;

		FindWordID(conn, word_buff, phrase_length, word_length);
	}

	// This is just a test framework
	void TestRetrieveSimilarWordID(const char word[]) {

		Initialize();
		Reset();
		COpenConnection conn;
		int phrase_length = strlen(word);
		int word_length = -1;
		m_is_spell_check = false;
		strcpy(CUtility::ThirdTempBuffer(), word);
		char *word_buff = CUtility::ThirdTempBuffer();

		for(int i=0; i<phrase_length; i++) {
			if(!CUtility::AskOkCharacter(word_buff[i])) {
				word_buff[i] = '^';
				if(word_length < 0) {
					word_length = i;
				}
			}
		}

		if(word_length < 0) {
			word_length = phrase_length;
		} else if(CUtility::AskOkCharacter(word_buff[phrase_length-1])) {
			word_buff[phrase_length++] = '^';
		}

		for(int i=0; i<phrase_length; i++) {
			cout<<word_buff[i];
		}cout<<endl;
		
		FindWordID1(conn, word_buff, phrase_length, word_length);
	}
};
