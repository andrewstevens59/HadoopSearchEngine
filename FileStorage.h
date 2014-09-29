
#include "./NetworkThread.h"

// defines the maximum size of the lexon
static const int MAX_LEXON_SIZE = 200000;
// 4-byte maximum signed integer
#define MAX_SIGNED 0x7FFFFFFF

// stores the comp block size in bytes of each hit list
// used in CompileLookup for determining final buffer size 
// for each of the four different hit lists
static const int HIT_LIST_COMP_SIZE = 1 << 13;
// Number of hits per lookup division - this
// corresponds to a single comp block so indexes
// in a given comp block can be looked up during retrieval
static const int LOOKUP_DIV_SIZE = 1000;
// This defines the maximum number of bytes before a new 
// spatial boundary is recorded
static const int MAX_SPAT_NUM = 0xFF;

// defines the index of title hits
static const int TITLE_HIT_INDEX = 0;
// defines the index of excerpt hits
static const int EXCERPT_HIT_INDEX = 1;
// defines the index of image hits
static const int IMAGE_HIT_INDEX = 2;
// This defines the total number of associations classes
static const int ASSOC_SET_NUM = 1;

// define an image hit type attribute
#define IMAGE_TYPE_HIT 0x01
// defines a word with  a suffix  
#define SUFFIX_TYPE_HIT 0x02
// defines link anchor type attribute
#define LINK_TYPE_HIT 0x04
// defines title type attribute
#define TITLE_TYPE_HIT 0x08
// defines title type attribute
#define EXCERPT_TYPE_HIT 0x10
// defines the first hit in an attribute
#define FIRST_TYPE_HIT 0x20
// defines the exclude hit type
#define EXCLUDE_TYPE_HIT 0x40
// defines the stopword hit type
#define STOP_TYPE_HIT 0x80

// This stores the root byte offset for a ab_node
struct SABRootByteOffset {
	// This stores the tree byte offset
	_int64 byte_offset;
	// This stores the global node offset
	_int64 node_offset;
};

// This defines one of the wave pass 
// distributions stored for a node
struct SWaveDist {
	// This is the node that belongs to the distribution
	S5Byte node;
	// This stores the majority class
	u_short cluster;
	// This stores the class ownership of the majority class
	float majority_weight;

	// This reads a wave distribution from file
	template <class X>
	inline bool ReadWaveDist(X &from_file) {
		return ReadWaveDist(from_file, *this);
	}

	// This writes a wave distribution to file
	template <class X>
	inline void WriteWaveDist(X &to_file) {
		WriteWaveDist(to_file, *this);
	}

	// This reads a wave distribution from file
	template <class X>
	static bool ReadWaveDist(X &from_file, SWaveDist &dist) {

		if(!from_file.ReadCompObject(dist.node)) {
			return false;
		}

		from_file.ReadCompObject(dist.cluster);
		from_file.ReadCompObject(dist.majority_weight);

		return true;
	}

	// This writes a wave distribution to file
	template <class X>
	static void WriteWaveDist(X &to_file, SWaveDist &dist) {

		to_file.AskBufferOverflow(sizeof(S5Byte) + sizeof(u_short) + sizeof(float));
		to_file.WriteCompObject(dist.node);
		to_file.WriteCompObject(dist.cluster);
		to_file.WriteCompObject(dist.majority_weight);
	}
};

// This defines the a mapping between a key value pair
// for a word id and an occurrence
struct SWordIDOccurrMap {
	// This stores the word id
	uLong word_id;
	// This stores the word occurrence
	uLong word_occurr;

	SWordIDOccurrMap() {
		word_occurr = INFINITE;
	}

	// This reads a word occurrence map from file
	bool ReadWordOccurrMap(CSegFile &file) {

		if(!file.ReadCompObject(word_id)) {
			return false;
		}

		file.ReadCompObject(word_occurr);

		return true;
	}

	// This writes a word occurrence map to file
	void WriteWordOccurrMap(CSegFile &file) {
		file.AskBufferOverflow(sizeof(uLong) << 1);
		file.WriteCompObject(word_id);
		file.WriteCompObject(word_occurr);
	}

	// This reads a word occurrence map
	static bool ReadOccurrMap(CSegFile &file, SWordIDOccurrMap &map, uLong &key) {
		if(!map.ReadWordOccurrMap(file)) {
			return false;
		}

		key = map.word_id;

		return true;
	}

	// This transfers a map from one file to another file
	static bool TransferMap(CSegFile &from_file, CSegFile &to_file) {

		static SWordIDOccurrMap map;
		if(!from_file.ReadCompObject(map.word_occurr)) {
			return false;
		}

		to_file.WriteCompObject(map.word_occurr);

		return true;
	}
};

// Stores the global offset for each type of log entry
struct SGlobalIndexOffset {
	// Stores the size of spidered link size dict
	int base_url_size;
	// Stores the non spiderd link offset
	_int64 link_offset;
	// Stores the word offset
	uLong word_offset;

	void Reset() {
		link_offset = 0;
		word_offset = 0;
	}

	// Assigns the current dictionary offset by incrementing
	// the previous dictionary offset by the curr dictionary sizes
	// @param prev_entry - the previous global offset division
	// @param link_url_size - the size of the url dictionary
	// @param word_size - the size of the word dictionary
	void Increment(SGlobalIndexOffset &prev_entry,
		int link_url_size, int word_size) {

		this->link_offset = prev_entry.link_offset + link_url_size;
		this->word_offset = prev_entry.word_offset + word_size;
	}
};

// This stores a query result for one of the parallel clients
struct SQueryRes {
	// This stores the document id
	S5Byte doc_id;
	// This stores the hiearchy node id
	S5Byte node_id;
	// This stores the number of focus term matches
	uChar focus_term_match;
	// This stores the document hit score
	float hit_score;
	// This stores the document traversal probability
	float trav_prob;
	// This stores the document expected reward
	float exp_rew;
	// This stores the excerpt keyword score
	float keyword_score;
};

// stores information relating to a word
// in a webpage
struct SWordHit {
	// stores the log file word division
	u_short word_div; 
	// stores the encoding for the word
	// such as anchor text image text 
	uChar term_type; 
	// stores the word id for the given term
	int word_id; 

	// returns true if the word has already
	// been assigned a word id through the lexon
	inline bool AskStopWord() {
		return word_id >= 0; 
	}

	// returns true if an image hit
	inline bool AskImageHit() {
		return (term_type & IMAGE_TYPE_HIT) == IMAGE_TYPE_HIT; 
	}

	// returns true if the word contains an illegal suffix
	inline bool AskSuffix() {
		return (term_type & SUFFIX_TYPE_HIT) == SUFFIX_TYPE_HIT; 
	}

	// returns true if a link type hit
	inline bool AskLinkHit() {
		return (term_type & LINK_TYPE_HIT) == LINK_TYPE_HIT; 
	}

	// returns true if a the word is a title
	inline bool AskTitleHit() {
		return (term_type & TITLE_TYPE_HIT) == TITLE_TYPE_HIT; 
	}

	// returns true if a the word is a excerpt
	inline bool AskExcerptHit() {
		return (term_type & EXCERPT_TYPE_HIT) == EXCERPT_TYPE_HIT; 
	}

	// returns true if the first of a hit type
	inline bool AskFirstHitType() {
		return (term_type & FIRST_TYPE_HIT) == FIRST_TYPE_HIT; 
	}

	// returns true if an exclude hit type
	inline bool AskExcludeHitType() {
		return (term_type & EXCLUDE_TYPE_HIT) == EXCLUDE_TYPE_HIT; 
	}

	// returns true if an stop hit type
	inline bool AskStopHitType() {
		return (term_type & STOP_TYPE_HIT) == STOP_TYPE_HIT; 
	}

	// sets a hit to the first of its type
	inline void SetFirstHitType() {
		term_type |= FIRST_TYPE_HIT; 
	}

	// sets a hit to an exclude word
	inline void SetExcludeHitType() {
		term_type |= EXCLUDE_TYPE_HIT; 
	}

	// sets a hit to an stop word
	inline void SetStopHitType() {
		term_type |= STOP_TYPE_HIT; 
	}
}; 

// This defines a hit item which is stored for every keyword
// in a given excerpt.
struct SKeywordHitItem {

	// this stores the word id offset
	S5Byte keyword_id;
	// this stores the doc id for the hit
	S5Byte doc_id;
	// This stores the keyword score
	float keyword_score;
	// This stores the pulse rank score
	float pulse_score;
	// This stores the checksum of the excerpt
	uLong check_sum;

	// This writes a keyword hit to file
	template <class X> void WriteKeywordHit(X &file) {

		file.AskBufferOverflow((sizeof(S5Byte) << 1) + (sizeof(float) << 1) + sizeof(uLong));
		file.WriteCompObject(keyword_id);
		file.WriteCompObject(doc_id);
		file.WriteCompObject(keyword_score);
		file.WriteCompObject(pulse_score);
		file.WriteCompObject(check_sum);
	}

	// This reads a keyword hit to file
	template <class X> bool ReadKeywordHit(X &file) {
		if(!file.ReadCompObject(keyword_id)) {
			return false;
		}

		file.ReadCompObject(doc_id);
		file.ReadCompObject(keyword_score);
		file.ReadCompObject(pulse_score);
		file.ReadCompObject(check_sum);
	}
};

// This defines a hit item which is stored for every word 
// in every document. 
struct SHitItem {
	// this stores the word id offset
	uLong word_id;
	// this stores the word encoding of the hit
	uLong enc;
	// this stores the doc id for the hit
	S5Byte doc_id;
	// this stores the image id if the hit is an image hit
	S5Byte image_id;
	// This stores the keyword score
	float keyword_score;
	// This stores the pulse rank score
	float pulse_score;

	SHitItem() {
		word_id = 0;
		enc = 0;
	}

	// This returns the hit type index
	inline int HitTypeIndex() {
		if(IsTitleHit()) {
			return 0;
		}

		if(IsExcerptHit()) {
			return 1;
		}

		return 2;
	}

	// This returns a predicate specifying an image hit
	inline bool IsImageHit() {
		return (enc & 0x01) == 0x01;
	}

	// This returns a predicate specifying an excerpt hit
	inline bool IsExcerptHit() {
		return (enc & 0x02) == 0x02;
	}

	// This returns a predicate specifying an title hit
	inline bool IsTitleHit() {
		return (enc & 0x04) == 0x04;
	}

	// This writes a hit to file
	template <class X> void WriteHitDocOrder(X &file) {
		file.WriteCompObject(doc_id);
		file.WriteCompObject((char *)&word_id, 4);
		file.WriteCompObject((char *)&enc, 2);

		if((enc & 0x01) == 0x01) {
			file.WriteCompObject(image_id);
		}
	}

	// This writes a hit to file
	// @param encoding - this is a new encoding
	// @parm document_id - this is a new doc id
	template <class X> void WriteHitDocOrder(X &file, 
		uLong encoding, _int64 &doc_id) {

		file.WriteCompObject((char *)&doc_id, 5);
		file.WriteCompObject((char *)&word_id, 4);
		file.WriteCompObject((char *)&encoding, 2);

		if((encoding & 0x01) == 0x01) {
			file.WriteCompObject(image_id);
		}
	}

	// This writes a hit to file
	template <class X> void WriteHitWordOrder(X &file) {
		file.WriteCompObject((char *)&enc, 2);

		if((enc & 0x01) == 0x01) {
			file.WriteCompObject(image_id);
		}

		file.WriteCompObject(doc_id);
		file.WriteCompObject((char *)&word_id, 4);
	}

	// This reads a hit from file
	template <class X> bool ReadHitDocOrder(X &file) {
		if(!file.ReadCompObject(doc_id)) {
			return false;
		}

		file.ReadCompObject((char *)&word_id, 4);
		file.ReadCompObject((char *)&enc, 2);

		if((enc & 0x01) == 0x01) {
			file.ReadCompObject(image_id);
		}
		
		return true;
	}

	// This reads a hit from file
	template <class X> bool ReadHitWordOrder(X &file) {

		if(!file.ReadCompObject((char *)&enc, 2)) {
			return false;
		}

		if((enc & 0x01) == 0x01) {
			file.ReadCompObject(image_id);
		}

		file.ReadCompObject(doc_id);
		file.ReadCompObject((char *)&word_id, 4);
		
		return true;
	}

	// This writes the hit item excluding the word id. This is 
	// done after sorting and word id's are no longer needed.
	template <class X> void WriteHitExcWordID(X &file) {

		file.WriteCompObject(doc_id);
		file.WriteCompObject((char *)&enc, 1);
	}

	// This reads the hit item excluding the word id. This is
	// done after sorting and word id's are no longer needed.
	template <class X> bool ReadHitExcWordID(X &file) {

		if(!file.ReadCompObject(doc_id)) {
			return false;
		}

		file.ReadCompObject((char *)&enc, 1);
		
		return true;
	}

	// @return the number of bytes that make up the hit item
	int ReadHitExcWordID(char buff[]) {

		int byte_num = 6;
		memcpy((char *)&doc_id, buff, 5);
		memcpy((char *)&enc, buff + 5, 1);
		
		return byte_num;
	}
};

// This stores the header of a particular title
struct STitleHeader {
	// This stores the doc id of the title
	S5Byte doc_id;
	// This stores the number of tokens in the title
	uChar token_num;

	STitleHeader() {
		token_num = 0;
	}

	// This writes a title header to file
	template <class X> void WriteTitleHeader(X &file) {
		file.WriteCompObject(doc_id);
		file.WriteCompObject(token_num);
	}

	// This reads a title header to file
	template <class X> bool ReadTitleHeader(X &file) {
		if(!file.ReadCompObject(doc_id)) {
			return false;
		}

		file.ReadCompObject(token_num);
		return true;
	}
};

// Stores the dictionary size and base
// url dictionary size for each log division
struct SBaseDictionarySize {
	// stores the size of the dictionary
	// before the addition of all the non
	// document base urls
	int base_url_size; 
	// stores the size of the url dictionary
	// after the addition of all links
	int dict_size; 
}; 

// Stores the size of all the different
// dictionaries for each log file
struct SDictionarySize {
	// stores the size of the url dictionary
	SBaseDictionarySize url_dict_size; 
	// stores the size of the word dictionary
	int word_dict_size; 
}; 


// Used to store a reference to a word
// in the priority queue
struct SWord {
	// the word index
	int index;
	// the occurrence of the word
	uLong occurrence;
};

// This class is used primarily for stemming purposes. It is
// an in memory global dictionary that keeps frequently occurring
// words with different suffixes that need to be stemmmed quickly.
// It is used during indexing and retrieval to stem and words to
// their base form.
class CLexon {

	// Stores the main word dictionary used in the lexon - binary hash table
	CHashDictionary<int> m_word_dictionary; 
	// Stores the word occurrence of each word
	CArrayList<uLong> m_word_occurrence; 
	// Stores the global word id for each word
	CArrayList<uLong> m_global_word_id;
	// Sotres the base word used in the dictionary - no suffixes appended
	CRootWord m_root_dict; 

	// Stores the size of the stop word list
	int m_stop_word_size; 
	// Stores the number of exclude words
	int m_exclude_word_num;

	// Reads a text file and indexes the terms - used
	// for initilizing stopwords
	void ReadTextFile(const char str[]) {

		char ch; 
		int word_length = 0; 
		bool new_word = false; 

		CHDFSFile file;
		file.OpenReadFile(str);

		while(file.ReadObject(ch)) {
			ch = tolower(ch); 

			if(CUtility::AskEnglishCharacter(ch)) {
				new_word = false; 
				CUtility::TempBuffer()[word_length++] = ch; 
			} else if(!new_word && word_length) {
				// adds the stop word to dictionary
				int id = m_word_dictionary.AddWord(CUtility::TempBuffer(), word_length);
				if(CStemWord::GetStem(CUtility::TempBuffer(), word_length) < 0) {
					m_root_dict.AddRootWord(CUtility::TempBuffer(), word_length);
				}
				
				m_word_occurrence.PushBack(1);
				m_global_word_id.PushBack(id);
				new_word = true; 
				word_length = 0; 
			} 
		}
	}

public:

	CLexon() {
	}

	// Starts the dictionary - only called once at the start
	inline void LoadLexon() {	
		ReadLexonFromFile();
	}

	// Reads the stopword list in from memory 
	void LoadStopWordList() {

		CUtility::Initialize(); 
		m_root_dict.Initialize();
		m_word_dictionary.Initialize(0xFFFFFF, 16); 	
		m_word_occurrence.Initialize(1024);
		m_global_word_id.Initialize(1024);

		CStemWord::Initialize("GlobalData/Lists/Suffixes");
		ReadTextFile("GlobalData/Lists/ExcludeWords"); 	
		m_exclude_word_num = m_word_dictionary.Size();

		ReadTextFile("GlobalData/Lists/StopWords"); 	
		ReadTextFile("GlobalData/Lists/Verbs"); 		
		m_stop_word_size = m_word_dictionary.Size(); 

		WriteLexonToFile();
	}

	// Initilizes the root word creation index
	// that is used for stemming and storing 
	// high occurring words - used in indexing
	void InitializeStemWords(int max_dict_size) {
		CStemWord::Initialize("GlobalData/Lists/IllegalSuffixes");
		CUtility::Initialize(); 
	}

	// Returns a reference to the word dictionary used in the lexon
	inline CHashDictionary<int> &Dictionary() {
		return m_word_dictionary; 
	}

	// Return the current size of the dictionary
	inline int LexonSize() {
		return m_word_dictionary.Size(); 
	}

	// Returns true if the word should be excluded 
	// @param index - the lexon word index
	inline bool AskExcludeWord(int index) {
		if(index < m_exclude_word_num && index >= 0) {
			m_word_occurrence[index]++;
			return true;
		}

		return false;
	}

	// Returns whether a term with a given index is a stopword
	inline bool AskStopWord(int index) {
		if(index < m_stop_word_size && index >= 0) {
			m_word_occurrence[index]++;
			return true;
		}

		return false;
	}

	// Checks to see if a given character string is a stopword
	inline bool AskStopWord(const char str[], int length, int start = 0) {
		int index = WordIndex(str, length, start); 
		if(index < 0)return false; 
		if(AskStopWord(index)) {
			m_word_occurrence[index]++;
			return true;
		}

		return false;
	}

	// Returns the word occurrence for a particular word index
	inline uLong WordOccurrence(int global_index) {
		return m_word_occurrence[global_index]; 
	}

	// Returns just the word index in the lexon
	inline int WordIndex(const char str[], int length, int start = 0) {
		return m_word_dictionary.FindWord(str, length, start); 
	}

	// This is used to increment the word occurrence of a particular
	// word id. This is used during the indexing phase to increment
	// the occurrence of words already stored in the lexon
	// @param net_occurrence - the net occurrence of all words in
	//                       - a particular log division
	inline void IncrementWordOccurr(int word_id) {
		m_word_occurrence[word_id]++;
	}

	// This compiles the set of additional occurrence vectors that were
	// created by each client during the indexing phase. This is so the
	// global word occurrence of each word in the lexon is correctly
	// established. This should be done after indexing is complete.
	void CompileClientWordOccurr(int client_num, 
		const char str[] = "GlobalData/WordDictionary/lexon") {

		CMemoryChunk<int> occurr;
		for(int i=0; i<client_num; i++) {

			cout<<"Occur "<<i<<" Out Of "<<client_num<<endl;
			occurr.ReadMemoryChunkFromFile
				(CUtility::ExtendString(str, ".client_occurr", i)); 

			for(int j=0; j<min(LexonSize(), occurr.OverflowSize()); j++) {
				m_word_occurrence[j] += occurr[j];
			}
		}
	}

	// This writes the word occurrence of all words stored in the lexon
	// as parsed by a particular client
	void WriteClientLexonWordOccurr(int client, 
		const char str[] = "GlobalData/WordDictionary/lexon") {

		m_word_occurrence.WriteMemoryChunkToFile
			(CUtility::ExtendString(str, ".client_occurr", client)); 
	}

	// This is the function used to creating the stemming index
	// that is used later to stem words and in the indexing phase
	// @param word_id - this is the global word id being added to the lexon
	// @param word_occur - the global occurence of the word
	void AddStemWord(uLong word_id, uLong word_occur, const char str[], int length) {

		int id = m_word_dictionary.AddWord(str, length); 

		if(m_word_dictionary.AskFoundWord() == false) {	
			m_word_occurrence.PushBack(word_occur);
			m_global_word_id.PushBack(word_id);

			if(CStemWord::GetStem(str, length) < 0) {
				m_root_dict.AddRootWord(str, length);
			} 
		}
	}

	// Returns the number of stopwords used 
	// assumes that the dictinary is already loaded
	inline int InternalStopWordSize() {
		return m_stop_word_size;
	}

	// Returns the number of stopwords used 
	// reads the size from file
	static int ExternalLexonSize(const char str[] = "GlobalData/WordDictionary/lexon") {

		int dict_size;
		CHDFSFile file; 
		file.OpenReadFile(str); 
		file.ReadCompObject(dict_size); 
		return dict_size;
	}

	// Reads the current lexon from file
	void ReadLexonFromFile(const char str[] = "GlobalData/WordDictionary/lexon") {

		CHDFSFile file; 
		file.OpenReadFile(str); 

		// skips past dictionary size
		file.ReadCompObject(m_stop_word_size);
		file.ReadCompObject(m_stop_word_size); 
		file.ReadCompObject(m_exclude_word_num); 

		m_word_dictionary.ReadHashDictionaryFromFile(file); 
		m_root_dict.ReadRootWordFromFile(file);
		m_word_occurrence.ReadArrayListFromFile(file);
		m_global_word_id.ReadArrayListFromFile(file);
	}

	// This reads just the lexon statistics from file
	int ReadLexonStatFromFile(const char str[] = "GlobalData/WordDictionary/lexon") {

		int dict_size;
		CHDFSFile file; 
		file.OpenReadFile(str); 
		file.ReadCompObject(dict_size);
		file.ReadCompObject(m_stop_word_size); 
		file.ReadCompObject(m_exclude_word_num); 

		return dict_size;
	}

	// Writes the current lexon to file
	void WriteLexonToFile(const char str[] = "GlobalData/WordDictionary/lexon") {

		int dict_size = m_word_dictionary.Size();

		CHDFSFile file; 
		file.OpenWriteFile(str); 
		file.WriteCompObject(dict_size); 
		file.WriteCompObject(m_stop_word_size); 
		file.WriteCompObject(m_exclude_word_num); 
 
		m_word_dictionary.WriteHashDictionaryToFile(file); 
		m_root_dict.WriteRootWordToFile(file);
		m_word_occurrence.WriteArrayListToFile(file);
		m_global_word_id.WriteArrayListToFile(file);
	}
}; 

// This class is used to create a spell checking mechanism for all of 
// the high occuring words in the lexicon. This is done by creating 
// a set of permuations for each lexicon word and then storing the 
// checksum of the permutation in a dictionary along with the id of
// the lexicon term. During retrieval when searching for alternate
// spellings of a word, the supplied word is permuted and the closest
// matching lexicon term is returned. 
class CSpellCheck : public CLexon {

	// This defines the number of letters to use for indexing
	static const int LETTER_NUM = 6;

	// This stores the set of reverse mappings for a given permutation
	struct SPermutation {
		// This stores the checksum of the permutation
		uLong check_sum;
		// This stores the lexicon id
		int id;
		// This stores the ptr to the next permutation
		SPermutation *next_ptr;
	};

	// This stores an instance of one of the retrieved words
	struct SWord {
		// This stores the word id
		int id;
		// This stores the term occurrence
		uLong occur;
		// This stores the term match
		uChar term_match;
	};

	// This stores the hash table
	CMemoryChunk<SPermutation *> m_hash_table;
	// This stores the set of permuatations
	CLinkedBuffer<SPermutation> m_perm_buff;
	// This stores the set of letter combinations
	CArrayList<const char *> m_letter_comb;

	// This stores the set of vowels
	CMemoryChunk<bool> m_vowel;
	// This stores the set of word indexes that follow vowels
	CArray<int> m_vowel_branch;
	// This stores the set of letters that make up
	// the word in expansion order
	CArray<int> m_letter_set;

	// This is used to compare words
	CLimitedPQ<SWord> m_word_queue;
	// This is used to store suffixes of the query term
	CTrie m_term_suffix;
	// This stores the maximum match score
	uChar m_match_score;
	// This stores the best matching lexicon id
	int m_lex_id;

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

	// This adds to the hash for the current letter index
	// @parma buff - this stores the word
	// @param length - this is the word length
	// @param i - this is the letter id
	void UpdateHash(char buff[], int length, int i) { 

		if(buff[i] == '*') {
			return;
		}

		m_letter_set.PushBack(i);

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

			m_letter_set.PushBack(index);
			buff[index] = '*';
		}

		for(int i=0; i<min(length, LETTER_NUM); i++) {
			m_letter_set.PushBack(i);
		}
	}

	// This adds a permuted word to the set 
	void AddPermutedWord(int id, uLong check_sum) {

		SPermutation *perm = m_perm_buff.ExtendSize(1);
		perm->check_sum = check_sum;
		perm->id = id;

		int hash = perm->check_sum % m_hash_table.OverflowSize();
		SPermutation *prev_ptr = m_hash_table[hash];
		m_hash_table[hash] = perm;
		perm->next_ptr = prev_ptr;
	}

	// This creates a set of permuatations for a given word
	void AddPermuteWord(uLong check_sum, int id, int offset, int num) {

		if(num >= 4) {
			AddPermutedWord(id, check_sum);
			return;
		}

		check_sum *= m_letter_comb[offset][0] + m_letter_comb[offset][1];
		for(int i=offset+1; i<m_letter_comb.Size(); i++) {
			AddPermuteWord(check_sum, id, i, num + 1);
		}
	}

	// This finds the set of closest matches
	void CycleThroughMatchSet(uLong check_sum) {

		static int len;
		SWord item;
		int hash = check_sum % m_hash_table.OverflowSize();
		SPermutation *ptr = m_hash_table[hash];

		while(ptr != NULL) {

			if(ptr->check_sum == check_sum) {
				char *word = Dictionary().GetWord(ptr->id, len);

				m_term_suffix.AddWord((char *)&ptr->id, sizeof(int));
				if(m_term_suffix.AskFoundWord() == false) {
					item.id = ptr->id;
					item.term_match = MatchScore(word, len);
					item.occur = WordOccurrence(ptr->id);
					m_word_queue.AddItem(item);
				}
			}

			ptr = ptr->next_ptr;
		}
	}

	// This finds the set of closest matches
	void PermuteClosestMatch(uLong check_sum, int offset, int num) {

		if(num >= 4) {
			CycleThroughMatchSet(check_sum);
			return;
		}

		check_sum *= m_letter_comb[offset][0] + m_letter_comb[offset][1];
		for(int i=offset+1; i<m_letter_comb.Size(); i++) {
			PermuteClosestMatch(check_sum, i, num + 1);
		}
	}

	// This is used to find the match score of the top N terms based on the edit distance
	void FindBestMatch(const char *word1, int len1) {

		int len2;
		m_lex_id = -1;
		m_match_score = 0xFF;
		while(m_word_queue.Size() > 0) {
			SWord word = m_word_queue.PopItem();
			char *word2 = Dictionary().GetWord(word.id, len2);

			uChar score = CUtility::EditDistance(word1, len1, word2, len2);
			if(score < m_match_score) {
				m_lex_id = word.id;
				m_match_score = score;
			}
		}
	}

public:

	CSpellCheck() {

		m_vowel.AllocateMemory(256, false);
		m_vowel['a'] = true;
		m_vowel['e'] = true;
		m_vowel['i'] = true;
		m_vowel['o'] = true;
		m_vowel['u'] = true;
	}

	// This loads the lexicon into memory and creates the spell check mapping
	void CreateSpellCheck(char str[] = "GlobalData/Lexon/spell_check") {

		LoadLexon();
		m_hash_table.AllocateMemory(CHashFunction::
			FindClosestPrime(0xFFFFFF), NULL); 

		int len;
		m_letter_comb.Initialize(1024);
		m_perm_buff.Initialize();
		CHashDictionary<int> &dict = Dictionary();
		for(int i=0; i<dict.Size(); i++) {cout<<i<<" "<<LexonSize()<<endl;
			char *word = dict.GetWord(i, len);

			memcpy(CUtility::SecondTempBuffer(), word, len);
			FindLetterSet(CUtility::SecondTempBuffer(), len);

			m_letter_comb.Resize(0);
			for(int j=0; j<min(m_letter_set.Size(), 6); j++) {
				if(m_letter_set[j] < len - 1) {
					m_letter_comb.PushBack(word + m_letter_set[j]);
				}
			}

			if(m_letter_comb.Size() >= 4) {

				for(int j=0; j<m_letter_comb.Size(); j++) {
					AddPermuteWord(1, i, j, 1);
				}
			}
		}

		m_perm_buff.WriteLinkedBufferToFile(str);
	}

	// This loads spell check from file
	void LoadSpellCheck(char str[] = "GlobalData/Lexon/spell_check") {

		m_perm_buff.ReadLinkedBufferFromFile(str);
		LoadLexon();

		m_term_suffix.Initialize(4);
		m_letter_comb.Initialize(1024);
		m_word_queue.Initialize(5, CompareWords);

		m_hash_table.AllocateMemory(CHashFunction::
			FindClosestPrime(0xFFFFFF), NULL); 

		SPermutation *ptr;
		m_perm_buff.ResetPath();
		while((ptr = m_perm_buff.NextNode()) != NULL) {
			int hash = ptr->check_sum % m_hash_table.OverflowSize();
			SPermutation *prev_ptr = m_hash_table[hash];
			m_hash_table[hash] = ptr;
			ptr->next_ptr = prev_ptr;
		}
	}

	// This finds the closest matching term in the lexicon to a given spelling 
	// @param edit_dist - this is the match score of the closest matched term
	// @return the closest match term
	char *FindClosestMatch(const char *word, int &len, uChar &edit_dist) {

		m_term_suffix.Reset();
		m_word_queue.Reset();
		m_term_suffix.AddWord(word, len);
		m_match_score = 0;

		for(int i=0; i<len-1; i++) {
			m_term_suffix.AddWord(word + i, 2);
		}

		memcpy(CUtility::SecondTempBuffer(), word, len);
		FindLetterSet(CUtility::SecondTempBuffer(), len);

		m_letter_comb.Resize(0);
		for(int j=0; j<m_letter_set.Size(); j++) {
			if(m_letter_set[j] < len - 1) {
				m_letter_comb.PushBack(word + m_letter_set[j]);
			}
		}

		for(int j=0; j<m_letter_comb.Size(); j++) {
			PermuteClosestMatch(1, j, 1);
		}

		FindBestMatch(word, len);
		edit_dist = m_match_score;

		if(m_lex_id >= 0) {
			return Dictionary().GetWord(m_lex_id, len);
		}

		edit_dist = 0xFF;
		return NULL;
	}

	// This is just a test framework
	void TestSpellCheck() {

	}
};


// Used in conjunction with compression to store
// the current hit position in the compression buffer
// so it can be accessed later.
struct SHitPosition {
	// stores the hit offset in the in memory buffer
	int hit_offset; 
	// stores the comp block in the comp buffer
	int comp_block; 
	// stores the number of comp blocks retrieved
	int blocks;

	SHitPosition() {
		hit_offset = 0; 
		comp_block = 0; 
		blocks = 4;
	}

	// This initializes a hit offset by supplying the comp block and offset
	// @param hit_offset - this is the offset in the comp block
	// @param comp_block = this is the compression block id
	SHitPosition(int hit_offset, int comp_block) {
		this->hit_offset = hit_offset; 
		this->comp_block = comp_block; 
		blocks = 4;
	}

	// Returns the difference between two hit positions 
	// in bytes given a consant comp buffer size.
	// @param pos1, pos2 - the first and second position
	//                   - of two hits (pos1 > pos2)
	// @param buffer_size - the size of the comp block
	// @return the difference between the two in bytes
	static _int64 ByteDiff(SHitPosition &pos1, SHitPosition &pos2, _int64 buffer_size) {

		if(pos2.comp_block > pos1.comp_block) {
			throw EIllegalArgumentException("pos2 > pos1"); 
		}

		if(pos2.comp_block == pos1.comp_block && pos2.hit_offset > pos1.hit_offset) {
			throw EIllegalArgumentException("pos2 > pos1"); 
		}
		
		_int64 offset1 = ((_int64)pos2.comp_block * buffer_size) + pos2.hit_offset;
		_int64 offset2 = ((_int64)pos1.comp_block * buffer_size) + pos1.hit_offset;
		
		return offset2 - offset1; 
	}

	// This sets the hit position back to the start
	void Reset() {
		hit_offset = 0; 
		comp_block = 0; 
		blocks = 4;
	}
}; 

// This is the infamous compression class, really just a buffer
// used for file storage. When the buffer becomes full it is 
// compressed and written out to file. GetCompressedBlock and 
// Next Hit can be used to retreive hits from the Compression buffer. 
// Compression buffer is also the class of choice for performing 
// all external memory operations such as sorting
class CCompression {

	// This defines a LocalData structure used to define statistics
	// relating to the comp buffer.
	struct SDivSize {
		// stores the current number of compression blocks
		int comp_block_num; 
		// stores the buffer size for the comp buffer
		int buffer_size; 
		// stores the comp block offset in the file
		int base_division_offset; 
		// stores the total number of bytes stored
		_int64 bytes_stored; 

		void Initialize(int buffer_size) {
			base_division_offset = 0; 
			comp_block_num = 0; 
			bytes_stored = 0; ; 
			this->buffer_size = buffer_size; 
		}
	}; 

	// Used for storing information related to 
	// indexing comp blocks 
	struct SCompIndex1 {
		// stores the uncompressed size
		// of the comp block
		uLong uncomp_size; 
		// stores the byte offset with in
		// the comp file for which a given
		// comp block starts
		_int64 byte_offset; 

		void Reset() {
			uncomp_size = 0; 
			byte_offset = 0; 
		}
	}; 

	// stores the compression block info index
	SDivSize m_div_size; 
	// used to store the compression blocks
	CArray<char> m_hit_buffer; 
	// used to store the size of the compression blocks
	CArray<SCompIndex1> m_comp_size; 
	// This stores the directory of the comp buffer
	char m_directory[512]; 
	// This stores the number of bytes read
	_int64 m_bytes_read;
	// This stores the current byte offset
	_int64 m_curr_byte_offset;

	// stores the current compression block and offset in the block
	SHitPosition m_next_pos; 
	// stores the start of the currently being accessed comp blocks
	SHitPosition m_curr_pos; 
	// stores the compression block
	CMemoryChunk<char> m_word_comp; 

	// stores an instance of the writing file used to store 
	// compressed blocks written to external storage
	CHDFSFile m_hit_file;
	// This stores the lookup blocks
	CHDFSFile m_lookup_file;

	// Writes the compression block offset to file
	void FlushCompressionSizeToFile() {

		for(int i=0; i<m_comp_size.Size(); i++) {
			m_lookup_file.WriteObject(m_comp_size[i].byte_offset); 
			m_lookup_file.WriteObject(m_comp_size[i].uncomp_size); 
		}

		m_comp_size.Resize(0); 
	}

	// Compress the buffer and writes to the end of the file
	// @return the most recent file division
	void FlushHitListToFile() {

		CMemoryChunk<char> compress_buff; 
		int compress_size = CHDFSFile::CompressBuffer(m_hit_buffer.Buffer(), 
			compress_buff, m_hit_buffer.Size()); 

		if(compress_size <= 0) {
			return; 
		}

		// writes the compressed hit buffer
		m_hit_file.WriteObject(compress_buff.Buffer(), compress_size); 

		if((m_comp_size.Size() + 1) >= m_comp_size.OverflowSize()) {
			FlushCompressionSizeToFile(); 
		}

		// stores the base offset first followed by the uncompressed block size
		// increments the base offest of the compression block
		m_curr_byte_offset += compress_size; 

		// stores the comp block index info
		m_comp_size.ExtendSize(1); 
		m_comp_size.LastElement().byte_offset = m_curr_byte_offset; 
		m_comp_size.LastElement().uncomp_size = m_hit_buffer.Size(); 

		// increments the number of compression blocks 
		// stored in the current file division
		m_div_size.comp_block_num++; 
		m_div_size.bytes_stored += m_hit_buffer.Size(); 
		m_hit_buffer.Resize(0); 
	}

	// Reads the given comp index information in from file
	// so the comp retr_comp_buff offset can be retrieved an read
	// in from memory
	// @param comp_block_num - the number of comp blocks that
	//       - are being retrieved
	// @param comp_block_offset - an index givin the comp block that is
	//       - being retrieved
	// @param comp_index - stores enough room to store the entire compression index
	//      - in memory for the current set of comp blocks
	void RetrieveCompIndexLookup(int comp_block_num, int comp_block_offset,
		CArray<SCompIndex1> &comp_index) {

		int index_num = comp_block_num; 
		if(comp_block_offset > 0) {
			// used to read the start of the first index
			// if not at the start of the file
			comp_block_offset--; 
			index_num++; 
		} else {
			// makes the first comp index point
			// to the start of the file
			comp_index.ExtendSize(1); 
			comp_index.LastElement().Reset(); 
		}

		CHDFSFile file; 
		file.OpenReadFile(CUtility::ExtendString
			(m_directory, ".comp.comp_lookup")); 
		file.SeekReadFileFromBeginning(comp_block_offset * (sizeof(_int64) + sizeof(uLong))); 

		// reads the lookup index information first so
		// the correct comp retr_comp_buff can be retrieved
		for(int i=0; i<index_num; i++) {
			comp_index.ExtendSize(1); 
			file.ReadObject(comp_index.LastElement().byte_offset); 	
			file.ReadObject(comp_index.LastElement().uncomp_size); 	
		}
	}

	// Reads all the given comp blocks in from memory at the 
	// specified location - more than one comp retr_comp_buff may
	// have been specified in which case each sequential
	// comp retr_comp_buff is concanated on top of each other
	// @param curr_comp_block - an id for the current comp block being retrieved
	// @param comp_index - stores enough room to store the entire compression index
	//      - in memory for the current set of comp blocks
	// @param retr_comp_buff - a buffer used to store
	//       - the retrieved uncompressed blocks
	// @param comp_block_num - the number of comp blocks to retrieve
	void RetrieveCompBlocks(int curr_comp_block, CArray<SCompIndex1> &comp_index, 
		CMemoryChunk<char> &retr_comp_buff, int comp_block_num) {

		int retr_offset = 0; 
		// stores the current byte offset within comp file
		_int64 curr_byte_offset = comp_index[0].byte_offset; 

		CHDFSFile file; 
		file.OpenReadFile(CUtility::ExtendString
			(m_directory, ".comp.hit")); 
		file.SeekReadFileFromBeginning(curr_byte_offset * sizeof(char)); 

		for(int i=1; i<comp_block_num + 1; i++) {

			CMemoryChunk<char> decompress(comp_index[i].byte_offset - curr_byte_offset);
			// reads a single compression block from file
			file.ReadObject(decompress.Buffer(), decompress.OverflowSize()); 

			// now decompress the buffer
			file.DecompressBuffer(decompress.Buffer(), retr_comp_buff.Buffer() + retr_offset, 
				decompress.OverflowSize(), comp_index[i].uncomp_size); 

			curr_byte_offset = comp_index[i].byte_offset; 
			retr_offset += comp_index[i].uncomp_size; 	
		}

	}

	// This is called when the hit buffer is currently loaded 
	// into memory and needs to be be flushed out to file.
	// This is usually done after all LocalData is written to the
	// comp buffer and it needs to be flushed.
	void FinishInternalCompression() {

		if(m_hit_buffer.Size() > 0) {
			FlushHitListToFile(); 
		}

		if(m_comp_size.Size() > 0) {
			FlushCompressionSizeToFile(); 
		}

		m_lookup_file.WriteObject(m_div_size); 

		m_hit_file.CloseFile();
		m_lookup_file.CloseFile();

		m_comp_size.FreeMemory();
		m_hit_buffer.FreeMemory();
	}

public:

	CCompression() {
	}

	// Sets up the buffer and the compression index
	// return true if the comp buffer has already been
	// initialized before, false otherwise
	CCompression(const char str[], int buffer_size = 1500000) {
		Initialize(str, buffer_size); 
	}

	// Sets up the buffer and the compression index
	// return true if the comp buffer has already been
	// initialized before, false otherwise
	void Initialize(const char str[], int buffer_size = 1500000) {
		if(str == NULL || buffer_size <= 0) {
			throw EIllegalArgumentException("buffer size <= 0 or str NULL");
		}

		strcpy(m_directory, str); 
		m_div_size.Initialize(buffer_size); 

		m_div_size.buffer_size = buffer_size; 
		m_hit_buffer.Initialize(BufferSize()); 
		m_comp_size.Initialize(50); 

		m_curr_byte_offset = 0;
		m_hit_file.OpenWriteFile(CUtility::ExtendString
			(m_directory, ".comp.hit"));
		m_lookup_file.OpenWriteFile(CUtility::ExtendString
			(m_directory, ".comp.comp_lookup"));
	}

	// forces flushes the hit buffer
	inline void ForceFlush() {
		if(m_hit_buffer.Size() > 0) {
			FlushHitListToFile(); 
		}
	}

	// This sets the buffer size, if the comp buffer is currently
	// full it flushes it and then changes the buffer size.
	// @param buffer_size - the size in bytes to set the buffer to
	inline void SetBufferSize(int buffer_size) {
		ForceFlush();
		m_div_size.buffer_size = buffer_size; 
	}

	// Sets a new directory name
	inline void SetDirectoryName(const char str[]) {
		strcpy(m_directory, str); 
	}

	// Replaces an old compression buffer with a new one
	// note that the old compression buffer is simply renamed and the new 
	// compression buffer is loaded up with the old compression buffer
	// @param old_comp - the compression buffer for which the name is changed
	//                 - to the new compression buffer
	// @param new comp - the name that will be superinposed onto the old comp
	//                 - and reloaded with the old comp contents when finished
	static void ReplaceCompression(CCompression &old_comp, CCompression &new_comp) {

		new_comp.RemoveCompression(); 

		strcpy(CUtility::ThirdTempBuffer(), old_comp.DirectoryName()); 
		old_comp.RenameCompression(new_comp.DirectoryName()); 

		old_comp.HitList().FreeMemory(); 
		// resets the old comp directory back to its original name
		old_comp.SetDirectoryName(CUtility::ThirdTempBuffer()); 

		if(old_comp.BytesStored() <= 0) {
			return; 
		}

		// reloads the sorted compression buffer
		new_comp.LoadIndex(new_comp.DirectoryName()); 
		new_comp.ResetNextHitItem(); 
	}

	// Returns the total number of bytes stored
	// in the comp buffer
	inline _int64 BytesStored() {
		return m_div_size.bytes_stored + m_hit_buffer.Size(); 
	}

	// Checks for an overflow for a given offset in the buffer
	inline bool AskBufferOverflow(int offset) {
		if((m_hit_buffer.Size() + offset) > m_hit_buffer.OverflowSize()) {
			FlushHitListToFile(); 
			return true; 
		}
		return false; 
	}

	// Copies the a character string to the buffer
	void AddToBuffer(const char str[], int length, int start = 0) {
		while((m_hit_buffer.Size() + (length - start)) > m_hit_buffer.OverflowSize()) {
			// copies the external buffer into the hit buffer
			memcpy(m_hit_buffer.Buffer() + m_hit_buffer.Size(), str + start, 
				m_hit_buffer.OverflowSize() - m_hit_buffer.Size()); 
			// increments the start offset
			start += m_hit_buffer.OverflowSize() - m_hit_buffer.Size(); 
			// resizes the buffer
			m_hit_buffer.Resize(m_hit_buffer.OverflowSize()); 
			FlushHitListToFile(); 
		}

		// copies the external buffer into the hit buffer
		memcpy(m_hit_buffer.Buffer() + m_hit_buffer.Size(), str + start, length - start); 
		// resizes the buffer
		m_hit_buffer.Resize(m_hit_buffer.Size() + length - start); 
	}


	// Copies a linked buffer to the buffer
	void AddToBuffer(CLinkedBuffer<char> &str) {

		int start = 0; 
		str.ResetPath(); 
		while((m_hit_buffer.Size() + (str.Size() - start)) > m_hit_buffer.OverflowSize()) {
			// copies the external buffer into the hit buffer
			str.CopyLinkedBufferToBuffer(m_hit_buffer.Buffer() + m_hit_buffer.Size(), 
				m_hit_buffer.OverflowSize() - m_hit_buffer.Size()); 
			// resizes the buffer
			start += m_hit_buffer.OverflowSize() - m_hit_buffer.Size(); 
			// resizes the buffer
			m_hit_buffer.Resize(m_hit_buffer.OverflowSize()); 
			FlushHitListToFile(); 
		}
		// copies the external buffer into the hit buffer
		str.CopyLinkedBufferToBuffer(m_hit_buffer.Buffer() + 
			m_hit_buffer.Size(), str.Size() - start); 
		// resizes the buffer
		m_hit_buffer.Resize(m_hit_buffer.Size() + (str.Size() - start)); 
	}

	// Renames the current compression index
	void RenameCompression(char new_name[], char old_name[] = NULL) {
		if(old_name != NULL) {
			strcpy(m_directory, old_name); 
		}

		if(BytesStored() <= 0) {
			return; 
		}

		char base_file[1024]; 
		strcpy(base_file, CUtility::ExtendString(new_name, ".comp.hit_buffer_temp")); 
		CHDFSFile::Rename(CUtility::ExtendString(m_directory, ".comp.hit_buffer_temp"), base_file); 

		strcpy(base_file, CUtility::ExtendString(new_name, ".comp.hit")); 
		CHDFSFile::Rename(CUtility::ExtendString(m_directory, ".comp.hit"), base_file); 

		strcpy(base_file, CUtility::ExtendString(new_name, ".comp.comp_lookup")); 
		CHDFSFile::Rename(CUtility::ExtendString(m_directory, ".comp.comp_lookup"), base_file); 

		strcpy(m_directory, new_name); 
	}

	// Gets the next char from the uncompressed block
	bool NextHit(char &word, int comp_block = 4) {
		
		if(m_next_pos.hit_offset >= m_word_comp.OverflowSize()) {
			m_curr_pos.comp_block = m_next_pos.comp_block; 
			m_curr_pos.blocks = comp_block;
			if(GetUncompressedBlock(m_word_comp, 
				comp_block, m_next_pos.comp_block) < 0)
				return false; 

			m_next_pos.hit_offset = 0; 
			m_next_pos.comp_block += comp_block; 
		}

		m_bytes_read++;
		word = m_word_comp[m_next_pos.hit_offset++]; 
		return true; 
	}

	// Checks to see if there are any more bytes left in the
	// comp buffer.
	// @return true if there are more hits availabe, false otherwise
	bool AskNextHitAvailable() {

		if(m_next_pos.comp_block < CompBlockNum()) {
			return true; 
		}

		if(m_next_pos.hit_offset < m_word_comp.OverflowSize()) {
			return true; 
		}

		return false; 
	}

	// Returns the current position associated with
	// the next hit item 
	// @return a pointer to the current hit position
	inline SHitPosition GetCurrentHitPosition() {
		SHitPosition pos = m_curr_pos;
		pos.hit_offset = m_next_pos.hit_offset; 
		return pos; 
	}

	// Sets the current position associated with
	// the next hit item.
	// @param pos - the comp buffer position that needs to be accessed
	// @param comp_block - the number of comp blocks to retrieve
	// @return false if the current position cannot be reached
	void SetCurrentHitPosition(const SHitPosition &pos) {

		if(pos.comp_block < m_curr_pos.comp_block 
			|| pos.comp_block > m_curr_pos.comp_block
			|| pos.hit_offset > m_word_comp.OverflowSize()) {

			int comp_blocks = pos.blocks;
			if(GetUncompressedBlock(m_word_comp, comp_blocks, pos.comp_block) < 0) {
				throw EIllegalArgumentException("Invalid comp position"); 
			}

			m_curr_pos.comp_block = pos.comp_block; 
			m_next_pos.comp_block = pos.comp_block + pos.blocks; 
		}

		m_next_pos.hit_offset = pos.hit_offset;
	}

	// Sets the current position associated with the next hit item.
	// @param byte_offset - this is the byte offset of the hit
	// @return false if the current position cannot be reached
	void SetCurrentHitPosition(_int64 byte_offset) {

		SHitPosition pos;
		pos.comp_block = byte_offset / BufferSize();
		pos.hit_offset = byte_offset % BufferSize();

		m_bytes_read = byte_offset;
		SetCurrentHitPosition(pos);
	}

	// Restarts the reading of the compression blocks
	void ResetNextHitItem() {
		m_next_pos.Reset(); 
		m_curr_pos.Reset(); 
		m_word_comp.FreeMemory(); 
		m_bytes_read = 0;
	}

	// This returns the percent of the file read
	inline float PercentageHitsRead() {
		return (float)m_bytes_read / BytesStored();
	}

	// Gets the next item number from the uncompressed block
	// @param item - a buffer to store the retrieved uncompressed hits
	// @param item_num - the number of bytes to retrieve
	// @param comp_blocks - the number of comp blocks to retrieve
	//                    - at any one time
	bool NextHit(char item[], int item_num, int comp_blocks = 4) {
		if(item_num <= 0)throw EIllegalArgumentException("item_num <= 0"); 

		int size = m_word_comp.OverflowSize() - m_next_pos.hit_offset; 
		m_bytes_read += item_num;

		if(size >= item_num) {
			memcpy(item, m_word_comp.Buffer() + m_next_pos.hit_offset, item_num); 
			m_next_pos.hit_offset += item_num; 
			return true; 
		}

		memcpy(item, m_word_comp.Buffer() + m_next_pos.hit_offset, size); 
		item_num -= size; 
		item += size; 

		while(true) {
			m_curr_pos.comp_block = m_next_pos.comp_block; 
			m_curr_pos.blocks = comp_blocks;
			if(GetUncompressedBlock(m_word_comp, 
				comp_blocks, m_next_pos.comp_block) < 0) {
					
				m_next_pos.hit_offset = m_word_comp.OverflowSize();
				return false; 
			}

			m_next_pos.comp_block += comp_blocks; 
			if(m_word_comp.OverflowSize() >= item_num) {
				// finished since comp buffer exceeds local buffer
				memcpy(item, m_word_comp.Buffer(), item_num); 
				m_next_pos.hit_offset = item_num; 
				return true; 
			}

			memcpy(item, m_word_comp.Buffer(), m_word_comp.OverflowSize()); 
			item += m_word_comp.OverflowSize(); 
			item_num -= m_word_comp.OverflowSize(); 
		}

		return true; 
	}

	// Gets the coding associated with an
	// escaped four byte integer
	// @returns the size of the integer in bytes
	int GetEscapedEncoding(char buffer[]) {
		int byte_count = 1; 

		while(true) {
			if(!NextHit(*buffer))return -1; 
			// checks if finished
			if(((uChar)*buffer & 0x80) == 0)return byte_count; 
			byte_count++; 
			buffer++; 
		}
		return byte_count; 
	}

	// Retrieves next 5-bytes and places in a 8-byte integer
	// @param item - used to store the 8-byte item
	// @return true if more bytes available, false otherwise
	inline bool Get5ByteItem(_int64 &item) {
		item = 0;
		return NextHit((char *)&item, 5);
	}

	// Gets an escaped four byte integer
	// @returns the number of bytes used to store it
	int GetEscapedItem(uLong &item) {
		item = 0; 
		int offset = 0; 
		int byte_count = 1; 
		char hit; 

		while(true) {
			if(!NextHit(hit))return -1; 
			item |= (uLong)(((uChar)(hit & 0x7F)) << offset); 
			// checks if finished
			if(!((uChar)hit & 0x80))return byte_count; 
			byte_count++; 
			offset += 7; 
		}

		return byte_count; 
	}

	// Gets an escaped 56-bit integer and places
	// the results in the buffer
	// @returns the number of bytes used to store it
	int GetEscapedItem(char buffer[]) {
		int offset = 0; 
		uLong item = 0; 
		int byte_count = 1; 
		static char hit; 

		while(offset < 28) {
			if(!NextHit(hit))return -1; 
			item |= (uLong)(hit & 0x7F) << offset; 
			// checks if finished
			if(((uChar)hit & 0x80) == 0) {
				*(uLong *)buffer = item;
				return byte_count; 
			}
			byte_count++; 
			offset += 7; 
		}

		*(uLong *)buffer = item;
		offset = 0;
		item = 0;

		while(true) {
			NextHit(hit);
			item |= (uLong)(hit & 0x7F) << offset; 
			if(((uChar)hit & 0x80) == 0)break;
			byte_count++; 
			offset += 7; 
		}

		*(uChar *)(buffer + 3) |= item << 4;
		*(uLong *)(buffer + 4) |= item >> 4;
		return byte_count; 
	}

	// Gets an escaped n-byte integer and places
	// the results in the buffer
	// @returns the number of bytes used to store it
	inline int GetEscapedItem(_int64 &item) {
		item = 0; 
		return GetEscapedItem((char *)&item); 
	}
	
	// Gets an escaped n-byte integer and places
	// the results in the buffer
	// @returns the number of bytes used to store it
	int GetEscapedItem(S5Byte &item) {
		_int64 temp = 0; 
		int bytes = GetEscapedItem(temp); 
		item.SetValue(temp); 
		return bytes; 
	}

	// Adds a X byte escaped integer
	// @return the number of bytes added
	template <class X> int AddEscapedItem(X item) {
		if(item < 0)throw EIllegalArgumentException("item < 0"); 

		int bytes = 1; 
		while(item >= 128) {
			AskBufferOverflow(1);
			m_hit_buffer.PushBack((uChar)(item|0x80)); 
			item >>= 7; 
			bytes++; 
		}

		AskBufferOverflow(1);
		m_hit_buffer.PushBack((uChar)item); 
		return bytes; 
	}

	// just a test framework
	void TestNextHit() {
		Initialize("test", 999); 
		CVector<char> buffer; 
		CVector<int> size_space; 

		for(int i=0; i<999; i++) {
			int size = (rand() % 9999) + 10; 
			for(int c=0; c<size; c++) {
				char test = rand(); 
				buffer.PushBack(test); 
				AddToBuffer(&test, 1); 
			}

			cout<<i<<endl;
			size_space.PushBack(size); 
		}

		FinishCompression(); 
		LoadIndex("test");
		ResetNextHitItem(); 
		CMemoryChunk<char> temp(19999); 
		int offset = 0; 
		for(int i=0; i<999; i++) {cout<<i<<endl;

			if(!NextHit(temp.Buffer(), size_space[i], 1)) {
				cout<<"Error3"; getchar(); 
			}

			for(int c=0; c<size_space[i]; c++) {
				if(buffer[offset++] != temp[c]) {
					cout<<"Error "<<i; getchar(); 
					for(int c=0; c<size_space[i]; c++)cout<<(int)temp[c]<<" "; getchar(); 
				}
			}
		}
		if(NextHit(temp.Buffer(), 1, 1)) {
			cout<<"Error4"; getchar(); 
		}
	}

	// just a test framework
	void TestCompression() {
		RemoveCompression("Test"); 
		Initialize("Test", 4443); 
		CMemoryChunk<_int64> compare(99999); 
	
		for(int i=0; i<compare.OverflowSize(); i++) {
			compare[i] = (_int64)rand() << 31; 
			AskBufferOverflow(6); 
			AddEscapedItem(compare[i]); 
		}

		FinishCompression(); 
		ResetNextHitItem(); 
		CMemoryChunk<char> buffer(100); 

		_int64 item; 
		for(int i=0; i<compare.OverflowSize(); i++) {
			item = 0; 
			GetEscapedItem(item); 
			if(item != compare[i]) {
				cout << "error "<< compare[i] <<" "<< i; 
				getchar(); 
			}
		}

		RemoveCompression("Test"); 
	}

	// Just frees the memory associated with the comp buffer
	void FreeMemory() {
		m_hit_buffer.FreeMemory(); 
		m_comp_size.FreeMemory(); 
		m_word_comp.FreeMemory(); 
	}
	
	// Adds a 5 - bytes integer
	inline void AddHitItem5Bytes(_int64 item) {
		m_hit_buffer.PushBack(*((char *)&item)); 
		m_hit_buffer.PushBack(*(((char *)&item) + 1)); 
		m_hit_buffer.PushBack(*(((char *)&item) + 2)); 
		m_hit_buffer.PushBack(*(((char *)&item) + 3)); 
		m_hit_buffer.PushBack(*(((char *)&item) + 5)); 
	}

	// Adds a 5 - bytes integer
	inline void AddHitItem5Bytes(S5Byte & item) {
		_int64 value = item.Value();
		AddHitItem5Bytes(value);
	}

	// Adds a 4 - byte integer
	template <class X> inline void AddHitItem4Bytes(X item) {
		m_hit_buffer.PushBack(*((char *)&item)); 
		m_hit_buffer.PushBack(*(((char *)&item) + 1)); 
		m_hit_buffer.PushBack(*(((char *)&item) + 2)); 
		m_hit_buffer.PushBack(*(((char *)&item) + 3)); 
	}

	// Adds a 3 - byte integer
	template <class X> inline void AddHitItem3Bytes(X item) {
		m_hit_buffer.PushBack(*((char *)&item)); 
		m_hit_buffer.PushBack(*(((char *)&item) + 1)); 
		m_hit_buffer.PushBack(*(((char *)&item) + 2)); 
	}

	// adds a 2 - byte integer
	template <class X> inline void AddHitItem2Bytes(X item) {
		m_hit_buffer.PushBack(*((char *)&item)); 
		m_hit_buffer.PushBack(*(((char *)&item) + 1)); 
	}

	// Gets the current hit list
	inline CArray<char> &HitList() {
		return m_hit_buffer; 
	}

	// Gets the number of compression block stored
	inline int CompBlockNum() {
		return m_div_size.comp_block_num; 
	}

	// Gets the buffer size
	inline int BufferSize() {
		return m_div_size.buffer_size; 
	}

	// Erase all the files for the singular compression
	bool RemoveCompression(const char str[] = NULL) {

		if(str != NULL) {
			strcpy(m_directory, str); 
		}

		FinishCompression();
		CHDFSFile::Remove(CUtility::ExtendString(DirectoryName(), ".comp.hit")); 
		CHDFSFile::Remove(CUtility::ExtendString(DirectoryName(), ".comp.comp_lookup")); 
		m_hit_buffer.FreeMemory(); 
		m_comp_size.FreeMemory();

		return true; 
	}

	// Gets the name of the compression buffer
	inline char *DirectoryName() {
		return m_directory; 
	}

	// Gets an uncompressed retr_comp_buff from file at the given
	// @param retr_comp_buff - a buffer used to store
	//                       - the retrieved uncompressed blocks
	// @param comp_block_num - the number of compression blocks to retrieve
	//                       - note this number may be modified to reflect
	//                       - the actual number of comp blocks retrieved
	//                       - if the given number exceeds the total comp
	//                       - blocks stored
	// @param comp_block_offset - an index given the comp block to retrieve
	// @return the total uncompressed size, -1 if 
	//         the comp retr_comp_buff could not be retrieved
	int GetUncompressedBlock(CMemoryChunk<char> &retr_comp_buff, 
		int &comp_block_num, int comp_block_offset = 0) {

		if(comp_block_offset < 0) {
			throw EIllegalArgumentException
			("comp_block_offset < 0"); 
		}

		// checks for a division overflow
		if(comp_block_offset >= CompBlockNum()) {
			return -1;
		}

		// resizes the number of compression blocks if it exceeds the max boundary
		if((comp_block_offset + comp_block_num) > CompBlockNum()) {
			comp_block_num = CompBlockNum() - comp_block_offset; 
		}

		// allocates enough room to store the entire compression index
		// in memory for the current set of comp blocks
		CArray<SCompIndex1> comp_index(comp_block_num + 1); 	

		RetrieveCompIndexLookup(comp_block_num, comp_block_offset, comp_index); 

		int net_uncomp_size = 0; 
		// calculates the net uncompressed size
		for(int i=1; i<comp_block_num + 1; i++) {
			net_uncomp_size += comp_index[i].uncomp_size; 
		}

		// retrieves all the comp blocks
		retr_comp_buff.AllocateMemory(net_uncomp_size); 
		RetrieveCompBlocks(comp_block_offset, comp_index, 
			retr_comp_buff, comp_block_num); 

		return net_uncomp_size; 
	}

	// loads the compression index in from memory
	void LoadIndex(const char str[] = NULL) {

		if(str != NULL) {
			strcpy(DirectoryName(), str); 
		}

		CHDFSFile file; 
		strcpy(CUtility::ThirdTempBuffer(), DirectoryName());
		strcat(CUtility::ThirdTempBuffer(), ".comp.comp_lookup");
		file.OpenReadFile(CUtility::ThirdTempBuffer()); 
		file.SeekReadFileFromBeginning(file.ReadFileSize() - sizeof(SDivSize));
		file.ReadObject(m_div_size);

		ResetNextHitItem(); 
	}

	// Called once at the end when no more information 
	// needs to be written to the compression index
	void FinishCompression() {

		if(m_comp_size.OverflowSize() == 0) {
			return;
		}

		FinishInternalCompression();
	}

	~CCompression() {
		FinishCompression(); 
	}
}; 

// This defines a range of bytes for which a loaded comp
// block exists in memory. It also stores a pointer to
// the region in memory that contains the comp block.
struct SCompBlockPtr {
	// This stores the start bound offset
	_int64 start_bound;
	// This stores the end bound offset
	_int64 end_bound;
	// This stores the current access instance of a comp 
	// block that must be honoured for the duration of a query
	uLong session_id;
	// This stores the comp block
	CMemoryChunk<char> block;
	// This stores a ptr to the hit block set that contains 
	// this comp block
	void *hit_set_ptr;
	// This stores a ptr to the next comp block
	SCompBlockPtr *next_hash_ptr;
	// This stores a ptr to the next cache block
	SCompBlockPtr *next_cache_ptr;
	// This stores a ptr to the next cache block
	SCompBlockPtr *prev_cache_ptr;
};

// This class stores a list of high occuring comp blocks in memory and
// returns requested segments to the user. If a comp block is not available
// than it has to be loaded externally from memory. If there is not enough
// memory to store all requested comp blocks, than a LRU eviction policy
// is utilized to free older blocks. Hashed search lists are used to search
// for the correct comp block given a requested byte offset by the user. When
// a block is added to the list an entry needs to be made in the tree. Like wise
// when a block is freed, the previous entry needs to be removed from the 
// linked list.
class CHitItemBlock {

	// This defines the maximum number of bytes allowed in memory
	static const int MAX_BYTE_NUM = 500000000;

	// This stores the hashed red black trees, which gives pointers
	// to comp blocks loaded into memory
	CMemoryChunk<SCompBlockPtr *> m_hash_list;
	// This stores the number of hash blocks per hash division
	int m_hash_block_num;
	// This stores an instance of the comp buffer
	CCompression m_comp;
	// This stores the previois comp block accessed
	SCompBlockPtr *m_prev_comp_ptr;

	// This stores the head of the linked cache blocks
	static SCompBlockPtr *m_head_cache_ptr;
	// This stores the tail of the linked cache blocks
	static SCompBlockPtr *m_tail_cache_ptr;
	// This stores all the comp blocks loaded into memory
	static CLinkedBuffer<SCompBlockPtr> m_loaded_comp_block;
	// This stores a pointer to the free list of comp blocks
	static SCompBlockPtr *m_free_list;
	// This stores the total number of bytes loaded into memory
	static int m_bytes_loaded;
	// This stores the current query instance
	static uLong m_session_id;

	// This removes a comp block from the hash list when it has been evicted.
	// @param comp_block - this is a pointer to the comp block being removed
	void RemoveCompBlock(SCompBlockPtr *comp_block) {

		int hash_div = (int)(comp_block->start_bound / 
			(m_comp.BufferSize() * m_hash_block_num));

		SCompBlockPtr *prev_ptr = NULL;
		SCompBlockPtr *curr_ptr = m_hash_list[hash_div];

		while(curr_ptr != NULL) {
			if(curr_ptr == comp_block) {

				if(prev_ptr == NULL) {
					m_hash_list[hash_div] = curr_ptr->next_hash_ptr;
					return;
				}

				prev_ptr->next_hash_ptr = curr_ptr->next_hash_ptr;

				return;
			}
			prev_ptr = curr_ptr;
			curr_ptr = curr_ptr->next_hash_ptr;
		}
	}

	// This moves the most recently accessed comp_block to 
	// the head of the LRU list.
	void PromoteCompBlock(SCompBlockPtr *comp_ptr) {

		if(comp_ptr == m_head_cache_ptr) {
			// already at the head of the list
			return;
		}

		if(comp_ptr == m_tail_cache_ptr) {
			// reassigns the tail
			m_tail_cache_ptr = comp_ptr->prev_cache_ptr;
			m_tail_cache_ptr->next_cache_ptr = NULL;
		} else {
			// links up the nodes in the list
			(comp_ptr->prev_cache_ptr)->next_cache_ptr = comp_ptr->next_cache_ptr;
			(comp_ptr->next_cache_ptr)->prev_cache_ptr = comp_ptr->prev_cache_ptr;

			if(comp_ptr->prev_cache_ptr == NULL) {
				cout<<"mo";getchar();
			}
		}

		SCompBlockPtr *prev_ptr = m_head_cache_ptr;
		m_head_cache_ptr = comp_ptr;
		m_head_cache_ptr->next_cache_ptr = prev_ptr;
		m_head_cache_ptr->prev_cache_ptr = NULL;
		prev_ptr->prev_cache_ptr = m_head_cache_ptr;
	}

	// This evicts a comp block from memory. This means adding a comp buffer
	// entry to the free list for later use.
	bool EvictCompBlock() {

		if(m_tail_cache_ptr == NULL) {
			return false;
		}
		
		if(m_tail_cache_ptr->session_id == m_session_id) {
			return false;
		}
		
		m_bytes_loaded -= m_tail_cache_ptr->block.OverflowSize();
		m_tail_cache_ptr->block.FreeMemory();

		SCompBlockPtr *cache_ptr = m_tail_cache_ptr;
		m_tail_cache_ptr = cache_ptr->prev_cache_ptr;
		if(m_tail_cache_ptr != NULL) {
			m_tail_cache_ptr->next_cache_ptr = NULL;
		}

		CHitItemBlock *this_ptr = (CHitItemBlock *)cache_ptr->hit_set_ptr;
		this_ptr->RemoveCompBlock(cache_ptr);

		SCompBlockPtr *prev_ptr = m_free_list;
		m_free_list = cache_ptr;
		m_free_list->next_cache_ptr = prev_ptr;
		return true;
	}

	// This adds a new comp_block to the set of cached comp_blocks
	inline void AddCompBlockToCachedSet(SCompBlockPtr *ptr) {

		SCompBlockPtr *prev_ptr = m_head_cache_ptr;
		m_head_cache_ptr = ptr;
		m_head_cache_ptr->next_cache_ptr = prev_ptr;

		if(prev_ptr != NULL) {
			prev_ptr->prev_cache_ptr = m_head_cache_ptr;
		} else {
			m_tail_cache_ptr = m_head_cache_ptr;
		}

		m_head_cache_ptr->prev_cache_ptr = NULL;
	}

	// This adds a new comp block to the hash table so it can be 
	// later retrieved during a lookup.
	// @param ptr - this is a ptr to the comp block being added
	// @param comp_offset - this is the id of the comp block
	void AddCompBlockToHashTable(SCompBlockPtr *ptr, int comp_offset) {

		int hash_div = (int)(comp_offset / m_hash_block_num);
		SCompBlockPtr *prev_ptr = m_hash_list[hash_div];
		m_prev_comp_ptr = ptr;

		m_hash_list[hash_div] = ptr;
		ptr->next_hash_ptr = prev_ptr;

		ptr->session_id = m_session_id;
		ptr->start_bound = (_int64)comp_offset * m_comp.BufferSize();
		ptr->end_bound = ptr->start_bound + ptr->block.OverflowSize();
		ptr->hit_set_ptr = this;
	}

	// This loads in a comp block from external storage and stores it
	// internally. One of the existing comp blocks may need to be evicted.
	// @param byte_offset - this is the offset for which the comp buffer is 
	//                    - being retrieved
	void LoadCompBlock(_int64 &byte_offset) {

		while(m_bytes_loaded >= MAX_BYTE_NUM) {
			if(EvictCompBlock() == false) {
				break;
			}
		}
		
		SCompBlockPtr *ptr = m_free_list;
		if(ptr == NULL) {
			ptr = m_loaded_comp_block.ExtendSize(1);
		} else {
			m_free_list = m_free_list->next_hash_ptr;
		}

		int comp_blocks = 1;
		int comp_offset = (int)(byte_offset / m_comp.BufferSize());
		m_comp.GetUncompressedBlock(ptr->block, comp_blocks, comp_offset);
		m_bytes_loaded += ptr->block.OverflowSize();

		AddCompBlockToHashTable(ptr, comp_offset);
		AddCompBlockToCachedSet(ptr);

		if(byte_offset < ptr->start_bound) {
			cout<<"start bound error";getchar();
		}

		if(byte_offset >= ptr->end_bound) {
			cout<<"end bound error";getchar();
		}
	}

	// This looks for an existing comp block that bounds a given 
	// byte offset. If the block does not exist than null is returned.
	// @param byte_offset - this is the byte offset being retrieved
	// @return a ptr to the comp block if it exists, NULL otherwise
	SCompBlockPtr *FindCompBlock(_int64 &byte_offset) {

		if(m_prev_comp_ptr != NULL) {
			if(byte_offset >= m_prev_comp_ptr->start_bound && 
				byte_offset < m_prev_comp_ptr->end_bound) {

				return m_prev_comp_ptr;
			}
		}

		int hash_div = (int)(byte_offset / (m_comp.BufferSize() * m_hash_block_num));
		SCompBlockPtr *curr_ptr = m_hash_list[hash_div];

		while(curr_ptr != NULL) {
			if(byte_offset >= curr_ptr->start_bound && 
				byte_offset < curr_ptr->end_bound) {

				m_prev_comp_ptr = curr_ptr;
				PromoteCompBlock(curr_ptr);

				return curr_ptr;
			}
			curr_ptr = curr_ptr->next_hash_ptr;
		}

		return NULL;
	}

	// This is the entry function used to retrieve a set of bytes from
	// storage at some offset. The comp block will need to be loaded 
	// into memory if it not already availabe. Also an existing comp 
	// block may need to be evicted if there is not enough memory available.
	// @param byte_offset - this is the byte offset in the comp buffer
	// @param byte_num - this is the number of bytes to retrieve
	// @param buff - this is where all the retrieved bytes are stored 
	void RecurseByteSet(_int64 &byte_offset, int byte_num, char buff[]) {
		
		SCompBlockPtr *curr_ptr = FindCompBlock(byte_offset);

		if(curr_ptr != NULL) {
			// found the block copy accross the bytes
			int offset = (int)(byte_offset - curr_ptr->start_bound);
			int copy_byte_num = curr_ptr->block.OverflowSize() - offset;

			copy_byte_num = min(byte_num, copy_byte_num);
			memcpy(buff, curr_ptr->block.Buffer() + offset, copy_byte_num);

			byte_num -= copy_byte_num;
			buff += copy_byte_num;
			byte_offset += copy_byte_num;
			
			if(byte_num > 0) {
				RecurseByteSet(byte_offset, byte_num, buff);
			} 

			return;
		}

		LoadCompBlock(byte_offset);
		RecurseByteSet(byte_offset, byte_num, buff);
	}

public:

	CHitItemBlock() {
		m_prev_comp_ptr = NULL;
	}

	// This initializes the priority queue for LRU
	static void InitializeLRUQueue() {

		m_session_id = 0;
		m_bytes_loaded = 0;
		m_loaded_comp_block.Initialize(0xFF);

		m_free_list = NULL;
		m_head_cache_ptr = NULL;
		m_tail_cache_ptr = NULL;
	}

	// This is called to setup the hit item block initially
	// @param dir - this is the directory of the comp buffer
	// @param max_byte_size - this is the maximum number of 
	//                      - bytes allowed for the hash header
	void Initialize(const char dir[], int max_byte_size) {

		m_comp.LoadIndex(dir);

		int hash_breadth = max_byte_size / sizeof(SCompBlockPtr *);
		if(hash_breadth >= m_comp.CompBlockNum()) {
			m_hash_list.AllocateMemory(m_comp.CompBlockNum(), NULL);
			m_hash_block_num = 1;
			return;
		}

		m_hash_list.AllocateMemory(hash_breadth, NULL);
		m_hash_block_num = m_comp.CompBlockNum() / hash_breadth;
		if(m_comp.CompBlockNum() % hash_breadth) {
			m_hash_block_num++;
		}
	}

	// This returns a reference to the comp buffer
	inline CCompression &CompBuffer() {
		return m_comp;
	}

	// This starts a new session instance for a query
	static void BeginNewQuery() {
		m_session_id++;
	}

	// This is the entry function used to retrieve a set of bytes from
	// storage at some offset. 
	// @param byte_offset - this is the byte offset in the comp buffer
	// @param byte_num - this is the number of bytes to retrieve
	// @param buff - this is where all the retrieved bytes are stored 
	void RetrieveByteSet(_int64 byte_offset, int byte_num, char buff[]) {

		if(byte_num < 0) {
			throw EIllegalArgumentException("Illegal Byte Num");
		}

		if(byte_offset + byte_num > m_comp.BytesStored()) {
			byte_num = m_comp.BytesStored() - byte_offset;

			if(byte_num < 0) {
				throw EException("Byte Underflow");getchar();
			}
		}
		
		RecurseByteSet(byte_offset, byte_num, buff);
	}

	// This is just a test framework. It creates a series of comp blocks
	// and makes calls to retrieve certain blocks from memory.
	void TestHitItemBlock() {

		CArrayList<char> byte_buff(4);
		m_comp.Initialize("test_comp", 1024);
		char command = 'f';

		for(int i=0; i<40; i++) {
			for(int j=0; j<1024; j++) {
				command = rand() % 256;
				m_comp.AddToBuffer(&command, 1);
				byte_buff.PushBack(command);
			}
		}

		m_comp.FinishCompression();

		InitializeLRUQueue();
		Initialize("test_comp", 10000);

		CMemoryChunk<char> buff(4000);
		for(int i=0; i<1000; i++) {
			int byte_num = rand() % 3096;
			int byte_offset = rand() % m_comp.BytesStored();
			byte_num = min((int)(m_comp.BytesStored() - byte_offset), byte_num);
			RetrieveByteSet(byte_offset, byte_num, buff.Buffer());

			for(int j=byte_offset; j<byte_offset+byte_num; j++) {
				if(buff[j - byte_offset] != byte_buff[j]) {
					cout<<"byte mismatch";getchar();
				}
			}
		}
	}

};
CLinkedBuffer<SCompBlockPtr> CHitItemBlock::m_loaded_comp_block;
SCompBlockPtr *CHitItemBlock::m_free_list;
int CHitItemBlock::m_bytes_loaded;
uLong CHitItemBlock::m_session_id;
SCompBlockPtr *CHitItemBlock::m_head_cache_ptr;
SCompBlockPtr *CHitItemBlock::m_tail_cache_ptr;


// This is simply a bit string that uses a comp buffer
// to store the contents externally in file.
class CBitStringCompression : public CCompression {
	// used to store the current byte
	uChar m_curr_byte;
	// stores the bit offset within the given byte
	int m_bit_offset;
	
public:

	CBitStringCompression() {
		m_bit_offset = 0;
		m_curr_byte = 0;
	}

	CBitStringCompression(const char str[], int buffer_size = 1500000) {
		m_bit_offset = 0;
		m_curr_byte = 0;
		CCompression::Initialize(str, buffer_size); 
	}

	// adds a bit to the string
	// @param value - true or false
	void PushBack(bool value) {
		if(m_bit_offset >= 8) {
			AskBufferOverflow(1);
			HitList().PushBack(m_curr_byte);
			m_curr_byte = 0;
			m_bit_offset = 0;
		}

		m_curr_byte |= value << m_bit_offset++;
	}

	// resets the next bit 
	inline void ResetNextBit() {
		m_bit_offset = 8;
		m_curr_byte = 0;
	}

	// loads the bit string
	inline void LoadIndex(const char str[]) {
		CCompression::LoadIndex(str);
		ResetNextBit();
	}

	// returns the next bit in the string
	bool NextBit() {
		if(m_bit_offset >= 8) {
			char word;
			if(!NextHit(word))throw EOverflowException("No More Bits");
			m_curr_byte = (uChar)word;
			m_bit_offset = 0;
		}

		return (m_curr_byte & (1 << m_bit_offset++)) != 0;
	}

	// just a test framework
	void TestBitStringCompression() {
		CMemoryChunk<bool> check(999999);
		Initialize("test", 100000);

		for(int i=0; i<check.OverflowSize(); i++) {
			int value = rand() % 2;
			check[i] = (value == 1);
			PushBack(check[i]);
		}

		FinishCompression();
		LoadIndex("test");

		for(int i=0; i<check.OverflowSize(); i++) {
			if(NextBit() != check[i]) {
				cout<<"error";getchar();
			}
		}
	}

	// finishes the bit string
	void FinishCompression() {
		if(m_bit_offset > 0 && HitList().OverflowSize() > 0) {
			AskBufferOverflow(1);
			HitList().PushBack(m_curr_byte);
		}

		CCompression::FinishCompression();
	}

	~CBitStringCompression() {
		if(m_bit_offset <= 0 || HitList().OverflowSize() <= 0) {
			return;
		}
		
		AskBufferOverflow(1);
		HitList().PushBack(m_curr_byte);
	}
};

// This is a container class for the Compression class - 
// allows individual bit strings of variable length to be 
// stored and extracted without complicated manipulation on
// singular bytes - Note a compression buffer can be supplied
// or created internally.
class CBitFieldComp {

	// stores a pointer to the comp buffer used to store LocalData
	CCompression *m_comp; 
	// stores the current offset in the byte

	int m_byte_offset; 
	// stores the current loaded byte
	char m_curr_byte; 
	// stores the buffer of the loaded bit stream
	 _int64 m_byte_buffer; 
	// indicates whether a compression buffer
	// has been created for the comp bit field
	CCompression *m_comp_set; 

public:

	CBitFieldComp() {
		m_comp_set = NULL; 
	}

	CBitFieldComp(CCompression &comp) {
		m_comp_set = NULL; 
		Initialize(comp); 
	}

	CBitFieldComp(const char str[]) {
		Initialize(str); 
	}

	void Initialize(const char str[]) {
		m_comp_set = new CCompression; 
		m_comp_set->Initialize(str); 
		Initialize(*m_comp_set); 
	}

	void Initialize(CCompression &comp) {
		m_comp = &comp; 
		comp.ResetNextHitItem(); 
		m_byte_offset = 0; 
		m_byte_buffer = 0; 
		m_curr_byte = 0; 
	}

	// loads the contents of the bit stream
	void LoadBitStream(const char str[]) {
		m_comp_set = new CCompression; 
		m_comp = m_comp_set; 
		m_comp->LoadIndex(str); 
		ResetNextBitField(); 
	}

	// returns an integer corresponding to the bit field
	// of size given by bit size
	 _int64 & GetNextBitField(int bit_size) {
		m_byte_buffer = 0; 
		GetNextBitField((char *)&m_byte_buffer, bit_size); 
		return m_byte_buffer; 
	}

	// returns an integer corresponding to the bit field
	// of size given by bit size - takes a buffer
	// of characters to store the bit stream
	// assumes the bytes retrieved will be no greater than 50
	// @param buffer - a buffer to which the results will be copied
	// @param bit_size - the number of bits that are to be retrieved
	// @param offset - the offset within the buffer to which the 
	// 				results will be appended (measured in bits)
	void GetNextBitField(char buffer[], int bit_size, int offset) {
		static CMemoryChunk<char> copy1(50); 
		static CMemoryChunk<char> copy2(50); 
		int shift = offset >> 3; 
		int bit_space = offset % 8; 

		if(bit_space)
			memcpy(copy2.Buffer(), buffer, shift + 1); 
		else 
			memcpy(copy2.Buffer(), buffer, shift); 

		GetNextBitField(copy1.Buffer(), bit_size); 
		ShiftBufferLeft(copy1.Buffer(), buffer, bit_size, offset); 
		memcpy(buffer, copy2.Buffer(), shift); 

		copy2[shift] &= (uChar)0xFF >> (8 - bit_space); 
		buffer[shift] |= copy2[shift]; 
	}

	// returns an integer corresponding to the bit field
	// of size given by bit size - takes a buffer
	// of characters to store the bit stream
	void GetNextBitField(char buffer[], int bit_size) {

		int offset = 0; 
		// first retrieve blocks of 8 - bits
		while(bit_size >= 8) {
			bit_size -= 8; 
			if(!m_byte_offset) {
				if(!m_comp->NextHit(m_curr_byte))
					throw EOverflowException("Nothing More Stored"); 
				// assigns straight to the byte if the recieved byte is aligned
				buffer[offset++] = (uChar)m_curr_byte; 
				continue; 
			}
			// assigns the lower part of the byte
			buffer[offset] = (uChar)m_curr_byte >> m_byte_offset; 

			if(!m_comp->NextHit(m_curr_byte))
				throw EOverflowException("Nothing More Stored"); 
			// assigns the upper part of the byte
			buffer[offset++] |= (uChar)m_curr_byte << (8 - m_byte_offset); 
		}

		if(bit_size <= 0)return; 

		if(bit_size + m_byte_offset > 8) {
			// the rest of the bit field overflows into the next byte
			buffer[offset] = (uChar)m_curr_byte >> m_byte_offset; 

			if(!m_comp->NextHit(m_curr_byte))
				throw EOverflowException("Nothing More Stored"); 

			bit_size -= 8 - m_byte_offset; 
			// removes any excess from the start of the byte that 
			// is not part of the current byte
			const uChar bit_mask = 0xFF >> (8 - bit_size); 
			buffer[offset] |= ((uChar)m_curr_byte & bit_mask) << (8 - m_byte_offset); 
			// assigns the next time byte offset based on the left over bit size
			m_byte_offset = bit_size; 
		} else {
			// the rest of the bit field is contained in the current byte
			if(!m_byte_offset) {
				if(!m_comp->NextHit(m_curr_byte))
					throw EOverflowException("Nothing More Stored"); 
			}
			// removes any excess from the start of the byte that 
			// is not part of the current byte
			const uChar bit_mask = 0xFF >> (8 - bit_size - m_byte_offset); 
			buffer[offset] = ((uChar)m_curr_byte & bit_mask) >> m_byte_offset; 
			// assigns the next time byte offset based on the left over bit size
			m_byte_offset += bit_size; 
			if(m_byte_offset >= 8)m_byte_offset = 0; 
		}
	}

	// copies an input buffer shifted some number of bits left
	// into an output buffer where the bytes copied depends on the 
	// shift size and bit field size
	// 
	// @param input_buffer - the buffer frow which to copy from
	// @param output_buffer - the buffer which to copy the shifted input
	// @param input_bit_size - the number of bits to copy across from the
	// 						the input buffer
	// @param shift_size - the the number of bits the input buffer is shifted left
	static void ShiftBufferLeft(char buffer[], int bit_size, int shift_size) {

		ShiftBufferLeft(buffer, CUtility::TempBuffer(), bit_size, shift_size); 
		memcpy(buffer, CUtility::TempBuffer(), 
			CUtility::ByteCount(bit_size + shift_size)); 
	}

	// copies an input buffer shifted some number of bits left
	// into an output buffer where the bytes copied depends on the 
	// shift size and bit field size
	// 
	// @param input_buffer - the buffer frow which to copy from
	// @param output_buffer - the buffer which to copy the shifted input
	// @param input_bit_size - the number of bits to copy across from the
	// 						the input buffer
	// @param shift_size - the the number of bits the input buffer is shifted left
	static void ShiftBufferLeft(char input_buffer[], char output_buffer[], 
		int bit_size, int shift_size) {

		if(shift_size <= 0)return; 
		int curr_byte = shift_size >> 3; 
		int byte_offset = shift_size % 8; 
		for(int i=0; i<curr_byte + 1; i++) 
			output_buffer[i] = 0; 

		output_buffer[curr_byte++] = (uChar)*input_buffer << byte_offset; 
		bit_size -= (8 - byte_offset); 

		// first retrieve blocks of 8 - bits
		while(bit_size >= 8) {
			bit_size -= 8; 
			if(!byte_offset) {
				// assigns straight to the byte if the recieved byte is aligned
				output_buffer[curr_byte++] = (uChar)*(++input_buffer); 
				continue; 
			}
			// assigns the lower part of the byte
			output_buffer[curr_byte] = (uChar)*input_buffer >> (8 - byte_offset); 
			input_buffer++; 
			// assigns the upper part of the byte
			output_buffer[curr_byte++] |= (uChar)*input_buffer << byte_offset; 
		}

		if(bit_size <= 0)return; 

		if(bit_size > byte_offset) {
			// the rest of the bit field overflows into the next byte
			output_buffer[curr_byte] = (uChar)*input_buffer >> (8 - byte_offset); 
			input_buffer++; 

			bit_size -= byte_offset; 
			// removes any excess from the start of the byte that 
			// is not part of the current byte
			const uChar bit_mask = 0xFF >> (8 - bit_size); 
			output_buffer[curr_byte] |= ((uChar)*input_buffer & bit_mask) << byte_offset; 
		} else {
			if(!byte_offset)input_buffer++; 
			// the rest of the bit field is contained in the current byte
			// removes any excess from the start of the byte that 
			// is not part of the current byte
			const uChar bit_mask = 0xFF >> (8 - bit_size); 
			output_buffer[curr_byte] = (((uChar)*input_buffer) >> (8 - byte_offset)) & bit_mask; 
		}
	}

	// copies an input buffer shifted some number of bits right
	// into an output buffer where the bytes copied depends on the 
	// shift size and bit field size
	// 
	// @param input_buffer - the buffer frow which to copy from
	// @param output_buffer - the buffer which to copy the shifted input
	// @param input_bit_size - the number of bits to copy across from the
	// 						the input buffer
	// @param shift_size - the the number of bits the input buffer is shifted right
	static void ShiftBufferRight(char buffer[], int bit_size, int shift_size) {

		ShiftBufferRight(buffer, CUtility::TempBuffer(), bit_size, shift_size); 
		memcpy(buffer, CUtility::TempBuffer(), 
			CUtility::ByteCount(bit_size - shift_size)); 
	}

	// copies an input buffer shifted some number of bits right
	// into an output buffer where the bytes copied depends on the 
	// shift size and bit field size
	// 
	// @param input_buffer - the buffer frow which to copy from
	// @param output_buffer - the buffer which to copy the shifted input
	// @param input_bit_size - the number of bits to copy across from the
	// 						the input buffer
	// @param shift_size - the the number of bits the input buffer is shifted right
	static void ShiftBufferRight(char input_buffer[], char output_buffer[], 
		int bit_size, int shift_size) {

		if(shift_size <= 0)return; 
		int curr_byte = shift_size >> 3; 
		int byte_offset = shift_size % 8; 
		input_buffer += curr_byte; 

		// first retrieve blocks of 8 - bits
		while(bit_size >= 8) {
			bit_size -= 8; 
			if(!byte_offset) {
				// assigns straight to the byte if the recieved byte is aligned
				 * output_buffer++= (uChar)*input_buffer++; 
				continue; 
			}
			// assigns the lower part of the byte
			 * output_buffer = (uChar)*input_buffer >> byte_offset; 
			input_buffer++; 

			// assigns the upper part of the byte
			 * output_buffer++ |= (uChar)*input_buffer << (8 - byte_offset); 
		}

		if(bit_size <= 0)return; 

		if(bit_size + byte_offset > 8) {
			// the rest of the bit field overflows into the next byte
			 * output_buffer = (uChar)*input_buffer >> byte_offset; 
			input_buffer++; 

			bit_size -= 8 - byte_offset; 
			// removes any excess from the start of the byte that 
			// is not part of the current byte
			const uChar bit_mask = 0xFF >> (8 - bit_size); 
			 * output_buffer |= ((uChar)*input_buffer & bit_mask) << (8 - byte_offset); 
		} else {
			// the rest of the bit field is contained in the current byte
			if(!byte_offset)input_buffer++; 
			// removes any excess from the start of the byte that 
			// is not part of the current byte
			const uChar bit_mask = 0xFF >> (8 - bit_size - byte_offset); 
			 * output_buffer = ((uChar)*input_buffer & bit_mask) >> byte_offset; 
		}
	}

	// used to bitwise OR two buffers together the result is 
	// put in the second buffer
	// @param buffer1 - the first buffer that is to take part in the bitwise OR
	// @pamam buffer2 - the second buffer that is to take part in the bitwise OR
	// @param size - the number of bytes that take part in the bitwise OR
	inline static void BitwiseOR(char buffer1[], char buffer2[], int size) {
		for(int i=0; i<size; i++)
			buffer2[i] |= buffer1[i]; 
	}

	// Concantanates buffer1 onto the end of buffer2. Assumes that buffer1 has
	// been shifted left (8 % offset) so all bits left of this are zero and
	// buffer1 is already aligned to the start of the bit offset. For example
	// to concantanate 3 onto the end with an offset of 11 -> 3 would need to be
	// shifted left (8 % 11) to give 11000b.
	// @param buffer1 - the first buffer that is to take part in the bitwise OR
	// @pamam buffer2 - the second buffer that is to take part in the bitwise OR
	// @param bit_size - the number of bits from buffer 1 that need to be concanated
	// @param offset - the bit offset from the start of buffer2 from which to 
	// 				 - concantanate buffer1
	static void ConcantanateBuffer(char buffer1[], char buffer2[], 
		int bit_size, int offset) {

		int byte_offset = offset >> 3; 
		int left_over = offset % 8; 

		if(left_over) {
			char prev_byte = buffer2[byte_offset];
			buffer2[byte_offset] = 0;
			// first clears the bits in the bytes greater than the byte offset
			buffer2[byte_offset] |= prev_byte & (0xFF >> (8 - left_over));
			// assumes that buffer1 has already been shifted left_over
			// bits to the left (done for speed - have to shift left otherwise)
			buffer2[byte_offset] |= *buffer1; 
			bit_size -= 8 - left_over; 
			if(bit_size <= 0)return; 

			byte_offset++; 
			memcpy(buffer2 + byte_offset, buffer1 + 1, 
				CUtility::ByteCount(bit_size)); 
			return; 
		}

		memcpy(buffer2 + byte_offset, buffer1, 
			CUtility::ByteCount(bit_size)); 
	}

	// just a test frame work
	void TestShiftBuffer() {
		char in_buffer[12]; 
		char out_buffer[40]; 
		char fin_buffer[12]; 
		for(int i=0; i<12; i++) {
			in_buffer[i] = rand(); 
		}

		for(int i=0; i<40; i++) {
			out_buffer[i] = 0; 
		}

		for(int i=0; i<99999; i++) {
			for(int c=0; c<12; c++) {
				in_buffer[c] = rand(); 
			}
			int offset = (rand() % 24) + 1; 
			ShiftBufferLeft(in_buffer, out_buffer, 96, offset); 
			// CUtility::PrintBitStream(out_buffer, 6); cout << endl; 
			ShiftBufferRight(out_buffer, fin_buffer, 96, offset); 
			// CUtility::PrintBitStream(fin_buffer, 6); cout << endl << endl; 
			for(int j = 0; j < 12; j++) {
				if(in_buffer[j] != fin_buffer[j]) {
					cout << "error "<< i; 
					getchar(); 
				}
			}
		}
	}

	// resets the next bit stream from the buffer
	void ResetNextBitField() {
		m_byte_offset = 0; 
		m_byte_buffer = 0; 
		m_curr_byte = 0; 
		m_comp->ResetNextHitItem(); 
	}

	// resets the entire bit field comp - erases
	// all previous content
	void ResetBitFieldComp() {
		ResetNextBitField(); 
	}

	// adds an integer of a particular bit size to the comp buffer
	template <class X> int AddBitField(X item) {
		int bit_size = CMath::LogBase2((int)item); 
		AddBitField(item, bit_size); 
		return bit_size; 
	}

	// returns an integer that can be found in a buffer at
	// a given offset
	// @param buffer - the buffer from which to extract the integer
	// @param offset - the number of bits shifted left in the buffer
	// @param bit_size - the number of bits for which to extract (expects < 32)
	static uLong ExtractIntegerFromBuffer(char buffer[], int offset, int bit_size) {
		int shift = offset >> 3; 
		buffer += shift; 
		const uLong bit_mask = 0xFFFFFFFF; 
		uLong value = (uLong)((*(_int64 * )buffer) >> (offset % 8)); 
		return value & (bit_mask >> (32 - bit_size)); 
	}

	// adds an integer of a particular bit size to the comp buffer
	template <class X> void AddBitField(X item, int bit_size) {
		AddBitField((char *)&item, bit_size); 
	}

	// adds an array of characters to the comp buffer
	void AddBitField(char buffer[], int bit_size) {
		if(bit_size <= 0)return; 

		int offset = 0; 
		m_curr_byte |= (uChar)buffer[offset] << m_byte_offset; 
		bit_size -= 8 - m_byte_offset; 
		if(bit_size < 0) {
			m_byte_offset = 8 + bit_size; 
			// used to ensure no extra bits have been appended
			m_curr_byte &= 0xFF >> (8 - m_byte_offset); 
			return; 
		}

		m_comp->AskBufferOverflow((bit_size >> 3) + 1); 
		m_comp->HitList().PushBack(m_curr_byte); 

		while(bit_size >= 8) {
			bit_size -= 8; 
			if(!m_byte_offset) {
				m_comp->HitList().PushBack(buffer[++offset]); 
				continue; 
			}
			m_curr_byte = (uChar)buffer[offset++] >> (8 - m_byte_offset); 
			m_curr_byte |= (uChar)buffer[offset] << m_byte_offset; 
			m_comp->HitList().PushBack(m_curr_byte); 
		}

		m_curr_byte = (uChar)buffer[offset++] >> (8 - m_byte_offset); 
		m_curr_byte |= (uChar)buffer[offset] << m_byte_offset; 
		// used to ensure no extra bits have been appended
		m_curr_byte &= 0xFF >> (8 - bit_size); 
		m_byte_offset = bit_size; 
	}

	// gets an escaped two byte integer
	bool GetEscapedItem(uLong &item) {

		item = 0; 
		int offset = 0; 
		char hit; 

		while(true) {
			GetNextBitField(&hit, 8); 
			item |= (uLong)(((uChar)hit & 0x7F) << offset); 
			// checks if finished
			if(!((uChar)hit & 0x80))return true; 
			offset += 7; 
		}

		return true; 
	}

	// adds a four byte escaped integer
	template <class X> void AddEscapedItem(X item) {
		if(item < 0)throw EIllegalArgumentException("item < 0"); 

		while(item >= 128) {
			AddBitField((uChar)(item|0x80), 8); 
			item >>= 7; 
		}

		AddBitField((uChar)item, 8); 
	}

	// gets the coding associated with an
	// escaped four byte integer - returns
	// the size of the integer in bytes
	int GetEscapedEncoding(char buffer[]) {
		int byte_count = 1; 

		while(true) {
			GetNextBitField(buffer, 8); 
			// checks if finished
			if(!((uChar)*buffer & 0x80))return byte_count; 
			byte_count++; 
			buffer++; 
		}
		return byte_count; 
	}

	// just a test framework 
	void TestEscapedEncoding() {
		Initialize("test"); 
		ResetBitFieldComp(); 
		CMemoryChunk<_int64> buffer(99999); 
		CMemoryChunk<uLong> spacing(99999); 
		char temp[20]; 

		for(int i=0; i<buffer.OverflowSize(); i++) {
			buffer[i] = (_int64)rand() << 31; 
			spacing[i] = (rand() % 30) + 7; 
			AddBitField(temp, spacing[i]); 
			AddEscapedItem(buffer[i]); 
		}

		FinishBitStream(); 
		ResetNextBitField(); 

		for(int i=0; i<buffer.OverflowSize(); i++) {
			GetNextBitField(temp, spacing[i]); 
			GetEscapedEncoding(temp); 
			char *ge = temp; 
			_int64 compare = 0; 
			CArray<char>::GetEscapedItem(ge, (char *)&compare); 
			if(buffer[i] != compare) {
				cout << "Error "<< i <<" "<< buffer[i] <<" "<<compare<<" "<< spacing[i]; 
				getchar(); 
			}
		}
	}

	// finishes writing the bit stream to the comp buffer
	void FinishBitStream() {
		if(m_byte_offset) {
			m_comp->AskBufferOverflow(1); 
			m_comp->HitList().PushBack(m_curr_byte); 
		}
		m_comp->FinishCompression(); 
	}

	// returns a reference to the comp buffer
	inline CCompression &CompBuffer() {
		return *m_comp; 
	}

	// just a test framework
	void TestBitField() {
		CCompression comp; 
		comp.RemoveCompression("Test"); 
		comp.Initialize("Test"); 
		Initialize(comp); 

		CVector<bool> bit_stream; 
		CVector<int> bit_start; 
		bit_start.PushBack(0); 

		for(int i=0; i<9999; i++) {
			int length = (rand() % 63) + 1; 
			 _int64 item = 0; 
			for(int c=0; c<length; c++) {
				int value = (rand() % 2); 
				bit_stream.PushBack(value == 1); 
				item |= (_int64)value << c; 
			}

			AddBitField(item, length); // getchar(); 
			bit_start.PushBack(bit_stream.Size());
			
		}

		FinishBitStream(); 
		ResetNextBitField(); 

		for(int i=0; i<bit_start.Size() - 1; i++) {
			_int64 next = GetNextBitField(bit_start[i+1] - bit_start[i]); 
			int offset = 0; 
			for(int c=bit_start[i]; c<bit_start[i+1]; c++) {
				if((next & 1) != bit_stream[c]) {
					cout << endl; 
					cout << (int)(bit_start[i+1] - bit_start[i]) << endl; 
					cout << "Error "<< i <<" "<< offset << " Length "
						<< (bit_start[i+1] - bit_start[i]); getchar(); 
				}
				offset++; 
				next >>= 1; 
			}
		}

		comp.RemoveCompression("Test"); 
	}

	~CBitFieldComp() {
		// removes the memory associated with the comp buffer
		if(m_comp_set != NULL) {
			delete m_comp_set; 
		}
	}

}; 

// This is a comp buffer whoese interface is identical to the File class 
// interface. It's designed to replace an existing File implementation by
// just changing the File to CFileComp. The class itself doesn't implement
// any new functionality, it just changes the interface using an adapter pattern.
class CFileComp {
	// This is the comp buffer
	CCompression m_comp;
	// This stores the directory of the file comp -> is necessary
	char m_directory[500];

public:

	CFileComp() {
	}

	// @param dir - this stores the directory where the file is 
	//            - to be stored, if this is NULL then it will just
	//            - use the previously specified directory
	CFileComp(const char dir[]) {
		SetFileName(dir);
	}

	// This opens a write file, it just stores the directory of the file
	// @param dir - this stores the directory where the file is 
	//            - to be stored, if this is NULL then it will just
	//            - use the previously specified directory
	// @param buff_size - this is the number of bytes to use to store
	//                  - a single compression block
	void OpenWriteFile(const char dir[] = NULL, int buff_size = 1500000) {
		if(dir == NULL) {
			dir = m_comp.DirectoryName();
		}

		strcpy(m_directory, dir);
		m_comp.Initialize(m_directory, buff_size);
	}

	// This opens a read file, it closes any previous files that 
	// were opened before reading from a new file.
	// @param dir - this stores the directory where the file is 
	//            - currently stored, if this is NULL then it will just
	//            - use the previously specified directory
	void OpenReadFile(const char dir[] = NULL) {
		if(dir == NULL) {
			dir = m_comp.DirectoryName();
		}

		strcpy(m_directory, dir);
		CloseFile();
		m_comp.LoadIndex(m_directory);
	}

	// This just sets the comp block buffer size
	inline void InitializeCompression(int buff_size = 1500000) {
		m_comp.SetBufferSize(buff_size);
	}

	// seeks a number of bytes from the beginning of the file
	void SeekReadFileFromBeginning(_int64 offset) {
		m_comp.SetCurrentHitPosition(offset);
	}

	// This returns a reference to the comp buffer
	inline CCompression &CompBuffer() {
		return m_comp;
	}

	// This flushes the buffer if the current buffer size 
	// plus the additional size exceeds the total capacity
	inline void AskBufferOverflow(int buff_size) {
		m_comp.AskBufferOverflow(buff_size);
	}

	// This returns the percent of the file read
	inline float PercentageFileRead() {
		return m_comp.PercentageHitsRead();
	}

	// Writes a single compressoin object of type X
	// @param object - the object of type X to write
	template <class X> inline void WriteCompObject(X object) {
		m_comp.AddToBuffer((char *)&object, sizeof(X));
	}

	// Writes a buffer of type X items size times
	// @param size - the number of items to write
	// @param object - a buffer containing the items that need to be writted
	template <class X> inline void WriteCompObject(X object[], int size) {
		m_comp.AddToBuffer((char *)object, sizeof(X) * size);
	}

	// This resests the readfile at the beginning
	inline void ResetReadFile() {
		m_comp.ResetNextHitItem();
	}

	// Writes a buffer of type X items size times
	// @param size - the number of items to write
	// @param object - a buffer containing the items that need to be writted
	template <class X> inline void WriteCompObject(char object[], int size) {
		m_comp.AddToBuffer(object, size);
	}

	// Reads a compression object of type X
	// @param object - the object of type X to write
	// @return false if the object could not be retrieved (end of file)
	template <class X> inline bool ReadCompObject(X &object) {
		return m_comp.NextHit((char *)&object, sizeof(X));
	}

	// Retrieves next 5-bytes and places in a 8-byte integer
	// @param item - used to store the 8-byte item
	// @return true if more bytes available, false otherwise
	inline bool Get5ByteCompItem(_int64 &item) {
		item = 0; 
		return ReadCompObject((char *)&item, 5); 
	}

	// Reads a buffer of compression objects of type X size times
	// @param object - a buffer containing the items that need to be writted
	// @return false if the object could not be retrieved (end of file)
	template <class X> inline bool ReadCompObject(X object[], int size) {
		return m_comp.NextHit((char *)object, sizeof(X) * size);
	}

	// Reads a buffer of compression objects of type X size times
	// @param object - a buffer containing the items that need to be writted
	// @return false if the object could not be retrieved (end of file)
	template <class X> inline bool ReadCompObject(char object[], int size) {
		return m_comp.NextHit(object, size);
	}

	// Adds a X byte escaped integer
	// @return the number of bytes added
	template <class X> int AddEscapedItem(X item) {
		return m_comp.AddEscapedItem(item);
	}

	// Gets an escaped n-byte integer and places
	// the results in the buffer
	// @returns the number of bytes used to store it
	inline int GetEscapedItem(_int64 &item) {
		return m_comp.GetEscapedItem(item);
	}

	// Gets an escaped n-byte integer and places
	// the results in the buffer
	// @returns the number of bytes used to store it
	inline int GetEscapedItem(S5Byte &item) {
		return m_comp.GetEscapedItem(item);
	}

	// Gets an escaped four byte integer
	// @returns the number of bytes used to store it
	inline int GetEscapedItem(uLong &item) {
		return m_comp.GetEscapedItem(item);
	}

	// This returns the escaped encoding from the comp buffer.
	// @param buffer - this is used to store the contents of the
	//               - escaped integer
	inline int GetEscapedEncoding(char buffer[]) {
		return m_comp.GetEscapedEncoding(buffer);
	}

	// This removes a file - closes if necessary
	inline void RemoveFile() {
		m_comp.RemoveCompression();
	}

	// Returns the reference name of the file
	inline char *GetFileName() {
		return m_comp.DirectoryName();
	}


	// This sets the reference name for the file
	// @param str - a buffer containing the name of the file
	inline void SetFileName(const char str[]) {
		m_comp.SetDirectoryName(str);
		strcpy(m_directory, str);
	}

	// This replaces one file with another and stores
	// the new file under the name of the old file
	// @param replace_file - the file that is replacing the 
	//                     - instance of this file 
	//                     - this will be renamed to this instance
	//                     - directory name
	inline void SubsumeFile(CFileComp &replace_file) {
		replace_file.CloseFile();
		CCompression::ReplaceCompression(replace_file.m_comp, m_comp);
	}

	// This closes the file 
	inline void CloseFile() {
		m_comp.FinishCompression();
	}

	~CFileComp() {
		CloseFile();
	}
};

// This is a external file storage class used for storing webpages - 
// includes a time stamp for each webpage - main features include
// buffering the webpage and writing out to disk using the CCompression class - 
// allows a single document to be retreived using a simpled index to lookup
// the document - this class is thread safe - this class is used in conjunction
// with the search engine spider
class CFileStorage {

	// used to store the document
	CCompression m_doc_comp; 
	// used to store the document lookup indexing 
	// information - comp offset 
	CCompression m_lookup_comp; 

	// gets the system time of when the document was indexed
	// and adds it to the start of the document
	// @param url_length - the length of the url
	// @param document_length - the length of the document
	void AddTimeStamp(int url_length, int document_length) {
		m_doc_comp.AddToBuffer((char *)&url_length, sizeof(short));
		m_doc_comp.AddToBuffer((char *)&document_length, sizeof(int));
	}

	// stores the compression block information offset after 
	// each document has been added to the file storage
	void IndexLookupInfo() {
		m_lookup_comp.AskBufferOverflow(6); 
		m_lookup_comp.AddHitItem3Bytes(m_doc_comp.CompBlockNum()); 
		m_lookup_comp.AddHitItem3Bytes(m_doc_comp.HitList().Size()); 
	}

protected:

	// Used to store the size of the lookup
	// comp buffer this must be constant to 
	// correctly index into the lookup comp
	static const int LOOKUP_SIZE = 0x10000;
	// used to store the size of the document
	// comp buffer - must be constant
	static const int DOCUMENT_SIZE = 5000000;

public:

	CFileStorage() {
	}

	// This creates a document set
	// @param dir - this is a ptr to the desired directory
	void Initialize(const char dir[]) {

		strcpy(CUtility::SecondTempBuffer(), dir); 
		strcat(CUtility::SecondTempBuffer(), ".file_system"); 

		m_lookup_comp.Initialize(CUtility::ExtendString
			(CUtility::SecondTempBuffer(), ".lookup"), LOOKUP_SIZE * 6); 

		m_doc_comp.Initialize(CUtility::ExtendString
			(CUtility::SecondTempBuffer(), ".document"), DOCUMENT_SIZE);

		IndexLookupInfo();
	}

	// This returns the lookup byte size
	static int LookupByteSize() {
		return LOOKUP_SIZE;
	}

	// This returns the lookup byte size
	static int DocumentByteSize() {
		return DOCUMENT_SIZE;
	}

	// Loads the index into memory, used when trying to retrieve a document
	void LoadDocumentIndex(const char str[]) {

		char buff[4096];
		strcpy(buff, str); 
		strcat(buff, ".file_system"); 

		m_lookup_comp.LoadIndex(CUtility::ExtendString(buff, ".lookup"));
		m_doc_comp.LoadIndex(CUtility::ExtendString(buff, ".document")); 
	}

	// Extracts the document url
	// @param document - the document buffer
	// @param url - buffer to store the url
	// @return the length of the url, -1 if an error occured
	static int ExtractURL(char document[], CMemoryChunk<char> &url_buff) {

		int length = 0; 
		memcpy((char *)&length, document, sizeof(short));
		if(length > url_buff.OverflowSize()) {
			url_buff.AllocateMemory(length);
		}

		memcpy(url_buff.Buffer(), document + sizeof(short) + sizeof(int), length);

		return length; 
	}

	// This returns the offset of the beginning of the document removing the 
	// length encoding and the url
	static int DocumentBegOffset(const char document[]) {

		int url_len = 0;
		int doc_len = 0;
		memcpy((char *)&url_len, document, sizeof(short));
		memcpy((char *)&doc_len, document + sizeof(short), sizeof(int));

		return url_len + sizeof(short) + sizeof(int);
	}

	// adds a current document to the collection
	void AddDocument(CLinkedBuffer<char> &document, 
		const char url[], int url_length) { 

		// adds the base url to the start of the file
		AddTimeStamp(url_length, document.Size());
		m_doc_comp.AddToBuffer(url, url_length); 
		m_doc_comp.AddToBuffer(document); 

		IndexLookupInfo(); 
	}

	// adds a current document to the collection
	void AddDocument(const char document[], int doc_length, 
		const char url[], int url_length) { 

		// adds the base url to the start of the file
		AddTimeStamp(url_length, doc_length);
		m_doc_comp.AddToBuffer(url, url_length); 
		m_doc_comp.AddToBuffer(document, doc_length); 

		IndexLookupInfo(); 
	}

	// removes the files associated with file storage
	void RemoveFileStorage(const char str[]) {
		strcpy(CUtility::SecondTempBuffer(), str); 
		strcat(CUtility::SecondTempBuffer(), ".file_system"); 
		remove(CUtility::ExtendString(CUtility::SecondTempBuffer(), ".size")); 

		m_doc_comp.RemoveCompression(CUtility::
			ExtendString(CUtility::SecondTempBuffer(), ".document")); 
		m_lookup_comp.RemoveCompression(CUtility::
			ExtendString(CUtility::SecondTempBuffer(), ".lookup")); 
	}

	// calculates the size of a sequential document 
	// @param document - buffer containing at least the 
	//                 - the header information of the document
	// @return the total size of the document in bytes
	static int CalculateDocumentSize(char document[]) {

		int url_len = 0;
		int doc_len = 0;
		memcpy((char *)&url_len, document, sizeof(short));
		memcpy((char *)&doc_len, document + sizeof(short), sizeof(int));

		return url_len + doc_len + sizeof(short) + sizeof(int);
	}

	// retrieves a document from the collection
	bool GetDocument(CMemoryChunk<char> &document, 
		const _int64 & index, int document_num = 1) {

		if(index >= DocumentCount()) {
			return false; 
		}

		if(index + document_num >= DocumentCount()) {
			document_num = (int)(DocumentCount() - index); 
		}

		SHitPosition l_start_pos((int)(index % LOOKUP_SIZE) * 6, (int)(index / LOOKUP_SIZE)); 
		_int64 next_index = index + document_num; 
		SHitPosition l_end_pos((int)(next_index % LOOKUP_SIZE) * 6, (int)(next_index / LOOKUP_SIZE)); 
 
		m_lookup_comp.SetCurrentHitPosition(l_start_pos); 

		SHitPosition d_start_pos, d_end_pos; 
		m_lookup_comp.NextHit((char *)&d_start_pos.comp_block, 3); 
		m_lookup_comp.NextHit((char *)&d_start_pos.hit_offset, 3); 

		m_lookup_comp.SetCurrentHitPosition(l_end_pos); 
		m_lookup_comp.NextHit((char *)&d_end_pos.comp_block, 3); 
		m_lookup_comp.NextHit((char *)&d_end_pos.hit_offset, 3); 

		int bytes = SHitPosition::ByteDiff(d_end_pos, d_start_pos, m_doc_comp.BufferSize());
		document.AllocateMemory(bytes); 

		m_doc_comp.SetCurrentHitPosition(d_start_pos); 
		m_doc_comp.NextHit(document.Buffer(), bytes, 1); 

		return true; 
	}

	// Returns the number of documents stored
	inline _int64 DocumentCount() {
		return (m_lookup_comp.BytesStored() - 6) / 6;
	}

	// Just a test framework
	void TestFileStoage() {
		/////////////////////////////////////////////
		// note must remove all image and time stamp
		// information for this test to work
		//////////////////////////////////////////// 

		CVector<char> document; 
		CVector<int> document_start; 
		document_start.PushBack(0); 
		CLinkedBuffer<char> image_list; 
		RemoveFileStorage("TestSet"); 

		Initialize("TestSet"); 

		for(int i=0; i<600; i++) {
			char check = rand(); 
			int length = (int)(rand() % 2000) + 30; 
			CArray<char> sub_document(length); 
			for(int c=0; c<length; c++) {
				document.PushBack(check); 
				sub_document.PushBack(check); 
			}
			document_start.PushBack(document.Size()); 
			AddDocument(sub_document.Buffer(), sub_document.Size(), NULL, 0); 
			if(!(i % 100) && i > 99)FinishFileStorage(); 
		}
		FinishFileStorage(); 
		LoadDocumentIndex("TestSet"); 

		for(_int64 i=0; i<600; i += 1) {
			CMemoryChunk<char> doc; 
			GetDocument(doc, i, 1); 
			if(doc.OverflowSize() != (document_start[(int)i + 1] - document_start[(int)i])) {
				cout << "error - Size Don't Match "<<i<<" "<<doc.OverflowSize()
					<<" "<<(document_start[(int)i + 1] - document_start[(int)i]); 
				getchar(); 
			}
			int offset = document_start[(int)i]; 
			for(int c=0; c<doc.OverflowSize(); c++) {
				if(doc[c] != document[offset++]) {
					cout << "error - No Match "<< c; 
					getchar(); 
				}
			}
		}

		RemoveFileStorage("TestSet"); 

	}

	// called once after all indexing has been finished
	void FinishFileStorage(const char str[] = NULL) {
		m_lookup_comp.FinishCompression(); 
		m_doc_comp.FinishCompression(); 
	}
}; 

// This version of FileStorage utilizes a caching mechanism where the document 
// indedx and docuement set are cached. This is used to speed up the retrieval 
// process in relation to retrieving documents.
class CFileStorageCache {

	// Used to cache the document set
	CHitItemBlock m_doc_cache; 
	// Used to cache the lookup cache
	CHitItemBlock m_lookup_cache; 

public:

	CFileStorageCache() {
	}

	// This creates a document set
	// @param dir - this is a ptr to the desired directory
	void Initialize(const char dir[]) {

		m_lookup_cache.Initialize(CUtility::ExtendString
			(dir, ".file_system.lookup"), 1000000); 

		m_doc_cache.Initialize(CUtility::ExtendString
			(dir, ".file_system.document"), 10000000);
	}

	// Returns the number of documents stored
	inline _int64 DocumentCount() {
		return (m_lookup_cache.CompBuffer().BytesStored() - 6) / 6;
	}

	// Retrieves a document from the collection
	// @param document - this is a buffer used to store a document
	// @param index - this is the document index
	// @param document_num - this is the number of documents to retrieve
	// @return false if the document cannot be retrieved
	bool GetDocument(CMemoryChunk<char> &document, 
		const _int64 & index, int document_num = 1) {

		if(index >= DocumentCount()) {
			return false; 
		}

		if(index + document_num >= DocumentCount()) {
			document_num = (int)(DocumentCount() - index); 
		}

		SHitPosition d_start_pos, d_end_pos; 
		m_lookup_cache.RetrieveByteSet(index * 6, 6, CUtility::SecondTempBuffer());
		memcpy((char *)&d_start_pos.comp_block, CUtility::SecondTempBuffer(), 3);
		memcpy((char *)&d_start_pos.hit_offset, CUtility::SecondTempBuffer() + 3, 3);

		m_lookup_cache.RetrieveByteSet((index + 1) * 6, 6, CUtility::SecondTempBuffer());
		memcpy((char *)&d_end_pos.comp_block, CUtility::SecondTempBuffer(), 3);
		memcpy((char *)&d_end_pos.hit_offset, CUtility::SecondTempBuffer() + 3, 3);
		
		_int64 buff_size = m_doc_cache.CompBuffer().BufferSize();
		_int64 byte_start = buff_size * d_start_pos.comp_block;
		byte_start += d_start_pos.hit_offset;

		_int64 byte_end = buff_size * d_end_pos.comp_block;
		byte_end += d_end_pos.hit_offset;

		document.AllocateMemory((int)(byte_end - byte_start));
		m_doc_cache.RetrieveByteSet(byte_start, document.OverflowSize(), document.Buffer());
 
		return true; 

	}
};

// This class is used by multiple sub classes to perform an external
// merge of sorted blocks. That is blocks of sorted items need to 
// be merged together to create a single sorted block. This is done
// using a priority queue with multiple buckets loaded into memory
// at any one time. Two sequential buckets are loaded into memory 
// for a given bucket division at any one time. This is to allow 
// a load thread to operate in parallel in order to load the next
// sequential bucket into memory while the current block is being
// merged. This helps to reduce the overhead of disk IO.

// Also due to the possibility that not all sorted blocks can be 
// merged together in a single pass it may be necessary to peform
// multiple merge passes. On each pass a larger sorted blocks is
// constructed. The final pass will merge all sorted blocks which
// they themselves may have been created from multiple merges.
class CExternalMerge : public CSort <SExternalSort> { 

	protected:
	// defines the size of a disk block
	static const int DISK_BLOCK = 512;
	// defines the maximum amount of memory that 
	// can be utilized for sorting
	static const int MAX_MEM_SIZE = DISK_BLOCK * (1 << 12);

	// used for the priority queue sorting
	struct SSortedHit {
		SExternalSort hit; 
		int bucket; 
	}; 

	// used to store statistics related to sort progress
	struct SSortProgress {
		// the total number of comp blocks that
		// need to be sorted in a given sort pass
		uLong total_comp_blocks;
		// the current number of comp blocks that 
		// have been merged from any of the bucket divs
		int merged_comp_block_num;
		// this stores the total number of sorted bytes
		// on a block per block basis
		_int64 sorted_byte_num;
		// stores the number of sorted blocks
		int sorted_block_num;

		void Initialize() {
			merged_comp_block_num = 0;
			sorted_block_num = 0;
			sorted_byte_num = 0;
		}

		SSortProgress() {
			Initialize();
		}
	};

	// stores the number of characters associated with 
	// a hit item
	int m_hit_byte_size; 
	// this is the compare function used to sort hits
	static int (*m_compare)(const SExternalSort & arg1, 
		const SExternalSort & arg2); 

	// stores the number of bytes that need to be sorted
	 _int64 m_sort_size; 
	// stores a pointer to the compression block that
	// needs to be sorted
	CCompression *m_comp; 

	// stores the progress statisitcs related to sorting
	SSortProgress m_progress;
	// stores the sorted comp buffer after all sorted blocks
	// have been merged using a priority queue
	CCompression m_comp1; 
	// stores the sorted blocks that have been sorted internally
	// in memory - these need to be merged together
	CCompression m_comp2; 
	// This stores the sorted comp block directory
	CCompression *m_sort_comp_ptr;
	// This stores the merged comp bloc directory
	CCompression *m_merged_comp_ptr;
	
	// This defines a buffer that stores two consecutive
	// blocks belonging to a bucket that is being merged
	// together. To reduce the overhead of disk access a 
	// thread is spawned to load the new block in memory
	// while the preloaded buffer can still be used.
	struct SBucketBuff {

		private:
		// This stores one half of the current in memory bucket
		CMemoryChunk<char> temp_buff;
		// This store a pointer to the currently active buffer 
		// in memory, this is swapped with the next sequential 
		// bucket when a disk access need to be performed
		char *curr_buff;

		// stores the current comp block offset in the bucket division
		int comp_block_offset;
		// this stores the offset within the current buffer 
		int buffer_offset;
		// store the number of comp blocks left to be extracted
		int comp_block_left;
		// This stores a pointer to the comp buffer that is being read
		CExternalMerge *this_ptr;
		// stores the current buffer size
		int curr_buff_size;

		public:

		// This initializes all the variables and loads the first
		// comp block into the back buffer.
		// @param bucket_id - the sort division or bucket from 
		//                  - which to extract the hit
		// @param comp_sort_size - the number of compression blocks per sorted
		//                       - internal memory block
		// @param this_ptr - a pointer to the ExternalSortC class instance
		void Initialize(int bucket_id, int comp_sort_size, 
			CExternalMerge *this_ptr) {

			curr_buff_size = 0;
			comp_block_offset = bucket_id * comp_sort_size;
			comp_block_left = comp_sort_size - 1;
			this->this_ptr = this_ptr;
			buffer_offset = 0; 
		}

		// Retreives the next hit from a given bucket division comp_sort_size 
		// is the offset associated with the current sorted division stored 
		// sequentially in the comp buffer.
		// @param buffer - a buffer to store the retrieved hit item
		// @param bytes - the number of bytes to retrieve
		// @return if false if no more sort items available, true otherwise
		bool GetNextBucketHit(char buffer[], int bytes) {

			if(buffer_offset >= curr_buff_size) {
				// exceeded the sorted division size 
				if(comp_block_left < 0) {
					return false; 
				}

				int comp_blocks = 1;
				this_ptr->m_sort_comp_ptr->GetUncompressedBlock(temp_buff, 
					comp_blocks, comp_block_offset);

				comp_block_offset++;
				this_ptr->m_progress.merged_comp_block_num++;

				if((this_ptr->m_progress.merged_comp_block_num % 20) == 0) {
					cout<<(float)((this_ptr->m_progress.merged_comp_block_num) * 100) 
						/ this_ptr->m_progress.total_comp_blocks<<"% Merged"<<endl;
				}

				buffer_offset = 0; 
				curr_buff_size = temp_buff.OverflowSize();
				curr_buff = temp_buff.Buffer();

				if(comp_block_offset >= this_ptr->m_sort_comp_ptr->CompBlockNum()) {
					comp_block_left = 0;
				} 

				comp_block_left--;
			}

			memcpy(buffer, curr_buff + buffer_offset, bytes);
			buffer_offset += bytes;

			return true; 
		}
	};

	// Reassigns comp blocks to their original size
	// one it has been sorted to enable faster access.
	// @param old_comp - the sorted comp buffer with the 
	//                 - smaller comp block size
	// @param new_comp - the comp buffer that is being created
	//                 - to replace the old comp with the bigger 
	//                 - comp block size
	void ReassignBufferSize(CCompression &old_comp, 
		CCompression &new_comp) {

		int comp_blocks = 2000000 / (DISK_BLOCK * m_hit_byte_size); 
		CMemoryChunk<char> comp_buffer; 
		int comp_offset = 0; 

		new_comp.Initialize(new_comp.DirectoryName(), new_comp.BufferSize());

		while(old_comp.GetUncompressedBlock(comp_buffer, 
			comp_blocks, comp_offset) >= 0) {

			new_comp.AddToBuffer(comp_buffer.Buffer(), comp_buffer.OverflowSize()); 
			comp_offset += comp_blocks; 
		}

		new_comp.FinishCompression(); 
		new_comp.ResetNextHitItem(); 
	}

	// This initializes the buckets before merging commences.
	// Note that two sequential blocks have to be loaded to
	// allow parallization to reduce disk access.
	// @param queue_buff - this stores the sort items that are pointed to
	//                   - by entries in the priority queue
	// @param comp_sort_size - the number of comp blocks used for each sorted
	//                       - division or bucket set
	// @param bucket - stores all of the buckets that are being merged together
	// @param bucket_set - the current set of set of buckets used
	// 					 - ie an offset of times max bucket number gives 
	//                   - an id into the future sorted division
	// @param bucket_num - the number of buckets used in this sorting pass
	// @param queue - stores all the top priority sort items that are 
	//              - merged together based on priority
	void InitializeBuckets(CMemoryChunk<char> &queue_buff, 
		int comp_sort_size, CMemoryChunk<SBucketBuff> &bucket,
		int bucket_set, int bucket_num, 
		CPriorityQueue<SSortedHit> &queue) {

		int offset = 4;
		SSortedHit item; 
		// fills the priority queue up initially
		for(int i=0; i<bucket_num; i++) {
			item.bucket = i; 
			item.hit.hit_item = queue_buff.Buffer() + offset; 
			bucket[i].Initialize(bucket_set + i, comp_sort_size, this);

			if(!bucket[i].GetNextBucketHit(item.hit.hit_item, m_hit_byte_size)) {
				continue;
			}

			queue.AddItem(item); 
			offset += m_hit_byte_size; 
		}
	}

	// This is the first pass of the sorting process note bucket number is
	// the max number of buckets that can fit into memory and bucket 
	// set is an index giving the current lot of buckets (bucket num)
	// @param bucket_num - the number of buckets used in this sorting pass
	// @param bucket_set - the current set of set of buckets used
	// 					 - ie an offset of times max bucket number gives 
	//                   - an id into the future sorted division
	// @param comp_sort_size - the number of comp blocks used for each sorted
	//                       - division or bucket set
	void SortPass(int bucket_num, int bucket_set, int comp_sort_size) {

		CMemoryChunk<SBucketBuff> bucket(bucket_num);
		CPriorityQueue<SSortedHit> queue; 
		queue.Initialize(bucket_num, SortCompare); 
		CMemoryChunk<char> queue_buffer((m_hit_byte_size * bucket_num) + 4); 
		CArrayList<char *> released_slot(10); 

		InitializeBuckets(queue_buffer, comp_sort_size,
			bucket, bucket_set, bucket_num, queue);

		SSortedHit item; 
		// sequential retrieves hit items from 
		// the sorted subset buffers
		while(queue.PopItem(item)) {
			released_slot.PushBack(item.hit.hit_item); 
			// adds to the final sorted buffer
			m_merged_comp_ptr->AddToBuffer(item.hit.hit_item, m_hit_byte_size);

			// retrieves a hit from a different bucket if none 
			// left in current bucket
			if(!bucket[item.bucket].GetNextBucketHit
				(released_slot.LastElement(), m_hit_byte_size))
				continue;

			// adds another hit back to the priority queue from
			// the same bucket division if bucket division not empty
			item.hit.hit_item = released_slot.PopBack(); 
			queue.AddItem(item); 
		}

		m_merged_comp_ptr->ForceFlush();
	}

	// the compare function used in the priority queue
	static inline int SortCompare(const SSortedHit &arg1, 
		const SSortedHit &arg2) {
		
		return m_compare(arg1.hit, arg2.hit); 
	}

	// This updates the comps per block on the next larger merge pass. It 
	// also reassigns the merged sorted blocks back to the origininal blocks
	// that need to be merged by replacing m_indv comp with sort comp. A 
	// second merge is only necessary if all the bucket divisions for the 
	// current merge didn't fit into memory.
	// @param max_bucket_num - the maximum number of buckets allowed per merge sort
	// @return true when finished (all buckets fit into memory) false otherwise
	void UpdateMergeBlocks(int &bucket_subset, int max_bucket_num) {

		m_merged_comp_ptr->FinishCompression(); 
		m_sort_comp_ptr->FinishCompression();
		CSort<char>::Swap(m_merged_comp_ptr, m_sort_comp_ptr);

		// performs another sorting pass if necessary
		if(bucket_subset == 0) {
			bucket_subset = -1;
			return; 
		}

		bucket_subset = 0;
		m_merged_comp_ptr->Initialize(m_merged_comp_ptr->DirectoryName(), 
			m_merged_comp_ptr->BufferSize()); 
	}

	// This iteratively merges larger and larger sorted blocks until a single
	// sorted blocks has been created. In most cases only a single pass needs 
	// to be performed if the initial number of sorted blocks does not exceed
	// the memory limitations.
	// @param bucket_num - this is the initial number of sorted blocks created
	//                   - as done by quicksort for internal sorting
	// @param comp_sort_size - stores the number of comp blocks used per
	//                       - sorted block of items
	// @param max_bucket_num - the maximum number of buckets allowed per merge sort
	void MergeSort(int bucket_num, int max_bucket_num, int comp_sort_size) {

		int bucket_subset = 0; 
		
		while(bucket_subset >= 0) {
			int bucket_count = bucket_num; 
			int new_comp_sort_size = MAX_SIGNED;
			bucket_num = 0;

			m_progress.merged_comp_block_num = 0;
			m_progress.total_comp_blocks = m_sort_comp_ptr->CompBlockNum();

			while(bucket_count > max_bucket_num) {
				SortPass(max_bucket_num, bucket_subset, comp_sort_size); 
				new_comp_sort_size = min(new_comp_sort_size, 
					m_merged_comp_ptr->CompBlockNum());

				bucket_count -= max_bucket_num; 
				bucket_subset += max_bucket_num; 
				bucket_num++;
			}

			if(bucket_count > 0) {
				SortPass(bucket_count, bucket_subset, comp_sort_size); 
				bucket_num++;
			}

			comp_sort_size = min(new_comp_sort_size, m_merged_comp_ptr->CompBlockNum());

			if(bucket_num <= max_bucket_num) {
				// write back to the original comp buffer
				m_sort_comp_ptr = m_comp;
			}

			UpdateMergeBlocks(bucket_subset, max_bucket_num);
		}
	}

	// This sets up the necessary comp buffers and does the initial
	// creating of sorted blocks that need to be merged together
	void SetupMergeSort() {

		int max_mem_size = MAX_MEM_SIZE;
		max_mem_size -= max_mem_size % m_hit_byte_size;

		int bucket_num = (int)(m_sort_size / max_mem_size);
		if(m_sort_size % max_mem_size) {
			bucket_num++; 
		}

		// stores the number of comp blocks used per sorted block of items
		int comp_sort_size = max_mem_size / m_sort_comp_ptr->BufferSize(); 
		if(max_mem_size % m_sort_comp_ptr->BufferSize()) {
			comp_sort_size++;
		}

		// the maximum number of buckets allowed per merge sort
		int max_bucket_num = comp_sort_size >> 1; 

		if(bucket_num <= max_bucket_num) {
			// write back to the original comp buffer
			m_merged_comp_ptr = m_comp;
			m_comp->Initialize(m_merged_comp_ptr->DirectoryName(), 
				m_comp->BufferSize()); 
		} else {
			m_merged_comp_ptr->Initialize(m_merged_comp_ptr->DirectoryName(), 
				DISK_BLOCK * m_hit_byte_size); 
		}

		MergeSort(bucket_num, max_bucket_num, comp_sort_size);
		m_merged_comp_ptr->FinishCompression();
	}

public:

	// This is called to do the initial set up.
	// @param hit_size - the number of bytes used for each hit item
	// @param comp - the comp buffer that needs to be sorted
	// @param compare - the comparison function
	void Setup(int hit_size, CCompression &comp, 
		int (*compare)(const SExternalSort & arg1, 
		const SExternalSort & arg2)) {

		comp.FinishCompression();
		m_sort_size = comp.BytesStored(); 
		if(m_sort_size <= 0) {
			return;
		}

		comp.ResetNextHitItem(); 
		m_comp = &comp; 
		m_hit_byte_size = hit_size; 
		m_compare = compare; 

		m_progress.Initialize();
		m_comp1.Initialize(CUtility::ExtendString
			(comp.DirectoryName(), ".indv.ext_sort"), 
			DISK_BLOCK * m_hit_byte_size); 

		m_comp2.SetDirectoryName(CUtility::ExtendString
			(comp.DirectoryName(), ".sort.ext_sort"));

		m_sort_comp_ptr = &m_comp1;
		m_merged_comp_ptr = &m_comp2;
	}

};
int (*CExternalMerge::m_compare)(const SExternalSort & arg1, 
	const SExternalSort & arg2); 

// This is an external sorting class that uses quick sort create the
// sorted blocks internally in memory. Only use this class when the
// bit width for radix sort exceeds log(N), where N is the input size.
class CExternalQuickSort : public CExternalMerge {

	// this stores the total number of bytes sorted so far
	_int64 m_bytes_sorted;
	// this protects access to the sorting
	CMutex m_sort_mutex;

	// This loads a number of sort items into memory from a single
	// comp buffer. This must be mutex protected.
	// @param hit_sort - a buffer that stores pointers to each of
	//                 - the sort items
	// @param buffer - this stores the sort items
	// @param mem_size - the maximum amount of main memory
	void LoadSortItemBlock(CMemoryChunk<SExternalSort> &hit_sort,
		CMemoryChunk<char> &buffer, int mem_size) {

		char *curr_pos = buffer.Buffer(); 
		hit_sort.AllocateMemory(mem_size / m_hit_byte_size); 

		for(int i=0; i<hit_sort.OverflowSize(); i++) {
			hit_sort[i].hit_item = curr_pos; 
			m_comp->NextHit(curr_pos, m_hit_byte_size);
			curr_pos += m_hit_byte_size; 
		}

		if(curr_pos > buffer.Buffer() + buffer.OverflowSize()) {
			cout<<"ovf";getchar();
		}
	}

	// This fills a buffer with unsorted hit items, sorts it
	// internally using quick sort and then writes the buffer
	// to a comp buffer for later merging. Note that there 
	// is more than a single thread operating allowing contents
	// to be read from memory while sorting at the same time.
	// @param hit_sort - a buffer that stores pointers to each of
	//                 - the sort items
	void SortAndStore(CMemoryChunk<SExternalSort> &hit_sort) {

		m_sort_mutex.Acquire();
		CExternalMerge::HybridSort(hit_sort.Buffer(), hit_sort.OverflowSize()); 
		m_sort_mutex.Release();

		for(int i=0; i<hit_sort.OverflowSize(); i++) {
			m_sort_comp_ptr->AddToBuffer(hit_sort[i].hit_item, m_hit_byte_size);
		}

		m_sort_comp_ptr->ForceFlush();
		m_progress.sorted_block_num++;
		m_progress.sorted_byte_num += m_hit_byte_size *
			hit_sort.OverflowSize();

		cout<<"Sorted "<<m_progress.sorted_byte_num<<
			" Bytes Out Of "<<m_sort_size<<" Bytes"<<endl;
	}

	// Breaks the compression buffer up into MAX_MEM_SIZE
	// blocks and sorts them - writes them back to the compression 
	// buffer.
	void CreateSetOfSortedBlocks() {

		int max_byte_num = MAX_MEM_SIZE;
		max_byte_num -= max_byte_num % m_hit_byte_size;

		CMemoryChunk<char> buffer; 
		// stores pointers to the start of every hit item
		// used to sort the buffer
		CMemoryChunk<SExternalSort> hit_sort;

		while(m_bytes_sorted < m_sort_size) {
			if(m_sort_size - m_bytes_sorted < max_byte_num) {
				// the last sorted block is stored last
				break;
			}

			buffer.AllocateMemory(max_byte_num);
			LoadSortItemBlock(hit_sort, buffer, max_byte_num);
			m_bytes_sorted += max_byte_num;
			
			SortAndStore(hit_sort); 
		}
	}

	// This creates segmented sorted blocks that are later merged 
	// together. It does this by performing quick sort multiple times.. 
	void QuickSort() {

		m_bytes_sorted = 0;
		m_progress.sorted_byte_num = 0;
		m_progress.sorted_block_num = 0;
		CreateSetOfSortedBlocks();

		CMemoryChunk<char> buffer; 
		CMemoryChunk<SExternalSort> hit_sort;

		// store the last block last
		int byte_num = (int)(m_sort_size - m_bytes_sorted);
		buffer.AllocateMemory(byte_num);
		LoadSortItemBlock(hit_sort, buffer, byte_num);
		SortAndStore(hit_sort); 

		m_sort_comp_ptr->FinishCompression();
	}

	// this is used for testing
	static int CompareTestFunc(const SExternalSort 
		&arg1, const SExternalSort &arg2) {

		for(int i=4; i>=0; i--) {
			uChar value1 = *(uChar *)(arg1.hit_item + i);
			uChar value2 = *(uChar *)(arg2.hit_item + i);

			if(value1 < value2) {
				return 1;
			}

			if(value1 > value2) {
				return -1;
			}
		}

		return 0;
	}

public:

	CExternalQuickSort() {
		int (*m_next_hit)(char *buffer) = NULL; 
	}

	// @param hit_size - the number of bytes used for each hit item
	// @param comp - the comp buffer that needs to be sorted
	// @param compare - the comparison function
	CExternalQuickSort(int hit_size, CCompression &comp, 
		int (*compare)(const SExternalSort & arg1, 
		const SExternalSort & arg2)) {

		Initialize(hit_size, comp, compare); 
	}

	// This initializes all the internal comp buffers and comparison functions
	// @param hit_size - the number of bytes used for each hit item
	// @param comp - the comp buffer that needs to be sorted
	// @param compare - the comparison function
	void Initialize(int hit_size, CCompression &comp, 
		int (*compare)(const SExternalSort & arg1, 
		const SExternalSort & arg2)) {

		CExternalMerge::SetComparisionFunction(compare); 

		Setup(hit_size, comp, compare);

		QuickSort();

		if(m_progress.sorted_block_num < 2) {
			ReassignBufferSize(*m_sort_comp_ptr, *m_comp);
			return; 
		}

		SetupMergeSort();
	}

	// just a test framework
	void TestExternalQuickSort() {

		CCompression test_comp("test_comp", 1000);

		_int64 compare = 0;
		_int64 item = 0;
		for(int i=0; i<999999; i++) {
			*((char *)&item + 0) = rand() % 120;
			*((char *)&item + 1) = rand() % 120;
			*((char *)&item + 2) = rand() % 120;
			*((char *)&item + 3) = rand() % 120;
			*((char *)&item + 4) = rand() % 120;

			compare += item;
			test_comp.AddToBuffer((char *)&item, 5);
		}

		test_comp.FinishCompression();
	
		Initialize(5, test_comp, CompareTestFunc);

		_int64 sum = 0;
		_int64 curr_item = 0;
			for(int i=0; i<999999; i++) {

			test_comp.NextHit((char *)&item, 5);
			
			sum += item;
			if(item < curr_item) {
				cout<<"error";getchar();
			}

			curr_item = item;
		}

		if(compare != sum) {
			cout<<"error2";getchar();
		}

		test_comp.RemoveCompression();
	}
}; 

// This class sorts a compression buffer externally using radix sort
// instead of quick sort as the internal sorter before the merging 
// process takes place. Also this class is parallized using threads
// to reduce the overhead of disk access. That is a parallel thread
// can be sorting while external disk storage is being accessed.
// This sorting can be significantly faster given that the total 
// bit width of the items to be sorted are less than O(log n). This
// does not use a comparison model and hence it's difficult to sort
// items whose rank cannot be determined from their bit pattern.

// Note this class assumes that the byte order of a sort item follows
// least significant to most significant.
class CExternalRadixSort : public CExternalMerge {

	// This defines a linked bucket item that stores
	// a pointer to the internal buffer where a given 
	// sort item is located.
	struct SNode {
		// this stores a pointer to the next linked node
		SNode *next_ptr;
		// this stores a pointer to the sort item
		char *sort_item;
	};

	// this stores the size in bytes of a given sort item
	static int m_sort_byte_size;
	// this stores the number of bytes that are used for sorting
	// this is taken as the low order bytes of the sort item
	static int m_compare_byte_size;
	// this stores the initial bit offset -> used in the compare function
	static int m_init_offset;

	// this stores the maximum bit width allowed for radix
	// sort, it's calculated based upon the amount of internal memory
	int m_max_bit_width;
	// this stores the total number of bytes sorted so far
	_int64 m_bytes_sorted;

	// This defines how a sort item is compare. Since a non comparison
	// model is used for sorting, this means that this simply orders
	// sort items in lexographic order. Note that an gauranteed
	// extra four bytes that have been nulled need to be placed at
	// the start of the buffer storing sort items, this is because
	// it's possible that the offset could become negativde in the 
	// comparison function (done for efficiency).
	static int CompareLexographicItem(const SExternalSort &arg1, 
		const SExternalSort &arg2) {

		static uLong value1;
		static uLong value2;
		static int offset;

		offset = m_init_offset;
		char *buff1 = arg1.hit_item + m_sort_byte_size;
		char *buff2 = arg2.hit_item + m_sort_byte_size;

		while(true) {
			value1 = *(uLong *)buff1;
			value2 = *(uLong *)buff2;

			if(offset < 0) {
				value1 >>= -offset;
				value2 >>= -offset;
			}


			if(value1 < value2) {
				return 1;
			}

			if(value1 > value2) {
				return -1;
			}

			if(offset <= 0) {
				return 0;
			}
			offset -= 32;
			buff1 -= 4;
			buff2 -= 4;
		}

		return 0;
	}

	// This places each sort item loaded into memory into its repective
	// bucket. This is done by creating a linked list of sort items that
	// are attatched to each bucket. These can than later be retrieved
	// by traversing the linked list of sort items.
	// @param byte_offset - this stores a pointer to the current sort item
	//                    - preloaded into an internal buffer
	// @param sort_num - the number of sort items to be sorted
	// @param curr_bit_offset - the current bit offset from the start of 
	//                        - the sort item that have been processed
	// @param bit_size - the number of bits that forms the mask to select
	//                 - the bucket with repect to the bit offset
	// @param node_buff - this stores all the pointers that are used to
	//                  - create the linked list for each bucket
	// @param bucket_start - this stores a pointer to the head of the linked
	//                     - list for each bucket division
	void AddSortItemsToBuckets(char *byte_offset, int sort_num, 
		int curr_bit_offset, int bit_size, CArray<SNode> &node_buff,
		CMemoryChunk<SNode *> &bucket_start) {

		uLong mask = -1;
		int byte_num = curr_bit_offset >> 3;
		int bit_offset = curr_bit_offset % 8;
		byte_offset += sort_num * m_hit_byte_size;

		mask >>= 32 - min(m_max_bit_width, 
			(m_compare_byte_size << 3) - curr_bit_offset);

		for(int i=0; i<sort_num; i++) {
			byte_offset -= m_hit_byte_size;
			char *byte = byte_offset + byte_num;
			uLong value = *(uLong *)byte;
			value >>= bit_offset;

			node_buff.ExtendSize(1);
			int bucket = value & mask;
			SNode *prev_ptr = bucket_start[bucket];
			SNode *curr_ptr = &node_buff.LastElement();

			bucket_start[bucket] = curr_ptr;
			curr_ptr->next_ptr = (prev_ptr == NULL) ? NULL : prev_ptr;
			curr_ptr->sort_item = byte_offset;
		}
	}

	// This peforms a single bucket sort on a list of items
	// stored in internal memory. This must be applied multiple
	// times to sort the entire sequence of items.
	// @param buffer - the buffer that stores the items that
	//               - need to be sorted
	// @param curr_bit_offset - the current bit offset from the start of 
	//                        - the sort item that have been processed
	// @param sort_num - the number of sort items to be sorted
	void BucketSort(CMemoryChunk<char> &buffer, int sort_num, int curr_bit_offset) {

		CArray<SNode> node_buff(sort_num);
		CMemoryChunk<SNode *> bucket_start;

		int bits_left = (m_compare_byte_size << 3) - curr_bit_offset;
		if(bits_left > m_max_bit_width) {
			bucket_start.AllocateMemory(1 << m_max_bit_width, NULL);
		} else {
			bucket_start.AllocateMemory(1 << bits_left, NULL);
		}

		int bit_size = min(m_max_bit_width, 
			(m_compare_byte_size << 3) - curr_bit_offset);

		CMemoryChunk<char> temp_buff(buffer);
		AddSortItemsToBuckets(temp_buff.Buffer(), sort_num, 
			curr_bit_offset, bit_size, node_buff, bucket_start);

		char *buff = buffer.Buffer();
		for(int i=0; i<bucket_start.OverflowSize(); i++) {
			SNode *curr_ptr = bucket_start[i];

			while(curr_ptr != NULL) {
				memcpy(buff, curr_ptr->sort_item, m_hit_byte_size);
				curr_ptr = curr_ptr->next_ptr;
				buff += m_hit_byte_size;
			}
		}
	}

	// This loads a buffer with a number of unsorted sort items 
	// that are then proceeded to be sorted with radix sort.
	// @param max_byte_num - the maximum number of bytes that can
	//                     - be stored in the internal buffer
	// @param buff - the internal buffer to store the sort items
	// @return the number of sort items in the buffer
	void LoadSortBuffer(int max_byte_num, char buff[]) {

		int items = max_byte_num / m_hit_byte_size;
		for(int i=0; i<items; i++) {
			m_comp->NextHit(buff, m_hit_byte_size, 1);
			buff += m_hit_byte_size;
		}
	}

	// This stores the sorted block in the external comp block buffer.
	// Here cares must be taken to ensure that the smallest comp block
	// is stored last to allow for correct alignment of the comp blocks.
	// @param sort_item_num - the number of sort items stored in the buffer
	// @param temp_buff - this stores all the sort items
	void StoreSortedBuffer(int sort_item_num, CMemoryChunk<char> &temp_buff) {

		m_progress.sorted_block_num++;
		m_progress.sorted_byte_num += sort_item_num * m_hit_byte_size;
		cout<<"Sorted "<<m_progress.sorted_byte_num<<
			" Bytes Out Of "<<m_sort_size<<" Bytes"<<endl;
	
		char *buff = temp_buff.Buffer();
		for(int i=0; i<sort_item_num; i++) {
			m_sort_comp_ptr->AddToBuffer(buff, m_hit_byte_size);
			buff += m_hit_byte_size;
		}

		m_sort_comp_ptr->ForceFlush();
	}

	// This cycles through the entire bit spectrum of the sort item
	// and uses bucket sort on each segment of the spectrum. Once
	// completely sorted, the buffer is stored externally.
	// @param sort_buff - this stores all the sorted hit items
	// @param sort_item_num - this stores the number of sort items
	void CycleThroughBitWidth(CMemoryChunk<char> &sort_buff, int sort_item_num) {

		int curr_bit_offset = 0;
		while(curr_bit_offset < (m_compare_byte_size << 3)) {
			BucketSort(sort_buff, sort_item_num, curr_bit_offset);
			curr_bit_offset += m_max_bit_width;
		}

		StoreSortedBuffer(sort_item_num, sort_buff);
	}

	// This iteratively loades a number of sort items into memory
	// sorts them using a single bucket sort and then writes the
	// sorted items out to another comp buffer. It stops when
	// the entire bit width has been scanned.
	// @param write_buff_comp - the new comp buffer to store the
	//                        - sorted items
	// @param read_buff_comp - the comp buffer from which to read
	//                       - the unsorted items
	void LoadSortAndStore() {
		
		int max_byte_num = MAX_MEM_SIZE;
		max_byte_num -= max_byte_num % m_hit_byte_size;
		CMemoryChunk<char> temp_buff(max_byte_num + 4);

		while(m_bytes_sorted < m_sort_size) {
			if(m_sort_size - m_bytes_sorted < max_byte_num) {
				// the last sorted block is stored last
				break;
			}
			
			m_bytes_sorted += max_byte_num;
			LoadSortBuffer(max_byte_num, temp_buff.Buffer());
			int sort_item_num = max_byte_num / m_hit_byte_size;

			CycleThroughBitWidth(temp_buff, sort_item_num);
		}
	}

	// This finds the maximum bit width increment used for bucket sorting.
	void DetermineBitIncrement() {
		int max_byte_num = MAX_MEM_SIZE;
		max_byte_num -= max_byte_num % m_hit_byte_size;

		m_max_bit_width = CMath::LogBase2(max_byte_num);
		if((m_max_bit_width << 1) % max_byte_num) {
			//align at the bit boundary
			m_max_bit_width--;
		}

		m_max_bit_width = min(m_max_bit_width, 24);
	}

	// This creates segmented sorted blocks that are later merged 
	// together. It does this by performing bucket sort multiple times
	// using the radix sort idea. 
	void RadixSort() {

		m_bytes_sorted = 0;
		DetermineBitIncrement();

		LoadSortAndStore();

		// stores the last sorted block
		int byte_size = (int)(m_sort_size - m_bytes_sorted);
		CMemoryChunk<char> temp_buff(byte_size + 4);

		LoadSortBuffer(byte_size, temp_buff.Buffer());
		int sort_item_num = byte_size / m_hit_byte_size;
		CycleThroughBitWidth(temp_buff, sort_item_num);

		m_sort_comp_ptr->FinishCompression();
	}

public:

	CExternalRadixSort() {
	}

	// This performs the sorting using radix sort on a set of fixed 
	// size sort items. The compare byte size specifies the number of lower
	// order bytes that are used for comparison. If this is not set than
	// it's taken to be the sort item byte size.
	// @param sort_byte_size - the size of a given sort item
	// @param comp - the comp buffer that is being sorted.
	// @param compare_byte_size - this is the number of low order bytes used
	//                          - to do the comparison, if -1 then this is
	//                          - just the sort byte size
	CExternalRadixSort(int sort_byte_size, 
		CCompression &comp, int compare_byte_size = -1) {

		Initialize(sort_byte_size, comp, compare_byte_size);
	}
	
	// This performs the sorting using radix sort on a set of fixed 
	// size sort items. The compare byte size specifies the number of lower
	// order bytes that are used for comparison. If this is not set than
	// it's taken to be the sort item byte size.
	// @param sort_byte_size - the size of a given sort item
	// @param comp - the comp buffer that is being sorted.
	// @param compare_byte_size - this is the number of low order bytes used
	//                          - to do the comparison, if -1 then this is
	//                          - just the sort byte size
	void Initialize(int sort_byte_size, 
		CCompression &comp, int compare_byte_size = -1) {

		CExternalMerge::Setup(sort_byte_size, comp, CompareLexographicItem);

		if(compare_byte_size < 0) {
			compare_byte_size = sort_byte_size;
		}

		m_compare_byte_size = compare_byte_size;
		m_sort_byte_size = m_compare_byte_size - 4;
		m_init_offset = m_sort_byte_size;

		// this is used in the lexographic compare function
		if(m_init_offset < 0) {
			m_init_offset = -((-m_init_offset) << 3);
		} else {
			m_init_offset <<= 3;
		}
		
		RadixSort();
		
		if(m_progress.sorted_block_num < 2) {
			ReassignBufferSize(*m_sort_comp_ptr, *m_comp);
			return; 
		}

		CExternalMerge::SetupMergeSort();
	}

	// this is just a test framework
	void TestExternalRadixSort() {

		for(int item_num = 1; item_num < 24; item_num++) {
			cout<<"Item Byte Num "<<item_num<<endl;

			_int64 num = 0;
			CCompression test_comp("test_comp", 1000);

			_int64 sum = 0;
			uChar buff[40];

			for(int i=0; i<599999; i++) {
				num = rand();
				for(int j=0; j<item_num + 5; j++) {
					buff[j] = rand();
					test_comp.AddToBuffer((char *)&buff[j], 1);
				}

				for(int j=0; j<item_num; j++) {
					sum += buff[j];
				}
			}

			test_comp.FinishCompression();

			Initialize(item_num + 5, test_comp, item_num);

			uChar prev[40];
			for(int j=0; j<40; j++) {
				prev[j] = 0;
			}
			_int64 compare = 0;
			_int64 curr_num = 0;
			test_comp.LoadIndex();
			test_comp.ResetNextHitItem();

			num = 0;
			for(int i=0; i<599999; i++) {
				for(int j=0; j<item_num + 5; j++) {
					if(!test_comp.NextHit((char *)&buff[j], 1)) {
						cout<<"error";getchar();
					}
				}

				for(int j=item_num-1; j>=0; j--) {
					if(prev[j] > buff[j]) {
						cout<<"error2";getchar();
					}
					if(buff[j] > prev[j])break;
				}

				for(int j=item_num-1; j>=0; j--) {
					prev[j] = buff[j];
					compare += prev[j];
				}
			}

			if(sum != compare) {
				cout<<"error1 "<<sum<<" "<<compare ;getchar();
			}

			test_comp.RemoveCompression();
		}
	}
};
int CExternalRadixSort::m_sort_byte_size;
int CExternalRadixSort::m_init_offset;
int CExternalRadixSort::m_compare_byte_size;


