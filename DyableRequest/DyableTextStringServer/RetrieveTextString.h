#include "SpellCheck.h"

// This retrieves the corresponding text string for a given word id.
// There is a 1-1 index built over each word id to find the byte
// offset for the corresponding text string.
class CRetrieveTextString {

	// This stores the lookup indexes
	CMemoryChunk<CHitItemBlock> m_lookup_index;
	// This stores the set of sorted terms
	CMemoryChunk<CHitItemBlock> m_sort_word;

	// This finds the text string for a given word id
	char *TextString(_int64 &word_id, uChar &word_length) {

		int div = word_id % CNodeStat::GetHashDivNum();
		word_id /= CNodeStat::GetHashDivNum();

		CHitItemBlock &lookup = m_lookup_index[div];
		CHitItemBlock &word = m_sort_word[div];

		if(sizeof(S5Byte) * word_id >= lookup.CompBuffer().BytesStored()) {
			word_length = 0;
			return CUtility::SecondTempBuffer();
		}

		_int64 byte_offset = 0;
		lookup.RetrieveByteSet(sizeof(S5Byte) * word_id, sizeof(S5Byte), (char *)&byte_offset);

		if(byte_offset + sizeof(uChar) >= word.CompBuffer().BytesStored()) {
			word_length = 0;
			return CUtility::SecondTempBuffer();
		}

		word.RetrieveByteSet(byte_offset, sizeof(uChar), (char *)&word_length);

		if(byte_offset + sizeof(uChar) + word_length >= word.CompBuffer().BytesStored()) {
			word_length = 0;
			return CUtility::SecondTempBuffer();
		}

		word.RetrieveByteSet(byte_offset + sizeof(uChar), word_length, CUtility::SecondTempBuffer());

		return CUtility::SecondTempBuffer();
	}

public:

	CRetrieveTextString() {
	}

	// This sets up RetrieveTextString
	void Initialize() {

		m_lookup_index.AllocateMemory(CNodeStat::GetHashDivNum());
		m_sort_word.AllocateMemory(CNodeStat::GetHashDivNum());

		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			m_lookup_index[i].Initialize(CUtility::ExtendString
				("GlobalData/Retrieve/reverse_word_lookup", i), 100);
			m_sort_word[i].Initialize(CUtility::ExtendString
				("GlobalData/Retrieve/reverse_sorted_words", i), 100);
		}
	}

	// This processes a request for the lookup of a set of word id to find the 
	// corresponding text string for each word id
	void ProcessRequest(COpenConnection &conn) {

		int word_num = 0;
		_int64 word_id = 0;
		uChar word_length;
		conn.Receive((char *)&word_num, sizeof(int));

		for(int i=0; i<word_num; i++) {
			conn.Receive((char *)&word_id, sizeof(S5Byte));
			char *word_buff = TextString(word_id, word_length);
			conn.Send((char *)&word_length, sizeof(uChar));
			conn.Send(word_buff, word_length);
		}
	}

	// This is just a test framework
	void TestRetrieveTextString() {

		Initialize();
		static _int64 word_id = 39631;
		static uChar word_length = 0;

		CHitItemBlock::InitializeLRUQueue();

		for(long i=0; i<100; i++) {
			//word_id = rand() % 10000;
			char *word_buff = TextString(word_id, word_length);
			word_buff[word_length] = '\0';
			cout<<word_buff;getchar();
		}
	}
};









