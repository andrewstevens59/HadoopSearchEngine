#include "../HitScore.h"

// This class is responsible for compiling the list of excerpt keywords
// associated with all of the external nodes expanded while searching.
// It keeps a record of the every keyword and the number of occurrences
// of that keyword in the corpus. The keyword occurrence is then compared
// with the word weight. The class is used to assign a keyword match score
// to all of the high priority excerpts which is partially used to rank
// the documents.
class CExcerptKeywords {

	// This stores the keyword and the parent node. This is used
	// to prevent an excessive cluster term receiving undue weight.
	struct SKeyword {
		// This stores the keywor id
		S5Byte keyword_id;
		// This stores the parent node
		SABTreeNode *parent;
	};

	// This stores the set of keywords 
	CObjectHashMap<SKeyword> m_keyword_map;

	// This returns the hash code for a visit node
	static int HashCode(const SKeyword &arg) {
		return (int)arg.parent + S5Byte::Value(arg.keyword_id);
	}

	// This compares two visit nodes for equality
	static bool Equal(const SKeyword &arg1, const SKeyword &arg2) {

		if(arg1.parent != arg2.parent) {
			return false;
		}

		if(S5Byte::Value(arg1.keyword_id) != S5Byte::Value(arg2.keyword_id)) {
			return false;
		}

		return true;
	}

	// This function retrieves the next sequential word id in the excerpt
	// @param byte_offset - this is the byte offset of the keyword set being processed
	// @param tree_id - this is the id of the keyword set being processed
	inline void RetrieveExcerptWordID(_int64 &byte_offset, int tree_id, COpenConnection &conn) {

		static CMemoryChunk<char> ab_buff(sizeof(S5Byte));
		CByte::KeywordBytes(tree_id, ab_buff.Buffer(), sizeof(S5Byte), byte_offset);

		static SKeyword keyword;
		memcpy((char *)&keyword.keyword_id, ab_buff.Buffer(), sizeof(S5Byte));
		
		conn.Send((char *)&keyword.keyword_id, sizeof(S5Byte));

		byte_offset += 5;
	}

public:

	CExcerptKeywords() {
	}

	// This adds the excerpt keywords that belong to a particular document
	// @param byte_offset - this is the byte offset of the keyword set being processed
	// @param tree_id - this is the id of the keyword set being processed
	// @param check_sum - this is the checksum for the document created from 
	//                  - the hit positions in the document
	// @return true if not a duplicate excerpt, false otherwise
	bool AddExcerptKeywords(_int64 byte_offset, int tree_id,
		_int64 &check_sum, COpenConnection &conn) {

		static int enc_bytes;
		static CMemoryChunk<char> ab_buff(11);
		char *ab_buff_ptr = ab_buff.Buffer();
		uChar keyword_num = 0;

		CByte::KeywordBytes(tree_id, ab_buff.Buffer(), 
			ab_buff.OverflowSize(), byte_offset);

		if(CArray<char>::GetEscapedItem(ab_buff_ptr, enc_bytes) == 0) {
			// no keywords stored
			conn.Send((char *)&check_sum, sizeof(_int64));
			conn.Send((char *)&keyword_num, sizeof(uChar));
			return false;
		}

		byte_offset += enc_bytes;
		memcpy((char *)&check_sum + sizeof(uLong), ab_buff_ptr, sizeof(uLong));

		ab_buff_ptr += 4;
		keyword_num = *(uChar *)ab_buff_ptr;
		byte_offset += 5;

		conn.Send((char *)&check_sum, sizeof(_int64));
		conn.Send((char *)&keyword_num, sizeof(uChar));

		for(uChar i=0; i<keyword_num; i++) {
			RetrieveExcerptWordID(byte_offset, tree_id, conn);
		}

		return true;
	}

};
