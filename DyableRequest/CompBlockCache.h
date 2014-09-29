#include "./NameServer.h"

// This stores the directory where all the ABTrees are stored
const char *AB_TREE_DIR = "GlobalData/HitSearchTrees/search_tree";
// This stores the directory where all the keyword sets are stored
const char *KEYWORDS_DIR = "GlobalData/HitSearchTrees/keywords";
// This stores the directory where all the s_link sets are stored
const char *S_LINK_DIR = "GlobalData/HitSearchTrees/s_links";
// This stores the directory for the doc id lookup
const char *DOC_ID_LOOKUP_DIR = "GlobalData/HitSearchTrees/doc_id_mapping";

// This class stores instances of each of the different hit types so 
// that they can be accessed as need by the other classes.
class CHitItemBlockSet {

	// This stores each of the hit types
	struct SHitType {
		// This stores the comp buffers
		CHitItemBlock hit_type[2];
	};

	// This stores all of the ab_tree comp blocks
	CMemoryChunk<CHitItemBlock> m_ab_tree_block;
	// This stores hit list comp blocks for each word div
	CMemoryChunk<SHitType> m_hit_list_block;
	// This stores the lookup comp blocks
	CMemoryChunk<CHitItemBlock> m_lookup_block;
	// This stores the keywords comp blocks
	CMemoryChunk<CHitItemBlock> m_keyword_block;
	// This stores the s_link comp blocks
	CMemoryChunk<CHitItemBlock> m_s_link_block;
	// This stores the doc id and keyword lookup
	CMemoryChunk<CHitItemBlock> m_doc_id_block;
	// This stores the number of hit list divisions
	int m_hit_list_div_num;

	// This loads in each of the different hit block indexes
	void LoadHitBlockIndexes() {

		const char *dir[2];
		dir[TITLE_HIT_INDEX] = "GlobalData/Retrieve/title_hit";
		dir[EXCERPT_HIT_INDEX] = "GlobalData/Retrieve/excerpt_hit";
		
		for(int i=0; i<m_ab_tree_block.OverflowSize(); i++) {
			m_ab_tree_block[i].Initialize(CUtility::
				ExtendString(AB_TREE_DIR, i), 100);

			m_keyword_block[i].Initialize(CUtility::
				ExtendString(KEYWORDS_DIR, i), 100);

			m_s_link_block[i].Initialize(CUtility::
				ExtendString(S_LINK_DIR, i), 100);

			m_doc_id_block[i].Initialize(CUtility::
				ExtendString(DOC_ID_LOOKUP_DIR, i), 100);
		}

		for(int j=0; j<2; j++) {
			for(int i=0; i<m_hit_list_block.OverflowSize(); i++) {
				m_hit_list_block[i].hit_type[j].Initialize(CUtility::
					ExtendString(dir[j], i), 100);
			}
		}

		for(int i=0; i<m_lookup_block.OverflowSize(); i++) {
			m_lookup_block[i].Initialize(CUtility::ExtendString
				("GlobalData/Retrieve/lookup", i), 100);
		}
	}

public:

	CHitItemBlockSet() {
	}

	// This sets the number of hit list divisions
	inline int GetHitListDivNum() {
		return m_hit_list_div_num;
	}

	// This is called to start the comp block cache
	void Initialize(int hit_list_div_num, int ab_tree_num) {

		CHitItemBlock::InitializeLRUQueue(50000);

		m_hit_list_div_num = hit_list_div_num;
		m_ab_tree_block.AllocateMemory(ab_tree_num);
		m_keyword_block.AllocateMemory(ab_tree_num);
		m_s_link_block.AllocateMemory(ab_tree_num);
		m_hit_list_block.AllocateMemory(hit_list_div_num);
		m_lookup_block.AllocateMemory(hit_list_div_num);
		m_doc_id_block.AllocateMemory(hit_list_div_num);

		LoadHitBlockIndexes();
	}

	// This is the entry function used to retrieve a set of bytes from
	// storage at some offset. The comp block will need to be loaded 
	// into memory if it not already availabe. 
	// @param word_div - this is the word division being retrieved
	// @param byte_offset - this is the byte offset in the comp buffer
	// @param byte_num - this is the number of bytes to retrieve
	// @param buff - this is where all the retrieved bytes are stored 
	inline void LookupBytes(int word_div, char buff[], 
		int byte_num, _int64 &byte_offset) {

		m_lookup_block[word_div].RetrieveByteSet(byte_offset, byte_num, buff);
	}

	// This returns the total number of bytes in a lookup division
	// @param word_div - this is the word division being retrieved
	inline _int64 TotalLookupBytes(int word_div) {
		return m_lookup_block[word_div].CompBuffer().BytesStored();
	}

	// This is the entry function used to retrieve a set of bytes from
	// storage at some offset. The comp block will need to be loaded 
	// into memory if it not already availabe. 
	// @param word_div - this is the word division being retrieved
	// @param hit_type - this is the hit type being retrieved
	// @param byte_offset - this is the byte offset in the comp buffer
	// @param byte_num - this is the number of bytes to retrieve
	// @param buff - this is where all the retrieved bytes are stored 
	inline void HitListBytes(int word_div, int hit_type, char buff[], 
		int byte_num, _int64 &byte_offset) {

		m_hit_list_block[word_div].hit_type[hit_type].
			RetrieveByteSet(byte_offset, byte_num, buff);
	}

	// This returns the total number of bytes in a given hit list div
	inline _int64 HitListByteNum(int word_div, int hit_type) {
		return m_hit_list_block[word_div].hit_type[hit_type].CompBuffer().BytesStored();
	}

	// This is the entry function used to retrieve a set of bytes from
	// storage at some offset. The comp block will need to be loaded 
	// into memory if it not already availabe. 
	// @param tree_div - this is the ab_tree being accessed
	// @param byte_offset - this is the byte offset in the comp buffer
	// @param byte_num - this is the number of bytes to retrieve
	// @param buff - this is where all the retrieved bytes are stored 
	inline void ABTreeBytes(int tree_div, char buff[], 
		int byte_num, _int64 &byte_offset) {

		m_ab_tree_block[tree_div].RetrieveByteSet(byte_offset, byte_num, buff);
	}

	// This retrieves the set of keyword bytes
	// @param tree_div - this is the ab_tree being accessed
	// @param byte_offset - this is the byte offset in the comp buffer
	// @param byte_num - this is the number of bytes to retrieve
	// @param buff - this is where all the retrieved bytes are stored 
	inline void KeywordBytes(int tree_div, char buff[], 
		int byte_num, _int64 &byte_offset) {

		m_keyword_block[tree_div].RetrieveByteSet(byte_offset, byte_num, buff);
	}

	// This retrieves the set of s_link bytes
	// @param tree_div - this is the ab_tree being accessed
	// @param byte_offset - this is the byte offset in the comp buffer
	// @param byte_num - this is the number of bytes to retrieve
	// @param buff - this is where all the retrieved bytes are stored 
	inline void SLinkBytes(int tree_div, char buff[], 
		int byte_num, _int64 &byte_offset) {

		m_s_link_block[tree_div].RetrieveByteSet(byte_offset, byte_num, buff);
	}

	// This retrieves the set of doc_id_lookup bytes
	// @param tree_div - this is the ab_tree being accessed
	// @param byte_offset - this is the byte offset in the comp buffer
	// @param byte_num - this is the number of bytes to retrieve
	// @param buff - this is where all the retrieved bytes are stored 
	inline void DocIDLookup(int tree_div, char buff[], 
		int byte_num, _int64 &byte_offset) {

		m_doc_id_block[tree_div].RetrieveByteSet(byte_offset, byte_num, buff);
	}

};

// This is used to retrieve selected bytes
class CByte : public CNodeStat {

	// This stores a static instance of the byte set
	static CHitItemBlockSet m_byte;

public:

	CByte() {
	}

	// This is called to start the comp block cache
	// @param ab_tree_num - this is the total number of ab_trees
	inline static void Initialize(int hit_list_div_num, int ab_tree_num) {
		m_byte.Initialize(hit_list_div_num, ab_tree_num);
	}

	// This sets the number of hit list divisions
	inline static int GetHitListDivNum() {
		return m_byte.GetHitListDivNum();
	}

	// This is the entry function used to retrieve a set of bytes from
	// storage at some offset. The comp block will need to be loaded 
	// into memory if it not already availabe. 
	// @param word_div - this is the word division being retrieved
	// @param byte_offset - this is the byte offset in the comp buffer
	// @param byte_num - this is the number of bytes to retrieve
	// @param buff - this is where all the retrieved bytes are stored 
	inline static void LookupBytes(int word_div, char buff[], 
		int byte_num, _int64 byte_offset) {

		m_byte.LookupBytes(word_div, buff, byte_num, byte_offset);
	}

	// This returns the total number of bytes in a lookup division
	// @param word_div - this is the word division being retrieved
	inline static _int64 TotalLookupBytes(int word_div) {
		return m_byte.TotalLookupBytes(word_div);
	}

	// This is the entry function used to retrieve a set of bytes from
	// storage at some offset. The comp block will need to be loaded 
	// into memory if it not already availabe. 
	// @param word_div - this is the word division being retrieved
	// @param hit_type - this is the hit type being retrieved
	// @param byte_offset - this is the byte offset in the comp buffer
	// @param byte_num - this is the number of bytes to retrieve
	// @param buff - this is where all the retrieved bytes are stored 
	inline static void HitListBytes(int word_div, int hit_type, char buff[], 
		int byte_num, _int64 &byte_offset) {

		m_byte.HitListBytes(word_div, hit_type, buff, byte_num, byte_offset);
	}

	// This returns the total number of bytes in a given hit list div
	inline static _int64 HitListByteNum(int word_div, int hit_type) {
		return m_byte.HitListByteNum(word_div, hit_type);
	}

	// This is the entry function used to retrieve a set of bytes from
	// storage at some offset. The comp block will need to be loaded 
	// into memory if it not already availabe. 
	// @param tree_div - this is the ab_tree being accessed
	// @param byte_offset - this is the byte offset in the comp buffer
	// @param byte_num - this is the number of bytes to retrieve
	// @param buff - this is where all the retrieved bytes are stored 
	inline static void ABTreeBytes(int tree_div, char buff[], 
		int byte_num, _int64 &byte_offset) {

		m_byte.ABTreeBytes(tree_div, buff, byte_num, byte_offset);
	}

	// This retrieves the set of keyword bytes
	// @param tree_div - this is the ab_tree being accessed
	// @param byte_offset - this is the byte offset in the comp buffer
	// @param byte_num - this is the number of bytes to retrieve
	// @param buff - this is where all the retrieved bytes are stored 
	inline static void KeywordBytes(int tree_div, char buff[], 
		int byte_num, _int64 &byte_offset) {

		m_byte.KeywordBytes(tree_div, buff, byte_num, byte_offset);
	}

	// This retrieves the set of s_link bytes
	// @param tree_div - this is the ab_tree being accessed
	// @param byte_offset - this is the byte offset in the comp buffer
	// @param byte_num - this is the number of bytes to retrieve
	// @param buff - this is where all the retrieved bytes are stored 
	inline static void SLinkBytes(int tree_div, char buff[], 
		int byte_num, _int64 &byte_offset) {

		m_byte.SLinkBytes(tree_div, buff, byte_num, byte_offset);
	}

	// This retrieves the set of doc_id_lookup bytes
	// @param tree_div - this is the ab_tree being accessed
	// @param byte_offset - this is the byte offset in the comp buffer
	// @param byte_num - this is the number of bytes to retrieve
	// @param buff - this is where all the retrieved bytes are stored 
	inline static void DocIDLookup(int tree_div, char buff[], 
		int byte_num, _int64 &byte_offset) {

		m_byte.DocIDLookup(tree_div, buff, byte_num, byte_offset);
	}
};
CHitItemBlockSet CByte::m_byte;