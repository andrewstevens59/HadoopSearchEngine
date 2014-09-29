#include "./CompBlockCache.h"

// Strores the word id and word weight
// of each retrieved word
struct SWordItem {

	// This stores the global word id 
	// accross all words in the index
	S5Byte word_id; 
	// This stores the local id of the word 
	// used for duplicate terms with the same meaning
	uChar local_id;
	// This stores the weight of the word
	float factor; 
}; 

// This defines a data structure used to hold one of the overlapping
// hit segments. This just stores the byte boundaries of the hit segment
// aswell as the doc id boundaries.
struct SHitSegment {

	// This stores the byte boundary of the hit segment
	S64BitBound byte_bound;
	// This stores a pointer to the next overlapping segment
	SHitSegment *next_ptr;
	// This stores the start doc id in the hit segment
	_int64 start_doc_id;
	// This stores the end doc id in the hit segment
	_int64 end_doc_id;
	// This stores the word id of the hit segment
	uChar word_id;
	// This stores the word division for a given hit segment
	uChar word_div;
	// This stores the hit type index of the hit segment
	uChar hit_type_index;
};

// This defines one of the hit item boundaries
struct SHitBoundary {
	// This stores the doc id boundary
	S64BitBound doc_bound;
	// This stores the byte boundary
	S64BitBound byte_bound;
};

// This stores one of the index blocks used to retrieve various
// byte offsets for the different hit types for each hit item
// type. It loads the different spatial byte boundaries used
// to search for hit items in certain spatial regions. Each index
// block is of fixed size allowing a given word division to be 
// accessed directly at a known byte offset.
class CLookupIndex : public CNodeStat {

	// This is used to store the byte offset for each
	// of the different hit types
	struct SHitTypeOffset {
		// stores the hit offset for each of the different types
		_int64 hit_offset[2];

		SHitTypeOffset() {
			for(int j=0; j<2; j++) {
				hit_offset[j] = 0;
			}
		}
	};

	// This stores the byte offset for each of the different hit types
	SHitTypeOffset m_hit_pos;
	// This stores the number of hit segments in a given word division
	int m_hit_seg_num[2];
	// This stores the number of bytes in each hit type division
	SHitTypeOffset m_hit_byte_num;

	// stores the number of documents in the given word div
	_int64 m_document_num;
	// stores the word id division
	int m_division; 
	// stores the word id offset
	uLong m_word_id_offset; 

	// This loads the hit byte offset for this client for all of the
	// different hit types so that they can be indexed.
	// @param hit_byte_offset - this is the byte offset of the hit type
	// @param buff - this stores a pointer to the buffer where all of 
	//             - the different hit byte offsets are stored
	void LoadHitByteOffset(SHitTypeOffset &hit_byte_offset, char * &buff) {

		for(int j=0; j<2; j++) {
			memcpy((char *)&hit_byte_offset.hit_offset[j], buff, 5);
			buff += 5;
		}
	}

	// This is used to calculate the total number of bytes contained 
	// in a given hit list for a particular word id.
	// @param buff - this contains the index block of the next word id
	void CalculateHitByteNum(char buff[]) {

		// skips past the doc number
		buff += 5;

		SHitTypeOffset next_hit_type;
		LoadHitByteOffset(next_hit_type, buff);
	
		for(int j=0; j<2; j++) {

			if(next_hit_type.hit_offset[j] == m_hit_pos.hit_offset[j]) {
				m_hit_byte_num.hit_offset[j] = 0;
				m_hit_seg_num[j] = 0;
				continue;
			}

			next_hit_type.hit_offset[j] -= sizeof(int);
			m_hit_byte_num.hit_offset[j] = next_hit_type.hit_offset[j]
				- m_hit_pos.hit_offset[j];

			CByte::HitListBytes(m_division, j, (char *)&(m_hit_seg_num[j]), 
				sizeof(int), next_hit_type.hit_offset[j]);
		
			m_hit_byte_num.hit_offset[j] -= m_hit_seg_num[j] 
				* (sizeof(int) + sizeof(S5Byte) + sizeof(S5Byte));
		}
	}
	
public:

	CLookupIndex() {
	}

	// Loads the lookup index
	// @param word_id - the global word id
	// returns false if no hits available, true otherwise
	bool LoadLookupIndex(S5Byte &word_id) {
		
		m_division = word_id.Value() % CByte::GetHitListDivNum(); 
		m_word_id_offset = word_id.Value() / CByte::GetHitListDivNum(); 
		m_document_num = 0;

		static CMemoryChunk<char> index_block_buff
			(SLookupIndex::WORD_DIV_BYTE_SIZE * 2);

		_int64 byte_offset = m_word_id_offset * SLookupIndex::WORD_DIV_BYTE_SIZE;
		if(byte_offset > CByte::TotalLookupBytes(m_division)) {
			return false;
		}

		char *buff = index_block_buff.Buffer();
		CByte::LookupBytes(m_division, buff, 
			index_block_buff.OverflowSize(), byte_offset);

		memcpy((char *)&m_document_num, buff, 5);
		buff += 5;
		LoadHitByteOffset(m_hit_pos, buff);

		CalculateHitByteNum(buff);

		return true;
	}

	// Returns the number of documents in the given word div
	inline _int64 DocumentNum() {
		return m_document_num;
	}

	// Returns the word id offset
	inline int WorIdOffset() {
		return m_word_id_offset; 
	}

	// Returns the word id offset
	inline int WordDiv() {
		return m_division; 
	}

	// Returns the hit position start for one of the four hit type 
	// @param index - the index of the hit type
	inline _int64 HitPos(int index) {
		return m_hit_pos.hit_offset[index]; 
	}

	// Returns the number of bytes contained in a particular
	// hit list for a given word id
	// @param index - the index of the hit type
	inline _int64 HitByteNum(int index) {
		return m_hit_byte_num.hit_offset[index];
	}

	// Returns the number of hit segments that composes a word division
	// @param index - the index of the hit type
	inline int HitSegNum(int index) {
		return m_hit_seg_num[index];
	}
}; 

// This class is repsonsible for retrieving all the hits associated with
// a given word id. This means swapping between the different hit types 
// when a particular hit type becomes exhausted. A seperate instance of
// this class exists for each word id that user requests. 
class CWordDiv {

	// This stores the index block associated with
	// this word divisions. This is used to gain access to 
	// the different hit byte offsets.
	CLookupIndex m_lookup_index;
	// This stores the word id's for all type of hits
	static CArray<SWordItem> m_word_id_set;
	// This stores the number of focus terms
	static int m_focus_term_num;
	// This stores the number of cluster terms
	static int m_cluster_term_num;

public:

	CWordDiv() {
	}

	// This is called to initialize a given word division
	// @param word_id - this is the word id that is associated with
	//                - this word division
	// @return true if the word division loaded, false otherwise
	inline bool Initialize(S5Byte &word_id) {
		return m_lookup_index.LoadLookupIndex(word_id);
	}

	// This is used to set the number of focus and cluster terms
	inline static void SetTermNum(int focus_term_num, int cluster_term_num) {
		m_focus_term_num = focus_term_num;
		m_cluster_term_num = cluster_term_num;
	}
	
	// This returns the number of focus terms
	inline static int FocusTermNum() {
		return m_focus_term_num;
	}

	// This returns the number of cluster terms
	inline static int ClusterTermNum() {
		return m_cluster_term_num;
	}

	// This returns all of the word id's that make up the query
	inline static CArray<SWordItem> &WordIDSet() {
		return m_word_id_set;
	}

	// This returns one of the word id's in the set
	// @param word_id - this is the current word id in the set being processed
	inline static SWordItem &WordIDSet(int word_id) {
		return m_word_id_set[word_id];
	}

	// This returns the number of bytes contained in a given
	// hit list for the current word div.
	// @param index - the index of the hit type
	inline _int64 HitByteNum(int index) {
		return m_lookup_index.HitByteNum(index);
	}

	// Returns the number of hit segments that composes a word division
	// @param index - the index of the hit type
	inline int HitSegNum(int index) {
		return m_lookup_index.HitSegNum(index);
	}

	// This retrieves bytes associated with a particular hit type
	// @param buff - this is used to store all of the retrieved bytes
	// @param byte_num - this is the number of bytes to retrieve
	// @param byte_offset - this is the offset in the hit list from
	//                    - which to retrieve the bytes
	// @param hit_type_index - this is the hit type that is being retrieved
	void RetrieveHitBytes(char buff[], int byte_num, 
		_int64 byte_offset, int hit_type_index) {

		byte_offset += m_lookup_index.HitPos(hit_type_index);
		CByte::HitListBytes(m_lookup_index.WordDiv(),
			hit_type_index, buff, byte_num, byte_offset);
	}

};
CArray<SWordItem> CWordDiv::m_word_id_set;
int CWordDiv::m_focus_term_num;
int CWordDiv::m_cluster_term_num;

// This stores the hit item boundary for intermediate hit items found 
// while performing the binary search for a particualr doc id. By 
// storing previous hit item boundaries its possible to speed up future
// searches in the same word division. It uses a red black tree to 
// store hit item boundaries.
class CHitItemTreeCache {

	// This defines the minimum byte boundary width
	static const int MIN_BYTE_BOUND = 1024;

	// This stores a ptr to the hit boundary
	struct SHitBoundaryPtr {
		SHitBoundary *ptr;
	};

	// This stores the search tree
	CRedBlackTree<SHitBoundaryPtr> m_tree;
	// This stores the set of hit boundary 
	CLinkedBuffer<SHitBoundary> m_tree_node_buff;
	// This indicates that the updated child set is 
	// no longer valid since parent below minimum byte
	// bound threshold
	bool m_is_add_children;

	// This is used to compare nodes in the red black tree
	static int CompareTreeNode(const SHitBoundaryPtr &arg1, const SHitBoundaryPtr &arg2) {

		if(arg1.ptr->doc_bound.end < arg2.ptr->doc_bound.start) {
			return 1;
		}

		if(arg1.ptr->doc_bound.start > arg2.ptr->doc_bound.end) {
			return -1;
		}

		return 0;
	}

public:

	CHitItemTreeCache() {
		m_tree_node_buff.Initialize();
		m_tree.Initialize(2048, CompareTreeNode);
	}

	// This adds a node bound to the tree
	// @param word_div - this stores the current word division
	// @param hit_seg_ptr - this stores the top level hit segment
	void AddBound(CWordDiv &word_div, SHitSegment *hit_seg_ptr) {

		int byte_num;
		_int64 next_doc_id = 0;
		_int64 temp_doc_id = 0;
		_int64 hit_byte_offset = 0;

		static SHitBoundaryPtr ptr;
		_int64 curr_doc_id = hit_seg_ptr->start_doc_id;
		_int64 seg_byte_offset = hit_seg_ptr->byte_bound.end;
		int hit_seg_num = word_div.HitSegNum(hit_seg_ptr->hit_type_index);

		if(hit_seg_num == 0) {
			return;
		}

		for(int i=0; i<hit_seg_num; i++) {

			word_div.RetrieveHitBytes((char *)&byte_num, sizeof(int),
				seg_byte_offset, hit_seg_ptr->hit_type_index);
			seg_byte_offset += sizeof(int);

			word_div.RetrieveHitBytes((char *)&next_doc_id, sizeof(S5Byte),
				seg_byte_offset, hit_seg_ptr->hit_type_index);
			seg_byte_offset += sizeof(S5Byte);

			word_div.RetrieveHitBytes((char *)&temp_doc_id, sizeof(S5Byte),
				seg_byte_offset, hit_seg_ptr->hit_type_index);
			seg_byte_offset += sizeof(S5Byte);


			ptr.ptr = m_tree_node_buff.ExtendSize(1);
			ptr.ptr->byte_bound.start = hit_byte_offset;
			hit_byte_offset += byte_num;
			ptr.ptr->byte_bound.end = hit_byte_offset;

			ptr.ptr->doc_bound.start = curr_doc_id;
			ptr.ptr->doc_bound.end = next_doc_id;
			curr_doc_id = temp_doc_id;
			m_tree.AddNode(ptr);
		}

		if(hit_byte_offset < hit_seg_ptr->byte_bound.end) {
			if((hit_seg_ptr->byte_bound.end - hit_byte_offset) > MIN_BYTE_BOUND) {
				ptr.ptr = m_tree_node_buff.ExtendSize(1);
				ptr.ptr->byte_bound.start = hit_byte_offset;
				ptr.ptr->byte_bound.end = hit_seg_ptr->byte_bound.end;

				ptr.ptr->doc_bound.start = curr_doc_id;
				ptr.ptr->doc_bound.end = hit_seg_ptr->end_doc_id;
				m_tree.AddNode(ptr);
			}
		}
	}

	// This adds a node bound to the tree
	// @param bound_set - this is the hit item boundary
	void AddBound(CArray<SHitBoundary> &bound_set) {

		if(m_is_add_children == false) {
			return;
		}

		static SHitBoundaryPtr ptr;
		for(int i=0; i<bound_set.Size(); i++) {
			SHitBoundary &bound = bound_set[i];

			ptr.ptr = m_tree_node_buff.ExtendSize(1);
			ptr.ptr->byte_bound = bound.byte_bound;
			ptr.ptr->doc_bound = bound.doc_bound;
			m_tree.AddNode(ptr);
		}

		bound_set.Resize(0);
	}

	// This returns the value that's associated with the most
	// closest matching node bound in the tree.
	// @param doc_id - this is the doc id being searched for
	// @param bound - this is the hit item boundary
	void *FindCachedEntry(_int64 doc_id, SHitBoundary &bound) {

		static SHitBoundary node;
		node.doc_bound.start = doc_id;
		node.doc_bound.end = doc_id;

		static SHitBoundaryPtr ptr;
		ptr.ptr = &node;

		m_is_add_children = false;
		void *res = m_tree.NodePosition(ptr);
		if(res == NULL) {
			return NULL;
		}

		ptr = m_tree.NodeValue(res);
		bound = *ptr.ptr;

		if(doc_id < bound.doc_bound.start) {
			cout<<"low bound error";getchar();
		}

		if(doc_id > bound.doc_bound.end) {
			cout<<"upper bound error";getchar();
		}

		return res;
	}

	// This removes an entry from the cache
	void DeleteCachedEntry(void *entry) {

		static SHitBoundaryPtr ptr;
		ptr = m_tree.NodeValue(entry);

		if(ptr.ptr->byte_bound.Width() >= MIN_BYTE_BOUND) {
			m_tree.DeleteNode(entry);
			m_is_add_children = true;
		} 
	}

	// This is called to remove all the entries in the hash 
	// tree so the tree can be arbitrarily expanded again
	void Reset() {
		m_is_add_children = false;
		m_tree_node_buff.Restart();
		m_tree.Reset();
	}
};

// This class is responsible for caching the doc id boundaries for 
// particular word ids. This is used when hit items are being grouped
// spatially. If a doc id boundary exists in the cache then a binary
// search can be bypassed. Only hit item boundaries with a large breadths
// are added to the cache to save space. This class uses a LRU eviction 
// policy to remove old entries if there is no space left in the cache.
// The usual hash table is used to perform a lookup and store entries. 
// The key is a combination of the word id and the doc id boundary
class CHitItemBoundaryCache {

	// This defines the number of hash divisions to use
	static const int HASH_DIV_NUM = 100000;
	// This stores the maximum number of bytes allowed in memory
	static const int MAX_MEM_SIZE = 100000;

	// This stores a particular entry in the hash table
	struct SHashNode {
		// This stores a ptr to the next hach entry
		SHashNode *next_hash_ptr;
		// This stores a ptr to the next cache entry
		SHashNode *next_cache_ptr;
		// This stores a ptr to the next cache entry
		SHashNode *prev_cache_ptr;
		// This stores the byte boundary
		_int64 byte_bound;
		// This stores the word division
		S5Byte word_id;
		// This stores the doc id 
		S5Byte doc_id;
		// This stores the hit ytpe index
		uChar hit_type_index;
	};

	// This stores the hash table
	CMemoryChunk<SHashNode *> m_hash_table;
	// This stores all of the hash nodes
	CLinkedBuffer<SHashNode> m_hash_node_buff;
	// This stores the current number of bytes loaded
	int m_bytes_loaded;

	// This stores the head of the free hash node list
	SHashNode *m_free_hash_node_ptr;
	// This stores the head of the cache set
	SHashNode *m_head_cache_ptr;
	// This stores the tail of the cache set
	SHashNode *m_tail_cache_ptr;

	// This returns a free hash node 
	inline SHashNode *FreeHashNode() {

		if(m_free_hash_node_ptr == NULL) {
			return m_hash_node_buff.ExtendSize(1);
		}

		SHashNode *ptr = m_free_hash_node_ptr;
		m_free_hash_node_ptr = m_free_hash_node_ptr->next_hash_ptr;
		return ptr;
	}

	// This deletes an entry from the table
	// @param entry - this is the entry in the hash table
	void DeleteEntry(SHashNode *entry) {

		int hash = entry->word_id.Value() + entry->doc_id.Value() + entry->hit_type_index;
		hash %= m_hash_table.OverflowSize();

		SHashNode *curr_ptr = m_hash_table[hash];
		SHashNode *prev_ptr = NULL;

		while(curr_ptr != NULL) {
			if(curr_ptr == entry) {
				if(prev_ptr == NULL) {
					m_hash_table[hash] = curr_ptr->next_hash_ptr;
				} else {
					prev_ptr->next_hash_ptr = curr_ptr->next_hash_ptr;
				}

				m_bytes_loaded -= sizeof(SHashNode);
				return;
			}

			prev_ptr = curr_ptr;
			curr_ptr = curr_ptr->next_hash_ptr;
		}

		cout<<"not found1";getchar();
	}

	// This evicts an entry from the hash table
	// @param entry - this is the entry in the hash table
	void EvictEntry() {

		DeleteEntry(m_tail_cache_ptr);

		SHashNode *hash_ptr = m_tail_cache_ptr;
		m_tail_cache_ptr = hash_ptr->prev_cache_ptr;
		if(m_tail_cache_ptr != NULL) {
			m_tail_cache_ptr->next_cache_ptr = NULL;
		}

		SHashNode *prev_ptr = m_free_hash_node_ptr;
		m_free_hash_node_ptr = hash_ptr;
		m_free_hash_node_ptr->next_hash_ptr = prev_ptr;
	}

	// This moves the most recently accessed hash_node to 
	// the head of the LRU list.
	void PromoteHashNode(SHashNode *hash_ptr) {

		if(hash_ptr == m_head_cache_ptr) {
			// already at the head of the list
			return;
		}

		if(hash_ptr == m_tail_cache_ptr) {
			// reassigns the tail
			m_tail_cache_ptr = hash_ptr->prev_cache_ptr;
			m_tail_cache_ptr->next_cache_ptr = NULL;
		} else {
			// links up the nodes in the list
			(hash_ptr->prev_cache_ptr)->next_cache_ptr = hash_ptr->next_cache_ptr;
			(hash_ptr->next_cache_ptr)->prev_cache_ptr = hash_ptr->prev_cache_ptr;
		}

		SHashNode *prev_ptr = m_head_cache_ptr;
		m_head_cache_ptr = hash_ptr;
		m_head_cache_ptr->next_cache_ptr = prev_ptr;
		m_head_cache_ptr->prev_cache_ptr = NULL;
		prev_ptr->prev_cache_ptr = m_head_cache_ptr;
	}

	// This adds a new hash node to the set of cached ab_nodes
	inline void ABNodeToCachedSet(SHashNode *hash_ptr) {

		SHashNode *prev_ptr = m_head_cache_ptr;
		m_head_cache_ptr = hash_ptr;
		m_head_cache_ptr->next_cache_ptr = prev_ptr;

		if(prev_ptr != NULL) {
			prev_ptr->prev_cache_ptr = m_head_cache_ptr;
		} else {
			m_tail_cache_ptr = m_head_cache_ptr;
		}

		m_head_cache_ptr->prev_cache_ptr = NULL;
	}

public:

	CHitItemBoundaryCache() {
		int hash_num = CHashFunction::FindClosestPrime(HASH_DIV_NUM);
		m_hash_table.AllocateMemory(hash_num, NULL);

		m_hash_node_buff.Initialize(2048);
		m_bytes_loaded = 0;

		m_free_hash_node_ptr = NULL;
		m_head_cache_ptr = NULL;
		m_tail_cache_ptr = NULL;
	}

	// This adds an entry to the cache. It may be necessary to evict an older
	// entry if the cache is full. 
	// @param id - this is the particular word id
	// @param hit_type_index - this is the hit type being stored
	// @param doc_id - this is the hit item doc id boundary
	// @param byte_bound - the results are stored in this variable if successfull
	void AddHitItemBoundary(uLong id, uChar hit_type_index,
		const _int64 &doc_id, _int64 &byte_bound) {

		S5Byte word_id = CWordDiv::WordIDSet(id).word_id;
		int hash = word_id.Value() + doc_id + hit_type_index;
		hash %= m_hash_table.OverflowSize();

		SHashNode *prev_ptr = m_hash_table[hash];
		SHashNode *curr_ptr = FreeHashNode();
		m_hash_table[hash] = curr_ptr;

		curr_ptr->next_hash_ptr = prev_ptr;
		curr_ptr->byte_bound = byte_bound;
		curr_ptr->doc_id = doc_id;
		curr_ptr->hit_type_index = hit_type_index;
		curr_ptr->word_id = word_id;
		m_bytes_loaded += sizeof(SHashNode);

		ABNodeToCachedSet(curr_ptr);

		while(m_bytes_loaded >= MAX_MEM_SIZE) {
			EvictEntry();
		}
	}

	// This searches for a particular doc id byte boundary for a particular word
	// division. It searches the appropriate bucket in the hash table.
	// @param id - this is the particular word id
	// @param hit_type_index - this is the hit type being stored
	// @param doc_id - this is the hit item doc id boundary
	// @param byte_bound - the results are stored in this variable if successfull
	// @return true if the entry is in the cache, false otherwise
	bool FindHitItemBoundary(uLong id, uChar hit_type_index,
		const _int64 &doc_id, _int64 &byte_bound) {

		S5Byte word_id = CWordDiv::WordIDSet(id).word_id;
		int hash = word_id.Value() + doc_id + hit_type_index;
		hash %= m_hash_table.OverflowSize();

		SHashNode *curr_ptr = m_hash_table[hash];

		while(curr_ptr != NULL) {
			if(curr_ptr->word_id == word_id && curr_ptr->doc_id == doc_id
				&& curr_ptr->hit_type_index == hit_type_index) {

				PromoteHashNode(curr_ptr);
				byte_bound = curr_ptr->byte_bound;
				return true;
			}

			curr_ptr = curr_ptr->next_hash_ptr;
		}

		return false;
	}
};