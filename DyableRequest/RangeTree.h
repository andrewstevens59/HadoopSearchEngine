#include "./WordDivision.h"

// This stores one of the possible summary links
struct SReducedSummaryLink {
	// this stores the src node for a summary link
	S5Byte src;
	// this stores the dst node for a summary link
	S5Byte dst;
	// this stores the level at which a link is created
	uChar create_level;
	// This stores the traversal probability of 
	// a summary link
	float trav_prob;

	// This reads a s_link from a buffer
	// @param buff - this is where the s_link is stored
	// @return the number of bytes used to store the reduced s_link
	int ReadReducedSLink(char buff[]) {
		memcpy((char *)&src, buff, 5);
		memcpy((char *)&dst, buff + 5, 5);
		memcpy((char *)&trav_prob, buff + 10, sizeof(float));
		memcpy((char *)&create_level, buff + 14, sizeof(uChar));
		create_level >>= 1;

		return 15;
	}
};

// This defines a data structure used to store all the s_links
// for a given node in the ABTree. This is a linked list 
// structure where s_links are stored in priority order.
struct SLinkSet {
	// This is one of the s_links
	SReducedSummaryLink s_link;
	// This stores the link weight
	float link_weight;
	// This stores the src node ptr
	void *src_ptr;
	// This stores a pointer to the next s_link in the set
	SLinkSet *next_ptr;
};

// This stores one of the child nodes
struct SChildNode {
	// This stores a pointer to the child ab node
	void *ab_node_ptr;
	// This stores a pointer to the next child node
	SChildNode *next_ptr;
	// This stores the byte offset for the child
	_int64 child_byte_offset;
};

// This is used to calculate the spatial score for a ab_node
class CSpatialScore {

	// This stores the title spatial score
	static CMemoryChunk<float> m_title_hit_score;
	// This stores the excerpt spatial score
	static CMemoryChunk<float> m_excerpt_hit_score;

	// This finds the match score
	// @param word_div_num - this is the number of unique word ids contained in a given spatial region
	static float MatchScore(uChar &word_div_num) {

		int excerpt_div_num = 0;
		int title_div_num = 0;

		float min = 0;
		for(int i=0; i<m_title_hit_score.OverflowSize(); i++) {
			int id = CWordDiv::WordIDSet(i).local_id;
	
			if(m_title_hit_score[id] > 0) {
				min = min(min, m_title_hit_score[id]);
				m_title_hit_score[id] = -1;
				title_div_num++;
			}
		}

		for(int i=0; i<m_excerpt_hit_score.OverflowSize(); i++) {
			int id = CWordDiv::WordIDSet(i).local_id;
			if(m_excerpt_hit_score[id] > 0) {
				min = min(min, m_excerpt_hit_score[id]);
				m_excerpt_hit_score[id] = -1;
				excerpt_div_num++;
			}
		}

		word_div_num = max(excerpt_div_num, title_div_num);

		return min;
	}

public:

	CSpatialScore() {
	}

	// This Initializes the hit score 
	static void Initialize() {
		m_title_hit_score.AllocateMemory(CWordDiv::WordIDSet().Size()); 
		m_excerpt_hit_score.AllocateMemory(CWordDiv::WordIDSet().Size()); 
	}

	// This is used to approximate the hit score for a spatial region 
	// given a hit segment list. More weight is given to region with
	// a greater word heterogenity.
	// @param hit_seg_ptr - this is a set of hit segments
	// @param word_div_num - this is the number of unique word ids contained in a given spatial region
	static float SpatialHitScore(SHitSegment *hit_seg_ptr, uChar &word_div_num) {

		m_title_hit_score.InitializeMemoryChunk(0);
		m_excerpt_hit_score.InitializeMemoryChunk(0);

		while(hit_seg_ptr != NULL) {

			if(hit_seg_ptr->hit_type_index == TITLE_HIT_INDEX) {
				m_title_hit_score[hit_seg_ptr->word_id] += (hit_seg_ptr->end_doc_id - hit_seg_ptr->start_doc_id + 1);
			} else {
				m_excerpt_hit_score[hit_seg_ptr->word_id] += (hit_seg_ptr->end_doc_id - hit_seg_ptr->start_doc_id + 1);
			}

			hit_seg_ptr = hit_seg_ptr->next_ptr;
		}

		return MatchScore(word_div_num);
	}
};
CMemoryChunk<float> CSpatialScore::m_title_hit_score;
CMemoryChunk<float> CSpatialScore::m_excerpt_hit_score;


// This stores the set of documents attached to a given ab_node
struct SDocMatch {
	// This stores the document id (node id)
	S5Byte doc_id;
	// This is a flag indicating that this doc hit 
	// belongs to this client and thus should be expanded
	uChar is_client_hit;
	// This stores the title score
	uChar title_div_num;
	// This stores the hit score
	uChar hit_score;
	// This is a predicate indicating whether 
	// a document keyword set has been added
	bool is_added;
	// This is a predicate indicating whether a document
	// is too similar to another document or not
	bool is_valid;
	// This stores the match score
	float match_score;
	// This stores the document rank
	float rank;
	// ptr to the next doc match;
	SDocMatch *next_ptr;
};

// This stores one of the ABNodes in the ABTree 
struct SABTreeNode {
	// This stores the header of the ab_tree node which 
	// supplies various prestored statistics about the ab_node
	SABNode header;
	// This stores the current tree level
	uChar tree_level;
	// This is a predicate that indicates whether a node has been expanded
	uChar node_expanded;
	// This stores the number of nodes grouped under this node
	u_short doc_num;
	// This stores the tree external storage id
	u_short tree_id;
	// This is used for the ab_node cache
	uLong tree_inst_id;

	// This stores the set of document matches
	SDocMatch *doc_match_ptr;
	// This stores a pointer to the next hash node
	SABTreeNode *next_hash_ptr;
	// This stores a pointer to the next cache node
	SABTreeNode *next_cache_ptr;
	// This stores a pointer to the previous cache node
	SABTreeNode *prev_cache_ptr;
	// This stores the byte offset of the node
	_int64 byte_offset;

	// This stores all of the forward s_links out from this node
	SLinkSet *link_set_ptr;
	// This stores parent s_links that have been pushed down
	SLinkSet *parent_link_ptr;
	// This stores a pointe to the child set
	SChildNode *child_ptr;

	// This stores a pointer to the parent ab_tree node
	SABTreeNode *ab_node_parent;
	// This stores the global node bound of the node
	S64BitBound node_bound;

	// This stores the expected reward for a given node
	float exp_rew;
	// This stores the final spatial score combination of exp rew and keyword score
	float spatial_score;
	// This stores the accumulated document keyword score 
	// for all documents under this region in the ab_tree node
	float keyword_score;
	// This stores the net document match score for all documents
	float doc_match_score;

	SABTreeNode() {
		doc_match_ptr = NULL;
		tree_inst_id = 0;
		tree_id = 0;
	}

	// Used when the first loaded into the cache
	void Initialize() {
		Reset();
	}

	// This resets the node in the tree
	void Reset() {
		link_set_ptr = NULL;
		parent_link_ptr = NULL;
		doc_match_ptr = NULL;
		node_expanded = 0;
		spatial_score = 0;
		keyword_score = 0;
		doc_match_score = 0;
		doc_num = 0;
		exp_rew = 0;
	}

	// Return true if a not has previously been expanded
	inline bool IsNodeBranched() {
		return (node_expanded & 0x02) == 0x02;
	}

	// This sets the hits to loaded
	inline void SetNodeBranched() {
		node_expanded |= 0x02;
	}

	// Return true if the s_links have been attatched
	inline bool IsSLinksAttatched() {
		return (node_expanded & 0x04) == 0x04;
	}

	// This sets the hits to loaded
	inline void SetSLinksAttatched() {
		node_expanded |= 0x04;
	}

	// This calculates the doc match score
	void CalculateDocMatchScore() {

		int num = 0;
		doc_match_score = 0;
		SDocMatch *doc_ptr = doc_match_ptr;
		while(doc_ptr != NULL) {
			doc_match_score += doc_ptr->match_score;
			doc_ptr = doc_ptr->next_ptr;
			num++;
		}

		doc_match_score /= (num + 1);
	}
};

int d;
// This class is used to optimize the storing and retrieval of ab_nodes
// that make up the ab_tree which remains persistent for each query.
// In particular before loading an ab_node from external storage it's 
// first checked if the ab_node along with each of its attributes resides
// in the  In this case the ab_node does not need to be reloaded.
// If a match is found then the corresponding cached ab_node is returned,
// otherwise it is added to the  If the no matching ab_node is found
// then the range tree must be searched to find the appropriate ab_node.
// Search for a node in the ab_node cache is based around a hash map so 
// O(1) time complexity can be expected.

// Because it's possible that the entire ab_tree along with all the ab_nodes
// and s_link endpoints may not fit into internal memory, a LRU policy is 
// used to remove nodes from the cache to free up memory for more recent 
// entries. This entire data structure is based around a doubly linked list.
// When a ab_node is accessed its placed at the front of the list, so that
// LRU ab_nodes will be at the end of the list. The evicted node is taken
// from the end of the linked list.
class CABNodeCache : public CNodeStat {

	// This defines the number of hash divisions to use
	static const int HASH_DIV_NUM = 10000;
	// This stores the maximum number of bytes allowed in memory
	static const int MAX_MEM_SIZE = 1100000;

	// This stores all of the ab nodes
	CLinkedBuffer<SABTreeNode> m_ab_node_buff;
	// This stores all the child nodes
	CLinkedBuffer<SChildNode> m_child_node_buff;

	// This is the head of the free ab_node list
	SABTreeNode *m_ab_node_free_ptr;
	// This is the head of the free child node list
	SChildNode *m_child_free_ptr;

	// This stoers the head of the ab_node linked list
	SABTreeNode *m_head_ab_node_ptr;
	// This stores the tail of he ab_node linked list
	SABTreeNode *m_tail_ab_node_ptr;

	// This stores the hash list for all of the ab_nodes
	CMemoryChunk<SABTreeNode *> m_ab_node_hash;
	// This stores the current number of bytes loaded
	int m_bytes_loaded;
	// This stores the current tree expansion instance
	uLong m_tree_exp_inst;
	// This stores the node bounds for which this client is responsible
	S64BitBound m_node_bound;

	// This checks the list for correctness
	void CheckList() {

		int free_num = 0;
		SABTreeNode *curr_ptr = m_ab_node_free_ptr;
		while(curr_ptr != NULL) {
			free_num++;
			curr_ptr = curr_ptr->next_cache_ptr;
		}

		curr_ptr = m_head_ab_node_ptr;

		int num = 0;
		while(curr_ptr->next_cache_ptr != NULL) {
			num++;
			curr_ptr = curr_ptr->next_cache_ptr;
		}

		num++;
		if(abs((m_ab_node_buff.Size() - d - free_num) - num) > 0) {
			cout<<"num error1 "<<free_num<<" "<<m_ab_node_buff.Size()<<" "<<num;getchar();
		}

		if(curr_ptr != m_tail_ab_node_ptr) {
			cout<<"tail error";getchar();
		}

		if(curr_ptr->next_cache_ptr != NULL) {
			cout<<"tail error1";getchar();
		}

		curr_ptr = m_tail_ab_node_ptr;

		num = 0;
		while(curr_ptr->prev_cache_ptr != NULL) {
			num++;
			curr_ptr = curr_ptr->prev_cache_ptr;
		}

		num++;
		if(curr_ptr != m_head_ab_node_ptr) {
			cout<<"head error";getchar();
		}

		if(curr_ptr->prev_cache_ptr != NULL) {
			cout<<"head error1";getchar();
		}
	}

	// This evicts nodes if the memory cache has been exhausted
	inline void EvictNodes() {

		while(m_bytes_loaded >= MAX_MEM_SIZE) {
			if(EvictABNode() == false) {
				break;
			}
		}
	}

	// This returns a free ab_node
	inline SABTreeNode *FreeABNode() {

		EvictNodes();

		m_bytes_loaded += sizeof(SABTreeNode);

		SABTreeNode *ptr = m_ab_node_free_ptr;
		if(ptr == NULL) {
			ptr = m_ab_node_buff.ExtendSize(1);
		} else {
			m_ab_node_free_ptr = m_ab_node_free_ptr->next_hash_ptr;
		}

		return ptr;
	}

	// This returns a free child
	inline SChildNode *FreeChild() {

		EvictNodes();

		SChildNode *ptr = m_child_free_ptr;
		if(ptr == NULL) {
			ptr = m_child_node_buff.ExtendSize(1);
		} else {
			m_child_free_ptr = m_child_free_ptr->next_ptr;
		}

		return ptr;
	}

	// This moves the most recently accessed ab_node to 
	// the head of the LRU list.
	void PromoteABNode(SABTreeNode *ab_node_ptr) {

		if(ab_node_ptr == m_head_ab_node_ptr) {
			// already at the head of the list
			return;
		}

		if(ab_node_ptr == m_tail_ab_node_ptr) {
			// reassigns the tail
			m_tail_ab_node_ptr = ab_node_ptr->prev_cache_ptr;
			m_tail_ab_node_ptr->next_cache_ptr = NULL;
		} else {
			// links up the nodes in the list
			(ab_node_ptr->prev_cache_ptr)->next_cache_ptr = ab_node_ptr->next_cache_ptr;
			(ab_node_ptr->next_cache_ptr)->prev_cache_ptr = ab_node_ptr->prev_cache_ptr;
		}

		SABTreeNode *prev_ptr = m_head_ab_node_ptr;
		m_head_ab_node_ptr = ab_node_ptr;
		m_head_ab_node_ptr->next_cache_ptr = prev_ptr;
		m_head_ab_node_ptr->prev_cache_ptr = NULL;
		prev_ptr->prev_cache_ptr = m_head_ab_node_ptr;
	}

	// This searches for an ab_node at a particular byte offset in the table
	// @param hash - this is the hash division in the table
	// @param byte_offset - this is the byte offset being searched for including
	//                    - the tree division
	SABTreeNode *FindABNode(int hash, _int64 &byte_offset) {

		SABTreeNode *curr_hash_ptr = m_ab_node_hash[hash];
		while(curr_hash_ptr != NULL) {

			if(byte_offset == curr_hash_ptr->byte_offset) {
				PromoteABNode(curr_hash_ptr);
				return curr_hash_ptr;
			}

			curr_hash_ptr = curr_hash_ptr->next_hash_ptr;
		}

		return NULL;
	}

	// This adds a reference to an ab_node to the hash map so that it can be
	// retrieved later if necessary.
	// @param hash - this is the hash division in the table
	// @param ab_node_ptr - this is a ptr to the ab_node being added
	bool RemoveABNodeFromHashMap(SABTreeNode *ab_node_ptr) {

		int hash = (int)ab_node_ptr->byte_offset % m_ab_node_hash.OverflowSize();

		SABTreeNode *curr_ptr = m_ab_node_hash[hash];
		SABTreeNode *prev_ptr = NULL;

		while(curr_ptr != NULL) {
			if(ab_node_ptr->byte_offset == curr_ptr->byte_offset) {
				if(prev_ptr == NULL) {
					m_ab_node_hash[hash] = curr_ptr->next_hash_ptr;
				} else {
					prev_ptr->next_hash_ptr = curr_ptr->next_hash_ptr;
				}

				return false;
			}

			prev_ptr = curr_ptr;
			curr_ptr = curr_ptr->next_hash_ptr;
		}

		return true;
	}

	// This evicts an ab_node along with all of its attributes from the 
	bool EvictABNode() {

		if(m_tail_ab_node_ptr->tree_inst_id == m_tree_exp_inst) {
			return false;
		}

		if(RemoveABNodeFromHashMap(m_tail_ab_node_ptr)) {
			cout<<"not found";getchar();
		}
		
		SABTreeNode *ab_node_ptr = m_tail_ab_node_ptr;

		// frees the ab_node
		m_bytes_loaded -= sizeof(SABTreeNode);
		SABTreeNode *prev_ptr = m_ab_node_free_ptr;
		m_ab_node_free_ptr = ab_node_ptr;
		m_ab_node_free_ptr->next_hash_ptr = prev_ptr;

		// frees child nodes
		SChildNode *curr_child_ptr = ab_node_ptr->child_ptr;
		while(curr_child_ptr != NULL) {
			SChildNode *prev_ptr = m_child_free_ptr;
			m_child_free_ptr = curr_child_ptr;
			curr_child_ptr = curr_child_ptr->next_ptr;
			m_child_free_ptr->next_ptr = prev_ptr;
		}

		m_tail_ab_node_ptr = ab_node_ptr->prev_cache_ptr;
		if(m_tail_ab_node_ptr != NULL) {
			m_tail_ab_node_ptr->next_cache_ptr = NULL;
		}

		return true;
	}

	// This loads in all of the child nodes attatched to an ABNode. Here
	// the byte offset needs to be read in.
	// @param ab_node_ptr - this is the current ab_node being processed
	// @param offset - this is the byte offset where all the s_links are located
	// @param root_byte_offset - this is the byte offset of the 
	//                         - current ab_node being processed
	void LoadChildNodes(SABTreeNode *ab_node_ptr, _int64 &offset, _int64 &root_byte_offset) {

		if(ab_node_ptr->header.child_num <= 0) {
			return;
		}

		SChildNode *head_child_ptr = NULL;
		SChildNode *tail_child_ptr = NULL;

		for(uLong i=0; i<ab_node_ptr->header.child_num; i++) {
			SChildNode *child_node = FreeChild();
			child_node->ab_node_ptr = NULL;

			CByte::ABTreeBytes(ab_node_ptr->tree_id, CUtility::ThirdTempBuffer(), 6, offset);

			int bytes = CArray<char>::GetEscapedItem(CUtility::ThirdTempBuffer(),
				child_node->child_byte_offset);
			
			offset += bytes;
			child_node->child_byte_offset = root_byte_offset
				- child_node->child_byte_offset;

			if(head_child_ptr == NULL) {
				head_child_ptr = child_node;
				tail_child_ptr = child_node;
				continue;
			}

			(tail_child_ptr)->next_ptr = child_node;
			tail_child_ptr = child_node;
		}

		(tail_child_ptr)->next_ptr = NULL;
		ab_node_ptr->child_ptr = head_child_ptr;
	}

	// This function loads in one of the new ab_nodes from external memory
	// @param curr_child - this is the current child for the supplied
	//                   - ab_node that is being processed
	// @param child_node_ptr - this is a ptr to the ab_node to be used
	//                          - for the child node
	// @param ab_node_ptr - this is a pointer to a ab_node in the
    //                    - tree that is being expanded
	// @param node_offset - this stores the current start node bound for 
	//                    - the node being processed
	void LoadABNode(SChildNode *curr_child, SABTreeNode *child_node_ptr,
		SABTreeNode *ab_node_ptr, _int64 &node_offset) {

		static CMemoryChunk<char> ab_buff(sizeof(SABNode));

		CByte::ABTreeBytes(ab_node_ptr->tree_id, ab_buff.Buffer(), 
            ab_buff.OverflowSize(), curr_child->child_byte_offset);

        int bytes = child_node_ptr->header.ReadABNode(ab_buff.Buffer());
		_int64 offset = curr_child->child_byte_offset + bytes;

		child_node_ptr->tree_level = ab_node_ptr->tree_level + 1;
        child_node_ptr->node_bound.start = node_offset;
        child_node_ptr->node_bound.end = node_offset + 
            child_node_ptr->header.total_node_num;

		if(child_node_ptr->node_bound.start >= child_node_ptr->node_bound.end) {
			cout<<"node bound error";getchar();
		}

		node_offset = child_node_ptr->node_bound.end;
        child_node_ptr->ab_node_parent = ab_node_ptr;
		child_node_ptr->tree_id = ab_node_ptr->tree_id;

		if(child_node_ptr->header.child_num == 0) {
			return;
		}

        LoadChildNodes(child_node_ptr, offset, curr_child->child_byte_offset);
	}

	// This adds a new ab_node to the set of cached ab_nodes
	inline void ABNodeToCachedSet(SABTreeNode *ab_node_ptr) {

		SABTreeNode *prev_ptr = m_head_ab_node_ptr;
		m_head_ab_node_ptr = ab_node_ptr;
		m_head_ab_node_ptr->next_cache_ptr = prev_ptr;

		if(prev_ptr != NULL) {
			prev_ptr->prev_cache_ptr = m_head_ab_node_ptr;
		} else {
			m_tail_ab_node_ptr = m_head_ab_node_ptr;
		}

		m_head_ab_node_ptr->prev_cache_ptr = NULL;
	}

	// This adds a reference to an ab_node to the hash map so that it can be
	// retrieved later if necessary.
	// @param hash - this is the hash division in the table
	// @param byte_offset - this is the byte offset being searched for including
	//                    - the tree division
	// @param ab_node_ptr - this is a ptr to the ab_node being added
	void AddABNodeToHashMap(int hash, _int64 &byte_offset, SABTreeNode *ab_node_ptr) {

		ABNodeToCachedSet(ab_node_ptr);
		SABTreeNode *prev_ptr = m_ab_node_hash[hash];
		m_ab_node_hash[hash] = ab_node_ptr;
		ab_node_ptr->next_hash_ptr = prev_ptr;
	}

public:

	CABNodeCache() {

		int hash_num = CHashFunction::FindClosestPrime(HASH_DIV_NUM);
		m_ab_node_hash.AllocateMemory(hash_num, NULL);

		m_ab_node_buff.Initialize(10000);
		m_child_node_buff.Initialize(10000);

		m_ab_node_free_ptr = NULL;
		m_child_free_ptr = NULL;

		m_head_ab_node_ptr = NULL;
		m_tail_ab_node_ptr = NULL;

		m_bytes_loaded = 0;
		m_tree_exp_inst = 0;
	}

	// This is called to initialize the ab_node cache
	// @param client_node_bound - this stores the node bounds for 
	//                          - which this client is responsible
	void Initialize(S64BitBound &client_node_bound) {
		m_node_bound = client_node_bound;
	}

	// This increments the current time stamp for the current
	// session for the ab_tree.
	void SetTimeStamp() {
		m_tree_exp_inst++;
	}

	// This returns an instance of an ab_node. It first does a lookup 
	// operation to see if the node is already visible in the 
	// Otherwise it is loaded into memory. It may be also necessary
	// to evict an ab_node if there is no free memory.
	// @param ab_node_ptr - this ithe parent ab_node whose child nodes 
	//                    - are being attatched
	// @param curr_child_ptr - this is the current child of the ab_node
	//                       - being processed
	// @param node_offset - this is the node bound offset for the root node
	void AttatchChildNode(SABTreeNode *ab_node_ptr, SChildNode *curr_child_ptr, _int64 &node_offset) {

		static _int64 byte_offset;
		byte_offset = curr_child_ptr->child_byte_offset;
		*((u_short *)&byte_offset + 3) = ab_node_ptr->tree_id;
		int hash = (uLong)byte_offset % m_ab_node_hash.OverflowSize();

		SABTreeNode *child_node_ptr = FindABNode(hash, byte_offset);

		if(child_node_ptr == NULL) {
			// need to load the ab_node
			child_node_ptr = FreeABNode();
			child_node_ptr->Initialize();
			child_node_ptr->byte_offset = byte_offset;

			LoadABNode(curr_child_ptr, child_node_ptr, ab_node_ptr, node_offset);
			AddABNodeToHashMap(hash, byte_offset, child_node_ptr);
		} else {
			node_offset += child_node_ptr->header.total_node_num;	
		}

		child_node_ptr->Reset();
		child_node_ptr->tree_inst_id = m_tree_exp_inst;
		curr_child_ptr->ab_node_ptr = child_node_ptr;
		child_node_ptr->ab_node_parent = ab_node_ptr;

		EvictNodes();
	}

	// This adds an arbitrary node to the cache. It first does a lookup 
	// operation to see if the node is already visible in the 
	// Otherwise it is loaded into memory. It may be also necessary
	// to evict an ab_node if there is no free memory.
	// @param conn - this stores the connection to the query server
	// @return the pointer to the root ab_node
	SABTreeNode *AddABNode(COpenConnection &conn) {
		
		static u_short tree_id;
		static _int64 byte_offset;
		conn.Receive((char *)&tree_id, sizeof(u_short));
		conn.Receive((char *)&byte_offset, sizeof(_int64));

		bool load_child = false;
		*((u_short *)&byte_offset + 3) = tree_id;
		int hash = (uLong)byte_offset % m_ab_node_hash.OverflowSize();
		SABTreeNode *ab_node_ptr = FindABNode(hash, byte_offset);

		if(ab_node_ptr == NULL) {
			ab_node_ptr = FreeABNode();
			ab_node_ptr->tree_inst_id = m_tree_exp_inst;
			AddABNodeToHashMap(hash, byte_offset, ab_node_ptr);
			load_child = true;
		}

		ab_node_ptr->Initialize();
		*((u_short *)&byte_offset + 3) = 0;

		conn.Receive((char *)&ab_node_ptr->header, sizeof(SABNode));
		conn.Receive((char *)&ab_node_ptr->node_bound, sizeof(S64BitBound));
		conn.Receive((char *)&ab_node_ptr->tree_level, sizeof(uChar));

		ab_node_ptr->tree_inst_id = m_tree_exp_inst;
		ab_node_ptr->ab_node_parent = NULL;
		ab_node_ptr->tree_id = tree_id;
		load_child |= ab_node_ptr->header.child_num > 0 && ab_node_ptr->child_ptr == NULL;

		if(load_child == true) {
			_int64 offset = byte_offset + ab_node_ptr->header.header_byte_num;
			ab_node_ptr->byte_offset = byte_offset;

			LoadChildNodes(ab_node_ptr, offset, byte_offset);
		}

		int child_num = 0;
		SChildNode *curr_ptr = ab_node_ptr->child_ptr;
		while(curr_ptr != NULL) {
			curr_ptr = curr_ptr->next_ptr;
			child_num++;
		}

		if(child_num != ab_node_ptr->header.child_num) {
			cout<<"Child Num Mismatch "<<child_num<<" "<<ab_node_ptr->header.child_num;getchar();
		}

		return ab_node_ptr;
	}

	// This adds a root ab_node, once at the very start. Note that root
	// ab nodes are always kept in the cache and are never removed.
	// @param byte_offset - this is the byte offset of the root ab_node
	// @param node_offset - this is the node bound offset for the root node
	// @param tree_id - this is the current tree being processed
	// @return the pointer to the root ab_node
	SABTreeNode *AddRootABNode(_int64 &byte_offset, _int64 &node_offset, int tree_id) {

		static CMemoryChunk<char> ab_buff(sizeof(SABNode));

		CByte::ABTreeBytes(tree_id, ab_buff.Buffer(), 
			ab_buff.OverflowSize(), byte_offset);

		SABTreeNode *ab_node_ptr = m_ab_node_buff.ExtendSize(1);
		m_bytes_loaded += sizeof(SABTreeNode);
		ab_node_ptr->Initialize();

		int bytes = ab_node_ptr->header.ReadABNode(ab_buff.Buffer());
		ab_node_ptr->node_bound.start = node_offset;
		ab_node_ptr->node_bound.end = ab_node_ptr->node_bound.start
			+ ab_node_ptr->header.total_node_num;

		ab_node_ptr->ab_node_parent = NULL;
		ab_node_ptr->tree_id = tree_id;
		ab_node_ptr->tree_level = 1;
		ab_node_ptr->byte_offset = byte_offset;

		_int64 offset = byte_offset + bytes;
		LoadChildNodes(ab_node_ptr, offset, byte_offset);
		ab_node_ptr->byte_offset = byte_offset;

		return ab_node_ptr;
	}
};

// This class is used to find and store a set of ranges. It's designed to find
// the range in which a particular node resides. It does this by using a hashed
// red black tree. Specifically a node is first hashed into a given node range 
// and then a red black tree is used to perform a binary search to find the node
// in the correct range.
class CRangeTree {

	// This stores the breadth of the top level hash map
	static const int HASH_BREADTH = 100;

	// This stores one of the nodes in the red black tree
	struct SNode {
		// This stores the node bound
		S64BitBound bound;
		// This is the value stored in the range tree
		SABTreeNode *node;
	};

	// This stores a pointer to the red black tree node
	struct SNodePtr {
		SNode *ptr;
	};

	// This defines one of the hash nodes in the ab_tree
	struct SHashNode {
		// This stores the node bound of the root node
		S64BitBound node_bound;
		// This stores a pointer to the next hash node
		SHashNode *next_ptr;
		// This stores a pointer to the red black tree
		CRedBlackTree<SNodePtr> *tree_ptr;
	};

	// This stores each of the red black trees in 
	// each hash division
	CMemoryChunk<CRedBlackTree<SNodePtr> > m_hash_tree;
	// This stores all of the nodes in the red black tree
	CLinkedBuffer<SNode> m_tree_node_buff;
	// This stores the hash tree for the root level
	CMemoryChunk<SHashNode *> m_root_hash_list;
	// This stores all of the hash nodes
	CLinkedBuffer<SHashNode> m_hash_node_buff;
	// This stores the range of nodes for the hash map
	S64BitBound m_node_range;
	// This stores the hash breadth 
	_int64 m_hash_breadth;

	// This is used to compare nodes in the red black tree
	static int CompareTreeNode(const SNodePtr &arg1, const SNodePtr &arg2) {

		if(arg1.ptr->bound.end <= arg2.ptr->bound.start) {
			return 1;
		}

		if(arg1.ptr->bound.start >= arg2.ptr->bound.end) {
			return -1;
		}

		return 0;
	}

	// This finds the red black tree associated with a given
	// node id. This is done by a hash map lookup.
	// @param bound - this stores the node bound that is 
	//              - being added to the tree
	CRedBlackTree<SNodePtr> *FindTree(S64BitBound &bound) {

		_int64 hash_div = bound.start - m_node_range.start;
		hash_div /= m_hash_breadth;
		SHashNode *curr_ptr = m_root_hash_list[(int)hash_div];

		while(curr_ptr != NULL) {

			if(bound.start >= curr_ptr->node_bound.start &&
				bound.start < curr_ptr->node_bound.end) {

				return curr_ptr->tree_ptr;
			}
			curr_ptr = curr_ptr->next_ptr;
		}

		return NULL;
	}

public:

	CRangeTree() {
		m_hash_node_buff.Initialize();
		m_root_hash_list.AllocateMemory(HASH_BREADTH, NULL);
		m_tree_node_buff.Initialize();

		m_hash_tree.AllocateMemory(HASH_BREADTH);
		for(int i=0; i<m_hash_tree.OverflowSize(); i++) {
			m_hash_tree[i].Initialize(100, CompareTreeNode);
		}
	}

	// This is called to initialize the range tree
	// @param node_range - this is the range of nodes
	//                   - for which this class is responsible
	void Initialize(S64BitBound &node_range) {
		m_node_range = node_range;
		m_hash_breadth = m_node_range.Width() / HASH_BREADTH;
		m_hash_breadth++;
	}

	// This creates the set of hash nodes that are used to lookup
	// the root ab_node in the hash structure. It's possible that
	// a root node overlaps multiple hash divisions, in which case
	// multiple references to the ab_node need to be stored.
	// @param bound - this stores the node bound that is 
	//              - being added to the tree
	// @param ab_node_ptr - this is the ab_node that is being stored
	void CreateRootHashNode(S64BitBound &bound, SABTreeNode *ab_node_ptr) {

		_int64 hash_div_start = bound.start - m_node_range.start;
		_int64 hash_div_end = hash_div_start + bound.Width() - 1;
		hash_div_start /= m_hash_breadth;
		hash_div_end /= m_hash_breadth;
		hash_div_end++;

		CRedBlackTree<SNodePtr> *tree_ptr = &m_hash_tree[(int)hash_div_start];

		static SNodePtr ptr;
		ptr.ptr = m_tree_node_buff.ExtendSize(1);
		ptr.ptr->bound = bound;
		ptr.ptr->node = ab_node_ptr;
		tree_ptr->AddNode(ptr);

		for(int j=(int)hash_div_start; j<hash_div_end; j++) {
			SHashNode *prev_ptr = m_root_hash_list[j]; 
			m_root_hash_list[j] = m_hash_node_buff.ExtendSize(1);
			m_root_hash_list[j]->next_ptr = prev_ptr;
			m_root_hash_list[j]->node_bound = bound;
			m_root_hash_list[j]->tree_ptr = tree_ptr;
		}
	}

	// This adds a node bound to the tree
	// @param bound - this stores the node bound that is 
	//              - being added to the tree
	// @param ab_node_ptr - this is the ab_node that is being stored
	void AddBound(S64BitBound &bound, SABTreeNode *ab_node_ptr) {

		CRedBlackTree<SNodePtr> &tree = *FindTree(bound);

		static SNodePtr ptr;
		ptr.ptr = m_tree_node_buff.ExtendSize(1);
		ptr.ptr->bound = bound;
		ptr.ptr->node = ab_node_ptr;

		tree.AddNode(ptr);
	}

	// This deletes a node bound from the tree
	void DeleteBound(S64BitBound &bound) {

		CRedBlackTree<SNodePtr> &tree = *FindTree(bound);

		static SNode node;
		node.bound = bound;

		static SNodePtr ptr;
		ptr.ptr = &node;
		tree.DeleteNode(ptr);
	}

	// This returns the value that's associated with the most
	// closest matching node bound in the tree.
	// @param node_id - this is the node id that is being searched
	//                - for in the tree
	// @return the matching ab_node, NULL otherwise
	SABTreeNode *FindMatchingNode(_int64 node_id) {

		if(node_id < m_node_range.start) {
			return NULL;
		}

		if(node_id >= m_node_range.end) {
			return NULL;
		}

		static S64BitBound bound;
		bound.start = node_id;
		CRedBlackTree<SNodePtr> &tree = *FindTree(bound);

		static SNode node;
		node.bound.start = node_id;
		node.bound.end = node_id + 1;

		static SNodePtr ptr;
		ptr.ptr = &node;

		SNodePtr *res = tree.FindNode(ptr);
		if(res == NULL) {
			return NULL;
		}

		if(node_id < res->ptr->node->node_bound.start || node_id >= res->ptr->node->node_bound.end) {
			cout<<"match error";getchar();
		}

		return res->ptr->node;
	}

	// This is called to remove all the entries in the hash 
	// tree so the tree can be arbitrarily expanded again
	// @param root_node_set - this stores the set of root ab_nodes
	void Reset(CArrayList<SABTreeNode *> &root_node_set) {

		m_tree_node_buff.Restart();

		for(int i=0; i<m_hash_tree.OverflowSize(); i++) {
			if(m_hash_tree[i].Size() > 0) {
				m_hash_tree[i].Reset();
			}
		}

		for(int i=0; i<root_node_set.Size(); i++) {
			SABTreeNode *ab_node_ptr = root_node_set[i];
			_int64 hash_div_start = ab_node_ptr->node_bound.start - m_node_range.start;
			hash_div_start /= m_hash_breadth;

			CRedBlackTree<SNodePtr> *tree_ptr = &m_hash_tree[(int)hash_div_start];

			static SNodePtr ptr;
			ptr.ptr = m_tree_node_buff.ExtendSize(1);
			ptr.ptr->bound = ab_node_ptr->node_bound;
			ptr.ptr->node = ab_node_ptr;
			tree_ptr->AddNode(ptr);
		}
	}
};