#include "./GroupHitItemsSpatially.h"

// This defines a structure used store a priority spatial region.
// This is used as part of a priority queue when searching for 
// hits in optimal spatial regions in the graph.
struct SPriorityRegion {
	// This stores a pointer to the hit segment set
	SHitSegment *hit_seg_ptr;
	// This stores the number of hit bytes
	_int64 hit_bytes;
	// This stores the spatial score for the hit segment set
	float spatial_score;
	// This stores the rank of the region
	int rank;
	// This stores the number of unique terms in the spatial region
	uChar unique_word_num;
	// This stores the current depth of the recursion
	u_short tree_level;

	static bool is_set_spatial;

	// This assigns the spatial score 
	inline void AssignSpatialScore() {
		spatial_score = CSpatialScore::SpatialHitScore(hit_seg_ptr, unique_word_num);
	}	
};
bool SPriorityRegion::is_set_spatial;

// To facilitate a search through the hit items, a priority queue is used
// to progressively subdivide promising regions. When a region is 
// subdivided it's parent is removed from the queue and the children are 
// added to the queue. Upon reaching an external nodes in a tree that 
// correspond to a given document Bellman upadates are performed to propagate
// the hit score to neighbouring documents. This update is used to bias the 
// search towards regions that have a high expected hit score return. Updates 
// are only performed on promising nodes to save time during the search.
class CSearchHitItems : public CGroupHitItemsSpatially {

	// This defines the maximum number of nodes to transfer when
	// subdividing the search space
	static const int MAX_TRAN_NODE_NUM = 10;
	// This stores the maximum number of allowed title hits, before 
	// the query is supplemented with additional terms
	static const int MAX_TITLE_HIT_NUM = 3000;

	// This stores all of the documents that have been found
	// in the search process and later ranks them based 
	// upon their keyword match score
	CCompileRankedList m_doc_list;
	// This stores the high priority spatial regions that are 
	// continously pulled off the priority queue.
	CLimitedPQ<SPriorityRegion> m_hit_queue;

	// This stores the number of levels to partition the hit segments
	int m_part_level_num;
	// This stores the current rank
	int m_curr_rank;
	// This stores the maximum number of unique word ids found in a given document
	int m_max_word_div_num;
	// This is used for performance profiling
	CStopWatch m_timer;

	// This is used to compare different regions of the search space based 
	// on the desirability of the spatial region
	static int CompareSpatialRegion(const SPriorityRegion &arg1,
		const SPriorityRegion &arg2) {

		if(arg1.unique_word_num < arg2.unique_word_num) {
			return -1;
		}

		if(arg1.unique_word_num > arg2.unique_word_num) {
			return 1;
		}

		if(arg1.spatial_score < arg2.spatial_score) {
			return -1;
		}

		if(arg1.spatial_score > arg2.spatial_score) {
			return 1;
		}

		if(arg1.tree_level < arg2.tree_level) {
			return -1;
		}

		if(arg1.tree_level > arg2.tree_level) {
			return 1;
		}

		return 0;
	}

	// This adds a new hit item. If the document that belongs to this hit
	// item does not exist then it needs to be created. Also the hit score
	// for the document must be updated every time a new hit is discovered.
	// @param hit_seg_ptr - this is a pointer to the hit segment for 
	//                    - which the hit items are being extracted
	// @param doc_ptr - this is a ptr to the current document being processed
	void AddHitItemSet(SHitSegment *hit_seg_ptr, SDocument *doc_ptr) {

		m_timer.StartTimer();
		static SHitItem hit_item;
		_int64 byte_offset = hit_seg_ptr->byte_bound.start;
		CWordDiv &word_div = m_word_div[hit_seg_ptr->word_div];
		bool foun = false;
		while(byte_offset < hit_seg_ptr->byte_bound.end) {
			word_div.RetrieveHitBytes(CUtility::SecondTempBuffer(), 
				HIT_BYTE_NUM, byte_offset, hit_seg_ptr->hit_type_index);
			foun = true;
			byte_offset += hit_item.ReadHitExcWordID(CUtility::SecondTempBuffer());

			SDocHitItem *prev_ptr = doc_ptr->hit_ptr;
			doc_ptr->hit_ptr = CAddKeywords::NextHitItem();

			SDocHitItem *curr_ptr = doc_ptr->hit_ptr;
			curr_ptr->next_ptr = prev_ptr;
			curr_ptr->pos = hit_item.enc;
			curr_ptr->enc = hit_seg_ptr->hit_type_index;
			curr_ptr->word_id = hit_seg_ptr->word_id;
		}
		if(foun == false) {cout<<"bo";getchar();}
		m_timer.StopTimer();
	}

	// This adds all the hit items contained in a given hit segment for 
	// a particular word div to the document set. This is so a set of 
	// ranked documents can be compiled.
	// @param region - the region from which hits are being extracted
	void AddHitItems(SPriorityRegion &region) {

		if(region.unique_word_num < m_max_word_div_num) {
			return;
		}

		SDocument *excerpt_doc_ptr = NULL;
		SHitSegment *hit_seg_ptr = region.hit_seg_ptr;

		while(hit_seg_ptr != NULL) {

			if(excerpt_doc_ptr == NULL) {
				excerpt_doc_ptr = m_doc_list.NewExcerptDocument(hit_seg_ptr->start_doc_id,
					region.unique_word_num, hit_seg_ptr->hit_type_index);

				if(excerpt_doc_ptr == NULL) {
					// This document has already been added
					return;
				}

				excerpt_doc_ptr->word_div_num = max(excerpt_doc_ptr->word_div_num, region.unique_word_num);
				excerpt_doc_ptr->node_id = hit_seg_ptr->start_doc_id;
				excerpt_doc_ptr->hit_ptr = NULL;
				excerpt_doc_ptr->rank = region.rank;
			}

			AddHitItemSet(hit_seg_ptr, excerpt_doc_ptr);
			hit_seg_ptr = hit_seg_ptr->next_ptr;
		}

		m_max_word_div_num = max(m_max_word_div_num, region.unique_word_num);
	}

	// This adds the root search nodes from the root of the tree to the queue
	// @param region - the region from which hits are being extracted
	// @param tree_level - this is the current level in the hiearchy
	void AddSearchNodes(SPriorityRegion &region, u_short tree_level = 0) {

		if(SubdivideHitSegment(region.hit_seg_ptr) == false) {
			AddHitItems(region);
			return;
		}

		AddFreeHitSegment(region.hit_seg_ptr);

		int rank = m_curr_rank + region.rank;
		region.rank = rank;
		m_curr_rank++;

		region.hit_seg_ptr = LeftHitSegments();
		region.tree_level = tree_level;
		region.hit_bytes = LeftHitSegmentBytes();

		region.AssignSpatialScore();

		if(region.hit_seg_ptr != NULL && region.unique_word_num >= m_max_word_div_num) {
			m_hit_queue.AddItem(region);

			SPriorityRegion *ptr = m_hit_queue.LastDeletedItem();
			if(ptr != NULL) {
				AddFreeHitSegment(ptr->hit_seg_ptr);
			}
		}

		region.hit_seg_ptr = RightHitSegments();
		region.hit_bytes = RightHitSegmentBytes();
		region.AssignSpatialScore();

		if(region.hit_seg_ptr != NULL && region.unique_word_num >= m_max_word_div_num) {
			m_hit_queue.AddItem(region);

			SPriorityRegion *ptr = m_hit_queue.LastDeletedItem();
			if(ptr != NULL) {
				AddFreeHitSegment(ptr->hit_seg_ptr);
			}
		}
	}

	// This is used to partition a hit segment into multiple segments before commencing
	void PartitionHitSegment(SHitSegment *hit_seg_ptr, int level) {

		if(level >= m_part_level_num) {
			SPriorityRegion region;
			region.hit_seg_ptr = hit_seg_ptr;
			region.rank = 0;
			region.AssignSpatialScore();

			AddSearchNodes(region, 0);

			return;
		}

		SHitSegment *curr_ptr = hit_seg_ptr;

		S64BitBound doc_bound(0, 0);
		*((uLong *)&doc_bound.start) = 0xFFFFFFFF;
		*((uLong *)&doc_bound.start + 1) = 0x1FFFFFFF;

		while(curr_ptr != NULL) {
			doc_bound.start = min(doc_bound.start, curr_ptr->start_doc_id);
			doc_bound.end = max(doc_bound.end, curr_ptr->end_doc_id);
			curr_ptr = curr_ptr->next_ptr;
		}

		SubdivideHitSegment(hit_seg_ptr, (doc_bound.start + doc_bound.end) >> 1);

		AddFreeHitSegment(hit_seg_ptr);

		uChar word_div1;
		uChar word_div2;
		SHitSegment *left_hit_ptr = LeftHitSegments();
		SHitSegment *right_hit_ptr = RightHitSegments();

		CSpatialScore::SpatialHitScore(left_hit_ptr, word_div1);
		CSpatialScore::SpatialHitScore(right_hit_ptr, word_div2);

		if(left_hit_ptr != NULL && word_div1 >= m_max_word_div_num) {
			PartitionHitSegment(left_hit_ptr, level + 1);
		}

		if(right_hit_ptr != NULL && word_div2 >= m_max_word_div_num) {
			PartitionHitSegment(right_hit_ptr, level + 1);
		}
	}

	// This is called to add the first node in the search tree. This creates 
	// the list of hit segments for each word division and attatches it to
	// the priority node. This is then taken off the priority queue on the 
	// next pass to process all the relevant hits.
	// @param hit_type - this stores all of the different hit types 
	//                 - that are being searched in the given order
	void InitializePrioritySearch(int hit_type) {

		SHitSegment *hit_seg_ptr = NULL;
		S64BitBound doc_bound(0, 0);

		for(int i=0; i<m_word_div.OverflowSize(); i++) {

			if(m_word_div[i].HitByteNum(hit_type) <= 0) {
				continue;
			}
				
			SHitSegment *prev_hit_seg = hit_seg_ptr;
			hit_seg_ptr = m_hit_seg_buff.ExtendSize(1);
			hit_seg_ptr->start_doc_id = 0;
			hit_seg_ptr->end_doc_id = 0;


			hit_seg_ptr->next_ptr = prev_hit_seg;
			hit_seg_ptr->byte_bound.start = 0;
			hit_seg_ptr->byte_bound.end = m_word_div[i].HitByteNum(hit_type);
			hit_seg_ptr->hit_type_index = hit_type;
			hit_seg_ptr->word_id = CWordDiv::WordIDSet(i).local_id;
			hit_seg_ptr->word_div = i;

			m_word_div[i].RetrieveHitBytes((char *)&hit_seg_ptr->start_doc_id, 5, 0, hit_type);

			m_word_div[i].RetrieveHitBytes((char *)&hit_seg_ptr->end_doc_id,
				5, hit_seg_ptr->byte_bound.end - HIT_BYTE_NUM, hit_type);

			cout<<hit_seg_ptr->start_doc_id<<" "<<hit_seg_ptr->end_doc_id<<" "<<hit_type<<" (((("<<endl;

			doc_bound.end = max(doc_bound.end, hit_seg_ptr->end_doc_id);
		}
		
		if(hit_seg_ptr == NULL) {
			return;
		}

		CHashFunction::BoundaryPartion(CNodeStat::GetClientID(), 
			CNodeStat::GetClientNum(), doc_bound.end, doc_bound.start);

		if(CNodeStat::GetClientID() > 0) {
			SubdivideHitSegment(hit_seg_ptr, doc_bound.start);
			hit_seg_ptr = RightHitSegments(); 
		}

		if(CNodeStat::GetClientID() < CNodeStat::GetClientNum() - 1) {
			SubdivideHitSegment(hit_seg_ptr, doc_bound.end);
			hit_seg_ptr = LeftHitSegments(); 
		}

		if(hit_seg_ptr != NULL) {
			PartitionHitSegment(hit_seg_ptr, 0);
		}
	}

	// This sends information relating to a particular ab_node to the query server
	// so the search space can be duplicated.
	// @param region - the current priority region being processed
	void SendABNodeInfo(COpenConnection &conn, SPriorityRegion &region) {

		conn.Send((char *)&region.tree_level, sizeof(uChar));
		SHitSegment *hit_seg_ptr = region.hit_seg_ptr;

		uChar hit_seg_num = 0;
		while(hit_seg_ptr != NULL) {
			hit_seg_ptr = hit_seg_ptr->next_ptr;
			hit_seg_num++;
		}

		conn.Send((char *)&hit_seg_num, sizeof(uChar));
		hit_seg_ptr = region.hit_seg_ptr;
		while(hit_seg_ptr != NULL) {
			conn.Send((char *)hit_seg_ptr, sizeof(SHitSegment));
			hit_seg_ptr = hit_seg_ptr->next_ptr;
		}
	}

	// This subdivides the search space for longer search times. By subdividing the 
	// search space the level of parallelization is increased by a factor of two.
	// To subdivide the search space the top N nodes are subdivided equal amongst
	// two parallel seach streams. 
	void SubdivideSearchNodes(COpenConnection &conn) {

		int max_search_num = min(m_hit_queue.Size() >> 1, MAX_TRAN_NODE_NUM);

		for(int i=0; i<max_search_num; i++) {

			SPriorityRegion region = m_hit_queue.PopItem();

			conn.Send((char *)&region.tree_level, sizeof(uChar));
			SHitSegment *hit_seg_ptr = region.hit_seg_ptr;

			uChar hit_seg_num = 0;
			while(hit_seg_ptr != NULL) {
				hit_seg_ptr = hit_seg_ptr->next_ptr;
				hit_seg_num++;
			}

			conn.Send((char *)&hit_seg_num, sizeof(uChar));
			hit_seg_ptr = region.hit_seg_ptr;
			while(hit_seg_ptr != NULL) {
				conn.Send((char *)hit_seg_ptr, sizeof(SHitSegment));
				hit_seg_ptr = hit_seg_ptr->next_ptr;
			}
		}

		cout<<"k000000000000000000000000000"<<endl;
	}

	// This expands priority regions in the search queue
	// @param max_it - this is the maximum number of iterations to perform
	bool ExpandSearchRegions(int max_it) {

		cout<<"expand in "<<max_it<<endl;
		int it_num = 0;
		while(m_hit_queue.Size() > 0) {
			if(++it_num >= max_it) {
				cout<<"MAXXEDDDDDDDDDDDDDDDDD OUT"<<endl;
				return false;
			}

			if(m_doc_list.DocumentNum() >= MAX_TITLE_HIT_NUM) {
				return true;
			}

			if((it_num % 10000) == 0) {
				cout<<it_num<<" Out Of "<<max_it<<" "<<m_max_word_div_num<<" "<<m_hit_queue.Size()<<" "<<m_doc_list.DocumentNum()<<" "<<m_max_word_div_num<<endl;
			}

			SPriorityRegion curr_reg = m_hit_queue.PopItem();

			if(curr_reg.unique_word_num < m_max_word_div_num) {
				cout<<"no max hit left "<<m_max_word_div_num<<endl;
				return false;
			}

			AddSearchNodes(curr_reg, curr_reg.tree_level + 1);
		}

		cout<<"expand out"<<endl;

		return false;
	}

	// This creates the set of word id terms
	void CreateWordIDSet() {

		CSpatialScore::Initialize();
		m_word_div.AllocateMemory(CWordDiv::WordIDSet().Size());
		int offset = 0;

		for(int i=0; i<m_word_div.OverflowSize(); i++) {
			if(m_word_div[offset].Initialize(CWordDiv::WordIDSet(offset).word_id) == false) {
				CWordDiv::WordIDSet().DeleteElement(offset);
				continue;
			}

			offset++;
		}

		m_word_div.Resize(CWordDiv::WordIDSet().Size());
	}

public:

	CSearchHitItems() {
		m_hit_queue.Initialize(8000, CompareSpatialRegion);
	}

	// This returns a pointer to the instance of this class
	inline CSearchHitItems *ThisPtr() {
		return this;
	}

	// This returns a reference to the document list 
	inline CCompileRankedList &DocList() {
		return m_doc_list;
	}

	// This is called to perform the heuristic search for doc id's with
	// high correspondence to the query. Here a limit is supplied on the
	// maximum number of hits to extract before the search is terminated.
	// @param conn - this stores the connection to the query server
	// @param doc_sub_size - this is the number of high priority documents
	//                     - to compile once the searching process is finished
	// @param max_it - this is the maximum number of iterations
	void PerformSearch(COpenConnection &conn, int max_it) {

		CreateWordIDSet();
		m_max_word_div_num = 0;
		m_part_level_num = 15;
		InitializePrioritySearch(TITLE_HIT_INDEX);

		ExpandSearchRegions(200000);

		if(m_max_word_div_num < CWordDiv::ClusterTermNum() || m_doc_list.DocumentNum() < 1000) {
			m_hit_queue.Reset();
			m_part_level_num = 5;
			InitializePrioritySearch(EXCERPT_HIT_INDEX);
			ExpandSearchRegions(max_it);
		} else {
			m_doc_list.AttachKeywordHits();
		}

		m_doc_list.AttachExcerptHits();
		CGroupHitItemsSpatially::Reset();

		int dummy = -1;
		conn.Send((char *)&dummy, sizeof(int));
		conn.Send((char *)&m_max_word_div_num, sizeof(int));
		m_doc_list.SendSearchResults(conn, m_max_word_div_num);
	}

	// This resets the system ready for the next query
	void Reset() {
		cout<<"Search "<<m_timer.NetElapsedTime()<<endl;
		CGroupHitItemsSpatially::Reset();
		m_hit_queue.Reset();

		m_doc_list.Reset();
	}
};