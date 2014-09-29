#ifdef OS_WINDOWS
#include "./Render.h"
#else
#include "./ExcerptKeywords.h"
#endif

// This adds additional keywords to the set the set of existing documents
// in order to try and identify important documents as a preprocessing step
class CAddKeywords {

	// This defines the number of bytes that make up a hit
	static const int HIT_BYTE_NUM = 6;

	// This stores an instance of a document
	struct SDocMatch {
		// This stores the ptr to the document
		SDocument *doc_ptr;
		// This is the next ptr
		SDocMatch *next_ptr;
	};

	// This defines a data structure used to hold one of the overlapping
	// hit segments. This just stores the byte boundaries of the hit segment
	// aswell as the doc id boundaries.
	struct SHitSegment {
		// This stores the byte boundary of the hit segment
		S64BitBound byte_bound;
		// This stores the start doc id in the hit segment
		_int64 start_doc_id;
		// This stores the end doc id in the hit segment
		_int64 end_doc_id;
		// This stores the ptr to the set of documents
		SDocMatch *doc_ptr;
		// This stores the spatial score for the region
		float score;
		// This stores the this ptr
		CAddKeywords *this_ptr;
		// This stores the ptr to the next hit segment
		SHitSegment *next_ptr;
	};

	// This stores the current word division
	CWordDiv m_word_div;

	// This stores the hit type
	uChar m_hit_type;
	// This stores the keyword id
	int m_keyword_id;

	// This is used to expand spatial regions
	static CLimitedPQ<SHitSegment *> m_hit_queue;
	// This stores the set of hit segments
	static CLinkedBuffer<SHitSegment> m_hit_seg_buff;
	// This stores the set of keywords
	static CLinkedBuffer<SDocHitItem> m_hit_item_buff;
	// This stores the set of documents
	static CLinkedBuffer<SDocMatch> m_doc_buff;
	// This stores the set of free hit segments
	static SHitSegment *m_free_ptr;

	// This is used to compare spatial regions
	static int CompareRegions(SHitSegment *const &arg1, SHitSegment *const &arg2) {

		if(arg1->score < arg2->score) {
			return -1;
		}

		if(arg1->score > arg2->score) {
			return 1;
		}

		return 0;
	}

	// This performs a linear search in order to find the beginning of a doc id set.
	// This is used when a doc id is found that matches the doc id that's being searched for.
	// @param byte_bound - this stores the byte bound of the hit segment
	// @return the byte offset of the first hit item with the same doc id
	_int64 FindBeginningDocID(S64BitBound &byte_bound, _int64 byte_offset, _int64 part_doc_id) {

		_int64 byte_offset2 = byte_offset;
		_int64 curr_doc_id1 = part_doc_id;
		_int64 curr_doc_id2 = part_doc_id;
		while(true) {

			if(byte_offset2 >= byte_bound.end && byte_offset <= byte_bound.start) {cout<<"bo "<<byte_offset<<" "<<byte_bound.start;getchar();
				return byte_bound.start + HIT_BYTE_NUM;
			}

			byte_offset -= HIT_BYTE_NUM;
			if(byte_offset >= byte_bound.start) {
				m_word_div.RetrieveHitBytes((char *)&curr_doc_id1,
					5, byte_offset, m_hit_type);
			}

			byte_offset2 += HIT_BYTE_NUM;
			if(byte_offset < byte_bound.end) {
				m_word_div.RetrieveHitBytes((char *)&curr_doc_id2,
					5, byte_offset2, m_hit_type);
			}

			if(curr_doc_id1 != part_doc_id) {
				return byte_offset + HIT_BYTE_NUM;
			}

			if(curr_doc_id2 != part_doc_id) {
				return byte_offset2;
			}
		}

		return byte_offset + HIT_BYTE_NUM;
	}

	// This adds a new hit item. If the document that belongs to this hit
	// item does not exist then it needs to be created. Also the hit score
	// for the document must be updated every time a new hit is discovered.
	// @param hit_seg_ptr - this is a pointer to the hit segment for 
	//                    - which the hit items are being extracted
	// @param doc_ptr - this is a ptr to the current document being processed
	void AddHitItemSet(SHitSegment *hit_seg_ptr, SDocMatch *doc_ptr) {

		static SHitItem hit_item;
		_int64 byte_offset = hit_seg_ptr->byte_bound.start;
		
		while(byte_offset < hit_seg_ptr->byte_bound.end) {
			m_word_div.RetrieveHitBytes(CUtility::SecondTempBuffer(), 
				HIT_BYTE_NUM, byte_offset, m_hit_type);

			byte_offset += hit_item.ReadHitExcWordID(CUtility::SecondTempBuffer());

			SDocHitItem *prev_ptr = doc_ptr->doc_ptr->hit_ptr;
			doc_ptr->doc_ptr->hit_ptr = m_hit_item_buff.ExtendSize(1);

			SDocHitItem *curr_ptr = doc_ptr->doc_ptr->hit_ptr;
			curr_ptr->next_ptr = prev_ptr;
			curr_ptr->pos = hit_item.enc;
			curr_ptr->enc = m_hit_type;
			curr_ptr->word_id = m_keyword_id;
		}
	}

	// This group the documents under the children
	// @param left_hit_ptr - the left hit segment
	// @param right_hit_ptr - the right hit segment
	// @param doc_ptr - this stores the set of documents
	// @param part_doc_id - the middle doc id splitting the hit segment
	void GroupChildren(SHitSegment *left_hit_ptr, SHitSegment *right_hit_ptr,
		SDocMatch *doc_ptr, S64BitBound &doc_id) {

		int left_doc_num = 0; 
		int right_doc_num = 0; 

		while(doc_ptr != NULL) {

			SDocMatch *next_ptr = doc_ptr->next_ptr;
			if(doc_ptr->doc_ptr->node_id <= doc_id.start) {
				SDocMatch *prev_ptr = left_hit_ptr->doc_ptr;
				left_hit_ptr->doc_ptr = doc_ptr;
				doc_ptr->next_ptr = prev_ptr;
				left_doc_num++;
			} else if(doc_ptr->doc_ptr->node_id >= doc_id.end) {
				SDocMatch *prev_ptr = right_hit_ptr->doc_ptr;
				right_hit_ptr->doc_ptr = doc_ptr;
				doc_ptr->next_ptr = prev_ptr;
				right_doc_num++;
			}

			doc_ptr = next_ptr;
		}
	

		if(left_doc_num > 0) {
			left_hit_ptr->score = (float)left_doc_num / (left_hit_ptr->end_doc_id - left_hit_ptr->start_doc_id + 1);
			m_hit_queue.AddItem(left_hit_ptr);

			SHitSegment **ptr = m_hit_queue.LastDeletedItem();
			if(ptr != NULL) {
				AddFreeHitSegment(*ptr);
			}
		}

		if(right_doc_num > 0) {
			right_hit_ptr->score = (float)right_doc_num / (right_hit_ptr->end_doc_id - right_hit_ptr->start_doc_id + 1);
			m_hit_queue.AddItem(right_hit_ptr);

			SHitSegment **ptr = m_hit_queue.LastDeletedItem();
			if(ptr != NULL) {
				AddFreeHitSegment(*ptr);
			}
		}
	}

	void check(SHitSegment *hit_seg_ptr) {


		static S64BitBound doc_id(0, 0);
		static _int64 mid_byte_offset;

		m_word_div.RetrieveHitBytes((char *)&doc_id.end, 5,
			hit_seg_ptr->byte_bound.start, m_hit_type);

		m_word_div.RetrieveHitBytes((char *)&doc_id.start, 5,
			hit_seg_ptr->byte_bound.end - HIT_BYTE_NUM, m_hit_type);

		if(doc_id.end != hit_seg_ptr->start_doc_id) {
			cout<<"lo";getchar();
		}

		
		if(doc_id.start != hit_seg_ptr->end_doc_id) {
			cout<<"lo1";getchar();
		}
	}

	// This subdivides the hit segment into two equal parts and partitions the document
	// set appropriately between the two partitions.
	// @param hit_seg_ptr - the current segment being subdivided
	void SubdivideHitSegments(SHitSegment *hit_seg_ptr) {

		static S64BitBound doc_id(0, 0);
		static _int64 mid_byte_offset;
		
		if(hit_seg_ptr->start_doc_id == hit_seg_ptr->end_doc_id) {

			if(hit_seg_ptr->doc_ptr->doc_ptr->node_id != hit_seg_ptr->start_doc_id) {
				cout<<"error "<<hit_seg_ptr->doc_ptr->doc_ptr->node_id<<" "<<hit_seg_ptr->start_doc_id;getchar();
			}

			AddHitItemSet(hit_seg_ptr, hit_seg_ptr->doc_ptr);

			return;
		}

		mid_byte_offset = (hit_seg_ptr->byte_bound.start + hit_seg_ptr->byte_bound.end) >> 1;
		mid_byte_offset -= mid_byte_offset % HIT_BYTE_NUM;

		m_word_div.RetrieveHitBytes((char *)&doc_id.end, 5,
			mid_byte_offset, m_hit_type);

		mid_byte_offset = FindBeginningDocID(hit_seg_ptr->byte_bound, 
			mid_byte_offset, doc_id.end);

		m_word_div.RetrieveHitBytes((char *)&doc_id.end, 5,
			mid_byte_offset, m_hit_type);

		m_word_div.RetrieveHitBytes((char *)&doc_id.start, 5,
			mid_byte_offset - HIT_BYTE_NUM, m_hit_type);

		SHitSegment *left_hit_ptr = NextHitSegment();
		left_hit_ptr->byte_bound.start = hit_seg_ptr->byte_bound.start;
		left_hit_ptr->byte_bound.end = mid_byte_offset;
		left_hit_ptr->start_doc_id = hit_seg_ptr->start_doc_id;
		left_hit_ptr->end_doc_id = doc_id.start;
		left_hit_ptr->doc_ptr = NULL;
		left_hit_ptr->this_ptr = this;
		left_hit_ptr->next_ptr = NULL;

		SHitSegment *right_hit_ptr = NextHitSegment();
		right_hit_ptr->byte_bound.start = mid_byte_offset;
		right_hit_ptr->byte_bound.end = hit_seg_ptr->byte_bound.end;
		right_hit_ptr->start_doc_id = doc_id.end;
		right_hit_ptr->end_doc_id = hit_seg_ptr->end_doc_id;
		right_hit_ptr->doc_ptr = NULL;
		right_hit_ptr->this_ptr = this;
		right_hit_ptr->next_ptr = NULL;

		check(right_hit_ptr);

		GroupChildren(left_hit_ptr, right_hit_ptr, 
			hit_seg_ptr->doc_ptr, doc_id);
	}

	// This loads the hit segment into memory
	SHitSegment *InitializeHitSegment() {

		if(m_word_div.HitByteNum(m_hit_type) <= 0) {
			return NULL;
		}
			
		SHitSegment *hit_seg_ptr = NULL;
		hit_seg_ptr = m_hit_seg_buff.ExtendSize(1);
		hit_seg_ptr->start_doc_id = 0;
		hit_seg_ptr->end_doc_id = 0;

		hit_seg_ptr->byte_bound.start = 0;
		hit_seg_ptr->byte_bound.end = m_word_div.HitByteNum(m_hit_type);
		hit_seg_ptr->this_ptr = this;
		hit_seg_ptr->next_ptr = NULL;

		m_word_div.RetrieveHitBytes((char *)&hit_seg_ptr->start_doc_id, 5, 0, m_hit_type);
		m_word_div.RetrieveHitBytes((char *)&hit_seg_ptr->end_doc_id,
			5, hit_seg_ptr->byte_bound.end - HIT_BYTE_NUM, m_hit_type);

		return hit_seg_ptr;
	}

	// This returns the next free hit segment
	inline static SHitSegment *NextHitSegment() {

		if(m_free_ptr == NULL || 1) {
			return m_hit_seg_buff.ExtendSize(1);
		}

		SHitSegment *ptr = m_free_ptr;
		m_free_ptr = m_free_ptr->next_ptr;
		return ptr;
	}

	// This adds the next free hit item
	inline static void AddFreeHitSegment(SHitSegment *hit_seg_ptr) {
		return;
		SHitSegment *curr_ptr = hit_seg_ptr;
		while(curr_ptr->next_ptr != NULL) {
			curr_ptr = curr_ptr->next_ptr;
		}

		SHitSegment *prev_ptr = m_free_ptr;
		m_free_ptr = hit_seg_ptr;
		curr_ptr->next_ptr = prev_ptr;
	}

public:

	CAddKeywords() {
		m_hit_queue.Initialize(8000, CompareRegions);
		m_hit_seg_buff.Initialize();
		m_hit_item_buff.Initialize();
		m_doc_buff.Initialize();
		m_free_ptr = NULL;
	}

	// This resets the queue for the next round
	static void ResetQueue() {
		m_hit_queue.Reset();
		m_hit_seg_buff.Restart();
		m_doc_buff.Restart();
		m_free_ptr = NULL;
	}

	// This resets the class
	static void Reset() {
		ResetQueue();
		m_hit_item_buff.Restart();
	}

	// This returns the next hit item
	inline static SDocHitItem *NextHitItem() {
		return m_hit_item_buff.ExtendSize(1);
	}

	// This creates the set of documents which are used for intersection
	// @param doc_buff - this stores the set of existing documents
	// @param word_id - this is the id of the keyword set
	// @param local_id - this is the local word id assigned to the keyword
	// @param hit_type - this is the type of hit being added
	void AddDocumentSet(CLinkedBuffer<SDocument> &doc_buff, 
		S5Byte &word_id, int local_id, uChar hit_type, int max_title_div_num) {

		m_keyword_id = local_id;
		m_hit_type = hit_type;
		if(m_word_div.Initialize(word_id) == false) {
			return;
		}

		SHitSegment *hit_seg_ptr = InitializeHitSegment();
		if(hit_seg_ptr == NULL) {
			return;
		}

		int doc_num = 0;
		SDocument *doc_ptr;
		hit_seg_ptr->doc_ptr = NULL;
		doc_buff.ResetPath();
		while((doc_ptr = doc_buff.NextNode()) != NULL) {

			if(doc_ptr->node_id < hit_seg_ptr->start_doc_id || doc_ptr->node_id > hit_seg_ptr->end_doc_id) {
				continue;
			}

			if(doc_ptr->prev_hit_type != TITLE_HIT_INDEX || doc_ptr->title_div_num < max_title_div_num) {
				// only attach keywords to title exclusive documents
				continue;
			}

			SDocMatch *prev_ptr = hit_seg_ptr->doc_ptr;
			hit_seg_ptr->doc_ptr = m_doc_buff.ExtendSize(1);
			hit_seg_ptr->doc_ptr->doc_ptr = doc_ptr;
			hit_seg_ptr->doc_ptr->next_ptr = prev_ptr;
			doc_num++;
		}

		if(doc_num > 0) {
			hit_seg_ptr->score = (float)doc_num / (hit_seg_ptr->end_doc_id - hit_seg_ptr->start_doc_id + 1);
			m_hit_queue.AddItem(hit_seg_ptr);
		}
	}

	// This expands the next highest ranking region
	static bool ExpandRegion() {

		if(m_hit_queue.Size() == 0) {
			return false;
		}

		SHitSegment *hit_ptr = m_hit_queue.PopItem();
		hit_ptr->this_ptr->SubdivideHitSegments(hit_ptr);
		AddFreeHitSegment(hit_ptr);

		return true;
	}
};
CLimitedPQ<CAddKeywords::SHitSegment *> CAddKeywords::m_hit_queue;
CLinkedBuffer<CAddKeywords::SHitSegment> CAddKeywords::m_hit_seg_buff;
CLinkedBuffer<SDocHitItem> CAddKeywords::m_hit_item_buff;
CLinkedBuffer<CAddKeywords::SDocMatch> CAddKeywords::m_doc_buff;
CAddKeywords::SHitSegment *CAddKeywords::m_free_ptr;