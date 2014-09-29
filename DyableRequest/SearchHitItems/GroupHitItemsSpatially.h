#include "./CompileRankedList.h"

// This class is responsible for searching through all of the hit items
// to find doc id's that have a high keyword match score. This is done
// using a heuristic search. The heuristic search is based on two factors
// the density of hit items in a given spatial region and the overall
// expected reward of a region. The hit items are searched according to
// the spatial regions in which they are grouped. This introduces the idea
// of overlapping sets to which a set of hit items are grouped. When a 
// node in the ab_tree is expanded than the set of hit items grouped in 
// that region must be subdivided into the child regions. This is done 
// using a binary split. That is a binary search is used to search for
// all hits with doc id's that fall in the given range.
class CGroupHitItemsSpatially {

	// This stores the partition doc_id
	_int64 m_part_doc_id;
	// This stores the boundary partition byte offset
	_int64 m_part_byte_offset;
	// This stores the set of left hit segments grouped together
	SHitSegment *m_left_hit_seg_ptr;
	// This stores the set of right hit segments grouped together
	SHitSegment *m_right_hit_seg_ptr;
	// This stores the number of excerpt hits in the left segment
	int m_left_excerpt_num;
	// This stores the number of excerpt hits in the left segment
	int m_right_excerpt_num;
	// This stores the total number of bytes that compose a hit segment set
	_int64 m_left_hit_bytes;
	// This stores the total number of bytes that compose a hit segment set
	_int64 m_right_hit_bytes;
	// This stores the set of free hit segments
	SHitSegment *m_free_ptr;

	// This stores a pointer to the current word division being processed
	CWordDiv *m_curr_word_div;
	// This stores the hit item byte offset for the first hit in a given
	// spatial region boundary belonging to one of the children
	CMemoryChunk<_int64> m_hit_item_bound;
	// This stores the search path for hit item boundaries
	CArray<SHitBoundary> m_search_path;

	// This is used to test the partition
	void TestPartition(SHitSegment *hit_seg_ptr) {

		_int64 compare = 0;

		if(m_left_hit_seg_ptr->start_doc_id > m_left_hit_seg_ptr->end_doc_id) {
			cout<<"left error";getchar();
		}

		if(m_right_hit_seg_ptr->start_doc_id > m_right_hit_seg_ptr->end_doc_id) {
			cout<<"right error";getchar();
		}

		if(m_left_hit_seg_ptr != NULL) {
			compare += m_left_hit_seg_ptr->byte_bound.Width();
		}

		if(m_right_hit_seg_ptr != NULL) {
			compare += m_right_hit_seg_ptr->byte_bound.Width();
		}

		if(compare != hit_seg_ptr->byte_bound.Width()) {
			//cout<<"byte mis";getchar();
		}

		if(m_left_hit_seg_ptr != NULL && m_left_hit_seg_ptr->end_doc_id >= m_part_doc_id) {
			if(m_left_hit_seg_ptr->end_doc_id != m_left_hit_seg_ptr->start_doc_id) {
				cout<<"left mis";getchar();
			}
		}

		if(m_right_hit_seg_ptr != NULL && m_right_hit_seg_ptr->start_doc_id < m_part_doc_id) {
			cout<<"right mis "<<m_right_hit_seg_ptr->start_doc_id<<" "<<m_right_hit_seg_ptr->end_doc_id<<" "<<m_part_doc_id;getchar();
			cout<<"left mis "<<m_left_hit_seg_ptr->start_doc_id<<" "<<m_left_hit_seg_ptr->end_doc_id<<" "<<m_part_doc_id;getchar();
		}
	}

	// This function adds a hit segment to one of the child terminal lists if
	// it contains doc id's in the range of the child node bound. It must first
	// determine the start and end doc id. The end doc id can be found at the next
	// sequential hit segment byte offset. If this end doc_id exceeds the current
	// ab_node_bound than the doc id at the previous byte offset is taken.
	S64BitBound AddTerminalHitSegment(SHitSegment *hit_seg_ptr) {

		static S64BitBound doc_id(0, 0);
		m_curr_word_div->RetrieveHitBytes((char *)&doc_id.end, 5,
			m_part_byte_offset, m_curr_hit_type);

		m_curr_word_div->RetrieveHitBytes((char *)&doc_id.start, 5,
			max(hit_seg_ptr->byte_bound.start, 
			m_part_byte_offset - HIT_BYTE_NUM), m_curr_hit_type);

		SHitSegment *prev_ptr = m_left_hit_seg_ptr;
		m_left_hit_seg_ptr = NextHitSegment();
		m_left_hit_seg_ptr->word_id = hit_seg_ptr->word_id;
		m_left_hit_seg_ptr->hit_type_index = m_curr_hit_type;
		m_left_hit_seg_ptr->word_div = hit_seg_ptr->word_div;

		m_left_hit_seg_ptr->start_doc_id = hit_seg_ptr->start_doc_id;
		m_left_hit_seg_ptr->end_doc_id = doc_id.start;
		m_left_hit_seg_ptr->byte_bound.Set(hit_seg_ptr->byte_bound.start, m_part_byte_offset);
		m_left_hit_seg_ptr->next_ptr = prev_ptr;
		m_left_excerpt_num += (hit_seg_ptr->hit_type_index == EXCERPT_HIT_INDEX);
		m_left_hit_bytes += m_left_hit_seg_ptr->byte_bound.Width();
		
		prev_ptr = m_right_hit_seg_ptr;
		m_right_hit_seg_ptr = NextHitSegment();
		m_right_hit_seg_ptr->word_id = hit_seg_ptr->word_id;
		m_right_hit_seg_ptr->hit_type_index = m_curr_hit_type;
		m_right_hit_seg_ptr->word_div = hit_seg_ptr->word_div;

		m_right_hit_seg_ptr->start_doc_id = doc_id.end;
		m_right_hit_seg_ptr->end_doc_id = hit_seg_ptr->end_doc_id;
		m_right_hit_seg_ptr->byte_bound.Set(m_part_byte_offset, hit_seg_ptr->byte_bound.end);
		m_right_hit_seg_ptr->next_ptr = prev_ptr;
		m_right_excerpt_num += (hit_seg_ptr->hit_type_index == EXCERPT_HIT_INDEX);
		m_right_hit_bytes += m_right_hit_seg_ptr->byte_bound.Width();

		return doc_id;
	}

	// This performs a linear search in order to find the beginning of a doc id set.
	// This is used when a doc id is found that matches the doc id that's being searched for.
	// @param byte_bound - this stores the byte bound of the hit segment
	// @return the byte offset of the first hit item with the same doc id
	inline _int64 FindBeginningDocID(S64BitBound &byte_bound, _int64 byte_offset) {

		_int64 byte_offset2 = byte_offset;
		_int64 curr_doc_id1 = m_part_doc_id;
		_int64 curr_doc_id2 = m_part_doc_id;
		while(true) {

			if(byte_offset2 >= byte_bound.end && byte_offset <= byte_bound.start) {
				return byte_bound.start + HIT_BYTE_NUM;
			}

			if(byte_offset > byte_bound.start) {
				byte_offset -= HIT_BYTE_NUM;
				m_curr_word_div->RetrieveHitBytes((char *)&curr_doc_id1,
					5, byte_offset, m_curr_hit_type);
			}

			byte_offset2 += HIT_BYTE_NUM;
			if(byte_offset < byte_bound.end) {
				m_curr_word_div->RetrieveHitBytes((char *)&curr_doc_id2,
					5, byte_offset2, m_curr_hit_type);
			}

			if(curr_doc_id1 != m_part_doc_id) {
				return byte_offset + HIT_BYTE_NUM;
			}

			if(curr_doc_id2 != m_part_doc_id) {
				return byte_offset2 - HIT_BYTE_NUM;
			}
		}

		return byte_offset + HIT_BYTE_NUM;
	}

	// This creates the next level search nodes
	// @param hit_bound - this stores the current hit item boundary
	// @param mid_byte_offset - this is the byte offset of the hit item that's in the
	//                        - middle of the current hit segment 
	// @param doc_id - this stores the two middle doc ids
	void CreateNextLevelSearchNodes(SHitBoundary &hit_bound, _int64 &mid_byte_offset, S64BitBound &doc_id) {


		if(m_part_doc_id <= doc_id.start) {
			SHitBoundary *left_hit_bound_ptr = m_search_path.ExtendSize(1);
			left_hit_bound_ptr->byte_bound.start = hit_bound.byte_bound.start;
			left_hit_bound_ptr->byte_bound.end = mid_byte_offset;
			left_hit_bound_ptr->doc_bound.start = hit_bound.doc_bound.start;
			left_hit_bound_ptr->doc_bound.end = doc_id.start;
		} else {

			SHitBoundary *right_hit_bound_ptr = m_search_path.ExtendSize(1);
			right_hit_bound_ptr->byte_bound.start = mid_byte_offset;
			right_hit_bound_ptr->byte_bound.end = hit_bound.byte_bound.end;
			right_hit_bound_ptr->doc_bound.start = doc_id.end;
			right_hit_bound_ptr->doc_bound.end = hit_bound.doc_bound.end;
		}
	}

	// This peforms the binary search on all the different child nodes until
	// each child node has been resolved. That is a child node has found the 
	// corresponding byte offset for their given document bound. This uses
	// a recursive procedure to perform the binary search.
	void RecurseOnChildren() {

		static S64BitBound doc_id;
		static _int64 mid_byte_offset;
		doc_id.start = 0;
		doc_id.end = 0;

		while(true) {

			SHitBoundary hit_bound = m_search_path.LastElement();
			mid_byte_offset = (hit_bound.byte_bound.start + hit_bound.byte_bound.end) >> 1;
			mid_byte_offset -= mid_byte_offset % HIT_BYTE_NUM;

			if(hit_bound.byte_bound.Width() <= HIT_BYTE_NUM) {	
				m_part_byte_offset = FindBeginningDocID(hit_bound.byte_bound, mid_byte_offset);
				break;
			}

			m_curr_word_div->RetrieveHitBytes((char *)&doc_id.end, 5,
				mid_byte_offset, m_curr_hit_type);

			if(doc_id.end == m_part_doc_id) {
				m_part_byte_offset = FindBeginningDocID(hit_bound.byte_bound, mid_byte_offset);
				break;
			}

			m_curr_word_div->RetrieveHitBytes((char *)&doc_id.start, 5,
				mid_byte_offset - HIT_BYTE_NUM, m_curr_hit_type);

			CreateNextLevelSearchNodes(hit_bound, mid_byte_offset, doc_id);
		}
	}

	// This partitions the search nodes into cached hit boundary regions
	// @param tree - this stores the current cached tree being processed
	// @param hit_seg_ptr - this is the current hit segment being processed
	void PartitionSearchNodes(SHitSegment *hit_seg_ptr) {

		static SHitBoundary hit_bound;
		hit_bound.doc_bound.start = -1;
		hit_bound.doc_bound.end = -1;

		hit_bound.byte_bound = hit_seg_ptr->byte_bound;
		hit_bound.doc_bound.start = hit_seg_ptr->start_doc_id;
		hit_bound.doc_bound.end = hit_seg_ptr->end_doc_id;

		m_search_path.Resize(0);
		m_search_path.PushBack(hit_bound);

		RecurseOnChildren();
	}

	// This takes a set of child nodes and a hit segment and subdivides the hit
	// segments among each of the child nodes. A binary search is used to split
	// all relevant children into their correct groups.
	// @param hit_seg_ptr - the current set of hit segments that are bein split
	// @param max_ptr - this stores the maximum document bound
	void GroupHitItems(SHitSegment *hit_seg_ptr, SHitSegment *max_ptr, bool is_divide) {

		static int div;
		while(hit_seg_ptr != NULL) {

			if(hit_seg_ptr == max_ptr) {
				hit_seg_ptr = hit_seg_ptr->next_ptr;
				continue;
			}

			m_curr_hit_type = hit_seg_ptr->hit_type_index;
			m_curr_word_div = &m_word_div[hit_seg_ptr->word_div];
			SHitSegment *next_ptr = hit_seg_ptr->next_ptr;

			if(hit_seg_ptr->start_doc_id >= m_part_doc_id) {
				SHitSegment *prev_ptr = m_right_hit_seg_ptr;
				m_right_hit_seg_ptr = NextHitSegment();
				*m_right_hit_seg_ptr = *hit_seg_ptr;
				m_right_hit_seg_ptr->next_ptr = prev_ptr;
				m_right_hit_bytes += m_right_hit_seg_ptr->byte_bound.Width();
				hit_seg_ptr = next_ptr;
				continue;
			}

			if(hit_seg_ptr->end_doc_id < m_part_doc_id) {
				SHitSegment *prev_ptr = m_left_hit_seg_ptr;
				m_left_hit_seg_ptr = NextHitSegment();
				*m_left_hit_seg_ptr = *hit_seg_ptr;
				m_left_hit_seg_ptr->next_ptr = prev_ptr;
				m_left_hit_bytes += m_left_hit_seg_ptr->byte_bound.Width();
				hit_seg_ptr = next_ptr;
				continue;
			}
			
			if(is_divide == true) {
				div = (hit_seg_ptr->word_id << 1) + hit_seg_ptr->hit_type_index;
				PartitionSearchNodes(hit_seg_ptr);
				AddTerminalHitSegment(hit_seg_ptr);
			} else {

				SHitSegment *prev_ptr = m_left_hit_seg_ptr;
				m_left_hit_seg_ptr = NextHitSegment();
				*m_left_hit_seg_ptr = *hit_seg_ptr;
				m_left_hit_seg_ptr->next_ptr = prev_ptr;

				prev_ptr = m_right_hit_seg_ptr;
				m_right_hit_seg_ptr = NextHitSegment();
				*m_right_hit_seg_ptr = *hit_seg_ptr;
				m_right_hit_seg_ptr->next_ptr = prev_ptr;

				m_left_hit_bytes += m_left_hit_seg_ptr->byte_bound.Width();
				m_right_hit_bytes += m_right_hit_seg_ptr->byte_bound.Width();
			}

			hit_seg_ptr = next_ptr;
		}
	}

protected:

	// This defines the number of bytes that make up a hit
	static const int HIT_BYTE_NUM = 6;

	// This stores the current hit type being processed
	int m_curr_hit_type;
	// This buffer is used to store all of the hit segments 
	// created during the search. 
	CLinkedBuffer<SHitSegment> m_hit_seg_buff;
	// This stores all of the different word divisions that 
	// make up the query. Hits are extracted from each
	// respective word division.
	CMemoryChunk<CWordDiv> m_word_div;

public:

	CGroupHitItemsSpatially() {
		m_hit_seg_buff.Initialize();
		m_search_path.Initialize(4096);
		m_free_ptr = NULL;
	}

	// This returns the set of left adjusted hit segments
	inline SHitSegment *LeftHitSegments() {
		return m_left_hit_seg_ptr;
	}

	// This returns the set of right adjusted hit segments
	inline SHitSegment *RightHitSegments() {
		return m_right_hit_seg_ptr;
	}

	// This returns the number of left hit segment bytes
	inline _int64 LeftHitSegmentBytes() {
		return m_left_hit_bytes;
	}

	// This returns the set of right adjusted hit segments
	inline _int64 RightHitSegmentBytes() {
		return m_right_hit_bytes;
	}

	// This returns the next free hit segment
	inline SHitSegment *NextHitSegment() {

		if(m_free_ptr == NULL) {
			return m_hit_seg_buff.ExtendSize(1);
		}

		SHitSegment *ptr = m_free_ptr;
		m_free_ptr = m_free_ptr->next_ptr;
		return ptr;
	}

	// This adds the next free hit item
	inline void AddFreeHitSegment(SHitSegment *hit_seg_ptr) {

		SHitSegment *curr_ptr = hit_seg_ptr;
		while(curr_ptr->next_ptr != NULL) {
			curr_ptr = curr_ptr->next_ptr;
		}

		SHitSegment *prev_ptr = m_free_ptr;
		m_free_ptr = hit_seg_ptr;
		curr_ptr->next_ptr = prev_ptr;
	}

	// This divides a hit segment up into it's constituent children. The bounds
	// used to subdivided children are given by the ab_tree children. A binary
	// search is used to split all relevant children into their correct groups.
	// @param hit_seg_ptr - the current set of hit segments that are bein split
	bool SubdivideHitSegment(SHitSegment *hit_seg_ptr) {

		m_part_doc_id = 0;
		SHitSegment *curr_ptr = hit_seg_ptr;
		SHitSegment *max_ptr = hit_seg_ptr;

		S64BitBound doc_bound(0, 0);
		*((uLong *)&doc_bound.start) = 0xFFFFFFFF;
		*((uLong *)&doc_bound.start + 1) = 0x1FFFFFFF;

		while(curr_ptr != NULL) {

			if((curr_ptr->end_doc_id - curr_ptr->start_doc_id) > (max_ptr->end_doc_id - max_ptr->start_doc_id)) {
				max_ptr = curr_ptr;
			}

			doc_bound.start = min(doc_bound.start, curr_ptr->start_doc_id);
			doc_bound.end = max(doc_bound.end, curr_ptr->end_doc_id);

			curr_ptr = curr_ptr->next_ptr;
		}

		if(doc_bound.start == doc_bound.end) {
			return false;
		}

		m_left_hit_seg_ptr = NULL;
		m_right_hit_seg_ptr = NULL;
		m_left_hit_bytes = 0;
		m_right_hit_bytes = 0;

		if(max_ptr->start_doc_id == max_ptr->end_doc_id) {
			// can't subdivide the hit segment
			m_part_doc_id = (doc_bound.start + doc_bound.end) >> 1;
			m_part_doc_id = max(m_part_doc_id, doc_bound.start + 1);
			GroupHitItems(hit_seg_ptr, NULL, false);
			return true;
		}

		m_curr_hit_type = max_ptr->hit_type_index;
		m_curr_word_div = &m_word_div[max_ptr->word_div];
		m_part_byte_offset = (max_ptr->byte_bound.start + max_ptr->byte_bound.end) >> 1;
		m_part_byte_offset -= m_part_byte_offset % HIT_BYTE_NUM;

		m_part_doc_id = 0;
		m_curr_word_div->RetrieveHitBytes((char *)&m_part_doc_id, 5,
			m_part_byte_offset, m_curr_hit_type);
		m_part_byte_offset = FindBeginningDocID(max_ptr->byte_bound, m_part_byte_offset);

		m_curr_word_div->RetrieveHitBytes((char *)&m_part_doc_id, 5,
			m_part_byte_offset, m_curr_hit_type);

		AddTerminalHitSegment(max_ptr);

		GroupHitItems(hit_seg_ptr, max_ptr, false);

		return true;
	}

	// This divides a hit segment up into it's constituent children. The bounds
	// used to subdivided children are given by the ab_tree children. A binary
	// search is used to split all relevant children into their correct groups.
	// @param hit_seg_ptr - the current set of hit segments that are bein split
	// @param part_doc_id - this partition doc id
	void SubdivideHitSegment(SHitSegment *hit_seg_ptr, _int64 part_doc_id) {

		m_left_hit_seg_ptr = NULL;
		m_right_hit_seg_ptr = NULL;
		m_part_doc_id = part_doc_id;
		GroupHitItems(hit_seg_ptr, NULL, true);
	}

	// This resets ready for the next query
	void Reset() {
		m_hit_seg_buff.Restart();
		m_search_path.Resize(0);
		m_free_ptr = NULL;
	}

};