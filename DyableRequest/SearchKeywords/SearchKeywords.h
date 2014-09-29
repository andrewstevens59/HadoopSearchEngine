#include "../WordDivision.h"

// This defines a given keyword
struct SKeyword {
	// This stores the keyword id
	u_short keyword_id;
	// This stores the ptr to the next keyword
	SKeyword *next_ptr;
};

// This defines a given document
struct SDocument {
	// This stores the doc id
	S5Byte doc_id;
	// This stores the number of keywords
	u_short keyword_score;
	// This stores the ptr to the keyword set
	SKeyword *keyword_ptr;
	// This stores the ptr to the next document
	SDocument *next_ptr;
};

struct SGlobalKeyword {
	// This stores the keyword id
	S5Byte keyword_id;
	// This stores the occurrence
	uChar occur;
};

// This is used to attach the keywords to each doc match. Keywords are 
// refined by doing a binary search. 
class CSearchKeywords {

	// This defines the number of bytes that make up a hit
	static const int HIT_BYTE_NUM = 6;

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
		SDocument *doc_ptr;
	};

	// This stores the set of sorted keywords
	struct SSortKeyword {
		// This stores the byte bound size
		_int64 byte_bound;
		// This stores the word div
		int word_id;
	};

	// This stores the current word division
	CWordDiv m_word_div;
	// This stores the set of hit segments
	CLinkedBuffer<SHitSegment> m_hit_seg_buff;
	// This stores the set of keywords
	CLinkedBuffer<SKeyword> m_keyword_buff;
	// This stores the set of document
	CArray<SDocument> m_doc_buff;
	// This stores the set of keywords
	CMemoryChunk<SGlobalKeyword> m_keyword_set;
	// This stores the number of documents in the set
	int m_doc_size;

	// This is used to compare keywords
	static int CompareKeywords(const SSortKeyword &arg1, const SSortKeyword &arg2) {

		if(arg1.byte_bound < arg2.byte_bound) {
			return -1;
		}

		if(arg1.byte_bound > arg2.byte_bound) {
			return 1;
		}

		return 0;
	}

	// This is used to compare documents
	static int CompareDocuments(const SDocument &arg1, const SDocument &arg2) {

		if(arg1.keyword_score < arg2.keyword_score) {
			return -1;
		}

		if(arg1.keyword_score > arg2.keyword_score) {
			return 1;
		}

		return 0;
	}

	// This compares by node id
	static int CompareByNodeID(const SDocument &arg1, const SDocument &arg2) {

		_int64 node_id1 = S5Byte::Value(arg1.doc_id);
		_int64 node_id2 = S5Byte::Value(arg2.doc_id);

		if(node_id1 < node_id2) {
			return 1;
		}

		if(node_id1 > node_id2) {
			return -1;
		}

		return 0;
	}

	// This performs a linear search in order to find the end of a doc id set.
	// This is used when a doc id is found that matches the doc id that's being searched for.
	// @param min_byte_bound - this is the lower byte boundary
	// @return the byte offset of the first hit item with the same doc id
	inline _int64 FindEndDocID(_int64 byte_offset, _int64 part_doc_id) {
	
		int count = 0;
		_int64 curr_doc_id = part_doc_id;
		while(curr_doc_id == part_doc_id) {
			if(++count >= 2) {
				return byte_offset;	
			}

			m_word_div.RetrieveHitBytes((char *)&curr_doc_id,
				5, byte_offset, EXCERPT_HIT_INDEX);

			byte_offset += HIT_BYTE_NUM;
		}

		return byte_offset - HIT_BYTE_NUM;
	}

	// This group the documents under the children
	// @param left_hit_ptr - the left hit segment
	// @param right_hit_ptr - the right hit segment
	// @param doc_ptr - this stores the set of documents
	// @param part_doc_id - the middle doc id splitting the hit segment
	// @param keyword_id - the current keyword set being processed
	void GroupChildren(SHitSegment *left_hit_ptr, SHitSegment *right_hit_ptr,
		SDocument *doc_ptr, S64BitBound &doc_id, int keyword_id) {

		while(doc_ptr != NULL) {

			SDocument *next_ptr = doc_ptr->next_ptr;
			if(doc_ptr->doc_id <= doc_id.start) {
				SDocument *prev_ptr = left_hit_ptr->doc_ptr;
				left_hit_ptr->doc_ptr = doc_ptr;
				doc_ptr->next_ptr = prev_ptr;
			} else if(doc_ptr->doc_id >= doc_id.end) {
				SDocument *prev_ptr = right_hit_ptr->doc_ptr;
				right_hit_ptr->doc_ptr = doc_ptr;
				doc_ptr->next_ptr = prev_ptr;
			}

			doc_ptr = next_ptr;
		}

		if(left_hit_ptr->doc_ptr != NULL) {
			SubdivideHitSegments(left_hit_ptr, keyword_id);
		}

		if(right_hit_ptr->doc_ptr != NULL) {
			SubdivideHitSegments(right_hit_ptr, keyword_id);
		}
	}

	// This subdivides the hit segment into two equal parts and partitions the document
	// set appropriately between the two partitions.
	// @param hit_seg_ptr - the current segment being subdivided
	// @param keyword_id - the current keyword set being processed
	void SubdivideHitSegments(SHitSegment *hit_seg_ptr, int keyword_id) {

		static S64BitBound doc_id(0, 0);
		static _int64 mid_byte_offset;

		if(hit_seg_ptr->start_doc_id == hit_seg_ptr->end_doc_id) {

			hit_seg_ptr->byte_bound.end = FindEndDocID(hit_seg_ptr->byte_bound.end, hit_seg_ptr->start_doc_id);
			SKeyword *prev_ptr = hit_seg_ptr->doc_ptr->keyword_ptr;
			hit_seg_ptr->doc_ptr->keyword_ptr = m_keyword_buff.ExtendSize(1);
			hit_seg_ptr->doc_ptr->keyword_ptr->keyword_id = keyword_id;
			hit_seg_ptr->doc_ptr->keyword_ptr->next_ptr = prev_ptr;
			hit_seg_ptr->doc_ptr->keyword_score += min(10, m_keyword_set[keyword_id].occur);

			if(hit_seg_ptr->doc_ptr->doc_id != hit_seg_ptr->start_doc_id) {
				cout<<"error";getchar();
			}
			return;
		}

		mid_byte_offset = (hit_seg_ptr->byte_bound.start + hit_seg_ptr->byte_bound.end) >> 1;
		mid_byte_offset -= mid_byte_offset % HIT_BYTE_NUM;

		m_word_div.RetrieveHitBytes((char *)&doc_id.end, 5,
			mid_byte_offset, EXCERPT_HIT_INDEX);

		m_word_div.RetrieveHitBytes((char *)&doc_id.start, 5,
			mid_byte_offset - HIT_BYTE_NUM, EXCERPT_HIT_INDEX);

		SHitSegment *left_hit_ptr = m_hit_seg_buff.ExtendSize(1);
		left_hit_ptr->byte_bound.start = hit_seg_ptr->byte_bound.start;
		left_hit_ptr->byte_bound.end = mid_byte_offset;
		left_hit_ptr->start_doc_id = hit_seg_ptr->start_doc_id;
		left_hit_ptr->end_doc_id = doc_id.start;
		left_hit_ptr->doc_ptr = NULL;

		SHitSegment *right_hit_ptr = m_hit_seg_buff.ExtendSize(1);
		right_hit_ptr->byte_bound.start = mid_byte_offset;
		right_hit_ptr->byte_bound.end = hit_seg_ptr->byte_bound.end;
		right_hit_ptr->start_doc_id = doc_id.end;
		right_hit_ptr->end_doc_id = hit_seg_ptr->end_doc_id;
		right_hit_ptr->doc_ptr = NULL;

		GroupChildren(left_hit_ptr, right_hit_ptr, 
			hit_seg_ptr->doc_ptr, doc_id, keyword_id);
	}

	// This loads the hit segment into memory
	SHitSegment *InitializeHitSegment(int word_id) {

		if(m_word_div.HitByteNum(EXCERPT_HIT_INDEX) <= 0) {
			return NULL;
		}
			
		SHitSegment *hit_seg_ptr = NULL;
		hit_seg_ptr = m_hit_seg_buff.ExtendSize(1);
		hit_seg_ptr->start_doc_id = 0;
		hit_seg_ptr->end_doc_id = 0;

		hit_seg_ptr->byte_bound.start = 0;
		hit_seg_ptr->byte_bound.end = m_word_div.HitByteNum(EXCERPT_HIT_INDEX);

		m_word_div.RetrieveHitBytes((char *)&hit_seg_ptr->start_doc_id, 5, 0, EXCERPT_HIT_INDEX);
		m_word_div.RetrieveHitBytes((char *)&hit_seg_ptr->end_doc_id,
			5, hit_seg_ptr->byte_bound.end - HIT_BYTE_NUM, EXCERPT_HIT_INDEX);

		return hit_seg_ptr;
	}

	// This is used to attach keywords to the current document set
	void AttachKeywords(int word_id) {

		m_hit_seg_buff.Restart();
		SHitSegment *hit_seg_ptr = InitializeHitSegment(word_id);
		if(hit_seg_ptr == NULL) {
			return;
		}

		hit_seg_ptr->doc_ptr = NULL;
		for(int i=0; i<m_doc_buff.Size(); i++) {
			SDocument &doc = m_doc_buff[i];

			if(doc.doc_id < hit_seg_ptr->start_doc_id || doc.doc_id > hit_seg_ptr->end_doc_id) {
				continue;
			}

			SDocument *prev_ptr = hit_seg_ptr->doc_ptr;
			hit_seg_ptr->doc_ptr = &doc;
			doc.next_ptr = prev_ptr;
		}

		if(hit_seg_ptr->doc_ptr != NULL) {
			SubdivideHitSegments(hit_seg_ptr, word_id);
		}
	}

public:

	CSearchKeywords() {
		m_hit_seg_buff.Initialize();
		m_keyword_buff.Initialize();
	}

	// This adds the set of documents to the set
	void Initialize(COpenConnection &conn) {

		int num;
		conn.Receive((char *)&num, sizeof(int));
		m_doc_buff.Initialize(num);

		cout<<num<<"  #####"<<endl;

		m_hit_seg_buff.Restart();
		m_keyword_buff.Restart();

		for(int i=0; i<m_doc_buff.OverflowSize(); i++) {
			SDocument *doc_ptr = m_doc_buff.ExtendSize(1);
			conn.Receive((char *)&doc_ptr->doc_id, sizeof(S5Byte));
			doc_ptr->keyword_ptr = NULL;
			doc_ptr->next_ptr = NULL;
			doc_ptr->keyword_score = 0;
		}

		m_doc_size = m_doc_buff.Size();

		conn.Receive((char *)&num, sizeof(int));
		m_keyword_set.AllocateMemory(num);
		for(int i=0; i<m_keyword_set.OverflowSize(); i++) {
			conn.Receive((char *)&m_keyword_set[i].keyword_id, sizeof(S5Byte));
			conn.Receive((char *)&m_keyword_set[i].occur, sizeof(uChar));
		}
	}

	// This searches for a given keyword in the result set
	void PerformSearch() {

		float sum = 0;
		for(int j=0; j<m_keyword_set.OverflowSize(); j++) {

			if(m_word_div.Initialize(m_keyword_set[j].keyword_id) == false) {
				continue;
			}

			if(m_word_div.HitByteNum(EXCERPT_HIT_INDEX) <= 0) {
				continue;	
			}

			sum += m_word_div.HitByteNum(EXCERPT_HIT_INDEX);
		}

		int count = 0;
		for(int i=0; i<m_keyword_set.OverflowSize(); i++) {

			m_word_div.Initialize(m_keyword_set[i].keyword_id);
			float factor = m_word_div.HitByteNum(EXCERPT_HIT_INDEX) / sum;
			cout<<factor<<" ((("<<endl;

			if(factor > 0.05) {
				continue;
			}

			if(count >= 27 && m_doc_buff.Size() > 200) {

				CSort<SDocument> sort1(m_doc_buff.Size(), CompareDocuments);
				sort1.HybridSort(m_doc_buff.Buffer());

				m_doc_buff.Resize(m_doc_buff.Size() * 0.75f);
				cout<<m_doc_buff.Size()<<" *************************"<<endl;

				CSort<SDocument> sort2(m_doc_buff.Size(), CompareByNodeID);
				sort2.HybridSort(m_doc_buff.Buffer());
			}

			AttachKeywords(i);
			count++;
		}
	}

	// This sends the set of search results along with the keyword set to the query server
	void CompileSearchResults(COpenConnection &conn) {

		m_doc_buff.Resize(m_doc_size);

		CSort<SDocument> sort1(m_doc_buff.Size(), CompareByNodeID);
		sort1.HybridSort(m_doc_buff.Buffer());

		for(int i=0; i<m_doc_buff.Size(); i++) {
			SDocument &doc = m_doc_buff[i];

			int num = 0;
			SKeyword *keyword_ptr = doc.keyword_ptr;

			while(keyword_ptr != NULL) {
				keyword_ptr = keyword_ptr->next_ptr;
				num++;
			}

			conn.Send((char *)&doc.doc_id, sizeof(S5Byte));

			conn.Send((char *)&num, sizeof(uChar));
			keyword_ptr = doc.keyword_ptr;
			while(keyword_ptr != NULL) {
				conn.Send((char *)&m_keyword_set[keyword_ptr->keyword_id], sizeof(S5Byte));
				keyword_ptr = keyword_ptr->next_ptr;
			}
		}
	}

    // This is used to test the class
	void TestSearchKeywords() {

		CMemoryChunk<S5Byte> buff(10);
		for(int i=0; i<buff.OverflowSize() - 1; i++) {
			buff[i] = rand();
		}

		m_doc_buff.Initialize(1024);
		for(int i=0; i<m_doc_buff.OverflowSize(); i++) {
			SDocument *doc_ptr = m_doc_buff.ExtendSize(1);
			doc_ptr->doc_id = rand();
			doc_ptr->keyword_ptr = NULL;
			doc_ptr->next_ptr = NULL;
		}

		PerformSearch();
	}
};