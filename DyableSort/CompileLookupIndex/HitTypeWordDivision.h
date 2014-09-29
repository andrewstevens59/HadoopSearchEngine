#include "../../MapReduce.h"

// This class is responsible for writing one of the hit types
// excerpt, title, image or other to external file. It's primary
// responsible is to store byte offset information for each client.
class CHitTypeWordDivision {

	// This defines the maximum number of bytes allowed in a given 
	// segmented division
	static const int MAX_SEG_BYTES = 1 << 20;

	// This defines the byte offset and doc id for the start
	// of each segmented division
	struct SHitSegment {
		// This stores the number of bytes in the division
		int byte_num;
		// This stores the doc id before the partition
		_int64 left_doc_id;
		// This stores the doc id after the partition
		_int64 right_doc_id;
	};

	// Stores the final tree organised hit list
	CFileComp m_fin_hit_comp;
	// This stores the byte offset for each client
	_int64 m_hit_byte_offset;
	// This stores the number of bytes in each segmented word division
	CArrayList<SHitSegment> m_seg_div_byte_num;
	// This stores the number of bytes in the current segmented division
	_int64 m_curr_seg_byte_num;
	// This stores the current document id
	_int64 m_curr_doc_id;

public:

	CHitTypeWordDivision() {
	}

	// Called to initialize one of the hit types
	// @param str - used to store the directory for the hit type
	// @param sort_div - the current sort division being processed
	void Initialize(const char str[], int sort_div) {

		m_hit_byte_offset = 0;
		m_seg_div_byte_num.Initialize(2048);
		m_fin_hit_comp.OpenWriteFile(CUtility::ExtendString
			(str, sort_div), HIT_LIST_COMP_SIZE);
	}

	// Resets the document count when a new word 
	// division is being processed
	inline void StartNewDiv() {
		m_hit_byte_offset = m_fin_hit_comp.CompBuffer().BytesStored();
		m_seg_div_byte_num.Resize(0);
		m_curr_seg_byte_num = 0;
	}

	// This finishes the word division by storing relating to the 
	// spatial boundary for each client. This includes the byte offset
	// and the spatial node number.
	// @param lookup_comp - the comp buffer to store the index information
	inline void FinishCurrDiv(CCompression &lookup_comp) {
		lookup_comp.AddHitItem5Bytes(m_hit_byte_offset);
	}

	// This stores the hit segment information
	void StoreHitSegmentInfo() {

		for(int i=0; i<m_seg_div_byte_num.Size(); i++) {
			SHitSegment &seg = m_seg_div_byte_num[i];

			m_fin_hit_comp.WriteCompObject(seg.byte_num);
			m_fin_hit_comp.WriteCompObject((char *)&seg.left_doc_id, sizeof(S5Byte));
			m_fin_hit_comp.WriteCompObject((char *)&seg.right_doc_id, sizeof(S5Byte));
		}

		m_fin_hit_comp.WriteCompObject((int)m_seg_div_byte_num.Size());
	}

	// This prints out the current byte offset
	void PrintByteOffset() {
		cout<<m_fin_hit_comp.CompBuffer().BytesStored()<<" ";
	}

	// This adds a cluster hit to the buffer, with attention 
	// given to the correct client.
	// @param hit_item - stores the hit item being added
	inline void AddClusterHit(SHitItem &hit_item) {

		if(m_curr_seg_byte_num >= MAX_SEG_BYTES) {
			if(hit_item.doc_id != m_curr_doc_id) {
				m_seg_div_byte_num.ExtendSize(1);
				m_seg_div_byte_num.LastElement().byte_num = m_curr_seg_byte_num;
				m_seg_div_byte_num.LastElement().left_doc_id = m_curr_doc_id;
				m_seg_div_byte_num.LastElement().right_doc_id = hit_item.doc_id.Value();
				m_curr_seg_byte_num = 0;
			}
		}

		hit_item.enc >>= 3;
		hit_item.WriteHitExcWordID(m_fin_hit_comp);

		m_curr_seg_byte_num += 6;
		m_curr_doc_id = hit_item.doc_id.Value();
	}
};