#include "./ExpandABTree.h"

// Used for storing an occurrence of a particular item,
// this are attatched to a document as a linked list
struct SDocHitItem {
	// stores the hit position
	u_short pos; 
	// stores the hit encoding
	u_short enc; 
	// stores the local word id
	int word_id; 
	// stores a ptr to the next hit item
	SDocHitItem *next_ptr; 
}; 

// This defines an instance of a document that groups 
// all the composing hit items. 
struct SDocument {
	// This stores a pointer to the first hit item
	// in the list of linked hit items
	SDocHitItem *hit_ptr;
	// This stores the check sum
	uLong check_sum;
	// This stores the node id
	_int64 node_id;
	// This stores the number of unique terms that make up the hit score
	uChar word_div_num;
	// This stores the number of title hits
	uChar title_div_num;
	// This stores the hit score
	uChar hit_score;
	// This stores the previous hit type for this document
	uChar prev_hit_type;
	// This stores the rank for a given priority node
	int rank;
};

// This is used for calculating the hit score for a given string of words belonging
// to a document. Again priority classes are used to calculate the final score where
// a single string of closely matched heterogeneous terms are more desirable then a
// single term occurring multiple times. Word spacing and ordering are considered 
// with respect to the input query string as well as term weight of each of the 
// individual words.

// In order to prioritise hit scores on the number of terms closely matched in a 
// string, priority classes are used. Different priority classes are designated by 
// the number of homogenously closely matched term for a given string in the document.
// Two two closely matched homogeneous terms means a priority class of two. To ensure
// that lots of lower priority matches to not exceed the score of a single higher 
// priority match a limit is placed on the number of times a given priority class can
// contribute to the overall score. This limit is increased for high priority classes
// so for instance a priority class of two may only be counted 3 times a priority 
// class of four may be counted nine times.

// Also there are multiple gap divisions within a given priority class. A gap division
// is simply the maximum gap for which two terms are considered to be in the same priority
// class. Terms belonging in higher gap divisions will recieve a lower score than terms
// belonging to smaller gap divisions in the same priority class. Eg if for instance
// three different gap divisions are used say 4, 10, 30 than the terms such as |10 12|,
// |20 25| and |30 50| would all fall into the second class priority division. Where each
// would be designated gap divisions of (4), (10) and (30) respectively, |10 12| having
// the highest priority.
class CHitScore {

	// Defines the maximum allowed successive
	// word gap in a given scan path
	CMemoryChunk<int> m_max_word_gap;
	// Stores the number of times a word occurs in a given scan path
	CMemoryChunk<int> m_word_count; 
	// This stores a predicate for each word id found globally
	CMemoryChunk<bool> m_global_word;
	// Used to store the accumalative hit score for 
	// a given excerpt priority division 
	CMemoryChunk<float> m_excerpt_div_score;
	// Used to store the accumalative hit score for 
	// a given title priority division 
	CMemoryChunk<float> m_title_div_score;
	// Used to sort the hit items on position for excerpts
	CArray<SDocHitItem> m_excerpt_hit_buffer; 
	// Used to sort the hit items on position for titles
	CArray<SDocHitItem> m_title_hit_buffer; 

	// Stores the current max word gap division
	int m_curr_gap_div;
	// This stores the total number of unique terms
	int m_global_term_num;
	// This stores the min and max word position
	SBoundary m_word_bound;

	// Used to sort hit items based on position in document
	static int SortHitItem(const SDocHitItem &arg1, const SDocHitItem &arg2) {

		if(arg1.pos < arg2.pos) {
			return -1;
		}
		if(arg1.pos > arg2.pos) {
			return 1; 
		}

		return 0; 
	}

	// Initially loads the linked list into the hit buffer and
	// sorts it based on word position
	// @param hit_item_ptr - a lined list to all the hits in a given document
	// @param enc - this is the type of encoding being exctracted
	// @param hit_buff - this stores all of the hit items for titles or excerpts
	void LoadHitBuffer(SDocHitItem *hit_item_ptr, u_short enc, CArray<SDocHitItem> &hit_buff) {

		hit_buff.Resize(0); 
		m_global_word.InitializeMemoryChunk(false);
		m_global_term_num = 0;

		while(hit_item_ptr != NULL) {

			if(hit_item_ptr->enc != enc && enc == 1) {
				hit_item_ptr = hit_item_ptr->next_ptr; 
				continue;
			}

			if(hit_buff.Size() >= hit_buff.OverflowSize()) {
				break;
			}

			if(m_global_word[hit_item_ptr->word_id] == false) {
				m_global_word[hit_item_ptr->word_id] = true;
				m_global_term_num++;
			}

			m_word_bound.start = min(m_word_bound.start, hit_item_ptr->pos);
			m_word_bound.end = max(m_word_bound.end, hit_item_ptr->pos);
			hit_buff.PushBack(*hit_item_ptr); 
			hit_item_ptr = hit_item_ptr->next_ptr; 
		}
	}

	// This calculates the hit score for a given hit type
	// @param hit_score_buff - this stores the hit score scan div
	float HitScore(CMemoryChunk<float> &hit_score_buff, uLong div_score) {

		float score = 0; 
		for(int i=0; i<CWordDiv::WordIDSet().Size(); i++) {
			if(m_excerpt_div_score[i] > 0) {
				score += div_score + hit_score_buff[i];
			}

			div_score <<= 1;
		}

		return score;
	}

	// This creates the final score by factoring in the priority
	// classes in terms of number of hits in a single scan division.
	// A base 2 scale is used to allocate the number of bits allowed
	// to represent each priority division class starting at 2.
	float CalculateFinalScore() {

		float title_score = HitScore(m_title_div_score, 1);

		float excerpt_score = HitScore(m_excerpt_div_score, 0);

		if(m_excerpt_hit_buffer.Size() == m_title_hit_buffer.Size()) {
			return title_score + m_global_term_num; 
		}

		return max(excerpt_score, title_score) + m_global_term_num; 
	}

	// This is responsible for scaning forwards from the 
	// current word to establish the size of the scan spectrum
	// and sum up the differnt term weights. A scan division is broken
	// when the space between successive terms exceeds some maximum word gap.
	// The final scan division is weighted by the total density of the terms.
	// @param index - the index of the current hit being processed
	// @param scan_size - the number of unique terms in the scan spectrum
	// @param hit_buff - this stores all of the hit items for titles or excerpts
	// @return the score for the given scan spectrum
	float ScanForwards(int &index, int &scan_size, CArray<SDocHitItem> &hit_buff) {

		scan_size = 0; 
		float enc_score = 0; 
		m_word_count.InitializeMemoryChunk(0); 

		while(index < hit_buff.Size()) {
			// updates the encoding score
			int word_id = hit_buff[index].word_id;

			if(m_word_count[word_id] == 0) {
				scan_size++; 
			}

			if(m_word_count[word_id] < 1) {
				m_word_count[word_id]++; 
				enc_score += 1.0f;//CWordDiv::WordIDSet(word_id).factor;
			}

			if(index < hit_buff.Size() - 1) {
				int gap = hit_buff[index+1].pos - hit_buff[index].pos;
				if(abs(gap) > m_max_word_gap[m_curr_gap_div]) {
					break;
				}
			}
			index++;
		}

		return enc_score / m_max_word_gap[m_curr_gap_div]; 
	}

public:

	CHitScore() {
	}

	// This Initializes the hit score 
	void Initialize() {

		m_excerpt_hit_buffer.Initialize(50); 
		m_title_hit_buffer.Initialize(50); 
		m_max_word_gap.AllocateMemory(2);

		m_excerpt_div_score.AllocateMemory(CWordDiv::WordIDSet().Size());
		m_title_div_score.AllocateMemory(CWordDiv::WordIDSet().Size());

		m_word_count.AllocateMemory(CWordDiv::WordIDSet().Size()); 
		m_global_word.AllocateMemory(CWordDiv::WordIDSet().Size()); 

		int base_size = 7;
		int word_gap = base_size;
		for(int i=0; i<m_max_word_gap.OverflowSize(); i++) {
			m_max_word_gap[i] = word_gap;
			word_gap *= base_size;
		}
	}

	// Returns the hit score associated with a list
	// of hit items - stored as linked list
	// @param hit - the hit list for a given document
	float CalculateHitScore(SDocument *doc_ptr) {

		int scan_size = 0; 
		int max_scan_size = 0;

		m_word_bound.start = 0xFF;
		m_word_bound.end = 0;
		LoadHitBuffer(doc_ptr->hit_ptr, 1, m_title_hit_buffer);
		LoadHitBuffer(doc_ptr->hit_ptr, 0, m_excerpt_hit_buffer);
		doc_ptr->word_div_num = m_global_term_num;

		if(m_excerpt_hit_buffer.Size() > 16) {
			// flag this entry as spam to high a match
			return 0;
		}

		// used to sort hit regions based on total hit score
		CSort<SDocHitItem> sort1(m_title_hit_buffer.Size(), SortHitItem);
		sort1.ShellSort(m_title_hit_buffer.Buffer());

		CSort<SDocHitItem> sort2(m_excerpt_hit_buffer.Size(), SortHitItem);
		sort2.ShellSort(m_excerpt_hit_buffer.Buffer());

		doc_ptr->check_sum = 0;
		int bit_size = CMath::LogBase2(min(m_excerpt_hit_buffer.Size(), 15));
		bit_size = max(0, bit_size - 2);
		for(int i=0; i<m_excerpt_hit_buffer.Size(); i++) {
			doc_ptr->check_sum += (m_excerpt_hit_buffer[i].word_id & 0x03) << (2 * (i >> bit_size));
		}

		m_excerpt_div_score.InitializeMemoryChunk(0);
		m_title_div_score.InitializeMemoryChunk(0);

		for(int j=0; j<m_max_word_gap.OverflowSize(); j++) {
			m_curr_gap_div = j;
			for(int i=0; i<m_excerpt_hit_buffer.Size(); i++) {
				// iterates through the different max gap divisions
				float score = ScanForwards(i, scan_size, m_excerpt_hit_buffer); 
				m_excerpt_div_score[scan_size - 1] = max(score, m_excerpt_div_score[scan_size - 1]);
			}
		}

		for(int j=0; j<m_max_word_gap.OverflowSize(); j++) {
			m_curr_gap_div = j;
			for(int i=0; i<m_title_hit_buffer.Size(); i++) {
				// iterates through the different max gap divisions
				float score = ScanForwards(i, scan_size, m_title_hit_buffer); 
				m_title_div_score[scan_size - 1] = max(score, m_title_div_score[scan_size - 1]);
			}
		}

		return CalculateFinalScore(); 
	}
}; 