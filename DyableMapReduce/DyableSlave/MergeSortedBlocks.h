#include "./FindKeyWeight.h"

// This is used to merge sorted blocks together to complete the external
// sorting pass. Sort items are merged together using a priority queue.
// The sorted items are written back out to file. Multiple sorting passes
// may be required. This process is performed in parallel with a number
// of other sorting streams. 
class CMergeSortedBlocks {	

	// used for the priority queue sorting
	struct SSortedHit {
		SExternalSort hit; 
		int bucket; 
	}; 

	// This stores the size in bytes of a given sort item
	static int m_hit_byte_size;
	// This stores the size in bytes of a given sort item
	static int m_sort_byte_size;
	// This stores the initial bit offset -> used in the compare function
	static int m_init_offset;
	// this is the compare function used to sort hits
	static int (*m_compare)(const SExternalSort & arg1, 
		const SExternalSort & arg2); 

	// This stores the set of sorted blocks
	CMemoryChunk<CHDFSFile> m_sorted_block_set;
	// This stores the set of sorted items
	CPriorityQueue<SSortedHit> m_queue; 
	// This stores the queue buffer items
	CMemoryChunk<char> m_queue_buff;

	// This defines how a sort item is compare. Since a non comparison
	// model is used for sorting, this means that this simply orders
	// sort items in lexographic order. Note that an gauranteed
	// extra four bytes that have been nulled need to be placed at
	// the start of the buffer storing sort items, this is because
	// it's possible that the offset could become negativde in the 
	// comparison function (done for efficiency).
	static int CompareLexographicItem(const SSortedHit &arg1, 
		const SSortedHit &arg2) {

		static uLong value1;
		static uLong value2;
		static int offset;

		offset = m_init_offset;
		char *buff1 = arg1.hit.hit_item + m_sort_byte_size;
		char *buff2 = arg2.hit.hit_item + m_sort_byte_size;

		while(true) {
			value1 = *(uLong *)buff1;
			value2 = *(uLong *)buff2;

			if(offset < 0) {
				value1 >>= -offset;
				value2 >>= -offset;
			}

			if(value1 < value2) {
				return 1;
			}

			if(value1 > value2) {
				return -1;
			}

			if(offset <= 0) {
				return 0;
			}

			offset -= 32;
			buff1 -= 4;
			buff2 -= 4;
		}

		return 0;
	}

	// This is the comparison function used in quick sort
	static int CompareQuickSortItem(const SSortedHit &arg1, 
		const SSortedHit &arg2) {

		return m_compare(arg1.hit, arg2.hit);
	}

	// This initializes the set of bucket hit items
	// @param work_dir - this is the working directory to store the hashed
	//                 - keys produced as output
	// @param div_start - this is the beginning of the set of sorted files
	//					- being merged for this client
	void Initialize(const char work_dir[], int div_start) {

		int offset = 4;
		SSortedHit item; 
		for(int i=0; i<m_sorted_block_set.OverflowSize(); i++) {

			item.bucket = i; 
			item.hit.hit_item = m_queue_buff.Buffer() + offset; 
			m_sorted_block_set[i].OpenReadFile(CUtility::ExtendString(work_dir, i + div_start));

			if(!m_sorted_block_set[i].ReadCompObject(item.hit.hit_item, m_hit_byte_size)) {
				continue;
			}

			m_queue.AddItem(item); 
			offset += m_hit_byte_size; 
		}
	}

	// This is the first pass of the sorting process note bucket number is
	// the max number of buckets that can fit into memory and bucket 
	// set is an index giving the current lot of buckets (bucket num)
	// @param sort_merge_file - this stores tthe final sorted file
	template <class X>
	void SortPass(X &sort_merge_file, bool is_display) {

		SSortedHit item; 
		CArrayList<char *> released_slot(10); 
		int pass = 0;

		while(m_queue.PopItem(item)) {

			sort_merge_file.WriteCompObject(item.hit.hit_item, m_hit_byte_size);
			released_slot.PushBack(item.hit.hit_item); 

			// retrieves a hit from a different bucket if none 
			// left in current bucket
			if(!m_sorted_block_set[item.bucket].ReadCompObject
				(released_slot.LastElement(), m_hit_byte_size)) {
					continue;
			}

			if(is_display == true && ++pass >= 4000000) {
				float perc = 0;
				for(int i=0; i<m_sorted_block_set.OverflowSize(); i++) {
					perc += m_sorted_block_set[i].PercentageFileRead();
				}

				perc /= m_sorted_block_set.OverflowSize();
				cout<<(perc * 100)<<"% Done"<<endl;
				pass = 0;
			}

			// adds another hit back to the priority queue from
			// the same bucket division if bucket division not empty
			item.hit.hit_item = released_slot.PopBack(); 
			m_queue.AddItem(item); 
		}
	}

public:

	CMergeSortedBlocks() {
	}

	// This is the entry function for a radix sort
	// @param work_dir - this is the working directory to store the hashed
	//                 - keys produced as output
	// @param key_set_dir - this is the directory of the key set
	// @param key_set_byte_offset - this stores the byte offset in the 
	//                            - key set file
	// @oaram tuple_bytes - this is the number of bytes to retreive
	// @param sort_byte_size - the size of a given sort item
	// @param compare_byte_size - this is the first N bytes of the sort item to sort on
	void MergeRadixSortedBlocks(const char work_dir[], const char output_dir[], 
		int sort_byte_size, int compare_byte_size, int div_start, int div_end) {

		if(compare_byte_size < 0) {
			compare_byte_size = sort_byte_size;
		}

		m_sort_byte_size = compare_byte_size - 4;
		m_init_offset = m_sort_byte_size;
		m_hit_byte_size = sort_byte_size;

		// this is used in the lexographic compare function
		if(m_init_offset < 0) {
			m_init_offset = -((-m_init_offset) << 3);
		} else {
			m_init_offset <<= 3;
		}

		int bucket_num = div_end - div_start;
		m_sorted_block_set.AllocateMemory(bucket_num);
		m_queue.Initialize(bucket_num, CompareLexographicItem);
		m_queue_buff.AllocateMemory((m_hit_byte_size * bucket_num) + 4); 
		memset(m_queue_buff.Buffer(), 0, 4);

		Initialize(work_dir, div_start);

		if(CSetNum::GetClientID() > 0) {
			CHDFSFile sort_merge_file;
			sort_merge_file.OpenWriteFile(output_dir);
			sort_merge_file.InitializeCompression(1 << 16);
			SortPass(sort_merge_file, false);
		} else {
			CSegFile sort_merge_file;
			sort_merge_file.OpenWriteFile(output_dir);
			sort_merge_file.InitializeCompression(1 << 16);
			SortPass(sort_merge_file, true);
		}
	}

	// This is the entry function for a quick sort
	// @param work_dir - this is the working directory to store the hashed
	//                 - keys produced as output
	// @param key_set_dir - this is the directory of the key set
	// @param key_set_byte_offset - this stores the byte offset in the 
	//                            - key set file
	// @oaram tuple_bytes - this is the number of bytes to retreive
	// @param sort_byte_size - the size of a given sort item
	// @param compare_func - this is the comparison function used
	void MergeQuickSortedBlocks(const char work_dir[], const char output_dir[], 
		int sort_byte_size, int div_start, int div_end, 
		int (*compare_func)(const SExternalSort &arg1, const SExternalSort &arg2)) {

		m_hit_byte_size = sort_byte_size;
		m_compare = compare_func;

		int bucket_num = div_end - div_start;
		m_sorted_block_set.AllocateMemory(bucket_num);
		m_queue.Initialize(bucket_num, CompareQuickSortItem);
		m_queue_buff.AllocateMemory((m_hit_byte_size * bucket_num) + 4); 
		memset(m_queue_buff.Buffer(), 0, 4);

		Initialize(work_dir, div_start);

		if(CSetNum::GetClientID() > 0) {
			CHDFSFile sort_merge_file;
			sort_merge_file.OpenWriteFile(output_dir);
			sort_merge_file.InitializeCompression(1 << 16);
			SortPass(sort_merge_file, false);
		} else {
			CSegFile sort_merge_file;
			sort_merge_file.OpenWriteFile(output_dir);
			sort_merge_file.InitializeCompression(1 << 16);
			SortPass(sort_merge_file, true);
		}
	}
};
int CMergeSortedBlocks::m_hit_byte_size;
int CMergeSortedBlocks::m_sort_byte_size;
int CMergeSortedBlocks::m_init_offset;
int (*CMergeSortedBlocks::m_compare)(const SExternalSort & arg1, const SExternalSort & arg2); 