#include "./MergeSortedBlocks.h"

// This is used to sort a block of items internally. Once the block has
// been sorted it is rewritten to disk.
class CRadixSortBlock {

	// Defines the size of a disk block
	static const int DISK_BLOCK = 512;
	// Defines the maximum amount of memory that 
	// can be utilized for sorting
	static const int MAX_MEM_SIZE = DISK_BLOCK * (1 << 12);

	// This defines a linked bucket item that stores
	// a pointer to the internal buffer where a given 
	// sort item is located.
	struct SNode {
		// This stores a pointer to the next linked node
		SNode *next_ptr;
		// This stores a pointer to the sort item
		char *sort_item;
	};

	// This stores the key set directory
	CHDFSFile m_key_file;
	// This stores the size in bytes of a given sort item
	static int m_hit_byte_size;
	// This stores the size in bytes of a given sort item
	static int m_sort_byte_size;
	// This stores the number of bytes that are used for sorting
	// this is taken as the low order bytes of the sort item
	static int m_compare_byte_size;
	// This stores the initial bit offset -> used in the compare function
	static int m_init_offset;
	// This stores the maximum bit width allowed for radix
	// sort, it's calculated based upon the amount of internal memory
	int m_max_bit_width;

	// This defines how a sort item is compare. Since a non comparison
	// model is used for sorting, this means that this simply orders
	// sort items in lexographic order. Note that an gauranteed
	// extra four bytes that have been nulled need to be placed at
	// the start of the buffer storing sort items, this is because
	// it's possible that the offset could become negativde in the 
	// comparison function (done for efficiency).
	static int CompareLexographicItem(const SExternalSort &arg1, 
		const SExternalSort &arg2) {

		static uLong value1;
		static uLong value2;
		static int offset;

		offset = m_init_offset;
		char *buff1 = arg1.hit_item + m_sort_byte_size;
		char *buff2 = arg2.hit_item + m_sort_byte_size;

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

	// This places each sort item loaded into memory into its repective
	// bucket. This is done by creating a linked list of sort items that
	// are attatched to each bucket. These can than later be retrieved
	// by traversing the linked list of sort items.
	// @param byte_offset - this stores a pointer to the current sort item
	//                    - preloaded into an internal buffer
	// @param sort_num - the number of sort items to be sorted
	// @param curr_bit_offset - the current bit offset from the start of 
	//                        - the sort item that have been processed
	// @param bit_size - the number of bits that forms the mask to select
	//                 - the bucket with repect to the bit offset
	// @param node_buff - this stores all the pointers that are used to
	//                  - create the linked list for each bucket
	// @param bucket_start - this stores a pointer to the head of the linked
	//                     - list for each bucket division
	void AddSortItemsToBuckets(char *byte_offset, int sort_num, 
		int curr_bit_offset, int bit_size, CArray<SNode> &node_buff,
		CMemoryChunk<SNode *> &bucket_start) {

		uLong mask = -1;
		int byte_num = curr_bit_offset >> 3;
		int bit_offset = curr_bit_offset % 8;
		byte_offset += sort_num * m_hit_byte_size;

		mask >>= 32 - min(m_max_bit_width, 
			(m_compare_byte_size << 3) - curr_bit_offset);

		for(int i=0; i<sort_num; i++) {
			byte_offset -= m_hit_byte_size;
			char *byte = byte_offset + byte_num;
			uLong value = *(uLong *)byte;
			value >>= bit_offset;

			node_buff.ExtendSize(1);
			int bucket = value & mask;
			SNode *prev_ptr = bucket_start[bucket];
			SNode *curr_ptr = &node_buff.LastElement();

			bucket_start[bucket] = curr_ptr;
			curr_ptr->next_ptr = (prev_ptr == NULL) ? NULL : prev_ptr;
			curr_ptr->sort_item = byte_offset;
		}
	}

	// This peforms a single bucket sort on a list of items
	// stored in internal memory. This must be applied multiple
	// times to sort the entire sequence of items.
	// @param buffer - the buffer that stores the items that
	//               - need to be sorted
	// @param curr_bit_offset - the current bit offset from the start of 
	//                        - the sort item that have been processed
	// @param sort_num - the number of sort items to be sorted
	void BucketSort(CMemoryChunk<char> &buffer, int sort_num, int curr_bit_offset) {

		CArray<SNode> node_buff(sort_num);
		CMemoryChunk<SNode *> bucket_start;

		int bits_left = (m_compare_byte_size << 3) - curr_bit_offset;
		if(bits_left > m_max_bit_width) {
			bucket_start.AllocateMemory(1 << m_max_bit_width, NULL);
		} else {
			bucket_start.AllocateMemory(1 << bits_left, NULL);
		}

		int bit_size = min(m_max_bit_width, 
			(m_compare_byte_size << 3) - curr_bit_offset);

		CMemoryChunk<char> temp_buff(buffer);
		AddSortItemsToBuckets(temp_buff.Buffer(), sort_num, 
			curr_bit_offset, bit_size, node_buff, bucket_start);

		char *buff = buffer.Buffer();
		for(int i=0; i<bucket_start.OverflowSize(); i++) {
			SNode *curr_ptr = bucket_start[i];

			while(curr_ptr != NULL) {
				memcpy(buff, curr_ptr->sort_item, m_hit_byte_size);
				curr_ptr = curr_ptr->next_ptr;
				buff += m_hit_byte_size;
			}
		}
	}

	// This cycles through the entire bit spectrum of the sort item
	// and uses bucket sort on each segment of the spectrum. Once
	// completely sorted, the buffer is stored externally.
	// @param sort_buff - this stores all the sorted hit items
	// @param sort_item_num - this stores the number of sort items
	void CycleThroughBitWidth(CMemoryChunk<char> &sort_buff, int sort_item_num) {

		int curr_bit_offset = 0;
		while(curr_bit_offset < (m_compare_byte_size << 3)) {
			BucketSort(sort_buff, sort_item_num, curr_bit_offset);
			curr_bit_offset += m_max_bit_width;
		}
	}

	// This finds the maximum bit width increment used for bucket sorting.
	void DetermineBitIncrement() {
		int max_byte_num = MAX_MEM_SIZE;
		max_byte_num -= max_byte_num % m_hit_byte_size;

		m_max_bit_width = CMath::LogBase2(max_byte_num);
		if((m_max_bit_width << 1) % max_byte_num) {
			//align at the bit boundary
			m_max_bit_width--;
		}

		m_max_bit_width = min(m_max_bit_width, 24);
	}

public:

	CRadixSortBlock() {
	}

	// This is the entry function
	// @param work_dir - this is the working directory to store the hashed
	//                 - keys produced as output
	// @param key_set_dir - this is the directory of the key set
	// @param key_set_byte_offset - this stores the byte offset in the 
	//                            - key set file
	// @oaram tuple_bytes - this is the number of bytes to retreive
	// @param sort_byte_size - the size of a given sort item
	// @param compare_byte_size - this is the first N bytes of the sort item to sort on
	void SortBlock(const char work_dir[], const char data_dir[], _int64 key_set_byte_offset,
		_int64 tuple_bytes, int sort_byte_size, int compare_byte_size) {

		if(compare_byte_size < 0) {
			compare_byte_size = sort_byte_size;
		}

		m_compare_byte_size = compare_byte_size;
		m_sort_byte_size = m_compare_byte_size - 4;
		m_init_offset = m_sort_byte_size;

		// this is used in the lexographic compare function
		if(m_init_offset < 0) {
			m_init_offset = -((-m_init_offset) << 3);
		} else {
			m_init_offset <<= 3;
		}

		m_key_file.OpenReadFile(data_dir);
		m_key_file.SeekReadFileFromBeginning(key_set_byte_offset);
		m_hit_byte_size = sort_byte_size;

		CMemoryChunk<char> temp_buff(tuple_bytes);
		m_key_file.ReadCompObject(temp_buff.Buffer(), (int)tuple_bytes);

		DetermineBitIncrement();

		int sort_item_num = tuple_bytes / m_hit_byte_size;
		CycleThroughBitWidth(temp_buff, sort_item_num);

		m_key_file.OpenWriteFile(CUtility::ExtendString(work_dir, CSetNum::GetClientID())); 
		m_key_file.InitializeCompression(1 << 16);
		m_key_file.WriteCompObject(temp_buff.Buffer(), (int)tuple_bytes);
	}
};
int CRadixSortBlock::m_hit_byte_size;
int CRadixSortBlock::m_init_offset;
int CRadixSortBlock::m_compare_byte_size;
int CRadixSortBlock::m_sort_byte_size;

// This is used to sort blocks using quick sort as apposed to radix sort
class CQuickSortBlock {

	// This stores the key set directory
	CHDFSFile m_key_file;

public:

	CQuickSortBlock() {
	}

	// This is the entry function
	// @param work_dir - this is the working directory to store the hashed
	//                 - keys produced as output
	// @param key_set_dir - this is the directory of the key set
	// @param key_set_byte_offset - this stores the byte offset in the 
	//                            - key set file
	// @oaram tuple_bytes - this is the number of bytes to retreive
	// @param sort_byte_size - the size of a given sort item
	// @param compare_func - this is the comparison function used
	void SortBlock(const char work_dir[], const char data_dir[], 
		_int64 key_set_byte_offset, _int64 tuple_bytes, int sort_byte_size,
		int (*compare_func)(const SExternalSort &arg1, const SExternalSort &arg2)) {

		m_key_file.OpenReadFile(data_dir);
		m_key_file.SeekReadFileFromBeginning(key_set_byte_offset);

		CMemoryChunk<char> temp_buff(tuple_bytes);
		CMemoryChunk<SExternalSort> sort_buff(tuple_bytes / sort_byte_size);
		m_key_file.ReadCompObject(temp_buff.Buffer(), (int)tuple_bytes);

		int offset = 0;
		while(tuple_bytes > 0) {
			tuple_bytes -= sort_byte_size;
			sort_buff[offset++].hit_item = temp_buff.Buffer() + tuple_bytes;
		}
		
		CSort<SExternalSort> sort(sort_buff.OverflowSize(), compare_func);
		sort.HybridSort(sort_buff.Buffer());

		m_key_file.OpenWriteFile(CUtility::ExtendString(work_dir, CSetNum::GetClientID())); 
		m_key_file.InitializeCompression(1 << 16);

		for(int i=0; i<sort_buff.OverflowSize(); i++) {
			m_key_file.WriteCompObject(sort_buff[i].hit_item, sort_byte_size);
		}
	}
};