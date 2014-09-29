#include "./CreateABTrees.h"

// This class handles all overflow s_links. Overflow s_links are s_links
// that have been prempted by higher priority s_links and cannot fit
// under the given node. All overflow s_links are moved down to the next
// level on the hiearchy. Overflow s_links can be handled internally or
// externally. All overflow s_links are first written to an internal <<" ##"
// buffer on their respective level. As the depth first search progresses
// and there isn't enough memory to store all s_links in memory, than 
// the s_links stored in higher level buffers are free and written out
// to external memory. This is done to make the whole process more efficient.
class COverflowSLinks : public CNodeStat {

	// This stores all the s_links being currently processed
	// on a given level of the hiearchy for stationary s_links
	// and overflow s_links
	struct SCurrSLink {
		// This is the current stationary s_link
		SSummaryLink stat_s_link;
		// This is the overflow s_link
		SSummaryLink ovf_s_link;

		SCurrSLink() {
			Initialize();
		}
		
		// This initializes both the stationary and s_links
		// to null values, this indicates that no s_link
		// has currently been stored
		void Initialize() {
			stat_s_link.src = 0;
			ovf_s_link.src = 0;
			stat_s_link.create_level = 0xFF;
			ovf_s_link.create_level = 0xFF;
		}

		// This resets the current overflow s_link
		// to indicate that no s_link has currently been stored
		void ResetOvfLink() {
			ovf_s_link.create_level = 0xFF;
		}

		// This resets the current stationary s_link
		// to indicate that no s_link has currently been stored
		void ResetStatLink() {
			stat_s_link.create_level = 0xFF;
		}

		// returns true if the s_link has been read in
		inline bool StatActive() {
			return stat_s_link.create_level != 0xFF;
		}

		// returns true if the s_link has been read in
		inline bool OvfActive() {
			return ovf_s_link.create_level != 0xFF;
		}
	};

protected:

	// This defines the maximum number of s_links that are allowed
	// to be stored in internal memory
	static const int MAX_MEM_SIZE = 100000000;

	// This stores a ptr to one of the reduced links
	struct SSummaryLinkPtr {
		SSummaryLink *ptr;
	};

	// This stores all the s_links that have been overflowed from
	// each level and are to be prioritized on the next level
	CFileSet<CFileComp> m_s_link_ovf_set;
	// This stores the number of children still to be processed
	// on a given level of the tree
	CArray<SLevel> m_path;

	// This stores the current s_link being processed on a given
	// level after the change over from one subtree in the hiearchy
	// to another subtree
	CMemoryChunk<SCurrSLink> m_curr_s_link;
	// This stores the current global node offset on each
	// level of the hiearchy -> used to perform hiearchy
	// matching when performing the depth first search
	CMemoryChunk<_int64> m_curr_node_offset;
	// This is a buffer that stores the all the s_links that 
	// have been grouped under a given node in the hiearchy
	CArray<SSummaryLink> m_s_link_buff;

	// This is the limited priority queue used to store
	// high priority s_links that are grouped under a given node
	CLimitedPQ<SSummaryLinkPtr> m_s_link_queue;
	// This stores the total number of links that have been 
	// overflowed on all levels traversed so far
	int m_curr_ovf_num;

	// This is a buffer that stores all overflowed s_links 
	// internally they are only written out to file if they
	// cannot fit entirely in internal memory
	CMemoryChunk<CLinkedBuffer<SSummaryLink> > m_s_link_ovf_buff;
	// This stores a pointer to a currently free s_link node
	// that is stored in the priority queue
	SSummaryLink *m_free_s_link_node;

	// This stores the sorted s_links at each level in the
	// hiearchy, these have been sorted by src node id
	static CFileSet<CFileComp> m_s_link_set;

	// This handles the case when there are too many internal s_link
	// and some of the levels need to be written out to file from 
	// the top downwards. That is overflow s_links on each level are
	// written out top down until there is enough internal memory.
	void HandleSLinkFreeing() {

		for(int i=0; i<m_path.Size()-1; i++) {
			CLinkedBuffer<SSummaryLink> &s_link_buff = m_s_link_ovf_buff[i];
			if(s_link_buff.Size() < 1000000) {
				continue;
			}

			m_path[i].flushed = true;
			SSummaryLink *s_link_ptr;
			m_s_link_ovf_set.OpenWriteFile(i);
			CFileComp &set_file = m_s_link_ovf_set.File(i);

			// some of the overflow s_links may have already been
			// used so only store the s_links yet to be processed

			// writes out the buffer to external memory
			while((s_link_ptr = s_link_buff.NextNode()) != NULL) {
				s_link_ptr->WriteReducedSLink(set_file);
			}

			// frees the buffer and reduce the s_link count
			m_curr_ovf_num -= s_link_buff.Size();
			s_link_buff.FreeMemory();

			if(m_curr_ovf_num < MAX_MEM_SIZE) {
				return;
			}
		}
	}

public:

	COverflowSLinks() {
	}

	// This takes the s_links sorted by src at each level and 
	// creates priority groups anchored at each node. Once all
	// nodes in a priority group have been processed they are
	// written out to file, so that they can be processed later.
	// Any links that don't make the cut and pushed down onto then 
	// next level where the processing continues the same way.
	// @param s_link - this is the current s_link being processed
	void ProcessPriorityGroup(SSummaryLink &s_link) {
		
		if(s_link.create_level == 0xFF) {
			// the s_link hasn't been read in yet don't use
			return;
		}

		SSummaryLinkPtr ptr;
		if(m_free_s_link_node != NULL) {
			ptr.ptr = m_free_s_link_node; 
			*m_free_s_link_node = s_link;
		} else {
			m_s_link_buff.PushBack(s_link);
			ptr.ptr = &m_s_link_buff.LastElement(); 
		}

		m_s_link_queue.AddItem(ptr);
		SSummaryLinkPtr *deleted_ptr = m_s_link_queue.LastDeletedItem();
		if(deleted_ptr == NULL) {
			return;
		}

		// this is so s_link nodes can be reused
		m_free_s_link_node = deleted_ptr->ptr;

		// only add the s_link to the next level it wasn't
		// created on the current level
		if(deleted_ptr->ptr->create_level < GetHiearchyLevelNum() - m_path.Size()) {

			if(m_path.LastElement().flushed == true) {
				// write the s_link externally
				CFileComp &set_file = m_s_link_ovf_set.File(m_path.Size()-1);
				deleted_ptr->ptr->WriteReducedSLink(set_file);
				return;
			}
	
			m_s_link_ovf_buff[m_path.Size()-1].PushBack(*deleted_ptr->ptr);

			if(++m_curr_ovf_num >= MAX_MEM_SIZE) {
				HandleSLinkFreeing();
			}
		} 
	}

	// This takes the overflow s_links generated on the previous level 
	// and adds them to the priority queue. It's possible that the s_links
	// could not fit into memory and had to be stored externally. In this
	// case they must be read from an external file. Otherwise they are 
	// stored in an internal buffer and can just be read off.
	// @param node_bound - this is the current node boundary of all links being processed
	void ProcessOverflowSLinks(S64BitBound &node_bound) {

		if(m_path.Size() < 2) {
			return;
		}

		SSummaryLink &s_link = m_curr_s_link[m_path.Size() - 2].ovf_s_link;
		CHDFSFile &s_link_file = CCreateABTree::AB_S_LINK_SET().File(m_path.Size()-1);

		// first process the overflow links 
		if(m_path.SecondLastElement().flushed == true) {
			// process externally
			CFileComp &set_file = m_s_link_ovf_set.File(m_path.Size() - 2);
			if(m_path.SecondLastElement().is_ovf_s_link_open == false) {
				m_s_link_ovf_set.OpenReadFile(m_path.Size() - 2);
				m_path.SecondLastElement().is_ovf_s_link_open = true;
			}

			while(s_link.src < node_bound.end) {

				if(s_link.create_level != 0xFF) {
					if(s_link.src < node_bound.start) {
						cout<<"external s_link bound";getchar();
					}
				}

				ProcessPriorityGroup(s_link);
				if(!s_link.ReadReducedSLink(set_file)) {
					m_curr_s_link[m_path.Size() - 2].ResetOvfLink();
					break;
				}
			}

			return;
		}

		// process internally
		CLinkedBuffer<SSummaryLink> &buff = m_s_link_ovf_buff[m_path.Size() - 2];

		SSummaryLink *s_link_ptr = NULL;
		while(s_link.src < node_bound.end) {

			if(s_link.create_level != 0xFF) {
				if(s_link.src < node_bound.start) {
					cout<<"internal s_link bound";getchar();
				}
			}

			ProcessPriorityGroup(s_link);

			if((s_link_ptr = buff.NextNode()) == NULL) {
				m_curr_s_link[m_path.Size() - 2].ResetOvfLink();
				break;
			}
			s_link = *s_link_ptr;
		}
	}
};
CFileSet<CFileComp> COverflowSLinks::m_s_link_set;