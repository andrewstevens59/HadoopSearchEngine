#include "./OverflowSLinks.h"

// This class must take all the summary links that have been created
// and sorted and embed them at correct nodes in the hierachy. Summary
// links with high traversal probability are embedded higher up in the
// hiearchy, compared to summary links with low traversal probability.
// When a node becomes flooded with s_links all the low priority
// s_links are passed down to the appropriate chidren and the process
// repeats. It's possible that all the s_links are exhausted before
// an external node is reached in this case there are no s_links 
// attatched to these nodes. 

// A s_link itself consists of a creation level, traversal probability 
// and src and dst nodes in the hiearchy. Again creation level refers
// to the level at which a link was created. As a side note s_links
// cannot be embedded any higher than their subsume level since they
// no longer exist at this level. The algorithm that demotes lower
// traversal probability s_links is as follows. For each client a file
// records all s_links on the current level that have yet to be assigned
// a node. At every pass the s_links in this file are merged with the
// appropriate nodes. Any s_links that overflow a node are written to the
// demoted s_link file for the next level. This process is repeated until
// the demoted s_link file is empty. Note the number of s_links allowed
// at a given level exponentially decreases as a function of the height of
// a hiearchy. 

// Initially once all the s_links have been sorted by src node on a given
// level, the tree is traversed in depth first order. The original s_links
// are read off the current level until a s_link is reached whose src node
// does not belong to the current segment of the tree. In addition to this 
// the overflow s_links must also be processed. These are only stored 
// externally if they could not fit into memory. Otherwise they are stored
// in a buffer on the given level. If internal memory runs out then some
// of the s_link buffers must be freed and written out to external memory
// from the top of the tree down to the bottom. This is repeated until there
// is sufficient memory to support the current level of overflow s_links.
class CFloodSummaryLinks : public COverflowSLinks {

	// This stores a pointer to CreateABTrees
	CCreateABTree *m_create_ab_tree_ptr;

	// This is the comparison function used to sort s_links
	// based upon the s_link traversal probability
	static int CompareSLinksWeight(const SSummaryLinkPtr &arg1, 
		const SSummaryLinkPtr &arg2) {
		
		if(arg1.ptr->trav_prob < arg2.ptr->trav_prob) {
			return -1;
		}

		if(arg1.ptr->trav_prob > arg2.ptr->trav_prob) {
			return 1;
		}

		static _int64 diff1;
		static _int64 diff2;

		diff1 = abs(arg1.ptr->src.Value() - arg1.ptr->dst.Value());
		diff2 = abs(arg2.ptr->src.Value() - arg2.ptr->dst.Value());

		if(diff1 < diff2) {
			return 1;
		}

		if(diff1 > diff2) {
			return -1;
		}

		return 0;
	}

	// This is the comparison function used to sort s_links
	// based upon the src node id
	static int CompareSLinksNode(const SSummaryLinkPtr &arg1, 
		const SSummaryLinkPtr &arg2) {

		_int64 src1 = S5Byte::Value(arg1.ptr->src);
		_int64 src2 = S5Byte::Value(arg2.ptr->src);

		if(src1 < src2) {
			return 1;
		}

		if(src1 > src2) {
			return -1;
		}

		_int64 dst1 = S5Byte::Value(arg1.ptr->dst);
		_int64 dst2 = S5Byte::Value(arg2.ptr->dst);

		if(dst1 < dst2) {
			return 1;
		}

		if(dst1 > dst2) {
			return -1;
		}

		return 0;
	}

	// This processes the overflow s_links from the current level.
	// They need to be sorted. If s_links could not fit into internal
	// then they must be sorted externally.
	void SortOverflowSLinks() {

		SSummaryLink s_link;
		if(m_path.LastElement().flushed) {
			// sort externally 
			CFileComp &set_file = m_s_link_ovf_set.File(m_path.Size() - 1);
			set_file.CloseFile();

			CExternalRadixSort sort(15, set_file.CompBuffer(), sizeof(S5Byte));
			m_path.LastElement().is_ovf_s_link_open = false;

			return;
		}

		CLinkedBuffer<SSummaryLink> &buff = 
			m_s_link_ovf_buff[m_path.Size()-1];

		buff.ResetPath();
		// processes the internal links
		CMemoryChunk<SSummaryLink> temp_buff(buff.Size());
		buff.CopyLinkedBufferToBuffer(temp_buff.Buffer(), buff.Size());

		CMemoryChunk<SSummaryLinkPtr> s_link_ptr(buff.Size());
		for(int i=0; i<buff.Size(); i++) {
			s_link_ptr[i].ptr = &temp_buff[i];
		}

		CSort<SSummaryLinkPtr> sort(s_link_ptr.OverflowSize(), CompareSLinksNode);
		sort.HybridSort(s_link_ptr.Buffer());

		buff.Restart();
		for(int i=0; i<s_link_ptr.OverflowSize(); i++) {
			buff.PushBack(*s_link_ptr[i].ptr);
		}
	}

	// This handles the change over for different nodes in the tree.
	// That is when all s_links belonging to a particular region
	// in the hiearchy have been processed the next region is explored.
	// During the transition from one region to the next, the high 
	// priority s_links are written out to file to be processed later.
	// @param s_link_file - this stores all the high priority s_links
	//                    - that have been attatched at this level
	// @param node_bound - this stores the current number of nodes in
	//                   - a given region of the hiearchy
	// @param subtree - this is one of the subtrees stored in the hiearchy
	void HandleRegionChangeOver(CHDFSFile &s_link_file, SSubTree &subtree) {

		m_create_ab_tree_ptr->AddABTreeNodeBound(m_path.Size() - 1);

		static SABNode ab_node;
		// this stores statistics relating to the number of s_links
		ab_node.s_link_num = m_s_link_queue.Size();
		ab_node.child_num = subtree.child_num;
		ab_node.trav_prob = subtree.net_trav_prob;
		ab_node.total_node_num = subtree.node_num;
		ab_node.WriteABNode(s_link_file);

		while(m_s_link_queue.Size() > 0) {
			SSummaryLinkPtr ptr = m_s_link_queue.PopItem();
			ptr.ptr->WriteReducedSLink(s_link_file);
		}
	}
	
	// This cycles through all the s_links on a given level. This 
	// includes the s_links that were originally subsumed on this
	// level and also the s_links that have overflown and been 
	// pushed down onto this level.
	// @param subtree - this is the current subtree in the hiearchy
	//                - being processed
	// @param max_s_link_num - this is the maximum number of links allowed in a cluster
	void CycleThroughSLinks(SSubTree &subtree, int max_s_link_num) {

		m_s_link_ovf_buff[m_path.Size()-1].Restart();
		m_free_s_link_node = NULL;

		m_s_link_buff.Initialize(max_s_link_num + 4);
		m_s_link_queue.Initialize(m_s_link_buff.OverflowSize() - 4, CompareSLinksWeight);


		S64BitBound node_bound;
		// restart the node offset on the next level
		node_bound.start = m_curr_node_offset[m_path.Size()-1];
		m_curr_node_offset[m_path.Size()-1] += subtree.node_num;
		node_bound.end = m_curr_node_offset[m_path.Size()-1];

		CHDFSFile &ab_tree_file = CCreateABTree::AB_S_LINK_SET().File(m_path.Size()-1);
		// first process the original level s_links
		SSummaryLink &s_link = m_curr_s_link[m_path.Size()-1].stat_s_link;
		CFileComp &s_link_file = SLinkSet().File(m_path.Size()-1);

		ProcessOverflowSLinks(node_bound);

		while(s_link.src < node_bound.end) {
	
			if(s_link.create_level != 0xFF) {
				if(s_link.src < node_bound.start) {
					cout<<"normal s_link bound "<<(m_path.Size()-1)<<" "<<s_link.src.Value()<<" "<<node_bound.start;getchar();
				}
			}

			ProcessPriorityGroup(s_link);

			if(!s_link.ReadReducedSLink(s_link_file)) {
				m_curr_s_link[m_path.Size()-1].ResetStatLink();
				break;
			}
		}

		HandleRegionChangeOver(ab_tree_file, subtree);
		m_curr_s_link[m_path.Size()-1].ResetOvfLink();

		if(subtree.child_num > 0) {
			SortOverflowSLinks();
		}
	}

	// This is used for testing, prints the search path
	void PrintSearchPath() {

		for(int i=0; i<m_path.Size(); i++) {
			cout<<"Level: "<<i<<"     "<<m_path[i].child_num<<endl;
		}
	}

public:

	CFloodSummaryLinks() {
	}

	// This just sets up a number of files and variables used by this class
	// @param create_ab_tree_ptr - this stores a pointer to the ABTree class
	// @param client_node_offset - this stores the node bound offset for this client
	void Initialize(CCreateABTree *create_ab_tree_ptr, _int64 &client_node_offset) {

		m_curr_ovf_num = 0;
		m_create_ab_tree_ptr = create_ab_tree_ptr;
		m_curr_node_offset.AllocateMemory
			(GetHiearchyLevelNum(), client_node_offset);

		m_s_link_ovf_buff.AllocateMemory(GetHiearchyLevelNum());
		for(int i=0; i<GetHiearchyLevelNum(); i++) {
			m_s_link_ovf_buff[i].Initialize(1024);
		}

		SLinkSet().OpenReadFileSet(GetHiearchyLevelNum());

		m_path.Initialize(CNodeStat::GetHiearchyLevelNum());

		m_curr_s_link.AllocateMemory(GetHiearchyLevelNum());
		
		m_s_link_ovf_set.SetDirectory(CUtility::ExtendString
			("LocalData/s_link_overflow", GetInstID()));
		m_s_link_ovf_set.AllocateFileSet(GetHiearchyLevelNum(), GetInstID());
	}

	// This stores the sorted s_links at each level in the
	// hiearchy, these have been sorted by src node id
	static CFileSet<CFileComp> &SLinkSet() {
		return m_s_link_set;
	}

	// This creates and processes seperate level sets for all 
	// s_links that have been created. A level set is then sorted
	// by src node and then be link traversal probability. The 
	// tree is traversed in depth first order, where s_links sorted
	// by src node are taken off in order on each left. Any overlow
	// s_links are put in a temp file buffer. When all the s_links
	// can fit into memory, s_links are processed internally.
	// @param hiearchy_file - this is the hiearchy that stores the segments
	//                      - and the hiearchy subtrees
	// @param min_slink_num - this is the minimum number of s_links
	//                      - that can be grouped under a give node
	// @param max_slink_num - this is the maximum number of s_links
	//                      - that can be grouped under a give node
	void ProcessSubtreeSet(CHDFSFile &hiearchy_file, int min_s_link_num, int max_slink_num) {

		SSubTree subtree;
		subtree.ReadSubTree(hiearchy_file);
		m_curr_ovf_num = 0;

		m_path.Resize(1);
		m_path.LastElement().Set(subtree.child_num, max_slink_num);
		CycleThroughSLinks(subtree, max_slink_num);
		
		while(true) {
			while(m_path.LastElement().child_num == 0) {
				m_path.PopBack();

				if(m_path.Size() == 0) {
					return;
				}
			}

			m_path.LastElement().child_num--;
			if(m_path.Size() < m_path.OverflowSize()) {

				subtree.ReadSubTree(hiearchy_file);
				int max_s_link_num = max(min_s_link_num, m_path.LastElement().max_s_link_num / 2);

				m_path.ExtendSize(1);
				m_path.LastElement().Set(subtree.child_num, max_s_link_num);

				CycleThroughSLinks(subtree, max_slink_num);
			}
		}
	}

	~CFloodSummaryLinks() {
		m_s_link_set.RemoveFileSet();
		m_s_link_ovf_set.RemoveFileSet();
	}
};
