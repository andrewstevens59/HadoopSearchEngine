#include "./ABTree.h"

// This is used to reassign the s_link endpoints to the children
// of a node that is being branched.
class CReassignChildNodes {

	// This stores one of the nodes in the red black tree
	struct SBranchChild {
		// This stores the node bound
		S64BitBound bound;
		// This is the value stored in the range tree
		SABTreeNode *node;
	};

	// This stores one of the nodes in the red black tree
	struct SBranchChildPtr {
		SBranchChild *ptr;
	};

	// This is used as a temporary child node buffer when reassigning
	// s_link endpoints to the children of a branched node
	CRedBlackTree<SBranchChildPtr> m_child_tree;
	// This stores all of the child nodes 
	CLinkedBuffer<SBranchChild> m_branch_buff;

	// This is used to compare nodes in the red black tree
	static int CompareTreeNode(const SBranchChildPtr &arg1, const SBranchChildPtr &arg2) {

		if(arg1.ptr->bound.end <= arg2.ptr->bound.start) {
			return 1;
		}

		if(arg1.ptr->bound.start >= arg2.ptr->bound.end) {
			return -1;
		}

		return 0;
	}

public:

	CReassignChildNodes() {
		m_branch_buff.Initialize(4096);
		m_child_tree.Initialize(1024, CompareTreeNode);
	}

	// This adds the child nodes of a branched node
	// @param ab_node_ptr - this is the node being expanded
	void AddChildNodes(SABTreeNode *ab_node_ptr) {

		SBranchChildPtr ptr;
		SChildNode *curr_child = ab_node_ptr->child_ptr;
		m_child_tree.Reset();
		m_branch_buff.Restart();

		while(curr_child != NULL) {
			SABTreeNode *child_node_ptr = (SABTreeNode *)curr_child->ab_node_ptr;
			ptr.ptr = m_branch_buff.ExtendSize(1);
			m_branch_buff.LastElement().bound = child_node_ptr->node_bound;
			m_branch_buff.LastElement().node = child_node_ptr;
			m_child_tree.AddNode(ptr);

			curr_child = curr_child->next_ptr;
		}
	}

	// This resets the child nodes
	void Reset() {
		m_child_tree.Reset();
		m_branch_buff.Restart();
	}

	// This finds the corresponding ab_node for an s_link endpoint
	// @param node_id - this is the node id belong to the s_link
	// @return the child ab_node_ptr
	SABTreeNode *ChildNode(S5Byte &node_id) {

		static SBranchChild node;
		node.bound.start = node_id.Value();
		node.bound.end = node_id.Value() + 1;

		static SBranchChildPtr ptr;
		ptr.ptr = &node;

		SBranchChildPtr *child_ptr = m_child_tree.FindNode(ptr);
		if(child_ptr == NULL) {
			return NULL;
		}

		return child_ptr->ptr->node;
	}
};

// The tree is used to create an internal graph representation 
// that is continously expanded. It's this classes responsibility
// to create a set of pointers to internal nodes for each of the
// links in the graph. It's also responsible for updating the the
// pointers when a node is expanded and a set of child nodes is
// created. A set of back links attatched to each AB Node need to
// be maintained. This is so all links pointer to a node can be 
// updated when the node is subdivided into child nodes. 

// Also this class must handle the reassignment of s_links to child
// ab_nodes when a parent ab_node is expanded. This only applies if 
// one of the parent s_links has a creation level that does not
// exceed that of the child level. 
class CExpandABTree : public CABTree {

	// This is used to reassign s_link endpoints for child nodes
	CReassignChildNodes m_reassign_child;
	// This stores the expansion log file
	CHDFSFile m_log_file;	
	// This is a predicate indicating whether the log file is active
	bool m_is_log_file;

    // This places all of the parent s_links into their appropriate
    // child ab_nodes. This is only done if the creation level does
    // not exceed that of the child set. The src node is used to
    // determine to which child ab_node the s_link belongs. If the
	// parent s_links have already been added from a previous cached 
	// version of the ab_node than this operation is not necessary.
    // @param stub_link_ptr - this is a ptr to the head forward or backward
	//                      - link set
	// @param link_set_ptr - this is the parent s_link
    void TransferParentSLinks(SLinkSet *link_set_ptr) {
		
		SLinkSet *prev_ptr;
        while(link_set_ptr != NULL) {

            SABTreeNode *child_node_ptr = m_reassign_child.ChildNode(link_set_ptr->s_link.src);

			if(link_set_ptr->s_link.create_level > child_node_ptr->header.level) {
				link_set_ptr = link_set_ptr->next_ptr;
				continue;
			}

			SLinkSet *child_s_link = m_s_link_buff.ExtendSize(1);
			child_s_link->s_link = link_set_ptr->s_link;

			prev_ptr = child_node_ptr->parent_link_ptr;
			child_node_ptr->parent_link_ptr = child_s_link;
			child_s_link->next_ptr = prev_ptr;

			link_set_ptr = link_set_ptr->next_ptr;
		}
    }

public:

    CExpandABTree() {
		m_is_log_file = false;
    }

	// This resets the tree so only the root nodes can be expanded again
	void ResetTree() {
		m_reassign_child.Reset();
		CABTree::ResetTree();
	}

	// This returns a reference to the log file
	inline CHDFSFile &LogFile() {
		return m_log_file;
	}

	// This closes the log file
	void CloseLogFile() {
		m_log_file.CloseFile();
	}

	// This is a limited version of expand ab_tree, it just transfers
	// any parent s_links. It does not attatch child s_links or reassigns
	// ptrs for back links attatched to an expanded node.
    // @param noab_node_ptrde - this is a ptr to the node
	// @return the ptr to the child ab_node
    void ExpandABTreeNode(SABTreeNode *ab_node_ptr) {

		if(ab_node_ptr->header.child_num == 0) {
			cout<<"ho";getchar();
		}

		m_timer.StartTimer();
        SChildNode *curr_child = ab_node_ptr->child_ptr;
        _int64 node_offset = ab_node_ptr->node_bound.start;

        m_range_tree.DeleteBound(ab_node_ptr->node_bound);

        while(curr_child != NULL) {
			m_ab_node_cache.AttatchChildNode(ab_node_ptr, curr_child, node_offset);
			SABTreeNode *child_node_ptr = (SABTreeNode *)curr_child->ab_node_ptr;
			m_range_tree.AddBound(child_node_ptr->node_bound, child_node_ptr);
            curr_child = curr_child->next_ptr;
        }

		m_reassign_child.AddChildNodes(ab_node_ptr);
		TransferParentSLinks(ab_node_ptr->link_set_ptr);
		TransferParentSLinks(ab_node_ptr->parent_link_ptr);

		m_timer.StopTimer();
    }


	// This returns one of the root nodes belonging to a node
	inline SABTreeNode *Node(S5Byte &node) {

		SABTreeNode *ab_node_ptr = m_reassign_child.ChildNode(node);
		if(ab_node_ptr != NULL) {
			return ab_node_ptr;
		}

		return m_range_tree.FindMatchingNode(node.Value());
	}
};