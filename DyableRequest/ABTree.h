#include "./RangeTree.h"

// This stores the directory where pointers to the root
// nodes of all the ab_trees are stored
const char *AB_ROOT_DIR = "GlobalData/HitSearchTrees/search_root";
// This stores the directory of the client boundary
const char *CLIENT_BOUND_DIR = "GlobalData/ClusterHiearchy/cluster_boundary";

// This class is responsible for storing the internal representation
// of the ab_tree. It therefore holds linked data structures relating
// to the s_link set and the child set attatched to each ab_tree node.
// This class also facilitates the initialization of the ab_tree so
// that the root level can be loaded initially to begin expansion. The
// data required by the ab_tree must be retrieved through the hit item 
// server. A hash list structure is used for the root level in the ab
// tree in order to make the assignment of dst node pointers to their
// corresponding ab_node counterpart more efficient. This is built
// when initially loading in the ab_tree. The two useful features this
// class provides is the ability to load in s_links and child nodes 
// from external storage and store them in an internal data structure.
class CABTree : public CNodeStat {

	// This stores the node boundary for the set of ab_trees
	struct SABNodeBound {
		// This stores the node boundary
		_int64 node_offset;
		// This stores the tree id
		int tree_id;
	};

	// This stores the root ab_tree node that is 
	// created during initialization
	SABTreeNode *m_bin_root_ab_node_ptr;
	// This stores the root ab_tree node that is 
	// created during initialization
	SABTreeNode m_root_ab_node;
	// This stores the set of root ab_nodes
	CArrayList<SABTreeNode *> m_root_ab_node_set;
	// This stores all the child nodes
	CLinkedBuffer<SChildNode> m_child_node_buff;

	// This stores the nod bounds for each ab_tree
	CMemoryChunk<_int64> m_client_boundary;
	// This stores the node offset for each ab_tree
	CArrayList<SABNodeBound> m_ab_tree_boundary;
	// This stores the byte offset for all the top 
	// level nodes in the ABTree
	CArrayList<SABRootByteOffset> m_root_byte_offset;
	// This stores the root level node bound for this client
	SBoundary m_root_bound;
	// This stores the node bounds for which this node is responsible
	S64BitBound m_node_bound;

	// This assigns the node boundary for this client.
	void AssignClientNodeBoundary() {

		for(int i=m_root_byte_offset.Size()-1; i>=0; i--) {

			if(m_node_bound.start >= m_root_byte_offset[i].node_offset) {
				m_node_bound.start = m_root_byte_offset[i].node_offset;
				m_root_bound.start = i;
				break;
			}
		}

		if(GetClientID() == GetClientNum() - 1) {
			m_root_bound.end = m_root_byte_offset.Size();
			return;
		}

		for(int i=0; i<m_root_byte_offset.Size(); i++) {

			if(m_node_bound.end < m_root_byte_offset[i].node_offset) {
				m_node_bound.end = m_root_byte_offset[i-1].node_offset;
				m_root_bound.end = i - 1;
				break;
			}
		}
	}

	// This loads in the root ab tree blocks used to search the tree
	void LoadRootABNodeData() {

		m_client_boundary.ReadMemoryChunkFromFile(CLIENT_BOUND_DIR);
		m_ab_tree_boundary.Initialize();

		int set = 0;
		m_root_byte_offset.Initialize(1024);
		CArrayList<SABRootByteOffset> client_root_offset;
		for(int i=0; i<m_client_boundary.OverflowSize() - 1; i++) {
			client_root_offset.ReadArrayListFromFile
				(CUtility::ExtendString(AB_ROOT_DIR, set++));

			if(client_root_offset.Size() == 0) {
				continue;
			}

			m_ab_tree_boundary.ExtendSize(1);
			m_ab_tree_boundary.LastElement().node_offset = 
				client_root_offset[0].node_offset;
			m_ab_tree_boundary.LastElement().tree_id = set - 1;

			for(int k=0; k<client_root_offset.Size(); k++) {
				m_root_byte_offset.PushBack(client_root_offset[k]);
			}
		}

		m_ab_tree_boundary.ExtendSize(1);
		m_ab_tree_boundary.LastElement().node_offset = 
			m_client_boundary.LastElement();

		m_node_bound.Set(0, m_client_boundary.LastElement());
		CHashFunction::BoundaryPartion(GetClientID(), GetClientNum(),
			m_node_bound.end, m_node_bound.start);

		AssignClientNodeBoundary();
	}

	// This creates the very root ab_tree node. This is only a single node
	// that stores pointers to all of the child nodes on the top level of
	// the ab_tree. This is used as a convinience when searching the ab_tree.
	void CreateRootABTreeNode() {

		m_range_tree.Initialize(m_node_bound);
		for(int i=0; i<m_root_ab_node_set.Size(); i++) {
			SABTreeNode *ab_node_ptr = m_root_ab_node_set[i];
			m_range_tree.CreateRootHashNode(ab_node_ptr->node_bound, ab_node_ptr);
		}

		m_root_ab_node.child_ptr = m_child_node_buff.ExtendSize(1);
		SChildNode *curr_child = m_root_ab_node.child_ptr;
		SABTreeNode *ab_node_ptr = m_root_ab_node_set[0];
		curr_child->ab_node_ptr = ab_node_ptr;

		m_root_ab_node.node_bound.start = ab_node_ptr->node_bound.start;

		for(int i=1; i<m_root_ab_node_set.Size(); i++) {

			ab_node_ptr = m_root_ab_node_set[i];
			curr_child->next_ptr = m_child_node_buff.ExtendSize(1);
			curr_child = curr_child->next_ptr;
			curr_child->ab_node_ptr = ab_node_ptr;
		}

		m_root_ab_node.header.child_num = m_root_ab_node_set.Size();
		m_root_ab_node.node_bound.end = ab_node_ptr->node_bound.end;
		curr_child->next_ptr = NULL;

		m_root_ab_node.tree_level = 0;
		m_bin_root_ab_node_ptr = &m_root_ab_node;
	}

	// This retrieves the byte offset for the top level in the ab tree.
	// This is used initially to expand the tree from all the root nodes.
	void LoadABTreeRootNodes() {
	
		int set = 0;
		m_root_ab_node_set.Initialize(m_root_bound.Width());
		m_ab_node_cache.Initialize(m_node_bound);

		for(int i=m_root_bound.start; i<m_root_bound.end; i++) {

			SABRootByteOffset &root = m_root_byte_offset[i];
			while(root.node_offset >= m_ab_tree_boundary[set+1].node_offset) {
				set++;
			}

			int tree_id = m_ab_tree_boundary[set].tree_id;
			SABTreeNode *ab_node_ptr = m_ab_node_cache.AddRootABNode
				(root.byte_offset, root.node_offset, tree_id);

			m_root_ab_node_set.PushBack(ab_node_ptr);
		}
	}

protected:

	// This is an instance of the ab_node cache
	CABNodeCache m_ab_node_cache;
	// This stores all of the parent s_links
	CLinkedBuffer<SLinkSet> m_s_link_buff;
	// This is used to map node id's values to ab_nodes
	CRangeTree m_range_tree;
	// This is used for performance profiling
	CStopWatch m_timer;

public:

	CABTree() {
	}

	// This is called to initalize the ABTree. This means retrieving
	// the top level of the ABTree for this client from the hit item
	// server and creating the hash map used to quickly index into 
	// different regions of the ab_tree.
	void Initialize(const char *client_bound_dir, const char *ab_root_dir) {

		AB_ROOT_DIR = ab_root_dir;
		CLIENT_BOUND_DIR  = client_bound_dir;

		if(m_client_boundary.OverflowSize() > 0) {
			Reset();
			return;
		}

		m_child_node_buff.Initialize();
		m_s_link_buff.Initialize();
		m_ab_node_cache.SetTimeStamp();

		LoadRootABNodeData();
		LoadABTreeRootNodes();
		CreateRootABTreeNode();

		for(int i=0; i<m_root_ab_node_set.Size(); i++) {
			m_root_ab_node_set[i]->Reset();
		}
	}

	// This is called to create a set of root ab_tree nodes in the ab_tree
	// that are composed of a set of arbitrary nodes in the tree.
	// @param ab_node_set - this stores the set of ab_nodes
	inline void Initialize(CArrayList<SABTreeNode *> &ab_node_set) {
		m_range_tree.Reset(ab_node_set);
	}

	// This returns a ptr to an ab_node 
	inline SABTreeNode *ABNode(COpenConnection &conn) {
		return m_ab_node_cache.AddABNode(conn);
	}

	// This loads in all of the s_links attatched to an ABNode. These
	// are stored in a linked list structure.
	// @param ab_node_ptr - this is the current ab_node being processed
	// @param s_link_byte_offset - this is the byte offset where the s_links are stored
	void LoadSLinks(SABTreeNode *ab_node_ptr, _int64 &s_link_byte_offset) {

		ab_node_ptr->parent_link_ptr = NULL;
		ab_node_ptr->link_set_ptr = NULL;

		static CMemoryChunk<char> s_link_buff(15);
		static SReducedSummaryLink s_link;
		static uLong s_link_num;
		static int enc_bytes;

		CByte::SLinkBytes(ab_node_ptr->tree_id, s_link_buff.Buffer(), sizeof(uLong), s_link_byte_offset);

		char *buff_ptr = s_link_buff.Buffer();
		s_link_num = CArray<char>::GetEscapedItem(buff_ptr, enc_bytes);
		s_link_byte_offset += enc_bytes;

		for(uLong i=0; i<s_link_num; i++) {

			CByte::SLinkBytes(ab_node_ptr->tree_id, s_link_buff.Buffer(), 
				s_link_buff.OverflowSize(), s_link_byte_offset);

			s_link_byte_offset += s_link.ReadReducedSLink
				(s_link_buff.Buffer());
			
			if(s_link.dst < m_node_bound.start) {
				continue;
			}

			if(s_link.dst >= m_node_bound.end) {
				continue;
			}

			SLinkSet *s_link_set = m_s_link_buff.ExtendSize(1);
			s_link_set->s_link = s_link;

			/*if(s_link_set->s_link.src < ab_node_ptr->node_bound.start) {
				cout<<"s_link error bound "<<s_link_set->s_link.src.Value()<<" "<<ab_node_ptr->node_bound.start<<" "<<ab_node_ptr->node_bound.end;getchar();
			}

			if(s_link_set->s_link.src >= ab_node_ptr->node_bound.end) {
				cout<<"s_link error bound";getchar();
			}*/

			SLinkSet *prev_ptr = ab_node_ptr->link_set_ptr;
			ab_node_ptr->link_set_ptr = s_link_set;
			s_link_set->next_ptr = prev_ptr;
		}
	}

	// This stores the node bound for which this client is responsible
	inline S64BitBound &RootNodeBound() {
		return m_node_bound;
	}

	// This returns a ptr to the hard coded ab_tree structure
	// that was created as part of the cluster hiearchy
	inline SABTreeNode *RootNode() {
		return &m_root_ab_node;
	}

	// This returns a pointer to the cached binary
	// tree representation
	inline SABTreeNode *BinaryRootNode() {
		return m_bin_root_ab_node_ptr;
	}

	// This resets the tree so only the root nodes can be expanded again
	void Reset() {
		cout<<"ABTree "<<m_timer.GetElapsedTime()<<endl;
		m_timer.StartTimer();
		m_ab_node_cache.SetTimeStamp();
		//CGroupChildNodes::Reset();

		for(int i=0; i<m_root_ab_node_set.Size(); i++) {
			m_root_ab_node_set[i]->Reset();
		}
	
		m_timer.StopTimer();
	}

	// This resets the tree so only the root nodes can be expanded again
	void ResetTree() {
		m_s_link_buff.Restart();
		m_range_tree.Reset(m_root_ab_node_set);
	}
};