#include "./ExcerptKeywords.h"

double shuffle_time = 0;
double keyword_time = 0;
double tree_time = 0;
double spat_time = 0;

// This class is used to to rank the final set of documents. A limit is placed on
// the maximum number of nodes that can be exctracted. Documents are first ranked
// by their spatial score and then by their predetermined search rank. The number 
// of documents to be extracted from a given spatial region is determined by the 
// spatial score for that region.
class CComileDocumentSet {

	// This defines the maximum number of documents
	// that need to be compiled to rank excerpts
	static const int MAX_DOC_NUM = 100;

	// This is used to compare documents based on rank
	CLimitedPQ<SDocMatch *> m_doc_queue;
	// This stores the accumulative spatial score for each leaf node
	// this is designed to account precision limitations
	CMemoryChunk<float> m_acc_spatial_score;

	// This is used to compare branch nodes based on expected reward
	static int CompareDocNodes(SDocMatch *const &arg1, SDocMatch *const &arg2) {

		if(arg1->hit_score < arg2->hit_score) {
			return -1;
		}

		if(arg1->hit_score > arg2->hit_score) {
			return 1;
		}

		if(arg1->title_div_num < arg2->title_div_num) {
			return -1;
		}

		if(arg1->title_div_num > arg2->title_div_num) {
			return 1;
		}

		if(arg1->rank < arg2->rank) {
			return -1;
		}

		if(arg1->rank > arg2->rank) {
			return 1;
		}

		if(arg1->match_score < arg2->match_score) {
			return -1;
		}

		if(arg1->match_score > arg2->match_score) {
			return 1;
		}

		return 0;
	}

	// This calculates the expected reward for a given node
	void CalculateExpReward(SABTreeNode *ab_node_ptr, bool is_fringe = true) {

		SLinkSet *link_ptr = (SLinkSet *)((ab_node_ptr)->link_set_ptr);
		float exp_rew = 0;

		while(link_ptr != NULL) {
			SABTreeNode *dst_node_ptr = m_ab_tree.Node(link_ptr->s_link.dst);

			if(dst_node_ptr != NULL && dst_node_ptr->doc_match_ptr != NULL) {
				exp_rew += link_ptr->link_weight * dst_node_ptr->keyword_score;
				if(is_fringe == true) {
					exp_rew += link_ptr->link_weight * dst_node_ptr->exp_rew;
				}
			}

			link_ptr = link_ptr->next_ptr;
		}

		link_ptr = (SLinkSet *)((ab_node_ptr)->parent_link_ptr);

		while(link_ptr != NULL) {
			SABTreeNode *dst_node_ptr = m_ab_tree.Node(link_ptr->s_link.dst);

			if(dst_node_ptr != NULL && dst_node_ptr->doc_match_ptr != NULL) {
				exp_rew += link_ptr->link_weight * dst_node_ptr->keyword_score;

				if(is_fringe == true) {
					exp_rew += link_ptr->link_weight * dst_node_ptr->exp_rew;
				}
			}

			link_ptr = link_ptr->next_ptr;
		}

		ab_node_ptr->exp_rew = exp_rew;
	}

protected:

	// This defines one of the branching nodes
	struct SBranchNode {
		// This is a ptr to the ab_node
		SABTreeNode *ab_node_ptr;
	};

	// This stores the set of ranked branch nodes
	CLimitedPQ<SBranchNode> m_branch_queue;
	// This stores the nodes that have no children
	// and have been removed from the branch queue added later
	CLinkedBuffer<S5Byte> m_base_node_buff;
	// This stores the set of terminal leaf nodes found during the searc
	CArrayList<SBranchNode> m_leaf_node_buff;

	// This is used to extract the set of excerpt keywords
	CExcerptKeywords m_keywords;
	// This is used to expand the spatial tree
	CExpandABTree m_ab_tree;

	// This adds the set of nodes to the queue to be ranked 
	// and added to the document set
	void RankDocuments(SABTreeNode *ab_node_ptr, int num) {

		m_doc_queue.Initialize(num, CompareDocNodes);

		SDocMatch *curr_ptr = ab_node_ptr->doc_match_ptr;
		while(curr_ptr != NULL) {
			if(curr_ptr->is_added == false && curr_ptr->is_valid == true) {
				m_doc_queue.AddItem(curr_ptr);
			}

			curr_ptr = curr_ptr->next_ptr;
		}

		while(m_doc_queue.Size() > 0) {
			SDocMatch *ptr = m_doc_queue.PopItem();
			ptr->is_added = true;
			m_keywords.AddExcerptKeywords(ptr->doc_id);
			m_base_node_buff.PushBack(ptr->doc_id);

			if((m_base_node_buff.Size() % 10) == 0) {
				m_keywords.UpdateLimDist(20);
			}
		}
	}

	// This normalizes the set of spatial scores
	void NormalizeSpatialScore(int max_num) {

		float sum = 0;
		for(int i=0; i<m_leaf_node_buff.Size(); i++) {
			SABTreeNode *ab_node_ptr = m_leaf_node_buff[i].ab_node_ptr;
			sum += ab_node_ptr->spatial_score;
		}

		float tot_num = max_num - m_base_node_buff.Size();
		for(int i=0; i<m_leaf_node_buff.Size(); i++) {
			SABTreeNode *ab_node_ptr = m_leaf_node_buff[i].ab_node_ptr;
			m_acc_spatial_score[i] += ab_node_ptr->spatial_score / sum;
			int num = tot_num * m_acc_spatial_score[i];

			if(num < 1) {
				continue;
			}

			m_acc_spatial_score[i] = 0;

			if(num > 5) {
				tot_num += num - 5;
				num = 5;
			}

			RankDocuments(ab_node_ptr, num);
		}
	}

	// This calculates the keyword score for a given node
	_int64 CalculateKeywordScore(SABTreeNode *ab_node_ptr, bool is_invert_score, bool is_cache) {

		_int64 byte_offset = ab_node_ptr->header.s_link_byte_offset.Value();
		ab_node_ptr->keyword_score = m_keywords.KeywordDocumentScore
			(ab_node_ptr->tree_id, byte_offset, is_invert_score, is_cache);

		ab_node_ptr->keyword_score *= (ab_node_ptr->doc_match_score + 1.0f);

		return byte_offset;
	}

	// This calculates the spatial score for each document in the subset
	void CalculateSpatialScore() {

		for(int i=0; i<m_leaf_node_buff.Size(); i++) {
			SABTreeNode *ab_node_ptr = m_leaf_node_buff[i].ab_node_ptr;
			CalculateKeywordScore(ab_node_ptr, true, true);
		}

		for(int i=0; i<m_leaf_node_buff.Size(); i++) {
			SABTreeNode *ab_node_ptr = m_leaf_node_buff[i].ab_node_ptr;
			CalculateExpReward(ab_node_ptr, true);

			ab_node_ptr->spatial_score = (ab_node_ptr->keyword_score + 1.0f) * (ab_node_ptr->exp_rew + 1.0f);
		}
	}

public:

	CComileDocumentSet() {
	}

	// This ranks the set of documents by their expected reward and keyword score
	void RankDocuments(COpenConnection &conn) {

		cout<<"Shuffle Time "<<shuffle_time<<endl;
		cout<<"keyword_time "<<keyword_time<<endl;
		cout<<"Tree Time "<<tree_time<<endl;
		cout<<"Spatial Time "<<spat_time<<endl;

		m_acc_spatial_score.AllocateMemory(m_leaf_node_buff.Size(), 0);

		int num = 200;
		int prev_size = m_base_node_buff.Size();
		for(int i=0; i<20; i++) {

			if(m_base_node_buff.Size() != prev_size) {
				CalculateSpatialScore();
				prev_size = m_base_node_buff.Size();
			}

			NormalizeSpatialScore(min(MAX_DOC_NUM, num));
			num = MAX_DOC_NUM;
		}

		S5Byte *ptr;
		int size = m_base_node_buff.Size();
		cout<<"Sending ------------------- "<<size<<endl;	
		conn.Send((char *)&size, sizeof(int));

		m_base_node_buff.ResetPath();
		while((ptr = m_base_node_buff.NextNode()) != NULL) {
			m_keywords.SendExcerptKeywords(*ptr, conn);
		}

		m_keywords.SendSpatialKeywords(conn);
	}
};

// This class is responsible for calculating the expected reward for a node
// The expected reward of a node is the net sum of the product of the commute
// distance and the hit score of neighbouring nodes. This calculation is done
// incrementally for each node in the set and is progresively updated as desired
// by some control class. To calculate the expected reward a list of fringe nodes
// is stored for each node in the set. Fringe nodes are then progressively 
// expanded and their neighbours are added to the updated fringe set. If a fringe
// node has already been expanded then it is not added to the new fringe set a
// second time. Stored along with each node is an expected commute distace 
// anchor_exp and a expected reward value global_exp. The commute distance is 
// propagated forward from a node and attatched to a neighbour. This summed commute
// distance is then multiplied by the hit score + keyword score of the node. 

// When expanding the graph certain rules are followed. A node is only expanded if
// it has a hit score above some threshold. Fringe nodes are only added to the set 
// of a node that is an external node or has a hit score below some threshold. This
// ensures strong connectivity to the desired node for which the expected reward is
// being calculated.
class CBranchAndBound : public CComileDocumentSet {

	// This defines the maximum number of allowed nodes to be expanded
	static const int MAX_EXPAND_NUM = 50;
	// This defines the minimum number of doc matches that 
	// have to be resolved under a given node
	static const int MIN_DOC_MATCH = 1;

	// This stores all of the anchor nodes
	CLinkedBuffer<SABTreeNode *> m_anchor_node_buff;
	// This stores the set of document matches
	CLinkedBuffer<SDocMatch> m_doc_buff;

	// This stores the total number of title hits
	int m_title_hit_num;
	// This stores the average hit score
	float m_avg_hit_score;

	// This is used for performance profiling
	CStopWatch m_timer;

	// This compares branch nodes based on the document number
	static int CompareNodesByAvgRank(const SBranchNode &arg1, const SBranchNode &arg2) {

		if(arg1.ab_node_ptr->spatial_score < arg2.ab_node_ptr->spatial_score) {
			return 1;
		}

		if(arg1.ab_node_ptr->spatial_score > arg2.ab_node_ptr->spatial_score) {
			return -1;
		}


		return 0;
	}

	// This compares branch nodes based on the document number
	static int CompareDocumentNum(const SBranchNode &arg1, const SBranchNode &arg2) {

		if(arg1.ab_node_ptr->doc_num < arg2.ab_node_ptr->doc_num) {
			return -1;
		}

		if(arg1.ab_node_ptr->doc_num > arg2.ab_node_ptr->doc_num) {
			return 1;
		}

		if(arg1.ab_node_ptr->tree_level < arg2.ab_node_ptr->tree_level) {
			return -1;
		}

		if(arg1.ab_node_ptr->tree_level > arg2.ab_node_ptr->tree_level) {
			return 1;
		}

		if(arg1.ab_node_ptr->header.trav_prob < arg2.ab_node_ptr->header.trav_prob) {
			return -1;
		}

		if(arg1.ab_node_ptr->header.trav_prob > arg2.ab_node_ptr->header.trav_prob) {
			return 1;
		}

		return 0;
	}

	// This normalizes the set of link weights
	void NormalizeLinkWeights(SABTreeNode *ab_node_ptr) {

		SLinkSet *link_ptr = (SLinkSet *)((ab_node_ptr)->link_set_ptr);

		float link_weight = 0;
		while(link_ptr != NULL) {
			link_weight += link_ptr->s_link.trav_prob;
			link_ptr = link_ptr->next_ptr;
		}

		link_ptr = (SLinkSet *)((ab_node_ptr)->parent_link_ptr);

		while(link_ptr != NULL) {
			link_weight += link_ptr->s_link.trav_prob;
			link_ptr = link_ptr->next_ptr;
		}

		link_ptr = (SLinkSet *)((ab_node_ptr)->link_set_ptr);

		while(link_ptr != NULL) {
			link_ptr->link_weight = link_ptr->s_link.trav_prob / link_weight;
			link_ptr = link_ptr->next_ptr;
		}

		link_ptr = (SLinkSet *)((ab_node_ptr)->parent_link_ptr);

		while(link_ptr != NULL) {
			link_ptr->link_weight = link_ptr->s_link.trav_prob / link_weight;
			link_ptr = link_ptr->next_ptr;
		}
	}

	// This adds a new branch node to the set
	void AddBranchNode(SABTreeNode *ab_node_ptr) {
	
		static SBranchNode node;
		node.ab_node_ptr = ab_node_ptr;

		if(ab_node_ptr->doc_match_ptr == NULL) {
			return;
		}

		if(ab_node_ptr->doc_num > MIN_DOC_MATCH) {
			m_branch_queue.AddItem(node);
		} else {
			m_leaf_node_buff.PushBack(node);
		}
	}


	// This function expands all of the fringe nodes associated with 
	// the set of s_links and updates the expected reward for each ab_node.
	// @return true if more fringe nodes are available, false otherwise
	bool ExpandPriorityNodes() {

		if(m_branch_queue.Size() == 0) {
			return false;
		}

		SBranchNode node = m_branch_queue.PopItem();
		cout<<node.ab_node_ptr->doc_num<<endl;

		if(node.ab_node_ptr->header.child_num == 0) {
			m_leaf_node_buff.PushBack(node);
			return true;
		}

		ExpandABTree(node.ab_node_ptr);

		SChildNode *curr_ptr = node.ab_node_ptr->child_ptr;
		while(curr_ptr != NULL) {
			SABTreeNode *ab_node_ptr = (SABTreeNode *)curr_ptr->ab_node_ptr;
			AddBranchNode(ab_node_ptr);
			curr_ptr = curr_ptr->next_ptr;
		}

		return true;
	}

	// This expands a node in the ab_tree
	void ExpandABTree(SABTreeNode *ab_node_ptr) {

		m_timer.StartTimer();
		m_ab_tree.ExpandABTreeNode(ab_node_ptr);
		m_timer.StopTimer();
		tree_time += m_timer.GetElapsedTime();

		SDocMatch *doc_ptr = ab_node_ptr->doc_match_ptr;
		while(doc_ptr != NULL) {

			SDocMatch *next_ptr = doc_ptr->next_ptr;
			if(doc_ptr->is_client_hit == true) {
				SABTreeNode *child_ptr = m_ab_tree.Node(doc_ptr->doc_id);
				SDocMatch *prev_ptr = child_ptr->doc_match_ptr;
				child_ptr->doc_num = min(0xFFFF, child_ptr->doc_num + 1);
				child_ptr->doc_match_ptr = doc_ptr;
				doc_ptr->next_ptr = prev_ptr;		
			}

			doc_ptr = next_ptr;
		}
	}
		
public:

	CBranchAndBound() {
		m_anchor_node_buff.Initialize();
		m_doc_buff.Initialize();
		m_base_node_buff.Initialize();
		m_leaf_node_buff.Initialize();
	}

	// This sets up the necessary variables for branch and bound
	// @param title_hit_num - this stores the total number of title hits
	void Initialize(int document_num) {

		m_timer.StartTimer();
		m_branch_queue.Initialize(1024, CompareDocumentNum);
		m_ab_tree.Initialize(CLIENT_BOUND_DIR, AB_ROOT_DIR);
		m_ab_tree.ResetTree();

		m_leaf_node_buff.Resize(0);
		m_base_node_buff.Restart();
		m_anchor_node_buff.Restart();
		m_keywords.Reset();
		m_doc_buff.Restart();
	}

	// This sets the total number of title hits
	inline void SetTitleHitNum(int title_hit_num, float avg_hit_score) {
		m_title_hit_num = title_hit_num;
		m_avg_hit_score = avg_hit_score;
	}

	// This returns access to excerpt keywords
	inline CExcerptKeywords &Keywords() {
		return m_keywords;
	}

	// This returns access to excerpt keywords
	inline CExpandABTree &ABTree() {
		return m_ab_tree;
	}

	// This function fully expands the ab_tree nodes that contain 
	// high priority hit node. It then proceeds to create the fringe
	// set associated with the node so its expected reward can be 
	// updated on future cycles.
	// @param tree_node - this is the node in the ab_tree being expanded
	// @return the pointer to the ab_node if it is the first doc match to
	//         that node otherwise return NULL       
	SABTreeNode *InitializeAnchorNode(STreeNode &tree_node, SDocMatch * &doc_ptr) {

		SABTreeNode *ab_node_ptr = m_ab_tree.Node(tree_node.node);

		SDocMatch *prev_ptr = ab_node_ptr->doc_match_ptr;
		ab_node_ptr->doc_match_ptr = m_doc_buff.ExtendSize(1);
		ab_node_ptr->doc_match_ptr->doc_id = tree_node.node;
		ab_node_ptr->doc_match_ptr->next_ptr = prev_ptr;
		ab_node_ptr->doc_match_ptr->is_client_hit = tree_node.is_client_hit;
		ab_node_ptr->doc_match_ptr->title_div_num = tree_node.title_div_num;
		ab_node_ptr->doc_match_ptr->hit_score = tree_node.hit_score;
		doc_ptr = ab_node_ptr->doc_match_ptr;
		doc_ptr->is_added = false;
		doc_ptr->is_valid = true;
		doc_ptr->match_score = 0;

		if(tree_node.is_client_hit == true) {
			ab_node_ptr->doc_num = min(0xFFFF, ab_node_ptr->doc_num + 1);
		}

		if(prev_ptr == NULL) {
			return ab_node_ptr;
		}

		return NULL;
	}

	// This creates the set of seed nodes to initialize the expansion
	inline void AddNodeToQueue(SABTreeNode *ab_node_ptr) {
		AddBranchNode(ab_node_ptr);
	}

	// This adds a single base node to the set
	inline void AddBaseNode(S5Byte &node) {
		m_base_node_buff.PushBack(node);
	}

	// This returns the number of base nodes
	inline int NodeNum() {
		return m_base_node_buff.Size();
	}

	// This creates the set of seed nodes. Seed nodes have a perceived high keyword match.
	// Seed nodes are used to expand the rest of the tree using s_links grouped in relation
	// to the set of seed nodes.
	void CreateSeedNodes() {

		for(int i=0; i<MAX_EXPAND_NUM; i++) {
			if(ExpandPriorityNodes() == false) {
				break;
			}
		}

		while(m_branch_queue.Size() > 0) {
			m_leaf_node_buff.PushBack(m_branch_queue.PopItem());
		}

		m_branch_queue.Initialize(80, CompareDocumentNum);

		for(int i=0; i<m_leaf_node_buff.Size(); i++) {
			m_branch_queue.AddItem(m_leaf_node_buff[i]);
		}

		m_leaf_node_buff.Resize(0);
		while(m_branch_queue.Size() > 0) {
			SBranchNode ptr = m_branch_queue.PopItem();
			_int64 byte_offset = CalculateKeywordScore(ptr.ab_node_ptr, false, false);
			m_ab_tree.LoadSLinks(ptr.ab_node_ptr, byte_offset);
			NormalizeLinkWeights(ptr.ab_node_ptr);

			ptr.ab_node_ptr->spatial_score = ptr.ab_node_ptr->doc_num;
			m_leaf_node_buff.PushBack(ptr);
		}

		cout<<m_leaf_node_buff.Size()<<"  (((("<<endl;
	}

};