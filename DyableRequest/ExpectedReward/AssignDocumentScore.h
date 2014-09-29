#include "./BranchAndBound.h"

// This class handles compiling the final set of high ranking documents.
// It first starts with a large subset of the documents produced from 
// the search phase. It then progressively culls a percentage of the 
// documents on each cycle following an update to the ranking scheme. 
// The webgraph score involves the progressive approximation of the expected
// reward of a webpage to other webpages who have a match to the query. 

// After each cycle the queue of high ranking webpages is reduced so as to 
// remove webpages that had a lower accumulative score compared to other 
// webpages. This process continues until a set of N webpages remains at 
// this point they are ranked in CompileRankedDocuments and handed back
// to the user.
class CAssignDocumentScore {

	// This defines the number of seed iterations
	static const int SEED_IT_NUM = 20;

	// This controls access to the branch and bound class used
	// to selectively expand high priority regions in the graph
	CBranchAndBound m_branch_bound;
	// This stores the set of seed nodes
	CArray<SABTreeNode *> m_seed_buff;
	// This stores the set of anchor nodes
	CArray<SABTreeNode *> m_anchor_node_set;
	// This stoers the set of tree nodes
	CArray<STreeNode> m_tree_node_set;
	// This stores the anchor bound
	SBoundary m_anchor_bound;

	// This is used to compare nodes based on rank. This is done to 
	// find a set of seed nodes for expansion.
	static int CompareSeedNodes(const STreeNode &arg1, const STreeNode &arg2) {

		if(arg1.hit_score < arg2.hit_score) {
			return -1;
		}

		if(arg1.hit_score > arg2.hit_score) {
			return 1;
		}

		if(arg1.title_div_num < arg2.title_div_num) {
			return -1;
		}

		if(arg1.title_div_num > arg2.title_div_num) {
			return 1;
		}

		if(arg1.rank < arg2.rank) {
			return -1;
		}

		if(arg1.rank > arg2.rank) {
			return 1;
		}

		return 0;
	}

	// This is used to compare nodes based on the node value. Used to remove
	// nodes that are heavily clustered in one location
	static int CompareNodeID(const STreeNode &arg1, const STreeNode &arg2) {

		_int64 node1 = S5Byte::Value(arg1.node);
		_int64 node2 = S5Byte::Value(arg2.node);

		if(node1 < node2) {
			return 1;
		}

		if(node1 > node2) {
			return -1;
		}

		return 0;
	}

	// This is used to compare nodes based on doc number
	static int CompareDocNum(SABTreeNode *const &arg1, SABTreeNode *const &arg2) {

		if(arg1->doc_match_score < arg2->doc_match_score) {
			return -1;
		}

		if(arg1->doc_match_score > arg2->doc_match_score) {
			return 1;
		}

		if(arg1->doc_num < arg2->doc_num) {
			return -1;
		}

		if(arg1->doc_num > arg2->doc_num) {
			return 1;
		}

		return 0;
	}

	// This compiles the set of seed nodes used for search
	void CompileSeedNodes() {

		for(int i=0; i<min(SEED_IT_NUM, m_tree_node_set.Size()); i++) {
			STreeNode &node = m_tree_node_set[i];
			m_branch_bound.Keywords().AddExcerptKeywords(node.node);
		}

		for(int i=0; i<m_anchor_node_set.Size(); i++) {
			m_branch_bound.AddNodeToQueue(m_anchor_node_set[i]);
		}

		m_branch_bound.CreateSeedNodes();
	}


public:

	CAssignDocumentScore() {
	}

	// This is called to seed all of the high scoring documents in the graph.
	// This is a precursor to calculating expected reward
	void Initialize(COpenConnection &conn) {
		
		int node_num;
		int client_id = 0;
		int client_num = 1;
		conn.Receive((char *)&node_num, sizeof(int));
		conn.Receive((char *)&client_num, sizeof(int));
		conn.Receive((char *)&client_id, sizeof(int));

		/*CArrayList<STreeNode> node_set(4);
		node_set.ReadArrayListFromFile("GlobalData/se");
		node_num = node_set.Size();*/

		m_branch_bound.Initialize(node_num);
		m_anchor_node_set.Initialize(node_num);
		m_tree_node_set.Initialize(node_num);
		m_branch_bound.Keywords().AddSeedTerms(conn);
		CNeighbourNodes::Initialize();

		cout<<node_num<<" ######"<<endl;

		SBoundary bound(0, node_num);
		CHashFunction::BoundaryPartion(client_id, client_num, bound.end, bound.start);
	
		STreeNode tree_node;
		int title_hit_num = 0;
		int avg_hit_score = 0;

		for(int i=0; i<node_num; i++) {
			conn.Receive((char *)&tree_node.node, sizeof(S5Byte));
			conn.Receive((char *)&tree_node.rank, sizeof(float));
			conn.Receive((char *)&tree_node.title_div_num, sizeof(uChar));
			conn.Receive((char *)&tree_node.hit_score, sizeof(uChar));

			tree_node.is_client_hit = (i >= bound.start && i < bound.end);

			if(tree_node.is_client_hit == true) {
				title_hit_num += tree_node.title_div_num;
				avg_hit_score += tree_node.hit_score;
				m_tree_node_set.PushBack(tree_node);
			}
		}

		m_branch_bound.SetTitleHitNum(title_hit_num, (float)avg_hit_score / node_num);

		SDocMatch *doc_ptr;
		for(int i=0; i<m_tree_node_set.Size(); i++) {
			STreeNode &node = m_tree_node_set[i];

			SABTreeNode *ab_node_ptr = m_branch_bound.InitializeAnchorNode(node, doc_ptr);
			doc_ptr->match_score = 0;

			if(doc_ptr->is_client_hit == true) {
				CNeighbourNodes::AddNeighbourNode(doc_ptr);
				CExpRew::AddNeighbourNode(doc_ptr);
			}

			if(ab_node_ptr != NULL) {
				m_anchor_node_set.PushBack(ab_node_ptr);
			}
		}

		CSort<STreeNode> sort(m_tree_node_set.Size(), CompareSeedNodes);
		sort.HybridSort(m_tree_node_set.Buffer());

		CompileSeedNodes();

		m_branch_bound.RankDocuments(conn);
	}

	// This retrieves the set of doc ids for a given set of node ids
	void RetrieveDocIDs(COpenConnection &conn) {

		int num;
		conn.Receive((char *)&num, sizeof(int));

		S5Byte doc_id;
		_int64 keyword_byte_offset;

		for(int i=0; i<num; i++) {
			conn.Receive((char *)&doc_id, sizeof(S5Byte));
			m_branch_bound.Keywords().RetrieveDocID(doc_id, keyword_byte_offset, doc_id);
			conn.Send((char *)&doc_id, sizeof(S5Byte));
		}

		conn.CloseConnection();
	}

};