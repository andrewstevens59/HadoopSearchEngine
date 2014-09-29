#include "./CompileRankedList.h"

// This is used to find the set of documents that are linked to by 
// another document. From these subset of documents it extracts
// the keywords. This is used for testing.
class CFindSimilar {

	// This stores the set of word ids for the excerpts
	CLinkedBuffer<S5Byte> m_word_id_set;
	// This stores the number of keywords in each excerpt
	CLinkedBuffer<uChar> m_keyword_num;
	// This stores the set of doc ids
	CLinkedBuffer<SABTreeNode *> m_ab_node_set;
	// This is the ab_tree
	CExpandABTree m_expand_tree;

	// This function retrieves the next sequential word id in the excerpt
	// @param ab_node_ptr - this is a ptr to the current ab_node being processed
	void RetrieveExcerptWordID(SABTreeNode *ab_node_ptr, _int64 &byte_offset) {

		static CMemoryChunk<char> ab_buff(5);
		char *ab_buff_ptr = ab_buff.Buffer();
		CByte::ABTreeBytes(ab_node_ptr->tree_id, 
			ab_buff_ptr, 5, byte_offset);

		m_word_id_set.PushBack(*((S5Byte *)ab_buff_ptr));
		byte_offset += 5;
	}

	// This drills down on a given ab_node
	SABTreeNode *DrillABNode(S5Byte &doc_id) {

		SABTreeNode *ab_node_ptr = m_expand_tree.Node(doc_id);

		while(ab_node_ptr->header.child_num > 0) {
			m_expand_tree.ExpandABTreeNode(ab_node_ptr);
			ab_node_ptr = m_expand_tree.Node(doc_id);
		}

		m_expand_tree.LoadSLinks(ab_node_ptr);

		return ab_node_ptr;
	}

public:

	CFindSimilar() {
		m_keyword_num.Initialize();
		m_word_id_set.Initialize();
		m_ab_node_set.Initialize();
	}

	// This resets the excerpt keywords
	void Reset() {
		m_word_id_set.Restart();
		m_keyword_num.Restart();
		m_ab_node_set.Restart();
	}

	// This retrieves the set of global keywords that have a high association
	// to the query supplied by the user. This terms are checked for existence
	// in each fo the excerpts being searched.
	void RetreiveKeywordSet(COpenConnection &conn) {
	}

	// This adds the excerpt keywords that belong to a particular document
	// @param ab_node_ptr - this is a ptr to the current ab_node being processed
	// @return true if not a duplicate excerpt, false otherwise
	bool FindExcerptKeywords(SABTreeNode *ab_node_ptr) {


		return true;
	}

	// This finds neighbouring nodes for a given document
	void AddExcerptKeywords(S5Byte &doc_id) {

	}
};