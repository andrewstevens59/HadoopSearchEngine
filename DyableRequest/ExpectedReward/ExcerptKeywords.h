#include "./NeighbourNodes.h"

// This stores the keyword score of each document
struct SDocument {
	// This stores the local document id
	int doc_id;
	// The document score 
	float score;
	// This stores the document id
	S5Byte node_id;
};

// This defines a node in the tree along with its value
struct STreeNode {
	// This is the node id
	S5Byte node;
	// This is a predicate indicating that this hit belongs to this client
	uChar is_client_hit;
	// This stores the number of title hits
	uChar title_div_num;
	// This stores the hit score 
	uChar hit_score;
	// This stores the rank of the node
	float rank;
};

// This is used to find the top N high ranking excerpts. This is done using
// the standard keyword update procedure. The top N documents are used as
// seed nodes to expand the tree futher.
class CRankExcerpt {

	// This stores the occurrence and minimum tree level of a keyword
	struct SKeyword {
		// This stores the occurrence of the keyword
		float occur;
		// This score the keyword weight
		float weight;
		// This is a predicate indicating an association keyword
		bool is_assoc;
		// This stores the spatial occcurrence of a given term
		u_short spatial_score;
		// This stores the keyword id
		S5Byte keyword_id;
	};

	struct SKeywordPtr {
		SKeyword *ptr;
	};

	// This is used to store the top ranking documents
	CLimitedPQ<SDocument> m_doc_queue;
	// This stores the set of documents
	CArray<SDocument> m_doc_buff;
	// This is used to find the top N spatial keywords
	CLimitedPQ<SKeyword *> m_keyword_queue;

	// This is used to compare documents based on the keyword score
	static int CompareDocuments(const SDocument &arg1, const SDocument &arg2) {

		if(arg1.score < arg2.score) {
			return -1;
		}

		if(arg1.score > arg2.score) {
			return 1;
		}

		return 0;
	}

	// This compares keywords based on their spatial score
	static int CompareSpatialKeywords(SKeyword *const &arg1, SKeyword *const &arg2) {

		if(arg1->spatial_score < arg2->spatial_score) {
			return -1;
		}

		if(arg1->spatial_score > arg2->spatial_score) {
			return 1;
		}

		return 0;
	}

	// This is used to sort keywords
	static int CompareKeywords(const SKeywordPtr &arg1, const SKeywordPtr &arg2) {

		if(arg1.ptr->weight < arg2.ptr->weight) {
			return -1;
		}

		if(arg1.ptr->weight > arg2.ptr->weight) {
			return 1;
		}

		return 0;
	}

	// This calculates the keyword score for a given document
	float CalculateKeywordScoreForDocument(int doc_id) {

		float score = 0;
		for(int i=m_keyword_offset[doc_id]; i<m_keyword_offset[doc_id+1]; i++) {
			score += m_occur[m_keyword_buff[i]].weight;
		}

		return score;
	}

	// This updates the keyword score for all keywords in a document
	void UdpateKeywordScoreForDocument(int doc_id, float keyword_score) {

		for(int i=m_keyword_offset[doc_id]; i<m_keyword_offset[doc_id+1]; i++) {
			m_occur[m_keyword_buff[i]].weight += keyword_score;
		}
	}

	// This calculates the keyword score for a set of documents
	void CalculateKeywordScore() {

		for(int i=0; i<m_occur.Size(); i++) {
			m_occur[i].weight = m_occur[i].occur;
		}

		int doc_num = 50;
		for(int i=0; i<3; i++) {

			m_doc_queue.Initialize(doc_num, CompareDocuments);

			for(int j=0; j<m_doc_buff.Size(); j++) {
				SDocument &doc = m_doc_buff[j];
				m_doc_buff[j].score = CalculateKeywordScoreForDocument(doc.doc_id);
				m_doc_queue.AddItem(m_doc_buff[i]);
			}

			CArray<SDocument> buff(m_doc_queue.Size());
			m_doc_queue.CopyNodesToBuffer(buff);

			for(int j=0; j<buff.Size(); j++) {
				SDocument &doc = buff[j];
				UdpateKeywordScoreForDocument(doc.doc_id, doc.score);
			}

			doc_num >>= 1;
		}
	}

protected:

	// This stores the occurrence of each keyword
	CArrayList<SKeyword> m_occur;
	// This stores the keyword set for a particular document
	CArrayList<int> m_keyword_buff;
	// This stores the keyword offset for each keyowrd set
	CArrayList<int> m_keyword_offset;
	// This stores the document map for a given spatial region
	CHashDictionary<int> m_doc_map;

public:

	CRankExcerpt() {
	}

	// This assigns the final score to a set of documents. This is used the standard
	// keyword score reranking to find the set of high scoring documents. High scoring
	// documents are used as seed documents. 
	void AssignHighRankingScore(CLinkedBuffer<S5Byte> &doc_buff) {

		S5Byte *ptr;
		doc_buff.ResetPath();
		m_doc_buff.Initialize(doc_buff.Size());
		while((ptr = doc_buff.NextNode()) != NULL) {
			m_doc_buff.ExtendSize(1);
			S5Byte &node = (*ptr);
			m_doc_buff.LastElement().doc_id = m_doc_map.FindWord((char *)&node, sizeof(S5Byte));
			m_doc_buff.LastElement().node_id = *ptr;
		}

		CalculateKeywordScore();

		CSort<SDocument> sort(m_doc_buff.Size(), CompareDocuments);
		sort.HybridSort(m_doc_buff.Buffer());
	}

	// This zeros out the N-k terms for the top K ranking documents
	// @param term_num - this is the number of top ranking keywords to select
	void SelectHighRankingKeywords(int term_num) {

		m_doc_buff.Initialize(m_doc_map.Size());
		m_doc_buff.Resize(m_doc_map.Size());

		for(int i=0; i<m_doc_buff.Size(); i++) {
			m_doc_buff[i].doc_id = i;
		}

		CalculateKeywordScore();

		CMemoryChunk<SKeywordPtr> temp(m_occur.Size());
		for(int i=0; i<temp.OverflowSize(); i++) {	
			temp[i].ptr = &m_occur[i];
		}

		CSort<SKeywordPtr> sort(temp.OverflowSize(), CompareKeywords);
		sort.HybridSort(temp.Buffer());

		for(int i=term_num; i<temp.OverflowSize(); i++) {
			temp[i].ptr->occur = 0;
		}

		for(int i=0; i<min(temp.OverflowSize(), term_num); i++) {
			temp[i].ptr->occur = 1.0f;
		}
	}

	// This sends the top N spatial keywords
	void SendSpatialKeywords(COpenConnection &conn) {

		m_keyword_queue.Initialize(200, CompareSpatialKeywords);

		for(int i=0; i<m_occur.Size(); i++) {

			if(m_occur[i].spatial_score > 1 && m_occur[i].occur > 0) {
				m_keyword_queue.AddItem(&m_occur[i]);
			}
		}

		uChar size = m_keyword_queue.Size();
		conn.Send((char *)&size, sizeof(uChar));

		while(m_keyword_queue.Size() > 0) {
			SKeyword *ptr = m_keyword_queue.PopItem();
			conn.Send((char *)&ptr->keyword_id, sizeof(S5Byte));
			conn.Send((char *)&ptr->spatial_score, sizeof(uChar));
		}
	}

	// This returns the set of ranked documents
	inline CArray<SDocument> &DocBuff() {
		return m_doc_buff;
	}
};

// This class is responsible for compiling the list of excerpt keywords
// associated with all of the external nodes expanded while searching.
// It keeps a record of the every keyword and the number of occurrences
// of that keyword in the corpus. The class is used to assign a keyword 
// match score to all of the high priority excerpts which is partially
// used to rank the documents.
class CExcerptKeywords : public CRankExcerpt {

	// This defines a bound in the tree
	struct STreeBound : public S64BitBound {
		// This stores the tree id
		int tree_id;
	};

	// This stores the checsum map
	CHashDictionary<int> m_checksum_map;
	// This stores the set of keywords 
	CObjectHashMap<S5Byte> m_keyword_map;

	// This stores the nod bounds for each ab_tree
	CMemoryChunk<_int64> m_client_boundary;
	// This stores the node boundaries for each cluster group
	CRedBlackTree<STreeBound> m_bound_tree;

	// This is used to process neighbour nodes 
	CExpRew m_exp_rew;

	// This returns the hash code for a visit node
	static int HashCode(const S5Byte &arg) {
		return (int)S5Byte::Value(arg);
	}

	// This compares two visit nodes for equality
	static bool Equal(const S5Byte &arg1, const S5Byte &arg2) {
		return S5Byte::Value(arg1) == S5Byte::Value(arg2);
	}

	// This is used to compare nodes in the red black tree
	static int CompareTreeNode(const STreeBound &arg1, const STreeBound &arg2) {

		if(arg1.end <= arg2.start) {
			return 1;
		}

		if(arg1.start >= arg2.end) {
			return -1;
		}

		return 0;
	}

	// This function retrieves the next sequential word id in the excerpt
	// @param byte_offset - this is the byte offset of the keyword set being processed
	// @param tree_id - this is the id of the keyword set being processed
	float RetrieveExcerptWordID(_int64 &byte_offset, int tree_id) {

		static S5Byte keyword_id;
		CByte::KeywordBytes(tree_id, (char *)&keyword_id, sizeof(S5Byte), byte_offset);

		int id = m_keyword_map.Put(keyword_id);
		if(m_keyword_map.AskFoundWord() == false) {
			m_occur.ExtendSize(1);
			m_occur.LastElement().occur = 0;
			m_occur.LastElement().is_assoc = false;
			m_occur.LastElement().spatial_score = 0;
			m_occur.LastElement().keyword_id = keyword_id;
		}

		m_keyword_buff.PushBack(id);

		m_occur[id].occur++;
		byte_offset += sizeof(S5Byte);

		return m_occur[id].occur;
	}

	// This adds the excerpt keywords that belong to a particular document
	// @param byte_offset - this is the byte offset of the keyword set being processed
	// @param tree_id - this is the id of the keyword set being processed
	void SendKeywordSet(_int64 byte_offset, int tree_id, COpenConnection &conn) {

		static int enc_bytes;
		static CMemoryChunk<char> ab_buff(11);
		char *ab_buff_ptr = ab_buff.Buffer();
		uLong keyword_num;

		CByte::SLinkBytes(tree_id, ab_buff.Buffer(), 
			ab_buff.OverflowSize(), byte_offset);

		if((keyword_num = CArray<char>::GetEscapedItem(ab_buff_ptr, enc_bytes)) == 0) {
			conn.Send((char *)&keyword_num, sizeof(uChar));
			return;
		}

		static S5Byte keyword_id;
		byte_offset += enc_bytes;
		conn.Send((char *)&keyword_num, sizeof(uChar));

		for(uLong i=0; i<keyword_num; i++) {
			CByte::SLinkBytes(tree_id, (char *)&keyword_id, sizeof(S5Byte), byte_offset);
			conn.Send((char *)&keyword_id, sizeof(S5Byte));
			byte_offset += sizeof(S5Byte);
		}
	}

	// This calculates the keyword score for a given spatial region, this is the cached version
	// @param doc_id - this is the document for which the keyword set is being returned
	float KeywordDocumentScore(int doc_id) {

		float score = 0;
		for(int i=m_keyword_offset[doc_id]; i<m_keyword_offset[doc_id+1]; i++) {

			int id = m_keyword_buff[i];
			if(m_occur[id].occur < 200 && m_occur[id].occur > 0) {
				if(m_occur[id].is_assoc == false) {
					score += 1.0f / m_occur[id].occur;
				} else {
					score += 1.0f;
				}
			}
		}

		return score;
	}
	
	// This retrieves the set of neighbour nodes
	// @param byte_offset - this is the byte offset of the keyword set being processed
	// @param tree_id - this is the id of the keyword set being processed
	// @param avg_weight - this is the average distribution weight of keywords
	void ProcessNeighbourNodes(_int64 &byte_offset, S5Byte &doc_id, int tree_id, float avg_weight) {

		static uChar num;
		static S5Byte node_id;
		static uChar match;
		CByte::KeywordBytes(tree_id, (char *)&num, sizeof(uChar), byte_offset);
		byte_offset += sizeof(uChar);
		int src_id = -1;

		for(uChar i=0; i<num; i++) {
			CByte::KeywordBytes(tree_id, (char *)&node_id, sizeof(S5Byte), byte_offset);
			byte_offset += sizeof(S5Byte);
			CByte::KeywordBytes(tree_id, (char *)&match, sizeof(uChar), byte_offset);
			byte_offset += sizeof(uChar);

			CNeighbourNodes::UpdateNeighbourHoodScore(node_id, match);
			src_id = m_exp_rew.AddLink(doc_id, node_id, match);
		}	

		if(src_id > 0) {
			m_exp_rew.NormalizeLinkSet(src_id);
		}
	}

public:

	CExcerptKeywords() {
		m_occur.Initialize(1024);
		m_keyword_map.Initialize(HashCode, Equal, 4096);
		m_doc_map.Initialize(4096);
		m_keyword_buff.Initialize(4096);
		m_keyword_offset.Initialize(4096);
		m_checksum_map.Initialize(1024);

		m_client_boundary.ReadMemoryChunkFromFile(CLIENT_BOUND_DIR);
		m_bound_tree.Initialize(m_client_boundary.OverflowSize(), CompareTreeNode);

		STreeBound node_bound;
		for(int i=0; i<m_client_boundary.OverflowSize()-1; i++) {
			node_bound.start = m_client_boundary[i];
			node_bound.end = m_client_boundary[i+1];
			node_bound.tree_id = i;
			m_bound_tree.AddNode(node_bound);
		}
	}

	// This retrieves the keyword byte offset and the doc id for a given node id
	// @param doc_id - this is the corresponding doc id for the node id
	// @param keyword_byte_offset - this is the byte offset for the keyword set
	// @param node_id - this is the supplied node id
	// @return the tree id
	int RetrieveDocID(S5Byte &doc_id, _int64 &keyword_byte_offset, S5Byte &node_id) {

		static STreeBound node_bound;
		node_bound.start = node_id.Value();
		node_bound.end = node_bound.start + 1;
		STreeBound *ptr = m_bound_tree.FindNode(node_bound);

		_int64 offset = (node_bound.start - ptr->start) * (sizeof(S5Byte) << 1);
		CByte::DocIDLookup(ptr->tree_id, CUtility::SecondTempBuffer(), sizeof(S5Byte) << 1, offset);
		memcpy((char *)&doc_id, CUtility::SecondTempBuffer(), sizeof(S5Byte));
		memcpy((char *)&keyword_byte_offset, CUtility::SecondTempBuffer() + sizeof(S5Byte), sizeof(S5Byte));

		return ptr->tree_id;
	}

	// This updates the limiting distribution for each node
	inline void UpdateLimDist(int it_num) {
		m_exp_rew.ApproxTravProb(it_num);
	}

	// This approximates the traversal prob of each node
	inline SDocMatch *NextNode() {
		SDocMatch *doc_ptr = m_exp_rew.NextNode();

		if(doc_ptr != NULL) {
			AddExcerptKeywords(doc_ptr->doc_id);
		}

		return doc_ptr;
	}

	// This resets this class after finished processing a query
	void Reset() {
		m_occur.Resize(0);
		m_keyword_map.Reset();
		m_checksum_map.Reset();
		m_keyword_offset.Resize(0);
		m_keyword_offset.PushBack(0);
		m_keyword_buff.Resize(0);
		m_doc_map.Reset();
		m_exp_rew.Reset();
	}

	// This adds the set of associated terms for a given query which are used
	// as seed terms to guide the search.
	void AddSeedTerms(COpenConnection &conn) {

		S5Byte keyword_id;
		float weight;
		int assoc_num;

		conn.Receive((char *)&assoc_num, sizeof(int));
		cout<<assoc_num<<" Associations"<<endl;

		for(int i=0; i<assoc_num; i++) {

			conn.Receive((char *)&keyword_id, sizeof(S5Byte));
			conn.Receive((char *)&weight, sizeof(float));

			int id = m_keyword_map.Put(keyword_id);
			if(m_keyword_map.AskFoundWord() == false) {
				m_occur.ExtendSize(1);
				m_occur.LastElement().occur = 1.0f;
				m_occur.LastElement().is_assoc = true;
				m_occur.LastElement().spatial_score = 0;
				m_occur.LastElement().keyword_id = keyword_id;
			}
		}
	}

	// This adds the excerpt keywords that belong to a particular document
	bool AddExcerptKeywords(S5Byte &node_id) {

		static int enc_bytes;
		static CMemoryChunk<char> ab_buff(11);
		char *ab_buff_ptr = ab_buff.Buffer();
		_int64 byte_offset = 0;
		S5Byte doc_id;

		int tree_id = RetrieveDocID(doc_id, byte_offset, node_id);

		int id = m_doc_map.AddWord((char *)&node_id, sizeof(S5Byte));
		if(m_doc_map.AskFoundWord() == true) {
			return false;
		}

		CByte::KeywordBytes(tree_id, ab_buff.Buffer(), 
			ab_buff.OverflowSize(), byte_offset);

		if(CArray<char>::GetEscapedItem(ab_buff_ptr, enc_bytes) == 0) {
			m_keyword_offset.PushBack(m_keyword_buff.Size());
			return false;
		}

		byte_offset += enc_bytes;

		ab_buff_ptr += 4;
		uChar keyword_num = *(uChar *)ab_buff_ptr;
		byte_offset += 5;

		float avg = 0;
		for(uChar i=0; i<keyword_num; i++) {
			avg += RetrieveExcerptWordID(byte_offset, tree_id);
		}

		avg /= keyword_num;
		m_keyword_offset.PushBack(m_keyword_buff.Size());

		if(tree_id != 197) {
			ProcessNeighbourNodes(byte_offset, node_id, tree_id, avg);
		}

		return true;
	}

	// This calculates the keyword score for a given document 
	void SendExcerptKeywords(S5Byte &node_id, COpenConnection &conn) {

		static S5Byte doc_id;
		static int enc_bytes;
		static _int64 byte_offset = 0;
		static CMemoryChunk<char> ab_buff(11);
		char *ab_buff_ptr = ab_buff.Buffer();
		uChar keyword_num = 0;

		int tree_id = RetrieveDocID(doc_id, byte_offset, node_id);

		CByte::KeywordBytes(tree_id, ab_buff.Buffer(), 
			ab_buff.OverflowSize(), byte_offset);

		if(CArray<char>::GetEscapedItem(ab_buff_ptr, enc_bytes) == 0) {
			conn.Send((char *)&keyword_num, sizeof(uChar));
			return;
		}

		byte_offset += enc_bytes;
		ab_buff_ptr += 4;
		keyword_num = *(uChar *)ab_buff_ptr;
		byte_offset += 5;

		conn.Send((char *)&keyword_num, sizeof(uChar));

		for(uChar i=0; i<keyword_num; i++) {
			CByte::KeywordBytes(tree_id, ab_buff.Buffer(), sizeof(S5Byte), byte_offset);
			conn.Send(ab_buff.Buffer(), sizeof(S5Byte));
			byte_offset += sizeof(S5Byte);
		}
	}

	// This calculates the keyword score for a given spatial region
	// @param keyword_byte_offset - this is the keyword offset for this document
	// @param is_invert_score - true if the inverted score is being used false otherwise
	float KeywordDocumentScore(int tree_id, _int64 &keyword_byte_offset,
		bool is_invert_score, bool is_cache = false) {

		_int64 temp = keyword_byte_offset;
		*((u_short *)&temp + 3) = tree_id;
		int id = m_doc_map.AddWord((char *)&temp, sizeof(_int64));
		if(m_doc_map.AskFoundWord() == true && is_cache == true) {
			return KeywordDocumentScore(id);
		}

		float score = 0;
		static int enc_bytes;
		static CMemoryChunk<char> ab_buff(6);
		char *ab_buff_ptr = ab_buff.Buffer();
		uLong keyword_num;

		CByte::SLinkBytes(tree_id, ab_buff.Buffer(), 
			ab_buff.OverflowSize(), keyword_byte_offset);

		if((keyword_num = CArray<char>::GetEscapedItem(ab_buff_ptr, enc_bytes)) == 0) {
			if(m_doc_map.AskFoundWord() == false) {
				m_keyword_offset.PushBack(m_keyword_buff.Size());
			}

			keyword_byte_offset += enc_bytes;
			return 0;
		}

		static S5Byte keyword_id;
		keyword_byte_offset += enc_bytes;

		for(uLong i=0; i<keyword_num; i++) {

			CByte::SLinkBytes(tree_id, (char *)&keyword_id, sizeof(S5Byte), keyword_byte_offset);

			int id = m_keyword_map.Put(keyword_id);
			if(m_keyword_map.AskFoundWord() == false) {
				m_occur.ExtendSize(1);
				m_occur.LastElement().occur = 0;
				m_occur.LastElement().is_assoc = false;
				m_occur.LastElement().spatial_score = 0;
				m_occur.LastElement().keyword_id = keyword_id;
			}

			m_occur[id].spatial_score++;
			if(m_doc_map.AskFoundWord() == false) {
				m_keyword_buff.PushBack(id);
			}

			if(id >= 0 && m_occur[id].occur < 200 && m_occur[id].occur > 0) {
				if(is_invert_score == true) {
					score += 1.0f / m_occur[id].occur;
				} else {
					score += min(8, m_occur[id].occur);
				}
			}

			keyword_byte_offset += sizeof(S5Byte);
		}

		if(m_doc_map.AskFoundWord() == false) {
			m_keyword_offset.PushBack(m_keyword_buff.Size());
		}
		
		return score;
	}



};
