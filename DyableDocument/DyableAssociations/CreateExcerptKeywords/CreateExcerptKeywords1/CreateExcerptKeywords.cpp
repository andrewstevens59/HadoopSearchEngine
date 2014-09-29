#include "../../../../MapReduce.h"

// This parses the document set and the set of mapped associations
// and creates a set of keywords for each excerpt. Keywords are ranked
// based upon their asssociation occurrence and the sum of individual
// word id occurrences. These are placed in a priority queue to limit
// the size of the total number of keywords.
class CCreateExcerptKeywords : public CNodeStat {

	struct SToken {
		// This stores the word id 
		S5Byte word_id;
		// This stores the position of the term 
		uChar pos;
		// This stores the association size
		uChar assoc_num;
		// This stores the term id hit encoding
		uChar hit_enc;
		// This stores the global occurrence of the term
		uLong occur;
		// This stores the association weight
		float term_weight;
	};

	// This stores one of the terms in the sliding window
	struct SWindowTerm {
		// This stores the term id
		uLong word_id;
		// This stores the occurrence of the term
		uLong occur;
		// This stores the hit encoding
		uChar hit_enc;
	};

	// This stores a ptr to a token
	struct STokenPtr {
		SToken *ptr;
	};

	// This stores the list of tokens that make up a document 
	CLinkedBuffer<SToken> m_tok_buff;
	// This stores the list of priority tokens
	CLimitedPQ<STokenPtr> m_tok_queue;
	// This is used to find duplicate terms
	CTrie m_duplicate_term;
	// This stores the occurrence of each term in the document
	CArrayList<int> m_term_occurr;
	// This is used to store the terms once they have been reduced
	CArray<STokenPtr> m_sorted_tok_set;
	// This stores the set of singular terms 
	CLinkedBuffer<SToken> m_singular_term_buff;

	// This stores the compiled hitlist containing attributes only
	CMemoryChunk<CHDFSFile> m_attr_hit_list;

	// This is used to store the excerpt priority terms
	CSegFile m_excerpt_file;
	// This stores the hit encoding for eac word id that composes an excerpt
	CHDFSFile m_hit_enc_file;
	// This stores the weight and association id for each id in the set
	CMemoryChunk<CSegFile> m_assoc_id_file;
	// This stores the number of word ids that compose each document
	CHDFSFile m_doc_size_file;

	// This stores the occurrence of each term in the excerpt set
	CHDFSFile m_excerpt_occur_map_file;
	// This stores the current association
	CCyclicArray<SWindowTerm> m_assoc_set;

	// This is used to compare tokens on word occurrence
	static int CompareTokens(const STokenPtr &arg1, const STokenPtr &arg2) {

		if(arg1.ptr->assoc_num < arg2.ptr->assoc_num) {
			return -1;
		}

		if(arg1.ptr->assoc_num > arg2.ptr->assoc_num) {
			return 1;
		}

		if(arg1.ptr->term_weight < arg2.ptr->term_weight) {
			return -1;
		}

		if(arg1.ptr->term_weight > arg2.ptr->term_weight) {
			return 1;
		}

		return 0;
	}

	// This is used to sort the keywords based upon their position in the document
	static int CompareTermPosition(const STokenPtr &arg1, const STokenPtr &arg2) {

		if(arg1.ptr->pos < arg2.ptr->pos) {
			return 1;
		}

		if(arg1.ptr->pos > arg2.ptr->pos) {
			return -1;
		}

		return 0;
	}

	// This creates the list of excerpt terms that are later stored in the ab_tree.
	// @param doc_id - this is the doc id of the excerpt
	void StoreExcerptPriorityTerms(_int64 &doc_id) {

		uChar size = (uChar)m_tok_queue.Size();
		int ovf_size = sizeof(S5Byte) + sizeof(uLong) + sizeof(uChar);
		ovf_size += (sizeof(S5Byte) + sizeof(uChar)) * m_tok_queue.Size();

		m_excerpt_file.AskBufferOverflow(ovf_size);
		m_excerpt_file.WriteCompObject5Byte(doc_id);
		m_excerpt_file.WriteCompObject(size);

		m_tok_queue.CopyNodesToBuffer(m_sorted_tok_set);
		CSort<STokenPtr> sort(m_sorted_tok_set.Size(), CompareTermPosition);
		sort.HybridSort(m_sorted_tok_set.Buffer());
	
		for(int i=0; i<m_sorted_tok_set.Size(); i++) {
			SToken &ptr = *m_sorted_tok_set[i].ptr;
			int id = m_duplicate_term.FindWord((char *)&ptr.word_id, sizeof(S5Byte));
			ptr.assoc_num <= 3;
			ptr.assoc_num |= m_term_occurr[id];

			m_excerpt_file.WriteCompObject(ptr.word_id);
			m_excerpt_file.WriteCompObject(ptr.occur);
			m_excerpt_file.WriteCompObject(ptr.term_weight);
			m_excerpt_file.WriteCompObject(ptr.assoc_num);
		}
	}

	// This retrieves the next association type from the set. If the association
	// exceeds the threshold then it is added to the list of priority associations
	// otherwise the association is disregarded. A disregarded association a byte
	// size of zero.
	// @param token - this is one of the associations
	// @param term_num - this is the length of the association
	bool RetrieveAssociation(SToken &token, int term_set) {

		static uChar term_size;
		static CMemoryChunk<uLong> set_id(2);
		CSegFile &assoc_file = m_assoc_id_file[term_set];

		if(!assoc_file.ReadCompObject(set_id.Buffer(), set_id.OverflowSize())) {
			cout<<"goe "<<term_set;getchar();
			return false;
		}

		if(set_id[0] != m_assoc_set[0].word_id) {
			cout<<"ID Mismatch";getchar();
		}

		if(set_id[1] != m_assoc_set[1].word_id) {
			cout<<"ID Mismatch";getchar();
		}

		assoc_file.ReadCompObject(term_size);

		if(term_size == 0) {
			return false;
		}
	
		token.assoc_num = 2;
		assoc_file.ReadCompObject(token.word_id);
		assoc_file.ReadCompObject(token.occur);
		token.hit_enc = m_assoc_set[0].hit_enc | m_assoc_set[1].hit_enc;

		float f11 = token.occur;
		float f01 = m_assoc_set[0].occur - token.occur + 1;
		float f10 = m_assoc_set[1].occur - token.occur + 1;
		token.term_weight = f11 / (f01 * f10);

		if(f01 < 0 || f10 < 0) {
			cout<<"subsume error";getchar();
		}

		return true;
	}

	// This adds one of the terms to the set of terms
	// @param token - this is one of the associations
	bool AddTermToSet(SToken &token) {

		int id = m_duplicate_term.AddWord((char *)&token.word_id, sizeof(S5Byte));

		if(m_duplicate_term.AskFoundWord()) {
			if(m_term_occurr[id] < 4) {
				m_term_occurr[id]++;
			}

			return true;
		}

		m_term_occurr.PushBack(1);
		return false;
	}

	// This adds one of the associations to the ranked queue
	// @param token - this is one of the associations
	inline bool AddAssocToQueue(SToken &token) {

		if(token.hit_enc == 0xFF) {
			// does not accept title terms as keywords
			return false;
		}

		if(AddTermToSet(token)) {
			return false;
		}

		static STokenPtr ptr;
		m_tok_buff.PushBack(token);
		ptr.ptr = &m_tok_buff.LastElement();
		m_tok_queue.AddItem(ptr);
		return true;
	}

	// This processes the next association available in each of the different
	// size classes. It first checks if the number of terms passed passed
	// matches the size class. It then proceeds to add a hit item to match
	// the association in the set.
	// @param id - the current id of the term being processed
	// @param doc_id - this is the doc id of the excerpt
	// @param token - this is one of the associations
	inline bool ProcessAssociations(uLong id, _int64 &doc_id, SToken &token) {

		static SHitItem hit_item;
		static uLong word_id;
		hit_item.image_id = -1;

		bool assoc_found = false;
		for(int j=0; j<ASSOC_SET_NUM; j++) {
			if(RetrieveAssociation(token, j)) {
				AddAssocToQueue(token);
				assoc_found = true;

				hit_item.doc_id = doc_id;
				hit_item.enc = token.hit_enc;
				hit_item.word_id = token.word_id.Value() / CNodeStat::GetHashDivNum();
				int div = token.word_id.Value() % CNodeStat::GetHashDivNum();

				hit_item.enc |= (uLong)(id << 3);
				hit_item.WriteHitDocOrder(m_attr_hit_list[div]);
			}
		}

		return assoc_found;
	}

	// This retrieves the next term in the excerpt. It also retrieves the local
	// and global term occurrence of the singular term. Global term occurrence
	// refers to the occurrence in the entire document set. Local term occurrence
	// refers to the occurrence in the associaiton set.
	void RetrieveNextTerm(SToken &token) {

		static uLong word_id1;
		static uChar byte_num;
		static SWindowTerm window;

		if(!m_excerpt_occur_map_file.ReadCompObject(word_id1)) {
			cout<<"no more1";getchar();
		}

		token.word_id = word_id1;
		m_hit_enc_file.ReadCompObject(token.hit_enc);
		m_excerpt_occur_map_file.ReadCompObject(token.occur);

		window.word_id = word_id1;
		window.occur = token.occur;
		window.hit_enc = token.hit_enc;
		m_assoc_set.PushBack(window);

		token.assoc_num = 1;
		token.term_weight = 1.0f / token.occur;
		// singular terms with lower occurrence have higher weight
		m_singular_term_buff.PushBack(token);
	}

	// This runs through all of the terms in a document and creates a mapping
	// between a word text string and the id. It also find the top N words
	// with the lowest occurrence and no suffix to add to a set of keywords.
	// @param doc_id - the current doc id being processed
	// @param doc_size - this is the number of words in the document
	inline void ProcessDocument(_int64 &doc_id, uLong doc_size) {

		static SToken token;
		static bool assoc_found;
		static uLong word_id;
		static int space = 0;
		static uChar enc;

		m_assoc_set.Initialize(2);
		for(uLong i=0; i<doc_size; i++) {

			token.pos = i;
			RetrieveNextTerm(token);

			if(++space >= 10000) {
				cout<<(m_excerpt_occur_map_file.PercentageFileRead() * 100)<<"% Done"<<endl;
				space = 0;
			}

			if(m_assoc_set.Size() < m_assoc_set.OverflowSize()) {
				continue;
			}

			if(m_assoc_set[0].word_id == m_assoc_set[1].word_id) {
				continue;
			}

			if(ProcessAssociations(i, doc_id, token)) {
				token.word_id = m_assoc_set[0].word_id;
				AddTermToSet(token);
				token.word_id = m_assoc_set[1].word_id;
				AddTermToSet(token);
			}
		}
	}

	// This takes the set of word ids created during the hit list
	// phase and writes the final set of word ids with word occurrence
	void WriteDocuments() {

		uLong doc_size;
		_int64 doc_id; 

		SToken *ptr;
		while(m_doc_size_file.GetEscapedItem(doc_size) >= 0) {
			m_doc_size_file.Get5ByteCompItem(doc_id);

			m_tok_buff.Restart();
			m_tok_queue.Reset();
			m_term_occurr.Resize(0);
			m_duplicate_term.Reset();
			m_singular_term_buff.Restart();

			ProcessDocument(doc_id, doc_size);

			m_singular_term_buff.ResetPath();
			while((ptr = m_singular_term_buff.NextNode()) != NULL) {

				if(ptr->occur >= 40) {
					AddAssocToQueue(*ptr);
				}
			}

			StoreExcerptPriorityTerms(doc_id);
		}

		static uChar term_size;
		CSegFile &assoc_file = m_assoc_id_file[0];
		if(assoc_file.ReadCompObject(term_size)) {
			cout<<"still more";getchar();
		}
	}

	// This is called to initialize the document set reading for parsing
	void Initialize() {

		m_doc_size_file.OpenReadFile(CUtility::ExtendString
			("GlobalData/DocumentDatabase/doc_size", GetClientID()));

		m_excerpt_file.OpenWriteFile(CUtility::ExtendString
			("GlobalData/ABTrees/excerpt_term_set", GetClientID()));

		m_attr_hit_list.AllocateMemory(CNodeStat::GetHashDivNum());
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			m_attr_hit_list[i].OpenWriteFile(CUtility::ExtendString
				("GlobalData/HitList/assoc_fin_hit",
				i, ".client", GetClientID()));
		}
	}

public:

	CCreateExcerptKeywords() {
		
		CHDFSFile::Initialize();
		m_assoc_id_file.AllocateMemory(ASSOC_SET_NUM);
		m_duplicate_term.Initialize(4);
		m_singular_term_buff.Initialize(100);
		m_term_occurr.Initialize(1024);
		m_tok_buff.Initialize();
	}

	// This creates the final keyword set that composes each of the excerpts. Only
	// the top N keywords are stored which are ranked on their keyword weight.
	// @param hit_list_breadth - this is the number of hit list divisions
	// @param max_keyword_size - this is the maximum number of keywords 
	//                         - allowed in the initial excerp set
	void CompileExcerptKeywordSet(int client_id, int client_num, 
		int hit_list_breadth, int max_keyword_size) {

		CNodeStat::SetClientID(client_id);
		CNodeStat::SetClientNum(client_num);
		CNodeStat::SetHashDivNum(hit_list_breadth);

		m_tok_queue.Initialize(max_keyword_size, CompareTokens);
		m_sorted_tok_set.Initialize(max_keyword_size);

		Initialize();

		m_hit_enc_file.OpenReadFile(CUtility::ExtendString
			("LocalData/hit_encoding", GetClientID()));

		m_excerpt_occur_map_file.OpenReadFile(CUtility::ExtendString
			("LocalData/excerpt_occurr", GetClientID()));

		for(int i=0; i<m_assoc_id_file.OverflowSize(); i++) {
			m_assoc_id_file[i].OpenReadFile(CUtility::ExtendString
				("LocalData/mapped_assoc_file", i, ".client", GetClientID()));
		}

		cout<<endl<<"Writing Excerpt Documents"<<endl;
		WriteDocuments();
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);
	int client_num = atoi(argv[2]);
	int hit_list_breadth = atoi(argv[3]);
	int max_keyword_size = atoi(argv[4]);

	CBeacon::InitializeBeacon(client_id);
	CMemoryElement<CCreateExcerptKeywords> set;
	set->CompileExcerptKeywordSet(client_id, client_num, hit_list_breadth, max_keyword_size);
	set.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();
}
