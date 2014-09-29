#include "../../../../MapReduce.h"

// This takes the set of grouped terms and their occurrences
// and ranks the set of excerpt keywords. Any new keywords
// formed by grouped terms are assigned a unique id. If none
// of the new keywords have a higher weight than the existing
// keyword set than this excerpt is complete. At this point
// the top N keywords are taken and added to the final set
// of keywords for a given excerpt.
class CReduceExcerptKeywordSet : public CNodeStat {

	// This defines one of the grouped terms pairs
	struct SGroupTerm {
		// This is the id of the first grouped term
		S5Byte keyword_id1;
		// This is the id of the second grouped terms
		S5Byte keyword_id2;
		// This stores the occurrence of the first term
		uLong keyword_occur1;
		// This stores the occurrence of the second term
		uLong keyword_occur2;
		// This stores the id of the grouped term
		S5Byte group_id;
		// This store the occurrence of the grouped term
		uLong occur;
		// This store the group term weight
		float term_weight;
	};

	// This stores a particular term in the set 
	struct SKeyword {
		// This stores the keyword id
		S5Byte id;
		// This stores the group size
		uChar group_size;
		// This stores the position of the keyword in the document
		uChar pos;
		// This stores the occurrence
		uLong occurr;
		// This stores the weight of the grouped term
		float term_weight;
	};

	// This is used to sort keywords
	struct SKeywordPtr {
		SKeyword *ptr;
	};

	// This stores the current set of excerpt keywords for each document
	CHDFSFile m_curr_excerpt_keyword_file;
	// This stores the new set of excerpt keywords for each document
	CHDFSFile m_new_excerpt_keyword_file;
	// This stores the set of grouped terms, occurrences and ids
	CSegFile m_group_term_file;
	// This stores the set of keywords for a given document
	CArray<SKeyword> m_keyword_set;

	// This stores the list of priority tokens
	CLimitedPQ<SKeywordPtr> m_tok_queue;
	// This stores the buffer of tok queue used for sorting by position
	CArray<SKeywordPtr> m_tok_buff;
	// This is used to remove duplicate terms
	CTrie m_duplicate_map;

	// This is used to store the keyword set
	CLinkedBuffer<SKeyword> m_keyword_buff;
	// This stores the set of grouped terms
	CArrayList<SGroupTerm> m_group_term_set;
	// This stores the total number of new keywords that are added to the set
	_int64 m_new_keyword_num;
	// This stores the scan size window when grouping terms
	int m_scan_size;

	// This is used to compare tokens on word occurrence
	static int CompareKeywords(const SKeywordPtr &arg1, const SKeywordPtr &arg2) {

		if(arg1.ptr->group_size < arg2.ptr->group_size) {
			return -1;
		}

		if(arg1.ptr->group_size > arg2.ptr->group_size) {
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

	// This is used to compare keywords based upon their position in the document
	static int ComparePosition(const SKeywordPtr &arg1, const SKeywordPtr &arg2) {

		if(arg1.ptr->pos < arg2.ptr->pos) {
			return 1;
		}

		if(arg1.ptr->pos > arg2.ptr->pos) {
			return -1;
		}

		return 0;
	}

	// This adds a keyword to the set
	// @param group_term - this is one of the grouped terms
	// @param group_size - the number of terms that make up the keyword
	// @param pos - this is the position of the keyword in the document
	// @return false if a duplicate term, false otherwise
	bool AddKeyword(SGroupTerm &group_term, uChar group_size, uChar pos) {

		m_duplicate_map.AddWord((char *)&group_term.group_id, sizeof(S5Byte));
		if(m_duplicate_map.AskFoundWord()) {
			return false;
		}

		SKeyword *ptr = m_keyword_buff.ExtendSize(1);
		ptr->id = group_term.group_id;
		ptr->occurr = group_term.occur;
		ptr->group_size = min(group_size, 2);
		ptr->term_weight = group_term.term_weight;
		ptr->pos = pos;

		static SKeywordPtr keyword;
		keyword.ptr = ptr;
		m_tok_queue.AddItem(keyword);

		return true;
	}

	// This retrieves the next group term from the set 
	// @param group_term - this is the current group term being processed
	bool RetrieveNextGroupTerm(SGroupTerm &group_term) {

		static uChar term_size;
		static CMemoryChunk<S5Byte> set_id(2);

		if(!m_group_term_file.ReadCompObject(set_id.Buffer(), set_id.OverflowSize())) {
			cout<<"goe ";getchar();
			return false;
		}

		if(set_id[0] != group_term.keyword_id1) {
			cout<<"ID Mismatch";getchar();
		}

		if(set_id[1] != group_term.keyword_id2) {
			cout<<"ID Mismatch";getchar();
		}

		m_group_term_file.ReadCompObject(term_size);

		if(term_size == 0) {
			return false;
		}
	
		m_group_term_file.ReadCompObject(group_term.group_id);
		m_group_term_file.ReadCompObject(group_term.occur);

		float f11 = group_term.occur;
		float f01 = group_term.keyword_occur1 - group_term.occur;
		float f10 = group_term.keyword_occur2 - group_term.occur;
		group_term.term_weight = f11 / (f01 * f10);

		if(f01 < 0 || f10 < 0) {
			cout<<"subsume error";getchar();
		}

		return true;

	}

	// This stores the keyword set in the queue
	void StoreUpdateKeyworSet(S5Byte &doc_id) {
		
		uChar set_size = m_tok_queue.Size();
		m_new_excerpt_keyword_file.WriteCompObject(doc_id);
		m_new_excerpt_keyword_file.WriteCompObject(set_size);

		m_tok_queue.CopyNodesToBuffer(m_tok_buff);

		CSort<SKeywordPtr> sort(m_tok_buff.Size(), ComparePosition);
		sort.HybridSort(m_tok_buff.Buffer());

		for(int i=0; i<m_tok_buff.Size(); i++) {
			SKeyword *ptr = m_tok_buff[i].ptr;
			m_new_excerpt_keyword_file.WriteCompObject(ptr->id);
			m_new_excerpt_keyword_file.WriteCompObject(ptr->occurr);
			m_new_excerpt_keyword_file.WriteCompObject(ptr->term_weight);
			m_new_excerpt_keyword_file.WriteCompObject(ptr->group_size);
		}
	}

	// This groups terms based upon the keyword set
	void GroupTerms() {

		static SGroupTerm group_term;
		static uLong occur;

		for(int i=0; i<m_keyword_set.Size(); i++) {
			int end = min(i + m_scan_size, m_keyword_set.Size());
			for(int j=i+1; j<end; j++) {

				SKeyword &keyword1 = m_keyword_set[i];
				SKeyword &keyword2 = m_keyword_set[j];
				group_term.keyword_id1 = keyword1.id;
				group_term.keyword_id2 = keyword2.id;

				if(group_term.keyword_id1 == group_term.keyword_id2) {
					continue;
				}

				group_term.keyword_occur1 = m_keyword_set[i].occurr;
				group_term.keyword_occur2 = m_keyword_set[j].occurr;

				if(group_term.keyword_id1 > group_term.keyword_id2) {
					CSort<char>::Swap(group_term.keyword_id1, group_term.keyword_id2);
				}

				if(RetrieveNextGroupTerm(group_term)) {
					uChar pos = (i + j) >> 1;
					uChar group_size = keyword1.group_size + keyword2.group_size;
					if(AddKeyword(group_term, group_size, pos)) {
						m_new_keyword_num++;
					}
				}
			}
		}
	}

	// This parses the set of excerpt keywords in order to join terms
	void ParseExerptKeywordSet() {

		S5Byte doc_id;
		uChar keyword_num;
		uLong occur;
		float weight;

		while(m_curr_excerpt_keyword_file.ReadCompObject(doc_id)) {
			m_curr_excerpt_keyword_file.ReadCompObject(keyword_num);
			m_keyword_set.Resize(0);
			m_duplicate_map.Reset();
			m_tok_queue.Reset();

			for(uChar i=0; i<keyword_num; i++) {
				SKeyword *ptr = m_keyword_set.ExtendSize(1);
				m_curr_excerpt_keyword_file.ReadCompObject(ptr->id);
				m_curr_excerpt_keyword_file.ReadCompObject(occur);
				m_curr_excerpt_keyword_file.ReadCompObject(weight);
				m_curr_excerpt_keyword_file.ReadCompObject(ptr->group_size);
			}

			GroupTerms();
			StoreUpdateKeyworSet(doc_id);
		}
	}

	// This sets up the various files
	// @param level - this is the current level in the hiearchy of grouped terms
	void Initialize(int level) {

		if(level == 0) {
			m_curr_excerpt_keyword_file.OpenReadFile(CUtility::ExtendString
				("GlobalData/ABTrees/excerpt_term_set", GetClientID()));
		} else {
			m_curr_excerpt_keyword_file.OpenReadFile(CUtility::ExtendString
				("LocalData/excerpt_keywords", level, ".set", GetClientID()));
		}

		m_new_excerpt_keyword_file.OpenWriteFile(CUtility::ExtendString
			("LocalData/excerpt_keywords", level + 1, ".set", GetClientID()));

		m_group_term_file.OpenReadFile(CUtility::ExtendString
			("LocalData/mapped_group_terms", GetClientID()));
	}

public:

	CReduceExcerptKeywordSet() {
		CHDFSFile::Initialize();
	}

	// This is the entry function
	// @param max_set_size - this is the maximum number of keywords 
	//                     - allowed in the excerpt set
	// @param level - this is the current level in the hiearchy of grouped terms
	// @param scan_window - this is the size of the scan window when joining terms
	void ReduceExcerptKeywordSet(int client_id, int max_set_size, 
		int level, int scan_window) {

		CNodeStat::SetClientID(client_id);
		m_tok_queue.Initialize(max_set_size, CompareKeywords);
		m_tok_buff.Initialize(max_set_size);
		m_scan_size = scan_window;

		m_duplicate_map.Initialize(4);
		m_keyword_set.Initialize(100);
		m_keyword_buff.Initialize();
		m_new_keyword_num = 0;

		Initialize(level);
		ParseExerptKeywordSet();

		CHDFSFile new_keyword_num_file;
		new_keyword_num_file.OpenWriteFile(CUtility::ExtendString
			("LocalData/keyword_added", GetClientID()));
		new_keyword_num_file.WriteObject(m_new_keyword_num);
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);
	int max_set_size = atoi(argv[2]);
	int level = atoi(argv[3]);
	int scan_window = atoi(argv[4]);

	CBeacon::InitializeBeacon(client_id);
	CMemoryElement<CReduceExcerptKeywordSet> group;
	group->ReduceExcerptKeywordSet(client_id, max_set_size, level, scan_window);

	group.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();
}
