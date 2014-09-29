#include "../../../../MapReduce.h"

// This takes the set of excerpt keywords that belongs to each
// document and groups terms together. Terms are grouped togeher
// within some finite distance from one another to limite the 
// search space. Terms that exceed this distance are not grouped
// and instead must wait until the next cycle.
class CGroupTerms : public CNodeStat {

	// This defines one of the grouped terms 
	struct SGroupTerm {
		// This is the number of terms in the keyword
		uChar group_size;
		// This is the keyword id of the term
		S5Byte id;
	};

	// This stores the set of excerpt keywords 
	// for each document 
	CHDFSFile m_excerpt_keyword_file;
	// This stores the set of grouped terms
	CSegFile m_group_term_file;
	// This stores the set of keywords for a given document
	CArray<SGroupTerm> m_keyword_set;
	// This stores the scan size window when grouping terms
	int m_scan_size;

	// This groups terms based upon the keyword set
	void GroupTerms() {

		static SGroupTerm term1;
		static SGroupTerm term2;
		for(int i=0; i<m_keyword_set.Size(); i++) {
			int end = min(i + m_scan_size, m_keyword_set.Size());
			for(int j=i+1; j<end; j++) {
				term1 = m_keyword_set[i];
				term2 = m_keyword_set[j];

				if(term1.id == term2.id) {
					continue;
				}

				if(term1.id > term2.id) {
					CSort<char>::Swap(term1, term2);
				}

				m_group_term_file.AskBufferOverflow(sizeof(S5Byte) << 1);
				m_group_term_file.WriteCompObject(term1.id);
				m_group_term_file.WriteCompObject(term2.id);
			}
		}
	}

	// This parses the set of excerpt keywords in order to join terms
	void ParseExerptKeywordSet() {

		S5Byte doc_id;
		uChar keyword_num;
		float weight;
		uLong occur;

		while(m_excerpt_keyword_file.ReadCompObject(doc_id)) {
			m_excerpt_keyword_file.ReadCompObject(keyword_num);
			m_keyword_set.Resize(0);

			for(uChar i=0; i<keyword_num; i++) {
				SGroupTerm *ptr = m_keyword_set.ExtendSize(1);
				m_excerpt_keyword_file.ReadCompObject(ptr->id);
				m_excerpt_keyword_file.ReadCompObject(occur);
				m_excerpt_keyword_file.ReadCompObject(weight);
				m_excerpt_keyword_file.ReadCompObject(ptr->group_size);
			}

			GroupTerms();
		}
	}

public:

	CGroupTerms() {
		CHDFSFile::Initialize();
	}

	// This is the entry function
	// @param excerpt_dir - this is the directory of the excerpt keywords
	// @param level - this is the current level in the hiearchy being processed
	// @param scan_window - this is the size of the scan window when joining terms
	void GroupTermss(int client_id, int level, int scan_window) {

		CNodeStat::SetClientID(client_id);
		m_keyword_set.Initialize(100);
		m_scan_size = scan_window;

		if(level == 0) {
			m_excerpt_keyword_file.OpenReadFile(CUtility::ExtendString
				("GlobalData/ABTrees/excerpt_term_set", GetClientID()));
		} else {
			m_excerpt_keyword_file.OpenReadFile(CUtility::ExtendString
				("LocalData/excerpt_keywords", level, ".set", GetClientID()));
		}

		m_group_term_file.OpenWriteFile(CUtility::ExtendString
			("LocalData/grouped_terms", GetClientID()));

		ParseExerptKeywordSet();
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);
	int level = atoi(argv[2]);
	int scan_window = atoi(argv[3]);

	CBeacon::InitializeBeacon(client_id);
	CMemoryElement<CGroupTerms> group;
	group->GroupTermss(client_id, level, scan_window);
	group.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();
}