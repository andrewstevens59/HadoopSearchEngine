#include "../../MapReduce.h"

// This class is used to test that all the hits are sorted correctly
class CTestSortHitList : public CNodeStat {

	// This stores the final sorted hit list
	CFileComp m_sorted_file;
public:

	CTestSortHitList() {
	}

	// This is the entry function
	void TestSortHitList() {
		m_sorted_file.OpenReadFile(CUtility::ExtendString
			("GlobalData/SortedHits/sorted_base_hits", GetClientID()));

		SHitItem hit;
		_int64 doc_id = 0;
		uLong word_id = 0;
		u_short enc = 0;
		hit.enc = 0;

		while(hit.ReadHitWordOrder(m_sorted_file)) {

			if(hit.word_id < word_id) {
				cout<<"Word ID error";getchar();
			}

			if(hit.word_id > word_id) {
				word_id = hit.word_id;
				doc_id = hit.doc_id.Value();
				enc = hit.enc;
			}

			if(hit.doc_id < doc_id) {
				cout<<"Doc ID error";getchar();
			}

			if(hit.doc_id > doc_id) {
				doc_id = hit.doc_id.Value();
				enc = hit.enc;
			}

			if(hit.enc < enc) {
				cout<<"ENC error";getchar();
			}

			enc = hit.enc;
		}
	}
};