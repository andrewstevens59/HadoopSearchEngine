#include "../../../MapReduce.h"

// This is used to join keyword hit doc ids using a catesian join.
// The maximum size of the cartesian join is specified to reduce
// the place a limit on the time complexity. Note also that keyword
// hits are sorted by their pulse score so documents with similar 
// pulse scores are joined together with a finite window size.
class CCreateKeywordLinks : public CNodeStat {

	// This is used for sorting
	struct SKeywordHitItemPtr {
		SKeywordHitItem *ptr;
	};

	// This is used to store the keyword hits
	CLinkedBuffer<SKeywordHitItem> m_keyword_hit_buff;
	// This is used to sort keyword hits
	CArray<SKeywordHitItemPtr> m_keyword_ptr;
	// This stores the window size
	int m_window_size; 

	// This stores the set of merged hit items
	CHDFSFile m_keyword_hit_file;
	// This stores the keyword links
	CSegFile m_keyword_link_file;

	// This is used to compare hit items
	static int CompareKeywordHits(const SKeywordHitItemPtr &arg1, const SKeywordHitItemPtr &arg2) {

		if(arg1.ptr->pulse_score < arg2.ptr->pulse_score) {
			return -1;
		}

		if(arg1.ptr->pulse_score > arg2.ptr->pulse_score) {
			return 1;
		}

		return 0;
	}

	// This processes a given keyword division
	void ProcessKeywordDivision() {

		static SKeywordHitItem *keyword;
		if(m_keyword_ptr.OverflowSize() < m_keyword_hit_buff.Size()) {
			m_keyword_ptr.Initialize(m_keyword_hit_buff.Size());
		}

		SKeywordHitItemPtr ptr;
		m_keyword_hit_buff.ResetPath();
		m_keyword_ptr.Resize(0);
		while((keyword = m_keyword_hit_buff.NextNode()) != NULL) {
			ptr.ptr = keyword;
			m_keyword_ptr.PushBack(ptr);
		}

		if(m_keyword_ptr.Size() > m_window_size) {
			CSort<SKeywordHitItemPtr> sort(m_keyword_ptr.Size(), CompareKeywordHits);
			sort.HybridSort(m_keyword_ptr.Buffer());
		}

		SWLink w_link;
		for(int i=0; i<m_keyword_ptr.Size(); i++) {
			int scan_size = min(m_keyword_ptr.Size(), i + m_window_size);
			for(int j=i+1; j<scan_size; j++) {

				SKeywordHitItem &hit1 = *m_keyword_ptr[i].ptr;
				SKeywordHitItem &hit2 = *m_keyword_ptr[j].ptr;

				if(hit1.check_sum == hit2.check_sum) {
					continue;
				}

				w_link.src = hit1.doc_id;
				w_link.dst = hit2.doc_id;
				w_link.link_weight = hit2.keyword_score;

				w_link.WriteWLink(m_keyword_link_file);
				CSort<char>::Swap(w_link.src, w_link.dst);
				w_link.WriteWLink(m_keyword_link_file);
			}
		}
	}

	// This parses a given keyword division
	void ParseKeywordDivision() {

		SKeywordHitItem hit_item;
		_int64 curr_keyword_id = -1;
		while(hit_item.ReadKeywordHit(m_keyword_hit_file)) {

			if(hit_item.keyword_id != curr_keyword_id) {
				ProcessKeywordDivision();
				curr_keyword_id = hit_item.keyword_id.Value();
				m_keyword_hit_buff.Restart();
			}

			m_keyword_hit_buff.PushBack(hit_item);
		}
		
		ProcessKeywordDivision();
	}

public:

	CCreateKeywordLinks() {
		CHDFSFile::Initialize();
	}

	// This is the entry function
	// @param window_size - this is the maximum size of the cartesian join
	void CreateKeywordLinks(int client_id, int window_size) {

		CNodeStat::SetClientID(client_id);
		m_window_size = window_size;

		m_keyword_hit_file.OpenReadFile(CUtility::ExtendString
			("LocalData/keyword_hit_file", CNodeStat::GetClientID()));

		m_keyword_link_file.OpenWriteFile(CUtility::ExtendString
			("LocalData/bin_link_file", CNodeStat::GetClientID()));

		m_keyword_hit_buff.Initialize();
		m_keyword_ptr.Initialize(2048);

		ParseKeywordDivision();
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);
	int window_size = atoi(argv[2]);

	CBeacon::InitializeBeacon(client_id);
	CMemoryElement<CCreateKeywordLinks> group;
	group->CreateKeywordLinks(client_id, window_size);

	group.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();
}