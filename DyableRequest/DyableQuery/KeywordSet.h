#include "../ExpandABTree.h"

// This defines one of the keywords 
struct SKeyword {
	// This stores the keyword id
	S5Byte keyword_id;
	// This is a flag indicating an invalid keyword
	bool is_valid;
	// This stores the keyword occurrence
	float occur;
	// This stores the local id
	int local_id;
	// This stores the sorted rank of the keyword
	int rank;
};

// This is used to process and display the global set of keywords 
// that compose the global set of excerpts. These are displayed
// to the user in a histogram formant.
class CKeywordSet {

	struct SKeywordPtr {
		SKeyword *ptr;
	};

	// This stores the global and local keyword id
	struct SKeywordID {
		// This stores the keyword id
		S5Byte keyword_id;
		// This stores the local keyword_id
		int local_id;
	};

	// This stores the compiled set of keywords 
	static CLimitedPQ<SKeyword> m_keyword_queue;
	// This stores the set of excerpt keywords
	static CArrayList<SKeywordID> m_keyword_set;
	// This stores the set of sorted keywords
	static CArrayList<SKeyword> m_sorted_keyword_set;
	// This stores the number of keywords in each excerpt
	static CArrayList<int> m_keyword_offset;
	// This is used to resolve excerpt keywords
	static COpenConnection m_conn;
	// This stores the global set of keywords
	static CArrayList<SKeyword> m_global_keywords;
	// This stores the keyword map
	static CHashDictionary<int> m_keyword_map;
	// This stores the maximum word occurrence
	static float m_max_word_occur;
	// This stores the global keyword set
	static CString m_cluster_term_str;

	// This is used to compare the set of keywords
	static int CompareKeywords(const SKeywordPtr &arg1, const SKeywordPtr &arg2) {

		if(arg1.ptr->occur < arg2.ptr->occur) {
			return -1;
		}

		if(arg1.ptr->occur > arg2.ptr->occur) {
			return 1;
		}

		_int64 keyword1 = S5Byte::Value(arg1.ptr->keyword_id);
		_int64 keyword2 = S5Byte::Value(arg2.ptr->keyword_id);

		if(keyword1 < keyword2) {
			return -1;
		}

		if(keyword1 > keyword2) {
			return 1;
		}

		return 0;
	}

	// This is used to compare the set of keywords
	static int CompareKeywords(const SKeyword &arg1, const SKeyword &arg2) {

		if(arg1.occur < arg2.occur) {
			return -1;
		}

		if(arg1.occur > arg2.occur) {
			return 1;
		}

		_int64 keyword1 = S5Byte::Value(arg1.keyword_id);
		_int64 keyword2 = S5Byte::Value(arg2.keyword_id);

		if(keyword1 < keyword2) {
			return -1;
		}

		if(keyword1 > keyword2) {
			return 1;
		}

		return 0;
	}

	// This checks to see if a phrase is valid, looks for illegal characters
	static bool IsValidKeyword(char *buff, int len) {

		int char_num = 0;
		int total_char_num = 0;
		int max_word_len = 0;
		for(int i=0; i<len; i++) {
			if(buff[i] == '^') {
				max_word_len = max(max_word_len, char_num);
				char_num = 0;
				continue;
			}

			if(buff[i] >= 'a' && buff[i] <= 'z') {
				total_char_num++;
				char_num++;
			}
		}

		if(max_word_len == 0) {
			max_word_len = char_num;
		}

		if(total_char_num < 4 || max_word_len < 4) {
			return false;
		}

		return true;
	}

	// This compiles the global list of keywords
	static void CompileGlobalKeywordList(COpenConnection &conn, int list_size) {

		uChar word_length;
		cout<<"<table border=0>";
		for(int i=0; i<list_size; i++) {
			SKeyword &keyword = m_sorted_keyword_set[i];

			conn.Send((char *)&keyword.keyword_id, sizeof(S5Byte));
			conn.Receive((char *)&word_length, sizeof(uChar));
			conn.Receive(CUtility::SecondTempBuffer(), word_length);

			if(IsValidKeyword(CUtility::SecondTempBuffer(), word_length) == false) {
				continue;
			}

			CUtility::SecondTempBuffer()[word_length] = '\0';
			m_cluster_term_str += CUtility::SecondTempBuffer();
			m_cluster_term_str += "+";
			continue;

			cout<<"<tr><td height=0>";
			CUtility::SecondTempBuffer()[word_length] = '\0';
			cout<<"<font size=\"0\">";
			//cout<<keyword.keyword_id.Value()<<"&nbsp;";
			cout<<CUtility::SecondTempBuffer();
			cout<<"</font><td height=0>";

			float width = (float)keyword.occur / m_max_word_occur;
			cout<<"<div style=\"background-color:blue; width:"<<(int)(width * 500)<<"px;\">";
			cout<<"<font size=\"0\" color=\"white\">Count: "<<(float)keyword.occur<<" "<<keyword.keyword_id.Value()<<"</font></div>";
		}

		cout<<"</table>";
		conn.CloseConnection();
	}

public:

	CKeywordSet() {
	}

	// This initializes the different variables
	static void Initialize() {
		m_keyword_queue.Initialize(1024, CompareKeywords);
		m_global_keywords.Initialize(1024);
		m_keyword_set.Initialize(1024);
		m_keyword_offset.Initialize(1024);
		m_keyword_map.Initialize();
		m_keyword_offset.PushBack(0);
		m_max_word_occur = 0;
	}

	// This returns the number of keywords
	inline static int KeywordNum() {
		return m_global_keywords.Size();
	}

	// This resets the set of statistics for each keyword
	static void ResetStatistics() {

		m_max_word_occur = 0;
		for(int i=0; i<m_global_keywords.Size(); i++) {
			m_global_keywords[i].occur = 0;
		}
	}

	// This normalizes the keyword score
	static void NormalizeKeywordScore() {

		float sum = 0;
		for(int i=0; i<m_global_keywords.Size(); i++) {
			sum += m_global_keywords[i].occur;
		}

		for(int i=0; i<m_global_keywords.Size(); i++) {
			m_global_keywords[i].occur /= sum;
		}
	}

	// This sets a keyword as invalid, usually an indication that
	// the variance is too low
	inline static void SetInvalidKeyword(int keyword_id) {
		m_global_keywords[keyword_id].is_valid = false;
	}

	// This removes keywords with too high of an occurrence, flagged as domain specific
	static void FilterKeywords() {
		for(int i=0; i<m_global_keywords.Size(); i++) {
			if(m_global_keywords[i].occur > 100) {
				m_global_keywords[i].is_valid = false;
			}
		}
	}

	// This resets the keyword set for each document
	static void ResetKeywordSet() {
		m_keyword_set.Resize(0);
		m_keyword_offset.Resize(0);
		m_keyword_offset.PushBack(0);
	}

	// This adds the keywords set for each document from the keyword server
	static uLong AssignKeywordSet(SOCKET &sock, S5Byte &node) {

		static uChar occur;
		static uChar keyword_num;
		static SKeyword keyword;
		static SKeywordID key_id;
		keyword.occur = 0;
		keyword.is_valid = false;
		uLong check_sum = 0;

		COpenConnection::Receive(sock, (char *)&keyword_num, sizeof(uChar));

		for(int i=0; i<keyword_num; i++) {
			COpenConnection::Receive(sock, (char *)&key_id.keyword_id, sizeof(S5Byte));

			check_sum += key_id.keyword_id.Value();

			key_id.local_id = m_keyword_map.AddWord((char *)&key_id.keyword_id, sizeof(S5Byte));
			m_keyword_set.PushBack(key_id);

			if(m_keyword_map.AskFoundWord() == false) {
				keyword.local_id = key_id.local_id;
				keyword.keyword_id = key_id.keyword_id;
				m_global_keywords.PushBack(keyword);
			}
		}

		m_keyword_offset.PushBack(m_keyword_set.Size());

		return check_sum;
	}

	// This adds the keyword set that appears spatially
	static void AssignSpatialKeywords(SOCKET &sock) {
		
		static uChar occur;
		static uChar keyword_num;
		static SKeyword keyword;
		keyword.occur = 0;
		keyword.is_valid = false;

		COpenConnection::Receive(sock, (char *)&keyword_num, sizeof(uChar));

		for(uChar i=0; i<keyword_num; i++) {
			COpenConnection::Receive(sock, (char *)&keyword.keyword_id, sizeof(S5Byte));
			COpenConnection::Receive(sock, (char *)&occur, sizeof(uChar));

			int id = m_keyword_map.AddWord((char *)&keyword.keyword_id, sizeof(S5Byte));

			if(m_keyword_map.AskFoundWord() == false) {
				keyword.local_id = id;
				keyword.keyword_id = keyword.keyword_id;
				m_global_keywords.PushBack(keyword);
			}

			m_global_keywords[id].occur += occur;
			if(m_global_keywords[id].occur >= 3) {
				m_global_keywords[id].is_valid = true;
			}
		}

	}

	// This adds the keyword set for anchor nodes to the global set of keywords
	static void AssignAnchorNodeKeywords(SOCKET &sock) {
		
		static uChar keyword_num;
		static SKeyword keyword;
		static SKeywordID key_id;
		keyword.occur = 0;
		keyword.is_valid = false;

		COpenConnection::Receive(sock, (char *)&keyword_num, sizeof(uChar));

		for(uChar i=0; i<keyword_num; i++) {
			COpenConnection::Receive(sock, (char *)&key_id.keyword_id, sizeof(S5Byte));

			key_id.local_id = m_keyword_map.AddWord((char *)&key_id.keyword_id, sizeof(S5Byte));
			m_keyword_set.PushBack(key_id);

			if(m_keyword_map.AskFoundWord() == false) {
				keyword.local_id = key_id.local_id;
				keyword.keyword_id = key_id.keyword_id;
				m_global_keywords.PushBack(keyword);
			}

			m_global_keywords[key_id.local_id].occur += 1.0f;
			if(m_global_keywords[key_id.local_id].occur >= 3) {
				m_global_keywords[key_id.local_id].is_valid = true;
			}
		}

		m_keyword_offset.PushBack(m_keyword_set.Size());
	}

	// This returns the maximum word occurrence
	inline static float MaxWordOccur() {
		return m_max_word_occur;
	}

	// This initializes the connection ready to create the excerpt keywords
	static void InitializeExcerptKeywordSet() {

		CNameServer::TextStringServerInst(m_conn);
	
		int match_num = m_keyword_set.Size();
		static char request[20] = "WordIDRequest";

		m_conn.Send(request, 20);
		m_conn.Send((char *)&match_num, sizeof(int));
	}

	// This renders the keyword set for a given excerpt
	static void RenderExcerptKeywordSet(int doc_id) {

		uChar word_length;

		cout<<"<table border=0>";
		for(int i=m_keyword_offset[doc_id]; i<m_keyword_offset[doc_id+1]; i++) {
			S5Byte &word_id = m_keyword_set[i].keyword_id;
			m_conn.Send((char *)&word_id, sizeof(S5Byte));
			m_conn.Receive((char *)&word_length, sizeof(uChar));
			m_conn.Receive(CUtility::SecondTempBuffer(), word_length);

			cout<<"<tr><td height=0>";
			CUtility::SecondTempBuffer()[word_length] = '\0';
			cout<<"<font size=\"0\">";
			cout<<word_id.Value()<<"&nbsp;";
			cout<<CUtility::SecondTempBuffer()<<"</font><td height=0>";
			int local_id = m_keyword_map.FindWord((char *)&word_id, sizeof(S5Byte));

			if(local_id >= 0) {

				float width = (float)m_global_keywords[local_id].occur / m_max_word_occur;
				cout<<"<div style=\"background-color:blue; width:"<<(int)(width * 500)<<"px;\">";
				cout<<"<font size=\"0\" color=\"white\">Count: "<<(int)m_global_keywords[local_id].occur<<"</font></div>";	
			}
		}

		cout<<"</table>";
	}

	// This updates the global keyword score for each 
	// keyword that appers in a given excerpt
	// @param doc_id - this is the current doc id being processed
	// @param keyword_score - this is the current keyword score of the document
	// @return the checksum of the document
	static uLong UpdateGlobalKeywordOccur(int doc_id, float keyword_score) {

		uLong check_sum = 0;
		for(int i=m_keyword_offset[doc_id]; i<m_keyword_offset[doc_id+1]; i++) {
			int local_id = m_keyword_set[i].local_id;
			m_global_keywords[local_id].occur += keyword_score;
			m_max_word_occur = max(m_max_word_occur, m_global_keywords[local_id].occur);
			check_sum += m_keyword_set[i].keyword_id.Value();
		}

		return check_sum;
	}

	static void se(int doc_id) {

		for(int i=m_keyword_offset[doc_id]; i<m_keyword_offset[doc_id+1]; i++) {
			cout<<m_keyword_set[i].keyword_id.Value()<<"<>"<<m_global_keywords[m_keyword_set[i].local_id].occur<<"   ";
		}
	}

	// This calculates the keyword score of a particular document
	// @param doc_id - this is the current doc id being processed
	static float CalculateOccurKeywordScore(int doc_id, int cap = -1) {

		float avg = 0;
		for(int i=m_keyword_offset[doc_id]; i<m_keyword_offset[doc_id+1]; i++) {
			int local_id = m_keyword_set[i].local_id;
			if(m_global_keywords[local_id].is_valid == true) {

				if(cap < 0) {
					avg += m_global_keywords[local_id].occur;
				} else {
					avg += min(cap, m_global_keywords[local_id].occur);
				}
			} 
		}

		return avg;
	}

	// This returns the bounds for a given document
	inline static SBoundary DocumentBound(int doc_id) {
		SBoundary bound(m_keyword_offset[doc_id], m_keyword_offset[doc_id+1]);
		return bound;
	}

	// This returns the local id for a given keyword
	inline static int KeywordID(int offset) {
		return m_keyword_set[offset].local_id;
	}

	// This updates the keyword set for a particular excerpt. This is used to detect 
	// duplicate excerpts which need to be removed from the set.
	// @param buff - this is the set of previous rank stamps for each keyword in a given excerpt
	// @param doc_id1, doc_id2 - the first and second doc id
	static int IsDuplicateExcerpt(CMemoryChunk<int> &buff, int doc_id1, int doc_id2) {

		bool is_dup = true;
		for(int i=m_keyword_offset[doc_id1]; i<m_keyword_offset[doc_id1+1]; i++) {
			int keyword_id = m_keyword_set[i].keyword_id.Value() % buff.OverflowSize();
			buff[keyword_id] = doc_id1;
		}

		for(int i=m_keyword_offset[doc_id2]; i<m_keyword_offset[doc_id2+1]; i++) {
			int keyword_id = m_keyword_set[i].keyword_id.Value() % buff.OverflowSize();

			if(buff[keyword_id] != doc_id1) {
				is_dup = false;
			}
		}

		return is_dup;
	}

	// This returns the number of terms in common
	// @param doc_id1 - this is the first doc id
	// @param doc_id2 - this is the second doc id
	static int TermsInCommon(CMemoryChunk<int> &buff, int doc_id1, int doc_id2) {

		for(int i=m_keyword_offset[doc_id1]; i<m_keyword_offset[doc_id1+1]; i++) {
			int keyword_id = m_keyword_set[i].keyword_id.Value() % buff.OverflowSize();
			buff[keyword_id] = doc_id1;
		}

		int count = 0;
		for(int i=m_keyword_offset[doc_id2]; i<m_keyword_offset[doc_id2+1]; i++) {
			int keyword_id = m_keyword_set[i].keyword_id.Value() % buff.OverflowSize();
			if(buff[keyword_id] == doc_id1) {
				count++;
			}
		}

		return count;
	}

	// This displays the top N keyword ids for a given excerpt
	// @param doc_id - this is the docu id being retrieved
	// @param term_num - this is the maximum number of top terms to choose
	static void DisplayTopNKeywords(int doc_id, int term_num) {

		static SKeyword keyword;
		static CArray<SKeyword> buff(term_num);
		m_keyword_queue.Initialize(term_num, CompareKeywords);

		for(int i=m_keyword_offset[doc_id]; i<m_keyword_offset[doc_id+1]; i++) {
			int keyword_id = m_keyword_set[i].local_id;
			keyword.occur = m_global_keywords[keyword_id].occur;
			keyword.keyword_id = m_keyword_set[i].keyword_id;
			m_keyword_queue.AddItem(keyword);
		}

		m_keyword_queue.CopyNodesToBuffer(buff);

		while(m_keyword_queue.Size() > 0) {
			SKeyword keyword = m_keyword_queue.PopItem();
			cout<<"'"<<keyword.keyword_id.Value()<<"'";
			if(m_keyword_queue.Size() > 0) {
				cout<<", ";
			}
		}
	}

	// This lists the top N keywords as part of the excerpt set
	static void RenderGlobalKeywordList(int keyword_num) {
		
		int set_num = min(keyword_num, m_global_keywords.Size());
		for(int i=0; i<set_num; i++) {
			SKeyword &keyword = m_global_keywords[i];
			cout<<"'"<<keyword.keyword_id.Value()<<"'";
			if(i < set_num - 1) {
				cout<<", ";
			}
		}
	}

	// This lists the top N keywords as part of the excerpt set
	static void RenderGlobalKeywordListOccur(int keyword_num) {
		
		int set_num = min(keyword_num, m_global_keywords.Size());
		for(int i=0; i<set_num; i++) {
			SKeyword &keyword = m_global_keywords[i];
			cout<<(int)keyword.occur;
			if(i < set_num - 1) {
				cout<<", ";
			}
		}
	}

	// This lists the top N cluster keywords 
	static void RenderClusterKeywordList() {
		cout<<m_cluster_term_str.Buffer();
	}

	// This adds an arbitrary keyword id to be displayed as one 
	// of the additional keywords
	static void AddAdditionalDisplayKeyword(S5Byte &keyword_id) {
		m_global_keywords.ExtendSize(1);
		m_global_keywords.LastElement().keyword_id = keyword_id;
		m_global_keywords.LastElement().occur = 1000;
	}

	// This assigns the keyword score by rank
	// @param keyword_buff - this stores the top ranking keywords
	static void AssignKeywordScoreByRank(CArray<SKeyword> &keyword_buff) {

		CMemoryChunk<SKeywordPtr> buff(m_global_keywords.Size());	
		for(int i=0; i<m_global_keywords.Size(); i++) {
			buff[i].ptr = &m_global_keywords[i];
		}

		CSort<SKeywordPtr> sort(buff.OverflowSize(), CompareKeywords);
		sort.HybridSort(buff.Buffer());

		for(int i=0; i<buff.OverflowSize(); i++) {
			buff[i].ptr->rank = i;
		}

		for(int i=0; i<buff.OverflowSize(); i++) {

			if(buff[i].ptr->occur < 2) {
				break;
			}

			if(keyword_buff.Size() >= keyword_buff.OverflowSize()) {
				break;
			}

			if(buff[i].ptr->is_valid == true) {
				keyword_buff.PushBack(*buff[i].ptr);
			}
		}
	}

	// This calculates the keyword score of a particular document
	// @param doc_id - this is the current doc id being processed
	static float CalculateRankKeywordScore(int doc_id) {

		uLong score = 0;
		int rank = min(98, m_global_keywords.OverflowSize());

		static CMemoryChunk<uChar> occur(3, 0);
		occur.InitializeMemoryChunk(0);

		for(int i=m_keyword_offset[doc_id]; i<m_keyword_offset[doc_id+1]; i++) {
			int local_id = m_keyword_set[i].local_id;
			int div = rank - m_global_keywords[local_id].rank;

			if(div < 0 || m_global_keywords[local_id].is_valid == false) {
				continue;
			}

			div /= 33;
			occur[div]++;
		}

		for(int i=0; i<occur.OverflowSize(); i++) {
			score |= min(16, occur[i]) << (i * 4);
		}

		return score;
	}

	// This sorts the keywords by occurrence
	static void SortKeywordsByOccurrence() {

		m_sorted_keyword_set.MakeArrayListEqualTo(m_global_keywords);
		CSort<SKeyword> sort(m_sorted_keyword_set.Size(), CompareKeywords);
		sort.HybridSort(m_sorted_keyword_set.Buffer());
	}

	// This renders the keyword set with the histogram
	static void RenderKeywordHistogram() {

		COpenConnection conn;
		CNameServer::TextStringServerInst(conn);
	
		int match_num = min(65, m_sorted_keyword_set.Size());
		while(match_num > 0 && m_sorted_keyword_set[match_num-1].occur < 2) {
			match_num--;
		}

		static char request[20] = "WordIDRequest";

		conn.Send(request, 20);
		conn.Send((char *)&match_num, sizeof(int));

		CompileGlobalKeywordList(conn, match_num);
	}
};
CArrayList<SKeyword> CKeywordSet::m_sorted_keyword_set;
float CKeywordSet::m_max_word_occur;
CHashDictionary<int> CKeywordSet::m_keyword_map;
CLimitedPQ<SKeyword> CKeywordSet::m_keyword_queue;
CArrayList<CKeywordSet::SKeywordID> CKeywordSet::m_keyword_set;
CArrayList<int> CKeywordSet::m_keyword_offset;
CArrayList<SKeyword> CKeywordSet::m_global_keywords;
COpenConnection CKeywordSet::m_conn;
CString CKeywordSet::m_cluster_term_str;
