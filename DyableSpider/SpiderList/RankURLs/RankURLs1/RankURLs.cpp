#include "../../../../MapReduce.h"

// This class takes the set of url text strings that 
// have been hashed on the domain name and finds the
// occurrence of each domain name from the base node
// set. URLs are then ranked on their domain name 
// occurrence and then by their pulse score. In 
// particular URLs with a domain name with a small
// occurrence receiver a higher ranking.
class CRankURLs : public CNodeStat {

	// This stores an instance of a ranked url
	struct SRankURL {
		// This stores the occurrence of the url
		uLong domain_occur;
		// This stores the pulse score of the url
		float pulse_score;
		// This stores the local url id
		int url_id;
		// This stores the domain name url
		int domain_id;
	};

	// This is used to sort urls
	struct SRankURLPtr {
		SRankURL *ptr;
	};

	// This is used to compare ranked urls
	static int CompareURLs(const SRankURLPtr &arg1, const SRankURLPtr &arg2) {

		SRankURL *ptr1 = arg1.ptr;
		SRankURL *ptr2 = arg2.ptr;
		if(ptr1->domain_occur < ptr2->domain_occur) {
			return 1;
		}

		if(ptr1->domain_occur > ptr2->domain_occur) {
			return -1;
		}

		if(ptr1->pulse_score < ptr2->pulse_score) {
			return -1;
		}

		if(ptr1->pulse_score > ptr2->pulse_score) {
			return 1;
		}

		return 0;
	}

	// This stores the set of base urls domain names
	CHashDictionary<int> m_domain_dict;
	// This stores the occurrence of each base domain name
	CVector<uChar> m_domain_occur;

	// This stores the set of ranked urls
	CLinkedBuffer<SRankURL> m_rank_url_buff;
	// This stores the set of non spidered urls
	CArrayList<char> m_url_buff;
	// This stores the size of each url
	CArrayList<uLong> m_url_offset;
	// This stores the maximum number of urls in the set
	int m_spider_num;

	// This stores the final spider list
	CHDFSFile m_spider_list_file;

	// This adds one of the urls to the set
	// @param pulse_map - the current pulse map for the set
	inline void AddURLToSet(const char url[], int url_length, SPulseMap &pulse_map) {

		int domain_start = 0;
		while(domain_start < url_length) {
			if(url[domain_start++] == '.') {
				break;
			}
		}

		int domain_length = domain_start;
		while(domain_length < url_length) {
			if(url[domain_length] == '/') {
				break;
			}
			domain_length++;
		}

		int id = m_domain_dict.AddWord(url, domain_length, domain_start);

		if(!m_domain_dict.AskFoundWord()) {
			m_domain_occur.PushBack(1);
		} else if(m_domain_occur[id] < 16) {
			if(pulse_map.node < CNodeStat::GetBaseNodeNum()) {
				m_domain_occur[id]++;
			}
		}

		if(pulse_map.node < CNodeStat::GetBaseNodeNum()) {
			return;
		}

		m_url_buff.CopyBufferToArrayList(url, url_length, m_url_buff.Size());
		m_url_offset.PushBack(m_url_buff.Size());

		SRankURL *ptr = m_rank_url_buff.ExtendSize(1);
		ptr->url_id = m_rank_url_buff.Size() - 1;
		ptr->pulse_score = pulse_map.pulse_score;
		ptr->domain_id = id;
	}

	// This creates the final set of ranked urls
	void CreateSpiderList(CArray<SRankURLPtr> &ptr_buff) {

		m_spider_list_file.OpenWriteFile(CUtility::ExtendString
			("GlobalData/SpiderList/fin_spider_list", GetClientID()));

		int list_num = 0;
		int offset = 0;
		while(list_num < m_spider_num && offset < ptr_buff.Size()) {
			SRankURL *ptr = ptr_buff[offset++].ptr;

			if(m_domain_occur[ptr->domain_id] < 25) {
				char *url = m_url_buff.Buffer() + m_url_offset[ptr->url_id];
				int url_length = m_url_offset[ptr->url_id+1] - m_url_offset[ptr->url_id];
				m_spider_list_file.WriteCompObject(url_length);
				m_spider_list_file.WriteCompObject(url, url_length);
				m_domain_occur[ptr->domain_id]++;
				list_num++;
			}
		}
	}

	// This ranks the set of urls 
	void SortURLs() {

		SRankURL *ptr;
		m_rank_url_buff.ResetPath();
		CArray<SRankURLPtr> ptr_buff(m_rank_url_buff.Size());
		while((ptr = m_rank_url_buff.NextNode()) != NULL) {
			ptr->domain_occur = m_domain_occur[ptr->domain_id];
			ptr_buff.ExtendSize(1);
			ptr_buff.LastElement().ptr = ptr;
		}

		cout<<"Sorting"<<endl;
		CSort<SRankURLPtr> sort(ptr_buff.Size(), CompareURLs);
		sort.HybridSort(ptr_buff.Buffer());

		CreateSpiderList(ptr_buff);
	}


public:

	CRankURLs() {
		m_domain_dict.Initialize(999999);
		m_rank_url_buff.Initialize();
		m_url_buff.Initialize();
		m_url_offset.Initialize();
		m_url_offset.PushBack(0);
		CHDFSFile::Initialize();
	}

	// This is the entry function
	// @param spider_num - this is the maximum number of urls
	//                   - to have in a given spider list
	void RankURLs(int client_id, int client_num, int spider_num) {

		CNodeStat::SetClientID(client_id);
		CNodeStat::SetClientNum(client_num);
		CNodeStat::LoadNodeLinkStat();
		m_spider_num = spider_num;

		SPulseMap pulse_map;
		int url_length;

		CHDFSFile url_set_file;
		CMemoryChunk<char> url_buff(4096);
		for(int i=0; i<CNodeStat::GetClientNum(); i++) {
			url_set_file.OpenReadFile(CUtility::ExtendString
				("LocalData/hash_url_set", i, ".set", CNodeStat::GetClientID()));

			while(pulse_map.ReadPulseMap(url_set_file)) {
				url_set_file.ReadCompObject(url_length);
				url_set_file.ReadCompObject(url_buff.Buffer(), url_length);
				AddURLToSet(url_buff.Buffer(), url_length, pulse_map);
			}
		}

		SortURLs();
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);
	int client_num = atoi(argv[2]);
	int spider_num = atoi(argv[3]);

	CBeacon::InitializeBeacon(client_id);
	CMemoryElement<CRankURLs> rank;
	rank->RankURLs(client_id, client_num, spider_num);
	rank.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();
}