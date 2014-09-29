#include "../../../MapReduce.h"

// This takes the set of mapped url text strings and
// extracts the domain name from the url. Each url is
// then hashed on its domain name so the occurrence 
// of each domain can be found.
class CDistributeDomainName : public CNodeStat {

	// This stores the distributed urls
	CFileSet<CHDFSFile> m_url_set;
	// This stores the mapped url set
	CSegFile m_fin_spider_file;
	// This stores the pulse map
	CSegFile m_pulse_map_file;

	// This stores the hash boundary for which this client is responsible
	// that is which hash divisions this client must access 
	SBoundary m_client_bound;
	// stores the global dictionary offset for each of the log types
	CArray<SGlobalIndexOffset> m_dict_offset;

	// This is called to initialize the various variables needed
	// to perform the mapping this includes the dictionary offsets
	void Initialize() {
		
		_int64 dummy1;
		uLong dummy2;

		CHDFSFile file;
		file.OpenReadFile("GlobalData/WordDictionary/dictionary_offset");
		// writes the number of base nodes
		file.ReadCompObject(dummy1);
		// writes the total number of nodes
		file.ReadCompObject(dummy1);
		file.ReadCompObject(dummy2);
		m_dict_offset.ReadArrayFromFile(file);

		m_client_bound.Set(0, m_dict_offset.Size() - 1);
		CHashFunction::BoundaryPartion(GetClientID(), GetClientNum(), 
			m_client_bound.end, m_client_bound.start);
	}

	// This returns the hash of a urls domain
	inline uLong DomainNameHash(const char url[], int url_length) {

		uLong hash = 0;
		int domain_length = 0;

		while(domain_length < url_length) {
			if(url[domain_length++] == '.') {
				break;
			}
		}


		while(domain_length < url_length) {
			if(url[domain_length] == '/') {
				break;
			}
			hash += abs(url[domain_length++]);
		}

		return hash;
	}

	// This distributes a url based upon its domain name
	// @param pulse_map - this is the current pulse map being processed
	void HashURLOnDomain(const char url[], int url_length, SPulseMap &pulse_map) {
		
		if(CUtility::FindSubFragment(url, "blog", url_length) >= 0) {
			return;
		}

		for(int i=0; i<url_length; i++) {
			if(url[i] == '#' || url[i] == '?') {
				return;
			}
		}

		uLong hash = DomainNameHash(url, url_length);
		CHDFSFile &hash_file = m_url_set.File(hash % m_url_set.SetNum());
		pulse_map.WritePulseMap(hash_file);
		hash_file.WriteCompObject(url_length);
		hash_file.WriteCompObject(url, url_length);
	}

	// This distributes the base url set
	void DistributeBaseURLSet() {

		int url_length;
		SPulseMap pulse_map;
		pulse_map.node = 0;

		CHashDictionary<int> dict;
		for(int i=m_client_bound.start; i<m_client_bound.end; i++) {
			SGlobalIndexOffset &link_offset = m_dict_offset[i];
			dict.ReadHashDictionaryFromFile((CUtility::ExtendString
				("GlobalData/URLDictionary/url_dictionary", i)));

			cout<<"Loading Set "<<(i - m_client_bound.start)<<" Out Of "
				<<m_client_bound.Width()<<endl;

			for(int j=0; j<link_offset.base_url_size; j++) {

				char *url = dict.GetWord(j, url_length);
				if(CUtility::FindFragment(url, "ext", 3, 0)) {
					continue;
				}

				uLong hash = DomainNameHash(url, url_length);
				CHDFSFile &hash_file = m_url_set.File(hash % m_url_set.SetNum());
				pulse_map.WritePulseMap(hash_file);
				hash_file.WriteCompObject(url_length);
				hash_file.WriteCompObject(url, url_length);
			}
		}
	}

public:

	CDistributeDomainName() {
		CHDFSFile::Initialize();
	}

	// This is the entry function
	// @param spider_set_num -  this is the number of spider sets to create
	void DistributeDomainName(int client_id, int client_num, int spider_set_num) {

		CNodeStat::SetClientID(client_id);
		CNodeStat::SetClientNum(client_num);
		Initialize();

		m_fin_spider_file.OpenReadFile(CUtility::ExtendString
			("LocalData/fin_spider_list", GetClientID()));

		m_pulse_map_file.OpenReadFile(CUtility::ExtendString
			("LocalData/pulse_map_set", GetClientID()));

		m_url_set.OpenWriteFileSet("LocalData/hash_url_set", 
			spider_set_num, GetClientID());

		S5Byte node;
		int length;
		SPulseMap pulse_map;
		CMemoryChunk<char> url_buff(4096);

		DistributeBaseURLSet();

		while(m_fin_spider_file.ReadCompObject(node)) {
			m_fin_spider_file.ReadCompObject(length);
			m_fin_spider_file.ReadCompObject(url_buff.Buffer(), length);

			pulse_map.ReadPulseMap(m_pulse_map_file);

			if(pulse_map.node != node) {
				cout<<"url mismatch";getchar();
			}

			HashURLOnDomain(url_buff.Buffer(), length, pulse_map);
		}
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);
	int client_num = atoi(argv[2]);
	int spider_set_num = atoi(argv[3]);

	CBeacon::InitializeBeacon(client_id);
	CMemoryElement<CDistributeDomainName> name;
	name->DistributeDomainName(client_id, client_num, spider_set_num);
	name.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();
}