#include "../../../MapReduce.h"

// This creates the mapping between non spidered doc id's and the
// the text string url. This is used to map the final set of doc
// id's to their corresponding urls.
class CCreateURLTextMap : public CNodeStat {

	// This stores the hash boundary for which this client is responsible
	// that is which hash divisions this client must access 
	SBoundary m_client_bound;
	// stores the global dictionary offset for each of the log types
	CArray<SGlobalIndexOffset> m_dict_offset;

	// This creates a set of doc id/text string mappings. This is
	// done by loading each of the precompiled dictionaries and
	// associating a doc id with a url string.
	// @param map_file - this stores the mapping between a word id
	//                 - and the corresponding text string
	void CreateURLTextMap(CSegFile &map_file) {

		_int64 doc_id;
		int length;

		CHashDictionary<int> dict;
		for(int i=m_client_bound.start; i<m_client_bound.end; i++) {
			SGlobalIndexOffset &link_offset = m_dict_offset[i];
			dict.ReadHashDictionaryFromFile((CUtility::ExtendString
				("GlobalData/URLDictionary/url_dictionary", i)));

			cout<<"Loading Set "<<(i - m_client_bound.start)<<" Out Of "
				<<m_client_bound.Width()<<endl;

			int offset = 0;
			for(int j=link_offset.base_url_size; j<dict.Size(); j++) {
				doc_id = link_offset.link_offset + offset++;

				char *url = dict.GetWord(j, length);
				map_file.AskBufferOverflow(sizeof(S5Byte) + sizeof(int) + length);
				map_file.WriteCompObject5Byte(doc_id);
				map_file.WriteCompObject(length);
				map_file.WriteCompObject(url, length);
			}
		}
	}

	// This is called to initialize the various variables needed
	// to perform the mapping this includes the dictionary offsets
	void Initialize() {

		m_client_bound.Set(0, CNodeStat::GetHashDivNum());
		CHashFunction::BoundaryPartion(GetClientID(), GetClientNum(), 
			m_client_bound.end, m_client_bound.start);
		
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
	}

public:

	CCreateURLTextMap() {
	}

	// This creates the url text map for this client
	void CreateURLTextMap() {

		Initialize();

		CSegFile map_file;
		map_file.OpenWriteFile(CUtility::ExtendString
			("LocalData/url_map", GetClientID()));

		CreateURLTextMap(map_file);
	}

};

// This takes the set of pulse nodes and culls them to produce
// a reduced set of nodes. This reduced set represents the set
// of urls to spider.
class CCullPulseNodes : public CNodeStat {

	// This creates the url text map
	CCreateURLTextMap m_url_text_map;
	// This stores the final pulse node set
	CSegFile m_pulse_node_file;
	// This stores the set of pulse maps
	CSegFile m_pulse_map_file;

public:

	CCullPulseNodes() {
		CHDFSFile::Initialize();
	}

	// This is the entry function
	void CullPulseNodes(int client_id, int client_num, int log_div_size) {
		CNodeStat::SetClientID(client_id);
		CNodeStat::SetClientNum(client_num);
		CNodeStat::LoadNodeLinkStat();
		CNodeStat::SetHashDivNum(log_div_size);

		m_url_text_map.CreateURLTextMap();

		CHDFSFile pulse_rank_file;
		pulse_rank_file.OpenReadFile(CUtility::ExtendString
			(WEBGRAPH_PULSE_SCORE_DIR, GetClientID()));
		
		m_pulse_node_file.OpenWriteFile(CUtility::ExtendString
			("LocalData/pulse_node_set", GetClientID()));

		m_pulse_map_file.OpenWriteFile(CUtility::ExtendString
			("LocalData/pulse_map_set", GetClientID()));

		SPulseMap pulse_map;
		while(pulse_map.ReadPulseMap(pulse_rank_file)) {
			if(pulse_map.node >= CNodeStat::GetBaseNodeNum()) {
				m_pulse_node_file.WriteCompObject(pulse_map.node);
				pulse_map.WritePulseMap(m_pulse_map_file);
			}
		}
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);
	int client_num = atoi(argv[2]);
	int log_div_size = atoi(argv[3]);

	CBeacon::InitializeBeacon(client_id);
	CMemoryElement<CCullPulseNodes> spider;
	spider->CullPulseNodes(client_id, client_num, log_div_size);
	spider.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();
}