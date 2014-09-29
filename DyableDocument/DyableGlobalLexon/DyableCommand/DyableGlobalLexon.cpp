#include "./CreateAssociationMapSet.h"

// This class creates a global lexon which can be used to retrieve
// word ids for all word text strings, which can be looked up 
// efficiently during retrieval. To do this a word hash is created
// for every word text string. Along with this hash the word id is
// stored. Every word id is then sorted based upon it's hash. The
// word text string is then mapped to the sorted word ids and stored
// in a final index. A usual index lookup is created for each word
// id to be used for fast access.
class CGlobalLexon : public CNodeStat {

	// This is used to create the association map set
	CCreateAssociationMapSet m_assoc_map_set;
	// This is used to create the reverse word id lookup
	CCreateWordIDLookup m_create_word_id_lookup;


	// This creates the set of word hash values
	void CreateWordHashSet(int log_div_size) {
		
		// CNodeStat::GetHashDivNum() + log_div_size to include the set of singular terms
		for(int i=0; i<CNodeStat::GetHashDivNum() + log_div_size; i++) {

			CString arg("Index ");
			arg += i;
			arg += " ";
			arg += CNodeStat::GetHashDivNum();

			CCreateWordIDLookup::ProcessSet().CreateRemoteProcess("../CreateWordHashSet/Debug"
				"/CreateWordHashSet.exe", arg.Buffer(), i);
		}

		CCreateWordIDLookup::ProcessSet().WaitForPendingProcesses();
	}

	// This creates the set of sorted hash values 
	void SortWordHashSet(int log_div_size) {
		
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {

			CString arg("Index ");
			arg += i;
			arg += " ";
			arg += CNodeStat::GetHashDivNum() + log_div_size;

			CCreateWordIDLookup::ProcessSet().CreateRemoteProcess("../SortWordHashSet/Debug"
				"/SortWordHashSet.exe", arg.Buffer(), i);
		}

		CCreateWordIDLookup::ProcessSet().WaitForPendingProcesses();
	}

	// This creates the word hash lookup
	void CreateWordHashLookup() {

		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {

			CString arg("Index ");
			arg += i;

			CCreateWordIDLookup::ProcessSet().CreateRemoteProcess("../CreateWordHashLookup/Debug"
				"/CreateWordHashLookup.exe", arg.Buffer(), i);
		}

		CCreateWordIDLookup::ProcessSet().WaitForPendingProcesses();
	}

public:

	CGlobalLexon() {
		CHDFSFile::Initialize();
	}

	// This is the entry function that creates the global lexon
	void CreateGloblaLexon(int max_process_num, int hash_div_num, 
		int log_div_size, const char *work_dir) {

		CNodeStat::SetHashDivNum(hash_div_num);
		CMapReduce::SetMaximumProcessNum(max_process_num);

		CCreateWordIDLookup::ProcessSet().SetWorkingDirectory(CUtility::ExtendString(DFS_ROOT, 
			"DyableDocument/DyableGlobalLexon/DyableCommand/"));

		CreateWordHashSet(log_div_size);
		SortWordHashSet(log_div_size);

		m_assoc_map_set.CreateAssociationMapSet(log_div_size);
		m_create_word_id_lookup.CreateWordIDLookup();

		CMapReduce::ExternalHashMap("RetrieveAssociationMap", "LocalData/word_id", 
			ASSOC_MAP_FILE, "LocalData/word_map", "LocalData/map_assoc",
			sizeof(S5Byte), 80);

		CMapReduce::ExternalHashMap("CreateSimilarTermMap", "LocalData/word_id", 
			"LocalData/assoc_map_file", "LocalData/assoc_set_mapped", "LocalData/map_assoc",
			sizeof(S5Byte), 1024);

		CreateWordHashLookup();
	}

	// This is a test framework
	void TestGlobalLexon() {
		cout<<"Testing"<<endl;
		CTestGlobalLexon test1;
		test1.TestMapExcerptKeywords();
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	bool is_test = atoi(argv[1]);
	int max_process_num = atoi(argv[2]);
	int hash_div_num = atoi(argv[3]);
	int log_div_size = atoi(argv[4]);
	const char *work_dir = argv[5];
	
	CBeacon::InitializeBeacon(0, 2222);
	CMemoryElement<CGlobalLexon> map;
	map->CreateGloblaLexon(max_process_num, hash_div_num, log_div_size, work_dir);

	if(is_test == true) {
		map->TestGlobalLexon();
	}

	map.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();

	return 0;
}