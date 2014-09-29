#include "./MapAssociationToTextString.h"

// This is used to initiate the different components that create
// the keyword link set. This includes creating excerpts and creating
// a set of keywords for each document.
class CKeywordLinkSetControl : public CNodeStat {

	// This is used to spawn individual clients
	CProcessSet m_process_set;
	// This creates the maps word occurrence to each of the sets
	CCreateWordOccurrenceMap m_word_occur_map;
	// This finds the association occurrence 
	CFindAssociationOccurrence m_assoc_occurr;
	// This maps the associations to the document set
	CMapAssociationsToDocument m_map_assoc;
	// This is used to map associations to their text string
	CMapAssociationToTextString m_map_assoc_to_text;

	// This culls the word set
	void CullWordSet() {

		for(int i=0; i<CNodeStat::GetClientNum(); i++) {
			CString arg("Index ");
			arg += i;
			arg += " ";
			arg += CNodeStat::GetClientNum();
			m_process_set.CreateRemoteProcess("../CullWords/Debug"
				"/CullWords.exe", arg.Buffer(), i);
		}

		m_process_set.WaitForPendingProcesses();
	}

	// This creates the association set
	void CreateAssociations() {

		for(int i=0; i<CNodeStat::GetClientNum(); i++) {
			CString arg("Index ");
			arg += i;
			arg += " ";
			arg += CNodeStat::GetClientNum();
			m_process_set.CreateRemoteProcess("../CreateAssociations/Debug"
				"/CreateAssociations.exe", arg.Buffer(), i);
		}

		m_process_set.WaitForPendingProcesses();
	}

	// This removes low ranking associations
	// @param max_assoc_num - this is the maximum number of associations
	//                      - that can be created, the rest are culled
	void FilterAssociations(int max_assoc_num) {

		int assoc_num = max_assoc_num / CNodeStat::GetHashDivNum();
		for(int j=0; j<ASSOC_SET_NUM; j++) {
			for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
				CString arg("Index ");
				arg += i;
				arg += " ";
				arg += CNodeStat::GetHashDivNum();
				arg += " ";
				arg += j + 2;
				arg += " ";
				arg += assoc_num;
				m_process_set.CreateRemoteProcess("../FilterAssociations/Debug"
					"/FilterAssociations.exe", arg.Buffer(), i);
			}
		}
		
		m_process_set.WaitForPendingProcesses();
	}

	// This creates the set of keywords
	// @param hit_list_breadth - this is the number of hit list divisions
	// @param max_keyword_size - this is the maximum number of keywords 
	//                         - allowed in the initial excerp set
	void CreateExcerptKeywords(int hit_list_breadth, int max_keyword_size) {

		for(int i=0; i<CNodeStat::GetClientNum(); i++) {
			CString arg("Index ");
			arg += i;
			arg += " ";
			arg += CNodeStat::GetClientNum();
			arg += " ";
			arg += hit_list_breadth;
			arg += " ";
			arg += max_keyword_size;
			m_process_set.CreateRemoteProcess("../CreateExcerptKeywords/Debug"
				"/CreateExcerptKeywords.exe", arg.Buffer(), i);
		}

		m_process_set.WaitForPendingProcesses();
	}

public:

	CKeywordLinkSetControl() {
		CHDFSFile::Initialize();
	}

	// This is the entry function
	// @param hit_list_breadth - this is the number of hit list divisions
	// @param max_assoc_num - this is the maximum number of associations
	//                      - that can be created, the rest are culled
	// @param max_keyword_size - this is the maximum number of keywords 
	//                         - allowed in the initial excerp set
	void CreateKeywordLinkSet(int client_id, int client_num, 
		int hash_div_num, int hit_list_breadth, int max_process_num, 
		int max_assoc_num, int max_keyword_size, const char *work_dir) {

		CNodeStat::SetClientID(client_id);
		CNodeStat::SetClientNum(client_num);
		CNodeStat::SetHashDivNum(hash_div_num);
		CNodeStat::SetInstID(0);

		m_process_set.SetPort(5555);
		m_process_set.SetMaximumClientNum(max_process_num);
		m_process_set.SetWorkingDirectory(CUtility::ExtendString
			(DFS_ROOT, "DyableDocument/DyableAssociations/DyableCommand/"));

		CMapReduce::SetMaximumProcessNum(max_process_num);

		cout<<"Creating Word Occurrence Map"<<endl;
		m_word_occur_map.CreateWordOccurrenceMap();

		cout<<"Culling Words"<<endl;
		CullWordSet();

		cout<<"Creating Associations"<<endl;
		CreateAssociations();

		cout<<"Finding Association Occurrence"<<endl;
		for(int i=0; i<ASSOC_SET_NUM; i++) {
			m_assoc_occurr.FindAssociationOccurrence(i + 2);
		}

		cout<<"Filtering Associations"<<endl;
		FilterAssociations(max_assoc_num);

		cout<<"Map Assoc To Text String"<<endl;
		m_map_assoc_to_text.MapAssociationToTextString();

		cout<<"Map Associations To Document"<<endl;
		m_map_assoc.MapAssociationsToDocument();

		cout<<"Creating Excerpt Keywords"<<endl;
		CreateExcerptKeywords(hit_list_breadth, max_keyword_size);
	}

	// This is just a test framework
	void TestCreateKeywordLinkSet(int client_id, int client_num) {

		cout<<"Testing"<<endl;

		CTestCreateAssociationsText test8;
		test8.CreateAssociations();
		test8.CheckAssociations();

		cout<<"here4"<<endl;
		CTestCreateAssociations test;
		test.LoadOccurrence();
		cout<<"here1"<<endl;
		CTestAssociationOccurrence test2;
		test2.TestAssociationOccurrence();
		cout<<"here2"<<endl;
		CTestMapAssociationToTextString test5;
		test5.TestMapAssociationToTextString();
		cout<<"here3"<<endl;
		CTestMapAssociationsToDocument test3;
		test3.TestMapAssociationsToDocument(client_id, client_num, CNodeStat::GetHashDivNum());
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);
	int client_num = atoi(argv[2]);
	int hash_div_num = atoi(argv[3]);
	int hit_list_breadth = atoi(argv[4]);
	bool is_test = atoi(argv[5]);
	int max_process_num = atoi(argv[6]);
	int max_assoc_num = atoi(argv[7]);
	int max_keyword_size = atoi(argv[8]);
	const char *work_dir = argv[9];

	CBeacon::InitializeBeacon(0, 2222);
	CMemoryElement<CKeywordLinkSetControl> command;
	command->CreateKeywordLinkSet(client_id, client_num, hash_div_num, 
		hit_list_breadth, max_process_num, max_assoc_num, max_keyword_size, work_dir);

	if(is_test == true) {
		command->TestCreateKeywordLinkSet(client_id, client_num);
	}

	command.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();

	return 0;
}
