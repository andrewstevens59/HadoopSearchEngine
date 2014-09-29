#include "./FindGroupedTermOccurrence.h"

// This takes the set of excerpt keywords for each document and 
// iteratively reduces the set size by joining terms together.
// Grouped terms are selected based upon their global occurrence
// in the set. Grouped terms will a small occurrence are culled
// from the set. For each set of grouped terms a new keyword is 
// created with its own id. This procedure is applied multiple
// times until the excerpt keyword set for a document is below
// some finite size at which point it can be removed from the set.
class CExcerptKeywordsControl : public CNodeStat {

	// This is used to spawn individual clients
	CProcessSet m_process_set;
	// Ths finds the occurrence of all of the groupd terms
	CFindGroupedTermOccurrence m_group_term_occur;
	// This is used to map grouped terms to the excerpt documet set
	CMapGroupedTerms m_map_group_terms;
	// This stores the number of new keywords that are added to a set
	CMemoryChunk<_int64> m_new_keyword_num;
	// This stores the set of grouped terms
	CHDFSFile m_group_term_file;

	// This creates the association set
	// @param level - this is the current level in the hiearchy being processed
	// @param scan_window - this stores the size of the scan window when 
	//                    - joining terms
	void CreateGroupedTerms(int level, int scan_window) {


		for(int i=0; i<CNodeStat::GetClientNum(); i++) {

			CString arg("Index ");
			arg += i;
			arg += " ";
			arg += level;
			arg += " ";
			arg += scan_window;

			m_process_set.CreateRemoteProcess("../GroupTerms/Debug"
				"/GroupTerms.exe", arg.Buffer(), i);
		}

		m_process_set.WaitForPendingProcesses();
	}

	// This culls the set of grouped terms based upon their occurrence
	// and assigns a unique term id to all of the grouped terms
	// @param max_group_set_num - this is the maximum number of grouped 
	//                          - terms allowed in a given cycle
	// @param level - this is the current level in the hiearchy
	void CullGroupedTerms(int max_group_set_num, int level) {

		int set_num = max_group_set_num / CNodeStat::GetHashDivNum();
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			CString arg("Index ");
			arg += i;
			arg += " ";
			arg += CNodeStat::GetHashDivNum();
			arg += " ";
			arg += set_num;
			arg += " ";
			arg += level;

			m_process_set.CreateRemoteProcess("../CullGroupedSets/Debug"
				"/CullGroupedSets.exe", arg.Buffer(), i);
		}

		m_process_set.WaitForPendingProcesses();
	}

	// This reduces the excerpt keyword set for each document
	// @param init_keyword_size - this is the maximum number of terms
	//                          - allowed in a given grouped set before
	//                          - the final keyword set is compiled
	// @param level - this is the current level in the grouped hiearchy
	// @param scan_window - this stores the size of the scan window when 
	//                    - joining terms
	// @return false if no new keywords were added, true otherwise
	bool ReduceKeywordSet(int init_keyword_size, int level, int scan_window) {

		for(int i=0; i<CNodeStat::GetClientNum(); i++) {

			CString arg("Index ");
			arg += i;
			arg += " ";
			arg += init_keyword_size;
			arg += " ";
			arg += level;
			arg += " ";
			arg += scan_window;

			m_process_set.CreateRemoteProcess("../ReduceExcerptKeywordSet/Debug"
				"/ReduceExcerptKeywordSet.exe", arg.Buffer(), i);
		}

		m_process_set.WaitForPendingProcesses();

		_int64 total_num = 0;
		CHDFSFile new_keyword_num_file;
		for(int i=0; i<CNodeStat::GetClientNum(); i++) {
			new_keyword_num_file.OpenReadFile(CUtility::ExtendString
				("LocalData/keyword_added", i));
			new_keyword_num_file.ReadObject(m_new_keyword_num[i]);
			total_num += m_new_keyword_num[i];
		}

		return total_num > 100;
	}

	// This creates a local copy of the keyword number set
	void CreateKeywordNumSet() {

		_int64 id;
		CHDFSFile assoc_term_id_file;
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			assoc_term_id_file.OpenReadFile(CUtility::ExtendString
				("GlobalData/Keywords/assoc_num", i));
			assoc_term_id_file.ReadObject(id);

			assoc_term_id_file.OpenWriteFile(CUtility::ExtendString
				("LocalData/assoc_num", i));
			assoc_term_id_file.WriteObject(id);
		}
	}

	// This creates the final keyword set
	// @param max_keyword_size - this is the maximum number of keywords
	//                         - allowed in a given set
	void CreateFinalKeywordSet(int hit_list_breadth, int level_num, int max_keyword_size) {

		for(int i=0; i<CNodeStat::GetClientNum(); i++) {

			CString arg("Index ");
			arg += i;
			arg += " ";
			arg += hit_list_breadth;
			arg += " ";
			arg += level_num;
			arg += " ";
			arg += max_keyword_size;

			m_process_set.CreateRemoteProcess("../FinalExcerptKeywordSet/Debug"
				"/FinalExcerptKeywordSet.exe", arg.Buffer(), i);
		}

		m_process_set.WaitForPendingProcesses();
	}

	// This stores the culled grouped terms for later lookup
	void StoreGroupedTerms(int level) {

		S5Byte curr_id;
		uLong occur;
		CMemoryChunk<S5Byte> group_set(2);
		CHDFSFile fin_group_term_file;
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			fin_group_term_file.OpenReadFile(CUtility::ExtendString
			("GlobalData/Keywords/fin_group_term_set", level, ".set", i));

			while(fin_group_term_file.ReadCompObject(group_set.Buffer(), 2)) {
				fin_group_term_file.ReadCompObject(curr_id);
				fin_group_term_file.ReadCompObject(occur);

				m_group_term_file.WriteCompObject(group_set.Buffer(), 2);
				m_group_term_file.WriteCompObject(curr_id);
			}
		}
	}

public:

	CExcerptKeywordsControl() {
		CHDFSFile::Initialize();
	}

	// This is the entry function
	// @param init_keyword_size - this is the maximum number of terms
	//                          - allowed in a given grouped set before
	//                          - the final keyword set is compiled
	// @param max_keyword_size - this is the maximum number of keywords
	//                         - allowed in a given set
	// @param max_group_set_num - this is the maximum number of grouped 
	//                          - terms allowed in a given cycle
	// @param group_cycle_num - this is the number of keyword merging cycles
	//                        - in order to create new keywords
	// @param scan_window - this stores the size of the scan window when 
	//                    - joining terms
	void ExcerptKeywordsControl(int client_num, int hash_div_num, int hit_list_breadth,
		int max_process_num, int init_keyword_size, int max_keyword_size, int max_group_set_num,
		int group_cycle_num, bool is_test, int scan_window, const char *work_dir) {

		CNodeStat::SetClientNum(client_num);
		CNodeStat::SetHashDivNum(hash_div_num);
		CNodeStat::SetInstID(0);

		m_process_set.SetPort(5555);
		m_process_set.SetMaximumClientNum(max_process_num);
		CMapReduce::SetMaximumProcessNum(max_process_num);
		m_new_keyword_num.AllocateMemory(client_num, 9999);

		m_process_set.SetWorkingDirectory(CUtility::ExtendString
			(DFS_ROOT, "DyableDocument/DyableExcerptKeywords/DyableCommand/"));

		if(is_test == true) {
			m_group_term_file.OpenWriteFile("GlobalData/Keywords/cull_group_set");
		}

		CreateKeywordNumSet();

		int level_num = 0;
		for(int i=0; i<group_cycle_num; i++) {
			CreateGroupedTerms(i, scan_window);
			m_group_term_occur.FindGroupedTermOccurrence();

			CullGroupedTerms(max_group_set_num, i + 1);

			if(is_test == true) {
				StoreGroupedTerms(i + 1);
			}

			m_map_group_terms.MapGroupedTerms(i);

			level_num++;
			if(ReduceKeywordSet(init_keyword_size, i, scan_window) == false) {
				break;
			}

			for(int i=0; i<m_new_keyword_num.OverflowSize(); i++) {
				cout<<m_new_keyword_num[i]<<" ";
			}
			cout<<endl;
		}

		CreateFinalKeywordSet(hit_list_breadth, level_num, max_keyword_size);
	}

	// This is just a test framework
	void TestExcerptKeywordsControl(int group_cycle_num) {

		CTestMapGroupedTermsToDocument test1;
		test1.TestMapGroupedTermsToDocument(CNodeStat::GetHashDivNum(), group_cycle_num);

		CTestFindGroupedTermOccurrence test3;
			test3.TestFindGroupedTermOccurrence();
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_num = atoi(argv[1]);
	int hash_div_num = atoi(argv[2]);
	int hit_list_breadth = atoi(argv[3]);
	bool is_test = atoi(argv[4]);
	int max_process_num = atoi(argv[5]);
	int init_keyword_size = atoi(argv[6]);
	int max_keyword_size = atoi(argv[7]);
	int group_cycle_num = atoi(argv[8]);
	int scan_window = atoi(argv[9]);
	int max_group_set_num = atoi(argv[10]);
	const char *work_dir = argv[11];

	CBeacon::InitializeBeacon(0, 2222);
	CMemoryElement<CExcerptKeywordsControl> command;
	command->ExcerptKeywordsControl(client_num, hash_div_num, 
		hit_list_breadth, max_process_num, init_keyword_size, max_keyword_size, 
		max_group_set_num, group_cycle_num, is_test, scan_window, work_dir);

	if(is_test == true) {
		command->TestExcerptKeywordsControl(group_cycle_num);
	}

	command.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();

	return 0;
}
