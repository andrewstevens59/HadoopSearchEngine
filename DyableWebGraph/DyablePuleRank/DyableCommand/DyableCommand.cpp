#include "./CreateKeywordLinks.h"

// This class is responsible for coordinating the different pulse
// rank modules that are initated in parallel. The control class
// also handles incomplete processes. The procedure is to 
class CPulseRankControl : public CNodeStat {

	// This stores the number of client sets per hash division
	int m_client_set_num;
	// This defines the number of pulse rank cycles
	int m_pulse_rank_cycles;
	// This is the maximum number of documents
	// that can be joined in a limited cartesian join
	int m_max_window_size;
	// This is used to spawn individual clients
	CProcessSet m_process_set;
	// This is used to merge binary links based upon their
	// src node in sorted order
	CMergeBinaryLinks m_merge_bin_links;
	// This is used to create the set of keyword links
	CCreateKeywordLinks m_keyword_links;
	// This is used to test pulse rank
	CTestPulseRank m_test;

	// This creates a set of binary links from the
	// clustered link set
	void CreateWebGraphLinks() {

		for(int i=0; i<CNodeStat::GetClientNum(); i++) {
			CString arg("Index ");
			arg += i;

			m_process_set.CreateRemoteProcess("../CreateWebGraphLinks/Debug/"
				"CreateBinaryLinks.exe", arg.Buffer(), i);
		}

		m_process_set.WaitForPendingProcesses();
	}

	// This creates the set of keyword links
	void CreateKeywordLinks() {

		for(int i=0; i<CNodeStat::GetClientNum(); i++) {

			CString arg("Index ");
			arg += i;
			arg += " ";
			arg += m_max_window_size;

			m_process_set.CreateRemoteProcess("../CreateKeywordLinks/Debug"
				"/CreateKeywordLinks.exe", arg.Buffer(), i);
		}

		m_process_set.WaitForPendingProcesses();
	}

	// This initializes the various attributes
	// @param max_clus_link_num - this is the maximum number of links allowed in 
	//                          - in a given link cluster
	void CreateClusteredLinkSet(bool is_keyword_link_set, int max_clus_link_num) {

		if(is_keyword_link_set == false) {
			CreateWebGraphLinks();
		} else {
			m_keyword_links.CreateKeywordLinks();
			CreateKeywordLinks();
		}

		m_merge_bin_links.MergeBinaryLinks();

		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			CString arg("Index ");
			arg += i;
			arg += " ";
			arg += CNodeStat::GetHashDivNum();
			arg += " ";
			arg += is_keyword_link_set;
			arg += " ";
			arg += max_clus_link_num;

			m_process_set.CreateRemoteProcess("../CreateClusteredLinkSet/Debug/"
				"CreateClusteredLinkSet.exe", arg.Buffer(), i);
		}

		m_process_set.WaitForPendingProcesses();
	}

	// This distribute the pulse scores to neighbours
	void DistributePulseScores(bool is_keyword_set, _int64 start_bound, _int64 end_bound) {

		int curr_client = 0;
		for(int j=0; j<CNodeStat::GetHashDivNum(); j++) {
			m_client_set_num = 0;
			for(int i=0; i<CNodeStat::GetHashDivNum(); i+=CNodeStat::GetHashDivNum()) {
				CString arg("Index ");
				arg += m_client_set_num;
				arg += " ";
				arg += i;
				arg += " ";
				arg += min(i + CNodeStat::GetHashDivNum(), CNodeStat::GetHashDivNum());
				arg += " ";
				arg += j;
				arg += " ";
				arg += CNodeStat::GetHashDivNum();
				arg += " ";
				arg += curr_client;
				arg += " ";
				arg += is_keyword_set;
				arg += " ";
				arg += start_bound;
				arg += " ";
				arg += end_bound;

				m_client_set_num++;
				m_process_set.CreateRemoteProcess("../DistributePulseScores/Debug/"
					"DistributePulseScores.exe", arg.Buffer(), curr_client);

				curr_client++;
			}
		}

		m_process_set.WaitForPendingProcesses();
	}

	// This updates the back buffer
	void UpdateBackBuffer(bool is_external_pulse, bool is_keyword_set) {

		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			CString arg("Index ");
			arg += i;
			arg += " ";
			arg += CNodeStat::GetHashDivNum();
			arg += " ";
			arg += m_client_set_num;
			arg += " ";
			arg += is_external_pulse;
			arg += " ";
			arg += is_keyword_set;
                        
			m_process_set.CreateRemoteProcess("../AccumulateHashDivision/Debug/"
				"AccumulateHashDivision.exe", arg.Buffer(), i);
		}

		m_process_set.WaitForPendingProcesses();
	}

public:

	CPulseRankControl() {
		CHDFSFile::Initialize();
	}

	// This is the entry function that initiates the different modules
	// that compose PulseRank.
	// @param pulse_rank_cycles - the number of pulse rank cycles to perform
	// @param hash_div_num - this number of hash divisions to use 
	//                     - while performing pulse rank
	// @param is_keyword_set - true if the keyword link set is being used 
	//                       - false if the webgraph link set is being used
	// @param max_clus_link_num - this is the maximum number of links allowed in 
	//                          - in a given link cluster
	// @param max_window_size - this is the maximum number of documents that can
	//                        - be joined in a limited cartesian join
	void PerformPulseRank(int client_id, int client_num, int pulse_rank_cycles, 
		int hash_div_num, bool is_keyword_set, int max_clus_link_num,
		bool is_test, int max_process_num, int max_window_size, const char *work_dir) {

		m_pulse_rank_cycles = pulse_rank_cycles;
		CNodeStat::SetClientID(client_id);
		CNodeStat::SetClientNum(client_num);
		CNodeStat::SetHashDivNum(hash_div_num);
		m_process_set.SetMaximumClientNum(max_process_num);
		m_process_set.SetPort(5555);
		CMapReduce::SetMaximumProcessNum(max_process_num);
		m_max_window_size = max_window_size;

		m_process_set.SetWorkingDirectory(CUtility::ExtendString
			(DFS_ROOT, "DyableWebGraph/DyablePuleRank/DyableCommand/"));

		CNodeStat::InitializeNodeLinkStat
			("GlobalData/WordDictionary/dictionary_offset");
		CNodeStat::WriteNodeLinkStat();

		CreateClusteredLinkSet(is_keyword_set, max_clus_link_num);

		if(is_test == true) {
			m_test.LoadBackBuffer();
		}

		for(int i=0; i<m_pulse_rank_cycles; i++) {
			cout<<"Pulse Rank Cycle "<<i<<" Out Of "<<m_pulse_rank_cycles<<endl;
			DistributePulseScores(is_keyword_set, 0, CNodeStat::GetBaseNodeNum());
			cout<<"Update Back Buffer"<<endl;
			UpdateBackBuffer(false, is_keyword_set);
		}

		DistributePulseScores(is_keyword_set, CNodeStat::GetBaseNodeNum(), 
			CNodeStat::GetGlobalNodeNum());

		UpdateBackBuffer(true, is_keyword_set);
	}

	// This is a test framework 
	void TestPulseRankControl(bool is_keyword_link_set, int client_num) {

		if(is_keyword_link_set == true) {
			cout<<"Testing Keyword Links"<<endl;
			CTestKeywordLinks test1;
			test1.LoadExcerpts();
			test1.CheckKeywordLinks();
		}

		cout<<"Testing Pulse Rank"<<endl;
		m_test.TestPulseRank(is_keyword_link_set, m_pulse_rank_cycles);
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);
	int client_num = atoi(argv[2]);
	int pulse_rank_cycles = atoi(argv[3]);
	int hash_div_num = atoi(argv[4]);
	bool is_keyword_set = atoi(argv[5]);
	int max_clus_link_num = atoi(argv[6]);
	bool is_test = atoi(argv[7]);
	int max_process_num = atoi(argv[8]);
	int max_window_size = atoi(argv[9]);
	const char *work_dir = argv[10];

	CBeacon::InitializeBeacon(client_id, 2222);
	CMemoryElement<CPulseRankControl> command;

	command->PerformPulseRank(client_id, client_num, pulse_rank_cycles,
		hash_div_num, is_keyword_set, max_clus_link_num, 
		is_test, max_process_num, max_window_size, work_dir);

	if(is_test == true) {
		command->TestPulseRankControl(is_keyword_set, client_num);
	}

	command.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();

	return 0;
}
