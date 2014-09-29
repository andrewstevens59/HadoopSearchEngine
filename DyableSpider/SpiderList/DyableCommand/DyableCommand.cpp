#include "./TestSpiderMapping.h"

// This is used to create the spiderlist which is used to
// rebuild the document set.
class CSpiderListControl : public CNodeStat {

	// This stores the process set 
	CProcessSet m_process_set;
	// This stores the final pulse node set
	CSegFile m_pulse_node_file;
	// This stores the reduced set of pulse nodes
	CSegFile m_fin_spider_file;
	// This stores the url text map file
	CSegFile m_url_map_file;

	// This creates the set of urls
	// @param log_div_size - this is the number of dictionary log divisions
	void CreateURLMap(int log_div_size) {

		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			CString command("Index ");
			command += i;
			command += " ";
			command += CNodeStat::GetHashDivNum();
			command += " ";
			command += log_div_size;

			m_process_set.CreateRemoteProcess("../CullPulseNodes/Debug"
				"/CullPulseNodes.exe", command.Buffer(), i);
		}

		m_process_set.WaitForPendingProcesses();
	}

	// This distributes the urls based upon their domain name
	// @param spider_set_num - this is the number of individual spider sets to create
	void DistributeURLs(int spider_set_num) {

		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			CString command("Index ");
			command += i;
			command += " ";
			command += CNodeStat::GetHashDivNum();
			command += " ";
			command += spider_set_num;

			m_process_set.CreateRemoteProcess("../DistributeDomainName/Debug"
				"/DistributeDomainName.exe", command.Buffer(), i);
		}

		m_process_set.WaitForPendingProcesses();
	}

	// This creates the final spider list
	// @param max_spider_num - the maximum size of the spider list
	// @param spider_set_num - this is the number of individual spider sets to create
	void CreateSpiderList(int max_spider_num, int spider_set_num) {

		int indv_size = max_spider_num / spider_set_num;
		for(int i=0; i<spider_set_num; i++) {
			CString command("Index ");
			command += i;
			command += " ";
			command += CNodeStat::GetHashDivNum();
			command += " ";
			command += indv_size;

			m_process_set.CreateRemoteProcess("../RankURLs/Debug"
				"/RankURLs.exe", command.Buffer(), i);
		}

		m_process_set.WaitForPendingProcesses();
	}

public:

	CSpiderListControl() {
		CHDFSFile::Initialize();
	}

	// This is the entry function
	// @param max_spider_num - the maximum size of the spider list
	// @param log_div_size - this is the number of dictionary log divisions
	// @param spider_set_num - this is the number of individual spider sets to create
	void SpiderListControl(int max_spider_num, int hash_div_num, 
		int log_div_size, int spider_set_num, bool is_test,
		int max_process_num, const char *work_dir) {

		CNodeStat::SetHashDivNum(hash_div_num);

		m_process_set.SetWorkingDirectory(CUtility::ExtendString
			(DFS_ROOT, "DyableSpider/SpiderList/DyableCommand/"));

		m_process_set.SetMaximumClientNum(max_process_num);
		CMapReduce::SetMaximumProcessNum(max_process_num);

		CreateURLMap(log_div_size);

		CTestSpiderMapping test;
		if(is_test == true) {
			test.LoadURLMapping();
		}

		m_url_map_file.SetFileName("LocalData/url_map");
		m_pulse_node_file.SetFileName("LocalData/pulse_node_set");
		m_fin_spider_file.SetFileName("LocalData/fin_spider_list");
		CMapReduce::ExternalHashMap("CreateURLTextMap", m_pulse_node_file, m_url_map_file, 
			m_fin_spider_file, "LocalData/url_set", sizeof(S5Byte), 500);

		if(is_test == true) {
			test.TestURLMapping();
		}

		DistributeURLs(spider_set_num);
		CreateSpiderList(max_spider_num, spider_set_num);
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int hash_div_num = atoi(argv[1]);
	int max_spider_num = atoi(argv[2]);
	bool is_test = atoi(argv[3]);
	int log_div_size = atoi(argv[4]);
	int spider_set_num = atoi(argv[5]);
	int max_process_num = atoi(argv[6]);
	const char *work_dir = argv[7];

	CBeacon::InitializeBeacon(0, 2222);
	CMemoryElement<CSpiderListControl> command;
	command->SpiderListControl(max_spider_num, hash_div_num, 
		log_div_size, spider_set_num, is_test, max_process_num, work_dir);
	command.DeleteMemoryElement();

	CBeacon::SendTerminationSignal();

	return 0;
}
