#include "./MergeClusterMapping.h"

// This stores the association map file
const char *ASSOC_MAP_FILE = "GlobalData/Lexon/assoc_map";

// This class is responsible for coordinating all of the different
// stages in the pipeline necessary compile the document index.
// It is run in the background while all other tasks are being completed.
class CCommandServer : public CNodeStat {

	// This defines the number of local client processes 
	static const int LOCAL_CLIENT_PROCESS = 256;
	// This defines the number of dictionary divisions
	static const int LOG_DIV_SIZE = 256;
	// This defines the percentage of terms to keep
	// when creating the excerpt keywords
	static const int WORD_PERCENTAGE = 95;
	// This defines the number of hit divisions used
	static const int HIT_DIV_BREADTH = 256;
	// This defines the maximum number of processes to spawn
	// at any one time during the execution of the pipeline
	static const int MAX_PROCESS_NUM = 6;
	// This defines the number of hash divisions for each client
	static const int HASH_DIV_NUM = 256;
	// This defins a predicate indicating whether testing is used
	static const int IS_PERFORM_TESTS = false;

	// This defines the number of webgraph instances
	static const int WEB_INST_NUM = 1;
	// This defines the number of wave pass instances
	static const int WAVE_PASS_INST = 1;
	// This defines the number of wave pass classes
	static const int WAVE_PASS_CLASS_NUM = 3;
	// This specifies whether to create a new lexon or not
	static const int IS_NEW_LEXON = true;
	// This defines the number of pulse rank cycles
	static const int PULSE_RANK_CYCLES = 20;
	// This defines the number of wave pass cycles
	static const int WAVE_PASS_CYCLES = 6;

	// This is the maximum number of keywords allowed in a given group
	static const int FINAL_KEYWORD_SIZE = 17;
	// This defines the initial keyword size which is later reduced 
	// on subsequent iterations
	static const int INITIAL_KEYWORD_SIZE = 30;
	// This defines the number of times keywords are merged together
	// in order to create new keywords
	static const int GROUP_CYCLE_NUM = 1;
	// This defines the maximum number of associations to create
	static const int MAX_ASSOC_NUM = 99999999;
	// This defines the maximum number of terms that can be grouped
	// together in a given join cycle
	static const int MAX_GROUP_NUM = 9999999;
	// This defines the size of the scan window used to join terms
	// together in the set
	static const int SCAN_WINDOW_SIZE = 4;
	// This stores maximum number of documents that can be grouped
	// under a given keyword for a limited cartesian join
	static const int KEYWORD_WINDOW_SIZE = 2;

	// This defines the maximum number of keyword links allowed
	static const int MAX_KEYWORD_LINK_NUM = (KEYWORD_WINDOW_SIZE * 2 * FINAL_KEYWORD_SIZE) + 2;
	// This defines the maximum number of webgraph links
	static const int MAX_WEBGRAPH_LINK_NUM = 10;
	// This defines the maximum number of s_links that can be 
	// grouped under any one node in the hiearchy
	static const int MAX_GROUP_SLINK_NUM = 30;
	// This defines the maximum number of nodes that can exist
	// on the top level of the hiearchy
	static const int TOP_LEVEL_NODE_NUM = 100;
	// This defines the maximum number of children that 
	// can exist under any node in the hiearchy
	static const int MAX_CHILD_NUM = 10;

	// This defines the size of the spider list
	static const int MAX_SPIDER_NUM = 11000000;
	// This defines the number of spider sets
	static const int SPIDER_SET_NUM = 30;
	// This defines the total number of spider web connections
	static const int SPIDER_WEB_CONNECTIONS = 60;

	// This is used to initiate the set of processes
	CProcessSet m_process_set;

	// This coalesces the document set 
	void CoalesceDocumentSet(int set_num) {

		CProcessSet::RemoveFiles("../GlobalData/CoalesceDocumentSets");
		for(int i=0; i<set_num; i++) {
			CString arg("Index ");
			arg = "Index ";
			arg += i;
			arg += " ";
			arg += set_num;

			m_process_set.CreateRemoteProcess("../DyableDocument/"
				"CoalesceDocumentSets/Debug/CoalesceDocumentSets.exe", arg.Buffer(), i);
		}

		m_process_set.WaitForPendingProcesses();
		m_process_set.ResetProcessSet();
	}

	// This finds the total number of documents that have already been parsed
	// and it also finds the current number of sets that have been produced.
	int FindParsedDocumentSetNum(_int64 &doc_num_parsed) {

		int set_id = 0;
		doc_num_parsed = 0;
		_int64 docs_parsed;
		CHDFSFile size_file;

		while(true) {

			try {
				size_file.OpenReadFile(CUtility::ExtendString
					("GlobalData/CompiledAttributes/doc_set_size", set_id));
				size_file.ReadObject(docs_parsed);
				doc_num_parsed += docs_parsed;
				set_id++;
			} catch(...) {
				break;
			}
		}

		return set_id;
	}

	// This compiles the HTML text
	void CompileHTMLText() {
	
		_int64 doc_num;
		int set_id = FindParsedDocumentSetNum(doc_num);
		CProcessSet::RemoveFiles("../LocalData");
		for(int i=0; i<LOCAL_CLIENT_PROCESS; i++) {
			CString arg("Index ");
			arg = "Index ";
			arg += i;
			arg += " ";
			arg += LOCAL_CLIENT_PROCESS;
			arg += " ";
			arg += set_id + i;
			arg += " ";
			arg += doc_num;
			arg += " ";
			arg += 100000;

			m_process_set.CreateRemoteProcess("../DyableDocument/"
				"DyableParseHTML/Debug/DyableParseHTML.exe", arg.Buffer(), i);
		}

		m_process_set.WaitForPendingProcesses();
		m_process_set.ResetProcessSet();
	}

	// This handles the creation of the document index. This means 
	// starting up the command server and starting each of the 
	// indexers as specified by the number of clients.
	void CreateDocumentIndex() {

		CLexon lexon;
		lexon.LoadStopWordList();

		CProcessSet::RemoveFiles("../LocalData");
		for(int i=0; i<LOCAL_CLIENT_PROCESS; i++) {
			CString arg("Index ");
			arg += i;
			arg += " ";
			arg += LOCAL_CLIENT_PROCESS;
			arg += " ";
			arg += LOG_DIV_SIZE;
			//arg += " ";
			//arg += 100000;

			m_process_set.CreateRemoteProcess("../DyableDocument/"
				"DyableIndex/Debug/DyableIndex.exe", arg.Buffer(), i);
		}

		m_process_set.WaitForPendingProcesses();
		m_process_set.ResetProcessSet();
	}

	// This compiles all the log files creating by each of the 
	// clients in the indexing stage. 
	void CompileLogFiles() {

		CProcessSet::RemoveFiles("../LocalData");
		for(int i=0; i<LOG_DIV_SIZE; i++) {
			CString arg("Index ");
			arg += i;
			arg += " ";
			arg += LOCAL_CLIENT_PROCESS;
			arg += " ";
			arg += LOG_DIV_SIZE;

			if(IS_NEW_LEXON) {
				arg += " New+Lexon";
			} else {
				arg += " Old+Lexon"; 
			}

			m_process_set.CreateRemoteProcess("../DyableHitList/DyableLogFile/Debug/"
				"DyableLogFile.exe", arg.Buffer(), i);
		}

		m_process_set.WaitForPendingProcesses();
		m_process_set.ResetProcessSet();
	}

	// This compiles the lexon. This means adding high priority words
	// and stemming words to their base form.
	void CompileLexon() {

		CProcessSet::RemoveFiles("../LocalData");

		CString arg("Index ");
		arg += WORD_PERCENTAGE;
		arg += " ";
		arg += LOCAL_CLIENT_PROCESS;
		arg += " ";
		arg += LOG_DIV_SIZE;

		m_process_set.CreateRemoteProcess("../DyableLexonWords/"
			"Debug/DyableLexonWords.exe", arg.Buffer(), 0);
		m_process_set.WaitForPendingProcesses();
	}

	// This creates the word list which creates a word map for each word.
	// It also find the occurrence threshold used to cull a certain 
	// percentage of the terms.
	void CreateWordList() {

		CProcessSet::RemoveFiles("../LocalData");

		for(int i=0; i<LOG_DIV_SIZE; i++) {
			CString arg("Index ");
			arg += i;

			m_process_set.CreateRemoteProcess("../DyableHitList/CreateWordList/Debug/"
				"CreateWordList.exe", arg.Buffer(), i);
		}

		m_process_set.WaitForPendingProcesses();
		m_process_set.ResetProcessSet();
	}

	// This processes the hit list created by each of the different
	// clients. This is done once the log file has been processed.
	void CompileHitList() {

		CProcessSet::RemoveFiles("../LocalData");
		for(int i=0; i<LOCAL_CLIENT_PROCESS; i++) {
			CString arg("Index ");
			arg += i;
			arg += " ";
			arg += LOCAL_CLIENT_PROCESS;
			arg += " ";
			arg += LOG_DIV_SIZE;
			arg += " ";
			arg += HIT_DIV_BREADTH;

			m_process_set.CreateRemoteProcess("../DyableHitList/DyableHitList/Debug/"
				"DyableHitList.exe", arg.Buffer(), i);
		}

		m_process_set.WaitForPendingProcesses();
		m_process_set.ResetProcessSet();
	}

	// This processes all the titles produced previously and 
	// produces a list of statistically significant token sets.
	void CompileTokenSet() {

		CProcessSet::RemoveFiles("../LocalData");
		for(int i=0; i<LOCAL_CLIENT_PROCESS; i++) {
			CString command("Index ");
			command += CANConvert::NumericToAlpha(i);
			command += " ";
			command += CANConvert::NumericToAlpha(LOCAL_CLIENT_PROCESS);

			m_process_set.CreateRemoteProcess("../DyableTitleSet/"
				"DyableTokenSet/DyableTokenSet.exe", command.Buffer(), i);
		}

		m_process_set.WaitForPendingProcesses();
		m_process_set.ResetProcessSet();
	}

	// This is used to create the set of association maps
	void CreateAssociationMapSet() {

		// HASH_DIV_NUM + LOG_DIV_SIZE to include singular terms aswell
		for(int i=0; i<HASH_DIV_NUM + LOG_DIV_SIZE; i++) {
			CString arg("Index ");
			arg += i;
			arg += " ";
			arg += LOG_DIV_SIZE;

			m_process_set.CreateRemoteProcess("../DyableDocument/DyableAssociations/"
				"CreateAssociationMap/Debug/CreateAssociationMap.exe", arg.Buffer(), i);
		}

		m_process_set.WaitForPendingProcesses();
	}

	// This compiles the global lexon word set used in retrieval
	void CompileGlobalLexonWordSet() {

		CProcessSet::RemoveFiles("../LocalData");

		CString command("Index ");
		command += IS_PERFORM_TESTS;
		command += " ";
		command += MAX_PROCESS_NUM;
		command += " ";
		command += HASH_DIV_NUM;
		command += " ";
		command += LOG_DIV_SIZE;
		command += " ../DyableDocument/DyableGlobalLexon/DyableCommand/";

		m_process_set.CreateRemoteProcess("../DyableDocument/"
			"DyableGlobalLexon/DyableCommand/Debug/DyableCommand.exe", command.Buffer(), 0);

		m_process_set.WaitForPendingProcesses();
		m_process_set.ResetProcessSet();
	}

	// This starts pulse rank that is used to find the stationary distribution
	// accross all nodes. This is done in parallel.
	void CalculatePulseRank(bool is_keyword_set) {

		CProcessSet::RemoveFiles("../LocalData");

		CString arg("Index 0 ");
		arg += LOCAL_CLIENT_PROCESS;
		arg += " ";
		arg += PULSE_RANK_CYCLES;
		arg += " ";
		arg += HASH_DIV_NUM;
		arg += " ";
		arg += is_keyword_set;
		arg += " ";

		if(is_keyword_set == true) {
			arg += MAX_KEYWORD_LINK_NUM;
		} else {
			arg += MAX_WEBGRAPH_LINK_NUM;
		}

		arg += " ";
		arg += IS_PERFORM_TESTS;
		arg += " ";
		arg += MAX_PROCESS_NUM;
		arg += " ";
		arg += KEYWORD_WINDOW_SIZE;
		arg += " ../DyableWebGraph/DyablePuleRank/"
			"DyableCommand/";

		m_process_set.CreateRemoteProcess("../DyableWebGraph/DyablePuleRank/"
			"DyableCommand/Debug/DyableCommand.exe", arg.Buffer(), 0);
		m_process_set.WaitForPendingProcesses();
		m_process_set.ResetProcessSet();
	}

	// This is responsible for initializing the webgraph stage that 
	// builds the hiearchy and groups nodes into clusters. This is 
	// partly based on Wave Pass and the stationary distribution.
	void ClusterGraph(bool is_keyword_set) {

		CProcessSet::RemoveFiles("../LocalData");
		CProcessSet::RemoveFiles("../GlobalData/SummaryLinks");

		for(int i=0; i<WEB_INST_NUM; i++) {
			CString arg("Index ");
			arg += LOCAL_CLIENT_PROCESS;
			arg += " ";
			arg += i;
			arg += " ";
			arg += WEB_INST_NUM;
			arg += " ";
			arg += HASH_DIV_NUM;
			arg += " ";
			arg += WAVE_PASS_CYCLES;
			arg += " ";
			arg += IS_PERFORM_TESTS;
			arg += " ";
			arg += WAVE_PASS_INST;
			arg += " ";
			arg += WAVE_PASS_CLASS_NUM;
			arg += " ";
			arg += TOP_LEVEL_NODE_NUM;
			arg += " ";
			arg += MAX_CHILD_NUM;
			arg += " ";
			arg += MAX_PROCESS_NUM;
			arg += " ../DyableWebGraph/DyableClusterGraph/"
				"DyableCommand/";

			m_process_set.CreateRemoteProcess("../DyableWebGraph/DyableClusterGraph/"
				"DyableCommand/Debug/DyableCommand.exe", arg.Buffer(), i);
		}

		m_process_set.WaitForPendingProcesses();
		m_process_set.ResetProcessSet();
	}

	// This is repsonsible for creating the ABTrees which are used 
	// to store the summary links between nodes in the hiearchy.
	void CreateABTrees() {

		CProcessSet::RemoveFiles("../LocalData");

		CString arg("Index ");
		arg += WEB_INST_NUM;
		arg += " ";
		arg += HASH_DIV_NUM;
		arg += " ";
		arg += LOCAL_CLIENT_PROCESS;
		arg += " ";
		arg += IS_PERFORM_TESTS;
		arg += " ";
		arg += MAX_PROCESS_NUM;
		arg += " ";
		arg += MAX_GROUP_SLINK_NUM;
		arg += " ../DyableWebGraph/DyableABTree/"
			"DyableCommand/";

		m_process_set.CreateRemoteProcess("../DyableWebGraph/DyableABTree/"
			"DyableCommand/Debug/DyableCommand.exe", arg.Buffer(), 0);

		m_process_set.WaitForPendingProcesses();
		m_process_set.ResetProcessSet();
	}

	// This takes the list of hit items and sorts them, this is 
	// done seperately for cluster hits and lexon hits.
	void SortHitItems() {

		CProcessSet::RemoveFiles("../LocalData");

		for(int i=0; i<HIT_DIV_BREADTH; i++) {
			CString command("Index ");
			command += i;
			command += " ";
			command += LOCAL_CLIENT_PROCESS;

			m_process_set.CreateRemoteProcess("../DyableSort/SortHitList/"
				"Debug/SortHitList.exe", command.Buffer(), i);
		}

		m_process_set.WaitForPendingProcesses();
		m_process_set.ResetProcessSet();
	}

	// This compiles the final lookup index
	void CompileLookupIndex() {

		for(int i=0; i<HIT_DIV_BREADTH; i++) {
			CString command("Index ");
			command += i;

			m_process_set.CreateRemoteProcess("../DyableSort/CompileLookupIndex/"
				"Debug/CompileLookupIndex.exe", command.Buffer(), i);
		}

		m_process_set.WaitForPendingProcesses();
		m_process_set.ResetProcessSet();
	}

	// This compiles the set of keyword links used in webgraph
	void CompileAssociationSet() {

		CProcessSet::RemoveFiles("../LocalData");

		CString arg("Index 0 ");
		arg += LOCAL_CLIENT_PROCESS;
		arg += " ";
		arg += HASH_DIV_NUM;
		arg += " ";
		arg += HIT_DIV_BREADTH;
		arg += " ";
		arg += IS_PERFORM_TESTS;
		arg += " ";
		arg += MAX_PROCESS_NUM;
		arg += " ";
		arg += MAX_ASSOC_NUM;
		arg += " ";
		arg += INITIAL_KEYWORD_SIZE;
		arg += " ";
		arg += LOG_DIV_SIZE;
		arg += " ../DyableDocument/DyableAssociations/"
			"DyableCommand/";

		m_process_set.CreateRemoteProcess("../DyableDocument/DyableAssociations/"
			"DyableCommand/Debug/DyableCommand.exe", arg.Buffer(), 0);
		m_process_set.WaitForPendingProcesses();
		m_process_set.ResetProcessSet();
	}

	// This creates the set of excerpt keywords my discovering patterns
	// in the exisiting keyword set.
	void CreateExcerptKeywords() {

		CProcessSet::RemoveFiles("../LocalData");

		CString arg("Index ");
		arg += LOCAL_CLIENT_PROCESS;
		arg += " ";
		arg += HASH_DIV_NUM;
		arg += " ";
		arg += HIT_DIV_BREADTH;
		arg += " ";
		arg += IS_PERFORM_TESTS;
		arg += " ";
		arg += MAX_PROCESS_NUM;
		arg += " ";
		arg += INITIAL_KEYWORD_SIZE;
		arg += " ";
		arg += FINAL_KEYWORD_SIZE;
		arg += " ";
		arg += GROUP_CYCLE_NUM;
		arg += " ";
		arg += SCAN_WINDOW_SIZE;
		arg += " ";
		arg += MAX_GROUP_NUM;
		arg += " ../DyableDocument/DyableExcerptKeywords/"
			"DyableCommand/";

		m_process_set.CreateRemoteProcess("../DyableDocument/DyableExcerptKeywords/"
			"DyableCommand/Debug/DyableCommand.exe", arg.Buffer(), 0);
		m_process_set.WaitForPendingProcesses();
		m_process_set.ResetProcessSet();
	}

	// This creates the sorted set of pulse scores
	void CreateSortedPulseScores() {

		CProcessSet::RemoveFiles("../LocalData");

		CString arg("Index ");
		arg += HASH_DIV_NUM;
		m_process_set.CreateRemoteProcess("../DyableSort/SortPulseScores/Debug/"
			"SortPulseScores.exe", arg.Buffer(), 0);

		m_process_set.WaitForPendingProcesses();
		m_process_set.ResetProcessSet();
	}

	// This creates the spider list so the index can be updated
	void CreateSpiderList() {

		CProcessSet::RemoveFiles("../GlobalData/SpiderList");

		CString arg("Index ");
		arg += HASH_DIV_NUM;
		arg += " ";
		arg += MAX_SPIDER_NUM;
		arg += " ";
		arg += IS_PERFORM_TESTS;
		arg += " ";
		arg += LOG_DIV_SIZE;
		arg += " ";
		arg += SPIDER_SET_NUM;
		arg += " ";
		arg += MAX_PROCESS_NUM;
		arg += " ../DyableSpider/SpiderList/"
			"DyableCommand/";

		m_process_set.CreateRemoteProcess("../DyableSpider/SpiderList/"
			"DyableCommand/Debug/DyableCommand.exe", arg.Buffer(), 0);

		m_process_set.WaitForPendingProcesses();
		m_process_set.ResetProcessSet();
	}

	// This spiders the set of predefined urls
	void SpiderURLSet(int spider_num) {

		int connections = SPIDER_WEB_CONNECTIONS / spider_num;
		m_process_set.SetMaximumClientNum(spider_num);
		for(int i=0; i<SPIDER_SET_NUM; i++) {
			CString command("Index ");
			command += i;
			command += " ";
			command += connections;

			m_process_set.CreateRemoteProcess("../DyableSpider/ExternalSpider/"
				"TestProject/Debug/TestProject.exe", command.Buffer(), i);
		}
	}

	// This is used to test the entire retrieval system by creating a number query instances
	void TestQuerySystem() {

		CHDFSFile assoc_file;
		assoc_file.OpenReadFile(CUtility::ExtendString
			(ASSOC_MAP_FILE, (int)0));

		S5Byte word_id;
		float weight;
		uChar word_length;
		int count = 0;

		CString arg;
		m_process_set.SetMaximumClientNum(3);
		while(assoc_file.ReadCompObject(word_id)) {
			assoc_file.ReadCompObject(weight);
			assoc_file.ReadCompObject(word_length);
			assoc_file.ReadCompObject(CUtility::SecondTempBuffer(), word_length);
			CUtility::SecondTempBuffer(word_length) = '\0';

			if(++count < 6001) {
				continue;
			}

			arg = "Index q=";
			arg += CUtility::SecondTempBuffer();
			arg += "&f= ";
			arg += count % 30;
			m_process_set.CreateRemoteProcess("../DyableRequest/DyableQuery/"
				"Debug/DyableQuery.exe", arg.Buffer(), count % 30);
			Sleep(150);
		}

	}

public:

	CCommandServer() {
		CNodeStat::SetClientNum(LOCAL_CLIENT_PROCESS);
		CNodeStat::SetClientNum(LOCAL_CLIENT_PROCESS);
		m_process_set.SetPort(2222);
		m_process_set.SetMaximumClientNum(MAX_PROCESS_NUM);
		m_process_set.SetWorkingDirectory(CUtility::ExtendString
			(DFS_ROOT, "DyableCommand/"));

		CHDFSFile::Initialize();
	}

	// This is called to compile all the different stages in the pipeline
	void ProcessPipeline() {

		//TestQuerySystem();

		//SpiderURLSet(1);
		//CoalesceDocumentSet(LOCAL_CLIENT_PROCESS);return;

		
		/*CProcessSet::RemoveFiles("../GlobalData/HitList");
		CProcessSet::RemoveFiles("../GlobalData/ABTrees");
		CProcessSet::RemoveFiles("../GlobalData/Keywords");
		CProcessSet::RemoveFiles("../GlobalData/Lexon");
		CProcessSet::RemoveFiles("../GlobalData/ClusterHiearchy");
		CProcessSet::RemoveFiles("../GlobalData/SpiderList");
		CProcessSet::RemoveFiles("../GlobalData/LogFile");
		CProcessSet::RemoveFiles("../GlobalData/LinkSet");
		CProcessSet::RemoveFiles("../GlobalData/SortedHits");
		CProcessSet::RemoveFiles("../GlobalData/PulseRank");
		CProcessSet::RemoveFiles("../GlobalData/DocumentDatabase");
		CProcessSet::RemoveFiles("../GlobalData/Retrieve");
		CProcessSet::RemoveFiles("../GlobalData/WordDictionary");
		CProcessSet::RemoveFiles("../GlobalData/SummaryLinks");
		CProcessSet::RemoveFiles("../GlobalData/Excerpts");

		cout<<"Parsing HTML text"<<endl;
		//CompileHTMLText();

		cout<<"Creating Document Index"<<endl;
		CreateDocumentIndex();

		cout<<"Compiling the Log File"<<endl;
		CompileLogFiles();

		cout<<"Compiling the Lexon"<<endl;
		CompileLexon();

		cout<<"Creating Word List"<<endl;
		CreateWordList();

		cout<<"Compiling the Hit List"<<endl;
		CompileHitList();

		cout<<"Calculating Pulse Rank"<<endl;
		CalculatePulseRank(false);

		cout<<"Create Sorted Pulse Scores"<<endl;
		CreateSortedPulseScores();

		cout<<"Creating Spider List"<<endl;
		//CreateSpiderList();

		cout<<"Compiling Association Set"<<endl;
		CompileAssociationSet();
		
		cout<<"Creating the Excerpt Keyword Set"<<endl;
		CreateExcerptKeywords();

		cout<<"Creating Association Map"<<endl;
		CreateAssociationMapSet();

		cout<<"Compiling Global Lexon"<<endl;
		CompileGlobalLexonWordSet();

		cout<<"Calculating Pulse Rank"<<endl;
		CalculatePulseRank(true);

		cout<<"Compiling Webgraph"<<endl;
		ClusterGraph(false);*/
	
		cout<<"Creating ABTrees"<<endl;
		CreateABTrees();

		/*CMergeClusterMapping merge_clus;
		merge_clus.MergeClusterMapping(HASH_DIV_NUM);

		cout<<"Sorting Hit Items"<<endl;
		SortHitItems();

		cout<<"Compiling Lookup Index"<<endl;
		CompileLookupIndex();*/

		cout<<"Compiling Global Lexon"<<endl;
		CompileGlobalLexonWordSet();
	}

	void TestDocumentIndex() {

		ProcessPipeline();

		CTestLogFile remap;
		remap.RemapWordIDs();

		CTestHitList test_hit_list;
		test_hit_list.CheckLinkSet(LOCAL_CLIENT_PROCESS);
		test_hit_list.CheckHitList(LOCAL_CLIENT_PROCESS);
		test_hit_list.CheckAnchorHitList();
		test_hit_list.CheckTitleSegments();
		test_hit_list.WriteFinalHitList();
	
		cout<<"Sorting Hit Items"<<endl;
		SortHitItems();

		CTestSortHitList sort;
		sort.TestSortHitList();

		CTestCompileLookupIndex compile;
		compile.TestCompileLookupIndex();
	}

};

int main() {

	//freopen("OUTPUT.txt", "w", stdout);

	CCommandServer command;
	command.ProcessPipeline();
	
	return 0;
}
