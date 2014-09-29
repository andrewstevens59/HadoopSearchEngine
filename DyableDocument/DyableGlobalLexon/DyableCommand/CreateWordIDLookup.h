#include "./TestGlobalLexon.h"

// This is responsible for creating the lookup used to map word ids
// to their equivalent text string representation. To accomplish this
// text string maps must first be mapped to their sequential word id
// counterpart. Finally the lookup index is created to index into 
// every word id that has been created.
class CCreateWordIDLookup {

	// This stores the set of word ids 
	CMemoryChunk<CSegFile> m_word_id_set;
	// This is used to spawn processes
	static CProcessSet m_process_set;

	// This creates the word id lookup
	void WordIDLookup() {

		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {

			CString arg("Index ");
			arg += i;
			arg += " ";
			arg += CNodeStat::GetHashDivNum();

			CCreateWordIDLookup::ProcessSet().CreateRemoteProcess
				("../CreateWordIDLookup/Debug/CreateWordIDLookup.exe", arg.Buffer(), i);
		}

		CCreateWordIDLookup::ProcessSet().WaitForPendingProcesses();
	}

public:

	CCreateWordIDLookup() {
	}

	// This returns the process set
	inline static CProcessSet &ProcessSet() {
		return m_process_set;
	}

	// This is the entry function. It firstly creates the set of sequential word 
	// ids that have been hashed to different clients. The associations are then
	// mapped to each sequential word id. Finally the lookup index is created 
	// to map word ids to associations.
	void CreateWordIDLookup() {

		_int64 max_id;
		_int64 max_assoc_id = 0;
		CHDFSFile assoc_term_id_file;
		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			assoc_term_id_file.OpenReadFile(CUtility::ExtendString
				("LocalData/assoc_num", i));

			assoc_term_id_file.ReadObject(max_id);
			max_assoc_id = max(max_assoc_id, max_id);
		}

		m_word_id_set.AllocateMemory(CNodeStat::GetHashDivNum());
		for(int i=0; i<m_word_id_set.OverflowSize(); i++) {
			m_word_id_set[i].OpenWriteFile(CUtility::ExtendString("LocalData/seq_word_id", i));
		}

		for(_int64 i=0; i<max_assoc_id; i++) {
			int div = i % m_word_id_set.OverflowSize();

			if((i % 100000) == 0) {
				cout<<(((float)i / max_assoc_id) * 100)<<"% Done"<<endl;
			}
			m_word_id_set[div].WriteCompObject((char *)&i, sizeof(S5Byte));
		}

		m_word_id_set.FreeMemory();
		CMapReduce::ExternalHashMap("CreateAssociationMapNULL", "LocalData/seq_word_id", 
			ASSOC_MAP_FILE, "LocalData/word_map", "LocalData/map_assoc", sizeof(S5Byte), 80);

		WordIDLookup();
	}
};
CProcessSet CCreateWordIDLookup::m_process_set;