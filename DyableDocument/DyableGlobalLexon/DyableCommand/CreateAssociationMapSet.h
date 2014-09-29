#include "./CreateWordIDLookup.h"

// This is used to merge associations together based upon their src term.
// Once merged a set of map values can be created for each src term along
// with their relative occurrence.
class CCreateAssociationMapSet : public CNodeStat {

	// This stores the set of dst association ids
	CSegFile m_dst_assoc_file;
	// This stores the mapped word text strings
	CSegFile m_word_map_output_file;

	// This creates the final keyword set
	void ProcessAssociations() {

		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {

			CString arg("Index ");
			arg += i;

			CCreateWordIDLookup::ProcessSet().CreateRemoteProcess
				("../ProcessAssociations/Debug/ProcessAssociations.exe", arg.Buffer(), i);
		}

		CCreateWordIDLookup::ProcessSet().WaitForPendingProcesses();
	}

	// This creates the final association map set
	void CreateAssociationMapSet(int log_div_size, int max_assoc_num) {

		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {

			CString arg("Index ");
			arg += i;
			arg += " ";
			arg += log_div_size + CNodeStat::GetHashDivNum();
			arg += " ";
			arg += max_assoc_num;

			CCreateWordIDLookup::ProcessSet().CreateRemoteProcess
				("../CreateAssociationMapSet/Debug/CreateAssociationMapSet.exe", arg.Buffer(), i);
		}

		CCreateWordIDLookup::ProcessSet().WaitForPendingProcesses();
	}

	// This combines grouped term files for each hash division
	void CombineGroupedTerms() {

		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {

			CString arg("Index ");
			arg += i;

			CCreateWordIDLookup::ProcessSet().CreateRemoteProcess
				("../CombineGroupedTermSets/Debug/CombineGroupedTermSets.exe", arg.Buffer(), i);
		}

		CCreateWordIDLookup::ProcessSet().WaitForPendingProcesses();
	}

public:

	CCreateAssociationMapSet() {
	}

	// This is the entry function
	void CreateAssociationMapSet(int log_div_size) {

		CombineGroupedTerms();

		CSegFile assoc_map_set("LocalData/group_term_set");
		CMapReduce::MergeSet(NULL, assoc_map_set, assoc_map_set, "LocalData/assoc_map_set", 
			sizeof(S5Byte), sizeof(S5Byte) + sizeof(uLong) + sizeof(S5Byte), CNodeStat::GetHashDivNum());

		ProcessAssociations();

		m_dst_assoc_file.SetFileName("LocalData/dst_assoc_set");
		m_word_map_output_file.SetFileName("LocalData/mapped_assoc_text_string");

		CSegFile assoc_map_file(ASSOC_MAP_FILE);
		CMapReduce::ExternalHashMap("RetrieveAssociationMap", m_dst_assoc_file, 
			assoc_map_file, m_word_map_output_file, "LocalData/map_assoc",
			sizeof(S5Byte), 2048);

		CreateAssociationMapSet(log_div_size, 5);
	}
};