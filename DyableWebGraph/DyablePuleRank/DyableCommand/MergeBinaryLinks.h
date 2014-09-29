#include "./TestKeywordLinks.h"

// This class takes the set of binary links and merges
// them based upon the src node so the cluster link 
// set can be created.
class CMergeBinaryLinks {

	// This stores the set of binary links
	CSegFile m_bin_link_file;

public:

	CMergeBinaryLinks() {
	}

	// This is the entry function
	void MergeBinaryLinks() {

		m_bin_link_file.SetFileName("LocalData/bin_link_file");
		CMapReduce::MergeSortedSet(NULL, m_bin_link_file, m_bin_link_file,
			"LocalData/merge_bin_link", sizeof(S5Byte), sizeof(S5Byte)
			+ sizeof(float), CNodeStat::GetHashDivNum());
	}
};