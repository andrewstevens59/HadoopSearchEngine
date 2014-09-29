#include "./MergeBinaryLinks.h"

// This class is responsible for creating the set of keyword links that 
// are used to join excerpts together based upon the content they contain.
// Firstly the set of keyword hits must be merged based upon their keyword
// id. Secondly excerpts must be joined in a given keyword division.
class CCreateKeywordLinks : public CNodeStat {

	// This is used to spawn of the different processes
	CProcessSet m_process_set;

	// This stores the keyword hit file
	CSegFile m_keyword_hit_file;
	// This stores the set of merged keyword hits
	CSegFile m_merge_keyword_hit_file;

public:

	CCreateKeywordLinks() {
	}

	// This is the entry function
	// @param window_size - this is the maximum size of the cartesian join
	void CreateKeywordLinks() {

		m_keyword_hit_file.SetFileName("GlobalData/HitList/assoc_hit_file");
		m_merge_keyword_hit_file.SetFileName("LocalData/keyword_hit_file");

		CMapReduce::MergeSet(NULL, m_keyword_hit_file, m_merge_keyword_hit_file, 
			"LocalData/keyword_set", sizeof(S5Byte), sizeof(S5Byte)
			+ sizeof(float) + sizeof(float) + sizeof(uLong), CNodeStat::GetClientNum());
	}
};