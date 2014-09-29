#include "./TestKeywordLinks.h"

// This class takes the set of excerpt and keywords generated
// in HitList and maps each term id to it's corresponding occurrence.
class CCreateWordOccurrenceMap : public CNodeStat {

	// This stores the occurrence of each term in the set
	CSegFile m_excerpt_occur_map_file;
	// This stores the list of word ids that compose each document
	CSegFile m_excerpt_id_set_file;

public:

	CCreateWordOccurrenceMap() {
		CHDFSFile::Initialize();
	}

	// This is the entry function 
	void CreateWordOccurrenceMap() {

		CSegFile occur_map_file;
		occur_map_file.SetFileName(CUtility::ExtendString
			(KEYWORD_OCCURRENCE_FILE, GetClientID()));

		m_excerpt_id_set_file.SetFileName(CUtility::ExtendString
			("GlobalData/DocumentDatabase/full_excerpt_word_id_set", GetClientID()));
		m_excerpt_occur_map_file.SetFileName(CUtility::ExtendString
			("LocalData/full_excerpt_occur_map", GetClientID()));

		CMapReduce::ExternalHashMap(NULL, m_excerpt_id_set_file, 
			occur_map_file, m_excerpt_occur_map_file,
			"LocalData/map_occur1", 4, 4);
	}
};