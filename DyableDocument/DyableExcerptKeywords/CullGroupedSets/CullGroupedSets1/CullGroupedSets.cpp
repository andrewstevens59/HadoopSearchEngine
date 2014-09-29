#include "../../../../MapReduce.h"

// This takes the set of grouped terms and their respective occurrence
// and culls grouped terms with an occurrence below some threshold.
// Grouped terms that have an occurrence above the threshold are 
// assigned a unique id and written out to disk.
class CCullGroupedSets : public CNodeStat {

	// This stores the occurrence of each grouped set
	CSegFile m_grouped_term_occur_file;
	// This stores the final set of grouped terms that
	// exceed the threshold
	CSegFile m_fin_group_term_file;
	// This stores the set of grouped keywords used in 
	// the lexon to find high associations with a given term
	// This stores the final weight file
	CHDFSFile m_term_weight_file;
	// This stores the total number of grouped terms
	_int64 m_group_term_num;
	// This stores the current group term id
	_int64 m_curr_id;

	// This reads a virtual weight from file
	static bool ReadKeywordWeight(CHDFSFile &file, int &occur) {
		return file.ReadCompObject(occur);
	}

	// This writes a virtual weight to file
	static void WriteKeywordWeight(CHDFSFile &file, int &occur) {
		file.WriteCompObject(occur);
	}

	// This finds the threshold weight for the set of associations based
	// upon the maximum number of associations allowed
	// @param max_term_num - this is the maximum number of terms allowed
	int FindThresholdWeight(int max_term_num) {

		int threshold = -1;
		if(m_group_term_num > max_term_num) {

			CKthOrderStat<int, CHDFSFile> cutoff;
			cutoff.Initialize(99999, CUtility::ExtendString
				("LocalData/thresh", GetClientID()),
				ReadKeywordWeight, WriteKeywordWeight);

			threshold = cutoff.FindKthOrderStat(m_term_weight_file,
				 m_group_term_num - max_term_num);
		}

		return max(threshold, 3);
	}

	// This creates the final set of culled grouped terms
	// @param max_term_num - this is the maximum number of terms allowed
	void CreateGroupedTerms(int max_term_num) { 

		int threshold = FindThresholdWeight(max_term_num);

		uLong occur;
		CMemoryChunk<S5Byte> group_term_set(2);
		m_grouped_term_occur_file.OpenReadFile();
		while(m_grouped_term_occur_file.ReadCompObject(group_term_set.Buffer(), 2)) {
			m_grouped_term_occur_file.ReadCompObject(occur);

			if(occur < threshold) {
				continue;
			}

			m_fin_group_term_file.AskBufferOverflow((sizeof(S5Byte) << 1) +
				sizeof(S5Byte) + sizeof(uLong));

			m_fin_group_term_file.WriteCompObject(group_term_set.Buffer(), 2);
			m_fin_group_term_file.WriteCompObject5Byte(m_curr_id);
			m_fin_group_term_file.WriteCompObject(occur);
			m_curr_id += CNodeStat::GetClientNum();
		}
	}

public:

	CCullGroupedSets() {
		CHDFSFile::Initialize();
	}

	// This is the entry function
	// @param max_group_set_num - this is the maximum number of grouped 
	//                          - terms allowed in a given cycle
	// @param level - this is the current level in the hiearchy
	void CullGroupedSets(int client_id, int client_num, 
		int max_group_set_num, int level) {

		CNodeStat::SetClientID(client_id);
		CNodeStat::SetClientNum(client_num);

		CHDFSFile assoc_term_id_file;
		assoc_term_id_file.OpenReadFile(CUtility::ExtendString
			("LocalData/assoc_num", GetClientID()));
		assoc_term_id_file.ReadObject(m_curr_id);

		m_grouped_term_occur_file.OpenReadFile(CUtility::ExtendString
			("LocalData/grouped_term_occur", GetClientID()));

		m_fin_group_term_file.OpenWriteFile(CUtility::ExtendString
			("GlobalData/Keywords/fin_group_term_set", level, ".set", GetClientID()));

		m_term_weight_file.OpenWriteFile(CUtility::ExtendString
			("LocalData/group_term_weight", GetClientID()));

		uLong occur;
		m_group_term_num = 0;
		CMemoryChunk<S5Byte> group_term_set(2);
		while(m_grouped_term_occur_file.ReadCompObject(group_term_set.Buffer(), 2)) {
			m_grouped_term_occur_file.ReadCompObject(occur);
			m_term_weight_file.WriteCompObject(occur);
			m_group_term_num++;
		}

		CreateGroupedTerms(max_group_set_num);

		assoc_term_id_file.OpenWriteFile();
		assoc_term_id_file.WriteObject(m_curr_id);
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);
	int client_num = atoi(argv[2]);
	int max_group_set_num = atoi(argv[3]);
	int level = atoi(argv[4]);

	CBeacon::InitializeBeacon(client_id);
	CMemoryElement<CCullGroupedSets> group;
	group->CullGroupedSets(client_id, client_num, max_group_set_num, level);
	group.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();
}
