#include "../../../MapReduce.h"

// This merges the differnent grouped term sets into a single file
class CCombineGroupedTermSets {

	// This stores the set of merged associations
	CSegFile m_comb_group_file;
	// This stores the set of merged associations
	CHDFSFile m_merge_assoc_set_file;

public:

	CCombineGroupedTermSets() {
		CHDFSFile::Initialize();
	}

	// This is the entry function
	void CombineGroupedTermSets(int client_id) {

		CNodeStat::SetClientID(client_id);

		CMemoryChunk<uLong> assoc_id_set1(2);
		CMemoryChunk<S5Byte> assoc_id_set2(2);
		m_merge_assoc_set_file.OpenReadFile(CUtility::ExtendString
			("GlobalData/Keywords/fin_group_term_set0.set", client_id));

		m_comb_group_file.OpenWriteFile(CUtility::ExtendString
			("LocalData/group_term_set", client_id));

		S5Byte id;
		uLong occur;
		while(m_merge_assoc_set_file.ReadCompObject(assoc_id_set1.Buffer(), 2)) {
			m_merge_assoc_set_file.ReadCompObject(id);
			m_merge_assoc_set_file.ReadCompObject(occur);

			assoc_id_set2[0] = assoc_id_set1[0];
			assoc_id_set2[1] = assoc_id_set1[1];
			
			id = 0;
			m_comb_group_file.AskBufferOverflow((sizeof(S5Byte) << 1) + sizeof(uLong) + sizeof(S5Byte));
			m_comb_group_file.WriteCompObject(assoc_id_set2.Buffer(), 2);
			m_comb_group_file.WriteCompObject(id);
			m_comb_group_file.WriteCompObject(occur);
		}

		m_merge_assoc_set_file.OpenReadFile(CUtility::ExtendString
			("GlobalData/Keywords/fin_group_term_set1.set", client_id));

		while(m_merge_assoc_set_file.ReadCompObject(assoc_id_set2.Buffer(), 2)) {
			m_merge_assoc_set_file.ReadCompObject(id);
			m_merge_assoc_set_file.ReadCompObject(occur);

			m_comb_group_file.AskBufferOverflow((sizeof(S5Byte) << 1) + sizeof(uLong) + sizeof(S5Byte));
			m_comb_group_file.WriteCompObject(assoc_id_set2.Buffer(), 2);
			m_comb_group_file.WriteCompObject(id);
			m_comb_group_file.WriteCompObject(occur);

			id = 0;
			CSort<char>::Swap(assoc_id_set2[0], assoc_id_set2[1]);
			m_comb_group_file.AskBufferOverflow((sizeof(S5Byte) << 1) + sizeof(uLong) + sizeof(S5Byte));
			m_comb_group_file.WriteCompObject(assoc_id_set2.Buffer(), 2);
			m_comb_group_file.WriteCompObject(id);
			m_comb_group_file.WriteCompObject(occur);

		}
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);

	CBeacon::InitializeBeacon(client_id);
	CMemoryElement<CCombineGroupedTermSets> group;
	group->CombineGroupedTermSets(client_id);
	group.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();
}