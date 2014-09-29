#include "../../../MapReduce.h"

// This creates the dst ids for all of the associations once they have been merged
class CProcessAssociations {

	// This stores the set of merged associations
	CHDFSFile m_merge_assoc_set_file;
	// This stores the set of dst association ids
	CSegFile m_dst_assoc_file;

public:

	CProcessAssociations() {
		CHDFSFile::Initialize();
	}

	// This is the entry function
	void ProcessAssociations(int client_id) {

		CNodeStat::SetClientID(client_id);

		CMemoryChunk<S5Byte> assoc_id_set(2);
		m_merge_assoc_set_file.OpenReadFile(CUtility::ExtendString
			("LocalData/group_term_set", client_id));

		m_dst_assoc_file.OpenWriteFile(CUtility::ExtendString
			("LocalData/dst_assoc_set", client_id));

		S5Byte id;
		uLong occur;
		_int64 prev_assoc_id = -1;
		while(m_merge_assoc_set_file.ReadCompObject(assoc_id_set.Buffer(), 2)) {
			m_merge_assoc_set_file.ReadCompObject(id);
			m_merge_assoc_set_file.ReadCompObject(occur);

			if(assoc_id_set[0] != prev_assoc_id) {
				
				if(id != 0) {
					m_dst_assoc_file.WriteCompObject(assoc_id_set[0]);
				}

				prev_assoc_id = assoc_id_set[0].Value();
			}

			m_dst_assoc_file.WriteCompObject(assoc_id_set[1]);
		}
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);

	CBeacon::InitializeBeacon(client_id);
	CMemoryElement<CProcessAssociations> group;
	group->ProcessAssociations(client_id);
	group.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();
}