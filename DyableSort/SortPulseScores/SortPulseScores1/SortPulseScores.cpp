#include "../../../ProcessSet.h"


// This class takes the sets of pulse scores and sorts them.
// Each set must be merged before sorting can begin.
class CSortPulseScores : public CNodeStat {

	// This stores the merged sets of pulse scores
	CSegFile m_merged_pulse_file;

public:

	CSortPulseScores() {
		CHDFSFile::Initialize();
	}

	// This is the entry function
	void SortPulseScores(int client_num) {

		CNodeStat::SetClientNum(client_num);
		CNodeStat::InitializeNodeLinkStat("GlobalData/WordDictionary/dictionary_offset");

		m_merged_pulse_file.SetFileName("GlobalData/PulseRank/sorted_pulse_score0");
		CSegFile pulse_score_file("GlobalData/PulseRank/webgraph_pulse_score");

		int hit_size = sizeof(S5Byte) + sizeof(float);
		CMapReduce::ExternalRadixSort(pulse_score_file, m_merged_pulse_file,
			 "LocalData/sort", hit_size, sizeof(S5Byte));
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_num = atoi(argv[1]);

	CBeacon::InitializeBeacon(0, 2222);
	CMemoryElement<CSortPulseScores> pulse;
	pulse->SortPulseScores(client_num);
	pulse.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();

	return 0;
}

