#include "./IndexLogFile.h"

int main(int argc, char *argv[]) {

	//freopen("OPEN.txt", "w", stdout);
	if(argc < 3)return 0;

	int client_id = atoi(argv[1]);
	int client_num = atoi(argv[2]);
	int log_div_size = atoi(argv[3]);

	CBeacon::InitializeBeacon(client_id, 2222);

	if(client_id >= log_div_size) {
		CBeacon::SendTerminationSignal();
		return 0;
	}


	CNodeStat::SetHashDivNum(log_div_size);
	CNodeStat::SetClientNum(client_num);

	CMemoryElement<CIndexLogEntries> log;
	log->Initialize(client_id); 
	log->ActivateHighPrioritySearchTerms();
	log->CompileLogEntries(client_num); 
	log.DeleteMemoryElement();

	CBeacon::SendTerminationSignal();
}
