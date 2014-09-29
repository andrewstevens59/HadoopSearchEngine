#include "./CompileLinkSet.h"


int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);
	int client_num = atoi(argv[2]);
	int log_div_num = atoi(argv[3]);
	int hit_list_breadth = atoi(argv[4]);

	CBeacon::InitializeBeacon(client_id, 2222);
	CMemoryElement<CCompileLinkSet> log;
	log->LoadLinkSet(client_id, client_num, log_div_num, hit_list_breadth);
	log.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();

}