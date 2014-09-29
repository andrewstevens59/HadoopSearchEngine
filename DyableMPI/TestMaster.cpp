#include "../ProcessSet.h"

int main(int argc, char *argv[]) {

	cout<<"Master Active"<<endl;

	CProcessSet set;

	for(int j=0; j<10; j++) {
		cout<<"SET ----------------------------- "<<j<<endl;
	for(int i=0; i<64; i++) {
		set.CreateRemoteProcess("TestSlave", "TestSlave", 0);
	}

	set.WaitForPendingProcesses();
	}

	cout<<"Master Terminated"<<endl;

return 0;
}
