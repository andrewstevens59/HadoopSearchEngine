#include "../ProcessSet.h"

// This is a general purpose class that is primarily responsible
// for handling the communication between different clients. 
// This involves sharing cluster ID's and other global variables
// that allow the different clients to work in unisounce.
class CCommunication : public CNodeStat {

	// This stores an instances of a process set
	static CProcessSet m_process_set;
	// This stores the number of wave pass instances
	static int m_wave_pass_inst;
	// This stores the number of wave pass classes
	static int m_wave_pass_class;

public:

	CCommunication() {
	}

	// This returns a reference to the process set
	static inline CProcessSet &ProcessSet() {
		return m_process_set;
	}

	// This sets the wave pass statistics
	void SetWavePass(int wave_pass_inst, int class_num) {
		m_wave_pass_inst = wave_pass_inst;
		m_wave_pass_class = class_num;
	}

	// This returns the number of wave pass classes
	inline int WavePassClasses() {
		return m_wave_pass_class;
	}

	// This returns the number of wave pass instances
	inline int WavePassInstNum() {
		return m_wave_pass_inst;
	}
};
CProcessSet CCommunication::m_process_set;
int CCommunication::m_wave_pass_inst;
int CCommunication::m_wave_pass_class;