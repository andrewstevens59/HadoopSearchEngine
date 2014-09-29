#include "../../../../../MapReduce.h"

// This class is responsible for updating the wave pass class
// label after each wave pass instance has completed. 
class CWavePassHistory : public CNodeStat {

	// This stores the class number bit width
	int m_class_bit_width;
	// This stores the number of wave pass classes
	int m_class_num;

	// This stores the updated wave distribution
	CArrayList<SWaveDist> m_wave_dist_buff;
	// This stores all the back class distribution
	CHDFSFile m_wave_pass_file;
	// This stores the label file for the given hash division
	CSegFile m_label_file;

	// This is responsible for updating the class assignment, once the
	// initial class assignment has been created. It simply appends the
	// current class assignment to the previous class assignment.
	void UpadateClassDist() {

		float dist;
		SWaveDist wave_dist;
		float majority_weight;
		u_short majority_class;

		m_wave_dist_buff.Resize(0);
		while(m_wave_pass_file.ReadCompObject(wave_dist.node)) {
			wave_dist.ReadWaveDist(m_label_file);

			bool assign = false;
			majority_weight = 0;
			for(int j=0; j<m_class_num; j++) {
				m_wave_pass_file.ReadCompObject(dist);

				if(dist > majority_weight) {
					majority_weight = dist;
					majority_class = (u_short)j;
					assign = true;
				}
			}

			if(assign == false) {
				cout<<"assign error";getchar();
			}

			wave_dist.majority_weight += majority_weight;
			wave_dist.cluster |= majority_class << m_class_bit_width;
			m_wave_dist_buff.PushBack(wave_dist);
		}
	}

	// This assigns the initialial class distribution. The majority
	// class is calculated along with the neighbour similarity.
	void AssignInitClassDist() {

		float dist;
		SWaveDist wave_dist;
		while(m_wave_pass_file.ReadCompObject(wave_dist.node)) {

			bool assign = false;
			wave_dist.majority_weight  = 0;
			for(int j=0; j<m_class_num; j++) {
				m_wave_pass_file.ReadCompObject(dist);

				if(dist > wave_dist.majority_weight) {
					wave_dist.majority_weight = dist;
					wave_dist.cluster = (u_short)j;
					assign = true;
				}
			}

			if(assign == false) {
				cout<<"assign error";getchar();
			}

			wave_dist.WriteWaveDist(m_label_file);
		}	
	}
	
	// This assigns the cluster label to each node in the set that 
	// belongs to a particular client.
	// @param label_file - this stores the cluster label and wave 
	//                   - distribution for each node
	// @param wave_pass_file - this stores the wave pass distribution 
	//                       - for this client
	// @param update_label - this is set to true after the initial wave pass
	// @param base_node_num - this is the current number of nodes being processed
	void AssignLabel(bool update_label, _int64 base_node_num) {

		if(update_label == false) {
			m_label_file.OpenWriteFile();
			AssignInitClassDist();
			return;
 		} 

		m_label_file.OpenReadFile();
		m_wave_dist_buff.Initialize((int)(base_node_num / CNodeStat::GetClientNum()) + 1000);
		UpadateClassDist();

		m_label_file.OpenWriteFile();
		for(int i=0; i<m_wave_dist_buff.Size(); i++) {
			m_wave_dist_buff[i].WriteWaveDist(m_label_file);
		}
	}

public:

	CWavePassHistory() {
		CHDFSFile::Initialize();
	}

	// This is the entry function
	// @param hash_div - this is the current hash division being processed
	// @param class_bit_width - this is the class bit offset
	// @param hash_div_num - this is the total number of hash divisions
	// @param update_label - true if the the current wave pass dist 
	//                     - is being updated
	// @param class_num - this is the number of classes in the wave pass dist
	// @param base_node_num - this is the current number of nodes being processed
	void WavePassHistory(int hash_div, int hash_div_num, int class_bit_width, 
		bool update_label, int class_num, _int64 base_node_num) {

		CNodeStat::SetInstID(0);
		CNodeStat::SetClientID(hash_div);
		CNodeStat::SetClientNum(hash_div_num);

		m_class_bit_width = class_bit_width;
		m_class_num = class_num;

		m_wave_pass_file.OpenReadFile(CUtility::ExtendString
			("LocalData/final_wave_pass", GetClientID()));

		m_label_file.SetFileName(CUtility::ExtendString
			("LocalData/label_file", GetInstID(), ".set", GetClientID()));

		AssignLabel(update_label, base_node_num);
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int hash_div = atoi(argv[1]);
	int hash_div_num = atoi(argv[2]);
	int class_bit_width = atoi(argv[3]);
	bool update_label = atoi(argv[4]);
	int class_num = atoi(argv[5]);
	_int64 base_node_num = CANConvert::AlphaToNumericLong(argv[6], strlen(argv[6]));

	CBeacon::InitializeBeacon(hash_div, 5555);
	CMemoryElement<CWavePassHistory> history;
	history->WavePassHistory(hash_div, hash_div_num, class_bit_width, 
		update_label, class_num, base_node_num);

	history.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();
}