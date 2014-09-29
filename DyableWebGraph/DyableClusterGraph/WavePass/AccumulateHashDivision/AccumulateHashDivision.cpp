#include "../../../../MapReduce.h"

// This class accumulates all of the wave pass distributions 
// in a given hash division. It groups each distribution by
// their src node and sums together each component in the 
// wave pass distribution.
class CAccumulateHashDivision : public CNodeStat {

	// This maps global dst indexes to local indexes 
	CObjectHashMap<S5Byte> m_node_map;
	// This stores the wave pass distribution for every 
	// node in the local hash division
	CArrayList<float> m_wave_pass_dist;
	// This stores the updated back buffer distribution
	CArrayList<char> m_back_buff_dist;
	// This stores the global class weight used to keep wave pass balanced
	CMemoryChunk<float> m_class_weight;

	// This stores all the forward class distribution
	CFileSet<CHDFSFile> m_forward_set;
	// This stores all the back class distribution
	CHDFSFile m_back_file;
	// This stores the final wave pass file including external nodes
	CHDFSFile m_fin_wave_pass_file;
	// This defines the number of classes to use for wave pass,
	// this will most usually be set to the the number of clients
	int m_class_num;

	// This defines a function that computes the hash map for an object
	static int HashCode(const S5Byte &item) {
		return (int)S5Byte::Value(item);
	}

	// This defines a function to compare two nodes for equality
	static bool Equal(const S5Byte &arg1, const S5Byte &arg2) {
		return S5Byte::Value(arg1) == S5Byte::Value(arg2);
	}

	// This assigns a random distribution to one of the back buffer nodes.
	// This is done during start up before wave pass commences.
	// @param node - this is the current back buffer node being processed
	void AssignBackBuffDist(S5Byte &node) {

		static CMemoryChunk<float> dist(m_class_num);

		m_back_buff_dist.CopyBufferToArrayList((char *)&node, 
			sizeof(S5Byte), m_back_buff_dist.Size());

		for(int i=0; i<m_class_num; i++) {
			dist[i] = CMath::RandGauss(0.2f, 1.0f);
		}

		CMath::NormalizeVector(dist.Buffer(), m_class_num);

		for(int i=0; i<m_class_num; i++) {
			m_class_weight[i] += dist[i];
			m_back_buff_dist.CopyBufferToArrayList((char *)&(dist[i]), 
				sizeof(float), m_back_buff_dist.Size());
		}
	}

	// This updates the wave pass distribution for all nodes in a given hash 
	// division. This simply means accumulating the wave pass distribution
	// score for every dst node in a division. This is normalized before being
	// added to the existing back buffer.
	void AccumulateDistribution() {

		float dist;
		S5Byte dst;

		for(int j=0; j<GetClientNum(); j++) {
			m_forward_set.OpenReadFile(CNodeStat::GetClientID(), j);

			CHDFSFile &node_file = m_forward_set.File(CNodeStat::GetClientID());
			while(node_file.ReadCompObject(dst)) {
				int id = m_node_map.Put(dst);

				if(!m_node_map.AskFoundWord()) {
					for(int k=0; k<m_class_num; k++) {
						node_file.ReadCompObject(dist);
						m_wave_pass_dist.PushBack(dist);
					}
				} else {
					int offset = id * m_class_num;
					for(int k=0; k<m_class_num; k++) {
						node_file.ReadCompObject(dist);
						m_wave_pass_dist[offset++] += dist;
					}
				}
			}
		}
	}

	// This updates the wave pass distribution, based upon the 
	// neighbours linking to a given node.
	// @param curr_dist - this stores the wave pass distribution
	//                  - on the previous pass
	// @param new_dist - this is used to store the updated wave 
	//                 - pass distribution on this pass
	// @param id - this is the local if of the current node being processed
	// @param sum - this is used for normalization of the wave pass distribution
	// @param curr_class_weight - this stores the net wave pass score of each
	//                          - class oon the current pass
	void AssignUpdateDistribution(CMemoryChunk<float> &curr_dist,
		CMemoryChunk<float> &new_dist, int id, float &sum, 
		CMemoryChunk<float> &curr_class_weight) {

		if(id < 0) {
			// nothing linked to this base node so distribution is unchanged
			sum = 1.0f;
			return;
		}

		int offset = id * m_class_num;
		CMath::NormalizeVector(m_wave_pass_dist.Buffer() + offset, m_class_num);

		for(int i=0; i<m_class_num; i++) {
			new_dist[i] = m_wave_pass_dist[offset++] * 
				curr_dist[i] * curr_class_weight[i];
		}

		sum = 0;
		CMath::NormalizeVector(new_dist.Buffer(), m_class_num);
		for(int i=0; i<m_class_num; i++) {
			curr_dist[i] += new_dist[i];
			sum += curr_dist[i];
		}
	}

	// This writes the new back buffer by adding the accumulated wave pass 
	// distribution to the existing back buffer distribution.
	// @param curr_class_weight - this stores the current class weight 
	//                          - used to balance the clusters
	void WriteBackBuffer(CMemoryChunk<float> &curr_class_weight) {

		float sum = 0;
		S5Byte node;

		CMemoryChunk<float> curr_dist(m_class_num);
		CMemoryChunk<float> new_dist(m_class_num);

		m_back_file.OpenReadFile();
		m_back_buff_dist.Resize(0);
		while(m_back_file.ReadCompObject(node)) {
			for(int i=0; i<m_class_num; i++) {
				m_back_file.ReadCompObject(curr_dist[i]);
			}

			int id = m_node_map.Get(node);

			m_back_buff_dist.CopyBufferToArrayList((char *)&node, 
				sizeof(S5Byte), m_back_buff_dist.Size());

			int offset = id * m_class_num;
			AssignUpdateDistribution(curr_dist, new_dist, 
				id, sum, curr_class_weight);

			for(int i=0; i<m_class_num; i++) {
				curr_dist[i] /= sum;

				m_class_weight[i] += curr_dist[i];
				m_back_buff_dist.CopyBufferToArrayList((char *)&(curr_dist[i]), 
					sizeof(float), m_back_buff_dist.Size());
			}
		}

		m_back_file.OpenWriteFile();
		m_back_file.WriteCompObject(m_back_buff_dist.Buffer(), m_back_buff_dist.Size());
		m_back_file.CloseFile();
	}

	// After all the external nodes have been added now add in all the base
	// nodes that were not linked to be any of the dst nodes. This is need
	// to maintain a complete set of nodes and associated wave pass distributions.
	// All nodes that are added in this final pass have a distribution that is
	// unchanged from the original distribution.
	void AddMissingBaseNodes() {

		float dist;
		S5Byte node;

		m_back_file.OpenReadFile();
		m_back_buff_dist.Resize(0);
		while(m_back_file.ReadCompObject(node)) {
			int id = m_node_map.Put(node);

			if(m_node_map.AskFoundWord() == false) {
				// just adds the existing distribution
				for(int i=0; i<m_class_num; i++) {
					m_back_file.ReadCompObject(dist);
					m_wave_pass_dist.PushBack(dist);
				}
			} else {
				int offset = id * m_class_num;
				for(int i=0; i<m_class_num; i++) {
					m_back_file.ReadCompObject(dist);
					m_wave_pass_dist[offset++] = dist;
				}
			}

			AssignBackBuffDist(node);
		}

		m_back_file.OpenWriteFile();
		m_back_file.WriteCompObject(m_back_buff_dist.Buffer(), m_back_buff_dist.Size());
		m_back_file.CloseFile();
	}

	// This is called to create the final distribution for the external
	// nodes for a given hash division.
	void WriteExternalNodes() {

		int offset = 0;
		CMemoryChunk<float> curr_dist(m_class_num);
		CMemoryChunk<float> fin_dist(m_class_num);

		m_node_map.ResetNextObject();
		for(int j=0; j<m_node_map.Size(); j++) {
			S5Byte &node = m_node_map.NextSeqObject();
			m_fin_wave_pass_file.WriteCompObject(node);

			for(int i=0; i<m_class_num; i++) {
				fin_dist[i] = m_wave_pass_dist[offset++];
			}

			CMath::NormalizeVector(fin_dist.Buffer(), m_class_num);

			for(int i=0; i<m_class_num; i++) {
				m_fin_wave_pass_file.WriteCompObject(fin_dist[i]);
			}
		}
	}

public:

	CAccumulateHashDivision() {
	}

	// This initializes the different variables
	// @param hash_div - this is the hash division being processed
	// @param base_node_num - this is the current number of base nodes being processed
	void Initialize(int client_id, int client_num, _int64 base_node_num) {

		CHDFSFile::Initialize();
		CNodeStat::SetClientID(client_id);
		CNodeStat::SetClientNum(client_num);
		CNodeStat::SetInstID(0);

		CUtility::RandomizeSeed();
		m_back_file.SetFileName(CUtility::ExtendString
			("LocalData/back_wave_pass", GetClientID()));

		m_forward_set.SetFileName("LocalData/forward_wave_pass");
		m_forward_set.AllocateFileSet(CNodeStat::GetClientNum());
		m_class_num = 3;

		m_class_weight.AllocateMemory(m_class_num, 0);
		m_wave_pass_dist.Initialize((int)(base_node_num / CNodeStat::GetClientNum()) + 1000);

		m_back_buff_dist.Initialize((int)(base_node_num / CNodeStat::GetClientNum()));
		m_node_map.Initialize(HashCode, Equal);
	}

	// This is called after all link sets have passed their wave pass distribution
	// to their neighbours and it's now time to update the back buffer so the next
	// pass through the link set can begin.
	// @param is_add_external_nodes - this is true when the external nodes are being 
	//							    - added to the set
	void AccumulateHashDivision(bool is_add_external_nodes) {

		CMemoryChunk<float> curr_class_weight(CUtility::ExtendString
			("LocalData/fin_class_weight", GetInstID()));

		AccumulateDistribution();
		WriteBackBuffer(curr_class_weight);

		m_class_weight.WriteMemoryChunkToFile(CUtility::ExtendString
			("LocalData/class_weight", CNodeStat::GetClientID()));

		if(is_add_external_nodes == true) {

			m_fin_wave_pass_file.OpenWriteFile(CUtility::ExtendString
				("LocalData/final_wave_pass", CNodeStat::GetClientID()));

			m_class_weight.InitializeMemoryChunk(0);
			AddMissingBaseNodes();
			WriteExternalNodes();
		}
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);
	int client_num = atoi(argv[2]);
	bool is_add_external = atoi(argv[3]);
	_int64 base_node_num = CANConvert::AlphaToNumericLong(argv[4], strlen(argv[4]));
	
	CBeacon::InitializeBeacon(client_id, 5555);

	CMemoryElement<CAccumulateHashDivision> acc;
	acc->Initialize(client_id, client_num, base_node_num);
	acc->AccumulateHashDivision(is_add_external);

	acc.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();

	return 0;
}