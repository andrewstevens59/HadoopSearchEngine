#include "../../../../MapReduce.h"

// This class finds the set of maximum links that are attatched to a 
// given node. That is the maximum back link for each node is found
// and all the other back links for that node are disregarded.
class CFindMaximumLinks {

	// This stores one of the dst nodes
	struct SDstNode {
		// This is the dst node
		S5Byte dst_node;
		// This is the link weight
		float link_weight;
	};

	// This maps global dst indexes to local indexes 
	CObjectHashMap<S5Byte> m_node_map;
	// This stores the cluster label for a given node
	CArrayList<float> m_max_weight_buff;
	// This stores the set of dst nodes
	CArrayList<SDstNode> m_dst_node_buff;
	// This stores the number of nodes in each division
	CMemoryChunk<int> m_node_num;

	// This stores all the forward cluster labels
	CFileSet<CHDFSFile> m_forward_set;

	// This is an equality handle used to compare two 5-byte numbers
	static bool Equal(const S5Byte &arg1, const S5Byte &arg2) {
		return S5Byte::Value(arg1) == S5Byte::Value(arg2);
	}

	// This returns a hash code for a 5-byte number 
	static int HashCode(const S5Byte &node) {
		return (int)S5Byte::Value(node);
	}

	// This finds the maximum link for a given node based upon
	// the forward link weight.
	void AssignMaxLink() {

		SDstNode dst_node;
		for(int i=0; i<CNodeStat::GetClientNum(); i++) {
			m_forward_set.OpenReadFile(CNodeStat::GetClientID(), i);

			CHDFSFile &file = m_forward_set.File(CNodeStat::GetClientID());
			while(file.ReadCompObject(dst_node.dst_node)) {
				file.ReadCompObject(dst_node.link_weight);
				int id = m_node_map.Put(dst_node.dst_node);
				m_node_num[i]++;

				if((dst_node.dst_node.Value() % CNodeStat::GetClientNum()) != CNodeStat::GetClientID()) {
					cout<<"div mis2";getchar();
				}

				if(m_node_map.AskFoundWord() == false) {
					m_max_weight_buff.PushBack(0);
				}

				m_dst_node_buff.PushBack(dst_node);
				if(dst_node.link_weight > m_max_weight_buff[id]) {
					m_max_weight_buff[id] = dst_node.link_weight;
				}
			}
		}
	}

	// This creates the maximum link weight set
	void CreateMaxLinkSet() {

		int offset = 0;
		for(int i=0; i<CNodeStat::GetClientNum(); i++) {
			m_forward_set.OpenWriteFile(CNodeStat::GetClientID(), i);

			CHDFSFile &file = m_forward_set.File(CNodeStat::GetClientID());
			for(int j=0; j<m_node_num[i]; j++) {

				SDstNode &dst_node = m_dst_node_buff[offset++];
				int id = m_node_map.Get(dst_node.dst_node);
				if(dst_node.link_weight >= m_max_weight_buff[id]) {
					m_max_weight_buff[id] = 100;
				} else {
					dst_node.link_weight = -dst_node.link_weight;
				}

				file.WriteCompObject(dst_node.dst_node);
				file.WriteCompObject(dst_node.link_weight);
			}
		}

	}

public:

	CFindMaximumLinks() {
	}

	// This is the entry function
	void FindMaximumLinks(int client_id, int client_num) {
		CNodeStat::SetClientID(client_id);
		CNodeStat::SetClientNum(client_num);

		m_forward_set.SetFileName("LocalData/forward_wave_pass");
		m_forward_set.AllocateFileSet(CNodeStat::GetClientNum());

		m_node_map.Initialize(HashCode, Equal);
		m_max_weight_buff.Initialize(1024);
		m_dst_node_buff.Initialize(1024);
		m_node_num.AllocateMemory(CNodeStat::GetClientNum(), 0);

		AssignMaxLink();
		CreateMaxLinkSet();
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);
	int client_num = atoi(argv[2]);

	CBeacon::InitializeBeacon(client_id, 5555);
	CMemoryElement<CFindMaximumLinks> acc;
	acc->FindMaximumLinks(client_id, client_num);

	acc.DeleteMemoryElement();

	CBeacon::SendTerminationSignal();

	return 0;
}