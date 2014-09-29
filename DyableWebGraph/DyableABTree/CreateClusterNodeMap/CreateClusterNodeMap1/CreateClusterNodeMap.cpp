#include "../../../../MapReduce.h"

// This stores the directory where the mapping between 
// base nodes and their cluster mapping is stored
const char *FORWARD_CLUS_MAP_DIR = "GlobalData/ClusterHiearchy/forward_clus_map";
// This stores the directory where the mapping between 
// base nodes and their cluster mapping is stored
const char *BACKWARD_CLUS_MAP_DIR = "GlobalData/ClusterHiearchy/backward_clus_map";

// This class creates a set of cluster node mappings
// for a given web graph instance and sorts the mapping
// so it can be merged with the hit list.
class CCreateClusterNodeMap : public CNodeStat {

	// This stores the forward mapping
	CFileComp m_forward_map_file;
	// This stores the backward mapping
	CSegFile m_backward_map_file;
	// This stores the base level node set for each
	// hiearchy this is used to map excerpt keywords
	CSegFile m_base_node_file;

	// This creates the doc id cluster mapping used to map cut links before
	// the s_links can be compiled.
	// @param client_node_offset - this stores the node bound offset for this client
	void CreateDocIDClusterMap(_int64 &client_node_offset) {

		SSubTree subtree;
		SClusterMap clus_map;
		SHiearchyStat stat;

		CHDFSFile hiearchy_file;
		hiearchy_file.OpenReadFile(CUtility::ExtendString
			(CLUSTER_HIEARCHY_DIR, CNodeStat::GetInstID(),
			".set", CNodeStat::GetClientID()));
		
		while(stat.ReadHiearchyStat(hiearchy_file)) {
			for(int i=0; i<stat.total_node_num; i++) {
				hiearchy_file.ReadCompObject(clus_map.base_node);
				clus_map.cluster = client_node_offset++;

				m_base_node_file.WriteCompObject(clus_map.base_node);
				clus_map.WriteClusterMap(m_forward_map_file);
				clus_map.WriteClusterMap(m_backward_map_file);
			}

			for(int i=0; i<stat.total_subtrees; i++) {
				subtree.ReadSubTree(hiearchy_file);
			}
		}
	}

public:

	CCreateClusterNodeMap() {
		CHDFSFile::Initialize();
	}

	// This is the entry function
	// @param hash_div_num - the number of hash divisions that
	//                     - compose the hiearchy
	// @param client_node_start - this is the node start for this hash division
	// @param client_node_end - this is the node end for this hash division
	void CreateClusterNodeMap(int client_id, _int64 client_node_start, _int64 client_node_end) {

		CNodeStat::SetClientID(client_id);
		m_forward_map_file.OpenWriteFile(CUtility::ExtendString
			(FORWARD_CLUS_MAP_DIR, CNodeStat::GetClientID()));
		m_backward_map_file.OpenWriteFile(CUtility::ExtendString
			(BACKWARD_CLUS_MAP_DIR, CNodeStat::GetClientID()));

		m_base_node_file.OpenWriteFile(CUtility::ExtendString
			("LocalData/node_set", CNodeStat::GetClientID()));

		CreateDocIDClusterMap(client_node_start);

		if(client_node_start != client_node_end) {
			cout<<"End Bound Error "<<client_node_start<<" "<<client_node_end;getchar();
		}

		m_forward_map_file.CloseFile();
		// this sorts the forward mapping by base node
		CExternalRadixSort sort(10, m_forward_map_file.CompBuffer(), 5);
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);
	_int64 client_node_start = CANConvert::AlphaToNumericLong
		(argv[2], strlen(argv[2]));
	_int64 client_node_end = CANConvert::AlphaToNumericLong
		(argv[3], strlen(argv[3]));

	CBeacon::InitializeBeacon(client_id);
	CMemoryElement<CCreateClusterNodeMap> map;
	map->CreateClusterNodeMap(client_id, client_node_start, client_node_end);
	map.DeleteMemoryElement();

	CBeacon::SendTerminationSignal();
}

