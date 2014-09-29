#include "../../../../MapReduce.h"

// This class takes the node cluster mapping stored in the 
// cluster stat file and hashes each cluster hiearchy to 
// it's respective division. The cluster stat file is also hashed
// based upon the cluster
class CDistributeClusterHiearchies : public CNodeStat {

	// This stores the mapping between a base node and it's
	// cluster assignment -> this is continuously remapped
	CHDFSFile m_hiearchy_file;
	// This stores statistics relating ot a hiearchy
	CSegFile m_clus_stat_file;
	// This stores the orphan node set for this hash division
	CHDFSFile m_neighbour_node_file;

	// This stores the set of distributed cluster hiearchies
	CFileSet<CHDFSFile> m_hiearchy_set;
	// This stores the set of distributed cluster maps
	CFileSet<CHDFSFile> m_cluster_stat_set;

	// This stores the local number of clusters
	_int64 m_cluster_num;
	// This stores the local number of nodes in each hash division
	CMemoryChunk<_int64> m_node_num;
	// This stores the set of orphan nodes
	CObjectHashMap<S5Byte> m_orphan_map;
	// This is the maximum number of nodes allowed in one cluster
	_int64 m_max_node_num;

	// This retrieves a cluster hiearchy from file and writes it
	// out to a new file. This is so like cluster hiearchies can
	// be grouped together.
	// @return the total number of nodes in the current hiearchy
	int TransferHiearchy(CHDFSFile &from_file, CHDFSFile &to_file, SHiearchyStat &stat) {

		SHiearchyStat::WriteHiearchyStat(to_file, stat);

		for(int i=0; i<stat.total_node_num; i++) {
			// This retrieves the base node id for a
			// grouped node under this cluster segment 
			from_file.ReadCompObject(stat.clus_id);
			to_file.WriteCompObject(stat.clus_id);
		}

		static SSubTree tree;
		for(int i=0; i<stat.total_subtrees; i++) {
			// this transfers a subtree from one file to another
			SSubTree::ReadSubTree(from_file, tree);
			SSubTree::WriteSubTree(to_file, tree);
		}

		return stat.total_node_num;
	}

	// This writes the cluster mapping and transfers the cluster hiearchy
	// @param stat - the current hiearchy stat
	// @param clus_map - the cluster mapping for this hiearchy
	void CreateHiearchy(SHiearchyStat &stat, SClusterMap &clus_map) {

		uChar is_orphan_node = m_orphan_map.Get(clus_map.base_node) < 0;
		int hash = clus_map.cluster.Value() % CNodeStat::GetClientNum();

		CHDFSFile &clus_stat_file = m_cluster_stat_set.File(hash);
		clus_map.WriteClusterMap(clus_stat_file);
		clus_stat_file.WriteCompObject(is_orphan_node);

		if(is_orphan_node == true) {
			SHiearchyStat::WriteHiearchyStat(clus_stat_file, stat);
		}

		m_node_num[hash] += TransferHiearchy(m_hiearchy_file,
			m_hiearchy_set.File(hash), stat);
	}

	// This distributes the cluster hiearchies to each respective hash division
	// Note: clus_map.cluster |= 0x8000000000; ensures that the new cluster mapping
	// doesn't clash with the existing cluster mapping, that is each new cluster 
	// is assigned a unique and arbitrary index.
	void DistributeClusterHiearchies() {

		int node_num;
		uChar map_bytes;
		SClusterMap clus_map;
		SHiearchyStat stat;

		static _int64 max_value = 0;
		*((uChar *)&max_value + 4) = 0x80;
		m_node_num.AllocateMemory(CNodeStat::GetClientNum(), 0);
		while(m_clus_stat_file.ReadCompObject(map_bytes)) {
			SHiearchyStat::ReadHiearchyStat(m_hiearchy_file, stat);

			if(map_bytes == 0) {
				m_clus_stat_file.ReadCompObject(clus_map.base_node);
				clus_map.cluster = m_cluster_num;
				clus_map.cluster |= max_value;
				m_cluster_num += CNodeStat::GetClientNum();
				if(clus_map.cluster < max_value) {
					cout<<"Cluster und";getchar();
				}
			} else {
				clus_map.ReadClusterMap(m_clus_stat_file);
				if(clus_map.cluster >= max_value) {
					cout<<"Cluster ovf";getchar();
				}

				if(stat.total_node_num > m_max_node_num) {
					// break the join if the current number of 
					// nodes in the cluster being joined exceeds
					// the maximum allowed number
					clus_map.cluster = m_cluster_num;
					clus_map.cluster |= max_value;
					m_cluster_num += CNodeStat::GetClientNum();
				}
			}

			CreateHiearchy(stat, clus_map);
		}
	}

	// This returns one of the partitioned directories
	char *PartitionedDir(const char dir[]) {
		strcpy(CUtility::SecondTempBuffer(), CUtility::ExtendString
			(dir, CNodeStat::GetInstID(), ".client"));
		return CUtility::SecondTempBuffer();
	}

	// This loads the set of neighbour nodes
	void LoadNeighbourNodes() {

		S5Byte node;
		for(int i=0; i<CNodeStat::GetClientNum(); i++) {
			m_neighbour_node_file.OpenReadFile(CUtility::ExtendString
				("LocalData/neighbour_node_set", CNodeStat::GetInstID(),
				".set", i, ".set", GetClientID()));

			while(m_neighbour_node_file.ReadCompObject(node)) {
				m_orphan_map.Put(node);
			}
		}
	}	

	// This defines a function that computes the hash map for an object
	static int HashCode(const S5Byte &item) {
		return (long)S5Byte::Value(item);
	}

	// This defines a function to compare two nodes for equality
	static bool Equal(const S5Byte &arg1, const S5Byte &arg2) {
		return S5Byte::Value(arg1) == S5Byte::Value(arg2);
	}

public:

	CDistributeClusterHiearchies() {
		CHDFSFile::Initialize();
	}

	// This is the entry function
	// @param max_clus_node_num - this constant defines the maximum number
	//                          - of nodes allowed in one cluster
	void DistributeClusterHiearchies(int client_id, int client_num,
		int inst_id, int max_clus_node_num) {

		CNodeStat::SetClientID(client_id);
		CNodeStat::SetClientNum(client_num);
		CNodeStat::SetInstID(inst_id);
		m_cluster_num = CNodeStat::GetClientID();
		m_max_node_num = max_clus_node_num;

		m_orphan_map.Initialize(HashCode, Equal, 2000000);
		LoadNeighbourNodes();

		m_cluster_stat_set.OpenWriteFileSet(PartitionedDir("LocalData/cluster_map"),
			CNodeStat::GetClientNum(), CNodeStat::GetClientID());

		m_hiearchy_set.OpenWriteFileSet(PartitionedDir("LocalData/hiearchy"), 
			CNodeStat::GetClientNum(), CNodeStat::GetClientID());

		m_hiearchy_file.OpenReadFile(CUtility::
			ExtendString(CLUSTER_HIEARCHY_DIR, GetInstID(),
			".set", CNodeStat::GetClientID()));

		m_clus_stat_file.OpenReadFile(CUtility::
			ExtendString("LocalData/clus_stat_file", GetInstID(),
			".set", CNodeStat::GetClientID()));

		DistributeClusterHiearchies();

		m_node_num.WriteMemoryChunkToFile(CUtility::ExtendString
			("LocalData/node_num", CNodeStat::GetInstID(),
			".set", CNodeStat::GetClientID()));
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);
	int client_num = atoi(argv[2]);
	int inst_id = atoi(argv[3]);
	int max_clus_node_num = atoi(argv[4]);

	CBeacon::InitializeBeacon(client_id, 5555 + inst_id);
	CMemoryElement<CDistributeClusterHiearchies> set;
	set->DistributeClusterHiearchies(client_id, client_num,
		inst_id, max_clus_node_num);
	set.DeleteMemoryElement();

	CBeacon::SendTerminationSignal();
}
