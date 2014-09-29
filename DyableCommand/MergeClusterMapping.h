#include "./TestIndexing.h"

// This stores the directory where the mapping between 
// base nodes and their cluster mapping is stored
const char *FORWARD_CLUS_MAP_DIR = "GlobalData/ClusterHiearchy/forward_clus_map";

// This coordinates the different clients that are used
// to merge nodes together based on stationary distribution
// of each node. This is calculated in the pulse rank component.
// The server is primarily responsible for coordinating the 
// different clients so that they can work in unisounce.
class CMergeClusterMapping : public CNodeStat {

	// This returns a bucket that is being merged 
	struct SBucket {
		// This stores the bucket id
		int bucket_id;
		// This stores the map file
		SClusterMap map;
	};

	// This stores the sorted doc id cluster mappings for each
	// client these need to be merged together
	CMemoryChunk<CFileComp> m_sorted_set;
	// This stores the priority queue used to merged doc id cluster mappings
	CPriorityQueue<SBucket> m_merge_queue;
	// This stores the final sorted cluster map file
	CHDFSFile m_clus_map_file;

	// This is used to merge doc id cluster mappings
	static int CompareDocIDClusterMap(const SBucket &arg1, const SBucket &arg2) {

		_int64 src1 = S5Byte::Value(arg1.map.base_node);
		_int64 src2 = S5Byte::Value(arg2.map.base_node);

		if(src1 < src2) {
			return 1;
		}

		if(src1 > src2) {
			return -1;
		}

		return 0;
	}

	// This sorts each of the cluster node map buckets seperately
	// and is later merged with all the corresponding client buckets.
	void MergeDocIDClusterMap() {

		SBucket bucket;
		while(m_merge_queue.Size() > 0) {
			m_merge_queue.PopItem(bucket);
			bucket.map.WriteClusterMap(m_clus_map_file);

			int id = bucket.bucket_id;
			if(bucket.map.ReadClusterMap(m_sorted_set[id])) {
				m_merge_queue.AddItem(bucket);
			}
		}
	}

	// This starts of the merging processing by adding the first cluster mapping
	// from each client to the priority queue. This cluster mapping is then 
	// successively removed in sorted order.
	// @param dir - this is the director of the mapping that needs to be merged
	void SeedDocIDClusterMapping(const char dir[]) {

		m_merge_queue.Initialize(GetClientNum(), CompareDocIDClusterMap);

		m_sorted_set.AllocateMemory(GetClientNum());

		for(int i=0; i<GetClientNum(); i++) {
			m_sorted_set[i].OpenReadFile(CUtility::ExtendString(dir, i));
		}

		SBucket bucket;
		for(int i=0; i<GetClientNum(); i++) {
			bucket.bucket_id = i;
			bucket.map.ReadClusterMap(m_sorted_set[i]);
			m_merge_queue.AddItem(bucket);
		}

		MergeDocIDClusterMap();
	}

public:

	CMergeClusterMapping() {
	}

	// This is the entry function that merges cluster mappings
	// @param is_keyword - true when the keyword set being parsed
	void MergeClusterMapping(int client_num) {

		CNodeStat::SetClientNum(client_num);
		m_clus_map_file.OpenWriteFile(CUtility::ExtendString
			(FORWARD_CLUS_MAP_DIR, ".fin"));

		cout<<"Creating DocID base mapping"<<endl;
		SeedDocIDClusterMapping(FORWARD_CLUS_MAP_DIR);
		m_clus_map_file.CloseFile();
	}

};