#include "./TestCompileLookupIndex.h"

// This stores the directory where the mapping between 
// base nodes and their cluster mapping is stored
const char *BACKWARD_CLUS_MAP_DIR = "GlobalData/ClusterHiearchy/backward_clus_map";

// This class is used to test that all sort items have been sorted correctly
class CTestSortHitList : public CNodeStat {

	// This stores the list of hits that need to be sorted
	CMemoryChunk<CArrayList<SHitItem> > m_hit_list;
	// This stores the mapping between doc id's and cluster id's 
	CArrayList<int> m_doc_clus_map;
	// This stores the 8-bit pulse score for each doc id
	CMemoryChunk<uChar> m_pulse_map;

	// This is used to compare hit items
	static int CompareHits(const SHitItem &arg1, const SHitItem &arg2) {

		if(arg1.word_id < arg2.word_id) {
			return 1;
		}

		if(arg1.word_id > arg2.word_id) {
			return -1;
		}

		_int64 doc1 = S5Byte::Value(arg1.doc_id);
		_int64 doc2 = S5Byte::Value(arg2.doc_id);

		if(doc1 < doc2) {
			return 1;
		}

		if(doc1 > doc2) {
			return -1;
		}

		if(arg1.enc < arg2.enc) {
			return 1;
		}

		if(arg1.enc > arg2.enc) {
			return -1;
		}

		return 0;
	}

	// This loads the doc id mapping created during the clustering process
	void LoadDocClusMapping() {

		SClusterMap clus_map;

		CFileComp backward_map_file;
		m_doc_clus_map.Initialize(4);

		for(int i=0; i<GetClientNum(); i++) {
			backward_map_file.OpenReadFile(CUtility::ExtendString
				(BACKWARD_CLUS_MAP_DIR, i));
			backward_map_file.InitializeCompression();

			while(clus_map.ReadClusterMap(backward_map_file)) {
				m_doc_clus_map.ExtendSize(1);
			}
		}

		for(int i=0; i<GetClientNum(); i++) {
			backward_map_file.OpenReadFile(CUtility::ExtendString
				(BACKWARD_CLUS_MAP_DIR, i));
			backward_map_file.InitializeCompression();

			while(clus_map.ReadClusterMap(backward_map_file)) {
				m_doc_clus_map[clus_map.base_node.Value()] = clus_map.cluster.Value();
			}
		}

		CMemoryChunk<int> reverse_map(m_doc_clus_map.Size());

		for(int i=0; i<m_doc_clus_map.Size(); i++) {
			reverse_map[m_doc_clus_map[i]] = i;
		}

		reverse_map.WriteMemoryChunkToFile("../DyableRequest/reverse_doc_map");
	}

	// This loads the hit list from the indexing stage
	void LoadHitList() {

		LoadDocClusMapping();

		int breadth;
		CHDFSFile hit_list_file;
		hit_list_file.OpenReadFile("./hit_list_set");
		hit_list_file.InitializeCompression();
		hit_list_file.ReadCompObject(breadth);
		m_hit_list.AllocateMemory(breadth);

		for(int i=0; i<m_hit_list.OverflowSize(); i++) {
			m_hit_list[i].ReadArrayListFromFile(hit_list_file);

			for(int j=0; j<m_hit_list[i].Size(); j++) {	
				m_hit_list[i][j].doc_id = m_doc_clus_map[m_hit_list[i][j].doc_id.Value()];
			}

			CSort<SHitItem> sort(m_hit_list[i].Size(), CompareHits);
			sort.HybridSort(m_hit_list[i].Buffer());
		}
	}

public:

	CTestSortHitList() {

	}

	// This test the sorted hit list against the internal sorted hit list
	void TestSortHitList() {

		LoadHitList();

		cout<<"Checking Sorted Hits"<<endl;

		SHitItem hit_item;
		CFileComp cluster_hit_file;
		CFileComp image_hit_file;
		for(int i=0; i<m_hit_list.OverflowSize(); i++) {
			cluster_hit_file.OpenReadFile(CUtility::ExtendString
				("GlobalData/SortedHits/sorted_base_hits", i));
			cluster_hit_file.InitializeCompression();

			image_hit_file.OpenReadFile(CUtility::ExtendString
				("GlobalData/SortedHits/sorted_image_hits", i));
			image_hit_file.InitializeCompression();

			for(int j=0; j<m_hit_list[i].Size(); j++) {

				if(m_hit_list[i][j].IsImageHit()) {
					hit_item.ReadHitWordOrder(image_hit_file);
				} else {
					hit_item.ReadHitWordOrder(cluster_hit_file);
				}

				if(hit_item.word_id != m_hit_list[i][j].word_id) {
					cout<<"Word ID Mismatch "<<hit_item.word_id<<" "<<m_hit_list[i][j].word_id;getchar();
				}

				if(hit_item.doc_id != m_hit_list[i][j].doc_id) {
					cout<<"Doc ID Mismatch "<<hit_item.doc_id.Value()<<" "<<m_hit_list[i][j].doc_id.Value();getchar();
				}

				if(hit_item.enc != m_hit_list[i][j].enc) {
					cout<<"Enc ID Mismatch";getchar();
				}

			}
		}

		CHDFSFile hit_list_file;
		hit_list_file.OpenWriteFile("./hit_list_set");
		hit_list_file.InitializeCompression();
		int size = m_hit_list.OverflowSize();
		hit_list_file.WriteCompObject(size);

		for(int i=0; i<m_hit_list.OverflowSize(); i++) {
			m_hit_list[i].WriteArrayListToFile(hit_list_file);
		}

	}
};