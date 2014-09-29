#include "./TestSortHitList.h"

// define an image hit type attribute
#define IMAGE_TYPE_HIT 0x01
// defines a keyword or description type attribute
#define META_TYPE_HIT 0x02
// defines link anchor type attribute
#define LINK_TYPE_HIT 0x04
// defines a capital word
#define CAPITAL_HIT 0x08
// defines a word that is the first in
// a hyper link text fragment
#define NEW_LINK_TYPE_HIT 0x10
// defines a link with the same
// server as the base url
#define SERVER_SAME 0x20
// defines the start of an image caption
#define NEW_IMAGE_CAPTION 0x40

// used for testing
struct STestHit {
	int pos;
	bool image;
	uChar term_weight;
	bool title_segment;
	bool exclude_word;
	bool link_anchor;
	int image_id;
	// This stores the doc id for anchor text link
	int anchor_doc_id;
};

// This checks the external output of the hitlist against
// the internal hitlist.
class CTestHitList {

	CVector<int> m_word_index;
	CVector<int> m_base_url_index;
	CVector<int> m_link_url_index;
	CVector<STestHit> m_hit_list;
	CVector<int> m_test_link_div;
	CVector<int> m_test_cluster_size;
	CVector<int> m_link_strength;

	// This stores the hit list offset for each document
	CVector<int> m_doc_hit_offset;
	// This stores the url list offset for each document
	CVector<int> m_url_offset;

	// This stores the mapping between external
	// url indexes and internal url indexes
	CTrie m_url_map;
	// This stores the mapping between external
	// url indexes and internal url indexes
	CArrayList<int> m_url_value;

	// This stores the mapping between external
	// url indexes and internal word indexes
	CTrie m_word_map;
	// This stores the mapping between external
	// url indexes and internal word indexes
	CArrayList<int> m_word_value;
	// This stores the number of clients used
	int m_client_num;
	// This stores the total number of words
	uLong m_tot_word_num;

	// This stores the document id for all anchor hits
	CArrayList<int> m_anchor_doc;
	// This stores all the final hits
	CMemoryChunk<CArrayList<SHitItem> > m_fin_hit_list;
	// This stores the title segments for each client
	CMemoryChunk<CHDFSFile> m_title_seg;

	// This creates the mapping used to test word indices
	void CreateWordMapping() {

		CMemoryChunk<CFileComp> hit_list_set(CNodeStat::GetHashDivNum());

		SHitItem hit_item;

		m_word_map.Reset();
		m_word_value.Resize(0);

		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			for(int j=0; j<m_client_num; j++) {
				hit_list_set[i].OpenReadFile(CUtility::ExtendString
					("GlobalData/HitList/base_fin_hit", i, ".client", j));

				CFileComp &hit_file = hit_list_set[i];

				while(hit_item.ReadHitDocOrder(hit_file)) {
					int offset = m_doc_hit_offset[hit_item.doc_id.Value()]++;
					while(m_hit_list[offset].exclude_word) {
						// exclude words are not indexed
						offset = m_doc_hit_offset[hit_item.doc_id.Value()]++;
					}

					hit_item.word_id = hit_item.word_id + (0x1000000 * i);
					int word_id = m_word_index[offset];

					m_word_map.AddWord((char *)&word_id, 4);
					if(!m_word_map.AskFoundWord()) {
						m_word_value.PushBack(hit_item.word_id);
					}
				}
			}
		}

		m_word_map.WriteTrieToFile("../DyableRequest/word_trie");
		m_word_value.WriteArrayListToFile("../DyableRequest/word_map");
	}

	// This is used to create the mapping for url indices
	void CreateURLMapping() {

		for(int i=0; i<m_client_num; i++) {
			CFileComp fin_link_set;
			fin_link_set.OpenReadFile(CUtility::ExtendString
				("GlobalData/LinkSet/fin_link_set", i));

			_int64 src_url_index = 0;
			_int64 dst_url_index = 0;
			uLong cluster_size;

			while(fin_link_set.GetEscapedItem(cluster_size) >= 0) {
				fin_link_set.ReadCompObject((char *)&src_url_index, 5);

				int src_offset = src_url_index;

				if(cluster_size != m_test_cluster_size[src_offset]) {
					cout<<"Cluster Size mismatch "<<src_url_index;getchar();
				}

				m_url_map.AddWord((char *)&src_url_index, 5);
				if(!m_url_map.AskFoundWord()) {
					m_url_value.PushBack(m_base_url_index[src_offset]);
				}

				int dst_offset = m_url_offset[src_offset];

				for(uLong j=0; j<cluster_size; j++) {
					fin_link_set.ReadCompObject((char *)&dst_url_index, 5);
					dst_url_index >>= 2;

					m_url_map.AddWord((char *)&dst_url_index, 5);
					if(!m_url_map.AskFoundWord()) {
						m_url_value.PushBack(m_link_url_index[dst_offset]);
					}

					dst_offset++;
				}
			}
		}
	}

public:

	CTestHitList() {

		_int64 dummy;

		CHDFSFile file;
		file.OpenReadFile("GlobalData/WordDictionary/dictionary_offset");
		file.InitializeCompression();
		// writes the number of base nodes
		file.ReadCompObject(dummy);
		// writes the total number of nodes
		file.ReadCompObject(dummy);
		file.ReadCompObject(m_tot_word_num);

		m_anchor_doc.Initialize(4);

		m_fin_hit_list.AllocateMemory((m_tot_word_num >> 24) + 1);
		for(int i=0; i<m_fin_hit_list.OverflowSize(); i++) {
			m_fin_hit_list[i].Initialize();
		}
	}

	// This loads in all the test dictionaries and test vectors
	void LoadData() {

		m_word_map.Initialize(4);
		m_url_map.Initialize(4);
		m_url_value.Initialize(4);
		m_word_value.Initialize(4);

		m_word_index.ReadVectorFromFile("test_hit_index");
		m_base_url_index.ReadVectorFromFile("test_base_url_index");
		m_link_url_index.ReadVectorFromFile("test_link_url_index");

		m_hit_list.ReadVectorFromFile("test_hit_list");
		m_test_cluster_size.ReadVectorFromFile("test_cluster_size");
		m_link_strength.ReadVectorFromFile("link_strength");

	}

	// This creates the hit offsets for url and word indexes
	void CreateHitOffsets() {

		CVector<int> word_hit_size;
		word_hit_size.ReadVectorFromFile("word_hit_size");

		m_doc_hit_offset.Resize(0);
		m_doc_hit_offset.PushBack(0);

		int offset = 0;
		for(int i=0; i<word_hit_size.Size(); i++) {
			offset += word_hit_size[i];
			m_doc_hit_offset.PushBack(offset);
		}

		m_url_offset.Resize(0);
		m_url_offset.PushBack(0);

		offset = 0;
		for(int i=0; i<m_test_cluster_size.Size(); i++) {
			offset += m_test_cluster_size[i];
			m_url_offset.PushBack(offset);
		}
	}

	// This checks the title segments have been correctly recorded
	void CheckTitleSegments() {

		uChar div;
		uLong cluster_size;
		_int64 doc_set_id = 0;
		S5Byte temp;
		cout<<"Checking Title List"<<endl;

		CreateHitOffsets();

		m_title_seg.AllocateMemory(m_client_num);

		CVector<int> word_hit_size;
		word_hit_size.ReadVectorFromFile("word_hit_size");

		for(int j=0; j<m_client_num; j++) {
			m_title_seg[j].OpenReadFile(CUtility::ExtendString
				("GlobalData/Title/title_hits", j));

			CFileComp curr_link_set;
			curr_link_set.OpenReadFile(CUtility::ExtendString
				("GlobalData/HitList/meta_link_set", j));

			uLong title_word;
			// retrieves each sequential link set size
			while(curr_link_set.Get5ByteCompItem(doc_set_id)) {
				// gets the base url index
				curr_link_set.GetEscapedItem(cluster_size);

				for(uLong i=0; i<cluster_size; i++) {
					curr_link_set.ReadCompObject(temp);
				}

				curr_link_set.GetEscapedItem(cluster_size);

				for(uLong i=0; i<cluster_size; i++) {
					curr_link_set.ReadCompObject(div);
				}

				int count = 0;
				cout<<"Doc Set ID "<<doc_set_id<<endl;
				while(count < word_hit_size[doc_set_id]) {

					int offset = m_doc_hit_offset[doc_set_id]++;
					if(offset >= m_hit_list.Size()) {
						break;
					}
					count++;
					while(m_hit_list[offset].exclude_word) {

						// exclude words are not indexed
						offset = m_doc_hit_offset[doc_set_id]++;
						count++;
						if(offset >= m_hit_list.Size()) {
							break;
						}
					}

					if(count > word_hit_size[doc_set_id]) {
						break;
					}

					if(m_hit_list[offset].title_segment == true) { 
						m_title_seg[j].ReadCompObject(title_word);

						int word_id = m_word_index[offset];
						int id = m_word_map.FindWord((char *)&word_id, 4);

						if(m_word_value[id] != title_word) {
							cout<<"Word Id Mismatch "<<m_word_value[id]<<" "<<
								m_word_index[offset]<<" "<<title_word;getchar();
						}
					}
				}
			}
		}
	}

	// This is the entry function that is used to check that the 
	// hitlist has been compiled correctly by checking against 
	// the precompiled hit list.
	void CheckHitList(int client_num) {

		CMemoryChunk<CFileComp> hit_list_set(CNodeStat::GetHashDivNum());
		cout<<"Checking Hit List"<<endl;
		SHitItem hit_item;
		SWordHit word_hit;

		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			for(int j=0; j<m_client_num; j++) {
				hit_list_set[i].OpenReadFile(CUtility::ExtendString
					("GlobalData/HitList/base_fin_hit", i, ".client", j));
				hit_list_set[i].InitializeCompression();

				CFileComp &hit_file = hit_list_set[i];

				while(hit_item.ReadHitDocOrder(hit_file)) {

					int offset = m_doc_hit_offset[hit_item.doc_id.Value()]++;
					while(m_hit_list[offset].exclude_word) {
						// exclude words are not indexed
						offset = m_doc_hit_offset[hit_item.doc_id.Value()]++;
					}

					m_fin_hit_list[i].PushBack(hit_item);

					if(m_hit_list[offset].link_anchor == true) {
						m_anchor_doc.PushBack(offset);
					}

					int pos = m_hit_list[offset].pos & 0x1FFF;
					if(pos != hit_item.enc >> 3) {
						cout<<"Word Pos Mismatch "<<m_hit_list[offset].pos<<" "<<(hit_item.enc >> 3);getchar();
					}

					if(i > 0) {
						cout<<"new div";getchar();
					}

					hit_item.word_id = hit_item.word_id + (0x1000000 * i);
					int word_id = m_word_index[offset];

					int id = m_word_map.FindWord((char *)&word_id, 4);

					if(hit_item.word_id != m_word_value[id]) {
						cout<<"Word Id Mismatch";getchar();
					}

					int hit_type = 0;
					if(hit_item.IsExcerptHit()) {
						hit_type++;
					}

					if(hit_item.IsTitleHit()) {
						hit_type++;
					}

					if(hit_item.IsImageHit()) {
						hit_type++;
					}

					if(hit_type != 1) {
						cout<<"Cardinality Hit Type Num";getchar();
					}

					word_hit.term_type = m_hit_list[offset].term_weight;
					if(word_hit.AskExcerptHit() != hit_item.IsExcerptHit()) {
						cout<<"Excerpt Mismatch "<<(hit_item.enc & 0x07);getchar();
					}

					if(word_hit.AskImageHit() != hit_item.IsImageHit()) {
						cout<<"Image Mismatch "<<word_hit.AskImageHit()
							<<" "<<hit_item.IsImageHit();getchar();
					}

					bool title_hit = word_hit.AskLinkHit() 
						|| word_hit.AskTitleHit();

					if(title_hit != hit_item.IsTitleHit()) {
						cout<<"Title Mismatch "<<title_hit<<" "<<hit_item.IsTitleHit();getchar();
						cout<<word_hit.AskLinkHit()<<" "
							<<" "<<word_hit.AskTitleHit()<<" "<<(int)word_hit.term_type;getchar();
					}
				}
			}
		}
	}

	// This checks the anchor hits for the correct encoding and
	// document id. Anchor hits are only stored for non spidered webpages
	void CheckAnchorHitList() {

		int curr_hit = 0;
		CMemoryChunk<CFileComp> hit_list_set(CNodeStat::GetHashDivNum());
		cout<<"Checking Anchor Hit List"<<endl;
		SHitItem hit_item;
		SWordHit word_hit;

		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			for(int j=0; j<m_client_num; j++) {
				hit_list_set[i].OpenReadFile(CUtility::ExtendString
					("GlobalData/HitList/anchor_fin_hit", i, ".client", j));

				CFileComp &hit_file = hit_list_set[i];

				while(hit_item.ReadHitDocOrder(hit_file)) {

					m_fin_hit_list[i].PushBack(hit_item);

					int offset = m_anchor_doc[curr_hit++];
					word_hit.term_type = m_hit_list[offset].term_weight;

					int pos = m_hit_list[offset].pos & 0x1FFF;
					if(pos != hit_item.enc >> 3) {
						cout<<"Word Pos Mismatch "<<m_hit_list[offset].pos<<" "<<(hit_item.enc >> 3);getchar();
					}

					int src_id = m_url_map.FindWord((char *)&hit_item.doc_id, 5);
					if(m_url_value[src_id] != m_hit_list[offset].anchor_doc_id) {
						cout<<"Doc ID Mismatch "<<m_url_value[src_id]<<" "<<m_hit_list[offset].anchor_doc_id;getchar();
					}

					hit_item.word_id = hit_item.word_id + (0x1000000 * i);
					int word_id = m_word_index[offset];

					int id = m_word_map.FindWord((char *)&word_id, 4);

					if(hit_item.word_id != m_word_value[id]) {
						cout<<"Word Id Mismatch";getchar();
					}

					if(hit_item.IsTitleHit() == false) {
						cout<<"Not Title Hit";getchar();
					}

					if(word_hit.AskLinkHit() == false) {
						cout<<"Not Link Hit";getchar();
					}

				}
			}
		}
	}

	// This writes the final hit list that is later sorted
	void WriteFinalHitList() {

		CHDFSFile hit_list_file;
		hit_list_file.OpenWriteFile("./hit_list_set");
		hit_list_file.InitializeCompression();
		int size = m_fin_hit_list.OverflowSize();
		hit_list_file.WriteCompObject(size);

		for(int i=0; i<m_fin_hit_list.OverflowSize(); i++) {
			m_fin_hit_list[i].WriteArrayListToFile(hit_list_file);
		}
	}

	// This is the entry function that is used to check the link
	// set that has been produced.
	void CheckLinkSet(int client_num) {

		m_client_num = client_num;

		cout<<"Creating Mapping"<<endl;
		LoadData();
		CreateHitOffsets();
		CreateWordMapping();
		CreateURLMapping();
		CreateHitOffsets();
		cout<<" Testing URL Set"<<endl;

		_int64 src_url_index = 0;
		_int64 dst_url_index = 0;
		uLong cluster_size;

		for(int i=0; i<m_client_num; i++) {
			CFileComp fin_link_set;
			fin_link_set.OpenReadFile(CUtility::ExtendString
				("GlobalData/LinkSet/fin_link_set", i));
			fin_link_set.InitializeCompression();

			while(fin_link_set.GetEscapedItem(cluster_size) >= 0) {

				fin_link_set.ReadCompObject((char *)&src_url_index, 5);
				int src_id = m_url_map.FindWord((char *)&src_url_index, 5);
				if(m_url_value[src_id] != m_base_url_index[src_url_index]) {
					cout<<"Src Mismatch "<<m_url_value[src_id]<<" "<<
						m_base_url_index[src_url_index]<<" "<<src_url_index;getchar();
				}

				if(m_test_cluster_size[src_url_index] != cluster_size) {
					cout<<"Cluster Size Mismatch "<<
						m_test_cluster_size[src_url_index]<<" "<<cluster_size;getchar();
				}

				int dst_offset = m_url_offset[(int)src_url_index];

				for(uLong j=0; j<cluster_size; j++) {
					fin_link_set.ReadCompObject((char *)&dst_url_index, 5);
					uChar link_weight = dst_url_index & 0x03;
					dst_url_index >>= 2;

					int dst_id = m_url_map.FindWord((char *)&dst_url_index, 5);

					if(m_url_value[dst_id] != m_link_url_index[dst_offset]) {
						cout<<"Src "<<src_url_index<<endl;
						cout<<"Dst Mismatch "<<m_url_value[dst_id]<<" "<<
							m_link_url_index[dst_offset]<<" "<<src_url_index;getchar();
					}

					if(m_link_strength[dst_offset++] != link_weight) {
						cout<<"Link Weight Mismatch "<<m_link_strength[dst_offset-1]<<" "<<(int)link_weight;getchar();
					}
				}

				if(m_test_cluster_size[src_url_index] != cluster_size) {
					dst_offset -= cluster_size - m_test_cluster_size[src_url_index];
				}
			}
		}
	}
};