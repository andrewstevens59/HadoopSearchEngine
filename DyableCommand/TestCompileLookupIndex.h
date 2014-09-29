#include "../ProcessSet.h"

// This class is used to test that the final index has been compile correctly
class CTestCompileLookupIndex {

	// This defines the maximum number of bytes before a new 
	// spatial boundary is recorded
	static const int MAX_SPAT_NUM = 0xFF;

	struct SOffset {
		CMemoryChunk<int> hit_type;

		SOffset() {
			hit_type.AllocateMemory(3);
			for(int i=0; i<3; i++) {
				hit_type[i] = 0;
			}
		}
	};

	// This stores the list of hits that need to be sorted
	CMemoryChunk<CArrayList<SHitItem> > m_hit_list;
	// This stores the node boundary for different client's 
	// this allows s_links to be grouped to a client
	CMemoryChunk<_int64> m_client_boundary;
	// This stores the external hit list
	CMemoryChunk<CFileComp> m_ext_hit_list;
	// This stores the word division offset for each word div
	CMemoryChunk<CArrayList<int> > m_word_div_start;

	// This loads in the sorted hit list
	void LoadSortedHitList() {

		CHDFSFile hit_list_file;
		hit_list_file.OpenReadFile("./hit_list_set");
		hit_list_file.InitializeCompression();
		int breadth;
		hit_list_file.ReadCompObject(breadth);
		m_hit_list.AllocateMemory(breadth);

		m_word_div_start.AllocateMemory(breadth);

		for(int i=0; i<m_hit_list.OverflowSize(); i++) {
			m_hit_list[i].ReadArrayListFromFile(hit_list_file);
			m_word_div_start[i].Initialize();
		}

		m_client_boundary.ReadMemoryChunkFromFile
			("GlobalData/ClusterHiearchy/cluster_boundary");
	}

	// This checks two hit items to see if they match
	void IsMatch(SHitItem &hit1, SHitItem &hit2) {

		if(hit1.doc_id != hit2.doc_id) {
			cout<<"Doc ID Mismatch "<<hit1.doc_id.Value()<<" "<<hit2.doc_id.Value();getchar();
		}

		hit1.enc &= 0xFF;
		hit2.enc >>= 3;
		hit2.enc &= 0xFF;

		if(hit1.enc != hit2.enc) {
			cout<<"Enc ID Mismatch";getchar();
		}
	}

	// This reads in the appropriate hit from external memory depending 
	// upon the internal hit type.
	void ReadExternalHit(SOffset &hit_offset, SHitItem &curr_hit) {

		SHitItem hit_item;
		if(curr_hit.IsImageHit()) {
			hit_offset.hit_type[IMAGE_HIT_INDEX] += 11;
			m_ext_hit_list[IMAGE_HIT_INDEX].ReadCompObject(hit_item.image_id);
			hit_item.ReadHitExcWordID(m_ext_hit_list[IMAGE_HIT_INDEX]);
		} else if(curr_hit.IsTitleHit()) {
			hit_offset.hit_type[TITLE_HIT_INDEX] += 6;
			hit_item.ReadHitExcWordID(m_ext_hit_list[TITLE_HIT_INDEX]);
		} else if(curr_hit.IsExcerptHit()) {
			hit_offset.hit_type[EXCERPT_HIT_INDEX] += 6;
			hit_item.ReadHitExcWordID(m_ext_hit_list[EXCERPT_HIT_INDEX]);
		} 

		IsMatch(hit_item, curr_hit);
	}

	// This is called whenever a new word div is entered into.
	void BeginNewWordDiv(SHitItem &curr_hit, int &curr_word_div, 
		SLookupIndex &lookup_pos, int &doc_num, int hit_index, 
		SOffset &hit_offset, CFileComp &lookup_file, int word_div) {

		doc_num = 0;
		while(curr_word_div < (int)curr_hit.word_id) {
			curr_word_div++;
			m_word_div_start[word_div].PushBack(hit_index);
			lookup_pos.ReadLookupIndex(lookup_file.CompBuffer());
			for(int k=0; k<3; k++) {
				if(lookup_pos.client_offset.hit_pos[k] != 
					hit_offset.hit_type[k]) {

					cout<<"Offset Mismatch "<<lookup_pos.client_offset.hit_pos[k]
						<<" "<<hit_offset.hit_type[k]<<" "<<k<<"   "<<curr_word_div;getchar();
				}
			}
		}
	}

	// This cycles through all the hits in a given word div
	void CycleThroughHitList(SOffset &hit_offset, 
		int word_div, CFileComp &lookup_file) {

		int curr_word_div = -1;
		SLookupIndex lookup_pos;

		int doc_num = 0;
		_int64 curr_doc_id = -1;
		for(int j=0; j<m_hit_list[word_div].Size(); j++) {
			int hit_type_id = m_hit_list[word_div][j].HitTypeIndex();

			if(m_hit_list[word_div][j].word_id != curr_word_div) {
				BeginNewWordDiv(m_hit_list[word_div][j], curr_word_div, 
					lookup_pos, doc_num, j, hit_offset, lookup_file, word_div);

				curr_doc_id = -1;
			}

			if(m_hit_list[word_div][j].doc_id != curr_doc_id) {
				curr_doc_id = m_hit_list[word_div][j].doc_id.Value();
				doc_num++;
			}
			
			ReadExternalHit(hit_offset, m_hit_list[word_div][j]);
		}

		m_word_div_start[word_div].PushBack(m_hit_list[word_div].Size());
	}

	// This writes the final lookup index - used for testing
	void WriteLookupIndex() {

		CHDFSFile hit_list_file;
		hit_list_file.OpenWriteFile("../DyableRequest/hit_list_set");
		hit_list_file.InitializeCompression();
		int size = m_hit_list.OverflowSize();
		hit_list_file.WriteCompObject(size);

		for(int i=0; i<m_hit_list.OverflowSize(); i++) {
			m_hit_list[i].WriteArrayListToFile(hit_list_file);
			m_word_div_start[i].WriteArrayListToFile(hit_list_file);
		}
	}

public:

	// This checks the external lookup index against the internal one for consistency
	void TestCompileLookupIndex() {
		cout<<"Testing Compile Lookup Index"<<endl;

		LoadSortedHitList();

		SHitItem hit_item;
		for(int i=0; i<m_hit_list.OverflowSize(); i++) {
			SOffset hit_offset;
			m_ext_hit_list.AllocateMemory(4);

			CFileComp lookup_file;
			lookup_file.OpenReadFile(CUtility::ExtendString
				("GlobalData/Retrieve/lookup", i));
			lookup_file.InitializeCompression();

			m_ext_hit_list[TITLE_HIT_INDEX].OpenReadFile
				(CUtility::ExtendString("GlobalData/Retrieve/title_hit", i));
			m_ext_hit_list[TITLE_HIT_INDEX].InitializeCompression();

			m_ext_hit_list[EXCERPT_HIT_INDEX].OpenReadFile
				(CUtility::ExtendString("GlobalData/Retrieve/excerpt_hit", i));
			m_ext_hit_list[EXCERPT_HIT_INDEX].InitializeCompression();

			m_ext_hit_list[IMAGE_HIT_INDEX].OpenReadFile
				(CUtility::ExtendString("GlobalData/Retrieve/image_hit", i));
			m_ext_hit_list[IMAGE_HIT_INDEX].InitializeCompression();

			CycleThroughHitList(hit_offset, i, lookup_file);
		}

		WriteLookupIndex();
	}
};