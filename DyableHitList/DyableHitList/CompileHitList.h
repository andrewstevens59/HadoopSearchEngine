#include "./CreateFinalHitList.h"

// This is the directory of the keyword text strings
const char *WORD_TEXT_STRING_FILE = "GlobalData/WordDictionary/word_map";

// This class is responsible for compiling the final hitlist structure.
// This is done by sequentially reading each of the dictionary indexes 
// from the log files for the division given in the intermediate log file. 
// Also the local indexe stored in each of the log index files must be 
// converted into global indexes by adding the dictionary offset to the 
// start of the local index.
class CCompileHitList : public CCreateFinalHitList {

	// This stores the intermediate hit list 
	CHDFSFile m_curr_hit_list;
	// This stores the current image number
	_int64 m_curr_image_num;
	// This stores the word occurrence threshold,
	// terms with an occurrence greater than the
	// threshold are culled from the excerpt keywords
	uLong m_word_occur_thresh;
	// This stores the hit list breadth
	int m_hit_list_breadth;
	// This stores the number of excerpts terms in a document
	uLong m_excerpt_term_num;
	// This is a predicate indicating whether the title 
	// of the document has been parsed or not
	bool m_is_doc_title_parsed;

	// Used to store the base hits that have an
	// associated cluster hiearchy (low occuring words)
	CMemoryChunk<CHDFSFile> m_base_hit_set;
	// Used to store the anchor hits that have an
	// associated cluster hiearchy (low occuring words)
	CMemoryChunk<CFileComp> m_anchor_hit_set;

	// This stores the list of word ids that compose each excerpt
	CSegFile m_excerpt_id_set_file;
	// This stores the hit encoding for eac word id that composes an excerpt
	CHDFSFile m_hit_encoding_file;
	// This stores the number of word ids that compose each document
	CHDFSFile m_doc_size_file;
	// This stores all the title segments that are later processed
	// to find high occuring title segments
	CHDFSFile m_title_segment;
	// This is used to stem any existing words in the lexon that 
	// have been stemed during the previous stemming process
	CLexon m_lexon;

	// This is used for testing 
	CTrie m_test_dict;
	// This is used for testing 
	CTrie m_test_word_id_map;

	// This loads the test dictionary
	void LoadTestDictionary() {

		CHDFSFile word_set_file;
		word_set_file.OpenReadFile(CUtility::ExtendString
			(WORD_TEXT_STRING_FILE, (int)0));

		uLong word_id;
		uChar length;
		m_test_dict.Initialize(4);
		m_test_word_id_map.Initialize(4);
		while(word_set_file.ReadCompObject(word_id)) {
			word_set_file.ReadCompObject(length);
			word_set_file.ReadCompObject(CUtility::SecondTempBuffer(), length);
			m_test_dict.AddWord(CUtility::SecondTempBuffer(), length);
			m_test_word_id_map.AddWord((char *)&word_id, sizeof(uLong));
		}
	}

	// Retrieves the next word index either directly as it
	// is the case when a stop word has been indexed or
	// by getting the index of the log file stack
	// @param word_hit - this is the current hit being processed
	// @param word_index - this is the word index for the next word
	// @return the word occurrence
	inline uLong GetWordIndex(SWordHit &word_hit, uLong &word_index) {

		if(word_hit.AskStopHitType()) {
			// stop word don't need an index
			m_curr_hit_list.GetEscapedItem(word_index);
			return m_lexon.WordOccurrence(word_index);
		} 

		// retrieves the word id
		m_curr_hit_list.ReadCompObject(word_hit.word_div);
		CCreateFinalHitList::RetrieveNextWordIndex(word_hit.word_div, word_index);

		word_index = CCreateFinalHitList::GlobalWordIndex
			(word_index, word_hit.word_div);

		return 0;
	}

	// This checks if a particular term is an exclude word in which
	// case it is not indexed. If it is an exclude word the first term
	// type needs to be checked for consistency and correct indexing.
	// @param word_hit - this is the current hit being processed
	// @param link_offset - this is the current link in the set 
	//                    - being prcessed
	// @param link_cluster - the cluster of links that make
	//                     - up the current document
	// @param curr_link - this is the current link id being processed
	// @return false if an exclude word, true otherwise
	inline bool ProcessExcludeWord(SWordHit &word_hit, int &link_offset, 
		CArrayList<_int64> &link_cluster, _int64 &curr_link) {

		if(word_hit.AskExcludeHitType()) {
			// checks for an exclude word
			if(word_hit.AskFirstHitType() == false) {
				// not a first word hit
				return true;
			}

			if(word_hit.AskImageHit()) {
				m_curr_image_num++;
			}

			if(word_hit.AskLinkHit()) {
				curr_link = link_cluster[link_offset++] >> 1;
			}
			
			return true;
		} 

		return false;
	}
	// This function processes anchor text that has been indexed
	// and assigns each anchor term a document id that has been
	// previously created. This is only done for documents that
	// haven't been spidered.
	// @param word_hit - this is the current hit being processed
	// @param link_offset - this is the current link in the set 
	//                    - being prcessed
	// @param link_cluster - the cluster of links that make
	//                     - up the current document
	// @param curr_link - this is the current link id being processed
	// @param hit_item - this stores the current constructed hit item
	inline uLong ProcessHitType(SWordHit &word_hit, 
		int &link_offset, CArrayList<_int64> &link_cluster,
		_int64 &curr_link, SHitItem &hit_item) {

		if(word_hit.AskFirstHitType()) {
			if(word_hit.AskLinkHit()) {
				curr_link = link_cluster[link_offset++] >> 1;
			}

			if(word_hit.AskImageHit()) {
				// start of image caption words
				m_curr_image_num++;
			}
		}

		if(word_hit.AskImageHit()) {
			hit_item.image_id = (m_curr_image_num << 3) | GetClientID();
			return 0x01;
		}

		if(word_hit.AskExcerptHit()) {
			return 0x02;
		}

		uLong enc = 0;
		if(word_hit.AskTitleHit()) {
			m_title_segment.WriteCompObject(hit_item.word_id);
			enc |= 0x04;
		}

		if(word_hit.AskLinkHit()) {
			enc |= 0x04;
		}

		return enc;
	}

	// This adds one of the excerpt and keyword terms to the set
	// if it is below the word occurrence threshold. If the term
	// is culled a term is used as placeholder to indicate a vacancy.
	// @param word_occur - this is the word occurrence
	// @param word_index - this is the id of the word
	void AddExcerptTerm(uLong word_occur, uLong word_index, uChar enc) {

		if(word_occur < m_word_occur_thresh) {
			m_excerpt_id_set_file.WriteCompObject(word_index);

			if(m_is_doc_title_parsed == true) {
				m_hit_encoding_file.WriteCompObject((uChar)0xFF);
			} else {
				m_hit_encoding_file.WriteCompObject(enc);
			}

			m_excerpt_term_num++;
		}
	}

	// This sets up the various files
	void Initialize() {

		m_lexon.ReadLexonFromFile();
		m_curr_hit_list.OpenReadFile(CUtility::ExtendString
			("GlobalData/HitList/meta_hit_list", GetClientID()));

		m_title_segment.OpenWriteFile(CUtility::ExtendString
			("GlobalData/Title/title_hits", GetClientID()));

		m_hit_encoding_file.OpenWriteFile(CUtility::ExtendString
			("GlobalData/DocumentDatabase/hit_encoding", GetClientID()));

		m_excerpt_id_set_file.OpenWriteFile(CUtility::ExtendString
			("GlobalData/DocumentDatabase/full_excerpt_word_id_set", GetClientID()));

		m_doc_size_file.OpenWriteFile(CUtility::ExtendString
			("GlobalData/DocumentDatabase/full_doc_size", GetClientID()));
	}

public:

	CCompileHitList() {
		m_curr_image_num = -1;
	}

	// Loads the hit list that has been created for the given
	// process id - stores the log file div and other hit
	// semantics as defined in DyableIndex.
	// @param hit_list_breadth - this is the number of hit lists
	//                         - to create -> later sorted
	void LoadHitList(int hit_list_breadth) {

		LoadFinalIndex();
		Initialize();

		CHDFSFile occur_thresh_file;
		occur_thresh_file.OpenReadFile("GlobalData/HitList/word_occur_thresh");
		occur_thresh_file.ReadObject(m_word_occur_thresh);

		m_hit_list_breadth = hit_list_breadth;
		m_base_hit_set.AllocateMemory(hit_list_breadth);
		m_anchor_hit_set.AllocateMemory(hit_list_breadth);

		for(int i=0; i<m_base_hit_set.OverflowSize(); i++) {
			m_base_hit_set[i].OpenWriteFile(CUtility::ExtendString
				("GlobalData/HitList/base_fin_hit",
				i, ".client", GetClientID()));

			m_anchor_hit_set[i].OpenWriteFile(CUtility::ExtendString
				("GlobalData/HitList/anchor_fin_hit",
				i, ".client", GetClientID()));
		}
	}

	// Creates the final hit list with the appropriate
	// doc id's and word id's inserted. Note that each
	// hit is partially sorted by placing the hit in
	// a bin corresponding to the word id division
	// @param doc_id - the base doc id of the current document
	// @param link_cluster - the cluster of links that make
	//                     - up the current document
	// @param link_offset - this is the link offset at which the
	//                    - link set begins, it excludes local links
	void CompileFinalHitList(_int64 &doc_id, 
		CArrayList<_int64> &link_cluster, int link_offset) {

		static uLong document_size;
		static _int64 image_enc;
		static SHitItem hit_item;
		static _int64 curr_link;
		static SWordHit word_hit;
		static uLong word_index;
		static int word_div;
		static uLong word_occur;
		static uChar is_excerpt_doc;
		hit_item.doc_id = doc_id;
	
		// gets the document hit list size
		m_curr_hit_list.GetEscapedItem(document_size);
		m_curr_hit_list.ReadCompObject(is_excerpt_doc);

		if((doc_id % 5000) == 0) {
			cout<<(m_curr_hit_list.PercentageFileRead() * 100)<<"% Done"<<endl;
		}

		m_excerpt_term_num = 0;
		m_is_doc_title_parsed = true;
		for(uLong i=0; i<document_size; i++) {
			// retrieves the term weight + enc
			m_curr_hit_list.ReadCompObject(word_hit.term_type);
			if(word_hit.AskTitleHit() == false) {
				m_is_doc_title_parsed = false;
			}

			if(ProcessExcludeWord(word_hit, link_offset, 
				link_cluster, curr_link) == true) {
				continue;
			}

			word_occur = GetWordIndex(word_hit, word_index);

			hit_item.word_id = word_index / m_hit_list_breadth;
			word_div = word_index % m_hit_list_breadth;

			hit_item.enc = ProcessHitType(word_hit, 
				link_offset, link_cluster, curr_link, hit_item);

			if(is_excerpt_doc == false) {
				continue;
			}

			if(word_hit.AskSuffix() == false) {
				AddExcerptTerm(word_occur, word_index, (uChar)hit_item.enc);
			} 
			
			hit_item.enc |= (uLong)(i << 3);
			if(word_hit.AskLinkHit() && curr_link >= URLBaseNum()) {
				// encodes a non spidered anchor text hit so it can indexed
				hit_item.WriteHitDocOrder(m_anchor_hit_set[word_div], hit_item.enc, curr_link);
			}

			hit_item.WriteHitDocOrder(m_base_hit_set[word_div]);
		}

		m_doc_size_file.AddEscapedItem(m_excerpt_term_num);
		m_doc_size_file.WriteCompObject5Byte(doc_id);
	}
};