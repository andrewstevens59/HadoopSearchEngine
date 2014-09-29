#include "./DistributeKeys.h"

// This class defines a number of data handling functions to 
// be used by the mapreduce classes. These are user defined
// functions that are used throughout.
template <class X> class CDataHandleTag : public CSetNum {

	// stores the html tags used in looking up the handler functions
	CHashDictionary<short> m_data_func_tag;

public:

	// This is a function used to retrieve a key
	void (*m_retrieve_key)(CHDFSFile &key_file, int &bytes, char buff[]);
	// This a funcptr to retreive a map
	void (*m_retrieve_map)(CHDFSFile &map_file, char map[], 
		char key[], int &map_bytes, int &key_bytes);
	// This is the function that writes a set
	void (*m_write_set)(CHDFSFile &file, int &key_bytes, 
		char key_buff[], int &map_bytes, char map_buff[]);
	// This is the function that writes the key occurrence
	void (*m_write_occur)(CHDFSFile &map_file, char key[], 
		int bytes, const X &occurr);
	// This is a function used to retrieve a key
	void (*m_retrieve_key_weight)(CHDFSFile &key_file,
		int &bytes, char buff[], X &weight);
	// This is the function that writes the key weight
	void (*m_write_key_weight)(CHDFSFile &map_file, char key[], 
		int bytes, const X &weight);
	// this is a function pointer that writes the the final mapped value to file
	void (*m_write_map)(CHDFSFile &to_file, char map[], int &map_bytes,
		char key[], int &key_bytes);
	// this stores the comparison function used
	int (*m_compare_func)(const SExternalSort &arg1, const SExternalSort &arg2);

	// This is used to map associations
	static void MapAssociationsToDocument1(CHDFSFile &to_file, char map[],
		int &map_bytes, char key[], int &key_bytes) {
			
		to_file.WriteCompObject(key, key_bytes);
		to_file.WriteCompObject((uChar)map_bytes);
		if(map_bytes != 0) {
			to_file.WriteCompObject(map, map_bytes);
		}
	}

	// This is used to map associations
	void MapAssociationsToDocument() {
		m_write_map = &MapAssociationsToDocument1;
	}

	// This writes the set of maps and does not write the
	// set of keys to the output file.
	static void WriteOrderedMapsOnly1(CHDFSFile &to_file, char map[],
		int &map_bytes, char key[], int &key_bytes) {

		to_file.WriteCompObject(map, map_bytes);
	}

	// This writes the set of maps and does not write the
	// set of keys to the output file.
	void WriteOrderedMapsOnly() {
		m_write_map = &WriteOrderedMapsOnly1;
	}

	// This is used to map local cluster maps to cluster hiearchies
	static void WriteClusterMapping1(CHDFSFile &to_file, char map[],
		int &map_bytes, char key[], int &key_bytes) {

		to_file.WriteCompObject((uChar)map_bytes);
		to_file.WriteCompObject(key, key_bytes);

		if(map_bytes != 0) {
			to_file.WriteCompObject(map, map_bytes);
		}
	}

	// This is used to map local cluster maps to cluster hiearchies
	void WriteClusterMapping() {
		m_write_map = &WriteClusterMapping1;
	}

	// This is used to map assoc text string to word ids
	static void WriteTextStringMap1(CHDFSFile &to_file, char map[],
		int &map_bytes, char key[], int &key_bytes) {

		map_bytes--;
		to_file.WriteCompObject(key, key_bytes);
		to_file.WriteCompObject((uChar)map_bytes);
		to_file.WriteCompObject(map, map_bytes);
	}

	// This reads a word text string from file
	static void ReadTextStringMap1(CHDFSFile &map_file, char map[], 
		char key[], int &map_bytes, int &key_bytes) {

		static uChar word_length;
		key_bytes = sizeof(uLong);
		map_file.ReadCompObject(key, key_bytes);
		map_file.ReadCompObject(word_length);
		map_bytes = word_length + 1;

		map_file.ReadCompObject(map, word_length);
	}

	// This is used to map assoc text string to word ids
	void WriteAssocTextStringMap() {
		m_write_map = &WriteTextStringMap1;
		m_retrieve_map = &ReadTextStringMap1;
	}

	// This reads the set of keywords associated to excerpts
	static void ReadExcerptKeyword(CHDFSFile &file, char map_buff[], 
		char key_buff[], int &map_bytes, int &key_bytes) {

		key_bytes = 5;
		// reads in the word id
		file.ReadCompObject(key_buff, 5);

		static uChar keyword_num;
		// read in the keyword number and checksum
		char *prev_ptr = map_buff;
		file.ReadCompObject(map_buff, 5);
		keyword_num = *((uChar *)map_buff + 4);
		map_buff += 5;

		// reads the word id and the word pos
		for(uChar i=0; i<keyword_num; i++) {
			file.ReadCompObject(map_buff, 5);
			map_buff += 5;
		}

		map_bytes = (int)(map_buff - prev_ptr);
	}

	// This reads the keyword text string
	static void ReadKeywordTextStrings(CHDFSFile &file, char map_buff[], 
		char key_buff[], int &map_bytes, int &key_bytes) {

		key_bytes = sizeof(S5Byte);
		file.ReadCompObject(key_buff, 5);

		// read in the text string corresponding to the keyword
		static uChar word_length;
		// read the term weight
		file.ReadCompObject(map_buff, sizeof(float));
		file.ReadCompObject(word_length);
		file.ReadCompObject(map_buff + sizeof(float), word_length);
		map_bytes = word_length + sizeof(float) + 1;
	}

	// This writes the set of titles associated with a doc id
	static void WriteExcerptKeywordMap(CHDFSFile &file, char map_buff[], 
		int &map_bytes, char key_buff[], int &key_bytes) {

		file.WriteCompObject(key_buff, 5);

		file.AddEscapedItem(map_bytes);
		file.WriteCompObject(map_buff, map_bytes);
	}

	// This is used to map the set of excerpt keywords 
	void WriteExcerptKeywordSet() {
		m_write_map = &WriteExcerptKeywordMap;
		m_retrieve_map = &ReadExcerptKeyword;
	}

	// This is used to map the set of association text strings
	void WriteKeywordTextString() {
		m_write_map = &WriteExcerptKeywordMap;
		m_retrieve_map = &ReadKeywordTextStrings;
	}

	// This is used to map cluster labels to word ids 
	static void WriteWordIDClusMap1(CHDFSFile &file, char map_buff[], 
		int &map_bytes, char key_buff[], int &key_bytes) {

		static S5Byte node;

		if(map_bytes == 0) {
			node.SetMaxValue();
			file.WriteCompObject(node);
			return;
		}

		file.WriteCompObject(map_buff, map_bytes);
	}

	// This is used to map cluster labels to word ids 
	void WriteWordIDClusMap() {
		m_write_map = &WriteWordIDClusMap1;
	}

	// This reads the keyword text string
	static void RetrieveAssociationMap1(CHDFSFile &file, char map_buff[], 
		char key_buff[], int &map_bytes, int &key_bytes) {

		key_bytes = sizeof(S5Byte);
		file.ReadCompObject(key_buff, 5);

		// read in the text string corresponding to the keyword
		static uChar word_length;
		// read the term weight
		file.ReadCompObject(map_buff, sizeof(float));
		file.ReadCompObject(map_buff + sizeof(float), 1);
		word_length = (uChar)map_buff[4];
		file.ReadCompObject(map_buff + sizeof(float) + 1, word_length);
		map_bytes = word_length + sizeof(float) + 1;
	}

	// This retrieves the association text map with association weight
	void RetrieveAssociationMap() {
		m_retrieve_map = &RetrieveAssociationMap1;
	}

	// This retrieves the association text map with association weight
	void CreateAssociationMapNULL() {
		m_retrieve_map = &RetrieveAssociationMap1;
		m_write_map = &MapAssociationsToDocument1;
	}

	// This reads the url text map
	static void ReadURLTextMap(CHDFSFile &file, char map_buff[], 
		char key_buff[], int &map_bytes, int &key_bytes) {

		key_bytes = sizeof(S5Byte);
		file.ReadCompObject(key_buff, 5);

		// read in the text string corresponding to the keyword
		static int word_length;
		file.ReadCompObject(map_buff, sizeof(int));
		word_length = *(int *)map_buff;
		file.ReadCompObject(map_buff + sizeof(int), word_length);
		map_bytes = word_length + sizeof(int);
	}

	// This is used to map the url text map string
	void CreateURLTextMap() {
		m_retrieve_map = &ReadURLTextMap;
	}

	// This reads the keyword doc id map
	static void ReadKeywordLinkMap(CHDFSFile &file, char map_buff[], 
		char key_buff[], int &map_bytes, int &key_bytes) {

		key_bytes = sizeof(S5Byte);
		file.ReadCompObject(key_buff, 5);

		// read in the text string corresponding to the keyword
		static uChar doc_num;
		file.ReadCompObject(map_buff, sizeof(uChar));
		doc_num = *(uChar *)map_buff;
		file.ReadCompObject(map_buff + sizeof(uChar), doc_num * sizeof(S5Byte));
		map_bytes = (doc_num  * sizeof(S5Byte)) + sizeof(uChar);
	}

	// This is used to create the keyword link doc id map
	void CreateKeywordLinkMap() {
		m_write_map = &WriteOrderedMapsOnly1;
		m_retrieve_map = &ReadKeywordLinkMap;
	}

	// This reads one of the similar term maps
	static void ReadSimilarTermMap(CHDFSFile &file, char map_buff[], 
		char key_buff[], int &map_bytes, int &key_bytes) {

		static uChar length;
		static uChar set_num;
		file.ReadCompObject(key_buff, sizeof(S5Byte));
		file.ReadCompObject(map_buff, sizeof(uChar));
		key_bytes = sizeof(S5Byte);

		char *map_ptr = map_buff;
		set_num = *(uChar *)map_buff;
		map_buff++;

		for(uChar i=0; i<set_num; i++) {

			// id and weight
			file.ReadCompObject(map_buff, sizeof(S5Byte) + sizeof(float));
			map_buff += sizeof(S5Byte) + sizeof(float);

			// word length
			file.ReadCompObject(map_buff, sizeof(uChar));
			length = *(uChar *)map_buff;
			map_buff++;

			// read the text string word
			file.ReadCompObject(map_buff, length);
			map_buff += length;
		}

		map_bytes = (int)(map_buff - map_ptr);
	}

	// This is used to map associations
	static void WriteSimilarWord(CHDFSFile &to_file, char map[],
		int &map_bytes, char key[], int &key_bytes) {
			
		to_file.WriteCompObject(key, key_bytes);
		to_file.WriteCompObject((u_short)map_bytes);
		if(map_bytes != 0) {
			to_file.WriteCompObject(map, map_bytes);
		}
	}

	// This is used to create the similar term map part of GlobalLexon
	void CreateSimilarTermMap() {
		m_write_map = &WriteSimilarWord;
		m_retrieve_map = &ReadSimilarTermMap;
	}

	// This is used to sort w_links
	static int CompareWLinks(const SExternalSort &arg1, const SExternalSort &arg2) {

		float weight1 = *(float *)(arg1.hit_item + (sizeof(S5Byte) << 1));
		float weight2 = *(float *)(arg2.hit_item + (sizeof(S5Byte) << 1));

		if(weight1 < weight2) {
			return -1;
		}

		if(weight1 > weight2) {
			return 1;
		}

		return 0;
	}

	// This is used to sort w_links
	void SortWLinks() {
		m_compare_func = CompareWLinks;
	}

	// This is used to test quick sort
	static int TestSortNodes(const SExternalSort &arg1, const SExternalSort &arg2) {

		float weight1 = *(float *)(arg1.hit_item);
		float weight2 = *(float *)(arg2.hit_item);

		if(weight1 < weight2) {
			return 1;
		}

		if(weight1 > weight2) {
			return -1;
		}

		return 0;
	}

	// This is used to test quick sort
	void TestSortNodes() {
		m_compare_func = TestSortNodes;
	} 

public:

	CDataHandleTag() {

		m_data_func_tag.Initialize(200, 8);
		char data_handle_tag[][40]={"MapAssociationsToDocument", 
			"WriteOrderedMapsOnly", "WriteClusterMapping", "WriteAssocTextStringMap",
			"WriteExcerptKeywordSet", "WriteKeywordTextString", "WriteWordIDClusMap", 
			"RetrieveAssociationMap", "CreateURLTextMap", "CreateKeywordLinkMap",
			"CreateSimilarTermMap", "CreateAssociationMapNULL", "SortWLinks", "TestSortNodes", "//"};

		int index=0;
		while(!CUtility::FindFragment(data_handle_tag[index], "//")) {
			int length = (int)strlen(data_handle_tag[index]);
			m_data_func_tag.AddWord(data_handle_tag[index++], length);
		}

		m_retrieve_key = NULL;
		m_retrieve_map = NULL;
		m_write_set = NULL;
		m_write_occur = NULL;
		m_retrieve_key_weight = NULL;
		m_write_key_weight = NULL;
		m_write_map = NULL;
	}

	// This is used to set a particular data processing function as defined
	// by the user. There could be multiple instance of the same data handling funciton.
	// @param request - this is the request string being issued
	void SetDataHandleFunc(const char request[]) {

		static void (CDataHandleTag::*meta_handler[])() = {
			&CDataHandleTag::MapAssociationsToDocument, 
			&CDataHandleTag::WriteOrderedMapsOnly,
			&CDataHandleTag::WriteClusterMapping,
			&CDataHandleTag::WriteAssocTextStringMap,
			&CDataHandleTag::WriteExcerptKeywordSet, 
			&CDataHandleTag::WriteKeywordTextString,
			&CDataHandleTag::WriteWordIDClusMap,
			&CDataHandleTag::RetrieveAssociationMap,
			&CDataHandleTag::CreateURLTextMap,
			&CDataHandleTag::CreateKeywordLinkMap,
			&CDataHandleTag::CreateSimilarTermMap,
			&CDataHandleTag::CreateAssociationMapNULL,
			&CDataHandleTag::SortWLinks,
			&CDataHandleTag::TestSortNodes,
		};

		int length = strlen(request);
		int index = m_data_func_tag.FindWord(request, length);
		//if the appropriate index is found
		if(m_data_func_tag.AskFoundWord()) {	
			//call the appropriate handler through the function pointer
			(this->*meta_handler[index])();	
		}
	}

};