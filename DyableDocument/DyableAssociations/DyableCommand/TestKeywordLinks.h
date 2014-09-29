#include "./TestCreateAssociations.h"


// This stores the directory of the keyword occurrence
const char *KEYWORD_OCCURRENCE_FILE = "GlobalData/WordDictionary/occurrence_map";

// This stores the word id alogn with the occurrence
struct SToken {
	// This stores the word id 
	S5Byte word_id;
	// This stores the position of the term 
	uChar pos;
	// This stores the association size
	uChar assoc_num;
	// This stores the weight of the term
	float term_weight;
};

CTrie occur_map(4);

// This class stores the term occurrence of each term in the keyword 
// set and the excerpt set.
class CTestWordOccurrence : public CNodeStat {

	// This maps word ids to occurrences
	CTrie m_word_map;
	// This stores the set of occurrences associated with each term
	CArrayList<int> m_occur;
	// This stores the set of occurrences
	CArrayList<int> m_excerpt_occur;

	// This stores the list of word ids that compose each document
	CSegFile m_excerpt_id_set_file;
	// This stores the occurrence of each term in the set
	CSegFile m_excerpt_occur_map_file;

	// stores the global dictionary offset for each of the log types
	CArray<SGlobalIndexOffset> m_dict_offset;

	// This function stores tbe mapping between a word id and it's occurrence
	void CreateWordOccurrenceMap() {

		CHDFSFile occur_file;
		occur_file.OpenReadFile(CUtility::ExtendString(KEYWORD_OCCURRENCE_FILE, (int)0));
		
		uLong word_id;
		uLong occur;
		while(occur_file.ReadCompObject(word_id)) {
			occur_file.ReadCompObject(occur);

			m_word_map.AddWord((char *)&word_id, 4);
			if(!m_word_map.AskFoundWord()) {
				m_occur.PushBack(occur);
			}
		}
	}

	// This tests the word occurence set
	void TestWordOccurrence() {

		uLong term_id;
		int occur;
		int offset = 0;
		for(int i=0; i<GetClientNum(); i++) {
			m_excerpt_occur_map_file.OpenReadFile(CUtility::ExtendString
				("LocalData/full_excerpt_occur_map", i));

			while(m_excerpt_occur_map_file.ReadCompObject(term_id)) {
				m_excerpt_occur_map_file.ReadCompObject(occur);
				if(m_excerpt_occur[offset] != occur) {
					cout<<"excerpt occur mismatch "<<m_excerpt_occur[offset]<<" "<<occur;getchar();
				}
				offset++;
			}
		}
	}

public:

	CTestWordOccurrence() {
	}

	void LoadOccurrence() {
		m_word_map.Initialize(4);
		m_occur.Initialize();
		m_excerpt_occur.Initialize();

		CreateWordOccurrenceMap();

		uLong occur;
		uLong term_id;
		for(int i=0; i<GetClientNum(); i++) {
			m_excerpt_id_set_file.OpenReadFile(CUtility::ExtendString
				("LocalData/full_excerpt_occur_map", i));

			while(m_excerpt_id_set_file.ReadCompObject(term_id)) {
				m_excerpt_id_set_file.ReadCompObject(occur);
				int id = m_word_map.FindWord((char *)&term_id, 4);
				m_excerpt_occur.PushBack(m_occur[id]);
			}
		}

		TestWordOccurrence();
	}

	// This returns the occurrence of a particular term
	inline int TermOccurrence(int term_id) {
		int id = m_word_map.AddWord((char *)&term_id, 4);
		return m_occur[id];
	}
};

// This creates the set of associations that is used for testing
class CTestCreateAssociations : public CTestWordOccurrence {

	// This stores each of the association sets for keywords
	CMemoryChunk<CSegFile> m_keyword_assoc_set_file;

public:

	CTestCreateAssociations() {
	}

	// This is the entry function
	void TestCreateAssociations() {

		m_keyword_assoc_set_file.AllocateMemory(ASSOC_SET_NUM);

		for(int j=0; j<CNodeStat::GetClientNum(); j++) {
			for(int i=0; i<ASSOC_SET_NUM; i++) {
				m_keyword_assoc_set_file[i].OpenReadFile(CUtility::ExtendString
					("LocalData/keyword_assoc_file", i, ".client", j));

				int occur;
				CMemoryChunk<uLong> id_set(i + 2);
				CHDFSFile &keyword_file = m_keyword_assoc_set_file[i];
				while(keyword_file.ReadCompObject(id_set.Buffer(), i + 2)) {
					_int64 compare = 0;
					for(int k=0; k<id_set.OverflowSize(); k++) {
						compare += TermOccurrence(id_set[k]);
					}

					keyword_file.ReadCompObject(occur);
					if(compare != occur) {
						cout<<"Occurrence Mismatch "<<compare<<" "<<occur;getchar();
					}
				}
			}
		}
	}
};

// This is a test class that is used to test for the correct functionality
// of the creation of the keyword links. It performs identical operations
// using unsophisticated means and compares the two outputs.
class CTestKeywordLinkSet : public CNodeStat {

	// This stores the set of word ids and their occurrence
	struct SIndvKeyword {
		uLong word_id;
		uLong occur;
	};

	// This stores the keyword and its weight
	struct SDocTerm {
		float term_weight;
		uLong word_id;
	};

	// This stores one of the associations
	struct SAssoc {
		uLong occur;
		float assoc_weight;
		uLong assoc_id;
		uLong set[4];
	};

	// This stores one of the keyword links
	struct SKeywordLink {
		uLong word_id1;
		uLong word_id2;
		uLong occur;
	};

	// This stores the set of individual words
	CArrayList<SIndvKeyword> m_indv_keyword_buff;
	// This stores the set of document terms
	CArrayList<SDocTerm> m_doc_term_buff;
	// This finds the set of word id weights
	CArrayList<float> m_keyword_weight;
	// This stores the threshold weight for each of the association types
	CMemoryChunk<float> m_assoc_thresh_weight;
	// This stores the current assoc id
	uLong m_curr_assoc_id;

	// This stores the excerpt term number
	CArrayList<int> m_excerpt_num;
	// This stores each of the excerpt ids
	CArrayList<uLong> m_excerpt_term_id;
	// This stores the list of priority tokens
	CLimitedPQ<SToken> m_tok_queue;
	// This is used to find duplicate terms
	CTrie m_duplicate_term;
	// This stores the set of assoc terms for excerpts
	CMemoryChunk<CArrayList<SAssoc> > m_excerpt_assoc_term_buff;
	// This stores the set of assoc terms for keywords
	CMemoryChunk<CArrayList<SAssoc> > m_keyword_assoc_term_buff;

	// This stores the hash boundary for which this client is responsible
	// that is which hash divisions this client must access 
	SBoundary m_client_bound;
	// stores the global dictionary offset for each of the log types
	CArray<SGlobalIndexOffset> m_dict_offset;
	// This stores the lexon
	CLexon m_lexon;

	// This stores the list of word ids that compose each document
	CSegFile m_excerpt_id_set_file;
	// This stores the list of word ids that compose each document
	CSegFile m_keyword_id_set_file;
	// This stores the number of word ids that compose each document
	CHDFSFile m_doc_size_file;

	// This stores each association
	CTrie m_assoc_map;
	// This stores the keyword map
	CTrie m_keyword_map;
	// This stores the association offset for each association
	CMemoryChunk<int> m_assoc_offset;
	// This stores the weight of each association
	CMemoryChunk<CArrayList<SAssoc> > m_assoc_buff;
	// This stores the weight of each association
	CMemoryChunk<CArrayList<SAssoc> > m_assoc_buff_temp;
	// This stores the keyword link set
	CArrayList<SKeywordLink> m_keyword_link_buff;

	// This stores the mapping between word_id and local id
	CTrie m_word_map;
	// This stores the set of word text strings
	CTrie m_word_text_map;

	// This stores the association text map id
	CTrie m_assoc_id_map;
	// This stores the set of text strings that make up each association
	CArrayList<char> m_assoc_text_buff;
	// This stores the offset of each association text segment
	CArrayList<int> m_assoc_text_offset;

	// This is used to compare tokens on word occurrence
	static int CompareTokens(const SToken &arg1, const SToken &arg2) {

		if(arg1.assoc_num < arg2.assoc_num) {
			return -1;
		}

		if(arg1.assoc_num > arg2.assoc_num) {
			return 1;
		}

		if(arg1.term_weight < arg2.term_weight) {
			return -1;
		}

		if(arg1.term_weight > arg2.term_weight) {
			return 1;
		}

		return 0;
	}

	// This is used to sort associations
	static int CompareAssocWeight(const SAssoc &arg1, const SAssoc &arg2) {

		if(arg1.assoc_weight < arg2.assoc_weight) {
			return -1;
		}

		if(arg1.assoc_weight > arg2.assoc_weight) {
			return 1;
		}

		return 0;
	}

	// This is called to initialize the various variables needed
	// to perform the mapping this includes the dictionary offsets
	void Initialize() {

		int log_div_size = 19;

		m_client_bound.Set(0, log_div_size);

		CHDFSFile file;
		file.OpenReadFile("GlobalData/WordDictionary/dictionary_offset");

		_int64 dummy1;
		uLong word_num;
		// writes the number of base nodes
		file.ReadCompObject(dummy1);
		// writes the total number of nodes
		file.ReadCompObject(dummy1);
		file.ReadCompObject(word_num);
		m_dict_offset.ReadArrayFromFile(file);
		m_curr_assoc_id = word_num;
	}

	// This function stores tbe mapping between a word id and it's occurrence
	void CreateWordOccurrenceMap() {

		CHDFSFile file;
		CMemoryChunk<int> word_occurr;
		CHashDictionary<int> dict;
		SWordIDOccurrMap map;
		int length;

		for(int i=m_client_bound.start; i<m_client_bound.end; i++) {

			file.OpenReadFile(CUtility::ExtendString
				("GlobalData/WordDictionary/word_occurrence", i));

			word_occurr.ReadMemoryChunkFromFile(file);

			dict.ReadHashDictionaryFromFile((CUtility::ExtendString
				("GlobalData/WordDictionary/word_dictionary", i)));

			for(int j=0; j<word_occurr.OverflowSize(); j++) {
				m_indv_keyword_buff.ExtendSize(1);
				m_indv_keyword_buff.LastElement().word_id = m_dict_offset[i].word_offset + j;
				m_indv_keyword_buff.LastElement().occur = word_occurr[j];
				m_word_map.AddWord((char *)&m_indv_keyword_buff.
					LastElement().word_id, 4);

				if(m_word_text_map.Size() != m_indv_keyword_buff.LastElement().word_id) {
					cout<<"Word ID Mismatch2";getchar();
				}

				char *word = dict.GetWord(j, length);
				m_word_text_map.AddWord(word, length);

				m_keyword_weight.PushBack(word_occurr[j]);
			}
		}
	}

	// This loads the word id mapping and occurrence from the lexon
	void LoadLexonWordOccurMap() {

		m_lexon.ReadLexonFromFile("GlobalData/WordDictionary/lexon");
		SWordIDOccurrMap map;

		CHashDictionary<int> &dict = m_lexon.Dictionary();

		int length;
		for(int i=0; i<dict.Size(); i++) {

			m_indv_keyword_buff.ExtendSize(1);
			m_indv_keyword_buff.LastElement().word_id = i;
			m_indv_keyword_buff.LastElement().occur =
				m_lexon.WordOccurrence(i);

			if(m_word_text_map.Size() != i) {
				cout<<"Word ID Mismatch1";getchar();
			}

			char *word = dict.GetWord(i, length);
			m_word_text_map.AddWord(word, length);

			m_word_map.AddWord((char *)&i, 4);

			m_keyword_weight.PushBack(m_lexon.WordOccurrence(i));
		}
	}

	// This creats a set of n grouped terms so that the occurence
	// of each ossociation in the document corpus can be counted.
	// @param term_num - this is the number of terms to group in the association
	// @param assoc_file - this is the outputed set of associations
	void CreateAssociations(int term_num, const char word_id_dir[],
		CMemoryChunk<CArrayList<SAssoc> > &assoc_term_buff) {

		uLong doc_size;
		_int64 doc_id; 
		uLong occur;

		for(int k=0; k<GetClientNum(); k++) {
			m_excerpt_id_set_file.OpenReadFile(CUtility::ExtendString
				(word_id_dir, k));

			m_doc_size_file.OpenReadFile(CUtility::ExtendString
				("GlobalData/DocumentDatabase/doc_size", k));

			SWordIDOccurrMap term;
			CMemoryChunk<uLong> assoc_num(term_num);
			CCyclicArray<SWordIDOccurrMap> id_buff(term_num);
			while(m_doc_size_file.GetEscapedItem(doc_size) >= 0) {
				m_doc_size_file.Get5ByteCompItem(doc_id);

				id_buff.Reset();
				for(uLong i=0; i<doc_size; i++) {
					m_excerpt_id_set_file.ReadCompObject(term.word_id);

					int id = m_word_map.FindWord((char *)&term.word_id, 4);
					term.word_occurr = m_indv_keyword_buff[id].occur;
	
					id_buff.PushBack(term);
					if(id_buff.Size() >= id_buff.OverflowSize()) {
						occur = 0;
						for(int j=0; j<id_buff.OverflowSize(); j++) {
							assoc_num[j] = id_buff[j].word_id;
							occur += id_buff[j].word_occurr;
						}

						int id = m_assoc_map.AddWord((char *)assoc_num.Buffer(),
							sizeof(uLong) * id_buff.Size());

						assoc_term_buff[term_num-2].ExtendSize(1);
						for(int j=0; j<id_buff.Size(); j++) {
							assoc_term_buff[term_num-2].LastElement().set[j] = assoc_num[j];
						}

						if(!m_assoc_map.AskFoundWord()) {

							m_assoc_buff[term_num-2].ExtendSize(1);
							m_assoc_buff[term_num-2].LastElement().assoc_weight = occur;
							m_assoc_buff[term_num-2].LastElement().occur = 1;

							for(int j=0; j<id_buff.Size(); j++) {
								m_assoc_buff[term_num-2].LastElement().set[j] = assoc_num[j];
							}
						} else {
							m_assoc_buff[term_num-2][id].occur++;
						}
					}
				}
			}
		}
	}

	// This adds an association text segment
	void AddAssociationTextSegment(SAssoc &assoc, int term_num) {

		CString buff;
		for(int i=0; i<term_num; i++) {
			char *word = m_word_text_map.GetWord(assoc.set[i]);
			buff += word;
			buff += "^";
		}

		m_assoc_id_map.AddWord((char *)&assoc.assoc_id, 4);
		m_assoc_text_buff.CopyBufferToArrayList(buff.Buffer(),
			buff.Size(), m_assoc_text_buff.Size());
		m_assoc_text_offset.PushBack(m_assoc_text_offset.LastElement() + buff.Size());
	}

	// This adds one of the associations to the ranked queue
	// @param token - this is one of the associations
	// @param token - this is one of the associations
	// @param check_sum - this is the current check sum for the document
	// @param x_or_bit - this is the x_or_bit bit vector
	inline void AddAssocToQueue(SToken &token) {

		m_duplicate_term.AddWord((char *)&token.word_id, 5);
		if(!m_duplicate_term.AskFoundWord()) {
			//cout<<token.word_id.Value();getchar();
			m_tok_queue.AddItem(token);
		}
	}

	// This adds the excerpt terms to the set
	void AddExcerptTerms() {

		m_excerpt_num.PushBack(m_tok_queue.Size());
		while(m_tok_queue.Size() > 0) {
			SToken tok = m_tok_queue.PopItem();
			m_excerpt_term_id.PushBack(tok.word_id.Value());
		}
	}

	// This creates the keyword set from the document set
	void CreateKeywordSet() {

		uLong doc_size;
		_int64 doc_id; 

		CMemoryChunk<int> offset(4, 0);
		for(int k=0; k<GetClientNum(); k++) {
			m_excerpt_id_set_file.OpenReadFile(CUtility::ExtendString
				("GlobalData/DocumentDatabase/excerpt_word_id_set", k));

			m_doc_size_file.OpenReadFile(CUtility::ExtendString
				("GlobalData/DocumentDatabase/doc_size", k));

			SWordIDOccurrMap term;
			SToken token;
			while(m_doc_size_file.GetEscapedItem(doc_size) >= 0) {
				m_doc_size_file.Get5ByteCompItem(doc_id);
				//getchar();
				m_duplicate_term.Reset();

				for(uLong i=0; i<doc_size; i++) {
					m_excerpt_id_set_file.ReadCompObject(term.word_id);

					int id = m_word_map.FindWord((char *)&term.word_id, 4);
					term.word_occurr = m_indv_keyword_buff[id].occur;

					bool assoc_found = false;
					for(int j=2; j<2+ASSOC_SET_NUM; j++) {

						if(i + 1 >= j) {
							int id = m_assoc_map.FindWord((char *)
								m_excerpt_assoc_term_buff[j-2][offset[j-2]++].set,
								sizeof(uLong) * j);

							SAssoc &assoc = m_assoc_buff[j-2][id];
							if(assoc.occur >= 3 && assoc.assoc_weight >= m_assoc_thresh_weight[j-2]) {
								m_doc_term_buff.ExtendSize(1);
								m_doc_term_buff.LastElement().term_weight = assoc.assoc_weight;
								m_doc_term_buff.LastElement().word_id = assoc.assoc_id;
								assoc_found = true; 
								token.assoc_num = j;
								token.word_id = assoc.assoc_id;
								token.term_weight = assoc.assoc_weight;
								AddAssocToQueue(token);
							}
						}
					}

					if(assoc_found == false) {
						m_doc_term_buff.ExtendSize(1);
						m_doc_term_buff.LastElement().term_weight = 1.0f / term.word_occurr;
						m_doc_term_buff.LastElement().word_id = term.word_id;
						token.assoc_num = 1;
						token.word_id = term.word_id;
						token.term_weight = m_doc_term_buff.LastElement().term_weight;
						AddAssocToQueue(token);
					}
				}
				AddExcerptTerms();
			}
		}
	}

	// This creates the keyword link set by parsing the keyword set
	void CreateKeywordLinkSet(float keyword_thresh) {

		CCyclicArray<SDocTerm> id_buff(2);
		CMemoryChunk<uLong> term(2);
		for(int i=0; i<m_doc_term_buff.Size(); i++) {

			if(m_doc_term_buff[i].term_weight >= keyword_thresh) {
				id_buff.PushBack(m_doc_term_buff[i]);
			}

			if(id_buff.Size() >= 2) {
				term[0] = id_buff[0].word_id;
				term[1] = id_buff[1].word_id;
				int id = m_keyword_map.AddWord((char *)term.Buffer(), sizeof(uLong) * 2);
				if(!m_keyword_map.AskFoundWord()) {
					m_keyword_link_buff.ExtendSize(1);
					m_keyword_link_buff.LastElement().word_id1 = term[0];
					m_keyword_link_buff.LastElement().word_id2 = term[1];
					m_keyword_link_buff.LastElement().occur = 1;
				} else {
					m_keyword_link_buff[id].occur++;
				}
			}
		}
	}

	// This finds the keyword threshold weight for the set of keywords
	float KeywordThresholdWeight(float percentage) {
		return m_keyword_weight[(percentage * m_keyword_weight.Size()) - 1];
	}

public:

	CTestKeywordLinkSet() {
	}

	// This loads the keyword set
	void LoadKeywordSet(int client_id, int client_num) {

		CNodeStat::SetClientID(client_id);
		CNodeStat::SetClientNum(client_num);
		m_word_map.Initialize(4);
		m_doc_term_buff.Initialize(4);
		m_assoc_map.Initialize(4);
		m_keyword_map.Initialize(4);
		m_keyword_link_buff.Initialize(4);
		m_duplicate_term.Initialize(4);
		m_keyword_weight.Initialize(4);
		m_assoc_thresh_weight.AllocateMemory(4);
		m_word_text_map.Initialize(4);

		m_assoc_id_map.Initialize(4);
		m_assoc_text_buff.Initialize(4);
		m_assoc_text_offset.Initialize(4);
		m_assoc_text_offset.PushBack(0);

		m_excerpt_num.Initialize(4);
		m_excerpt_term_id.Initialize(4);
		m_assoc_offset.AllocateMemory(4);
		m_excerpt_assoc_term_buff.AllocateMemory(4);
		m_keyword_assoc_term_buff.AllocateMemory(4);

		m_tok_queue.Initialize(20, CompareTokens);

		m_excerpt_id_set_file.SetFileName(CUtility::ExtendString
			("GlobalData/DocumentDatabase/excerpt_word_id_set", GetClientID()));
		m_keyword_id_set_file.SetFileName(CUtility::ExtendString
			("GlobalData/DocumentDatabase/keyword_id_set", GetClientID()));

		m_doc_size_file.SetFileName(CUtility::ExtendString
			("GlobalData/DocumentDatabase/doc_size", GetClientID()));

		m_assoc_buff.AllocateMemory(4);
		for(int i=0; i<m_assoc_buff.OverflowSize(); i++) {
			m_assoc_buff[i].Initialize(4);
			m_keyword_assoc_term_buff[i].Initialize(4);	
			m_excerpt_assoc_term_buff[i].Initialize(4);	
		}

		m_indv_keyword_buff.Initialize(4);
		Initialize();
		LoadLexonWordOccurMap();
		CreateWordOccurrenceMap();

		int max_size[3] = {99999, 9999, 9999};
		for(int i=0; i<1; i++) {
			CreateAssociations(i + 2, "GlobalData/DocumentDatabase/excerpt_word_id_set",
				m_excerpt_assoc_term_buff);
			CreateAssociations(i + 2, "GlobalData/DocumentDatabase/keyword_id_set",
				m_keyword_assoc_term_buff);
		}
		
		CreateKeywordSet();

		float percentage = 1.0f;

		for(int i=0; i<4; i++) {
			float threhold = KeywordThresholdWeight(percentage);
			CreateKeywordLinkSet(threhold);
			percentage *= 0.6f;
		}

		m_assoc_id_map.WriteTrieToFile("assoc_id_map");
		m_assoc_text_buff.WriteArrayListToFile("assoc_text");
		m_assoc_text_offset.WriteArrayListToFile("assoc_offset");
	}

	// This tests the excerpt keywords
	void TestExcerptKeywords() {

		CHDFSFile excerpt_file;

		int offset = 0;
		int size_offset = 0;
		for(int i=0; i<GetClientNum(); i++) {
			excerpt_file.OpenReadFile(CUtility::ExtendString
				("GlobalData/ABTrees/excerpt_titles", GetClientID()));

			S5Byte doc_id;
			uLong check_sum;
			uChar size;
			uChar pos;
			S5Byte word_id;

			while(excerpt_file.ReadCompObject(doc_id)) {
				excerpt_file.ReadCompObject(check_sum);
				excerpt_file.ReadCompObject(size);

				if(size != m_excerpt_num[size_offset++]) {
					cout<<"Excerpt Num Mismatch "<<(int)size<<" "<<
						m_excerpt_num[size_offset-1];getchar();
				}
				
				for(uChar j=0; j<size; j++) {
					excerpt_file.ReadCompObject(word_id);
					excerpt_file.ReadCompObject(pos);

					if(word_id.Value() != m_excerpt_term_id[offset++]) {
						cout<<"Excerpt Term ID Mismatch "<<word_id.Value()<<
							" "<<m_excerpt_term_id[offset-1];getchar();
					}
				}
			}
		}
	}

	// This tests the final link set 
	void TestKeywordLinkSet() {

		CHDFSFile local_link_file;
		float weight;

		int offset = 0;
		for(int i=0; i<GetClientNum(); i++) {
			local_link_file.OpenReadFile(CUtility::ExtendString
				("Data/fin_keyword_set", i));

			S5Byte src = 0;
			CMemoryChunk<uLong> word_id(2);

			while(local_link_file.ReadCompObject(src)) {
				local_link_file.ReadCompObject(weight);

				if(m_doc_term_buff[offset++].word_id != src.Value()) {
					cout<<"Word ID Mismatch";getchar();
				}
			}
		}
	}
};
