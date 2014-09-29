#include "./MapReduce.h"


// This class is responsible for storing all instances of document sets
// that have been created throughout the spidering process. These must
// be accessed during the indexing phase to rebuild the necessary data
// structures. Only the most recent HTML documents need to be parsed
// to save time and the output is saved to a reduced compiled formed 
// which is then later parsed.

// The number of document sets is maintained, if however this information
// is lost each document set is scanned to check for its existence. This 
// class also handles fault tolerance. If for some reason a document set
// becomes corrupted and cannot be accessed then it is skipped and not
// indexed. This class is designed to help manage an expanding document
// set over a int period of time.
class CDocumentDatabase : public CNodeStat {

	// This stores statistics relating to the current global set
	struct SGlobalStat {
		// This stores the global number of sets
		int set_num;
		// This stores the global number of documents
		_int64 doc_num;

		void Reset() {
			doc_num = 0;
			set_num = 0;
		}

		// This reads the global stat
		inline bool ReadGloblalStat(CHDFSFile &file) {
			return file.ReadCompObject(set_num);
		}

		// This writes the global stat
		inline void WriteGloblalStat(CHDFSFile &file) {
			return file.WriteCompObject(set_num);
		}
	};

	// This stores the document bounds for a paticular client
	struct SDocBound {
		// This stores the docunent set
		int doc_set;
		// This stores the doc id in the set
		_int64 doc_id_offset;
		// This stores the local id of the document in the buffer
		int local_id;
	};

	// This stores the html working directory
	CMemoryChunk<char> m_html_directory;
	// This stores the compiled attributes working directory
	CMemoryChunk<char> m_attr_directory;
	// This is a ptr to the current directory
	const char *m_directory;

	// This stores the current global stat
	SGlobalStat m_global_stat;
	// This stores the start document bound of the client
	struct SDocBound m_start_bound;
	// This stores the end document bound of the client
	struct SDocBound m_end_bound;

	// This stores the current file storage being used
	CFileStorage m_file_set;
	// This stores the current document buffer
	CMemoryChunk<char> m_doc_buffer;
	// This stores the offset in the doc buffer
	int m_doc_buff_offset;
	// This stores the current document id being processed
	_int64 m_curr_doc_id;
	// This stores the bounds of the documents being parsed
	S64BitBound m_doc_bound;

	// This adds the file storage set to the list
	// @param doc_set - this is the current document set being processed
	void AddFileStorage(CFileStorage &doc_set) {

		m_global_stat.doc_num += doc_set.DocumentCount();

		m_doc_num.ExtendSize(1);
		m_doc_num.LastElement().doc_id_offset = m_global_stat.doc_num;
		m_doc_num.LastElement().doc_set = m_global_stat.set_num;
	}

	// This does a scan for the global set number
	// @param max_doc_num - the maximum number of documents to spider
	void FindGlobalHTMLSetNum(_int64 max_doc_num) {

		CHDFSFile set_file;
		CFileStorage doc_set;
		m_global_stat.set_num = 0;
		int set_num = 0;
		int count = 0;

		while(true) {

			try {
				doc_set.LoadDocumentIndex(CUtility::ExtendString
					(m_html_directory.Buffer(), m_global_stat.set_num));

				cout<<"Loading Set "<<m_global_stat.set_num<<"  "<<doc_set.DocumentCount()<<endl;

				set_num = m_global_stat.set_num;
				AddFileStorage(doc_set);

				if(max_doc_num >= 0 && m_global_stat.doc_num > max_doc_num) {
					m_global_stat.doc_num = max_doc_num;
					m_doc_num.LastElement().doc_id_offset = max_doc_num;
					m_global_stat.set_num = set_num + 1;
					return;
				}
			} catch(...) {
				if(++count >= 20) {
					m_global_stat.set_num = set_num + 1;
					return;
				}
			}

			m_global_stat.set_num++;
		}
	}

	// This does a scan for the global set number
	void FindGlobalCompAttrSetNum(_int64 max_doc_num) {

		CHDFSFile doc_num_file;
		CFileStorage doc_set;
		m_global_stat.set_num = 0;
		int doc_num = 0;
		int set_num = 0;
		int count = 0;

		while(true) {

			try {
				doc_num_file.OpenReadFile(CUtility::ExtendString
					("GlobalData/CompiledAttributes/doc_set_size", m_global_stat.set_num));
				doc_num_file.ReadObject(doc_num);
				
				// only loads the html index if the it hasn't already been compiled
				doc_set.LoadDocumentIndex(CUtility::ExtendString
					(m_attr_directory.Buffer(), m_global_stat.set_num));

				cout<<"Loading Set "<<m_global_stat.set_num<<"  "<<doc_set.DocumentCount()<<" Out of "<<doc_num<<endl;

				set_num = m_global_stat.set_num;
				AddFileStorage(doc_set);

				if(max_doc_num >= 0 && m_global_stat.doc_num > max_doc_num) {
					m_global_stat.doc_num = max_doc_num;
					m_doc_num.LastElement().doc_id_offset = max_doc_num;
					m_global_stat.set_num = set_num + 1;
					return;
				}

			} catch(...) {
				if(++count >= 20) {
					m_global_stat.set_num = set_num + 1;
					return;
				}
			}

			m_global_stat.set_num++;
		}
	}

	// This finds the location of a particular doc id 
	// @param doc_id - this is the doc id being searched for
	// @param bound - this is the document bound (start or end)
	void FindDocIDLoc(_int64 doc_id, SDocBound &bound) {

		_int64 curr_offset = 0;
		for(int i=0; i<m_doc_num.Size(); i++) {
			_int64 prev_offset = curr_offset;
			curr_offset = m_doc_num[i].doc_id_offset;

			if(doc_id >= prev_offset && doc_id < curr_offset) {
				bound.doc_set = m_doc_num[i].doc_set;
				bound.doc_id_offset = doc_id - prev_offset;
				bound.local_id = i;
				return;
			}
		}
	}

	// This loads in the next document set 
	bool LoadNextDocumentSet() {

		while(true) {
			if(++m_start_bound.local_id >= m_doc_num.Size()) {
				return false;
			}

			m_start_bound.doc_set = m_doc_num[m_start_bound.local_id].doc_set;
			m_start_bound.doc_id_offset = 0;

			if(m_start_bound.doc_set >= m_global_stat.set_num) {
				return false;
			}

			if(m_start_bound.doc_set > m_end_bound.doc_set) {
				return false;
			}

			try {
				m_file_set.LoadDocumentIndex(CUtility::ExtendString
					(m_directory, m_start_bound.doc_set));

cout<<"Get "<<m_start_bound.doc_set<<endl;
				int doc_num = 200;
				if(m_start_bound.doc_set >= m_end_bound.doc_set) {
					doc_num = (int)min((_int64)doc_num, m_end_bound.doc_id_offset);
				}

				m_file_set.GetDocument(m_doc_buffer, 0, doc_num);
				return true;
			} catch(...) {
			}
		} 

		return true;
	}

	// This calculates the bounds for the current client
	// @param docs_parsed - this is the number of documents
	//					  - that have already been parsed
	bool CalculateClientBounds(_int64 docs_parsed) {

		if(m_global_stat.doc_num == 0) {
			m_start_bound.doc_set = 1;
			m_end_bound.doc_set = 0;

			m_start_bound.local_id = 0;
			m_end_bound.local_id = m_doc_num.Size() - 1;
			m_start_bound.doc_id_offset = 1;
			m_end_bound.doc_id_offset = 0;
			return false;
		}

		m_doc_bound.start = docs_parsed;
		m_doc_bound.end = m_global_stat.doc_num;
		if(m_doc_bound.start >= m_doc_bound.end) {
			return false;
		}

		CHashFunction::BoundaryPartion(GetClientID(), 
			GetClientNum(), m_doc_bound.end, m_doc_bound.start);

		FindDocIDLoc(m_doc_bound.start, m_start_bound);
		FindDocIDLoc(m_doc_bound.end, m_end_bound);
cout<<m_start_bound.doc_set<<" "<<m_end_bound.doc_set<<"  **************************"<<endl;
		if(GetClientID() == GetClientNum() - 1) {
			m_end_bound.doc_set = m_global_stat.set_num - 1;
			m_end_bound.doc_id_offset = m_global_stat.doc_num;
			m_end_bound.local_id = m_doc_num.Size() - 1;
			
			if(m_doc_num.Size() > 1) {
				m_end_bound.doc_id_offset -= m_doc_num.SecondLastElement().doc_id_offset;
			}
		}
cout<<"in"<<endl;
		m_file_set.LoadDocumentIndex(CUtility::ExtendString
			(m_directory, m_start_bound.doc_set));
cout<<"out"<<endl;
		m_curr_doc_id = m_doc_bound.start;
		m_doc_buff_offset = 0;
		m_doc_buffer.FreeMemory();

		return true;
	}

protected:

	// This stores the number of documents in each set
	CArrayList<SDocBound> m_doc_num;

public:

	CDocumentDatabase() {
		m_html_directory.AllocateMemory(500);
		m_attr_directory.AllocateMemory(500);
	}

	// This loads the set and prepares for respidering
	// @param docs_parsed - this is the number of documents that have 
	//                    - already been parsed (this are not rebuilt)
	// @param max_doc_num - this is the maximum number of documents to parse
	bool LoadHTMLDatabase(const char html_dir[], const char attr_dir[],
		int client_id, int client_num, _int64 docs_parsed, _int64 max_doc_num = -1) {

		CNodeStat::SetClientID(client_id);
		CNodeStat::SetClientNum(client_num);
		strcpy(m_html_directory.Buffer(), html_dir);
		strcpy(m_attr_directory.Buffer(), attr_dir);
		m_doc_num.Initialize(4);

		m_global_stat.Reset();
		m_directory = html_dir;

		FindGlobalHTMLSetNum(max_doc_num - docs_parsed);
		return CalculateClientBounds(docs_parsed);
	}

	// This loads the set and prepares for respidering
	bool LoadCompiledAttrDatabase(const char dir[], int client_id,
		int client_num, _int64 max_doc_num = -1) {

		CNodeStat::SetClientID(client_id);
		CNodeStat::SetClientNum(client_num);
		strcpy(m_attr_directory.Buffer(), dir);

		m_global_stat.Reset();
		m_directory = dir;

		m_doc_num.Initialize(4);
		FindGlobalCompAttrSetNum(max_doc_num);
		return CalculateClientBounds(0);
	}

	// This returns the total number of documents to be parsed by this client
	inline _int64 DocNum() {
		return m_doc_bound.Width();
	}

	// This returns the global number of documents in the set
	inline _int64 GlobalDocNum() {
		return m_global_stat.doc_num;
	}

	// This returns the document range for which this client is responsible
	inline S64BitBound DocBound() {
		return m_doc_bound;
	}

	// This retrieves the next document from the set
	bool RetrieveNextDocument(_int64 &doc_id, CArray<char> &doc_buff) {

		if(m_start_bound.doc_set >= m_end_bound.doc_set) {
			if(m_start_bound.doc_id_offset >= m_end_bound.doc_id_offset) {
				return false;
			}
		}

		doc_id = m_curr_doc_id++;

		if(m_doc_buff_offset >= m_doc_buffer.OverflowSize()) {
			int doc_num = 50;
			if(m_start_bound.doc_set >= m_end_bound.doc_set) {
				doc_num = (int)min((_int64)doc_num, m_end_bound.doc_id_offset);
			}

			_int64 curr_doc_id = m_start_bound.doc_id_offset;
			if(!m_file_set.GetDocument(m_doc_buffer, curr_doc_id, doc_num)) {
				if(!LoadNextDocumentSet()) {
					return false;
				}
			}

			m_doc_buff_offset = 0;
		}

		m_start_bound.doc_id_offset++;
		int doc_size = CFileStorage::CalculateDocumentSize
			(m_doc_buffer.Buffer() + m_doc_buff_offset);

		if((m_doc_buff_offset + doc_size) > m_doc_buffer.OverflowSize()) {
			cout<<"----------------------------------- "<<m_curr_doc_id<<endl;getchar();
			if(LoadNextDocumentSet() == false) {
				return false;
			}

			return RetrieveNextDocument(doc_id, doc_buff);
		}

		doc_buff.Resize(0);
		if(doc_size > doc_buff.OverflowSize()) {
			doc_buff.Initialize(doc_size);
		}

		doc_buff.AddToBuffer(m_doc_buffer.Buffer() + m_doc_buff_offset, doc_size);
		doc_buff.Resize(doc_size);
		
		m_doc_buff_offset += doc_size;
		return true;
	}

	// This tests the file storage mechanism with multiple file sets
	void TestDocumentDatabase() {

		CArrayList<char> doc_buff(5);
		CArrayList<int> doc_start(5);
		CLinkedBuffer<char> temp_buff;

		CArray<char> ret_buff(300);

		doc_buff.Initialize(4);
		doc_start.Initialize(4);
		doc_start.PushBack(0);

		for(int i=0; i<5; i++) {

			if(i == 3 || i == 0)continue;
			
			CFileStorage set;
			set.Initialize(CUtility::ExtendString("../test_set", i));

			for(int j=0; j<100; j++) {
				int length = (rand() % 100) + 10;
				temp_buff.Reset();

				for(int k=0; k<length; k++) {
					char ch = rand();
					doc_buff.PushBack(ch);
					temp_buff.PushBack(ch);
				}
				doc_start.PushBack(doc_buff.Size());
				set.AddDocument(temp_buff, "sdf", 3);
			}

			set.FinishFileStorage();
		}

		_int64 doc_id;
		int curr_doc = 0;
		CMemoryChunk<char> url_buff;

		for(int i=0; i<7; i++) {
			cout<<"Client "<<i<<endl;
			LoadCompiledAttrDatabase("../test_set", i, 7);

			int div_start = 0;
			int div_end = doc_start.Size() - 1;

			CHashFunction::BoundaryPartion(i, 
				7, div_end, div_start);
	
			for(int j=div_start; j<div_end; j++) {
		
				if(RetrieveNextDocument(doc_id, ret_buff) == false) {
					cout<<"Not Found";getchar();
				}

				int offset = CFileStorage::DocumentBegOffset(ret_buff.Buffer());

				if(doc_id != curr_doc) {
					cout<<"Doc ID Mismatch";getchar();
				}

				if(ret_buff.Size() - offset != doc_start[curr_doc+1] - doc_start[curr_doc]) {
					cout<<"Doc Size Mismatch";getchar();
				}
				for(int k=doc_start[j]; k<doc_start[j+1]; k++) {
					if(ret_buff[offset++] != doc_buff[k]) {
						cout<<"character mismatch";getchar();
					}
				}

				curr_doc++;
			}

			if(RetrieveNextDocument(doc_id, ret_buff) == true) {
				cout<<"No Next Doc Error";getchar();
			}
		}
	}
};

// This class is responsible for retrieving a document from the set.
// Due to the fact that documents are dispersed amongst multiple 
// document sets, it's necessary to reference each individual set.
// This is done using a tree of doc id bounds for which a particular
// document set is responsible. This is used to discover to which 
// set a particular document belongs to,
class CRetrieveDocument {

	// This stores one of the set bounds
	struct SDocSet : public S64BitBound {
		// This stores the set id
		int set_id;
	};

	// This stores the different document set bounds
	CRedBlackTree<SDocSet> m_doc_bound;
	// This stores the working directory
	CString m_directory;
	// This is a temporary buffer to store the loaded index
	CString m_temp_string;
	// This stores the set of document caches
	CMemoryChunk<CFileStorageCache> m_doc_cache;

	// This is used to compare different doc bounds
	static int CompareDocBounds(const SDocSet &arg1, const SDocSet &arg2) {

		if(arg1.start < arg2.start) {
			return -1;
		}

		if(arg1.start >= arg2.end) {
			return 1;
		}

		return 0;
	}

public:

	CRetrieveDocument() {
		m_doc_bound.Initialize(10, CompareDocBounds);
		CHitItemBlock::InitializeLRUQueue();
	}

	// This reads in the doc number for each document set
	void Initialize(const char dir[]) {

		m_directory = dir;
		SDocSet bound;
		_int64 doc_offset = 0;
		int set_id = 0;
		CFileStorage doc_set;

		while(true) {	

			try { 
				doc_set.LoadDocumentIndex(CUtility::ExtendString
					(m_directory.Buffer(), set_id));
			} catch(...) {
				break;
			}

			doc_set.FinishFileStorage();
			bound.start = doc_offset;
			doc_offset += doc_set.DocumentCount();
			bound.end = doc_offset;
			bound.set_id = set_id;

			m_doc_bound.AddNode(bound);
			set_id++;
		}

		m_doc_cache.AllocateMemory(set_id);
		for(int i=0; i<m_doc_cache.OverflowSize(); i++) {
			strcpy(CUtility::SecondTempBuffer(), CUtility::ExtendString
				(m_directory.Buffer(), i));
			m_doc_cache[i].Initialize(CUtility::SecondTempBuffer());
		}
	}

	// This retrieves a particular document from the set
	// @param doc_id - this is the global doc id being searched for
	// @param doc_buff - this stores the contents of the retrieved document
	// @return the success of the retrieval
	bool RetrieveDocument(_int64 doc_id, CMemoryChunk<char> &doc_buff) {

		static SDocSet bound;
		bound.start = doc_id;
		bound.end = doc_id + 1;

		SDocSet *res = m_doc_bound.FindNode(bound);
		if(res == NULL) {
			return false;
		}

		doc_id -= res->start;
		return m_doc_cache[res->set_id].GetDocument(doc_buff, doc_id, 1);
	}

	// This is a test framework
	void TestRetrieveDocument() {

		Initialize("GlobalData/CoalesceDocumentSets/html_text");
		CMemoryChunk<char> doc_buff;

		_int64 set[] = {
57782155,
57782159,
350577768,
57782154,
728777716,
424584316,
57782158,
314929537,
23645765,
776349867,
230000868,
274901829,
174646842,
464519090,
244144477,
622339393,
381379510,
566029424,
314929536,
115438641,
352041626,
43362556,
65062499,
225165969,
403760044,
57782160,
520823871,
212181159,
230694021,
137564625,
334558788,
777786292,
334558791,
160460535,
234372306,
791740550,
457701776,
209780581,
358912860,
230309564,
230309566,
393632720,
227920004,
230309565,
41317137,
315265623,
746930842,
59839355,
236927702,
464519098,
234372305,
352142963,
647770257,
234372302,
352142965,
370843301,
264870521,
582319766,
746930841,
204067332,
227920005,
218510024,
603712186,
315952552,
352142966,
244144475,
101498962,
394162205,
381379511,
498290703,
698668603,
561766837,
549475163,
561401289,
370843302,
647770256,
582319758,
550483047,
524483234,
48423509,
204067333,
315952588,
359435506,
273144718,
218478313,
549475159,
13893456,
-1};

		int offset = 0;
		while(true) {

		if(set[offset] < 0) {
			break;
		}
		RetrieveDocument(set[offset], doc_buff);
		offset++;

		cout<<"****************************************************"<<endl;
		for(int i=0; i<doc_buff.OverflowSize(); i++) {
			cout<<doc_buff[i];
		}

		cout<<"****************************************************"<<endl;

		}
	}
};

