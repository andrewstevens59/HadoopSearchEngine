#include "./HTMLAttribute.h"

// This class is responsible for extracting each sequential document
// that exists as part of the document database. It must decode the 
// length of each document and extract the url.
class CProcessTRECDataSet {


	// This stores the TREC data set
	CHDFSFile m_trec_file;
	// This stores the html header
	CArrayList<char> m_header_buff;
	// This stores the total number of documets parsed
	_int64 m_docs_parsed;
	// This stores the current byte offset
	int m_curr_byte_offset;

	// This extracts the html header
	bool ExtractHTMLHeader() {

		char ch;
		m_header_buff.Resize(0);
		while(m_trec_file.ReadObject(ch)) {
			m_header_buff.PushBack(ch);

			if(m_header_buff.Size() < 2) {
				continue;
			}

			if(ch == '\0') {
				m_header_buff.LastElement() = '\n';
			}

			if(ch == '\n') {
				m_header_buff.LastElement() = '\0';
			}

			if(CUtility::FindFragment(m_header_buff.Buffer() + 
				m_header_buff.Size() - 2, "\0\0", 2, 0)) {
				return true;
			}
		}

		return false;
	}

public:

	

	CProcessTRECDataSet() {
	
	}

	// This opens the TREC data set
	inline void Initialize(const char *dir) {
		m_trec_file.OpenReadFile(dir);
		m_header_buff.Initialize(4096);
		m_curr_byte_offset = 0;
		m_docs_parsed = 0;
	}

	// This returns the percentage of the file that has been parsed
	inline float DocsParsed() {
		return (float)m_curr_byte_offset / m_trec_file.ReadFileSize();
	}


	// This retrieves the next document from the set
	bool RetrieveNextDocument(CArray<char> &doc_buff, CArray<char> &url) {

		if(ExtractHTMLHeader() == false) {
			return false;
		}

		int content_len = CUtility::FindSubFragment(m_header_buff.Buffer(), "Content-Length: ", m_header_buff.Size());
		if(content_len < 0) {cout<<"nott"<<endl;
			return false;
		}

		content_len += strlen("Content-Length: ");
		content_len = CANConvert::AlphaToNumeric(m_header_buff.Buffer() + content_len);
		m_curr_byte_offset += content_len;
		if(m_curr_byte_offset >= m_trec_file.ReadFileSize()) {
			return false;
		}

		if((m_docs_parsed % CNodeStat::GetClientNum()) != CNodeStat::GetClientID()) {
			m_trec_file.SeekReadFileCurrentPosition(content_len);
			m_docs_parsed++;
			return RetrieveNextDocument(doc_buff, url);
		}

		m_docs_parsed++;
		int url_pos = CUtility::FindSubFragment(m_header_buff.Buffer(), "WARC-Target-URI: ", m_header_buff.Size());
		if(url_pos < 0) {
			m_trec_file.SeekReadFileCurrentPosition(content_len);
			return RetrieveNextDocument(doc_buff, url);
		}

		char *url_ptr = m_header_buff.Buffer() + url_pos + strlen("WARC-Target-URI: ");
		int len = strlen(url_ptr);
		if(len > url.OverflowSize()) {
			url.Initialize(len);
		}

		url.Resize(len);
		memcpy(url.Buffer(), url_ptr, url.Size());
		
		if(content_len > doc_buff.OverflowSize()) {
			doc_buff.Initialize(content_len);
		}

		doc_buff.Resize(content_len);
		return m_trec_file.ReadObject(doc_buff.Buffer(), content_len);
	}
};


// This class is responsible for retrieve documents
// from the document server so that they can be indexed.
// This includes seperating the normal text from the HTML text.
class CDocumentServer {

	// Stores the retrieved document
	CArray<char> m_document;
	// This stores the url buffer
	CArray<char> m_url;
	// This is used to extract HTML attributes from the page
	CHTMLAttribute m_html_attr;
	// This stores the document index
	CProcessTRECDataSet m_doc_set;

	// This breaks the text document up into individual html tag 
	// segments and text string segments. It then feeds each 
	// segment into HTMLAttribute for further processing.
	// @param offset - the offset in the document buffer
	void CompileWebDocument(int offset) {
		bool html_tag = false; 
		int start = offset;

		for(int i=offset; i<m_document.Size(); i++) {
			if(m_document[i] == '<') {
				if(html_tag == false) {
					m_html_attr.AddData(m_document.Buffer(), i, start);	
				}

				start = i + 1; 
				html_tag = true;
				continue;
			} 
			if(m_document[i] == '>') {
				if(html_tag == true) {
					m_html_attr.AddAttribute(m_document.Buffer(), i, start);
				}
				html_tag = false;
				start = i + 1; 
				continue;
			} 
		}

		if(html_tag == false) {
			m_html_attr.AddData(m_document.Buffer(), m_document.Size(), start);	
		}
	}

	// This runs through all of the documents that belong to this client
	int CycleThroughDocuments(int client_id, int set_id) {

		int docs_parsed = 0;
		while(true) {

			try{
				if(m_doc_set.RetrieveNextDocument(m_document, m_url) == false) {
					return docs_parsed;
				}
			} catch(...) {
				return docs_parsed;
			}
	
			if((docs_parsed % 100) == 0) {
				float perc = m_doc_set.DocsParsed() * 100;
				//cout<<"Client "<<set_id<<": "<<perc<<"%"<<endl;
			}
			

			try { 
				m_html_attr.DocInstance().SetBaseURL(m_url.Buffer(), m_url.Size());
				int pos = CUtility::FindSubFragment(m_document.Buffer(), "\n\n", m_document.Size());

				m_html_attr.Reset();
				m_html_attr.DocInstance().CreateNewDocument();
				CompileWebDocument(pos + 2);
				m_html_attr.DocInstance().FinishDocument();
				docs_parsed++;	
			} catch(...) {
			}
		}

		return docs_parsed;
	}

public:

	// This makes a connection to the document server
	CDocumentServer() {
		CUtility::Initialize();
		CHDFSFile::Initialize();
	}

	// Indexes each sequential document until finished
	// @param client_id - this is the local id of this indexer
	// @param client_num - this is the global number of global indexers
	// @param set_id - this is the id of the compiled attributes 
	//               - being produced
	// @param html_dir - this stores the directory of the html document
	void IndexDocuments(int client_id, int client_num, int set_id, const char *html_dir) {

		CWebpage::SetClientID(client_id);
		CNodeStat::SetClientID(client_id);
		CNodeStat::SetClientNum(client_num);
		

		m_url.Initialize(2048);
		m_doc_set.Initialize(html_dir);
		m_document.Initialize(2000000);
		m_html_attr.DocInstance().InitializeDocumentInstance(set_id);
	
		int doc_num = CycleThroughDocuments(client_id, set_id);

		CHDFSFile size_file;
		size_file.OpenWriteFile(CUtility::ExtendString
			("GlobalData/CompiledAttributes/doc_set_size", set_id));
		size_file.WriteObject(doc_num);
	}
};


int main(int argc, char *argv[]) {

	if(argc < 2)return 0;

	int client_id;
	int client_num;
	int set_id;
	const char *doc_set_dir;

	if(argc > 3) {

		client_id = atoi(argv[1]);
		client_num = atoi(argv[2]);
		set_id = atoi(argv[3]);
		doc_set_dir = argv[4];
		int len = strlen(doc_set_dir);

		CHDFSFile size_file;
		size_file.OpenWriteFile(CUtility::ExtendString
			("GlobalData/CompiledAttributes/dir_file", set_id));
		size_file.WriteObject(client_id);
		size_file.WriteObject(client_num);
		size_file.WriteObject(set_id);
		size_file.WriteObject(len);
		size_file.WriteObject(doc_set_dir, len);
	} else {
		
		set_id = atoi(argv[1]);
		int len;

		CHDFSFile size_file;
		size_file.OpenReadFile(CUtility::ExtendString
			("GlobalData/CompiledAttributes/dir_file", set_id));
		size_file.ReadObject(client_id);
		size_file.ReadObject(client_num);
		size_file.ReadObject(set_id);
		size_file.ReadObject(len);
		size_file.ReadObject(CUtility::ThirdTempBuffer(), len);		
		doc_set_dir = CUtility::ThirdTempBuffer();

		/*int pid;
		if((pid = fork()) != 0) {
			int status;
			waitpid(pid, &status, 0);

		} else {
			CProcessSet::ExecuteProcess("/bin/gunzip", CUtility::ExtendString("IND ", DFS_ROOT, doc_set_dir));
		}*/
	}
	
	CBeacon::InitializeBeacon(client_id, 2222);
	CMemoryElement<CDocumentServer> index;
	index->IndexDocuments(client_id, client_num, set_id, doc_set_dir);
	index.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();

	return 0;
}
