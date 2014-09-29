#include "./DocumentInstance.h"

// This class is responsible for retrieve documents
// from the document server so that they can be indexed.
// This includes seperating the normal text from the HTML text.
class CDocumentServer {

	// Stores the retrieved document
	CArray<char> m_document;
	// This is used to compile the attribute types
	CDocumentInstance m_doc_inst;
	// This stores the document index
	CDocumentDatabase m_doc_set;

public:

	// This makes a connection to the document server
	CDocumentServer() {
		m_document.Initialize(2000000);
		CUtility::Initialize();
		CHDFSFile::Initialize();
	}

	// returns the length of the document
	inline int DocumentLength() {
		return m_document.Size();
	}

	// Indexes each sequential document until finished
	// @param client_id - this is the local id of this indexer
	// @param client_num - this is the global number of global indexers
	// @param log_div_num - this is the number of log divisions
	// @param max_doc_num - this is the maximum number of documents to index
	void IndexDocuments(int client_id, int client_num, 
		int log_div_num, int max_doc_num = -1) {

		_int64 doc_set_id;
		CWebpage::SetClientID(client_id);
		CNodeStat::SetHashDivNum(log_div_num);

		if(m_doc_set.LoadHTMLDatabase
			("GlobalData/CoalesceDocumentSets/html_text", "",
			client_id, client_num, 0, max_doc_num) == false) {
				return;
		}

		CMemoryChunk<char> url_buff(2048);
		m_doc_inst.InitializeDocumentInstance();

		_int64 docs_parsed = 0;
		_int64 temp_doc_id = -1;
		float global_perc = (float)m_doc_set.DocBound().start / m_doc_set.GlobalDocNum();
		cout<<m_doc_set.DocBound().start<<" "<<m_doc_set.DocBound().end<<endl;
		while(m_doc_set.RetrieveNextDocument(doc_set_id, m_document)) {
			docs_parsed++;

			temp_doc_id = doc_set_id;
			int url_length = CFileStorage::ExtractURL
				(m_document.Buffer(), url_buff);

			m_doc_inst.SetBaseURL(url_buff.Buffer(), url_length);

			if((docs_parsed % 10000) == 0) {
				float perc = (float)docs_parsed / m_doc_set.DocNum();
				cout<<"Client "<<client_id<<" "<<(perc * 100)
					<<"% Done From "<<(global_perc * 100)<<"% Total"<<endl;
			}


			int doc_offset = CFileStorage::DocumentBegOffset(m_document.Buffer());
			char *doc_ptr = m_document.Buffer() + doc_offset;
			int doc_length = m_document.Size() - doc_offset;

			// starts a new document instance
			m_doc_inst.ParseDocumentType(doc_set_id, doc_ptr, doc_length);

			m_doc_inst.FinishDocument();

		}

		cout<<"Finish "<<client_id<<" ------------------------------------"<<endl;

	
		if(temp_doc_id != m_doc_set.DocBound().end - 1) {
			cout<<"Doc ID Mismatch "<<doc_set_id<<" "<<m_doc_set.DocBound().end;getchar();
		}
		if(docs_parsed != m_doc_set.DocBound().Width()) {
			cout<<"Doc Num Mimstah";getchar();
		}
	}
};

int main(int argc, char *argv[]) {

	if(argc < 3)return 0;
	int client_id = atoi(argv[1]);
	int client_num = atoi(argv[2]);
	int log_div_num = atoi(argv[3]);
	int max_doc_num = -1;
	
	if(argc > 4) {
		max_doc_num = atoi(argv[4]);
	}
	
	CBeacon::InitializeBeacon(client_id, 2222);
	CMemoryElement<CDocumentServer> index;
	index->IndexDocuments(client_id, client_num, log_div_num, max_doc_num);
	index.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();

	return 0;

}
