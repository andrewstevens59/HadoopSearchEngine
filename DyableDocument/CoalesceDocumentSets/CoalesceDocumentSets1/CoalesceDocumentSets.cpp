#include "../../../DocumentDatabase.h"

// This class reduces the set of documents sets by coalescing
// documents from multiple sets in a single set. This is used
// to keep the HTML data set from growing too large.
class CCoalesceDocumentSets {

	// Stores the retrieved document
	CArray<char> m_document;
	// This stores the document index
	CDocumentDatabase m_doc_set;
	// This stores the new document set for this client
	CFileStorage m_coalese_set;

public:

	CCoalesceDocumentSets() {
		CHDFSFile::Initialize();
	}

	// This is the entry function
	void CoalesceDocumentSets(int client_id, int client_num) {

		_int64 doc_set_id;
		int max_doc_num = -1;
		if(m_doc_set.LoadCompiledAttrDatabase("GlobalData/CompiledAttributes/comp_doc",
			client_id, client_num,max_doc_num) == false) {
				return;
		}
	
		m_coalese_set.Initialize(CUtility::ExtendString
			("GlobalData/CoalesceDocumentSets/comp_doc", client_id + 268));

		_int64 docs_parsed = 0;
		m_document.Initialize(2000000);
		CMemoryChunk<char> url_buff(2048);
		float global_perc = (float)m_doc_set.DocBound().start / m_doc_set.GlobalDocNum();
		cout<<"Total Num: "<<m_doc_set.GlobalDocNum()<<endl;
		while(m_doc_set.RetrieveNextDocument(doc_set_id, m_document)) {
			docs_parsed++;

			int url_length = CFileStorage::ExtractURL
				(m_document.Buffer(), url_buff);

			if((docs_parsed % 10000) == 0) {
				float perc = (float)docs_parsed / m_doc_set.DocNum();
				cout<<(perc * 100)<<"% Done From "<<(global_perc * 100)<<"% Total"<<endl;
			}

			if(m_document.Size() < 20000000) {
				int doc_offset = CFileStorage::DocumentBegOffset(m_document.Buffer());
				char *document = m_document.Buffer() + doc_offset;
				int doc_length = m_document.Size() - doc_offset;
				m_coalese_set.AddDocument(document, doc_length, 
					url_buff.Buffer(), url_length);
			} 
		}
	}
};

int main(int argc, char *argv[]) {

	if(argc < 3)return 0;
	int client_id = atoi(argv[1]);
	int client_num = atoi(argv[2]);
	
	CBeacon::InitializeBeacon(client_id, 2222);
	CMemoryElement<CCoalesceDocumentSets> index;
	index->CoalesceDocumentSets(client_id, client_num);
	index.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();

	return 0;
}
