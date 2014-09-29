#include "./DocumentInstance.h"

// This makes a request for one of the documents in the set.
// This is most likely repsonding to an ajax call.
class CDocumentQuery {

	// This stores the document buffer
	CMemoryChunk<char> m_doc_buff;
	// This stores the keyword buffer
	CMemoryChunk<char> m_keyword_buff;
	// This is used to compile the web document
	CDocumentInstance m_doc_inst;


	// This issues a request to the document server
	bool IssueRequest(COpenConnection &conn, _int64 &doc_id) {

		char success;
		conn.Send((char *)&doc_id, 5);
		conn.Receive(&success, 1);

		if(success != 's') {
			cout<<"Could Not Retrieve Document"<<endl;
			conn.CloseConnection();
			return false;
		}

		int doc_length;
		int offset = 0;
		int bytes_received;

		conn.Receive((char *)&doc_length, 4);
		m_doc_buff.AllocateMemory(doc_length);
		while(offset < doc_length) {
			conn.Receive(bytes_received, 
				m_doc_buff.Buffer() + offset, doc_length - offset);

			offset += bytes_received;
		}

		conn.CloseConnection();
		return true;
	}

	// This processes a regular display document with a given document id
	bool ProcessDisplayDocument(char *buff) {

		int len;
		const char *doc_text = CUtility::ExtractText(buff, "doc=", len);
		if(doc_text == NULL) {
			return false;
		}
		
		_int64 doc_id = CANConvert::AlphaToNumericLong(doc_text, len);

		const char *query_text = CUtility::ExtractText(buff, "q=", len);
		if(query_text == NULL) {
			return false;
		}

		COpenConnection conn;
		CNameServer::DocumentServerInst(conn);
		if(IssueRequest(conn, doc_id) == false) {
			return false;
		}

		m_doc_inst.AddQueryTerms(query_text, len);

		const char *id_set = CUtility::ExtractText(buff, "keywords=", len);

		if(id_set != NULL) {
			m_doc_inst.AddKeywordTerms(id_set, len);
		} 

		m_doc_inst.Compile(m_doc_buff);

		int excerpt = CUtility::ExtractParameter(buff, "excerpt=");
		m_doc_inst.DisplayDocument(doc_id, m_keyword_buff, excerpt);

		return true;
	}

	// This encodes a message for transmission in a url
	char *EncodeURL(const char *message) {

		int len = strlen(message);

		char *buff = CUtility::SecondTempBuffer();
		for(int i=0; i<len; i++) {
			char ch = message[i];
			*buff = 'a' + (ch & 0x0F);
			buff++;

			*buff = 'a' + (ch >> 4);
			buff++;
		}

		return CUtility::SecondTempBuffer();
	}

public:

	CDocumentQuery() {
		CNameServer::Initialize();
	}

	// This is the entry function
	void DocumentQuery(const char text[]) {

		CMemoryChunk<char> buff(strlen(text) + 1);
		strcpy(buff.Buffer(), text);

		ProcessDisplayDocument(buff.Buffer());
	}
};

int main(int argc, char *argv[])
{
	
	//char *data = "doc=229454471&q=roman^colosseum^+vespasian^titus^+colosseum^gladiators^+ancient^roman^+citizens^plebeians^+colosseum^gladiatorial^+emperor^titus^+emperor^vespasian^+ancient^rome^+colosxeums+";
	char *data = getenv("QUERY_STRING");
	if(data == NULL)return 0;

	CDocumentQuery doc;
	doc.DocumentQuery(data);

    return 0;
}