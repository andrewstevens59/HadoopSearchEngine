#include "../NameServer.h"

// This finds the text string for a given keyword id.
class CKeywordQuery {

	// This stores the connection to the document server
	COpenConnection m_conn;

public:

	CKeywordQuery() {
		cout<<"Content-type: text/html\n\n";
		CNameServer::Initialize();
		CUtility::Initialize();
	}

	// This issues the request
	void IssueRequest(const char *buff) {

		int len;
		CNameServer::TextStringServerInst(m_conn);
		const char *keyword_text = CUtility::ExtractText(buff, "node=", len);
		if(keyword_text == NULL) {
			return;
		}

		_int64 keyword_id = CANConvert::AlphaToNumericLong(keyword_text, len);

		int match_num = 1;
		uChar word_length;
		static char request[20] = "WordIDRequest";

		m_conn.Send(request, 20);
		m_conn.Send((char *)&match_num, sizeof(int));

		m_conn.Send((char *)&keyword_id, sizeof(S5Byte));
		m_conn.Receive((char *)&word_length, sizeof(uChar));
		m_conn.Receive(CUtility::SecondTempBuffer(), word_length);

		if(CUtility::SecondTempBuffer()[word_length-1] == '^') {
			word_length--;
		}

		CUtility::SecondTempBuffer(word_length) = '\0';
		char *tok = strtok(CUtility::SecondTempBuffer(), "^");

		while(tok != NULL) {
			if(CUtility::AskNumericCharacter(tok[0]) == false) {
				CUtility::ConvertToUpperCaseCharacter(tok[0]);
			}
			cout<<tok;
			tok = strtok(NULL, "^");
			if(tok != NULL) {
				cout<<"^";
			}
		}

		m_conn.CloseConnection();
	}

};

int main(int argc, char *argv[])
{
	//char *data = "node=48267";
	char *data = getenv("QUERY_STRING");
	if(data == NULL)return 0;

	CKeywordQuery keyword;
	keyword.IssueRequest(data);
    return 0;
}