#include "./RetrieveServer.h"

// This is the main entry class for a query.
// It's responsible for parsing a text string
// and breaking it down into individual word ids.
class CParseQuery {

	// This is used to find similar doc ids for a src doc id
	CRetrieveServer m_retrieve_server;
	// This stores the query text supplied by the user
	CString m_query_text;
	// This stores the focus keywords 
	CString m_focus_text;

	// Reads a text file and indexes the terms - used
	// for initilizing stopwords
	void ReadTextFile(const char str[]) {
		// opens the file specified by the file Buffer
		CHDFSFile file;
		file.OpenReadFile(str);

		char ch;
		while(file.ReadObject(ch)) {
			cout<<ch;
		}
	}

	// This prints out a string and replaces spaces with +
	void PrintFormattedString(const char str[], int length) {
		for(int i=0; i<length; i++) {
			if(str[i] == ' ') {
				cout<<"+";
			} else {
				cout<<str[i];
			}
		}
	}

	// This prints the word id set 
	void PrintWordIDSet(SFindSimRes &arg) {

		cout<<"\"";
		for(int i=0; i<arg.word_id_num; i++) {
			cout<<arg.word_id_set[i].Value()<<"+";
		}

		cout<<"\"";
	}

	// This is the entry function that processes the results
	// @param res - this stores the list of ranked documents
	void RenderResults(CArray<SFindSimRes> &res) {

		cout<<"<head><title>Sinobi Search - "<<m_query_text.Buffer()<<"</title>";

		ReadTextFile("DyableRequest/FindSimilarQuery/Ajax.txt");
		cout<<"</head><body>";

		if(res.Size() == 0) {
			cout<<"<BR><B>No Results</B></body>"<<endl;
			return;
		}

		cout<<"<div id=\"container\" style=\"display:block\">";
		cout<<"<font size=\"0\">";
		for(int i=0; i<res.Size(); i++) {
			cout<<"<div style=\"display:none; left: 10px; top: 0px;\" id=\"excerpt"<<i<<"\"></div>";
			cout<<"<div class=\"sum\" style=\"left: 10px; display:none; zIndex:1; top: 0px;\" id=\"summary"<<i<<"\"></div>";
		}
		cout<<"</font></div>";

		cout<<"<script type=\"text/javascript\">";

		cout<<"var doc_set=new Array(";

		for(int i=0; i<res.Size(); i++) {
			if(i < res.Size() - 1) {
				cout<<res[i].doc_id.Value()<<",";
			} else {
				cout<<res[i].doc_id.Value();
			}
		}
		
		cout<<");";

		cout<<"var node_set=new Array(";

		for(int i=0; i<res.Size(); i++) {
			if(i < res.Size() - 1) {
				cout<<res[i].node_id.Value()<<",";
			} else {
				cout<<res[i].node_id.Value();
			}
		}
		
		cout<<");";

		cout<<"var word_id_set=new Array(";

		for(int i=0; i<res.Size(); i++) {
			PrintWordIDSet(res[i]);
			if(i < res.Size() - 1) {
				cout<<",";
			}
		}
		
		cout<<");";getchar();

		cout<<"var query_text=\"";
		PrintFormattedString(m_query_text.Buffer(), m_query_text.Size());
		cout<<"\";var focus_text=\"";
		PrintFormattedString(m_focus_text.Buffer(), m_focus_text.Size());
		cout<<"\";";

		ReadTextFile("DyableRequest/FindSimilarQuery/Code.txt");

		cout<<"</script></body>";
	}

public:

	CParseQuery() {
		CUtility::Initialize();
		CNameServer::Initialize();
	}

	// This is responsible for finding the search type
	// either search, image, excerpt
	void SearchQuery(char str[]) {

		char *node_text = CUtility::ExtractText(str, "node=");
		if(node_text == NULL) {
			return;
		}

		m_retrieve_server.ProcessQuery(CANConvert::AlphaToNumericLong
			(node_text, strlen(node_text)));

		char *query = CUtility::ExtractText(str, "q=");
		m_query_text += query;
		int query_length = m_query_text.Size();

		char *focus = CUtility::ExtractText(str, "f=");
		m_focus_text += focus;
		int focus_length = m_focus_text.Size();

		m_query_text.ConvertToLowerCase();
		m_focus_text.ConvertToLowerCase();

		RenderResults(m_retrieve_server.ResBuff());
	}

};



int main(int argc, char *argv[])
{
	char *data = "doc=11842&q=rainforest&f=rainforest&node=4446";
	//char *data = "q=hillary+clinton&f=";
	//char *data = getenv("QUERY_STRING");
	if(data == NULL)return 0;

	CParseQuery parse;
	CNodeStat::SetClientNum(1);

	parse.SearchQuery(data);getchar();

    return 0;
}