#include "./TextStringServer.h"

// This is the main entry class for a query.
// It's responsible for parsing a text string
// and breaking it down into individual word ids.
class CParseQuery {

	// This draws the final results
	CRender m_render_res;
	// This is used to retrieve the set of word ids for each query term
	CTextStringServer m_retrieve_word_set;
	// This is used to contact the retrieve servers
	CRetrieveServer m_retrieve_server;
	// This is used to measure the different component times
	CStopWatch m_timer;

	
	// This creates the final list of results and gives the user the results
	void RenderResults() {

		m_retrieve_server.ProcessQuery(m_retrieve_word_set.QueryTermNum());

		/*cout<<"<h4>Word ID Set: ";
		for(int i=0; i<CRetrieveServer::WordIDSet().Size(); i++) {
			cout<<CRetrieveServer::WordIDSet()[i].word_id.Value()<<" "<<
				CRetrieveServer::WordIDSet()[i].factor<<"<BR>";
		}
		cout<<"</h4>";*/

		m_render_res.RenderSearchHeader();
		m_retrieve_word_set.CreateDidYouMeanPhrase();
		m_render_res.RenderResults();
	}

public:

	CParseQuery() {
		cout<<"Content-type: text/html\n\n";
		CUtility::Initialize();
		CNameServer::Initialize();
	}

	// This is responsible for finding the search type
	// either search, image, excerpt
	void SearchQuery(char string[]) {

		time_t t;
		time(&t);
		FILE *fp = fopen("query_log.txt", "a+");
		if(fp != NULL) {
			fprintf(fp, "%s %s\n", string, asctime(localtime(&t)));
			fclose(fp);
		}

		int src_doc_id = CUtility::ExtractParameter(string, "Source=");

		m_retrieve_word_set.ParseQuery(string);

		if(CRetrieveServer::WordIDSet().Size() == 0) {
			m_render_res.RenderSearchHeader();
			cout<<"<BR><B>No Results</B>"<<endl;
			return;
		}

		RenderResults();
	}

};

int main(int argc, char *argv[])
{
	char *data = NULL;

	if(argc > 1) {
		data = argv[1];
		CBeacon::InitializeBeacon(atoi(argv[2]), 2222);
	} else {
		data = getenv("QUERY_STRING");
	}

	static const char *query[] = {"cold+war", "egypt+pyramids", "farming+agriculture", "global+warming",
			"roman+emperor", "nervous+system", "neural+networks", "saturated+fats", "amino+acids", "photosynthesis"};

	//data = CUtility::ExtendString("q=", query[3]);
	//data = "q=caffeine";

	if(data == NULL)return 0;

	CParseQuery parse;

	//try {
		parse.SearchQuery(data);
	//} catch(...) {
	//}

	if(argc > 1) {
		CBeacon::SendTerminationSignal();
	}

    return 0;
}