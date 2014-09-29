#include "./RetrieveServer.h"

// This stores the domain name for the server
const char *DOMAIN_NAME = "synopsis.cloud.eait.uq.edu.au";

const char *EXCLUDE_WORD = "nbsp midot amp s shtml quot and a amp www com of on or that the this to was what when where who will with the for"
	"from how in is for fromhow  in is it by  are as at i we'll we'll went were  weren't weren weve what whatever what'll whats whatve when whence"
	"whenever where whereafter whereas whereby wherein wheres whereupon great wherever whether which whichever while whilst whither who whod "
	"whoever whole who'll whom whomever who's whose why will willing wish with within without wonder wont would wouldn't wouldn want wants was"
	"wasn't wasn way we us use used useful uses using usually very than thank thanks thanx that that'll thats thats thatve the their their's"
	"them  themselves then thence there thereafter thereby thered therefore therein there'll therere theres  thereupon thereve these they theyd "
	"they'll they're they've thing things think third thirty this thorough thoroughly those though three through throughout thru  till to together "
	"too take taken taking some somebody  somehow  something sometime sometimes somewhat somewhere or other others otherwise ought oughtn't oughtn"
	"our ours ourselves much must mustn't mustn my might mightn't mightn mine make makes many may maybe mayn't mayn lest let lets is isn't isn it "
	"however itd it'll its its itself ive j k keep keeps kept know known knows had hadn't hadn has hasn't hasn have haven't haven having get gets "
	"getting given gives go goes going gone got gotten followed following follows for forever former formerly even ever evermore every everybody "
	"everyone everything everywhere do does doesn't doesn doing done don't don did didn't didn daren't daren could  couldn't couldn com come comes "
	"cause causes can't can but by be became because become becomes becoming been before beforehand are aren't aren around as as an and another "
	"any anybody anyhow anyone anything anyway anyway's anywhere ain't ain all allow allows almost also already again against about above a able";

// This class draws the response for a particular query
// given by the user. It must do a lookup on each docuement id.
class CRender {

	// This defines the maximum number of allowed terms
	static const int MAX_TERM_NUM = 4;

	// This stores the highlighted terms
	static CHashDictionary<int> m_exclude_dict;
	// This stores the query text supplied by the user (modified later)
	static CString m_query_text;
	// This storse additional query terms relating to synonyms
	static CString m_add_query_text;
	// This stores the original text supplied by the user
	static CString m_original_text;
	// This stores the did you mean phrase
	static CString m_dym_phrase;
	// This stores the did you mean url
	static CString m_dym_url;
	// This stores information supplied to the user
	// in certain circumstances
	static CString m_user_info;
	// This stores the set of duplicate terms
	static CArrayList<bool> m_duplicate_term;
	// This stores the number of query terms
	static int m_query_term_num;
	// This stores the number of exclude terms
	static int m_exclude_term_num;

	// Reads a text file and indexes the terms - used
	// for initilizing stopwords
	void ReadTextFile(const char str[]) {
		
		DFS_ROOT = "./";
		CHDFSFile file;
		file.OpenReadFile(str);

		char ch;
		while(file.ReadObject(ch)) {
			cout<<ch;
		}
	}

	// This draws the excerpts with debugging information
	void DrawDebugExcerpts() {

		CKeywordSet::InitializeExcerptKeywordSet();

		float max_keyword = 0;
		for(int i=0; i<CRetrieveServer::ResultNum(); i++) {
			max_keyword = max(max_keyword, CRetrieveServer::Doc(i).keyword_score);
		}

		cout<<"<font size=\"0\">";
		for(int i=0; i<CRetrieveServer::ResultNum(); i++) {
			cout<<"<div style=\"display:none; left: 10px; top: 0px;\" id=\"excerpt"<<i<<"\"></div>";
			cout<<"<div class=\"sum\" style=\"left: 10px; display:none; zIndex:1; top: 0px;\" id=\""<<i<<"\"></div>";
			float width = CRetrieveServer::Doc(i).keyword_score / max_keyword;
			cout<<"<div class=\"sum\" style=\"left: 10px;\">";

			cout<<"DocID: "<<CRetrieveServer::Doc(i).doc_id.Value()<<"&nbsp;&nbsp;"<<
				"Hit Score: "<<CRetrieveServer::Doc(i).hit_score<<"&nbsp;&nbsp;"
				<<"Keyword Score: "<<(width * 100)<<"%<p>";

			CKeywordSet::RenderExcerptKeywordSet(CRetrieveServer::Doc(i).local_doc_id);
			cout<<"</div>";
		}
		cout<<"</font>";
	}

	// This prints out information for the final ranked list
	void PrintRankListInfo() {

		cout<<"<table border=1>";
		for(int i=0; i<CRetrieveServer::ResultNum(); i++) {
			SQueryRes &res = CRetrieveServer::Doc(i);

			cout<<"<tr>";
			cout<<"<td><font size=\"0\">"<<res.doc_id.Value()<<"</font>";
			cout<<"<td><font size=\"0\">"<<res.node_id.Value()<<"</font>";
			cout<<"<td><font size=\"0\">"<<res.check_sum<<"</font>";
		}

		cout<<"</table>";
		cout<<"<P>";
	}

	// This displays the summary box used to refine the search by giving the 
	// user a number of suggested keyword terms
	void DisplaySummaryBox() {

		cout<<"<div class=\"sum\" style=\"left: 10px; top: 0px;\">";
		cout<<"<center><table border=0><tr><td><b>";

		strcpy(CUtility::SecondTempBuffer(), m_query_text.Buffer());
		char *tok = strtok(CUtility::SecondTempBuffer(), " ");

		while(tok != NULL) {
			if(CUtility::AskNumericCharacter(tok[0]) == false) {
				CUtility::ConvertToUpperCaseCharacter(tok[0]);
			}
			cout<<tok<<" ";
			tok = strtok(NULL, " ");
		}

		cout<<"</b>";
		for(int i=0; i<6; i++) {
			cout<<"<td><a href=\"#\" onclick=\"return Navigate("<<i<<");\" id=\"nav"<<i<<"\"></a>";
		}

		cout<<"</table><P>";

		cout<<"<table border=0 width=100%><tr>";

		for(int j=0; j<4; j++) {
			cout<<"<tr>";
			for(int i=0; i<5; i++) {
				cout<<"<td><a href=\"#\" onclick=\"return Contents("<<(i + 5 * j)<<");\" id=\"exp"<<(i + 5 * j)<<"\"></a>";
			}
		}
		cout<<"</table></center></div>";
	}

	// This stores the query results so that they can be stored 
	// in an external cache for verification purposes.
	void StoreQueryResults() {

		CHDFSFile res_file;
		res_file.OpenWriteFile(CUtility::ExtendString
			("GlobalData/Verification/", m_query_text.Buffer()));

		res_file.WriteCompObject(CRetrieveServer::ResultNum());
		for(int i=0; i<CRetrieveServer::ResultNum(); i++) {
			res_file.WriteCompObject(CRetrieveServer::Doc(i).doc_id);
			cout<<CRetrieveServer::Doc(i).keyword_score<<" ";
		}
		getchar();
	}

	// This draws the masthead includes search form
	void DrawMastHead() {

		CString temp = m_original_text;
		temp.ReplaceDelimeters();
		cout<<"<div style=\"width:70%;\"><center><form method=\"get\" action=\"http://"<<DOMAIN_NAME<<"/cgi-bin/DyableQuery\">";
		cout<<"<table cellpadding=\"0px\" cellspacing=\"0px\">";
		cout<<"<tr>";
		cout<<"<td>";
		cout<<"<a href='../index.html'><img src=\"http://"<<DOMAIN_NAME<<"/logo_small.bmp\" BORDER=0></a>";
		cout<<"</td>";
		cout<<"<td>";
		cout<<"<INPUT VALIGN=middle maxlength=2000 size=\"80\" value=\""<<temp.Buffer()<<"\" name=\"q\" style=\"font-size:25px;height:35px; border: 1px solid blue;\">"; 
		cout<<"</td>";
		cout<<"<td>";
		cout<<"<input type=\"image\" value=\"Search\" src=\"http://"<<DOMAIN_NAME<<"/search.bmp\">";
		cout<<"</td>";
		cout<<"</tr>";
		cout<<"</table>";
		cout<<"</form></center></div><hr style=\"color:blue; height:1px;\">"; 
	}

	// This draws the add keywords box
	void DrawAddKeywordBox() {

		cout<<"<center>";
		cout<<"<table cellpadding=\"0px\" cellspacing=\"0px\">";
		cout<<"<tr>";
		cout<<"<td>";
		cout<<"<INPUT type=\"text\" id=\"addkeywords\" VALIGN=middle maxlength=2000 size=\"80\" value=\"Add Additional Keywords\" name=\"q\" style=\"font-size:25px;height:35px; border: 1px solid blue;\">"; 
		cout<<"</td>";
		cout<<"<td>";
		cout<<"<input type=\"button\" onmouseover=\"setClickBox();\" onmouseout=\"clearClickBox();\" value=\"Add Keywords\" onclick=\"processKeywords();return false;\">";
		cout<<"</td>";
		cout<<"</tr>";
		cout<<"</table>";
		cout<<"</center><p>"; 
	}

	// This renders the did you mean phrase
	void RenderDidYouMeanPhrase() {

		if(CRender::DYMPhrase().Size() == 0) {
			return;
		}

		cout<<"<div class=\"sum\" style=\"left: 10px; top: 0px;\"><center><font size=4>Showing Results For: "
		"<a href=\"http://"<<DOMAIN_NAME<<"/cgi-bin/DyableQuery?q="
			<<CRender::DYMURL().Buffer()<<"\">";
		cout<<CRender::DYMPhrase().Buffer()<<"</a></font><BR>"<<endl;

		CString temp = CRender::OriginalText();
		temp.ReplaceSpaces();

		cout<<"<font size=3>Search Instead For: "
		"<a href=\"http://"<<DOMAIN_NAME<<"/cgi-bin/DyableQuery?q="
			<<temp.Buffer()<<"&spell=FALSE\">";

		temp.ReplaceDelimeters();
		cout<<temp.Buffer()<<"</a></font></center></div>"<<endl;
	}

	// This renders any information that needs to be displayed
	void RenderUserInfo() {

		if(m_user_info.Size() == 0) {
			return;
		}

		cout<<"<div class=\"sum\" style=\"left: 10px; top: 0px;\">";
		cout<<m_user_info.Buffer();
		cout<<"</div>";
	}

public:

	CRender() {
		m_duplicate_term.Initialize(20);
	}

	// This adds the query terms to the set of highlighted terms
	// @return the number of terms
	static int AddQueryTerms(char str[], int length) {

		int word_end;
		int word_start;
		m_query_term_num = 0;
		int exclude_len = strlen(EXCLUDE_WORD);
		m_exclude_dict.Initialize(1024);
		CTokeniser::ResetTextPassage(0);

		CString illegal_str;
		for(int i=0; i<exclude_len; i++) {

			if(CTokeniser::GetNextWordFromPassage(word_end, word_start, i, EXCLUDE_WORD, exclude_len)) {
				m_exclude_dict.AddWord(EXCLUDE_WORD, word_end, word_start);
			}
		}

		m_exclude_term_num = m_exclude_dict.Size();
		CTokeniser::ResetTextPassage(0);
		for(int i=0; i<length; i++) {

			if(CTokeniser::GetNextWordFromPassage(word_end, word_start, i, str, length)) {

				int word_len = word_end - word_start;
				if(word_len > 20) {
					continue;
				}

				int id = m_exclude_dict.AddWord(str, word_end, word_start);
				if(id >= 0 && id < m_exclude_term_num) {
					illegal_str.AddTextSegment(str + word_start, word_len);
					illegal_str += " ";
					continue;
				}

				if(word_len < 4 && CUtility::CountEnglishCharacters(str + word_start, word_len) == 0) {
					illegal_str.AddTextSegment(str + word_start, word_len);
					illegal_str += " ";
					continue;
				}

				if(m_exclude_dict.Size() - m_exclude_term_num == MAX_TERM_NUM + 1) {
					m_user_info = "<center><i>Sorry Limited To ";
					m_user_info += MAX_TERM_NUM;
					m_user_info += " Query Terms</i>&nbsp;&nbsp;&nbsp; Showing Results For:<B> ";
					for(int j=0; j<word_start; j++) {
						if(str[j] == '+') {
							m_user_info += " ";
						} else {
							m_user_info += str[j];
						}
					}

					m_user_info += "</b></center>";
				}

				if(m_exclude_dict.AskFoundWord() == false) {
					m_duplicate_term.PushBack(false);
					m_query_term_num++;
				} else {
					m_duplicate_term.PushBack(true);
				}

				if(m_exclude_dict.Size() - m_exclude_term_num > MAX_TERM_NUM) {
					m_duplicate_term.LastElement() = true;
				}
			}
		}

		if(illegal_str.Size() > 0) {
			m_user_info = "<center>Sorry, Synopsis presently does not index all terms, these include query term(s) <b>";
			m_user_info += illegal_str.Buffer();
			m_user_info += "</b></center>";
		}

		CExpRewServer::SetQueryTermNum(m_query_term_num);
		return m_query_term_num;
	}

	// This returns true if a given term is a duplicate term
	inline static bool IsDuplicateTerm(int id) {
		return m_duplicate_term[id];
	}

	// This returns the query term id
	inline static int QueryTermID(char str[], int length) {
		return m_exclude_dict.FindWord(str, length) - m_exclude_term_num;
	}

	// This returns the total number of terms in the set
	inline static int TotalTermNum() {
		return m_duplicate_term.Size();
	}

	// This returns the number of query terms
	inline static int QueryTermNum() {
		return m_query_term_num;
	}

	// This returns the query text (modified)
	inline static CString &QueryText() {
		return m_query_text;
	}

	// This returns the query text (modified)
	inline static CString &AdditionalQueryText() {
		return m_add_query_text;
	}

	// This returns the query text (unmodified)
	inline static CString &OriginalText() {
		return m_original_text;
	}

	// This returns the did you mean phrase
	inline static CString &DYMPhrase() {
		return m_dym_phrase;
	}

	// This returns the did you mean url
	inline static CString &DYMURL() {
		return m_dym_url;
	}

	// This renders the search page header
	void RenderSearchHeader() {

		cout<<"<head><title>Synopsis Search - "<<m_query_text.Buffer()<<"</title>";

		CKeywordSet::RenderKeywordHistogram();

		ReadTextFile("Ajax.txt");
		DrawMastHead();
		cout<<"</head>";
	}

	// This is the entry function that processes the results
	void RenderResults() {
	
		//StoreQueryResults();return;
		cout<<"<body>";

		//PrintRankListInfo();

		if(CRetrieveServer::ResultNum() == 0) {
			cout<<"<BR><B>No Results</B></body>"<<endl;
			return;
		}

		cout<<"<table cellpadding=\"0px\" cellspacing=\"0px\">";
		cout<<"<tr>";
		cout<<"<td width=\"70%\" VALIGN=\"top\">";

		RenderUserInfo();
		RenderDidYouMeanPhrase();
		DisplaySummaryBox();
		cout<<"<div class=\"sum\" onclick=\"return drawSummaryBox();\" onmouseover=\"showPassage(this);\" " 
			"onmouseout=\"hidePassage(this);\" style=\"left: 10px; top: 0px;\"><center><h3>Summary</h3></center></div>";

		cout<<"<div class=\"sum\" onclick=\"return drawSurvey();\" onmouseover=\"showPassage(this);\" " 
			"onmouseout=\"hidePassage(this);\" style=\"left: 10px; top: 0px;\"><center><h3>Plese complete this quick survey (only 5 questions)</h3></center></div>";

		DrawAddKeywordBox();
		cout<<"<div id=\"draw_excerpts\"></div>";
		cout<<"</td>";
		cout<<"<td width=\"30%\" VALIGN=\"top\">";

		cout<<"<div id=\"right_summary\" style=\"font-family:'arial'; border: 1px solid blue; background-color:white; z-index:100; position: relative; padding: 0.5em; margin: 0  0 0.75em;\"></div>";
		cout<<"</td>";
		cout<<"</tr>";
		cout<<"</table>";

		cout<<"<script type=\"text/javascript\">";
		cout<<"var domain_name=\""<<DOMAIN_NAME<<"\";";

		cout<<"var global_keyword_set=new Array(";
		CKeywordSet::RenderGlobalKeywordList(1024);
		cout<<");";
		cout<<"var global_keyword_set_occur=new Array(";
		CKeywordSet::RenderGlobalKeywordListOccur(1024);
		cout<<");";

		cout<<"var doc_set=new Array(";
		for(int i=0; i<CRetrieveServer::ResultNum(); i++) {
			if(i < CRetrieveServer::ResultNum() - 1) {
				cout<<"'"<<CRetrieveServer::Doc(i).doc_id.Value()<<"',";
			} else {
				cout<<"'"<<CRetrieveServer::Doc(i).doc_id.Value()<<"'";
			}
		}
		
		cout<<");";

		cout<<"var term_weight=new Array(";
		for(int i=0; i<CRetrieveServer::ResultNum(); i++) {
			CRetrieveServer::Doc(i).duplicate_num++;
			if(i < CRetrieveServer::ResultNum() - 1) {
				cout<<(int)CRetrieveServer::Doc(i).duplicate_num<<",";
			} else {
				cout<<(int)CRetrieveServer::Doc(i).duplicate_num;
			}
		}
		
		cout<<");";

		cout<<"var keyword_set = new Array();";
		for(int i=0; i<CRetrieveServer::ResultNum(); i++) {
			cout<<"keyword_set["<<i<<"]=new Array(";
			int doc_id = CRetrieveServer::Doc(i).local_doc_id;
			CKeywordSet::DisplayTopNKeywords(doc_id, 3);
			cout<<");";
		}

		cout<<"var cluster_keyword_set=\"";
		CKeywordSet::RenderClusterKeywordList();
		cout<<"\";";

		cout<<"var node_set=new Array(";

		for(int i=0; i<CRetrieveServer::ResultNum(); i++) {
			if(i < CRetrieveServer::ResultNum() - 1) {
				cout<<"'"<<CRetrieveServer::Doc(i).node_id.Value()<<"',";
			} else {
				cout<<"'"<<CRetrieveServer::Doc(i).node_id.Value()<<"'";
			}
		}
		
		cout<<");";

		cout<<"var query_text=\"";

		AdditionalQueryText().ReplaceSpaces();
		QueryText().ReplaceSpaces();
		cout<<AdditionalQueryText().Buffer()<<"+";
		cout<<QueryText().Buffer()<<"+";
		cout<<"\";";

		cout<<"</script><script type=\"text/javascript\" src=\"../summary.js\"></script>";

		cout<<"</body>";
	}
};
int CRender::m_query_term_num;
CString CRender::m_add_query_text;
CString CRender::m_query_text;
CString CRender::m_original_text;
CArrayList<bool> CRender::m_duplicate_term;
CString CRender::m_dym_phrase;
CString CRender::m_dym_url;
CString CRender::m_user_info;
CHashDictionary<int> CRender::m_exclude_dict;
int CRender::m_exclude_term_num;