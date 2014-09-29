#include "../../ProcessSet.h"

// define an image hit type attribute
#define IMAGE_TYPE_HIT 0x01
// defines a keyword or description type attribute
#define META_TYPE_HIT 0x02
// defines link anchor type attribute
#define LINK_TYPE_HIT 0x04
// defines a capital word
#define CAPITAL_HIT 0x08

// Stores the character bounds with a document
struct STextBound {
	// This stores the start of the text boundary
	char *start_ptr;
	// This stores the end of the text boundary
	char *end_ptr;

	inline void Initialize() {
		start_ptr = NULL;
		end_ptr = NULL;
	}

	// This returns the number of characters in the bound
	inline int Width() {
		return (int)(end_ptr - start_ptr);
	}

	// This sets the start and end ptr
	void Set(char *start, char *end) {
		start_ptr = start;
		end_ptr = end;
	}
};

// This stores a link text bound 
struct SLinkBound {
	// this stores the url associated with the link
	STextBound url;
	// this stores the anchor text associated with the link
	STextBound anchor;
};

// Defines font statistics relating
// to the average font size and max font size
struct SFont {
	// stores the average font size
	int avg_font; 
	// stores the maximum font size
	int max_font; 

	inline int Average() {
		return (max_font + avg_font) >> 1; 
	}

	inline int Width() {
		return max_font - avg_font; 
	}
}; 

// Defines a webpage to store attributes specific to the webpage. This 
// includes the URL of the webpage. Also this checks valid and invalid
// URL types that are to be indexed in the webgraph. Principally this
// class is responsible for creating the hit information that is used
// later by HitList to create the final hit items. Before this can 
// happen though all text strings need to be assigned unique id's. Each
// hit item is broken down into a title hit, excerpt hit, normal hit and
// image hit. This information is defined by the hypertext on the webpage.
class CWebpage {

	
	// stores information relating to a word
	// in a webpage
	struct SDocumentWordHit {
		// stores the log file word division
		int word_div; 
		// stores the encoding for the word
		// such as anchor text image text 
		uChar term_type; 
		// stores the word id for the given term
		int word_id; 
		// stores the font weight for the given term
		int font_size; 

		// returns true if the word has already
		// been assigned a word id through the lexon
		inline bool AskStopWord() {
			return word_id >= 0; 
		}

		// returns true if an image hit
		inline bool AskImageHit() {
			return (term_type & IMAGE_TYPE_HIT) == IMAGE_TYPE_HIT; 
		}

		// returns true if an meta type hit
		inline bool AskMetaHit() {
			return (term_type & META_TYPE_HIT) == META_TYPE_HIT; 
		}

		// returns true if a link type hit
		inline bool AskLinkHit() {
			return (term_type & LINK_TYPE_HIT) == LINK_TYPE_HIT; 
		}

		// returns true if a the word contains a 
		// capital at the start of the word
		inline bool AskCapitalHit() {
			return (term_type & CAPITAL_HIT) == CAPITAL_HIT; 
		}
	}; 

	// stores the url for the webpage
	CArray<char> m_base_url; 
	// stores the client id
	static int m_client_id; 

	// stores the start of the server in the base url
	int m_server_start;
	// used to store illegal characters in a url
	CMemoryChunk<bool> m_illegal_url_char;
	// stores a webpage type 
	CTrie m_webpage_type; 

	// This stores the compiled document
	CFileStorage m_comp_doc;
	// This stores the compiled document
	CLinkedBuffer<char> m_doc_text;
	// This stores the document title , stored with each excerpt
	STextBound m_title_bound;

	// Calculates the term weight of a given hit item scales the hit in 
	// the domain [1 7]. The hit weight is a combination of font size 
	// and html characteristics such as anchor hits and meta tag description
	// words. Note only two term size weights are used one for max and middle.
	// @param hit_item - a given hit item contained in the document
	// @param font_stat - stores the max and average statistics overall
	// @return the hit weight in domain [1 7]
	inline uChar CalculateHitWeight(SDocumentWordHit &hit_item, SFont &font_stat) {

		uChar hit_weight = 1; 
		// calculates the term weight
		if(hit_item.font_size < font_stat.avg_font) {
			hit_item.font_size = font_stat.avg_font;
		}
		float term_weight = (float)(hit_item.font_size - 
			font_stat.avg_font) / font_stat.Width(); 

		hit_weight += (uChar)(term_weight * 3); 

		if(hit_item.AskStopWord()) {
			return hit_weight; 
		}

		// keywords and description second highest
		if(hit_item.AskMetaHit()) {
			hit_weight += 3; 
		}

		// anchor text third highest
		if(hit_item.AskLinkHit()) {
			hit_weight += 2; 
		}

		// anchor text fourth highest
		if(hit_item.AskImageHit()) {
			hit_weight += 2; 
		}

		// adds the capital bit 
		if(hit_item.AskCapitalHit() && hit_weight < 7) {
			hit_weight++; 
		}

		return hit_weight; 
	}

	// Returns the number of directory levels to which
	// a given url matches the base url
	// @param url - the url being compared with the base url
	// @param length - the length of the url being compared
	int BaseServerMatch(char url[], int length) {

		length = min(length , m_base_url.Size());
		int server_match = 0;

		int offset = m_server_start;
		while(offset < length) {
			if(m_base_url[offset] == '/') {
				server_match++;
			}
			if(m_base_url[offset] != url[offset]) {
				break; 
			}

			offset++;
		}

		if(offset >= length && !server_match) {
			server_match = 1;
		}

		return (server_match > 3) ? 3 : server_match; 
	}

	// This copies one of the attribute types
	// @param atribute - this is the attribute being added
	// @param tag - this is the attribute type
	void AddAttribute(CArray<SLinkBound> &attribute, const char tag[]) {

		if(attribute.Size() == 0) {
			return;
		}

		int size = attribute.Size();
		m_doc_text.CopyBufferToLinkedBuffer(tag, strlen(tag));
		m_doc_text.CopyBufferToLinkedBuffer((char *)&size, 4);

		for(int i=0; i<attribute.Size(); i++) {
			SLinkBound &bound = attribute[i];
			// stores the url
			size = bound.url.end_ptr - bound.url.start_ptr;
			m_doc_text.CopyBufferToLinkedBuffer((char *)&size, 4);
			m_doc_text.CopyBufferToLinkedBuffer(bound.url.start_ptr, size);

			// stores the anchor text
			size = bound.anchor.end_ptr - bound.anchor.start_ptr;
			m_doc_text.CopyBufferToLinkedBuffer((char *)&size, 4);
			m_doc_text.CopyBufferToLinkedBuffer(bound.anchor.start_ptr, size);
		}
	}

	// This copies one of the attribute types
	// @param atribute - this is the attribute being added
	// @param tag - this is the attribute type
	void AddAttribute(CArray<STextBound> &attribute, const char tag[]) {

		if(attribute.Size() == 0) {
			return;
		}

		int size = attribute.Size();
		m_doc_text.CopyBufferToLinkedBuffer(tag, strlen(tag));
		m_doc_text.CopyBufferToLinkedBuffer((char *)&size, 4);

		for(int i=0; i<attribute.Size(); i++) {
			STextBound &bound = attribute[i];
			size = bound.end_ptr - bound.start_ptr;
			m_doc_text.CopyBufferToLinkedBuffer((char *)&size, 4);
			m_doc_text.CopyBufferToLinkedBuffer(bound.start_ptr, size);
		}
	}

	// This adds each of the excerpts as a seperate document that can later
	// be retrieved. Also stored along with each excerpt is it's id amongst
	// other excerpts that belong to the same document. This allows other
	// excerpts to be discovered that belong to the same document.
	// @param excerpt_bound - this stores the text bounds for all excerpts
	void AddExcerpts(CArray<STextBound> &excerpt_bound) {

		for(int i=0; i<excerpt_bound.Size(); i++) {

			m_doc_text.Restart();
			m_doc_text.CopyBufferToLinkedBuffer("<e>");

			int size = excerpt_bound.Size();
			// this stores information that connects all the excerpts
			// in a document together
			m_doc_text.CopyBufferToLinkedBuffer((char *)&size, 4);
			m_doc_text.CopyBufferToLinkedBuffer((char *)&i, 4);

			STextBound &bound = excerpt_bound[i];
			size = bound.end_ptr - bound.start_ptr;
			m_doc_text.CopyBufferToLinkedBuffer((char *)&size, 4);

			/*while(bound.start_ptr < bound.end_ptr) {
				cout<<*bound.start_ptr;
				bound.start_ptr++;
			}
getchar();*/
			int title_size = 0;
			if(m_title_bound.start_ptr != NULL && m_title_bound.end_ptr != NULL && m_title_bound.Width() < 100) {
				title_size = abs(m_title_bound.Width());
				m_doc_text.CopyBufferToLinkedBuffer((char *)&title_size, sizeof(u_short));
				m_doc_text.CopyBufferToLinkedBuffer(m_title_bound.start_ptr, m_title_bound.Width());
			} else {
				m_doc_text.CopyBufferToLinkedBuffer((char *)&title_size, sizeof(u_short));
			}

			m_doc_text.CopyBufferToLinkedBuffer(bound.start_ptr, size);

			m_comp_doc.AddDocument(m_doc_text, BaseURL().Buffer(), BaseURL().Size());
		}
	}

public:

	CWebpage() {
	}

	
	// This initializes the webpage at the very start. This includes loading
	// in the set of illegal web page type extensions.
	// @param set_id - this is the set id for the current
	//               - file storage for this client
	void InitializeWebpage(int set_id) {
		m_base_url.Initialize(2048); 
		m_webpage_type.Initialize(4); 
		m_doc_text.Initialize(2048);

		m_comp_doc.Initialize(CUtility::ExtendString
			("GlobalData/CompiledAttributes/comp_doc", set_id));

		m_illegal_url_char.AllocateMemory(256);
		char char_set[] = "#@&;\"$,\n\t![]()*|{}\0";
		// remember strlen stops when finds \0 -> must be length + 1
		CUtility::AddTextStringTypeSet(char_set,
			m_illegal_url_char.Buffer(), (int)strlen(char_set) + 1);

		char link_extension[][7] = {"css", "rss", "svg", "jsp", "ppt", "doc", "pt", "aspx", "rdf", "fp", 
			"jpg", "gif", "bmp", "pdf", "zip", "sit", "gz", "tar", "avi", "ram", "mpg", "mpeg", 
			"mov", "qt", "mp3", "mp4", "ico", "wma", "dtd", "js", "aiff", "xml", "wav", "ra", "//"}; 

		int index = 0; 
		while(!CUtility::FindFragment(link_extension[index], "//")) {
			int length = (int)strlen(link_extension[index]); 
			m_webpage_type.AddWord(link_extension[index++], length); 
		} 
	}

	// This sets the title for the document
	// @paraam text_starta_ptr - this is a ptr to the first character in the title
	// @param text_end_ptr - this is a ptr to the last character in the title
	inline void SetTitle(char *text_start_ptr, char *text_end_ptr) {
		m_title_bound.start_ptr = text_start_ptr;
		m_title_bound.end_ptr = text_end_ptr;
	}

	// Sets the base url for the webpage
	// @param url - the url text string
	// @param length - the length of the url
	void SetBaseURL(char url[], int length) {

		m_title_bound.Initialize();
		if(CUtility::FindFragment(url, "http://")) {
			length -= 7;
			url += 7;
		}

		if(length > m_base_url.OverflowSize()) {
			m_base_url.Initialize(length);
		}

		memcpy(m_base_url.Buffer(), url, length); 
		m_base_url.Resize(length); 

		m_server_start = 0;
		while(m_base_url[m_server_start++] != '.') {
			if(m_server_start >= m_base_url.Size()) {
				m_server_start = 0;
				break;
			}
		}
	}

	// Retrieves the base url for the webpage
	// @param url - the buffer to store the url
	// @return a buffer containing the base url
	inline CArray<char> &BaseURL() {
		return m_base_url; 
	}

	// Returns true if a given letter is not allowed in
	// a particular url, false otherwise
	// @param letter - the ASCII letter being examined
	inline bool AskIllegalURLCharacter(char letter) {
		return m_illegal_url_char[(int)(letter + 128)]; 
	}

	// Used to set the client id
	// @param client_id - the id of the current client
	static inline void SetClientID(int client_id) {
		m_client_id = client_id; 
	}

	// Gets the client id
	// @return the client id
	inline int GetClientID() {
		return m_client_id; 
	}

	// This returns the number of indexed documents
	inline _int64 DocumentCount() {
		m_comp_doc.DocumentCount();
	}

	// This compiles the list of attributes into multiple documents.
	// Specifically a header document is created for each webpage that
	// stores title hits. Seperate document instances are created for
	// each excerpt that are linked to the header document. This meta
	// document information is reparsed during the indexing phase.
	// @param excerpt_bound - this stores the text bounds for all excerpts
	// @param image_bound - this stores the text bounds for all images
	// @param link_bound - this stores the text bounds for all links
	// @param title_bound - this stores the text bounds for all titles
	// @param meta_bound - this stores the text bounds for all meta text
	bool ProcessDocument(CArray<STextBound> &excerpt_bound, 
		CArray<SLinkBound> &image_bound, CArray<SLinkBound> &link_bound,
		CArray<STextBound> &title_bound, CArray<STextBound> &meta_bound) {

		if(excerpt_bound.Size()  == 0 && link_bound.Size() == 0) {
			// don't index the document if it has no value
			return false;
		}

		m_doc_text.Restart();
		m_doc_text.CopyBufferToLinkedBuffer("<s>");
		int excerpt_size = excerpt_bound.Size();
		m_doc_text.CopyBufferToLinkedBuffer((char *)&excerpt_size, 4);

		AddAttribute(meta_bound, "<m>");
		AddAttribute(link_bound, "<l>");
		AddAttribute(title_bound, "<t>");
		AddAttribute(image_bound, "<i>");

		// first add the document stub
		m_comp_doc.AddDocument(m_doc_text, BaseURL().Buffer(), BaseURL().Size());

		if(excerpt_bound.Size() > 0) {
			AddExcerpts(excerpt_bound);
		}

		return true;
	}

	// Checks the link characters are valid. This is done by scanning
	// through the url string and looking for invalid characters.
	bool CheckLinkCharacters(char str[], int length, int start = 0) {

		int english_character = 0;
		for(int i=start; i<length; i++) {
			if(AskIllegalURLCharacter(str[i])) {
				return false;
			}

			if(CUtility::AskEnglishCharacter(str[i])) {
				english_character++;
			}
		}

		return english_character > ((length - start) * 0.5f);
	}

	// Checks a link type extension is valid. This is done by checking
	// against a list of invalid link type extensions.
	// @param str - a buffer containing the url
	// @param length - the length of the url in bytes
	// @param start - a byte offset to the start of the url
	bool CheckLinkType(char str[], int length, int start=0) {

		int base_offset = length - 7;
		if(base_offset < start) {
			base_offset = start;
		}

		//gets the file type
		for(int i=length-1; i>base_offset; i--) {
			if((str[i] == '.') || (str[i] == '/')) {
				int index = m_webpage_type.FindWord(str, length, i + 1); 
				if(m_webpage_type.AskFoundWord()) {
					return false; 	
				}

				return true; 
			}
		}

		return true; 
	}

}; 
int CWebpage::m_client_id;
