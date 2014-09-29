#include "../../MapReduce.h"

// This is really just a super class that creates the mapping 
// from local indexes to global indexes. A local index is created
// for each word and url string supplied by one of the HashDictionaries
// in the LogFile stage. Each of these local indexes need to be 
// mapped to unique global indexes. This is done by simply adding up
// the different hash dictionary sizes and adding the local index
// to the current hash dictionary offset.
class CCreateFinalHitList : public CNodeStat {

	// stores the log file associated with the link url
	CMemoryChunk<CHDFSFile> m_link_url_file;
	// stores the log file associated with the non stopwords
	CMemoryChunk<CHDFSFile> m_word_file;
	// stores the global dictionary offset for each of the log types
	CArray<SGlobalIndexOffset> m_dict_offset;
	// This stores the number of base urls
	_int64 m_base_url_num;

	// Loads one of the log files
	// @param comp - the comp buffer to add the text string to
	// @param str - the name of the comp buffer
	inline void LoadLogFile(CMemoryChunk<CHDFSFile> &comp, const char str[]) {

		comp.AllocateMemory(CNodeStat::GetHashDivNum());

		for(int i=0; i<CNodeStat::GetHashDivNum(); i++) {
			cout<<"Load "<<i<<" Out of "<<CNodeStat::GetHashDivNum()<<endl;
			comp[i].OpenReadFile(CUtility::ExtendString
				(str, GetClientID(), ".div", i));
		}
	}

public:

	CCreateFinalHitList() {
		CHDFSFile::Initialize();
	}

	// This is called once at the very start. It loads in the dictionary size
	// information that is used to map local indexes to global indexes. It
	// also loads the link urls, base urls and word log files.  It also 
	// writes the final number of base doc id's and the total number of nodes.
	void LoadFinalIndex() {

		uLong word_num;
		_int64 link_num;

		CHDFSFile file;
		file.OpenReadFile("GlobalData/WordDictionary/dictionary_offset");
		// writes the number of base nodes
		file.ReadCompObject(m_base_url_num);
		// writes the total number of nodes
		file.ReadCompObject(link_num);
		file.ReadCompObject(word_num);
		m_dict_offset.ReadArrayFromFile(file);

		// loads each of the different log files
		LoadLogFile(m_word_file, "GlobalData/ProcessedLogFile/word_log");
		LoadLogFile(m_link_url_file, "GlobalData/ProcessedLogFile/link_url_log");
	}

	// Returns true if the link has been spidered
	// @param link_index - the link index on a given document
	inline bool AskSpidered(_int64 &link_index) {
		return link_index < URLBaseNum();
	}

	// Finds the global url index corresponding to some
	// local index in a particular log division. Remember
	// that a base url has been encoded as a 1 in the first
	// bit of a local url index in log file (needs to be removed).
	// @param local_index - the given dictionary index
	// @param log_div - the log file division
	inline _int64 GlobalURLIndex(_int64 &local_index, int log_div) {
		//checks for base url index
		if((local_index & 0x01) == 0) {
			return local_index >> 1;
		}

		return m_dict_offset[log_div].link_offset + (local_index >> 1); 
	}

	// Finds the global word index corresponding to some
	// local index in a particular log division
	// @param local_index - the given dictionary index
	// @param log_div - the log file division
	inline int GlobalWordIndex(int local_index, int log_div) {
		return m_dict_offset[log_div].word_offset + local_index;
	}

	// Retrieves the next link url
	// @param div - the log division
	// @param index - the link url index
	inline void RetrieveNextLinkURLIndex(int div, _int64 &index) {
		m_link_url_file[div].GetEscapedItem(index);
	}

	// Retrieves the next base url
	// @param div - the log division
	// @param index - the word index
	inline void RetrieveNextWordIndex(int div, uLong &index) {
		m_word_file[div].GetEscapedItem(index);
	}

	// Returns the number of words that have been indexed
	// include all the stopwords initially in the lexon
	inline int WordNum() {
		return m_dict_offset.LastElement().word_offset;
	}

	// Returns the number of base urls
	inline _int64 URLBaseNum() {
		return m_base_url_num;
	}
};