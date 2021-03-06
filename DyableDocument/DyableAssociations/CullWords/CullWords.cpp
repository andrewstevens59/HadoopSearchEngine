#include "../../../MapReduce.h"

// This class takes the set of word ids generated by
// HitList and the mapped set of occurrences for each
// term. It proceeds to cull words based upon their 
// word occurrence. The document size also has to be
// updated based upon missing terms. Documents with 
// zero terms are removed all together. This reduced
// set is used to create the excerpt keywords.
class CCullWords : public CNodeStat {

	// This stores the word occurrence threshold,
	// terms with an occurrence greater than the
	// threshold are culled from the excerpt keywords
	uLong m_word_occur_thresh;

	// This stores the occurrence of each term in the set
	CSegFile m_full_excerpt_occur_map_file;
	// This stores the number of word ids that compose each document
	CHDFSFile m_full_doc_size_file;
	// This stores the hit encoding for eac word id that composes an excerpt
	CHDFSFile m_full_hit_enc_file;

	// This stores the reduced set of excerpt ids
	CSegFile m_red_excerpt_id_file;
	// This stores the reduced set of excerpt id occurrences
	CSegFile m_red_excerpt_map_file;
	// This stores the hit encoding for eac word id that composes an excerpt
	CHDFSFile m_red_hit_enc_file;
	// This stores the number of word ids that compose each document
	CHDFSFile m_red_doc_size_file;

	// This takes the set of terms and culls terms with an occcurrence
	// above a given threshold, the document size is updated accordingly.
	void ReduceDocumentSet() {

		uLong full_doc_size;
		uLong red_doc_size;
		_int64 doc_id; 

		uChar enc;
		uLong occur;
		uLong word_id;
		int space = 0;
		while(m_full_doc_size_file.GetEscapedItem(full_doc_size) >= 0) {
			m_full_doc_size_file.Get5ByteCompItem(doc_id);

			if(++space >= 10000) {
				cout<<(m_full_excerpt_occur_map_file.PercentageFileRead() * 100)<<"% Done"<<endl;
				space = 0;
			}

			red_doc_size = 0;
			for(uLong i=0; i<full_doc_size; i++) {
				m_full_excerpt_occur_map_file.ReadCompObject(word_id);
				m_full_excerpt_occur_map_file.ReadCompObject(occur);
				m_full_hit_enc_file.ReadCompObject(enc);

				if(occur > 6 && occur < m_word_occur_thresh) {
					red_doc_size++;
					m_red_excerpt_id_file.WriteCompObject(word_id);
					m_red_excerpt_map_file.WriteCompObject(word_id);
					m_red_excerpt_map_file.WriteCompObject(occur);
					m_red_hit_enc_file.WriteCompObject(enc);
				} 
			}

			if(red_doc_size > 0) {
				m_red_doc_size_file.AddEscapedItem(red_doc_size);
				m_red_doc_size_file.WriteCompObject5Byte(doc_id);
			}
		}

	}

public:

	CCullWords() {
		CHDFSFile::Initialize();
	}

	// This is the entry function
	void CullWords(int client_id, int client_num) {

		CNodeStat::SetClientID(client_id);
		CNodeStat::SetClientNum(client_num);

		CHDFSFile occur_thresh_file;
		occur_thresh_file.OpenReadFile("GlobalData/HitList/word_occur_thresh");
		occur_thresh_file.ReadObject(m_word_occur_thresh);

		m_full_excerpt_occur_map_file.OpenReadFile(CUtility::ExtendString
			("LocalData/full_excerpt_occur_map", GetClientID()));
		m_full_doc_size_file.OpenReadFile(CUtility::ExtendString
			("GlobalData/DocumentDatabase/full_doc_size", GetClientID()));
		m_full_hit_enc_file.OpenReadFile(CUtility::ExtendString
			("GlobalData/DocumentDatabase/hit_encoding", GetClientID()));

		m_red_excerpt_id_file.OpenWriteFile(CUtility::ExtendString
			("LocalData/excerpt_id", GetClientID()));
		m_red_excerpt_map_file.OpenWriteFile(CUtility::ExtendString
			("LocalData/excerpt_occurr", GetClientID()));
		m_red_hit_enc_file.OpenWriteFile(CUtility::ExtendString
			("LocalData/hit_encoding", GetClientID()));
		m_red_doc_size_file.OpenWriteFile(CUtility::ExtendString
			("GlobalData/DocumentDatabase/doc_size", GetClientID()));

		ReduceDocumentSet();
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);
	int client_num = atoi(argv[2]);

	CBeacon::InitializeBeacon(client_id);
	CMemoryElement<CCullWords> log;
	log->CullWords(client_id, client_num);
	log.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();

}
