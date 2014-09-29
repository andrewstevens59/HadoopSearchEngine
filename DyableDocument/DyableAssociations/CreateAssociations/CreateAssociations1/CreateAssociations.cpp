#include "../../../../MapReduce.h"

// This class is responsible for creating the set of associations
// from the keyword id set and the excerpt id set. It accomplishes
// this by parsing the document set and storing association terms 
// in an accyclic array.
class CCreateAssociations : public CNodeStat {

	// This stores the reduced set of excerpt ids
	CSegFile m_red_excerpt_id_file;
	// This stores the number of word ids that compose each document
	CHDFSFile m_doc_size_file;

	// This stores each of the association sets for all words
	CSegFile m_excerpt_assoc_set_file;
	// This stores the total number of associations parsed
	_int64 m_tot_assoc_num;

	// This creates the keyword associations for each of the word 
	// ids in the keyword word id set.
	// @param doc_size - the number of keywords in the document
	// @param id_set - this stores the associations
	void CreateAssociations(uLong doc_size, CCyclicArray<uLong> &id_set) {

		id_set.Reset();
		static uLong word_id;
		for(uLong i=0; i<doc_size; i++) {
			m_red_excerpt_id_file.ReadCompObject(word_id);

			m_tot_assoc_num++;
			if((m_tot_assoc_num % 10000) == 0) {
				cout<<(m_red_excerpt_id_file.PercentageFileRead() * 100)<<" % Done"<<endl;
			}

			id_set.PushBack(word_id);
			if(id_set.Size() >= id_set.OverflowSize()) {

				if(id_set[0] == id_set[1]) {
					continue;
				}

				m_excerpt_assoc_set_file.AskBufferOverflow(sizeof(uLong) * id_set.OverflowSize());
				m_excerpt_assoc_set_file.WriteCompObject(id_set[0]);
				m_excerpt_assoc_set_file.WriteCompObject(id_set[1]);
			}
		}
	}

	// This creats a set of n grouped terms so that the occurence
	// of each ossociation in the document corpus can be counted.
	// @param term_num - this is the number of terms to group in the association
	// @param assoc_file - this is the outputed set of associations
	void ParseDocumentSet() {

		uLong doc_size;
		uLong keyword_size;
		S5Byte doc_id; 

		m_doc_size_file.OpenReadFile(CUtility::ExtendString
			("GlobalData/DocumentDatabase/doc_size", GetClientID()));

		CCyclicArray<uLong> excerpt_id_buff(2);

		while(m_doc_size_file.GetEscapedItem(doc_size) >= 0) {
			m_doc_size_file.ReadCompObject(doc_id);

			CreateAssociations(doc_size, excerpt_id_buff);
		}
	}

public:

	CCreateAssociations() {
		CHDFSFile::Initialize();
	}

	// This is the entry function
	void CreateAssociations(int client_id, int client_num) {

		m_tot_assoc_num = 0;
		CNodeStat::SetClientID(client_id);
		CNodeStat::SetClientNum(client_num);

		m_red_excerpt_id_file.OpenReadFile(CUtility::ExtendString
			("LocalData/excerpt_id", GetClientID()));

		m_excerpt_assoc_set_file.OpenWriteFile(CUtility::ExtendString
			("LocalData/excerpt_assoc_file0.client", GetClientID()));

		ParseDocumentSet();
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);
	int client_num = atoi(argv[2]);

	CBeacon::InitializeBeacon(client_id);
	CMemoryElement<CCreateAssociations> set;
	set->CreateAssociations(client_id, client_num);
	set.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();
}