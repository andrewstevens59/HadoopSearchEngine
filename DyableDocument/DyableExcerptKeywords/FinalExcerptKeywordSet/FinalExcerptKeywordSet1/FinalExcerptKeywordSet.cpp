#include "../../../../MapReduce.h"

// This reduces the keyword set at each level in the hiearchy
// into a final keyword set that is used throughout. This is
// done using a limited priority queue. In addition the 
// excerpt hit list is created along with the keyword id list.
class CFinalExcerptKeywordSet : public CNodeStat {

	// This stores a particular term in the set 
	struct SKeyword {
		// This stores the keyword id
		S5Byte id;
		// This stores the group size
		uChar group_size;
		// This stores the occurrence
		uLong occurr;
		// This stores the term weight
		float term_weight;
	};

	// This is used to sort keywords
	struct SKeywordPtr {
		SKeyword *ptr;
	};

	// This stores the list of priority tokens
	CLimitedPQ<SKeywordPtr> m_tok_queue;
	// This is used to remove duplicate terms
	CTrie m_duplicate_map;
	// This is used to store the keyword set
	CLinkedBuffer<SKeyword> m_keyword_buff;
	// This stores the set of nodes in the queue
	CArray<SKeywordPtr> m_tok_buff;

	CMemoryChunk<CHDFSFile> m_excerpt_level_set;
	// This stores the final excerpt set
	CSegFile m_excerpt_file;
	// This stores the set of group term hits
	CSegFile m_group_term_file;
	// This stores the pulse scores in document order
	CSegFile m_pulse_score_file;

	// This is used to compare tokens on word occurrence
	static int CompareKeywords(const SKeywordPtr &arg1, const SKeywordPtr &arg2) {

		if(arg1.ptr->group_size < arg2.ptr->group_size) {
			return -1;
		}

		if(arg1.ptr->group_size > arg2.ptr->group_size) {
			return 1;
		}

		if(arg1.ptr->term_weight < arg2.ptr->term_weight) {
			return -1;
		}

		if(arg1.ptr->term_weight > arg2.ptr->term_weight) {
			return 1;
		}

		if(arg1.ptr->occurr < arg2.ptr->occurr) {
			return 1;
		}

		if(arg1.ptr->occurr > arg2.ptr->occurr) {
			return -1;
		}

		return 0;
	}

	// This adds one of the terms to the queue
	inline void AddKeyword(SKeywordPtr &ptr, S5Byte &doc_id) {

		m_duplicate_map.AddWord((char *)&ptr.ptr->id, sizeof(S5Byte));
		if(m_duplicate_map.AskFoundWord()) {
			return;
		}

		m_tok_queue.AddItem(ptr);
	}

	// This creates the final keyword set for an excerpt
	// @param check_sum - this is the check_sum for this excerpt
	//                  - used to enfore uniqueness
	void WriteFinalKeywordSet(S5Byte &doc_id) {

		uLong check_sum = 0;
		m_tok_queue.CopyNodesToBuffer(m_tok_buff);

		for(int i=0; i<m_tok_buff.Size(); i++) {
			check_sum += m_tok_buff[i].ptr->id.Value();
		}

		int ovf_size = sizeof(S5Byte) + sizeof(uLong) + sizeof(uChar);
		ovf_size += sizeof(S5Byte)  * m_tok_buff.Size();
		m_excerpt_file.AskBufferOverflow(ovf_size);
		m_excerpt_file.WriteCompObject(doc_id);
		m_excerpt_file.WriteCompObject(check_sum);
		m_excerpt_file.WriteCompObject((uChar)m_tok_buff.Size());

		static SKeywordHitItem hit_item;
		static SPulseMap pulse_map;

		pulse_map.ReadPulseMap(m_pulse_score_file);
		while(pulse_map.node < doc_id) {
			pulse_map.ReadPulseMap(m_pulse_score_file);
		}

		if(pulse_map.node != doc_id) {
			cout<<"Pulse Doc Mismatch "<<pulse_map.node.Value()<<" "<<doc_id.Value();getchar();
		}

		for(int i=0; i<m_tok_buff.Size(); i++) {

			SKeyword *ptr = m_tok_buff[i].ptr;
			hit_item.keyword_score = ptr->group_size;
			hit_item.keyword_score += 1.0f / ptr->occurr;

			m_excerpt_file.WriteCompObject(ptr->id);

			hit_item.doc_id = doc_id;
			hit_item.keyword_id = ptr->id;
			hit_item.check_sum = check_sum;
			hit_item.pulse_score = pulse_map.pulse_score;

			hit_item.WriteKeywordHit(m_group_term_file);
		}
	}

	// This parses the set of excerpt keywords in order to join terms
	void ParseExerptKeywordSet() {

		S5Byte doc_id;
		_int64 temp_doc_id;
		uChar keyword_num;
		SKeywordPtr ptr;

		while(true) {
			temp_doc_id = -1;
			m_duplicate_map.Reset();
			m_keyword_buff.Restart();
			m_tok_queue.Reset();

			for(int i=0; i<m_excerpt_level_set.OverflowSize(); i++) {
				CHDFSFile &excerpt_keyword_file = m_excerpt_level_set[i];

				if(!excerpt_keyword_file.ReadCompObject(doc_id)) {
					return;
				}

				if(temp_doc_id >= 0 && doc_id != temp_doc_id) {
					cout<<"Doc Id Mismatch";getchar();
				}

				temp_doc_id = doc_id.Value();
				excerpt_keyword_file.ReadCompObject(keyword_num);

				for(uChar i=0; i<keyword_num; i++) {
					ptr.ptr = m_keyword_buff.ExtendSize(1);
					excerpt_keyword_file.ReadCompObject(ptr.ptr->id);
					excerpt_keyword_file.ReadCompObject(ptr.ptr->occurr);
					excerpt_keyword_file.ReadCompObject(ptr.ptr->term_weight);
					excerpt_keyword_file.ReadCompObject(ptr.ptr->group_size);

					AddKeyword(ptr, doc_id);
				}
			}

			WriteFinalKeywordSet(doc_id);
		}
	}

	// This sets up the various files
	void Initialize() {

		m_excerpt_file.OpenWriteFile(CUtility::ExtendString
			("GlobalData/Excerpts/final_excerpt_set", GetClientID()));

		m_group_term_file.OpenWriteFile(CUtility::ExtendString
			("GlobalData/HitList/assoc_hit_file", GetClientID()));

		m_pulse_score_file.OpenReadFile("GlobalData/PulseRank/sorted_pulse_score0");
	}

public:

	CFinalExcerptKeywordSet() {
		CHDFSFile::Initialize();
	}

	// This is the entry function
	// @param level_num - this is the total number of levels in the hiearchy
	// @param max_keyword_size - this is the maximum number of keywords 
	//                         - allowed in a given set
	void FinalExcerptKeywordSet(int client_id, int hit_list_breadth, 
		int level_num, int max_keyword_size) {

		CNodeStat::SetClientID(client_id);
		CNodeStat::SetHashDivNum(hit_list_breadth);
		Initialize();

		m_tok_buff.Initialize(max_keyword_size);
		m_tok_queue.Initialize(max_keyword_size, CompareKeywords);
		m_duplicate_map.Initialize(4);
		m_keyword_buff.Initialize();

		m_excerpt_level_set.AllocateMemory(level_num);
		for(int i=0; i<level_num; i++) {
			if(i == 0) {
				m_excerpt_level_set[i].OpenReadFile(CUtility::ExtendString
					("GlobalData/ABTrees/excerpt_term_set", GetClientID()));
			} else {
				m_excerpt_level_set[i].OpenReadFile(CUtility::ExtendString
					("LocalData/excerpt_keywords", i, ".set", GetClientID()));
			}
		}

		ParseExerptKeywordSet();

		S5Byte doc_id;
		for(int i=0; i<m_excerpt_level_set.OverflowSize(); i++) {
			if(m_excerpt_level_set[i].ReadCompObject(doc_id)) {
				cout<<"Still More";getchar();
			}
		}
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);
	int hit_list_breadth = atoi(argv[2]);
	int level_num = atoi(argv[3]);
	int max_keyword_size = atoi(argv[4]);

	CBeacon::InitializeBeacon(client_id);
	CMemoryElement<CFinalExcerptKeywordSet> group;
	group->FinalExcerptKeywordSet(client_id, hit_list_breadth, level_num, max_keyword_size);

	group.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();
}
