#include "../../../MapReduce.h"

// This stores the association map file
const char *ASSOC_MAP_FILE = "GlobalData/Lexon/assoc_map";

// This creates the final set of associations for a given src term
// In particular it creates a map file with the association text string
// and association weight. It uses a priority queue to remove culled
// associations from the set of possible associations.
class CCreateAssociationMapSet : public CNodeStat {

	// This defines one of the associations
	struct SAssoc {
		// This stores the association id
		S5Byte id;
		// This stores the association weight
		float weight;
		// This stores the association length
		uChar length;
		// This stores the association text offset
		int text_offset;
	};

	// Used to sort associations
	struct SAssocPtr {
		SAssoc *ptr;
	};
	
	// This stores the set of merged associations
	CHDFSFile m_merge_assoc_set_file;
	// This stores the mapped set of singular terms
	CHDFSFile m_word_text_file;
	// This stores the final association map set
	CSegFile m_assoc_map_file;
	// This creates the joined set of terms that is used
	// to create the reverse index
	CSegFile m_join_term_file;

	// This stores the association id
	_int64 m_max_assoc_id;
	// This stores the association text strings
	CArrayList<char> m_assoc_text_buff;
	// This stores the set of associations
	CLinkedBuffer<SAssoc> m_assoc_buff;
	// This is used to rank associations
	CLimitedPQ<SAssocPtr> m_assoc_queue;
	// This stores the maximum word length
	int m_max_word_length;

	// This is used to sort associations
	static int CompareAssociations(const SAssocPtr &arg1, const SAssocPtr &arg2) {

		if(arg1.ptr->weight < arg2.ptr->weight) {
			return -1;
		}

		if(arg1.ptr->weight > arg2.ptr->weight) {
			return 1;
		}

		return 0;
	}

	// This stores the final set of associations in priority order
	// @param src_id - this stores the src association id 
	void StoreAssociations(_int64 &src_id) {

		if(m_assoc_queue.Size() == 0) {
			return;
		}

		int ovf_size = sizeof(S5Byte) + sizeof(uChar);
		ovf_size += m_assoc_queue.Size() * (sizeof(S5Byte) + 
			sizeof(float) + sizeof(uChar) + m_max_word_length);

		m_assoc_map_file.AskBufferOverflow(ovf_size);
		m_assoc_map_file.WriteCompObject5Byte(src_id);
		m_assoc_map_file.WriteCompObject((uChar)m_assoc_queue.Size());

		while(m_assoc_queue.Size() > 0) {
			SAssoc &assoc = *m_assoc_queue.PopItem().ptr;
			m_assoc_map_file.WriteCompObject(assoc.id);
			m_assoc_map_file.WriteCompObject(assoc.weight);
			m_assoc_map_file.WriteCompObject(assoc.length);
			m_assoc_map_file.WriteCompObject(m_assoc_text_buff.Buffer()
				+ assoc.text_offset, assoc.length);
		}

		m_assoc_text_buff.Resize(0);
		m_assoc_buff.Restart();
		m_max_word_length = 0;
	}

	// This creates the joined set of terms that makes up the new text map
	// @param assoc - this stores statistics relating to the association
	// @param group_id - this stores the id of the joined term
	// @param assoc_id_set - this stores the association term set
	// @param assoc_occur - this is the occurrence of the association
	// @param src_assoc_buff - this stores the src association text string
	// @param dst_assoc_length - this stores the src association text string length
	inline void CreateJoinedAssociations(SAssocPtr &assoc, S5Byte &group_id, 
		CMemoryChunk<S5Byte> &assoc_id_set, uLong assoc_occur,
		CMemoryChunk<char> &src_assoc_buff, uChar src_assoc_length) {

		static S5Byte id;
		static uLong term_occur;
		static CMemoryChunk<char> dst_assoc_buff(1024);
		static uChar dst_assoc_length;

		m_word_text_file.ReadCompObject(id);
		m_word_text_file.ReadCompObject(term_occur);
		m_word_text_file.ReadCompObject(dst_assoc_length);
		m_word_text_file.ReadCompObject(dst_assoc_buff.Buffer(), dst_assoc_length);

		src_assoc_buff[src_assoc_length] = '&';
		memcpy(src_assoc_buff.Buffer() + src_assoc_length + 1, dst_assoc_buff.Buffer(), dst_assoc_length);
		src_assoc_length += 1 + dst_assoc_length;

		if(id != assoc_id_set[1]) {
			cout<<"assoc id mismatch2";getchar();
		}

		assoc.ptr->weight = (float)assoc_occur / (term_occur - assoc_occur);
		assoc.ptr->id = id;
		assoc.ptr->length = dst_assoc_length;
		assoc.ptr->text_offset = m_assoc_text_buff.Size();

		m_max_word_length = max(m_max_word_length, dst_assoc_length);
		m_assoc_text_buff.CopyBufferToArrayList(dst_assoc_buff.Buffer(),
			dst_assoc_length, m_assoc_text_buff.Size());

		if(group_id != 0) {
			m_join_term_file.AskBufferOverflow(sizeof(S5Byte) + 
				sizeof(uLong) + sizeof(uChar) * (dst_assoc_length + 1));

			m_join_term_file.WriteCompObject(group_id);
			m_join_term_file.WriteCompObject(assoc_occur);
			m_join_term_file.WriteCompObject(src_assoc_length);
			m_join_term_file.WriteCompObject(src_assoc_buff.Buffer(), src_assoc_length);
		}
	}

	// This creates the set of associations maps
	void ProcessAssociationSet() {

		uLong term_occur;
		uLong assoc_occur;
		S5Byte group_id;
		S5Byte id;

		SAssocPtr ptr;
		uChar src_assoc_length = 0;
		CMemoryChunk<S5Byte> assoc_id_set(2);
		CMemoryChunk<char> src_assoc_buff(1024);
		_int64 prev_src_id = -1;

		long count = 0;
		m_max_assoc_id = 0;
		while(m_merge_assoc_set_file.ReadCompObject(assoc_id_set.Buffer(), 2)) {
			m_merge_assoc_set_file.ReadCompObject(group_id);
			m_merge_assoc_set_file.ReadCompObject(assoc_occur);

			if(++count >= 10000) {
				cout<<(m_merge_assoc_set_file.PercentageFileRead() * 100)<<"% Done"<<endl;
				count = 0;
			}

			if(assoc_id_set[0] != prev_src_id) {

				if(group_id != 0) {
					m_word_text_file.ReadCompObject(id);
					m_word_text_file.ReadCompObject(term_occur);
					m_word_text_file.ReadCompObject(src_assoc_length);
					m_word_text_file.ReadCompObject(src_assoc_buff.Buffer(), src_assoc_length);

					if(id != assoc_id_set[0]) {
						cout<<"assoc id mismatch1";getchar();
					}
				}

				StoreAssociations(prev_src_id);
				prev_src_id = assoc_id_set[0].Value();
			}

			m_max_assoc_id = max(m_max_assoc_id, group_id.Value());
			ptr.ptr = m_assoc_buff.ExtendSize(1);
			CreateJoinedAssociations(ptr, group_id, assoc_id_set, 
				assoc_occur, src_assoc_buff, src_assoc_length);

			m_assoc_queue.AddItem(ptr);
		}

		StoreAssociations(prev_src_id);
	}

public:

	CCreateAssociationMapSet() {
		CHDFSFile::Initialize();
	}

	// This is the entry function
	// @param max_assoc_num - this is the maximum number of associations
	//                      - for a given src term
	void CreateAssociationMapSet(int client_id, int client_num, int max_assoc_num) {

		CNodeStat::SetClientID(client_id);
		CNodeStat::SetClientNum(client_num);

		m_word_text_file.OpenReadFile(CUtility::ExtendString
			("LocalData/mapped_assoc_text_string", GetClientID()));

		m_assoc_map_file.OpenWriteFile(CUtility::ExtendString
			("LocalData/assoc_map_file", GetClientID()));

		// CNodeStat::GetClientNum() to skip past the two term associations
		// of which a map set has already been created
		m_join_term_file.OpenWriteFile(CUtility::ExtendString
			(ASSOC_MAP_FILE, GetClientID() + CNodeStat::GetClientNum()));

		m_assoc_buff.Initialize();
		m_assoc_text_buff.Initialize(2048);
		m_assoc_queue.Initialize(max_assoc_num, CompareAssociations);

		m_max_word_length = 0;
		CMemoryChunk<uLong> assoc_set1(2);
		CMemoryChunk<S5Byte> assoc_set2(2);

		m_merge_assoc_set_file.OpenReadFile(CUtility::ExtendString
			("LocalData/group_term_set", GetClientID()));

		ProcessAssociationSet();

		CHDFSFile assoc_term_id_file;
		assoc_term_id_file.OpenWriteFile(CUtility::ExtendString
			("LocalData/assoc_num", GetClientID()));
		assoc_term_id_file.WriteObject(m_max_assoc_id);
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);
	int client_num = atoi(argv[2]);
	int max_assoc_num = atoi(argv[3]);

	CBeacon::InitializeBeacon(client_id);
	CMemoryElement<CCreateAssociationMapSet> set;
	set->CreateAssociationMapSet(client_id, client_num, max_assoc_num);
	set.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();
}