#include "../../../../MapReduce.h"

// This class takes the set of associations and their occurrences
// and culls associations with an occurrence below a certain 
// threshold. This is done to reduce the size of the association
// set to a more manageable size.
class CFilterAssociations : public CNodeStat {

	// This stores the set of association ids and tokens
	// that make up the association
	CSegFile m_lexon_assoc_id_file;
	// This stores the global word number 
	// This stores the final set of association ids
	CSegFile m_fin_assoc_file;
	// This stores the permuted association file
	// to allow for associations in reverse order
	CSegFile m_perm_assoc_file;
	// This stores the mapped association occurrence
	CSegFile m_assoc_set_file;
	// This stores the final weight file
	CHDFSFile m_assoc_weight_file;
	// This is used to map text string words to each of the 
	// association word ids
	CSegFile m_acc_assoc_id_file;

	// This stoers the set of association 
	CTrie m_assoc_map;
	// This stores the forward and reverse occurrence
	CArrayList<uLong> m_occur;

	// This stores the total number of words 
	uLong m_word_num;
	// This stores the current association
	_int64 m_curr_assoc_id;
	// This stores the total number of associations present in the set
	_int64 m_total_assoc_num;
	// This stores the number of ters in an association
	int m_assoc_term_size;

	// This reads a virtual weight from file
	static bool ReadKeywordWeight(CHDFSFile &file, int &occur) {
		return file.ReadCompObject(occur);
	}

	// This writes a virtual weight to file
	static void WriteKeywordWeight(CHDFSFile &file, int &occur) {
		file.WriteCompObject(occur);
	}

	// This finds the threshold weight for the set of associations based
	// upon the maximum number of associations allowed
	// @param max_term_num - this is the maximum number of terms allowed
	int FindThresholdWeight(int max_term_num) {

		int threshold = -1;
		if(m_total_assoc_num > max_term_num) {

			CKthOrderStat<int, CHDFSFile> cutoff;
			cutoff.Initialize(99999, CUtility::ExtendString
				("LocalData/thresh", GetClientID()),
				ReadKeywordWeight, WriteKeywordWeight);

			threshold = cutoff.FindKthOrderStat(m_assoc_weight_file,
				 m_total_assoc_num - max_term_num);
		}

		return max(threshold, 3);
	}

	// This writes one of the final association maps
	// @param file - this is the file to write to
	// @param assoc_set - this is the current association
	// @param occur - this is the occurrence of the association
	inline void WriteFinalAssocMap(CSegFile &file, CMemoryChunk<uLong> &assoc_set,	uLong occur) {

		file.AskBufferOverflow((sizeof(uLong) << 1) +
			sizeof(S5Byte) + sizeof(uLong));

		file.WriteCompObject(assoc_set.Buffer(), 2);
		file.WriteCompObject5Byte(m_curr_assoc_id);
		file.WriteCompObject(occur);
	}

	// This creates the final set of associations that have a weight above
	// some threshold. These associations are selected to be merged together
	// The merged terms are stored in a final output set.
	// @param max_term_num - this is the maximum number of terms allowed
	// @param term_size - the number of terms in the association
	void CreateFinAssociations(int max_term_num, int term_num) {

		int threshold = FindThresholdWeight(max_term_num);

		uLong occur1;
		uLong occur2;
		uLong assoc_occur1;
		uLong assoc_occur2;
		bool swap = false;
		m_total_assoc_num = 0;

		m_assoc_set_file.OpenReadFile();
		CMemoryChunk<uLong> assoc_set(term_num);
		while(m_assoc_set_file.ReadCompObject(assoc_set.Buffer(), term_num)) {
			m_assoc_set_file.ReadCompObject(assoc_occur1);

			if(assoc_set[0] > assoc_set[1]) {
				CSort<char>::Swap(assoc_set[0], assoc_set[1]);
				swap = true;
			} else {
				swap = false;
			}

			int id = m_assoc_map.FindWord((char *)assoc_set.Buffer(), sizeof(uLong) << 1);
			occur1 = m_occur[(id << 1) + 1];
			occur2 = m_occur[id << 1];
			assoc_occur2 = occur1 + occur2;

			if(assoc_occur1 < threshold || assoc_occur1 == min(occur1, occur2)) {
				// don't write the reverse association 
				continue;
			}

			if(swap == true) {
				CSort<char>::Swap(assoc_set[0], assoc_set[1]);
			}

			m_acc_assoc_id_file.AskBufferOverflow(sizeof(uLong) * term_num);
			m_acc_assoc_id_file.WriteCompObject(assoc_set.Buffer(), term_num);

			m_lexon_assoc_id_file.AskBufferOverflow(sizeof(S5Byte) + sizeof(uLong));
			m_lexon_assoc_id_file.WriteCompObject5Byte(m_curr_assoc_id);
			m_lexon_assoc_id_file.WriteCompObject(assoc_occur2);

			WriteFinalAssocMap(m_fin_assoc_file, assoc_set, assoc_occur2);
			WriteFinalAssocMap(m_perm_assoc_file, assoc_set, assoc_occur2);

			if(min(occur1, occur2) > 0) {
				CSort<char>::Swap(assoc_set[0], assoc_set[1]);
				WriteFinalAssocMap(m_perm_assoc_file, assoc_set, assoc_occur2);
			}

			if(++m_total_assoc_num >= max_term_num) {
				break;
			}

			m_curr_assoc_id += GetClientNum();
		}
	}

	// This is called to initialize the various variables needed
	// to perform the mapping this includes the dictionary offsets
	void Initialize() {

		m_lexon_assoc_id_file.OpenWriteFile(CUtility::ExtendString
			("GlobalData/Lexon/assoc_id_file", GetClientID()));

		m_assoc_set_file.SetFileName(CUtility::ExtendString
			("LocalData/assoc_occur", m_assoc_term_size, ".client", GetClientID()));

		m_assoc_weight_file.SetFileName(CUtility::ExtendString
			("LocalData/assoc_weight", GetClientID()));

		m_perm_assoc_file.OpenWriteFile(CUtility::ExtendString
			("LocalData/perm_assoc_file", GetClientID()));

		m_fin_assoc_file.OpenWriteFile(CUtility::ExtendString
			("GlobalData/Keywords/fin_group_term_set0.set", GetClientID()));

		m_acc_assoc_id_file.OpenWriteFile(CUtility::ExtendString
			("LocalData/assoc_id_set", m_assoc_term_size, ".client", GetClientID()));

		_int64 dummy1;
		CHDFSFile file;
		file.OpenReadFile("GlobalData/WordDictionary/dictionary_offset");
		// writes the number of base nodes
		file.ReadCompObject(dummy1);
		// writes the total number of nodes
		file.ReadCompObject(dummy1);
		file.ReadCompObject(m_word_num);

		m_curr_assoc_id = m_word_num + GetClientID();
	}

	// This creates the occurrence map for the forward and backward association
	void CreateForwardReverseAssocMap() {
		
		bool swap = false;
		uLong assoc_occur;
		m_total_assoc_num = 0;
		m_assoc_map.Initialize(4);
		m_occur.Initialize(1024);

		CMemoryChunk<uLong> assoc_set(2);
		m_assoc_set_file.OpenReadFile();
		m_assoc_weight_file.OpenWriteFile();
		while(m_assoc_set_file.ReadCompObject(assoc_set.Buffer(), 2)) {
			m_assoc_set_file.ReadCompObject(assoc_occur);

			if(assoc_set[0] > assoc_set[1]) {
				CSort<char>::Swap(assoc_set[0], assoc_set[1]);
				swap = true;
			} else {
				swap = false;
			}

			int id = m_assoc_map.AddWord((char *)assoc_set.Buffer(), sizeof(uLong) << 1);
			if(m_assoc_map.AskFoundWord() == false) {
				m_occur.PushBack(0);
				m_occur.PushBack(0);
			}

			if(swap == true) {
				m_occur[(id << 1) + 1] = assoc_occur;
			} else {
				m_occur[id << 1] = assoc_occur;
			}

			if(m_occur[id << 1] == m_occur[(id << 1) + 1]) {
				m_occur[id << 1]++;
			}

			m_assoc_weight_file.WriteCompObject(assoc_occur);
			m_total_assoc_num++;
		}
	}

public:

	CFilterAssociations() {
		CHDFSFile::Initialize();
	}

	// This is the entry function
	// @param max_assoc_num - this is the maximum number of associations
	//                      - that can be created, the rest are culled
	void FilterAssociations(int client_id, int client_num, 
		int assoc_set_size, int max_assoc_num) {

		CNodeStat::SetClientID(client_id);
		CNodeStat::SetClientNum(client_num);

		m_assoc_term_size = assoc_set_size;

		Initialize();
		CreateForwardReverseAssocMap();

		CreateFinAssociations(max_assoc_num, assoc_set_size);

		CHDFSFile node_num_file;
		node_num_file.OpenWriteFile(CUtility::ExtendString
			("GlobalData/Keywords/assoc_num", GetClientID()));
		node_num_file.WriteObject(m_curr_assoc_id);
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);
	int client_num = atoi(argv[2]);
	int assoc_set_size = atoi(argv[3]);
	int max_assoc_num = atoi(argv[4]);

	CBeacon::InitializeBeacon(client_id);
	CMemoryElement<CFilterAssociations> filter;
	filter->FilterAssociations(client_id, client_num, assoc_set_size, max_assoc_num);
	filter.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();
}