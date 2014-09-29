#include "./CompileHitList.h"

// This class creates the final links set that
// is handed of to the clustering and pulse rank
// stage in the pipeline. This is done by simply
// reading the log url index sequentially by looking
// at the log division stored in the intermediate link
// set cluster. Note that two different link sets are
// needed, a clustered link set for pulse rank and a 
// binary link set for the black hole planet algorithm.
// The black hole planet algorithm also records the server
// index for the src node for each binary link.
class CCompileLinkSet {

	// stores the intermediate link set comp
	CHDFSFile m_curr_link_set;
	// stores the final link set comp
	CHDFSFile m_fin_link_set;
	// stores the link cluster for a given document
	CArrayList<_int64> m_link_cluster;
	// This is used to compile the hit list
	CCompileHitList m_compile_hit_list;

	// This retrieves the link cluster for all links joining 
	// the stub with each of its respective excerpts. This 
	// link set has been created artifically with the purpose
	// to connect the stub with excerpts and vice versa.
	void RetrieveLocalURLCluster() {

		static uLong cluster_size;
		static _int64 link_url_index;
		m_curr_link_set.GetEscapedItem(cluster_size);

		for(uLong i=0; i<cluster_size; i++) {
			m_curr_link_set.Get5ByteCompItem(link_url_index);
			link_url_index <<= 1;
			m_link_cluster.PushBack(link_url_index);
		}
	}

	// Reads the global url cluster in from external storage
	// for a given document. This includes links that are part
	// of the global set linking all webpages.
	uChar RetrieveGlobalURLCluster() {

		static u_short div;
		static uLong cluster_size;
		static _int64 link_url_index;
		static uChar domain_weight;

		m_curr_link_set.ReadCompObject(domain_weight);
		m_curr_link_set.GetEscapedItem(cluster_size);

		for(uLong i=0; i<cluster_size; i++) {
			// gets the link index
			m_curr_link_set.ReadCompObject(div);
			u_short fin_div = div & 0x7FFF;

			m_compile_hit_list.RetrieveNextLinkURLIndex((int)fin_div, link_url_index);
			link_url_index = m_compile_hit_list.GlobalURLIndex(link_url_index, (int)fin_div);

			// makes room for the same server encoding
			link_url_index <<= 1;
			// assigns the directory match number
			link_url_index |= div >> 15;
			// adds the link to the cluster
			m_link_cluster.PushBack(link_url_index);
		}

		return domain_weight;
	}

public:

	// loads the link set link div information
	// created in DyableIndex
	// @param client_id - the process id that has been
	//                  - assigned to this process
    // @param client_num - the number of client processes 
	//                   - that have been used thus far
	// @param log_div_num - this is the number of log divisions
	// @param hit_list_breadth - this is the number of hit lists
	//                         - to create -> later sorted
	void LoadLinkSet(int client_id, int client_num, 
		int log_div_num, int hit_list_breadth) {

		CNodeStat::SetClientID(client_id);
		CNodeStat::SetClientNum(client_num);
		CNodeStat::SetHashDivNum(log_div_num);
		
		m_curr_link_set.OpenReadFile(CUtility::ExtendString
			("GlobalData/HitList/meta_link_set", CNodeStat::GetClientID()));

		m_fin_link_set.OpenWriteFile(CUtility::ExtendString
			("GlobalData/LinkSet/fin_link_set", CNodeStat::GetClientID()));

		m_link_cluster.Initialize(10000);
		m_compile_hit_list.LoadHitList(hit_list_breadth);

		CompileFinalLinkSet();
	}

	// creates the final link set from which the 
	// clustered and binary link set can be derived
	// that is used in black hole/planet and pulse rank
	void CompileFinalLinkSet() {

		_int64 base_url_index = 0;
		// retrieves each sequential link set size
		while(m_curr_link_set.Get5ByteCompItem(base_url_index)) {

			m_link_cluster.Resize(0);
			RetrieveLocalURLCluster();

			int local_offset = m_link_cluster.Size();
			uChar domain_weight = RetrieveGlobalURLCluster();

			m_compile_hit_list.CompileFinalHitList
				(base_url_index, m_link_cluster, local_offset);

			// must include a null cluster to correctly align
			// the link set with pulse rank and other processes
			m_fin_link_set.AddEscapedItem(m_link_cluster.Size());
			m_fin_link_set.WriteCompObject(domain_weight);
			m_fin_link_set.WriteCompObject((char *)&base_url_index, 5);

			for(int i=0; i<m_link_cluster.Size(); i++) {
				m_fin_link_set.WriteCompObject((char *)&m_link_cluster[i], 5);
			}
		}
	}
};