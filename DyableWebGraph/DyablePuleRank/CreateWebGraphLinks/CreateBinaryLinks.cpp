#include "../../../MapReduce.h"

// This stores the directory where the pulse scores are stored
const char *VIRTUAL_LINK_DIR = "GlobalData/VirtualLinks/fin_virtual_list";
// This stores the directory where the keyword links are stored
const char *KEYWORD_LINK_DIR = "GlobalData/Keywords/keyword_links";
// This defines the directory where the final hashed cluster
// link is stored after processing
const char *FIN_LINK_SET_DIR = "GlobalData/LinkSet/fin_link_set";

// This class creates a set of binary links corresponding
// to the links in the clustered link set. Virtual Links
// are also incorperated into the set. These binary links 
// are then merged by their src node.
class CCreateBinaryLinks : public CNodeStat {

	// This stores the keyword links
	CSegFile m_keyword_link_set;
	// This stores the set of links in a cluster
	CArrayList<SWLink> m_link_cluster;
	// This stores the number of affiliated links
	int m_affiliate_link_num;
	// This stores the number of non affiliated links
	int m_non_affiliate_link_num;

	// This writes the binary link set to file once all of the links
	// have been normalized when taking into account affiliation
	void WriteLinkSet(uChar domain_weight) {
		
		for(int i=0; i<m_link_cluster.Size(); i++) {
			SWLink &w_link = m_link_cluster[i];

			if(w_link.link_weight < 0) {
				// an affiliated link
				w_link.link_weight = 0.1f * (1.0f / m_affiliate_link_num);
			} else {
				w_link.link_weight = 0.9f * (1.0f / m_non_affiliate_link_num);
			}

			if(w_link.dst >= CNodeStat::GetBaseNodeNum()) {
				w_link.link_weight *= 0.1f;
				w_link.WriteWLink(m_keyword_link_set);
				return;
			}

			w_link.WriteWLink(m_keyword_link_set);
			CSort<char>::Swap(w_link.src, w_link.dst);
			w_link.WriteWLink(m_keyword_link_set);
		}
	}


	// This loads in the hard link clusters and creates b_links.
	void LoadHardLinks(CHDFSFile &clus_link_file) {

		SWLink w_link;
		uLong cluster_size;
		_int64 dst_index;
		uChar domain_weight;
	
		int count = 0; 
		while(clus_link_file.GetEscapedItem(cluster_size) >= 0) {
			clus_link_file.ReadCompObject(domain_weight);
			clus_link_file.ReadCompObject(w_link.src);

			m_link_cluster.Resize(0);
			m_non_affiliate_link_num = 0;
			m_affiliate_link_num = 0;

			if(++count >= 100000) {
				cout<<GetClientID()<<" "<<(clus_link_file.PercentageFileRead() * 100)<<"% Done"<<endl;
				count = 0;
			}
		
			if(cluster_size == 0) {
				w_link.link_weight = -1.0f;
				// This is used as a placeholder so every 
				// src node is present even if it doesn't have any links
				w_link.WriteWLink(m_keyword_link_set);
			}

			for(uLong i=0; i<cluster_size; i++) {
				clus_link_file.Get5ByteCompItem(dst_index);

				if((dst_index & 0x01) == 0x01) {
					// an affiliated link
					w_link.link_weight = -1.0f;
					m_affiliate_link_num++;
				} else {
					w_link.link_weight = 1.0f;
					m_non_affiliate_link_num++;
				}

				w_link.dst = dst_index >> 1;
				m_link_cluster.PushBack(w_link);
			}

			WriteLinkSet(domain_weight);
		}
	}


public:

	CCreateBinaryLinks() {
		CHDFSFile::Initialize();
	}

	// This is the entry function that takes the hard links and the
	// virtual links and clusters them together anchored at some
	// src node. This must be done before the rest of webgraph can
	// be processed.
	void CreateBinaryLinks(int client_id) {

		CNodeStat::SetClientID(client_id);
		CNodeStat::LoadNodeLinkStat();
		m_link_cluster.Initialize(1024);

		m_keyword_link_set.OpenWriteFile(CUtility::ExtendString
			("LocalData/bin_link_file", CNodeStat::GetClientID()));

		CHDFSFile cluster_link_file;
		cluster_link_file.OpenReadFile(CUtility::ExtendString
			(FIN_LINK_SET_DIR, GetClientID()));

		LoadHardLinks(cluster_link_file);
	}
};

int main(int argc, char *argv[]) {

	if(argc < 2)return 0;
	int client_id = atoi(argv[1]);

	CBeacon::InitializeBeacon(client_id);
	CMemoryElement<CCreateBinaryLinks> set;
	set->CreateBinaryLinks(client_id);
	set.DeleteMemoryElement();
	CBeacon::SendTerminationSignal();
}
