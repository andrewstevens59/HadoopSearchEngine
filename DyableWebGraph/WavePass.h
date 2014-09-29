#include "./Communication.h"

// This defines one of the wave pass 
// distributions stored for a node
struct SWaveDist {
	// This is the node that belongs to the distribution
	S5Byte node;
	// This stores the majority class
	u_short cluster;
	// This stores the class ownership of the majority class
	float majority_weight;

	// This reads a wave distribution from file
	inline bool ReadWaveDist(CHDFSFile &from_file) {
		return ReadWaveDist(from_file, *this);
	}

	// This writes a wave distribution to file
	inline void WriteWaveDist(CHDFSFile &to_file) {
		WriteWaveDist(to_file, *this);
	}

	// This reads a wave distribution from file
	static bool ReadWaveDist(CHDFSFile &from_file, SWaveDist &dist) {

		if(!from_file.ReadCompObject(dist.node)) {
			return false;
		}

		from_file.ReadCompObject(dist.cluster);
		from_file.ReadCompObject(dist.majority_weight);

		return true;
	}

	// This writes a wave distribution to file
	static void WriteWaveDist(CHDFSFile &to_file, SWaveDist &dist) {

		to_file.WriteCompObject(dist.node);
		to_file.WriteCompObject(dist.cluster);
		to_file.WriteCompObject(dist.majority_weight);
	}

	// This transfers a wave distribution from one file to another
	static bool TransferWaveDist(CHDFSFile &from_file, CHDFSFile &to_file) {

		static SWaveDist dist;
		if(!SWaveDist::ReadWaveDist(from_file, dist)) {
			return false;
		}

		SWaveDist::WriteWaveDist(to_file, dist);
		return true;
	}
};

// This is a stochastic annealing algorithm that seeks to find closely
// connected clusters in the graph. It does this by dividing the node into
// a number of different classes. Each node is assigned a random distribution
// over class weights. These class weights are then propagated to each node's
// neighbour, with a specific class weight being amplified by the likeness
// between neighbours. This is carried out for a number of iterations. The 
// output of wave pass is used to generate the final set of join links between
// each node. Each node is assigned it's majority class value. Two nodes are 
// only considered connected if they have the same majority class value.
class CWavePass : public CCommunication {

	// defines the maximum number of bytes allowed in memory 
	// for this section of the program
	static const int MAX_BYTE_NUM = 1000000;

	// This stores all the forward class distribution
	CFileSet<CHDFSFile> m_forward_set;
	// This stores all the back class distribution
	CFileSet<CHDFSFile> m_back_set;

	// This maps global dst indexes to local indexes 
	CObjectHashMap<S5Byte> m_node_map;
	// This stores the wave pass distribution for every 
	// node in the local hash division
	CArrayList<float> m_wave_pass_dist;
	// This stores the global class weight used to keep wave pass balanced
	CMemoryChunk<float> m_class_weight;

	// This stores the hashed cluster link set used throughout
	CFileSet<CHDFSFile> m_hash_link_set;
	// This stores the number of link clusters in each hash division
	CMemoryChunk<_int64> m_link_clus_num;
	// This is a temporary directory buffer
	CMemoryChunk<char> m_directory;

	// This is used for testing stores the initial distribution
	CArrayList<float> m_test_dist;

	// This defines a function that computes the hash map for an object
	static int HashCode(const S5Byte &item) {
		return (int)S5Byte::Value(item);
	}

	// This defines a function to compare two nodes for equality
	static bool Equal(const S5Byte &arg1, const S5Byte &arg2) {
		return S5Byte::Value(arg1) == S5Byte::Value(arg2);
	}

	// This updates the global class weights used to keep the distribution
	// across classes globally balanced.
	void UpdateClassWeight() {

		if(GetClientNum() > 1) {
			char command = 'p';
			Command().Send(&command, 1);
			Command().Send((char *)&m_class_num, sizeof(int));
			Command().Send((char *)m_class_weight.Buffer(), 
				m_class_num * sizeof(float));
			Command().Receive((char *)m_class_weight.Buffer(), 
				m_class_num * sizeof(float));
		}

		CMath::NormalizeVector(m_class_weight.Buffer(), m_class_num);
		for(int j=0; j<m_class_num; j++) {
			m_class_weight[j] = (1 + m_class_weight[j]) * 
				(1 + m_class_weight[j]) * (1 + m_class_weight[j]);
		}

		CMath::NormalizeVector(m_class_weight.Buffer(), m_class_num);
		for(int j=0; j<m_class_num; j++) {
			m_class_weight[j] = 1 - m_class_weight[j];
		}

		CMath::NormalizeVector(m_class_weight.Buffer(), m_class_num);

	}

	// This runs through all the link sets in a given hash division. 
	// It passes the wave pass distribution to all the neighbours.
	// @param clus_link_file - this stores all the cluster link sets
	//                       - for this hash division
	// @param back_file - this stores all the back buffer distribution
	//                  - for this bucket and client
	void ProcessLinkSet(CHDFSFile &clus_link_file, CHDFSFile &back_file) {

		SJoinLink j_link;
		uLong cluster_size;
		CMemoryChunk<float> dist(m_class_num);

		while(clus_link_file.GetEscapedItem(cluster_size) >= 0) {
			clus_link_file.ReadCompObject(j_link.link.src);

			back_file.ReadCompObject(j_link.link.dst);
			for(int j=0; j<m_class_num; j++) {
				back_file.ReadCompObject(dist[j]);
			}

			if(j_link.link.src != j_link.link.dst) {
				cout<<"Node Mismatch "<<j_link.link.src.Value()<<" "<<j_link.link.dst.Value();getchar();
			}

			for(uLong i=0; i<cluster_size; i++) {
				clus_link_file.ReadCompObject(j_link.link.dst);
				clus_link_file.ReadCompObject(j_link.link_weight);

				int hash = (int)(j_link.link.dst.Value() % m_forward_set.SetNum());
				m_forward_set.File(hash).WriteCompObject(j_link.link.dst);

				for(int j=0; j<m_class_num; j++) {
					float weight = j_link.link_weight * dist[j];
					m_forward_set.File(hash).WriteCompObject(weight);
				}
			}
		}
	}

	// This updates the wave pass distribution for all nodes in a given hash 
	// division. This simply means accumulating the wave pass distribution
	// score for every dst node in a division. This is normalized before being
	// added to the existing back buffer.
	// @param hash_div - this is the current hash division being processed
	void AccumulateDistribution(int hash_div) {

		float dist;
		S5Byte dst;
		for(int j=0; j<GetClientNum(); j++) {
			m_forward_set.OpenReadFile(hash_div, j);

			CHDFSFile &node_file = m_forward_set.File(hash_div);
			while(node_file.ReadCompObject(dst)) {
				int id = m_node_map.Put(dst);

				if(!m_node_map.AskFoundWord()) {
					for(int k=0; k<m_class_num; k++) {
						node_file.ReadCompObject(dist);
						m_wave_pass_dist.PushBack(dist);
					}
				} else {
					int offset = id * m_class_num;
					for(int k=0; k<m_class_num; k++) {
						node_file.ReadCompObject(dist);
						m_wave_pass_dist[offset++] += dist;
					}
				}
			}

			node_file.CloseFile();
		}
	}

	// This updates the wave pass distribution, based upon the 
	// neighbours linking to a given node.
	// @param curr_dist - this stores the wave pass distribution
	//                  - on the previous pass
	// @param new_dist - this is used to store the updated wave 
	//                 - pass distribution on this pass
	// @param id - this is the local if of the current node being processed
	// @param sum - this is used for normalization of the wave pass distribution
	// @param curr_class_weight - this stores the net wave pass score of each
	//                          - class oon the current pass
	void AssignUpdateDistribution(CMemoryChunk<float> &curr_dist,
		CMemoryChunk<float> &new_dist, int id, float &sum, 
		CMemoryChunk<float> &curr_class_weight) {

		if(id < 0) {
			// nothing linked to this base node so distribution is unchanged
			sum = 1.0f;
			return;
		}

		int offset = id * m_class_num;
		for(int i=0; i<m_class_num; i++) {
			new_dist[i] = m_wave_pass_dist[offset++] * 
				curr_dist[i] * curr_class_weight[i];
		}

		CMath::NormalizeVector(new_dist.Buffer(), m_class_num);

		sum = 0;
		for(int i=0; i<m_class_num; i++) {
			curr_dist[i] += new_dist[i];
			sum += curr_dist[i];
		}
	}

	// This cycles through all the back buffer nodes in a given hash
	// division and accumulates the distribution. This is done by looking
	// up the local id for each node in a given hash division.
	// @param curr_class_weight - this stores the net wave pass score of each
	//                          - class oon the current pass
	// @param back_file - this stores the current wave pass distribution for 
	//                  - the current hash division
	// @param replace_file - this is used to store the updated wave pass distribution
	// @param curr_dist - this stores the wave pass distribution
	//                  - on the previous pass
	// @param new_dist - this is used to store the updated wave 
	//                 - pass distribution on this pass
	void ProcessBackBufferDiv(CMemoryChunk<float> &curr_class_weight,
		CHDFSFile &back_file, CHDFSFile &replace_file, CMemoryChunk<float> &curr_dist,
		CMemoryChunk<float> &new_dist) {

		float sum = 0;
		S5Byte node;
		while(back_file.ReadCompObject(node)) {
			int id = m_node_map.Get(node);
			replace_file.WriteCompObject(node);

			for(int i=0; i<m_class_num; i++) {
				back_file.ReadCompObject(curr_dist[i]);
			}

			AssignUpdateDistribution(curr_dist, new_dist, 
				id, sum, curr_class_weight);

			for(int i=0; i<m_class_num; i++) {
				curr_dist[i] /= sum;
				m_class_weight[i] += curr_dist[i];
				replace_file.WriteCompObject(curr_dist[i]);
			}
		}
	}

	// This writes the new back buffer by adding the accumulated wave pass 
	// distribution to the existing back buffer distribution.
	// @param curr_class_weight - this stores the current class weight 
	//                          - used to balance the clusters
	// @param hash_div - this stores the current bucket being processed
	void WriteBackBuffer(CMemoryChunk<float> &curr_class_weight, int hash_div) {

		CHDFSFile replace_file;
		replace_file.SetFileName(CUtility::ExtendString
			("LocalData/replace_file", GetInstID()));

		CMemoryChunk<float> curr_dist(m_class_num);
		CMemoryChunk<float> new_dist(m_class_num);

		for(int j=0; j<GetClientNum(); j++) {
			m_back_set.OpenReadFile(hash_div, j);
			CHDFSFile &back_file = m_back_set.File(hash_div);

			replace_file.OpenWriteFile();
			replace_file.InitializeCompression();

			ProcessBackBufferDiv(curr_class_weight, back_file, 
				replace_file, curr_dist, new_dist);

			back_file.SubsumeFile(replace_file);
		}
	}

	// This is called after all link sets have passed their wave pass distribution
	// to their neighbours and it's now time to update the back buffer so the next
	// pass through the link set can begin.
	void UpdateBackBuffer() {

		CCommunication::SynchronizeClients();

		SBoundary bound(0, m_forward_set.SetNum());
		CHashFunction::BoundaryPartion(GetClientID(), 
			GetClientNum(), bound.end, bound.start);

		m_wave_pass_dist.Initialize((int)(GetCurrNodeNum() / 
			m_forward_set.SetNum()) + 1000);

		m_node_map.Initialize(HashCode, 
			Equal, m_wave_pass_dist.OverflowSize());

		CMemoryChunk<float> curr_class_weight(m_class_weight);
		m_class_weight.InitializeMemoryChunk(0);

		for(int i=bound.start; i<bound.end; i++) {
			cout<<"Client: "<<GetClientID()<<" Processing Hash Div "<<i<<endl;
			m_wave_pass_dist.Resize(0);
			m_node_map.Reset();

			AccumulateDistribution(i);
			WriteBackBuffer(curr_class_weight, i);
		}

		UpdateClassWeight();
		m_forward_set.CloseFileSet();

		m_wave_pass_dist.FreeMemory();
		m_node_map.FreeMemory();
	}

	// After all the external nodes have been added now add in all the base
	// nodes that were not linked to be any of the dst nodes. This is need
	// to maintain a complete set of nodes and associated wave pass distributions.
	// All nodes that are added in this final pass have a distribution that is
	// unchanged from the original distribution.
	// @param hash_div - this is the current hash division being processed
	void AddMissingBaseNodes(int hash_div) {

		float sum;
		float dist;
		S5Byte node;
		CMemoryChunk<float> curr_dist(m_class_num);
		CMemoryChunk<float> new_dist(m_class_num);

		for(int j=0; j<GetClientNum(); j++) {
			m_back_set.OpenReadFile(hash_div, j);
			CHDFSFile &back_file = m_back_set.File(hash_div);

			while(back_file.ReadCompObject(node)) {
				int id = m_node_map.Put(node);
				if(!m_node_map.AskFoundWord()) {
					// just adds the existing distribution
					for(int j=0; j<m_class_num; j++) {
						back_file.ReadCompObject(dist);
						m_wave_pass_dist.PushBack(dist);
					}
				} else {
					for(int j=0; j<m_class_num; j++) {
						back_file.ReadCompObject(curr_dist[j]);
					}

					AssignUpdateDistribution(curr_dist, new_dist, 
						id, sum, m_class_weight);

					int offset = id * m_class_num;
					for(int i=0; i<m_class_num; i++) {
						m_wave_pass_dist[offset++] = curr_dist[i] / sum;
					}
				}
			}
		}
	}

	// This is called to create the final distribution for the external
	// nodes for a given hash division.
	// @param fin_wave_file - this stores the wave pass distribution for 
	//                      - all nodes upon completion
	// @param hash_div - this is the current hash division being processed
	void WriteExternalNodes(CHDFSFile &fin_wave_file, int hash_div) {

		int offset = 0;
		CMemoryChunk<float> curr_dist(m_class_num);
		CMemoryChunk<float> fin_dist(m_class_num);

		AddMissingBaseNodes(hash_div);

		m_node_map.ResetNextObject();
		for(int i=0; i<m_node_map.Size(); i++) {
			S5Byte &node = m_node_map.NextSeqObject();
			fin_wave_file.WriteCompObject(node);

			for(int i=0; i<m_class_num; i++) {
				fin_dist[i] = m_wave_pass_dist[offset++];
			}

			CMath::NormalizeVector(fin_dist.Buffer(), m_class_num);

			for(int i=0; i<m_class_num; i++) {
				fin_wave_file.WriteCompObject(fin_dist[i]);
			}
		}
	}

	// This assigns a random distribution to one of the back buffer nodes.
	// This is done during start up before wave pass commences.
	// @param back_file - this stores all the back buffer nodes
	// @param node - this is the current back buffer node being processed
	void AssignBackBuffDist(CHDFSFile &back_file, S5Byte &node) {

		static CMemoryChunk<float> dist(m_class_num);

		back_file.WriteCompObject(node);

		m_test_dist.PushBack(node.Value());

		for(int i=0; i<m_class_num; i++) {
			dist[i] = CMath::RandGauss(0.2f, 1.0f);
		}

		CMath::NormalizeVector(dist.Buffer(), m_class_num);

		for(int i=0; i<m_class_num; i++) {
			m_class_weight[i] += dist[i];
			back_file.WriteCompObject(dist[i]);
			m_test_dist.PushBack(dist[i]);
		}
	}

	// This places the cluster link set into different hash bins based 
	// upon the src node. This is so the back buffer can be matched up
	// to the src node for each link cluster.
	// @param clus_link_file - this stores the clustered version of the link set
	void HashClusterLinkSet(CHDFSFile &clus_link_file) {

		CCommunication::SynchronizeClients();

		SJoinLink j_link;
		uLong cluster_size;
		clus_link_file.OpenReadFile();
		clus_link_file.InitializeCompression();

		while(clus_link_file.GetEscapedItem(cluster_size) >= 0) {
			clus_link_file.ReadCompObject(j_link.link.src);

			int hash = (int)(j_link.link.src.Value() % m_hash_link_set.SetNum());
			CHDFSFile &hash_link_file = m_hash_link_set.File(hash);
			AssignBackBuffDist(m_back_set.File(hash), j_link.link.src);

			hash_link_file.AddEscapedItem(cluster_size);
			hash_link_file.WriteCompObject(j_link.link.src);

			for(uLong i=0; i<cluster_size; i++) {
				clus_link_file.ReadCompObject(j_link.link.dst);
				clus_link_file.ReadCompObject(j_link.link_weight);

				j_link.link_weight++;
				hash_link_file.WriteCompObject(j_link.link.dst);
				hash_link_file.WriteCompObject(j_link.link_weight);
			}
		}
	}

protected:

	// This defines the number of classes to use for wave pass,
	// this will most usually be set to the the number of clients
	int m_class_num;

	// This returns the directory for the wave pass set
	inline const char *Directory(const char dir[]) {
		if(GetInstNum() > 1) {
			strcpy(m_directory.Buffer(), CUtility::ExtendString(dir, GetInstID()));
			return m_directory.Buffer();
		}

		return dir;
	}

public:

	CWavePass() {
		m_directory.AllocateMemory(500);
	}

	// This initializes wave pass by creating the hashed link sets that is 
	// used throughout all iterations of wave pass.
	// @param clus_link_set - this stores the clustered version of the link set
	void InitializeWavePass(CHDFSFile &clus_link_set) {

		m_test_dist.Initialize(4);

		int bucket_num = max((_int64)GetClientNum(), ((sizeof(float) * m_class_num * 
			GetCurrNodeNum()) + sizeof(S5Byte)) / MAX_BYTE_NUM);

		m_class_weight.AllocateMemory(m_class_num, 0);
		m_hash_link_set.OpenWriteFileSet(Directory("LocalData/hash_clus_set"), bucket_num, GetClientID());

		m_link_clus_num.AllocateMemory(bucket_num, 0);

		m_back_set.OpenWriteFileSet(Directory("LocalData/back_wave_pass"), 
			m_hash_link_set.SetNum(), GetClientID());

		HashClusterLinkSet(clus_link_set);

		m_test_dist.PushBack(-1.0f);
	}
	
	// This creates the initial distribution for all the nodes in the back buffer.
	// These are grouped by a particular hash division so that they are aligned
	// with each respective hashed link cluster.
	void AssignInitDistribution() {

		CHDFSFile test_dist_file;
		test_dist_file.OpenWriteFile(CUtility::ExtendString
			("LocalData/test_dist", GetInstID()));
		test_dist_file.InitializeCompression();

		CHDFSFile replace_file(CUtility::ExtendString
			("LocalData/replace_file", GetInstID()));

		S5Byte node;
		for(int i=0; i<m_back_set.SetNum(); i++) {

			replace_file.OpenWriteFile();
			replace_file.InitializeCompression();

			m_back_set.OpenReadFile(i, GetClientID());
			CHDFSFile &back_file = m_back_set.File(i);
			while(back_file.ReadCompObject(node)) {
				back_file.ReadCompObject(CUtility::SecondTempBuffer(),
					m_class_num * sizeof(float));
				AssignBackBuffDist(replace_file, node);
			}

			back_file.SubsumeFile(replace_file);
		}

		m_test_dist.PushBack(-1.0f);
	}

	// This is the entry function that performs Wavepass for a number of 
	// iterations. Here the number of iterations must be supplied. Every
	// node that belongs to this client is assigned a initial random 
	// distribution over classes before Wavepass can begin. 
	// @param cycle_num - this is the number of cycles of wave pass to perform
	void PerformWavePass(int cycle_num) {

		m_back_set.CloseFileSet();
		UpdateClassWeight();

		for(int j=0; j<cycle_num; j++) {
			cout<<"Wave Pass Cycle "<<j<<" Out Of "<<cycle_num<<endl;

			m_forward_set.OpenWriteFileSet(Directory("LocalData/forward_wave_pass"),
				m_hash_link_set.SetNum(), GetClientID());

			for(int i=0; i<m_forward_set.SetNum(); i++) {
				cout<<"Client: "<<GetClientID()<<" Processing Link Set "<<i<<endl;
				m_back_set.OpenReadFile(i, GetClientID());
				m_hash_link_set.OpenReadFile(i, GetClientID());
				ProcessLinkSet(m_hash_link_set.File(i), m_back_set.File(i));
			}

			m_forward_set.CloseFileSet();
			m_back_set.CloseFileSet();

			if(j < cycle_num - 1) {
				UpdateBackBuffer();
			}
		}
	}

	// This is called once at the end to process all the external nodes 
	// that do not have a base node in the link set. These nodes are just
	// asssigned the sum of distributions of linking base nodes.
	// @param fin_wave_file - this stores the wave pass distribution for 
	//                      - all nodes upon completion
	void AssignExternalNodes(CHDFSFile &fin_wave_file) {

		fin_wave_file.OpenWriteFile(CUtility::ExtendString
			("LocalData/fin_wave_file", GetInstID()));
		fin_wave_file.InitializeCompression();

		m_forward_set.CloseFileSet();
		m_back_set.CloseFileSet();

		CCommunication::SynchronizeClients();
		SBoundary bound(0, m_forward_set.SetNum());
		CHashFunction::BoundaryPartion(GetClientID(), 
			GetClientNum(), bound.end, bound.start);

		m_wave_pass_dist.Initialize((int)(GetCurrNodeNum() / 
			m_forward_set.SetNum()) + 1000);
		m_node_map.Initialize(HashCode, 
			Equal, m_wave_pass_dist.OverflowSize());

		for(int i=bound.start; i<bound.end; i++) {
			cout<<"Client: "<<GetClientID()<<" Processing Hash Div "<<i<<endl;
			m_wave_pass_dist.Resize(0);
			m_node_map.Reset();

			AccumulateDistribution(i);
			WriteExternalNodes(fin_wave_file, i);
		}

		CCommunication::SynchronizeClients();

		m_forward_set.CloseFileSet();
		m_back_set.CloseFileSet();

		m_wave_pass_dist.FreeMemory();
		m_node_map.FreeMemory();
	}

	// This is called once at the end to remove unecessary files
	void CleanWavePass() {
		m_forward_set.RemoveFileSet();
		m_back_set.RemoveFileSet();
		m_hash_link_set.RemoveFileSet();

		m_test_dist.WriteArrayListToFile(CUtility::ExtendString
			("LocalData/test_dist", GetInstID()));
	}
};

// In order to ensure that nodes are only assigned the same class based on 
// connectivity, multiple passes are taken with different random initializers.
// Following this a k-fold validation technique is used to count the association
// between two nodes and assigning it to a given link. Nodes with high 
// association will be assigned the same majority class on a number of passes.
// Nodes with low association will be assigned different majority classes in 
// some casses. This is recorded each time a given pass over the set is performed.
// This is done a number of times until the probability of two disimilar classes
// being assigned the same majority class is below some threshold mainly (N)^-k, 
// where k is the number of iterations.
class CWavePassHistory : public CWavePass {

	// This stores the class number bit width
	int m_class_bit_width;

	// This is responsible for updating the class assignment, once the
	// initial class assignment has been created. It simply appends the
	// current class assignment to the previous class assignment.
	// @param wave_pass_file - this stores the wave pass distribution 
	//                       - for this client
	// @param label_file - this stores the class distribution created
	//                   - on the current pass
	// @param replace_file - this stores the updated accumulative distribution
	void UpadateClassDist(CHDFSFile &wave_pass_file, 
		CHDFSFile &label_file, CHDFSFile &replace_file) {

		CHDFSFile test_file;
		test_file.OpenWriteFile(CUtility::ExtendString
			("LocalData/wave_pass_test", GetInstID()));
		test_file.InitializeCompression();

		float dist;
		SWaveDist wave_dist;
		float majority_weight;
		u_short majority_class;

		while(label_file.ReadCompObject(wave_dist.node)) {
			wave_dist.ReadWaveDist(wave_pass_file);
			test_file.WriteCompObject(wave_dist.node);

			bool assign = false;
			majority_weight = 0;
			for(int j=0; j<m_class_num; j++) {
				label_file.ReadCompObject(dist);
				test_file.WriteCompObject(dist);

				if(dist > majority_weight) {
					majority_weight = dist;
					majority_class = (u_short)j;
					assign = true;
				}
			}

			if(assign == false) {
				cout<<"assign error";getchar();
			}

			wave_dist.majority_weight += majority_weight;
			wave_dist.cluster |= majority_class << m_class_bit_width;
			wave_dist.WriteWaveDist(replace_file);
		}	

		wave_pass_file.SubsumeFile(replace_file);
	}

	// This assigns the initialial class distribution. The majority
	// class is calculated along with the neighbour similarity.
	// @param wave_pass_file - this stores the wave pass distribution 
	//                       - for this client
	// @param label_file - this stores the class distribution created
	//                   - on the current pass
	void AssignInitClassDist(CHDFSFile &wave_pass_file, CHDFSFile &label_file) {

		CHDFSFile test_file;
		test_file.OpenWriteFile(CUtility::ExtendString
			("LocalData/wave_pass_test", GetInstID()));
		test_file.InitializeCompression();

		float dist;
		SWaveDist wave_dist;

		while(label_file.ReadCompObject(wave_dist.node)) {
			test_file.WriteCompObject(wave_dist.node);

			wave_dist.majority_weight  = 0;
			for(int j=0; j<m_class_num; j++) {
				label_file.ReadCompObject(dist);
				test_file.WriteCompObject(dist);

				if(dist > wave_dist.majority_weight) {
					wave_dist.majority_weight = dist;
					wave_dist.cluster = (u_short)j;
				}
			}

			wave_dist.WriteWaveDist(wave_pass_file);
		}	
	}
	
	// This assigns the cluster label to each node in the set that 
	// belongs to a particular client.
	// @param label_file - this stores the cluster label and wave 
	//                   - distribution for each node
	// @param wave_pass_file - this stores the wave pass distribution 
	//                       - for this client
	// @param update_label - this is set to true after the initial wave pass
	void AssignLabel(CHDFSFile &label_file, CHDFSFile &wave_pass_file, bool update_label) {

		CHDFSFile replace_file;
		AssignExternalNodes(label_file);

		label_file.OpenReadFile();	
		label_file.InitializeCompression();	

		if(update_label == false) {
			wave_pass_file.OpenWriteFile();
			wave_pass_file.InitializeCompression();

			AssignInitClassDist(wave_pass_file, label_file);
			return;
 		} 

		replace_file.OpenWriteFile(CUtility::ExtendString
			("LocalData/replace", GetInstID()));
		replace_file.InitializeCompression();

		wave_pass_file.OpenReadFile();
		wave_pass_file.InitializeCompression();

		UpadateClassDist(wave_pass_file, label_file, replace_file);
	}

public:

	CWavePassHistory() {
	}

	// This is the entry function that performs multiple instances
	// of wave pass with random initializations.
	// @param wave_pass_cycle - this is the number of wave pass cycles to perform
	// @param inst_num - this is the number of neighbour sets to create
	// @param wave_class_nun - this is the number of wave pass classes to create
	// @param wave_pass_file - this stores the wave pass distribution 
	//                       - for this client
	// @param clus_link_set - this stores the clustered version of the link set
	void FindNeighbours(int wave_pass_cycle, int inst_num, 
		int wave_class_num, CHDFSFile &wave_pass_file, CHDFSFile &clus_link_set) {

		m_class_num = wave_class_num;
		m_class_bit_width = 0;
		float assign_thresh = 1.0f;
		InitializeWavePass(clus_link_set);

		CHDFSFile label_file;
		while(assign_thresh > 0.0001) {

			if(inst_num < 1) {
				break;
			}

			if(assign_thresh < 1.0f) {
				AssignInitDistribution();
			}

			PerformWavePass(wave_pass_cycle);

			AssignLabel(label_file, wave_pass_file, assign_thresh < 1.0f);
			m_class_bit_width += CMath::LogBase2(m_class_num);
			assign_thresh /= m_class_num;
			inst_num--;
		}

		label_file.RemoveFile();
		CleanWavePass();
	}
};