#include "..\SortClusterHitList\WordIDSpatialSort.h"

// Once each word division has been sorted, an in memory
// representation of each tree word division needs to be
// created. This is done by doing a branch and bound
// depth first search. When a leaf in the tree is reached
// due to a bound in branch and bound, the node will be labelled
// as a leaf node and the lower and upper bound is expanded down
// to the base level where the cutoff braches down to. 

// This is so a group of spatially equivalent hit items can be grouped
// together to enable future lookup in retrieve once the culled
// spatial tree has been written out to file.
class CCompileLookupTree : public CGlobalClusterSort {

	// used to store the lower and upper bounds in the search tree
	struct SDepthSearch {
		// stores the lower bound on the level below
		// to which the cluster is expanded from the current level
		int upper_bound;
		// stores the upper bound on the level below
		// to which the cluster is expanded from the current level
		int lower_bound;
		// stores the total level score for the current cluster
		float net_level_score;
		// stores the root score associated with the higher
		// level node with a branching factor > 1
		float root_factor;
		// stores the width of the bound
		int width;

		void Set(int lower_bound, int upper_bound) {
			this->lower_bound = lower_bound;
			this->upper_bound = upper_bound;
			width = upper_bound - lower_bound;
		}

		inline int Gap() {
			return upper_bound - lower_bound;
		}

		inline int Width() {
			return width;
		}
	};

	// stores the number of leaf nodes in the tree
	int m_leaf_node_num;
	// defines the number of leaf nodes allowed for
	// the given tree word division
	float m_leaf_node_factor;

protected:

	// this defines the start and end of the spatial boundary
	// of a particular cluster region
	struct SBoundary {
		// stores the start of the node offset of the lower level
		int start;
		// stores the end of the node offset of the lower level
		int end;
		// stores the net score for a particular cluster division
		float score;
		// stores the cluster index
		int cluster;

		void Reset() {
			start = 0;
			end = 0;
			score = 0;
		}

		inline void SetCluster(int cluster) {
			this->cluster = cluster;
		}

		inline int GetCluster() {
			return cluster & 0x3FFFFFFF;
		}

		inline bool AskLeafNode() {
			return (cluster & 0x40000000) == 0x40000000;
		}

		inline void SetLeafNode() {
			cluster |= 0x40000000;
		}

		inline int Width() {
			return end - start;
		}
	};

	// used to store the lookup tree structure
	CMemoryChunk<CArrayList<SBoundary> > m_cluster_level;
	// used to store the number of nodes on each level
	// that have not been culled by branch and bound
	CMemoryChunk<uLong> m_level_node_num;

	// just a test framework
	void CompareTrees(int word_div_size,
		CMemoryChunk<CArrayList<SBoundary> > &copy_level) {	
		for(int i=0;i<LevelNum();i++) {
			for(int j=0;j<copy_level[i+1].Size();j++) {
				float sum = 0;
				for(int c=copy_level[i+1][j].start;c<copy_level[i+1][j].end;c++) {
					sum += copy_level[i][c].score;
				}

				float score1 = (copy_level[i+1][j].score/copy_level[i+1][j].Width());
				float score2 = sum;
				score2 = max(score1, score2) / min(score1, score2);
				// cout<<(copy_level[i+1][j].score/copy_level[i+1][j].Width())<<" "<<
				// sum<<" "<<copy_level[i+1][j].Width()<<endl;
				if(score2 > 1.01) {
					cout<<"Error "<<i<<" "<<copy_level[i+1][j].Width()<<endl;
				}

				for(int c=copy_level[i+1][j].start;c<copy_level[i+1][j].end;c++) {
					copy_level[i][c].score = (copy_level[i][c].score / sum) * 0x7FFF;
				}
			}
		}

		BranchAndBoundTree(word_div_size);
	}

	// Used For Testing
	void PrintTree(CMemoryChunk<CArrayList<SBoundary> > &tree) {
		cout<<endl;
		for(int i=0;i<LevelNum();i++) {
			cout<<"Level "<<i<<endl;
			for(int c=0;c<tree[i].Size();c++) {
				cout<<c<<" : "<<" Score : "<<tree[i][c].score<<
					" Cluster : "<<tree[i][c].GetCluster()<<
					" Start : "<<tree[i][c].start<<
					" End : "<<tree[i][c].end<<endl;
			}
		}
	}

	// Used For Testing
	void PrintTree() {
		PrintTree(m_cluster_level);
	}

	// used to sort each level in the tree once it has been compiled
	inline static int CompareTreeNode(const SBoundary &arg1, 
		const SBoundary &arg2) {

		if(arg1.score > arg2.score)return -1;
		if(arg1.score < arg2.score)return 1;
		return 0;
	}

	// used for debugging
	void PrintPath(CArray<SDepthSearch> &path) {
		for(int i=1;i<path.Size();i++) {
			cout<<i<<" : "<<path[i].lower_bound<<" "<<path[i].upper_bound<<" "<<
				path[i].net_level_score<<" "<<path[i].root_factor<<endl;
		}
	}

	// Finds the upper and lower boundary of all culled children
	// below a given node and sets the score to -1.0f to signify a leaf node.
	// This is done by performing a triangular search down to the base 
	// of the tree at which point the start and end indexes are changed
	// to reflect the lower and upper hit item byte offset.
	// level of the tree so the start and end index
	// @param curr_level - the leaf node level
	// @param curr_offset - the current offset within level segment
	inline void CulledChildrenBoundary(int curr_level, int curr_offset) {
		// indicates a leaf node - used for indexing later
		m_cluster_level[curr_level][curr_offset].SetLeafNode();
		if(curr_level < 1)return;

		int node_start = curr_offset;
		int node_end = curr_offset + 1;
		// need to decend down the lowest and highest path
		// to find the min and max bounds for the culled children
		for(int i=curr_level;i>=0;i--) {
			node_start = m_cluster_level[i][node_start].start;
			node_end = m_cluster_level[i][node_end - 1].end;
		}

		// adjusts the start and indexes of the culled node
		// to reflect the base level hit offset start and end indexes
		m_cluster_level[curr_level][curr_offset].start = node_start;
		m_cluster_level[curr_level][curr_offset].end = node_end;
	}

	// This function acutally performs the sorting of the spatial tree
	// @param path - the search path used in depth first search
	void DepthFirstSearch(CArray<SDepthSearch> &path) {
		
		while(true) {
			SDepthSearch &last_path = path.LastElement();
			CArrayList<SBoundary> &curr_level = 
				m_cluster_level.ElementFromEnd(path.Size() - 1);

			SBoundary &node = curr_level[last_path.lower_bound++];
			float parent_score = node.score;
			
			if(last_path.Width() > 1) {
				last_path.root_factor = node.score / last_path.net_level_score;
				last_path.root_factor *= path.SecondLastElement().root_factor;
			} else {
				last_path.root_factor = path.SecondLastElement().root_factor;
			}

			m_level_node_num[path.Size() - 2]++;
			node.score = 0x7FFF * last_path.root_factor;
			// max search depth reached
			if((last_path.root_factor >= m_leaf_node_factor) && path.Size() < LevelNum() + 1) {
				path.ExtendSize(1);
				// stores a references to the top element in the path
				SDepthSearch &last_path = path.LastElement();
				last_path.Set(node.start, node.end);
				last_path.net_level_score = parent_score / last_path.Width();
				continue;
			}

			// reached a leaf node 
			CulledChildrenBoundary(LevelNum() - path.Size() + 1, last_path.lower_bound - 1);
			m_leaf_node_num++;

			while(path.LastElement().Gap() <= 0) {
				if(path.Size() < 3)return;
				path.PopBack();
			}
		}
	}

public:

	CCompileLookupTree() {
	}

	// Culls regions of the tree that are not promising in a 
	// given word division by using branch and bound. Uses node
	// start/end to index into the lower levels of the tree. Once a
	// given leaf node has been culled, a triangular search is used
	// to move down to the base level of the tree. The start and
	// and indexes of the leaf level are changed to reflect the base
	// level boundary, so all hits can be written appropriately out to file.
	// @param doc_num - the number of documents in the word division
	void BranchAndBoundTree(int word_hit_num) {
		for(int i=0;i<LevelNum();i++)
			m_cluster_level[i].PopBack();

		// need to do a depth first search to sort each 
		// node in the tree
		CArray<SDepthSearch> path(LevelNum() + 1);
		path.ExtendSize(1);
		path.LastElement().root_factor = 1.0f;
		path.ExtendSize(1);

		// defines the number of leaf nodes allowed
		m_leaf_node_factor = 1.0f / 4096;
		m_leaf_node_num = 0;

		// sets the lower and upper bounds of the first level in the tree
		path.LastElement().Set(0, m_cluster_level.SecondLastElement().Size());
		path.LastElement().root_factor = 1.0f;

		// finds the first level total score
		path.LastElement().net_level_score = m_cluster_level[LevelNum()][0].score;
		DepthFirstSearch(path);
	}

	// returns the number of leaf nodes in the tree
	inline int LeafNodeNum() {
		return m_leaf_node_num;
	}

	// just a test framework
	void TestSortTree(int word_div_size) {
		CMemoryChunk<CArrayList<SBoundary> > copy_level(LevelNum()+1);
		for(int i=0;i<copy_level.OverflowSize();i++) {
			copy_level[i].MakeArrayListEqualTo(m_cluster_level[i]);
		}

		for(int i=0;i<LevelNum();i++)
			copy_level[i].PopBack();

		CArray<SDepthSearch> path(100);
		path.ExtendSize(1);
		// sets the lower and upper bounds of the first level in the tree
		path.LastElement().Set(0, copy_level.SecondLastElement().Size());

		while(true) {
			while(path.LastElement().Width() <= 0) {
				if(path.Size() < 2) {
					CompareTrees(word_div_size, copy_level);
					return;
				}
				path.PopBack();
			}

			// stores a references to the top element in the path
			SDepthSearch &last_path = path.LastElement();
			// stores a reference to the current level in the tree
			CArrayList<SBoundary> &last_level = 
				copy_level.ElementFromEnd(path.Size());

			// sort the lower level component
			CSort<SBoundary> sort(last_path.Width());
			// performs the sorting of the cluster subset
			sort.SetComparisionFunction(CompareTreeNode);
			sort.ShellSort(last_level.Buffer() + last_path.lower_bound);
			last_path.lower_bound++;

			// max search depth reached
			if(path.Size() < LevelNum()) {
				SBoundary &last = last_level[last_path.lower_bound - 1];
				path.ExtendSize(1);
				path.LastElement().Set(last.start, last.end);
			}
		}
	}
};