#include ".\CompileLookupTree.h"

// These are the magic classes that sorts hits within a word division
// based upon spatial location. A lookup tree structure is embedded
// at the start of the word division that defines the spatial boundaries
// of each of the clustered hit items. Also embedded in each node of the 
// tree structure is a node score that has been generated from the 
// sum of all the individual child pulse rank scores in the spatial region.
// Different spatial trees are superimposed on one another in order to find
// content belonging to a particular domain of interest.

// Specifically this class is responsible for summing pulse rank 
// scores in the same spatial region and floating the summed
// score upto adjoining levels in the cluster hiearchy. This 
// class is also responsible for creating the in memory version
// of the spatial tree that can be later culled using branch
// and bound in CompileLookupTree.
class CFloatSpatialScores : public CCompileLookupTree {

	// used for testing
	struct SSpatialTest {
		uLong word_offset;
		int level;
		int cluster;
	};

	// Propagates the base level scores up to a given level in the
	// cluster hiearchy until the two cluster hiearchies become equal.
	// This is done when one of the base level clusters is crossing over 
	// into a new cluster boundary. This is also responsible for creating the
	// boundaries at the lower levels. Note that the base level score is weighted
	// by the number of documents on the level and the hit weight. 
	// @param level - the maximum level for which to float the cluster
	// 				- score to
	// @param doc_hit_num - the number of documents in the current level
	//                    - cluster level grouping
	// @param hiearchy - stores the explicit cluster hiearchy for the 
	//                 - last hiearchy in a matching set -> used later 
	//                 - for writing cluster information along with the lookup tree
	void FloatLevelClusterDetails(int level, int doc_hit_num[], uLong hiearchy[]) {

		for(int i=0;i<level;i++) {
			// weights the final net score by the number of hits
			// in the division - power in numbers
			m_cluster_level[i].LastElement().score *= doc_hit_num[i];
			m_cluster_level[i].LastElement().SetCluster(hiearchy[LevelNum() - 1 - i]);

			// increment the upper level boundary offset
			m_cluster_level[i+1].LastElement().end++;

			// resets the number of hits in the current level to zero
			doc_hit_num[i] = 0;
			doc_hit_num[i+1]++;
			// passes the current level cluster score up into the
			// next level cluster hiearhcy
			m_cluster_level[i+1].LastElement().score += 
				m_cluster_level[i].LastElement().score;

			m_cluster_level[i].ExtendSize(1);
			// resets the current level score
			m_cluster_level[i].LastElement().score = 0;

			// initilizes the next cluster boundary start to the
			// previous level boundary end
			m_cluster_level[i].LastElement().start = 
				m_cluster_level[i].SecondLastElement().end;
			// intilizes the next cluster boundary end to the current
			// level boundary start
			m_cluster_level[i].LastElement().end = 
				m_cluster_level[i].LastElement().start;
		}
	}

public:

	CVector<int> test_clus_hit_start;
	CArrayList<STestHit2> test_clus_hit;

	CFloatSpatialScores() {
		__super::LoadClusterHiearchyLevel();
		m_cluster_level.AllocateMemory(LevelNum()+1);
		m_level_node_num.AllocateMemory(LevelNum(), 0);
		uLong level_size = 0x100000;

		// used for the total score of the word division
		m_cluster_level.LastElement().Initilize(1);
		m_cluster_level.LastElement().ExtendSize(1);

		for(int i=0;i<LevelNum();i++) {
			// allocates memory based upon an exponential scale
			// allocating more memory from the bottom up
			m_cluster_level[i].Initilize(level_size);
			level_size >>= 1;
		}

		test_clus_hit_start.ReadVectorFromFile("../../DyableRetrieve/test_clus_hit_start");
		test_clus_hit.ReadArrayListFromFile("../../DyableRetrieve/test_clus_hit");
	}

	// Sets up the first hit from the first word division
	// @param cluster1 - the first cluster belonging to the first hit
	// @param hiearchy1 - the first explicit cluster belonging to the first hit
	// @param sorted_comp - the merged hit list comp from which to 
	// 					 - extract the first hit
	// @param hit_item - contains all of the merged hits
	// @return the first word division
	uLong SetupClusterWordDivision(char *cluster1, uLong *hiearchy1,
		CCompression &sorted_comp, SMergedHitItem &hit_item) {

		for(int i=0;i<LevelNum()+1;i++) {
			m_cluster_level[i].Resize(1);
			m_cluster_level[i].LastElement().Reset();
		}

		// retrieves the first hit
		m_cluster_level[0].LastElement().end = 
			hit_item.RetrieveClusterHiearchy(sorted_comp, cluster1);
		// sets the hit score
		m_cluster_level[0].LastElement().score = 0;

		// retrieves the first cluster hiearchy
		CSortHitList::ClusterHiearchy(hiearchy1, cluster1, LevelNum() - 1);
		return hit_item.word_id_offset;
	}

	// Processes a particular word division and sums each level in the
	// cluster hiearchy propagating the score up to the root of the tree.
	// The final compiled tree is then stored in memory so branch and bound
	// can be used to cull less promising paths in the tree. Note that all hits
	// have been normalized within a given document so more documents in a given
	// spatial region will have more weight than a singular document with many hits.
	// @param sorted_comp - a comp buffer that stores all the merged hits
	// @param cluster1 - a buffer to store the first comparison cluster hiearchy
	// @param cluster2 - a buffer to store the second comparison cluster hiearchy
	// @param total_hit_size - the total number of bytes composing all the hits
	// 						 - in the current word division
	// @param doc_num - the total number of documents in the given word division
	// @param hiearhy1 - a buffer to store the first explicit cluster hiearchy
	// @param hiearhy2 - a buffer to store the second explicit cluster hiearchy
	// @param hit_item - the first hit in the current word division
	// @return returns the number of bytes composing the last hit item
	int ParseClusterLevelGrouping(CCompression &sorted_comp, char * &cluster1, 
		char * &cluster2, int &total_hit_size, uLong &doc_num, uLong * &hiearchy1,
		uLong * &hiearchy2, SMergedHitItem &hit_item, SHitPosition &next_pos) {

		m_cluster_level[0].LastElement().score = 0;
		float doc_hit_score = hit_item.pulse_score * ((hit_item.encoding & 0x07) + 1);
		// stores the number of hits in the current cluster level division
		CMemoryChunk<int> doc_hit_num(LevelNum() + 1, 0);

		uLong word_div = hit_item.word_id_offset;
		_int64 curr_doc_id = hit_item.doc_id;
		int hit_size = 0, doc_offset = 1;
		doc_num = 0;

		while(sorted_comp.AskNextHitAvailable()) {
			next_pos = sorted_comp.GetCurrentHitPosition();
			// retrieves the next hit item from the current word division
			hit_size = hit_item.RetrieveClusterHiearchy(sorted_comp, cluster2);
			int level = CSortHitList::ClusterLevelEquality
				(cluster1, cluster2, hiearchy2);

			if(hit_item.word_id_offset != word_div) 
				break;

			/*int index = test_clus_hit_start[word_div+1] + c++;
			if(hit_item.pulse_score != test_clus_hit[index].score) {
				cout<<"Pulse Error";getchar();
			}
			if(hit_item.encoding != test_clus_hit[index].enc) {
				cout<<"Encoding Error";getchar();
			}
			if(hit_item.doc_id != test_clus_hit[index].doc_id) {
				cout<<"Doc Error";getchar();
			}
			if(word_div == 59205) {
				for(int j=0;j<LevelNum();j++) {
					cout<<hiearchy2[j]<<" ";
					if(test_clus_hit[index].hiearchy[j] != hiearchy2[j]) {
						cout<<"Cluster Error";getchar();
					}
				}cout<<"     "<<test_clus_hit[index].doc_id<<" "<<(hit_item.pulse_score * ((hit_item.encoding & 0x07) + 1))<<endl;
			}*/

			total_hit_size += hit_size;
			if(curr_doc_id != hit_item.doc_id) {
				curr_doc_id = hit_item.doc_id;
				m_cluster_level[0].LastElement().score += doc_hit_score / doc_offset;
				doc_hit_num[0]++;	// increments the base level doc number
				doc_hit_score = 0;
				doc_offset = 0;
				doc_num++;
			}

			doc_offset++;
			if(level > 0)FloatLevelClusterDetails(level, doc_hit_num.Buffer(), hiearchy1);
			// increment the base level net score weighted by hit type
			doc_hit_score += hit_item.pulse_score * ((hit_item.encoding & 0x07) + 1);
			// increment the base level boundary offset
			m_cluster_level[0].LastElement().end += hit_size;	
			CSort<char>::Swap(cluster1, cluster2);
			CSort<char>::Swap(hiearchy1, hiearchy2);
		}

		doc_num++;
		doc_hit_num[0]++;	// increments the base level doc number
		m_cluster_level[0].LastElement().score += doc_hit_score / doc_offset;
		// finishes off the cluster hiearchy since crossing over into
		// a new word division
		FloatLevelClusterDetails(LevelNum(), doc_hit_num.Buffer(), hiearchy1);
		BranchAndBoundTree(doc_num);
		return hit_size;
	}
};
