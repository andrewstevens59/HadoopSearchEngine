#include "../../NameServer.h"

// This class is used to determine the percentage of overlap a 
// given summary has with another summary. This is done by 
// maintaining a range tree for the word ids in a given summary.
// Summaries that have an overlap with other summaries above
// a certain threshold are rejected as they provide no new 
// information.
class CSummaryOverlap {

	// This stores a node in the tree 
	struct STreeNode : public SBoundary {

		STreeNode() {
		}

		STreeNode(int start, int end, int phrase) {
			this->start = start;
			this->end = end;
			this->phrase = phrase;
		}

		// This stores the checksum of the phrase
		int phrase;
	};

	// This stores the boundary tree
	CRedBlackTree<STreeNode> m_summary_bound;

	// This is used to compare different summary bounds
	static int CompareSummaryBounds(const STreeNode &bound1, const STreeNode &bound2) {

		if(bound1.end < bound2.end) {
			return 1;
		}

		if(bound1.start > bound2.start) {
			return -1;
		}

		if(bound1.phrase < bound2.phrase) {
			return 1;
		}

		if(bound1.phrase > bound2.phrase) {
			return -1;
		}

		return 0;
	}

	// This accumulates the summary overlap with other summaries
	// @param bound - this is the root bound of the summary
	bool FindOverlap(STreeNode &bound) {

		void *ptr = m_summary_bound.FindNode(bound);

		if(ptr == NULL) {
			return false;
		}

		SBoundary node = m_summary_bound.NodeValue(ptr);

		if(bound.start < node.start) {
			return false;
		}

		if(bound.start >= node.end) {
			return false;
		}

		return true;
	}

public:

	CSummaryOverlap() {
		m_summary_bound.Initialize(100, CompareSummaryBounds);
	}

	// This adds a summary bound to the set
	void AddBound(int word_start, int word_end) {

		STreeNode bound(word_start, word_end, -1);
		m_summary_bound.AddNode(bound);
	}

	// This adjusts the sentence to the closest overlapping sentence bounds.
	// This is only increases the size of the sentence.
	bool SentenceOverlap(int &word_start, int &word_end,  int phrase) {

		STreeNode bound(word_start, word_end, phrase);

		void *ptr = m_summary_bound.FindNode(bound);

		if(ptr == NULL) {
			m_summary_bound.AddNode(bound);
			return true; 
		}

		STreeNode node = m_summary_bound.NodeValue(ptr);

		if(bound.end <= node.start) {
			m_summary_bound.AddNode(bound);
			return true;
		}

		if(bound.start >= node.end) {
			m_summary_bound.AddNode(bound);
			return true;
		}

		SBoundary overlap(max(word_start, node.start), min(word_end, node.end));
		float perc = overlap.Width() / (word_end - word_start);
		if(perc > 0.3f && node.phrase == phrase) {
			// too much overalp
			return false;
		}
		
		m_summary_bound.AddNode(bound);

		return true;
	}

	// This finds the overlap of a given summary
	// @param word_start - this is the lower bound on the summary
	// @param word_end - this is the upper bound on the summary
	// @return the overlap percentage of the summary
	bool SummaryOverlap(int word_start, int word_end) {

		STreeNode bound(word_start, word_end, -1);

		void *ptr = m_summary_bound.FindNode(bound);

		if(ptr == NULL) {
			return false;
		}

		SBoundary node = m_summary_bound.NodeValue(ptr);

		if(bound.end <= node.start) {
			return false;
		}

		if(bound.start >= node.end) {
			return false;
		}

		return true;
	}
};