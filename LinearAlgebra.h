#include ".\MyStuff.h"

/*
 * This stores a sparse matrix to be used in solving large
 * linear systems. This is typically used in methods
 * like multigrid to solve large sparse lineary systems.
 * The matrix itself is stored using an adjacency list 
 */
template <class X> class CSparseMatrix {
	
	/* This stores a node in the linked list that
	 * is used store the adjacency list */
	struct SLinkNode {
		/* stores a pointer to the adjaceny list in the row */
		SLinkNode *next_ptr;
		/* This stores the column index */
		int column;
		/* this stores an entry in the matrix */
		X value;
	};

	/* This stores the contents of the matrix */
	CLinkedBuffer<SLinkNode> m_mat_buff;
	/* This stores a list of pointers of adjacent
	 * nodes for the entire vector space */
	CMemoryChunk<SLinkNode *> m_adj_list;
	/* This stores a ptr to the current element
	 * in the row that is being processed */
	SLinkNode *m_curr_row;

	/* Returns the number of dimensions in the system */
	inline int Dim() {
		return m_adj_list.OverflowSize();
	}


public:

	CSparseMatrix() {
	}

	/* This creates an instance of a sparse matrix
	 * @param dim - the number of dimension in the system*/
	CSparseMatrix(int dim) {
		m_adj_list.AllocateMemory(dim, NULL);
		m_mat_buff.Initialize(1024);
		m_curr_row = NULL;
	}

	/* This adds a node to the sparse matrix. In order 
	 * to preserve row order it's necessary to do an 
	 * O(N) search in the given row of the matrix 
	 * to find the correct position to place the node
	 * @param i - the row in the matrix
	 * @param j - the column in the matrix
	 * @param element - the element that's being inserted
	 */
	void AddNode(int i, int j, X &element) {

		SLinkNode *curr_ptr = m_adj_list[i];
		SLinkNode *prev_ptr = m_adj_list[i];

		if(curr_ptr == NULL) {
			m_adj_list[i] = m_mat_buff.ExtendBuffer(1);
			curr_ptr = m_adj_list[i];
			curr_ptr->next_ptr = NULL;
			curr_ptr->column = j;
			curr_ptr->value = element;
		}

		while(curr_ptr != NULL) {

			if(curr_ptr->column > j) {
				// found an element with a greater column index
				prev_ptr->next_ptr = m_mat_buff.ExtendBuffer(1);
				prev_ptr = prev_ptr->next_ptr;
				prev_ptr->value = element;
				prev_ptr->column = j;
				prev_ptr->next_ptr = curr_ptr;
				break;
			} 
			
			if(curr_ptr->column == j) {
				// replace the existing element
				curr_ptr->value = element;
				return;
			}

			prev_ptr = curr_ptr;
			curr_ptr = curr_ptr->next_ptr;
		}
	}

	/* This sets the current row in the matrix from which
	 * to extract elements in a linear fashion 
	 * @param row - the row in the matrix that is being processed
	 */
	inline void SetProcessingRow(int row) {
		m_curr_row = m_adj_list[row];
	}

	/* This retrieves the next element in the matrix
	 * for the given processing row in column order 
	 *
	 * @param col - stores the column in the matrix 
	 *            - where the element is stored 
	 * @return the next element in a given row, null
	 *         if there are no more elements left */
	inline X *NextRowNode(int &col) {

		if(m_curr_row == NULL) {
			return NULL;
		}

		X *elem = &m_curr_row->value;
		col = m_curr_row->column;
		m_curr_row = m_curr_row->next_ptr;
		return elem;
	}

	/* This multiplies the matrix by a vector on the right.
	 * The output is stored in the input vector 
	 * @param vector - the supplied vector to multiply matrix by
	 */
	void MultiplyByVector(CMemoryChunk<X> &vector) {

		int col;
		X *node;
		CMemoryChunk<X> temp_vect(Dim());

		for(int i=0; i<Dim(); i++) {

			X sum = 0;
			SetProcessingRow(i);
			while((node = NextRowNode(col)) != NULL) {
				sum += vector[col] * (*node);
			}

			temp_vect[i] = sum;
		}

		vector.MakeMemoryChunkEqualTo(temp_vect);
	}
};

/*
 * This performs Gauss-Siedel to solve a linear system.
 * This method for solving linear systems is often used
 * in combination with another method like multigrid
 * to speed up the performance of the system. This 
 * implementation uses a lower triangular preconditioner
 * of the principle matrix. This reduces the storage 
 * space by a factor of two.
 */
template <class X> class CGaussSiedel {

	/* This stores the current solution for the system */
	CMemoryChunk<X> m_curr_soln;
	/* This stores the b vector -> the RHS of the equation */
	CMemoryChunk<X> *m_b_vect;
	/* This stores the sparse matrix */
	CSparseMatrix<X> *m_mat;

	/* Returns the number of dimensions in the system */
	inline int Dim() {
		return m_curr_soln.OverflowSize();
	}

public:

	CGaussSiedel() {
	}

	/* This creates an instance of Gauss-Siedel of a given dimension 
	 * @param dim - the number of dimension in the system 
	 * @param mat - the sparse matrix that stores the system
	 * @param b_vect - this stores the b vector -> the 
	 *               - RHS of the equation
	 */
	CGaussSiedel(int dim, CSparseMatrix &mat, 
		CMemoryChunk<X> *b_vect) {
		Initialize(dim, mat, b_vect);
	}

	/* This creates an instance of Gauss-Siedel of a given dimension 
	 * @param dim - the number of dimension in the system 
	 * @param mat - the sparse matrix that stores the system
	 * @param b_vect - this stores the b vector -> the 
	 *               - RHS of the equation
	 */
	void Initialize(int dim, CSparseMatrix<X> &mat, 
		CMemoryChunk<X> &b_vect) {

		m_b_vect = &b_vect;
		m_curr_soln.MakeMemoryChunkEqualTo(b_vect);
		m_mat = &mat;
	}

	/* This peforms a single iteration of Gauss-Siedel 
	 * using the LU factorization as a preconditioner */
	void PerformPass() {

		int col;
		X node;
		X diag;

		for(int i=0; i<Dim(); i++) {
			X u_sum = 0;
			X l_sum = 0;
			m_mat->SetProcessingRow(i);
			while((node = *m_mat->NextRowNode(col)) != NULL) {
				if(col < i) { 
					l_sum += node * m_curr_soln[col];
				} else if(col > i) {
					u_sum += node * m_curr_soln[col];
				} else {
					diag = 1 / node;
				}
			}
			m_curr_soln[i] = (m_b_vect->Element(i) - u_sum - l_sum) * diag;
		}
	}

	/* This returns the current solution to the system */
	inline CMemoryChunk<X> &CurrSolution() {
		return m_curr_soln;
	}

	/* This is a test framework. Randomly generates a
	 * matrix and test the error on the solution */
	void TestGaussSiedel() {

		X elem;
		for(int i=0; i<100; i++) {
			CSparseMatrix<X> mat(1000);
			CMemoryChunk<X> b_vect(1000);
			
			for(int j=0; j<b_vect.OverflowSize(); j++) {
				b_vect[j] = rand() % 10000;

				int size = (rand() % b_vect.OverflowSize()) + 5;
				for(int k=0; k<size; k++) {
					elem = rand() % 100000;
					mat.AddNode(j, rand() % b_vect.OverflowSize(), elem);
				}

				elem = rand() % 100000;
				mat.AddNode(j, j, elem);
			}

			Initialize(1000, mat, b_vect);

			for(int j=0; j<40; j++) {
				PerformPass();
			}

			mat.MultiplyByVector(CurrSolution());

			float error = 0;
			// finds the squared error
			for(int i=0; i<Dim(); i++) {
				float diff = m_curr_soln[i] - b_vect[i];
				error += diff * diff;
			}

			if(error > 1.0f) {
				cout<<"Error ";
				getchar();
			}
		}
	}
};

/*
 * This class solves a large sparse linear system by using 
 * multigrid. It's operation has been taylored to cater
 * for quickly approximating expected visits to a given
 * state on an approximation to the web graph this is
 * used in retrieval. This implemenation of multigrid
 * is designed entirely in memory and is limited to data 
 * sets only with a million nodes or so.
 */
template <class X> class CMultiGrid {

	/* This stores the current solution at a given
	 * level in the hiearchy */
	CMemoryChunk<CMemoryChunk<X> > m_curr_soln;

	/* Returns the number of dimensions in the system */
	inline int Dim() {
		return m_curr_soln.OverflowSize();
	}

	/* This performs the interpolation that increases
	 * the granuality of the grid. 
	 * @param level - the level in the hiearchy which 
	 *              - to interpolate to find the finer grid
	 */
	void InterpolateGrid(int level) {

		X prev = 0;
		int width = 1 << level;
		CMemoryChunk<X> &curr_level = m_curr_soln[level];
		CMemoryChunk<X> &next_level = m_curr_soln[level+1];

		for(int i=0; i<Dim(); i++) {
			next_level[i<<1] = (prev + curr_level[i]) * 0.5f;
			next_level[(i<<1) + 1] = curr_level[i];
			prev = curr_level[i];
		}
	}

	/* This performs the restriction of the current level
	 * to reduce the granuality of the grid 
	 * @param level - the level in the hiearchy which 
	 *              - to interpolate to find the finer grid
	 */
	void RestrictGrid(int level) {
	}

public:

	/* This creates an instance of multgrid of a given dimension 
	 * @param dim - the number of dimension in the system*/
	CMultiGrid(int dim) {
		Initialize(dim);
	}

	/* This creates an instance of multgrid of a given dimension 
	 * @param dim - the number of dimension in the system*/
	void Initialize(int dim) {
		int level_num = CMath::LogBase2(dim);
		m_curr_soln[level_num].AllocateMemory(dim);

		int width = 1;
		for(int i=0; i<level_num; i++) {
			m_curr_soln[i].AllocateMemory(width);
			width <<= 1;
		}
	}


};