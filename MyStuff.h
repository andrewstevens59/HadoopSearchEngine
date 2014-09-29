 #include "./DataStructure.h"

#ifdef WINDOWS
// This is an even semaphore that can be triggered
// by some event. Used to wait for an object to finish.
class CEvent {
	HANDLE _handle;
public:
	CEvent () {
		// start in non-signaled state (red light)
		// auto reset after every Wait
		_handle = CreateEvent (0, false, false, 0);
	}

	~CEvent () {
		CloseHandle (_handle);
	}

	// put into signaled state
	void Release() { SetEvent (_handle);}
	void Wait() {
		// Wait until event is in signaled (green) state
		WaitForSingleObject (_handle, INFINITE);
	}

	operator HANDLE () { return _handle;}
};
#endif

// This is a commonly used LocalData structure. It's just a set of files 
// that stores content at many different levels.
template <class X> class CFileSet {

	// This defines the maximum byte number allowed to store files
	static const int MAX_BYTE_NUM = 1000000;

	// This stores each of the files
	CMemoryChunk<X> m_level_set;
	// This stores the directory
	char m_directory[500];
	// This stores the client id
	int m_client_id;
	// This stores the size in bytes used to store the 
	// compression buffer, taking into account the number of buckets
	int m_comp_buff_size;

public:

	CFileSet() {
	}

	// This just sets the directory name
	// @param dir - this is the directory for the file set
	CFileSet(const char dir[]) {
		SetDirectory(dir);
	}

	// This opens a number of files for writing 
	// @param dir - this is the directory for the file set
	// @param set_num - this is the number of sets to create
	// @param client - the client id of this client
	CFileSet(const char dir[], int set_num, int client) {
		OpenWriteFileSet(dir, set_num, client);
	}

	// This just sets the directory
	// @param dir - this is the directory for the file set
	inline void SetDirectory(const char dir[]) {
		strcpy(m_directory, dir);
	}

	// This returns the directory name
	// @return the directory name
	inline char *GetDirectory() {
		return m_directory;
	}

	// This just allocates the buffer to stores all the differnt files
	// @param set_num - this is the number of sets to create
	// @param client - this is an optional argument that specifies the 
	//               - client of the file set
	inline void AllocateFileSet(int set_num, int client = -1) {
		m_level_set.AllocateMemory(set_num);
		if(client != -1) {
			m_client_id = client;
		}
	}

	// This opens a number of files for writing 
	// @param dir - this is the directory for the file set
	// @param set_num - this is the number of sets to create
	// @param client - the client id of this client
	void OpenWriteFileSet(const char dir[], int set_num, int client) {

		if(set_num <= 0 || client < 0) {
			throw EIllegalArgumentException("Invalid set num");
		}

		m_level_set.AllocateMemory(set_num);
		strcpy(m_directory, dir);
		m_comp_buff_size = MAX_BYTE_NUM / set_num;

		m_client_id = client;
		for(int i=0; i<set_num; i++) {
			m_level_set[i].OpenWriteFile(CUtility::
				ExtendString(dir, client, ".set", i));
			m_level_set[i].InitializeCompression(m_comp_buff_size);
		}
	}

	// This opens just a single write file 
	// @param dir - this is the directory for the file set
	// @param set_num - this is the number of sets to create
	// @param client - the client id of this client
	void OpenWriteFile(const char dir[], int level, int client) {

		m_client_id = client;
		strcpy(m_directory, dir);
		OpenWriteFile(level, client);
	}

	// This opens just a single write file 
	// @param set_num - this is the number of sets to create
	// @param client - the client id of this client
	void OpenWriteFile(int level, int client = -1) {

		if(level < 0) {
			throw EIllegalArgumentException("Invalid level num");
		}

		if(client != -1) {
			m_client_id = client;
		}

		m_level_set[level].OpenWriteFile(CUtility::
			ExtendString(m_directory, m_client_id, ".set", level));
	}

	// This opens just a single read file 
	// @param set_num - this is the number of sets to create
	// @param client - the client id of this client
	void OpenReadFile(int level, int client = -1) {

		if(level < 0) {
			throw EIllegalArgumentException("Invalid level num");
		}

		if(client != -1) {
			m_client_id = client;
		}

		m_level_set[level].OpenReadFile(CUtility::
			ExtendString(m_directory, m_client_id, ".set", level));
	}

	// This opens a number of files for reading 
	// @param set_num - this is the number of sets to create
	// @param client - the client id of this client
	void OpenReadFileSet(int set_num, int client_id = -1) {

		if(client_id != -1) {
			m_client_id = client_id;
		}
		m_level_set.AllocateMemory(set_num);
		m_comp_buff_size = MAX_BYTE_NUM / set_num;

		for(int i=0; i<set_num; i++) {
			m_level_set[i].OpenReadFile(CUtility::ExtendString
				(m_directory, m_client_id, ".set", i));
			m_level_set[i].InitializeCompression(m_comp_buff_size);
		}
	}

	// This opens a number of files for reading 
	// @param dir - this is the directory for the file set
	// @param set_num - this is the number of sets to create
	// @param client - the client id of this client
	void OpenReadFileSet(const char dir[], int set_num, int client_id = -1) {
		SetDirectory(dir);
		OpenReadFileSet(set_num, client_id);
	}

	// This opens a number of files for reading. In this version
	// only sets that belong to the client are being opened. This
	// is used for parallel sets when originally a client contributes
	// to all the other clients sets. Following this a client must
	// only access sets created for them.
	// @param dir - this is the directory for the file set
	// @param set_num - this is the number of sets to create
	void OpenClientReadFileSet(int set_num) {

		if(set_num <= 0) {
			throw EIllegalArgumentException("Invalid set num");
		}

		m_level_set.AllocateMemory(set_num);
		m_comp_buff_size = MAX_BYTE_NUM / set_num;

		for(int i=0; i<set_num; i++) {
			m_level_set[i].OpenReadFile(CUtility::ExtendString
				(m_directory, i, ".set", m_client_id));
			m_level_set[i].InitializeCompression(m_comp_buff_size);
		}
	}

	// This is a standard copy constructor
	inline void MakeFileSetEqualTo(CFileSet &set) {
		strcpy(m_directory, set.m_directory);
	}

	// This returns the number of sets
	inline int SetNum() {
		return m_level_set.OverflowSize();
	}

	// This sets the working directory
	inline void SetFileName(const char dir[]) {
		strcpy(m_directory, dir);
	}

	// This returns a reference to the file
	inline X &File(int level) {
		return m_level_set[level];
	}

	// This removes the file set 
	void RemoveFileSet() {
		for(int i=0; i<m_level_set.OverflowSize(); i++) {
			m_level_set[i].RemoveFile();
		}
	}

	// This closes all files in the file set
	void CloseFileSet() {
		for(int i=0; i<m_level_set.OverflowSize(); i++) {
			m_level_set[i].CloseFile();
		}
	}
};


// This is a very useful class that just acts as a
// buffer with a size attribute attatched so elements
// can be pushed and popped on and off the array.
template <class X> class CArray : public CBaseArray<X> {
public:

	CArray() {
	}

	// This initializes the array to some specified size
	// @param size - the maximum number of elements allowed in the array
	CArray(int size) {
		this->Initialize(size); 
	}

	// Copies an array passed in as a parameter
	// @param - this is the array that's being copied
	inline void MakeArrayEqualTo(CArray<X> &array) {
		this->Initialize(array.OverflowSize()); 
		this->Resize(array.Size()); 
		
		memcpy(this->m_memory.Buffer(), 
			array.Buffer(), array.Size() * sizeof(X)); 
	}

	// Retrieves a 4 - byte integer from an escaped 
	// @param buffer - contains the escaped integer encoding
	// @return the escaped integer
	static uLong GetEscapedItem(char * &buffer) {
		uLong item = 0; 
		int offset = 0; 

		while(true) {
			item |= (uLong)(((uChar)(*buffer & 0x7F)) << offset); 
			// checks if finished
			if(!((uChar)*buffer & 0x80)) {
				buffer++; 
				return item; 
			}
			offset += 7; 
			buffer++; 
		}
		return item; 
	}

	// Retrieves a 4 - byte integer from an escaped 
	// @param buffer - contains the escaped integer encoding
	// @param enc_bytes - the size of the encoding in bytes
	// @return the escaped integer
	static uLong GetEscapedItem(char * &buffer, int &enc_bytes) {
		uLong item = 0; 
		int offset = 0; 
		enc_bytes = 1; 

		while(true) {
			item |= (uLong)(((uChar)(*buffer & 0x7F)) << offset); 
			// checks if finished
			if(!((uChar)*buffer & 0x80)) {
				buffer++; 
				return item; 
			}
			offset += 7; 
			buffer++; 
			enc_bytes++; 
		}
		return item; 
	}

	// Gets an escaped 56-bit integer and places
	// the results in the buffer
	// @param in_buffer - the buffer from which to extract the integer
	// @param out_buffer - the buffer to store the extracted integer
	// @returns the number of bytes used to store it
	static int GetEscapedItem(char in_buffer[], char out_buffer[]) {
		int offset = 0; 
		uLong item = 0; 
		int byte_count = 1; 

		while(offset < 28) {
			item |= (uLong)(*in_buffer & 0x7F) << offset; 
			// checks if finished
			if(((uChar)(*in_buffer++) & 0x80) == 0) {
				*(uLong *)out_buffer = item;
				return byte_count; 
			}
			byte_count++; 
			offset += 7; 
		}

		*(uLong *)out_buffer = item;
		offset = 0;
		item = 0;

		while(true) {
			item |= (uLong)(*in_buffer & 0x7F) << offset; 
			if(((uChar)(*in_buffer++) & 0x80) == 0)break;
			byte_count++; 
			offset += 7; 
		}

		*(uChar *)(out_buffer + 3) |= item << 4;
		*(uLong *)(out_buffer + 4) |= item >> 4;
		return byte_count; 
	}

	// Gets an escaped n-byte integer and places
	// the results in the buffer
	// @param in_buffer - the buffer from which to extract the integer
	// @param item - the buffer to store the extracted integer
	// @returns the number of bytes used to store it
	static inline int GetEscapedItem(char in_buffer[], _int64 &item) {
		item = 0; 
		return GetEscapedItem(in_buffer, (char *)&item); 
	}

	// Adds a X byte escaped integer
	// @param item - this is the integer of arbitrary size
	// @return the number of bytes added
	template <class Y> int AddEscapedItem(Y item) {
		if(item < 0)throw EIllegalArgumentException("item < 0"); 

		int bytes = 1; 
		while(item >= 128) {
			this->PushBack((uChar)(item|0x80)); 
			item >>= 7; 
			bytes++; 
		}

		this->PushBack((uChar)item); 
		return bytes; 
	}

	// Returns the number of bytes that make up an
	// excaped 4 byte character as contained in the buffer
	// @param buffer - this contains the escaped item encoding
	// @return the number of bytes that makes up the encoding
	static int EscapedSize(char buffer[]) {
		int byte_count = 1; 

		while((uChar)(*buffer) & 0x80) {
			byte_count++; 
			buffer++; 
		}
		return byte_count; 
	}

	// This returns the number of bytes that would make up
	// an escaped version of the supplied integer, before
	// escape encoding has been applied.
	// @param buffer - this contains the item without escaped encoding
	// @return the number of bytes that makes up the encoding
	template <class Y>
	static int EscapedSize(Y item) {

		int bytes = 1; 
		while(item >= 128) {
			item >>= 7; 
			bytes++; 
		}

		return bytes; 
	}

	// Appends the current array to an array passed in as a parameter
	// @param array - this is the array on which the contents
	//              - of this array is appended
	void AppendArrayTo(CArray<X> &array) {
		memcpy(array.Buffer() + array.Size(), 
			this->m_memory.Buffer(), this->m_size * sizeof(X)); 
			
		array.Resize(array.Size() + this->m_size); 
	}

	// Copies elements directly from a buffer 
	// @param buffer - the buffer containing the elements
	// @param num - the number of elements to copy
	void AddToBuffer(const X buffer[], int num) {
		int prev_size = this->Size();
		this->ExtendSize(num);
		memcpy(this->Buffer() + prev_size, buffer, num);
	}

	// removes the file containing the array
	inline void RemoveArray(char file[]) {
		CHDFSFile::Remove(CUtility::ExtendString(file, ".array")); 
	}

	// Writes the array to file
	void WriteArrayToFile(const char str[]) {
		if(this->OverflowSize() <= 0)return; 

		CHDFSFile file; 
		file.OpenWriteFile(CUtility::ExtendString(str, ".array")); 
		WriteArrayToFile(file); 
	}

	// Writes the array to file
	void WriteArrayToFile(CHDFSFile &file) {
		int overflow = this->m_memory.OverflowSize(); 
		file.WriteCompObject(overflow); 
		file.WriteCompObject(this->m_size); 
		file.WriteCompObject(this->m_memory.Buffer(), this->Size()); 
	}

	// Reads the array from file
	void ReadArrayFromFile(const char str[]) {
		CHDFSFile file; 
		file.OpenReadFile(CUtility::ExtendString(str, ".array")); 
		ReadArrayFromFile(file); 
	}

	// Reads the array from file
	void ReadArrayFromFile(CHDFSFile &file) {
		int overflow; 
		file.ReadCompObject(overflow); 
		file.ReadCompObject(this->m_size); 
		this->m_memory.AllocateMemory(overflow); 
		file.ReadCompObject(this->m_memory.Buffer(), this->Size()); 
	}
}; 

// This is a very useful variable size buffer. It's advantages 
// are that it's very fast since it's implemented as a triple 
// indirect linked list, however it has significant memory 
// overhead and should be avoided for storing int list of 
// elements and should only be used when random access is required.
template <class X> class CVector {

	// creates a buffer to store a block of elements
	CMemory<X> m_memory_element; 
	// creates a buffer to store pointers to blocks of elements
	CMemory<X *> m_memory_column; 
	// creates a buffer to store double indirect 
	// pointers to blocks of elements
	CMemoryChunk<X ** > m_memory_depth; 

	// stores the current column number
	int m_column_num;
	// stores the element number in the row
	int m_element_num;
	// stores the current depth numbers
	int m_depth_num; 

	// stores a Buffer to the current row for speed reasons
	X *m_curr_row; 
	// stores the size of the vector
	int m_size; 

	// stores the number elements stored in a given row
	static const int DROWSIZE = 8192;
	// stores the number elements stored in a given column
	static const int DCOLUMNSIZE = 8192;
	// stores the number elements stored in a given depth
	static const int DDEPTHSIZE = 8192;

	// stores the number of bits for the row 
	static const int DROWSHIFT = 13;
	// stores the number of bits for the column 
	static const int DCOLUMNSHIFT = 13;
	// stores the number of bits for the depth 
	static const int DDEPTHSHIFT = 13;

	// Returns the coordinates for an index
	// @param depth - stores the depth dimension for the given index
	// @param column - stores the column dimension for the given index
	// @param element - stores the element offset for the given index
	// @param index - the global index into the vector
	inline void GetIndex(int &depth, int &column, int &element, int index) {
		depth = index / (DCOLUMNSIZE << DROWSHIFT);
		column = (index - ((depth << DCOLUMNSHIFT) << DROWSHIFT)) >> DROWSHIFT;
		element = index - (column << DROWSHIFT) - ((depth << DCOLUMNSHIFT) << DROWSHIFT); 
	}

	// Return the number of blocks used for a particular vector
	// @param size - the size of the vector
	inline int GetBlockSize(int size) {
		int base = size >> DROWSHIFT; 
		if(size - (base << DROWSHIFT) > 0) {
			base++; 
		}

		return base; 
	}

	// Frees the memory used to store the vector
	void DeleteVector() {
		for(int a=0; a<m_depth_num-1; a++) {
			for(int b=0; b<DCOLUMNSIZE; b++) {
				m_memory_element.FreeMemory(*(m_memory_depth[a] + b)); 
			}
			m_memory_column.FreeMemory(m_memory_depth[a]); 
		}

		for(int b=0; b<m_column_num; b++) {
			m_memory_element.FreeMemory(*(m_memory_depth[m_depth_num - 1] + b)); 	
		}

		m_memory_column.FreeMemory(m_memory_depth[m_depth_num - 1]); 
	}

public:

	CVector() {
		m_size = 0; 
		m_element_num = 0; 
		m_column_num = 0; 
		m_depth_num = 0; 

		// allocates an initial block of memory
		m_memory_depth.AllocateMemory(DDEPTHSIZE); 
		// allocates the first column
		m_memory_depth[m_depth_num++] = m_memory_column.AllocateMemory(DCOLUMNSIZE); 

		// allocates the first depth
		*(m_memory_depth[m_depth_num - 1] + m_column_num++) = 
		     m_memory_element.AllocateMemory(DROWSIZE); 

		// assigns the current row for speed reasons after reading the vector
		m_curr_row = *(m_memory_depth[m_depth_num - 1] + m_column_num - 1); 
	}

	// This is just the copy constructor that takes 
	// an existing vector and makes a copy of it
	// @param copy - this is the vector that's being copied
	CVector(CVector<X> &copy) {
		MakeVectorEqualTo(copy);
	}

	// Returns the size of the vector
	inline int Size() {
		return m_size; 
	}

	// This resizes the vector by some offset
	// @param offset - the amount to increase or decrease the vector by
	inline void ExtendSize(int offset) {
		if(Size() + offset < 0)throw EIllegalArgumentException
			("Illegal Offset");

		Resize(Size() + offset);
	}

	// Returns a reference to the indexed element
	inline X &operator[](int index) {
		if(index >= Size() || index < 0)throw EIndexOutOfBoundsException
			("Vector Out Of Bounds Index"); 

		int depth_num, column_num, element_num; 

		GetIndex(depth_num, column_num, element_num, index); 

		return *(*(m_memory_depth[depth_num] + column_num) + element_num); 
	}

	// Returns the last element of a vector
	inline X &LastElement() {
		if(Size() < 1)throw EUnderflowException
			("Vector Last Element Out of Bounds"); 
		return Element(m_size - 1); 
	}

	// Returns a reference to the indexed element
	inline X &Element(int index) {
		return this->operator [](index); 
	}

	// Copies a vector
	void MakeVectorEqualTo(CVector<X> &vector) {

		Resize(0); 
		int length = GetBlockSize(vector.m_size); 
		if(length <= 0)return; 

		m_size = vector.m_size; 
		int column_num = 0, depth_num = 0; 

		// copies straight into the buffer since first row segment already allocated
		memcpy(*(m_memory_depth[m_depth_num - 1] + m_column_num - 1), 
			 * (vector.m_memory_depth[depth_num] + column_num++), sizeof(X) * DROWSIZE); 

		for(int i=1; i<length; i++) {
			if(column_num >= DCOLUMNSIZE) {
				column_num = 0; 
				depth_num++; 
			} 
			
			if(m_column_num >= DCOLUMNSIZE) {
				m_column_num = 0; 
				m_memory_depth[m_depth_num++] = 
					m_memory_column.AllocateMemory(DCOLUMNSIZE); 
			} 
			// allocates more memory
			 * (m_memory_depth[m_depth_num - 1] + m_column_num++) = 
				 m_memory_element.AllocateMemory(DROWSIZE); 

			// copies the buffer
			memcpy(*(m_memory_depth[m_depth_num - 1] + m_column_num - 1), 
				 * (vector.m_memory_depth[depth_num] + column_num++), sizeof(X) * DROWSIZE); 
		}

		m_element_num = vector.m_element_num; 
		m_column_num = vector.m_column_num; 
		m_depth_num = vector.m_depth_num; 
	}

	// Adds a new element to the vector. It checks for overflow bounds
	// along the three dimensions and allocates memory as needed.
	// @param element - this is the element that's being added
	void PushBack(X element) {
		m_size++; 

		if(m_element_num >= DROWSIZE) {
			m_element_num = 0; 
			if(m_column_num >= DCOLUMNSIZE) {
				m_column_num = 0; 
				m_memory_depth[m_depth_num++] = m_memory_column.
					AllocateMemory(DCOLUMNSIZE); 
			} 
			*(m_memory_depth[m_depth_num - 1] + m_column_num++) = 
				 m_memory_element.AllocateMemory(DROWSIZE); 

			m_curr_row = *(m_memory_depth[m_depth_num - 1] + m_column_num - 1); 
		}
		*(m_curr_row + m_element_num++) = element; 
	}

	// Copies a buffer to the vector of size items 
	// and offset some amount into the vector
	// @param buffer - contains the elements to be copied
	// @param size - the number of elements in the buffer to copy accross
	// @param offset - an index into the vector to which the elements will
	//        - be copied accross to
	void CopyBufferToVector(X buffer[], int size, int offset) {
		if(offset < 0 || size < 0 || (offset + size) > Size())
			throw EIllegalArgumentException("invalid size params");

		if(offset > m_size) {
			Resize(offset); 
			CopyBufferToVector(buffer, size); 
			return; 
		}

		if(offset < Size()) {
			int element_num, column_num, depth_num; 
			GetIndex(depth_num, column_num, element_num, offset); 
			int gap = m_size - offset; 
			if((offset + size) <= m_size) {
				gap = size;		
			}	

			if(gap <= (DROWSIZE - element_num)) {
				memcpy(*(m_memory_depth[depth_num] + column_num) + 
					element_num, buffer, gap * sizeof(X)); 

				buffer += gap; 
				// copies the part of the buffer past the current size of the vector
				if((offset + size) > m_size)CopyBufferToVector(buffer, size - gap); 
				return; 
			}

			// copies the top
			memcpy(*(m_memory_depth[depth_num] + column_num++) + element_num, 
				buffer, (DROWSIZE - element_num) * sizeof(X)); 
			buffer += DROWSIZE - element_num; 
			gap -= DROWSIZE - element_num; 

			while(gap > DROWSIZE) {
				// copies the middle
				if(column_num >= DCOLUMNSIZE) {
					column_num = 0; 
					depth_num++; 
				} 
				memcpy(*(m_memory_depth[depth_num] + column_num++), 
					buffer, DROWSIZE * sizeof(X)); 
				buffer += DROWSIZE; 
				gap -= DROWSIZE; 
			}

			if(column_num >= DCOLUMNSIZE) {
				column_num = 0; 
				depth_num++; 
			} 
			// copies the bottom
			memcpy(*(m_memory_depth[depth_num] + column_num), buffer, gap * sizeof(X)); 
			buffer += gap; 
			// copies the part of the buffer past the current size of the vector
			if((offset + size) > m_size) {
				CopyBufferToVector(buffer, size + offset - m_size); 
			}
			return; 
		}

		// copies the part of the buffer past the current size of the vector
		CopyBufferToVector(buffer, size); 
	}

	// Copies a buffer to the end of the vector
	// @param buffer - contains the elements to be copied
	// @param size - the number of elements to copy into the vector
	void CopyBufferToVector(X buffer[], int size) {
		if(size < 0)throw EIllegalArgumentException
			("invalid size params");

		m_size += size; 
		if(size <= (DROWSIZE - m_element_num)) {
			memcpy(*(m_memory_depth[m_depth_num - 1] + m_column_num - 1) + 
				m_element_num, buffer, size * sizeof(X)); 
			m_element_num += size; 
			return; 
		}

		// copies the top
		memcpy(*(m_memory_depth[m_depth_num - 1] + m_column_num - 1) + m_element_num, 
			buffer, (DROWSIZE - m_element_num) * sizeof(X)); 

		buffer += DROWSIZE - m_element_num; 
		size -= DROWSIZE - m_element_num; 

		while(size > DROWSIZE) {
			// copies the middle
			if(m_column_num >= DCOLUMNSIZE) {
				m_column_num = 0; 
				m_memory_depth[m_depth_num++] = 
					m_memory_column.AllocateMemory(DCOLUMNSIZE); 
			} 
			*(m_memory_depth[m_depth_num - 1] + m_column_num++) = 
				m_memory_element.AllocateMemory(DROWSIZE); 

			memcpy(*(m_memory_depth[m_depth_num - 1] + m_column_num - 1), 
				buffer, DROWSIZE * sizeof(X)); 
			buffer += DROWSIZE; 
			size -= DROWSIZE; 
		}

		if(m_column_num >= DCOLUMNSIZE) {
			m_column_num = 0; 
			m_memory_depth[m_depth_num++] = m_memory_column.
				AllocateMemory(DCOLUMNSIZE); 
		} 
		// copies the bottom
		*(m_memory_depth[m_depth_num - 1] + m_column_num++) = 
			m_memory_element.AllocateMemory(DROWSIZE); 

		memcpy(*(m_memory_depth[m_depth_num - 1] + m_column_num - 1), 
			buffer, size * sizeof(X)); 
		m_element_num = size; 
	}

	// Copies size items of the vector into the buffer 
	// at some offset within the vector
	// @param buffer - place to store the elements
	// @param size - the number of elements to copy from the vector
	// @param offset - the starting index in the vector to start 
	//        - copying elements from
	void CopyVectorToBuffer(X buffer[], int size, int offset = 0) {
		if(offset < 0 || size < 0 || (offset + size) > Size())
			throw EIllegalArgumentException("invalid size params");

		int element_num, column_num, depth_num; 
		GetIndex(depth_num, column_num, element_num, offset); 

		if(offset % DROWSIZE) {
			if(size <= (DROWSIZE - element_num)) {
				memcpy(buffer, *(m_memory_depth[depth_num] + 
					column_num++) + element_num, sizeof(X) * size); 
				return; 
			}
			memcpy(buffer, *(m_memory_depth[depth_num] + column_num++)
				 + element_num, sizeof(X) * (DROWSIZE - element_num)); 
			buffer += DROWSIZE - element_num; 
			size -= DROWSIZE - element_num; 
		}

		while(size > DROWSIZE) {
			if(column_num >= DCOLUMNSIZE) {
				column_num = 0; 
				depth_num++; 
			} 
			memcpy(buffer, *(m_memory_depth[depth_num] + 
				column_num++), sizeof(X) * DROWSIZE); 
			buffer += DROWSIZE; 
			size -= DROWSIZE; 
		}

		if(column_num >= DCOLUMNSIZE) {
			column_num = 0; 
			depth_num++; 
		} 
		memcpy(buffer, *(m_memory_depth[depth_num] + 
			column_num++), sizeof(X) * size); 
	}

	// Returns the last element of the vector
	X PopBack() {
		if(!m_size)throw EUnderflowException
			("Vector Pop Back Underflow"); 

		X element = operator[](m_size - 1); 
		m_size--;

		if(m_element_num == 0) {
			m_element_num = DROWSIZE + 1; 
			if(m_depth_num > 1) {
				// removes the last depth and column
				if(m_column_num == 0) {
					m_column_num = DCOLUMNSIZE + 1; 
					m_memory_column.FreeMemory(m_memory_depth[m_depth_num - 1]); 
					m_depth_num--; 
				} else {
					m_memory_element.FreeMemory(*(m_memory_depth
						[m_depth_num - 1] + m_column_num - 1)); 
				}

				m_column_num--; 
			} else if(m_column_num > 1) {
				// removes the last column
				m_memory_element.FreeMemory(*(m_memory_depth
					[m_depth_num - 1] + m_column_num - 1)); 
				m_column_num--; 
			}
		}
		m_element_num--; 

		return element; 
	}

	// Resizes a vector - keeps the existing content in the vector and 
	// removes only what exceeds the current vector size.
	// @param index the size of the new vector
	void Resize(int index) {

		if(index < Size()) {	

			int depth_num;
			int column_num;
			int element_num;
			GetIndex(depth_num, column_num, element_num, index); 
			depth_num++;
			column_num++;

			if(m_depth_num > depth_num || m_column_num > column_num) {
				if(m_depth_num > 1) {
					// removes the last depth and column
					if(m_column_num == 0) {
						m_column_num = DCOLUMNSIZE + 1; 
						m_memory_column.FreeMemory(m_memory_depth[m_depth_num - 1]); 
						m_depth_num--; 
					} else {
						m_memory_element.FreeMemory(*(m_memory_depth
							[m_depth_num - 1] + m_column_num - 1)); 
					}

					m_column_num--; 
				} else if(m_column_num > 1) {
					// removes the last column
					m_memory_element.FreeMemory(*(m_memory_depth
						[m_depth_num - 1] + m_column_num - 1)); 
					m_column_num--; 
				}
			}

			GetIndex(m_depth_num, m_column_num, m_element_num, index); 
			m_depth_num++;
			m_column_num++;

		} else if(index > Size()) {
			int curr_blocks = GetBlockSize(Size());
			int next_blocks = GetBlockSize(index + 1);

			for(int i=curr_blocks; i<next_blocks; i++) {
				if(m_column_num >= DCOLUMNSIZE) {
					m_column_num = 0; 
					m_memory_depth[m_depth_num++] = 
						m_memory_column.AllocateMemory(DCOLUMNSIZE); 
				} 
				*(m_memory_depth[m_depth_num - 1] + m_column_num++) = 
					m_memory_element.AllocateMemory(DROWSIZE); 
			}
		}
		
		// assigns the current row for speed reasons after reading the vector
		m_curr_row = *(m_memory_depth[m_depth_num - 1] + m_column_num - 1); 
		m_size = index; 
	}

	// This is just a test framework
	void TestVector() {

		CArray<int> value(999999);

		for(int i=0; i<value.OverflowSize(); i++) {
			value.PushBack(rand());
			ExtendSize(1);
			LastElement() = value.LastElement();
		}

		Resize(0);

		for(int i=0; i<value.OverflowSize(); i++) {
			ExtendSize(1);
			LastElement() = value[i];
		}

		int offset = value.OverflowSize();
		while(offset > 0) {cout<<offset<<endl;
			for(int i=0; i<offset; i++) {
				if(value[i] != Element(i)) {
					cout<<"Element Mismatch";getchar();
				}

				i += rand() % 100;
			} 

			offset -= rand() % 50;
			Resize(offset);
		}
	}

	// Writes a compressed vector to file
	void ReadVectorFromFile(const char str[]) {

		CHDFSFile file; 
		file.OpenReadFile(CUtility::ExtendString(str, ".vector")); 

		ReadVectorFromFile(file); 
	}

	// Writes a compressed vector to file
	void WriteVectorToFile(const char str[]) {
		if(Size() <= 0)return; 

		CHDFSFile file; 
		file.OpenWriteFile(CUtility::ExtendString(str, ".vector")); 

		WriteVectorToFile(file); 
	}

	// Reads a vector in from file and stores 
	// it as a memory chunk - must be written as a vector
	static void ReadVectorFromFile(CHDFSFile &file, CMemoryChunk<X> &memory_chunk) {

		int size;
		file.ReadCompObject(size); 
		memory_chunk.AllocateMemory(size); 
		file.ReadCompObject(memory_chunk.Buffer(), size); 
	}

	// Writes a memory chunk as a vector
	// it as a memory chunk - must be written as a vector
	static void WriteVectorToFile(CHDFSFile &file, CMemoryChunk<X> &memory_chunk) {

		int size = memory_chunk.OverflowSize();
		file.WriteCompObject(size); 
		file.WriteCompObject(memory_chunk.Buffer(), size); 
	}

	// Reads a compressed vector from file
	void ReadVectorFromFile(CHDFSFile &file) {

		Resize(0); 
		file.ReadCompObject(m_size); 
		int length = GetBlockSize(Size()); 

		m_element_num = 0; 
		m_column_num = 1; 
		m_depth_num = 1; 

		if(Size() > 0) {
			// reads the first block in separately
			// first block of memory is automatically allocated at initilization
			file.ReadCompObject(*m_memory_depth[m_depth_num - 1], DROWSIZE); 
		}

		for(int i=1; i<length; i++) {
			if(m_column_num >= DCOLUMNSIZE) {
				m_column_num = 0; 
				m_memory_depth[m_depth_num++] = 
					m_memory_column.AllocateMemory(DCOLUMNSIZE); 
			} 
			*(m_memory_depth[m_depth_num - 1] + m_column_num++) = 
				m_memory_element.AllocateMemory(DROWSIZE); 

			file.ReadCompObject(*(m_memory_depth
				[m_depth_num - 1] + m_column_num - 1), DROWSIZE); 
		}

		// assigns the current row for speed reasons after reading the vector
		m_curr_row = *(m_memory_depth[m_depth_num - 1] + m_column_num - 1); 
	}

	// Writes a compressed vector to file
	void WriteVectorToFile(CHDFSFile &file) {

		int length = GetBlockSize(m_size); 
		file.WriteCompObject(m_size); 

		int column_num = 0, depth_num = 0; 
		for(int i=0; i<length; i++) {
			if(column_num >= DCOLUMNSIZE) {
				column_num = 0; 
				depth_num++; 
			} 
			file.WriteCompObject(*(m_memory_depth[depth_num] 
				+ column_num++), DROWSIZE); 
		}
	}

	~CVector() {
		DeleteVector(); 
	}
}; 

// This is a simple array structure that automatically reallocates
// it's size if it overflows it's current size. A expansion factor
// of two is used and allows an initial size to be specified.
template <class X> class CArrayList : public CMemoryChunk<X> {
	// stores the current size of the array list
	int m_size; 
	// stores the original max size of the array list
	int m_base_origin; 

public:

	CArrayList() {
	}

	// This creates the initial buffer used to store all
	// the elements in the buffer.
	// @param size - this is the initial size of the buffer
	CArrayList(int size) {
		Initialize(size); 
	}

	// Makes a copy of another ArrayList
	// @param copy - this is the ArrayList that's being copied
	CArrayList(CArrayList<X> &copy) {
		MakeArrayListEqualTo(copy); 
	}

	// This creates the initial buffer used to store all
	// the elements in the buffer.
	// @param size - this is the initial size of the buffer
	void Initialize(int size = 4) {
		m_size = 0; 
		m_base_origin = size; 
		this->AllocateMemory(size); 
	}

	// Makes a copy of another ArrayList
	// @param copy - this is the ArrayList that's being copied
	void MakeArrayListEqualTo(CArrayList<X> &copy) {
		CMemoryChunk<X>::MakeMemoryChunkEqualTo(copy); 
		m_size = copy.Size(); 
		m_base_origin = copy.m_base_origin; 
	}

	// Adds an elment to the array list
	// @param element - this is the element that is 
	//                - being added to the ArrayList
	void PushBack(X element) {
		if(Size() >= this->OverflowSize()) {
			CMemoryChunk<X>::Resize(this->OverflowSize() << 1); 
		}


		CMemoryChunk<X>::Element(m_size++) = element; 
	}

	// Removes the last element from the array list
	X PopBack() {
		if(Size() <= 0)throw EUnderflowException
			("Array List UnderFlow"); 

		if(Size() == (this->OverflowSize() >> 1)) {
			Resize(this->OverflowSize() >> 1); 
		}

		return this->Element(--m_size); 
	}

	// Returns a reference to the last element of the 
	// array list
	X &LastElement() {
		if(Size() <= 0)throw EUnderflowException
			("Array List Underflow"); 
			
		return operator[](Size() - 1); 
	}

	// Returns a reference to the second element of the 
	// array list
	X &SecondLastElement() {
		if(Size() <= 1)throw EUnderflowException
			("Array List Underflow"); 
		return this->Element(Size() - 2); 
	}

	// Returns the element a given index from the end
	X &ElementFromEnd(int index) {
		if(Size() - index < 0) {
			throw EIllegalArgumentException("UnderFlow"); 
		}

		return this->Element(Size() - 1 - index); 
	}

	// Returns the size of the array list
	inline int Size() {
		return m_size; 
	}

	// Retrieves an element
	inline X &operator[](int index) {
		if(index >= Size() || index < 0) {
			throw EIndexOutOfBoundsException
			("memory chunk overflow "); 
		}

		return *(this->Buffer() + index); 
	}

	// just a test framework
	void TestArrayList() {
		CMemoryChunk<int> compare(99999); 
		Initialize(); 
		for(int i=0; i<compare.OverflowSize(); i++) {
			int element = rand(); 
			compare[i] = element; 
			Resize(Size() + 1); 
			LastElement() = element; 
		}

		for(int i=0; i<compare.OverflowSize(); i++) {
			if(PopBack() != compare[compare.OverflowSize() - 1 - i]) {
				cout << "error"; getchar(); 
			}
			PushBack(0); 
			PopBack(); 
		}
	}

	// Extends the size of the array list
	// @param offset - the number of elements to extend the 
	//               - array by
	// @return a reference to the first elemnt in the extended buffer
	inline void ExtendSize(int offset) {
		if(Size() + offset < 0) {
			throw EIllegalArgumentException
				("Invalid Offset"); 
		}

		Resize(Size() + offset); 
	}

	// Copies a buffer of some length into the array list at some offset.
	// @param str - this contains the buffer of items that are being copied
	//            - to the ArrayList
	// @param length - this is the number of bytes to copy accross
	// @param offset - this is an optional offset in the buffer
	void CopyBufferToArrayList(const X str[], int length, int offset = 0) {

		Resize(length + Size()); 
		memcpy(this->Buffer() + offset, str, sizeof(X) * length); 
	}

	// Copies a buffer of some length into the array list at some offset.
	// @param str - this contains the buffer of items that are being copied
	//            - to the ArrayList
	// @param length - this is the number of bytes to copy accross
	// @param offset - this is an optional offset in the buffer
	void CopyBufferToArrayList(const X str[]) {

		int length = strlen(str);
		CopyBufferToArrayList(str, length, Size());
	}

	// Copies a buffer of some length into the array list at some offset.
	// @param item - the single item being copied
	// @param length - this is the number of bytes to copy accross
	// @param offset - this is an optional offset in the buffer
	void CopyBufferToArrayList(const X &item) {

		CopyBufferToArrayList(&item, 1, Size());
	}

	// This copies the ArrayList to an arbitrary buffer
	// @param str - this contains the buffer of items that are being copied
	//            - to the ArrayList
	// @param length - this is the number of bytes to copy accross
	// @param offset - this is an optional offset in the buffer
	void CopyArrayListToBuffer(X str[], int size, int offset = 0) {

		memcpy(str, this->Buffer() + offset, sizeof(X) * size); 
	}

	// Extends the size of the buffer
	void ExtendBuffer(int offset) {
		Resize(Size() + offset); 
	}

	// Resizes the ArrayList
	// @param size - this is the new size of the ArrayList
	void Resize(int size) {
		if(size > m_base_origin) {
			if(size > this->OverflowSize())
				CMemoryChunk<X>::Resize(size << 1); 
			else if(size < (this->OverflowSize() >> 1))
				CMemoryChunk<X>::Resize(size); 
		} else {
			CMemoryChunk<X>::Resize(m_base_origin); 
		}

		m_size = size; 
	}

	// Removes the file associated with the array list
	int RemoveArrayList(const char str[]) {
		return CHDFSFile::Remove(CUtility::
			ExtendString(str, ".array_list")); 
	}

	// Reads an array list to file
	void ReadArrayListFromFile(const char str[]) {
		CHDFSFile file; 
		file.OpenReadFile(CUtility::
			ExtendString(str, ".array_list")); 

		ReadArrayListFromFile(file); 
	}

	// Writes an array list to file
	void WriteArrayListToFile(const char str[]) {

		CHDFSFile file; 
		file.OpenWriteFile(CUtility::
			ExtendString(str, ".array_list")); 

		WriteArrayListToFile(file); 
	}

	// Reads an array list from file
	void ReadArrayListFromFile(CHDFSFile &file) {
		file.ReadCompObject(m_size); 
		file.ReadCompObject(m_base_origin); 
		this->AllocateMemory(Size()); 
		file.ReadCompObject(this->Buffer(), Size()); 
	}

	// Writes an array list to file
	void WriteArrayListToFile(CHDFSFile &file) {
		file.WriteCompObject(m_size); 
		file.WriteCompObject(m_base_origin); 
		file.WriteCompObject(this->Buffer(), Size()); 
	}
}; 

// This is a general purpose string class used to 
// do basic string manipulation
class CString {

	// This is the buffer used to store the srring 
	CArrayList<char> m_string_buff;

public:

	CString() {
		m_string_buff.Initialize(4);
		m_string_buff.ExtendSize(1);
	}

	// This is an initialization string
	// @param str - this contains the string to use
	CString(const char str[]) {
		m_string_buff.Initialize(max((int)strlen(str) << 1, (int)4));
		memcpy(m_string_buff.Buffer(), str, strlen(str));
		m_string_buff.ExtendSize((int)strlen(str) + 1);
	}

	// This returns the length of the string
	inline int Size() {
		return m_string_buff.Size() - 1;
	}

	// This resets the contents of the string
	inline void Reset() {
		m_string_buff.Resize(0);
		m_string_buff.ExtendSize(1);
	}

	// This converts the string to lower case
	void ConvertToLowerCase() {
		for(int i=0; i<m_string_buff.Size(); i++) {
			if(m_string_buff[i] < 'A' || m_string_buff[i] > 'Z') {
				continue;
			}

			CUtility::ConvertToLowerCaseCharacter(m_string_buff[i]);
		}
	}

	// This returns the buffer that contains the string
	inline char *Buffer() {
		m_string_buff.LastElement() = '\0';
		return m_string_buff.Buffer();
	}

	// Assignment operator
	inline CString &operator=(const char str[]) {
		m_string_buff.Initialize(max((int)strlen(str) << 1, (int)4));
		memcpy(m_string_buff.Buffer(), str, strlen(str));
		m_string_buff.ExtendSize((int)strlen(str) + 1);

		return *this; 
	}

	// Assignment operator
	inline CString &operator=(CString &string) {
		m_string_buff.MakeArrayListEqualTo(string.m_string_buff);

		return *this; 
	}

	// This removes the last character
	inline void PopBack() {
		m_string_buff.PopBack();
	}

	// This adds a limited text string
	void AddTextSegment(const char str[], int length) {

		if(length == 0) {
			return;
		}

		int offset = Size();
		m_string_buff.ExtendSize(length);
		memcpy(m_string_buff.Buffer() + offset, str, length);
	}

	// This is just an addition operator
	inline CString &operator+=(const char str[]) {
		AddTextSegment(str, strlen(str));
		return *this;
	}

	// This is just an addition operator
	inline CString &operator+=(char str[]) {
		AddTextSegment(str, strlen(str));
		return *this;
	}

	// This is just an addition operator
	inline CString &operator+=(char ch) {
		AddTextSegment(&ch, 1);
		return *this;
	}

	// This is an addition operator for a number 
	template <class X>
	inline CString &operator+=(X number) {
		return operator+=(CANConvert::NumericToAlpha(number));
	}
};

// This class is concerned with tokenising a character
// string into individual words. Several functions are 
// supplied to handle this job.
class CTokeniser : public CUtility {
	// stores if a new word has started in a buffer
	static bool m_new_word; 
	// stores the index for the start of the new word
	static int m_word_start; 

	// stores a list of word tokens
	CArrayList<char> m_token; 
	// stores an list of offsets for which tokens start
	CArrayList<int> m_token_start; 
	// stores the current token offset
	int m_token_offset; 

public:

	// resets the parsing of a text string
	static void ResetTextPassage(int start = 0) {
		m_new_word = true; 
		m_word_start = start; 
	}

	// empties all the tokens in the buffer
	void ResetTokens() {
		m_token_start.Resize(0); 
		m_token.Resize(0); 
	}

	// removes the next token from the buffer
	// @param length - stores the length of the token
	char *GetNextToken(int &length) {
		if(m_token_offset >= m_token_start.Size()) {
			return NULL; 
		}

		length = m_token_start[m_token_offset+1] - 
			m_token_start[m_token_offset]; 
		m_token_offset++; 
		return m_token.Buffer() + m_token_start[m_token_offset - 1]; 
	}

	// tokenises a string
	// @param buffer - the text string to tokenise
	void TokeniseString(char buffer[], int length, int start = 0) {
		ResetTextPassage(); 
		int new_length, new_start; 
		ResetTextPassage(start); 
		m_token_offset = 0; 

		for(int i=start; i<length; i++) {
			if(GetNextWordFromPassage(new_length, new_start, i, m_temp_buff1, length)) {
				m_token.CopyBufferToArrayList(m_temp_buff1 + new_start, new_length - new_start); 
				m_token_start.PushBack(m_token.Size()); 
			}
		}
	}

	// This can be used with multiple threads
	// @param word_end - stores the end of the tokenised word in the
	//                 - text string
	// @param word_start - stores the start of the tokenised word in the
	//                   - text string
	// @param index - the current index in the text buffer (text)
	// @param text - the text string that needs to be tokenised
	// @param text_length - the length text string that needs to be tokenised
	// @return true if a tokenised word is available, false otherwise
	static inline bool GetNextWordFromPassage(bool &new_word, int &first_word, 
		int &word_end, int &word_start, int index, const char text[],
		int text_length, int start = 0) {

		if(!AskOkCharacter(text[index]) && !new_word) {
			if(index - first_word) {
				word_end = index; 
				word_start = first_word; 
			}
			new_word = true; 
			first_word = index + 1; 
			return true;
		}

		if(index >= (text_length - 1)) {
			if(AskOkCharacter(text[text_length - 1]) && !new_word) {
				if(index - first_word) {
					word_end = text_length; 
					word_start = first_word; 
					return true; 
				}
			}
		} 

		if(AskOkCharacter(text[index])) {
			new_word = false; 
		} else {
			first_word = index + 1; 
		}
			
		return false; 
	}

	// Returns true when a new word has been encountered
	// @param word_end - stores the end of the tokenised word in the
	//                 - text string
	// @param word_start - stores the start of the tokenised word in the
	//                   - text string
	// @param index - the current index in the text buffer (text)
	// @param text - the text string that needs to be tokenised
	// @param text_length - the length text string that needs to be tokenised
	// @return true if a tokenised word is available, false otherwise
	static inline bool GetNextWordFromPassage(int &word_end, int &word_start, 
		int index, const char text[], int text_length, int start = 0) {

		return GetNextWordFromPassage(m_new_word, m_word_start, word_end,
			word_start, index, text, text_length, start);
	}

	// Looks to see if a string of terms are present in a given text
	// string. Note that the fragment must be terminated by a space.	
	// @param text - contains the character string that is being searched
	// @param fragment - the character string encoding that is being looked for
	// @param text_length - the length of the buffer string in bytes
	// @param start - the start of the buffer string if a match is found
	// - this is changed to point to the end of the match
	// @param term_num - the number of individual character strings that
	// - need to be matched
	// @return the position of the last character in the last term match
	//  if all terms were matched, -1 otherwise
	static int TermMatch(char text[], char fragment[], 
		int text_length, int start, int term_num) {

		ResetTextPassage(start); 
		int fragment_length = (int)strlen(fragment); 
		int word_end, word_start; 
		int fragment_start = 0; 
		int matched_terms = 0; 

		for(int i=start; i<text_length; i++) {

			if(CTokeniser::GetNextWordFromPassage(word_end, 
				word_start, i, text, text_length)) {

				bool match = true; 
				int fragment_end = fragment_start; 
				int word_length = word_end - word_start; 

				while(fragment[fragment_end] != ' ' && 
					(fragment_end < fragment_length)) {

					if(fragment[fragment_end++] != text[word_start++])
						match = false; 
				}

				if((fragment_end - fragment_start) == word_length) {
					if(match) {
						matched_terms++; 
						fragment_start = fragment_end + 1; 
					}
					if(matched_terms >= term_num)
						return word_end; 
				}
			}
		}

		return (matched_terms == term_num) ? word_end : -1; 
	}
}; 
bool CTokeniser::m_new_word; 
int CTokeniser::m_word_start; 

// This is a standard linked list LocalData structure
// that allocates elements individually. The linked
// buffer should be used instead for larger list sizes.
template <class X> class CLinkedList {
	// stores the size of the linked list
	int m_size; 

	struct SListItem {
		X item; 
		SListItem * next_item; 

		SListItem() {
			next_item = NULL; 
			item = NULL; 
		}
	} *m_curr_item, *m_root_item; 

	// frees the memory used to store the linked list
	void DestroyLinkedList() {
		m_curr_item = m_root_item; 
		// first	store a Buffer to every list item so they can then be deleted
		CArray< SListItem * > list_item_stack(m_size + 1); 
		while(m_curr_item != NULL) {
			list_item_stack.PushBack(m_curr_item); 
			m_curr_item = m_curr_item->next_item; 
		}

		m_size = 0; 
		for(int i=0; i<list_item_stack.Size(); i++) 
			delete list_item_stack[i]; 	// next deletes the list item
	}

	// creates a new list item
	void Initialize() {
		m_size = 0; 
		// allocates space for the next item
		try {
			m_root_item = new SListItem; 
		} catch(bad_alloc xa) {
			throw EAllocationFailure
				("Memory Chunk Allocation Failure"); 
		}

		m_root_item->next_item = NULL; 
		m_curr_item = m_root_item; 
	}

public:

	CLinkedList() {
		Initialize(); 
	}

	// adds an item to the list
	bool PushBack(X &value) {
		// have to be at the end of a list to append a new value
		if(m_curr_item->next_item != NULL)return false; 
		m_curr_item->item = value; 	// assigns the value to the current item
		m_size++; 

		// allocates space for the next item
		try {
			m_curr_item->next_item = new SListItem; 
		} catch(bad_alloc xa) {
			throw EAllocationFailure
				("Memory Chunk Allocation Failure"); 
		}

		m_curr_item = m_curr_item->next_item; 
		return true; 
	}

	// resets the linked list
	void Reset() {
		m_curr_item = m_root_item; 
	}

	// returns a reference to the current element
	X &GetCurrentElement() {
		return m_curr_item->item; 
	}

	// gets the next element in the linked list
	bool NextNode() {
		if(m_curr_item->next_item == NULL)return false; 
		m_curr_item = m_curr_item->next_item; 
		return true; 
	}

	// returns the size of the linked list
	inline int Size() {
		return m_size; 
	}

	// writes the linked list to file
	void WriteLinkedListToFile(const char str[]) {
		if(Size() <= 0)return; 

		CHDFSFile file; 
		file.OpenWriteFile(CUtility::
			ExtendString(str, ".linked_list")); 

		file.WriteCompObject(m_size); 
		m_curr_item = m_root_item; 
		CMemoryChunk<X> value(m_size); 
		for(int i=0; i<m_size; i++) {
			value[i] = GetCurrentElement(); 
			NextNode(); 
		}

		file.WriteCompObject(value.Reference(), 
			value.OverflowSize()); 
	}

	// reads the linked list from file
	void ReadLinkedListFromFile(const char str[]) {
		CHDFSFile file; 
		DestroyLinkedList(); 	// first destroys the current linked list 
		Initialize(); 		// initilzes the linked list

		file.OpenReadFile(CUtility::
			ExtendString(str, ".linked_list")); 

		file.ReadCompObject(m_size); 
		CMemoryChunk<X> value(m_size); 
		file.ReadCompObject(value.Reference(), m_size); 

		m_curr_item = m_root_item; 
		int size = m_size; 
		for(int i=0; i<size; i++)
			AppendToList(value[i]); 
	}

	~CLinkedList() {
		DestroyLinkedList(); 
	}
}; 

// This is just a simple heap based priority queue.
// There is a fixed limit to the size of the queue
// and any items that exceed the maximum size of
// the queue will just be rejected. This queue does
// not drop any elements if the queue overflows.
template <class X> class CPriorityQueue {

	// Stores the maximum allowed queue size
	int m_max_queue_size; 
	// Stores the size of the queue
	int m_size; 

	// Stores a buffer to the compare function
	int (*m_compare)(const X &item1, const X &item2); 

	// This moves a node up the heap
	void TrickleUp(int index) {
		int parent = (index - 1) >> 1; 
		X bottom = m_heap[index]; 
		while((index > 0) && (m_compare(m_heap[parent], bottom) < 0)) {
			// move it down
			m_heap[index] = m_heap[parent]; 
			index = parent; 
			parent = (parent - 1) >> 1; 
		}
		m_heap[index] = bottom; 
	}

	// This moves a node down the heap
	void TrickleDown(int index) {

		int larger_child; 
		X top = m_heap[index]; 	// save the root
		// while root has at least one child
		while(index < (m_size >> 1)) {
			int left_child = (index << 1) + 1; 
			int right_child = left_child + 1; 

			// find the larger child
			if((right_child < m_size) && 
				(m_compare(m_heap[left_child], m_heap[right_child]) < 0))
				larger_child = right_child; 
			else
				larger_child = left_child; 

			// top >= larger child
			if(m_compare(top, m_heap[larger_child]) >= 0)
				break; 

			// shift child up
			m_heap[index] = m_heap[larger_child]; 
			index = larger_child; 
		}
		// root to index
		m_heap[index] = top; 
	}

protected:

	// This is just a buffer used to store all
	// the elements in the heap using a binary
	// tree -> array LocalData structure
	CMemoryChunk<X> m_heap; 

public: 

	CPriorityQueue() {
	}

	// Starts the priority queue
	// @param max_queue_size - the maximum allowed number
	//                       - of elements can be added to the queue
	CPriorityQueue(int max_queue_size, 
		int (*compare)(const X &item1, const X &item2)) {

		Initialize(max_queue_size, compare); 
	}

	// Starts the priority queue
	// @param max_queue_size - the maximum allowed number
	//                       - of elements can be added to the queue
	void Initialize(int max_queue_size, 
		int (*compare)(const X &item1, const X &item2)) {

		if(max_queue_size <= 0)throw EIllegalArgumentException
			("max queue size <= 0");

		m_heap.AllocateMemory(max_queue_size); 
		m_max_queue_size = max_queue_size; 
		m_compare = compare; 
		m_size = 0; 
	}

	// Sets a comparison function
	inline void SetCompareFunction
		(int (*compare)(const X &item1, const X &item2)) {

		m_compare = compare; 
	}

	// Adds a item to the heap
	// @param item - the item that is being added
	bool AddItem(const X &item) {
		if(m_size >= m_max_queue_size)
			return false; 

		m_heap[m_size] = item; 
		TrickleUp(m_size++); 

		return true; 
	}

	// Returns the size of the queue
	inline int Size() {
		return m_size; 
	}

	// Resets the queue
	inline void Reset() {
		m_size = 0; 
	}

	// Returns the maximum queue size
	inline int OverflowSize() {
		return m_max_queue_size;
	}

	// Returns the top priority element
	inline X &Peek() {
		return m_heap[m_size - 1];
	}

	// Returns the highest priority item from the heap
	// @param item - stores the retrieved item
	bool PopItem(X &item) {
		if(Size() <= 0)return false; 

		item = m_heap[0]; 
		m_heap[0] = m_heap[--m_size]; 
		TrickleDown(0); 

		return true; 
	}

	// Just a test framework
	void TestPriorityQueue() {
		CMemoryChunk<int> element(99999, 0); 
		
		for(int i=0; i<99999; i++) {
			int item = (rand() % 99999); 
			AddItem(item); 
			element[item]++; 
		}

		int index = 99999 - 1; 
		for(int i=0; i<m_max_queue_size; i++) {
			while(!element[index]) {
				index--; 
			}
			int item; 
			PopItem(item); 

			if(item != index) {
				cout << "Priority Error "<< item <<" "<< index << endl; 
				getchar(); 
			}
			element[index]--; 
		}
	}

	// Reads the queue from file
	void ReadPriorityQueueFromFile(const char str[]) {
		CHDFSFile file; 
		file.OpenReadFile(CUtility::ExtendString
			(str, ".fast_priority_queue")); 

		ReadPriorityQueueFromFile(file); 
	}

	// Writes the queue to file
	void WritePriorityQueueToFile(const char str[]) {
		if(Size() <= 0) {
			return; 
		}

		CHDFSFile file; 
		file.OpenWriteFile(CUtility::ExtendString
			(str, ".fast_priority_queue")); 


		WritePriorityQueueToFile(file); 
		return true; 
	}

	// Reads the queue from file
	void ReadPriorityQueueFromFile(CHDFSFile &file) {
		file.ReadCompObject(m_size); 
		file.ReadCompObject(m_max_queue_size); 
		m_heap.AllocateMemory(m_size); 
		file.ReadCompObject(m_heap.Buffer(), m_size); 
	}

	// Writes the queue to file
	void WritePriorityQueueToFile(CHDFSFile &file) {
		file.WriteCompObject(m_size); 
		file.WriteCompObject(m_max_queue_size); 
		file.WriteCompObject(m_heap.Buffer(), m_size); 
	}
}; 

// This is a general purpose sorting class that 
// provides several sorting methods designed for
// sorting of different size LocalData sets. A comparison
// function must be provided.
template <class X> class CSort {

	// stores a Buffer to a compare function
	int (*m_compare)(const X &arg1, const X &arg2); 
	// stores the number of sorted elements
	static int m_size; 
	// stores a Buffer to the elements that need sorting
	static X *m_items; 

	// recursive quick sort
	void q_sort(X items[], X left, X right) {
		X pivot = items[left]; 
		while(left < right) {
			while((m_compare(items[right], pivot) >= 0) && (m_compare(left, right) < 0))
			right--; 
			if(left != right) {
				items[left] = items[right]; 
				left++; 
			}
			while((m_compare(items[right], pivot) <= 0) && (m_compare(left, right) < 0))
			left++; 
			if(m_compare(left, right)) {
				items[right] = items[left]; 
				right--; 
			}
		}
		items[left] = pivot; 
		pivot = left; 
		if(left < pivot)q_sort(items, left, pivot - 1); 
		if(right > pivot)q_sort(items, pivot + 1, right); 
	}

	// generic compare function
	static int GreaterCompare(const X &arg1, const X &arg2) {
		if(arg1 < arg2)return 1; 
		if(arg1 > arg2)return -1; 
		return 0; 
	}

	// generic compare function
	static int SmallerCompare(const X &arg1, const X &arg2) {
		if(arg1 > arg2)return 1; 
		if(arg1 < arg2)return -1; 
		return 0; 
	}

	// recursive quick sort used for hybrid sort
	void ReqQuickSort(int left, int right) {
		int size = right - left + 1; 
		if(size < 10)
			InsertionSort(left, right); 	// insertion sort if small
		else {	// quicksort if large
			X median = MedianOf3(left, right); 
			int partition = PartionIt(left, right, median); 
			ReqQuickSort(left, partition - 1); 
			ReqQuickSort(partition + 1, right); 
		}
	}

	// finds the median of three elements
	X MedianOf3(int left, int right) {
		int center = (left + right) >> 1; 

		// order left and center
		if(m_compare(m_items[left], m_items[center]) < 0)
			Swap(m_items[left], m_items[center]); 

		// order left and right
		if(m_compare(m_items[left], m_items[right]) < 0)
			Swap(m_items[left], m_items[right]); 

		// order center and right
		if(m_compare(m_items[center], m_items[right]) < 0)
			Swap(m_items[center], m_items[right]); 

		// put pivot on the right
		Swap(m_items[center], m_items[right - 1]); 
		return m_items[right - 1]; 	// return median value
	}

	// used for hybrid sort
	int PartionIt(int left, int right, X &pivot) {
		// right of the first element
		int left_ptr = left; 
		// left of the pivot
		int right_ptr = right - 1; 

		while(true) {
			// find the bigger
			while(m_compare(m_items[++left_ptr], pivot) > 0); 

			// find the smaller
			while(m_compare(m_items[--right_ptr], pivot) < 0); 

			// if Buffer cross partion done
			if(left_ptr >= right_ptr)
				break; 

			Swap(m_items[left_ptr], m_items[right_ptr]); 
		}

		// restore pivot
		Swap(m_items[left_ptr], m_items[right - 1]); 
		return left_ptr; 	// return pivot location
	}

	void InsertionSort(int left, int right) {

		int in, out; 
		for(out = left + 1; out <= right; out++) {	// out is dividing line
			X temp = m_items[out]; 	// removed marked item
			in = out; 	// start shift at out	
			while((in > left) && (m_compare(m_items[in - 1], temp) <= 0)) {	// until one is smaller
				m_items[in] = m_items[in - 1]; 
				--in; 	// go left one position
			}
			m_items[in] = temp; 
		}
	}

public:

	CSort() {
	}

	CSort(int size, int (*compare)(const X &arg1, const X &arg2)) {
		m_compare = compare; 
		m_size = size; 
	}

	CSort(int size) {
		m_size = size; 
	}

	// sets a comparison function given by the user
	void SetComparisionFunction
		(int (*compare)(const X &arg1, const X &arg2) = NULL) {
		m_compare = compare; 
	}

	// finds the maximum element from an buffer of elements
	template <class Y> static Y MaxElement(Y buffer[], int size) {
		Y max = buffer[0]; 
		for(int i=0; i<size; i++) {
			if(max < buffer[i]) {
				max = buffer[i]; 
			}
		}
		return max; 
	}

	// finds the minimum element from an buffer of elements
	template <class Y> static Y MinElement(Y buffer[], int size) {
		Y min = buffer[0]; 
		for(int i=0; i<size; i++) {
			if(min > buffer[i]) {
				min = buffer[i]; 
			}
		}
		return min; 
	}

	// sets the generic ascending compare function
	void SetAscendingCompareFunction() {
		m_compare = GreaterCompare; 
	}
	// sets the generic descending compare function
	void SetDescendingCompareFunction() {
		m_compare = SmallerCompare; 
	}

	// This is a very poor but simple sorting algorithm
	void BubbleSort(X *buffer, int size = m_size) {
		
		for(int a = 0; a < size; a++) {
			for(int b = a + 1; b < size; b++) {
				if(m_compare(buffer[b], buffer[a]) > 0) {
					Swap(buffer[a], buffer[b]); 
				}
			}
		}
	}

	// This performs a recursive quick sort
	static void QuickSort(X items[], int size = m_size) {
		q_sort(items, 0, size - 1); 
	}

	// This search performs in Nlog(N) time - quick sort
	// is still slightly faster
	void MergeSort(X items[], int size = m_size) {
		int level_size = 1; 
		while(true) {
			for(int i=0; i<size; i += (level_size << 1)) {
				int left = i, right = i + level_size; 
				int end = right + level_size; 
				if(end > size)end = size; 
				CMemoryChunk<X> index(end - i); 
				int offset = 0; 
				while(offset < (end - i)) {
					if(right >= end) 
						index[offset++] = items[left++]; 
					else if(left >= (i + level_size)) 
						index[offset++] = items[right++]; 
					else if(m_compare(items[left], items[right]) > 0) 
						index[offset++] = items[left++]; 	// first word id
					else index[offset++] = items[right++]; 	// first word id
				}	

				memcpy(items + i, index.Buffer(), sizeof(X) * (end - i)); 
			}
			level_size <<= 1; 
			if(level_size > size)break; 
		}
	}

	// performs a binary search in a buffer assumes the
	// buffer has been sorted
	// @param buffer - the buffer that contains the sorted items
	// @param value - the item that the search is looking for
	// @param start - the start offset for the buffer usually zero
	// @param end - the end offset for the buffer usually size
	// @param index - stores the index in the buffer where the element resides
	// @returns true if the element was found, false otherwise
	template <class Y> bool BinarySearch
		(X buffer[], X value, Y &start, Y &end, Y &index) {

		index = end; 
		while((end - start) > 1) {
			index = (end + start) >> 1;
			if(m_compare(buffer[index], value) == 0) 
				return true; 

			if(m_compare(buffer[index], value) < 0) 
				start = index; 
			else 
				end = index; 
		} 

		return false; 
	}

	// stores a list of item alphabetically
	static void AlphabeticSort(CArray<char> &text, CArray<X> &text_start, X order[]) {

		CUtility change; 
		CMemoryChunk<CMemoryChunk<char> > score(text_start.Size() - 1); 
		for(X i=0; i<text_start.Size() - 1; i++) {
			score[i].AllocateMemory(1000, 0); 
			int offset = 0; 
			for(int c=text_start[i]; c<text_start[i+1]; c++) {
				char ch = text[c]; 
				if(change.AskCapital(ch)) {
					CUtility::ConvertToLowerCaseCharacter(ch); 
				}
				if(ch >= 'a' && ch <= 'z')
					score[i][offset++] = ch; 
			}
			order[i] = i; 
		}

		for(int a = 0; a < text_start.Size() - 1; a++) {
			for(int b = a + 1; b < text_start.Size() - 1; b++) {
				for(int i=0; i<1000; i++) {
					int index1 = order[a], index2 = order[b]; 
					if(score[index2][i] < score[index1][i]) {
						Swap(order[a], order[b]); 
						break; 
					} 
					if(score[index2][i] > score[index1][i])
						break; 
				}
			}
		}
	}

	// most commonly used for O(N2)
	void InsertionSort(X items[], int size = m_size) {
		int in, out; 
		for(out = 1; out < size; out++) {	// out is dividing line
			X temp = items[out]; 	// removed marked item
			in = out; 	// start shift at out	
			while((in > 0) && (m_compare(items[in - 1], temp) <= 0)) {	// until one is smaller
				items[in] = items[in - 1]; 
				--in; 	// go left one position
			}
			items[in] = temp; 
		}
	}

	// most commonly used for O(N2)
	template <class Z, class Y> void InsertionSort
		(Z items[], Y *order, int size = m_size) {

		int in, out; 
		for(out = 1; out < size; out++) {	// out is dividing line
			Z temp1 = items[out]; 	// removed marked item
			Y temp2 = order[out]; 
			in = out; 	// start shift at out	
			while((in > 0) && (m_compare(items[in - 1], temp1) <= 0)) {	// until one is smaller
				items[in] = items[in - 1]; 
				order[in] = order[in - 1]; 
				--in; 	// go left one position
			}
			items[in] = temp1; 
			order[in] = temp2; 
		}
	}

	// This is the sorting algorithm that uses a heap
	// to sort the buffer
	void HeapSort(X items[], int size = m_size) {
		
		CPriorityQueue<X> priority_queue(size, m_compare); 
		
		for(int i=0; i<size; i++) {
			priority_queue.AddItem(items[i]); 
		}

		for(int i=0; i<size; i++) {
			items[i] = priority_queue.PopItem(); 
		}
	}

	// fast for smaller number of elements
	void ShellSort(X items[], int size = m_size) {
		
		if(size <= 0)return; 
		int inner, outer; 
		X temp; 

		int h = 1; 	// find the initial value of h
		while(h <= size / 3) {
			h = h * 3 + 1; 
		}

		while(h > 0) {
			for(outer = h; outer < size; outer++) {
				temp = items[outer]; 
				inner = outer; 

				while(inner > h - 1 && m_compare(items[inner - h], temp) >= 0) {
					items[inner] = items[inner - h]; 
					inner -= h; 
				}
				items[inner] = temp; 
			}
			h = (h - 1) / 3; 	// decreases h
		}
	}

	// this is the all in one super sort
	void HybridSort(X items[], int size = m_size) {
		if(size <= 1)return; 
		m_items = items; 
		ReqQuickSort(0, size - 1); 
	}

	// swaps two elements
	template <class Z> inline static 
		void Swap(Z &arg1, Z &arg2) {
			
		// swap them
		Z temp = arg1; 
		arg1 = arg2; 
		arg2 = temp; 
	}

	// delets an element from a list
	template <class Z, class Y> static void DeleteElement
		(Z list[], Y element, int size = m_size) {

		for(Y i=element; i<size; i++) {
			list[i] = list[i+1]; 
		}
	}

	// inserts an element into the list
	template <class Z, class Y> static void InsertElement
		(Z *list, Z element, Y index, int size) {
			
		for(Y i=index; i<size; i++) {
			list[size - i] = list[size - i - 1]; 
		}
		list[index] = element; 
	}
}; 
template <class X> int CSort<X>::m_size; 
template <class X> X *CSort<X>::m_items; 

class CArrayListBitString {
	// used to hold the bitstring
	CArrayList<char> m_string; 
	// stores the size of the bitstring
	int m_size; 

public:

	CArrayListBitString() {
	}

	// @param init_size - the initial size of the bit string
	CArrayListBitString(int init_size) {
		Initialize(init_size); 
	}

	// @param init_size - the initial size of the bit string
	void Initialize(int init_size) {
		m_string.Initialize(init_size); 
		m_size = 0; 
	}

	// returns the value of a particular index
	inline bool operator[](int index) {
		if(index >= Size() || index < 0)throw EOverflowException
			("Bit String Index Overflow"); 

		int base = index >> 3; 
		int offset = index - (base << 3); 
		return (m_string[base] & (1 << offset)) != 0; 
	}

	// sets a particular index to true
	void SetElement(int index) {
		if(index >= Size())throw EOverflowException
			("Bit String Index Overflow"); 
		int base = index >> 3; 
		int offset = index - (base << 3); 
		m_string[base] |= (1 << offset); 
	}

	// sets a particular index to false
	void ClearElement(int index) {
		if(index >= Size())throw EOverflowException
			("Bit String Index Overflow"); 

		int base = index >> 3; 
		int offset = index - (base << 3); 

		m_string[base] &= (char)~(1 << offset); 
	}

	// initilizes the bit string to some value
	// @param value - true for all one's, false for all zero's
	void FillBuffer(bool value) {
		if(value)
			m_string.InitializeMemoryChunk(-1);
		else
			m_string.InitializeMemoryChunk(0);
	}

	// returns the size of the bitstring
	inline int Size() {
		return m_size; 
	}

	// resizes a vector
	inline void Resize(int index) {
		m_size = index; 
		m_string.Resize(CUtility::ByteCount(index)); 
		if((index % 8) == 0)return;
		// clears the unused bits
		m_string.LastElement() &= (uChar)0xFF >> (8 - (index % 8));
	}

	// pushes back a single bit
	void PushBack(bool value) {	

		if(Size() >= (m_string.Size() << 3)) {
			m_string.PushBack((char)value); 
			m_size++;
			return; 
		} 

		int base = (m_size >> 3); 
		int offset = m_size - (base << 3); 
		m_string.LastElement() |= (value << offset); 
		m_size++;
	}

	// adds an element to the vector
	inline void PushBack(char element) {
		m_string.PushBack(element); 	// pushes back an entire byte
		m_size += 8; 
	}

	// returns a 4 - byte representation starting at index
	inline int GetNibble(int index) {
		int base = index >> 3; 
		if(index - (base << 3))return m_string[base] >> 4; 	// upper nibble
		
		return m_string[base] & 0x0F; 	// lower nibble
	}

	// returns a byte starting at index
	inline char GetByte(int index) {
		return m_string[index >> 3]; 
	}

	// just a testing framework
	void TestBitString() {
		CMemoryChunk<bool> test(999999); 
		for(int i=0; i<test.OverflowSize(); i++) {
			int number = (rand() % 2); 
			PushBack(number == 1); 
			test[i] = (number == 1); 
		}

		for(int i=0; i<test.OverflowSize(); i++) {
			if(this->operator[](i) != test[i]) {cout << "error"; getchar();}
		}
	}

	// reads a compressed bit string from file
	void ReadBitStringFromFile(const char str[]) {
		CHDFSFile file; 
		file.OpenReadFile(CUtility::ExtendString(str, ".bitstring")); 

		ReadBitStringFromFile(file);
	}

	// wrties a compressed bit string to file
	void WriteBitStringToFile(const char str[]) {
		if(Size() <= 0) {
			return; 
		}

		CHDFSFile file; 
		file.OpenWriteFile(CUtility::ExtendString(str, ".bitstring")); 

		WriteBitStringToFile(file);
	}

	// reads a compressed bit string from file
	void ReadBitStringFromFile(CHDFSFile &file) {
		file.ReadCompObject(m_size); 
		m_string.ReadArrayListFromFile(file); 
	}

	// writes a compressed bit string to file
	void WriteBitStringToFile(CHDFSFile &file) {
		file.WriteCompObject(m_size); 
		m_string.WriteArrayListToFile(file); 
	}
}; 

class CBitString {
	// used to hold the bitstring
	CVector<char> m_string; 
	// stores the size of the bitstring
	int m_size; 

public:

	CBitString() {
		m_size = 0; 
	}

	CBitString(CBitString & copy) {
		MakeBitStringEqualTo(copy); 
	}

	// copies an existing BitString
	void MakeBitStringEqualTo(CBitString & copy) {
		m_size = copy.Size(); 
		m_string.MakeVectorEqualTo(copy.m_string); 
	}

	// returns the value of a particular index
	inline bool operator[](int index) {
		if(index >= Size() || index < 0) {
			throw EIndexOutOfBoundsException
			("Bit String Index Overflow"); 
		}

		int base = index >> 3; 
		int offset = index - (base << 3); 
		return (m_string[base] & (1 << offset)) != 0; 
	}

	// sets a particular index to true
	void SetElement(int index) {
		if(index >= Size() || index < 0)throw EIndexOutOfBoundsException
			("Bit String Index Overflow"); 

		int base = index >> 3; 
		int offset = index - (base << 3); 
		m_string[base] |= (1 << offset); 
	}

	// sets a particular index to false
	void ClearElement(int index) {
		if(index >= Size())throw EOverflowException
			("Bit String Index Overflow"); 
		int base = index >> 3; 
		int offset = index - (base << 3); 

		m_string[base] &= (char)~(1 << offset); 
	}

	// returns the size of the bitstring
	inline int Size() {
		return m_size; 
	}

	// resizes a vector
	inline void Resize(int size) {
		m_size = size; 
		m_string.Resize(CUtility::ByteCount(Size())); 
	}

	// adds an element to the vector
	void PushBack(bool value) {	
		if(Size() >= (m_string.Size() << 3)) {
			m_string.PushBack((char)value); 
			m_size++;
			return; 
		} 

		int base = (m_size >> 3); 
		int offset = m_size - (base << 3); 
		m_string.LastElement() |= (value << offset); 
		m_size++;
	}

	// adds an element to the vector
	inline void PushBack(char element) {
		m_string.PushBack(element); 	// pushes back an entire byte
		m_size += 8; 
	}

	// returns a 4 - byte representation starting at index
	inline int GetNibble(int index) {
		int base = index >> 3; 
		if(index - (base << 3))return m_string[base] >> 4; 	// upper nibble
		
		return m_string[base] & 0x0F; 	// lower nibble
	}

	// just a test framework
	void TestBitString() {
		CMemoryChunk<bool> comp(99999); 
		for(int i=0; i<99999; i++) {
			comp[i] = (rand() % 2) == 1; 
			PushBack(comp[i] == 1); 
		}

		CBitString check(*this); 

		for(int i=0; i<99999; i++) {
			if(comp[i] != check[i]) {
				cout << "Error"; 
				getchar(); 
			}
		}
	}

	// returns a byte starting at index
	inline char GetByte(int index) {
		return m_string[index >> 3]; 
	}

	// reads a compressed bit string from file
	void ReadBitStringFromFile(const char str[]) {
		CHDFSFile file; 
		file.OpenReadFile(CUtility::ExtendString(str, ".bitstring")); 
		ReadBitStringFromFile(file);
	}

	// wrties a compressed bit string to file
	void WriteBitStringToFile(const char str[]) {

		CHDFSFile file; 
		file.OpenWriteFile(CUtility::ExtendString(str, ".bitstring")); 

		WriteBitStringToFile(file);
	}

	// reads a compressed bit string from file
	void ReadBitStringFromFile(CHDFSFile &file) {
		file.ReadCompObject(m_size); 
		m_string.ReadVectorFromFile(file); 
	}

	// writes a compressed bit string to file
	void WriteBitStringToFile(CHDFSFile &file) {
		file.WriteCompObject(m_size); 
		m_string.WriteVectorToFile(file); 
	}
}; 

// This is a general purpose tree that is constructed from a vector.
// It's a multidimensional tree with an arbitrary number of children.
// It's primarily used as a base class in CHashDictionary<int>.
template <class X> class CTree {
	// stores the current index in the tree
	X m_curr_segment;
	// stores the return point for the tree
	X m_return_point; 
	// stores the index values for the tree
	CVector<X> m_index; 
	// stores the vacant and node values for the tree
	CBitString m_leaf; 

	// returns the number of blocks for a given size
	int GetBlockSize(int size) {
		int base = size >> 3; 
		if(size - (base << 3))base++; 
		return base; 
	}

public:

	CTree() {
	}

	// Starts the tree by expanding the first level.
	// @param size - this is the breadth of the first level
	CTree(int size) {
		Initialize(size); 
	}

	// This is a copy constructor
	CTree(CTree & copy) {
		MakeTreeEqualTo(copy); 
	}

	// Starts the tree by expanding the first level.
	// @param size - this is the breadth of the first level
	void Initialize(int size) {
		Resize(0); 
		// sets all nodes to leafs
		for(int i=0; i<GetBlockSize(size); i++) {
			m_leaf.PushBack((char)0xFF); 	
		}

		for(int i=0; i<size; i++) {
			m_index.PushBack(-1); 	// initilizes to vacant
		}

		m_curr_segment = 0; 
	}

	// Copies a Tree
	void MakeTreeEqualTo(CTree<X> &copy) {
		m_curr_segment = copy.m_curr_segment; 
		m_return_point = copy.m_return_point; 
		m_index.MakeVectorEqualTo(copy.m_index); 
		m_leaf.MakeBitStringEqualTo(copy.m_leaf); 
	}

	// Expands the tree for a given size and child
	void ExpandTree(int size, int child) {
		// assigns a link to the new segment
		m_index[m_curr_segment + child] = (X)m_index.Size(); 	

		// indicates a new node in the path
		m_leaf.ClearElement(m_curr_segment + child); 		
		for(int i=0; i<size; i++) {
			m_index.PushBack(-1); 	// initilizes to vacant
		}

		int block_size = GetBlockSize(size); 
		// sets all nodes to leafs
		for(int i=0; i<block_size; i++) {
			m_leaf.PushBack((char)0xFF); 
		}

		// jumps to the next segment
		m_curr_segment = m_index[m_curr_segment + child]; 
	}

	// Traverses the tree on a given child
	bool TraverseTree(int child) {
		if(m_leaf[m_curr_segment + child]) {
			return true; 	
		}

		m_curr_segment = m_index[m_curr_segment + child]; 
		return false; 
	}

	// Sets a future return point
	inline X SetReturnPoint() {
		m_return_point = m_curr_segment; 
		return m_return_point; 
	}

	// Back track to return point
	inline void BackTrack() {
		m_curr_segment = m_return_point; 
	}

	// Back track to return point
	inline void BackTrack(X level) {
		m_curr_segment = level; 
	}

	// Returns the index for the tree
	inline X &operator[](int i) {
		return m_index[m_curr_segment + i]; 
	}

	// Resets the path for the tree
	inline void ResetPath() {
		m_curr_segment = 0; 
	}

	// Ask if a child is vacant
	inline bool AskVacant(int child) {
		return (m_index[m_curr_segment + child] == - 1); 
	}

	// Sets a child to vacant
	inline void SetVacant(int child) {
		m_index[m_curr_segment + child] = - 1; 	
	}

	// Ask if a child contains a subtree
	inline bool AskSubTree(int child) {
		return !m_leaf[m_curr_segment + child]; 
	}

	// Sets a child to contain a subtree
	inline void SetSubTree(int child) {
		m_leaf.ClearElement(m_curr_segment + child); 	
	}

	// Ask if a child is a node
	inline bool AskLeaf(int child) {
		return m_leaf[m_curr_segment + child]; 	
	}

	// Returns the current level of the tree
	inline X GetCurrentLevel() {
		return m_curr_segment; 
	}

	// Resize the tree
	inline void Resize(int size) {
		m_index.Resize(size); 
		m_leaf.Resize(size); 
	}

	// Remove the tree
	void RemoveTree(char file[]) {
		CHDFSFile::Remove(CUtility::ExtendString(file, ".tree")); 
	}

	// Reads a compressed tree to file
	void ReadTreeFromFile(const char str[]) {
		CHDFSFile file; 
		file.OpenReadFile(CUtility::
			ExtendString(str, ".tree")); 

		ReadTreeFromFile(file);
	}

	// Reads a compressed tree from file
	void ReadTreeFromFile(CHDFSFile &file) {
		m_index.ReadVectorFromFile(file); 
		m_leaf.ReadBitStringFromFile(file); 
	}

	// Writes a compressed tree from file
	void WriteTreeToFile(CHDFSFile &file) {
		m_index.WriteVectorToFile(file); 
		m_leaf.WriteBitStringToFile(file); 
	}

	// Writes a compressed tree to file
	void WriteTreeToFile(const char str[]) {

		CHDFSFile file; 
		file.OpenWriteFile(CUtility::
			ExtendString(str, ".tree")); 

		WriteTreeToFile(file);
	}
};  

// This is a very useful LocalData structure for mapping global
// indexes to local indexes as well as standard dictionary
// strings aswell. The branching factor of the trie can be 
// adjusted but must remain in the set 1, 2, 4, 8 for correct
// operation this is done automatically. Multi variable length
// strings can be supplied of different types although it's
// recommended that only primative LocalData types be used. 
// This LocalData structure is generally very fast although it does
// have prohibitive space requirements and should only be used
// on a smaller scale.
class CTrie {

	// stores a tree representation
	CTree<int> m_tree; 
	// stores the start of each word in the dictionary
	CVector<int> m_word_start; 
	// stores the dictionary of concanated words
	CVector<char> m_dictionary; 

	// stores the binary power expansion of the tree
	int m_base_size; 
	// stores the number to logical and with different
	// sections of the number
	int m_base_and; 
	// stores the power of the base in bits (power of 2)
	int m_base_power; 

	// stores the number of bits offset from the start of 
	// the word
	int m_word_offset; 
	// stores the start to of the dictionary word
	int m_dict_word_start; 
	// used to store temp words
	static char m_shuffle_word[100]; 
	// stores whether a word was found
	bool m_found; 

	// inserts a particular element in the subtree at
	// a given index
	// @param curr_index - the current row index in the tree
	// - corresponding to the branch index
	// @param global_index - the next index corresponding to 
	// - the number of strings stored in the trie
	bool InsertSubTree(int curr_index, int global_index) {
		if(!m_tree.AskSubTree(m_base_size)) { 
			m_tree.SetSubTree(m_base_size); 
			// expand the tree at the extension point
			m_tree.ExpandTree(m_base_size, m_base_size); 	

			m_tree[curr_index] = global_index; 
			return false; 
		}

		m_tree.TraverseTree(m_base_size); 
		if(m_tree.AskVacant(curr_index)) {
			m_tree[curr_index] = global_index; 
			return false; 
		}
		return true; 

	}

	// writes a word to the dictionary
	// @param str - a buffer containing the character
	// - str that is being written to the trie
	// @param length - the length of the character string
	// @return the number of character strings stored in the trie
	inline int WriteWord(const char str[], int length) {
		for(int i=0; i<length; i++) {
			m_dictionary.PushBack(str[i]); 
		}

		m_found = false; 
		m_word_start.PushBack((int)m_dictionary.Size()); 
		return(int)m_word_start.Size() - 2; 
	}

	// returns the next word index
	// @param str - contains the given charater string
	// @return the given word index corresponding to
	// a character in the str
	inline int GetWordIndex(const char str[]) {
		int base = m_word_offset >> 3; 
		int offset = m_word_offset % 8; 
		return (str[base] >> offset) & m_base_and; 
	}

	// returns the next dictionary index
	inline int GetDictionaryIndex() {
		int base = (m_dict_word_start + m_word_offset) >> 3; 
		int offset = m_word_offset % 8; 
		return (m_dictionary[base] >> offset) & m_base_and; 
	}

	// finds the size of the LocalData structure
	// @param size - the size of the LocalData structure
	// - in bytes
	// @return the log base 2 of param size
	static int BaseLog2Size(int size) {
		int base = 0; 
		int bit_pos = 0x01; 
		while((size & bit_pos) == 0) {
			bit_pos <<= 0x01; 
			base++; 
		}
		return base; 
	}

public:

	CTrie() {
	}

	CTrie(int base_power) {
		Initialize(base_power); 
	}

	CTrie(CTrie &copy) {
		MakeTrieEqualTo(copy); 
	}

	// starts the dictionary
	// @param base_power - the branching factor of the trie
	void Initialize(int base_power) {

		Reset();
		base_power = min(base_power, (int)8);
		// calculates the base size ie 2, 4, 8 ...
		m_base_power = 1; 
		while(m_base_power < base_power) {
			m_base_power <<= 1; 
		}

		m_base_size = 1 << m_base_power; 
		m_base_and = m_base_size - 1; 
	}

	// Used to copy an existing Trie
	// @param copy - the trie that is being copied
	void MakeTrieEqualTo(CTrie &copy) {
		m_base_size = copy.m_base_size; 
		m_base_and = copy.m_base_and; 
		m_base_power = copy.m_base_power; 
		m_word_offset = copy.m_word_offset; 
		m_dict_word_start = copy.m_dict_word_start; 
		m_found = copy.m_found; 

		m_tree.MakeTreeEqualTo(copy.m_tree); 
		m_word_start.MakeVectorEqualTo(copy.m_word_start); 
		m_dictionary.MakeVectorEqualTo(copy.m_dictionary); 
	}

	// Adds a word to the dictionary
	// @param word - a buffer containing the items that are 
	//             - being added to the trie
	// @param length - the number of items stored in the buffer
	// @param start - an offset in the items buffer
	// @return the corresponding index of the item string
	int AddWord(const char word[], int length, int start = 0) {
		m_found = true; 	
		m_tree.ResetPath(); 
		word += start; 
		length -= start; 

		int curr_index = 0; 
		int base_length = length << 3; 
		m_word_offset = 0; 

		for(int i=0; i<base_length; i += m_base_power) {
			curr_index = GetWordIndex(word); 
			m_word_offset += m_base_power; 
			m_tree.SetReturnPoint(); 	

			if(m_tree.TraverseTree(curr_index)) {	
				// reached the end of the tree
				if(m_tree.AskVacant(curr_index)) {		
					// checks to see if something previously stored
					// writes the word to dictionary
					m_tree[curr_index] = WriteWord(word, length); 	
					return m_tree[curr_index]; 
				}

				int dictionary_index = m_tree[curr_index]; 
				int dict_start = m_word_start[dictionary_index]; 
				int dictionary_length = (m_word_start[dictionary_index+1] - dict_start) << 3; 

				if(dictionary_length == base_length) {
					int begin = m_word_offset >> 3; 
					int offset = begin; 
					for(int c=dict_start + begin; c<m_word_start[dictionary_index+1]; c++) {
						if(word[offset++] != m_dictionary[c]) {
							m_found = false; 
							break; 
						}
					}
					if(m_found) {
						return m_tree[curr_index]; 
					}
				} 

				// word not in dictionary
				int dictionary_letter = curr_index; 
				m_dict_word_start = dict_start << 3; 
				m_found = false; 

				while(true) {
					if(m_word_offset == base_length) {
						m_tree.SetReturnPoint(); 
						// inserts the AskSubTree if required
						InsertSubTree(curr_index, WriteWord(word, length)); 	
						m_tree.BackTrack(); 
						m_tree.ExpandTree(m_base_size + 1, curr_index); 
						// get the next inex in the dictionary
						dictionary_letter = GetDictionaryIndex(); 	
						m_tree[dictionary_letter] = dictionary_index; 
						return m_word_start.Size() - 2; 

					} 
					if(m_word_offset == dictionary_length) {
						m_tree.SetReturnPoint(); 
						// inserts the AskSubTree if required
						InsertSubTree(curr_index, dictionary_index); 	
						m_tree.BackTrack(); 
						m_tree.ExpandTree(m_base_size + 1, curr_index); 
						// get the next index in the word
						curr_index = GetWordIndex(word); 	

						// writes the word to dictionary
						m_tree[curr_index] = WriteWord(word, length); 	
						return m_tree[curr_index]; 
					} 

					// expands the tree at the current index
					m_tree.ExpandTree(m_base_size + 1, curr_index); 	
					curr_index = GetWordIndex(word); 
					dictionary_letter = GetDictionaryIndex(); 
					m_word_offset += m_base_power; 
					if(dictionary_letter != curr_index) {
						break;
					}
				} 

				// sets the previously stored dictionary index in the new position
				m_tree[dictionary_letter] = dictionary_index; 	 
				// writes the word to dictionary
				m_tree[curr_index] = WriteWord(word, length); 	
				return m_tree[curr_index]; 
			} 
			
			if(m_word_offset == base_length) {
				m_tree.BackTrack(); 	// back tracks one 
				// writes the word if the AskSubTree doesn't exist
				if(!InsertSubTree(curr_index, m_word_start.Size() - 1))	{
					m_tree[curr_index] = WriteWord(word, length); 	
				}
					
				return m_tree[curr_index]; 
			}
		}
		return 0; 
	}

	// Looks for a word in the dictionary
	// @param closest_match - the index of the word that was mostly matched
	// @param word - a buffer containing the items that are 
	//             - being added to the trie
	// @param length - the number of items stored in the buffer
	// @param start - an offset in the items buffer
	// @return the corresponding index of the item string
	int FindWord(int &closest_match, const char word[], int length, int start = 0) {
		m_found = false; 	// assume the word is in the dictionary
		m_tree.ResetPath(); 
		word += start; 
		length -= start; 

		m_word_offset = 0; 
		int curr_index = 0; 
		int base_length = length << 3; 
		closest_match = -1;

		for(int i=0; i<base_length; i += m_base_power) {
			curr_index = GetWordIndex(word); 
			m_word_offset += m_base_power; 
			// set the return point for the tree
			m_tree.SetReturnPoint(); 

			if(m_tree.AskSubTree(m_base_size)) {
				m_tree.TraverseTree(m_base_size); 
				if(m_tree.AskVacant(curr_index) == false) {
					closest_match = m_tree[curr_index];
				}

				m_tree.BackTrack();
			}

			// reached the end of the tree
			if(m_tree.TraverseTree(curr_index)) {	
				if(m_tree.AskVacant(curr_index)) {
					return -1; 
				}

				int dictionary_index = m_tree[curr_index]; 
				int dict_start = m_word_start[dictionary_index]; 

				int dictionary_length = (m_word_start
					[dictionary_index+1] - dict_start) << 3; 

				if(dictionary_length != base_length) {
					closest_match = m_tree[curr_index];
					return -1; 
				}

				int begin = m_word_offset >> 3; 
				int offset = begin; 
				for(int c=dict_start + begin; c<m_word_start[dictionary_index+1]; c++) {
					if(word[offset++] != m_dictionary[c]) {
						closest_match = m_tree[curr_index];
						return -1; 
					}
				}

				m_found = true; 
				return m_tree[curr_index]; 
			} else if(m_word_offset == base_length) {
				m_tree.BackTrack(); 	
				if(!m_tree.AskSubTree(m_base_size)) {
					if(m_tree.AskLeaf(curr_index)) {
						closest_match = m_tree[curr_index];
					}
	
					return -1; 
				}

				m_tree.TraverseTree(m_base_size); 
				if(m_tree.AskVacant(curr_index)) {
					return -1; 
				}

				m_found = true; 
				return m_tree[curr_index]; 
			}
		}
		return -1; 
	}

	// Looks for a word in the dictionary
	// @param word - a buffer containing the items that are 
	//             - being added to the trie
	// @param length - the number of items stored in the buffer
	// @param start - an offset in the items buffer
	// @return the corresponding index of the item string
	inline int FindWord(const char word[], int length, int start = 0) {
		int closest_match;
		return FindWord(closest_match, word, length, start);
	}

	// This deletes a word in the dictionary
	// @param word - a buffer containing the items that are 
	//             - being added to the trie
	// @param length - the number of items stored in the buffer
	// @param start - an offset in the items buffer
	// @return the corresponding index of the item string
	bool DeleteWord(const char word[], int length, int start = 0) {
		m_found = true; 	// assume the word is in the dictionary
		m_tree.ResetPath(); 
		word += start; 
		length -= start; 

		m_word_offset = 0; 
		int curr_index = 0; 
		int base_length = length << 3; 

		for(int i=0; i<base_length; i += m_base_power) {
			curr_index = GetWordIndex(word); 
			m_word_offset += m_base_power; 
			m_tree.SetReturnPoint(); 	// set the return point for the tree

			if(m_tree.TraverseTree(curr_index)) {	
				// reached the end of the tree
				if(m_tree.AskVacant(curr_index))return false; 

				int dictionary_index = m_tree[curr_index]; 
				int dict_start = m_word_start[dictionary_index]; 

				int dictionary_length = (m_word_start
					[dictionary_index+1] - dict_start) << 3; 

				if(dictionary_length != base_length)return false; 

				int begin = m_word_offset >> 3; 
				for(int c=dict_start + begin, d = begin; c<
					m_word_start[dictionary_index+1]; c++, d++) 
						if(word[d] != m_dictionary[c])return false; 

				// found word
				m_tree.SetVacant(curr_index); 

			} else if(m_word_offset == base_length) {
				m_tree.BackTrack(); 	// back tracks one 
				if(!m_tree.AskSubTree(m_base_size))return false; 

				m_tree.TraverseTree(m_base_size); 
				if(m_tree.AskVacant(curr_index))return false; 

				m_tree.SetVacant(curr_index); 
			}
		}
		return false; 
	}

	// this is just a testing framework
	void TestTrie() {
		CArrayList<char> word; 
		word.Initialize(); 
		CArrayList<int> word_start; 
		word_start.Initialize(); 
		word_start.PushBack(0); 

		// ReadTrieFromFile("check"); 

		for(int i=0; i<200000; i++) {
			if((rand() % 2)) {
				int length = (rand() % 23) + 5; 
				for(int c=0; c<length; c++) {
					char a = (rand() % 99999999); 
					word.PushBack(a); 
				}

				word_start.PushBack(word.Size()); 
				int out = FindWord(word.Buffer(), word.Size(), word.Size() - length); 
				if(out >= 0) {cout << "new error"; getchar();}

				AddWord(word.Buffer(), word.Size(), word.Size() - length); 
				if(AskFoundWord()) {
					cout << "error1"<< endl; 
					for(int i=word.Size() - length; i<word.Size(); i++)
						cout << word[i]; getchar(); 
				}

			} else if((word_start.Size() - 1)) {
				int index = (rand() % (word_start.Size() - 1)); 
				int out = FindWord(word.Buffer(), word_start[index+1], word_start[index]); 
				if(out < 0) {
					cout << "error2"<< endl; 
					cout << word_start[index+1] <<" "<< word_start[index]; 
					getchar(); 
					for(int i=word_start[index]; i<word_start[index+1]; i++)
						cout << word[i]; 
					getchar(); 
				}
				if(out != index) {
					cout << "error3"; 
					getchar(); 
				}

				int length; 
				char *check = GetWord(index, length); 
				if(length != (word_start[index+1] - word_start[index])) {
					cout << "error4 "<< length <<" "<< word_start[index+1] - word_start[index]; 
					getchar(); 
				}
				for(int i=word_start[index], h = 0; i<word_start[index+1]; i++, h++) {
					if(check[h] != word[i]) {
						cout << "error5"; 
						getchar(); 
					}
				}
			}
		}
		// WriteTrieToFile("check"); 
	}

	// Checks if the previous word that was queried was found
	inline bool AskFoundWord() {
		return m_found; 
	}

	// Checks if a word exists in the dictionary
	bool AskFoundWord(const char word[], int length, int start = 0) {
		AddWord(word, length, start); 
		return AskFoundWord(); 
	}

	// Returns the item string for some index
	char *GetWord(int index, int &length) {
		if(index < 0 || index >= Size())throw
			EIllegalArgumentException("Invalid Index");

		length = m_word_start[index+1] - m_word_start[index]; 

		char *word = m_shuffle_word; 
		m_dictionary.CopyVectorToBuffer(word, 
			length, m_word_start[index]); 

		return word; 
	}

	// Returns the item string for some index
	inline char *GetWord(int index) {
		int length;
		char *word = GetWord(index, length);
		word[length] = '\0';
		return word;
	}

	// This retrieves a block of words from the 
	// trie buffer at once. Allows for faster
	// bulk access of all items stored.
	// @param buffer - a place to store the retrieved word block
	// @param start - the index for the lower boundary of word items to retrieve
	// @param end - the index for the upper boundary of word items to retrieve
	void GetWordBlock(CMemoryChunk<char> &buffer, int start, int end) {
		if(start < 0 || start >= Size() || end < 0 || end >= Size()) {
			throw EIllegalArgumentException("Invalid Index");
		}

		buffer.AllocateMemory(m_word_start[end] - m_word_start[start]);
		m_dictionary.CopyVectorToBuffer(buffer.Buffer(), 
			buffer.OverflowSize(), m_word_start[start]); 
	}

	// Returns the length of some word
	inline int WordLength(int index) {
		return m_word_start[index+1] - m_word_start[index]; 
	}

	// Shows a word
	inline void ShowWord(int index) {
		for(int i=m_word_start[index]; i<m_word_start[index+1]; i++)
			cout << m_dictionary[i]; 
	}

	// Returns the size of the dictionary
	inline int Size() {
		return m_word_start.Size() - 1; 
	}

	// Resets the dictionary
	inline void Reset() {
		m_tree.Resize(0); 
		m_word_start.Resize(0); 
		m_dictionary.Resize(0); 
		m_word_start.PushBack(0); 
		m_tree.Initialize(257); 	
	}

	// Removes the dictionary file
	void RemoveTrie(const char str[]) {
		CHDFSFile::Remove(CUtility::ExtendString(str, ".big_restring")); 
	}

	// Reads the dictionary from file
	void ReadTrieFromFile(const char str[]) {

		CHDFSFile file; 
		file.OpenReadFile(CUtility::
			ExtendString(str, ".big_restring")); 

		ReadTrieFromFile(file); 
	}

	// Reads the dictionary from file
	void ReadTrieFromFile(CHDFSFile &file) {

		file.ReadCompObject(m_base_size); 
		file.ReadCompObject(m_base_and); 
		file.ReadCompObject(m_base_power); 

		m_tree.ReadTreeFromFile(file); 
		m_word_start.ReadVectorFromFile(file); 
		m_dictionary.ReadVectorFromFile(file); 
	}

	// Writes the dictionary to file
	void WriteTrieToFile(const char str[]) {

		CHDFSFile file; 
		file.OpenWriteFile(CUtility::
			ExtendString(str, ".big_restring")); 

		WriteTrieToFile(file); 
	}

	// Writes the dictionary to file
	void WriteTrieToFile(CHDFSFile &file) {

		file.WriteCompObject(m_base_size); 
		file.WriteCompObject(m_base_and); 
		file.WriteCompObject(m_base_power); 

		m_tree.WriteTreeToFile(file); 
		m_word_start.WriteVectorToFile(file); 
		m_dictionary.WriteVectorToFile(file); 
	}
}; 
char CTrie::m_shuffle_word[100]; 


// This is a general purpose utility class related to
// math functions dealing specifically with calculating
// various statistics of a LocalData set given such as 
// standard deviation, mean, variance etc.
class CMath {

public:

	// calculates the mean
	// @param set - a buffer containing a string of items
	// @param size - the size of the item buffer
	// @return the mean
	template <class X> static float Mean
		(X set[], int size) {

		X sum = 0; 
		for(int i=0; i<size; i++) {
			sum += set[i]; 
		}
		return (float)sum / size; 
	}

	// This returns the value of PI
	static double PIConst() {
		return 3.141592654;
	}

	// calculates the variance
	// @param set - a buffer containing a string of items
	// @param size - the size of the item buffer
	// @return the variance
	template <class X> static float Variance
		(X set[], int size) {

		float mean = Mean(set, size); 

		float sum = 0; 
		for(int i=0; i<size; i++) {
			sum += set[i] - mean; 
		}
		return sum / (size - 1); 
	}

	// sums the elements in a buffer
	// @param set - a buffer containing a string of items
	// @param size - the size of the item buffer
	// @return the net sum
	template <class X> static X SumElements(X set[], int size) {
		X sum = 0;
		for(int i=0; i<size; i++) {
			sum += set[i];
		}

		return sum;
	}

	// finds the floor of a floating point number
	static float RoundNumber(float number) {
		float base = floor(number); 
		if(number - base > 0.5)
			number = base + 1; 
		return number; 
	}

	// this generates a gaussian random number, using 
	// rejection sampling (not very efficient)
	// @param [a b] the domain of the random number
	// @return the gaussian random number
	static float RandGauss(float a, float b) {

		while(true) {

			float x = (float)(rand() % 8000) - 4000;
			x /= 2000;
			float height = (float)(rand() % 10000) / 10000;

			if(height <= exp(-0.5f * x * x)) {
				float offset = (x + 2) / 4.0f;
				return ((b - a) * offset) + a;
			}
		}
	}

	// this normalises all the values in a given vector
	// @param vector - contains the buffer that needs to be
	//               - normalised
	// @param size - the number of elements in the vector
	template <class X> static X NormalizeVector(X vector[], int size) {
		X sum = 0;
		for(int i=0; i<size; i++) {
			sum += vector[i];
		}

		for(int i=0; i<size; i++) {
			vector[i] /= sum;
		}

		return sum;
	}

	// finds the log base 2 of a number
	// @param number - the number to find the log of
	// @return the log base 2 of a number
	template <class X> static int LogBase2(X number) {
		int byte_count = sizeof(X); 
		uChar *buffer = (uChar *)&number; 
		int size = byte_count << 3; 
		for(int i=byte_count - 1; i >= 0; i--) {
			size -= 8; 
			if(buffer[i] & 0xFF) {
				if(buffer[i] & 0xF0) {
					if(buffer[i] & 0xC0) {
						if(buffer[i] & 0x80)return size + 8; 
						return size + 7; 
					} else {
						if(buffer[i] & 0x20)return size + 6; 
						return size + 5; 
					}
				} else if(buffer[i] & 0x0C) {
					if(buffer[i] & 0x08)return size + 4; 
					return size + 3; 
				} else {
					if(buffer[i] & 0x02)return size + 2; 
					return size + 1; 
				}
			}
		}
		return 1; 
	}

	// returns the floor of a number 
	// @param number - the number which the floor is being found for
	// @return the floor of a given number 
	template<class X> X Floor(X number) {
		return floor(number);
	}

	// returns the ceiling of a number 
	// @param number - the number which the ceiling is being found for
	// @return the ceiling of a given number 
	template<class X> X Ceiling(X number) {
		return ceiling(number);
	}

	// calculates the variance
	// @param set - a buffer containing a string of items
	// @param size - the size of the item buffer
	// @param mean - the mean of the LocalData set buffer
	// @return the variance
	template <class X> static float Variance
		(X set[], int size, float mean) {

		float sum = 0; 
		for(int i=0; i<size; i++) {
			sum += (set[i] - mean) * (set[i] - mean); 
		}
		return sum / (size - 1); 
	}

	// calculates the standard deviation
	// @param set - a buffer containing a string of items
	// @param size - the size of the item buffer
	// @return the standard deviation
	template <class X> inline static float 
		StandardDeviation(X set[], int size) {
		return sqrt((float)Variance(set, size)); 
	}

	// calculates the standard deviation
	// @param set - a buffer containing a string of items
	// @param size - the size of the item buffer
	// @param mean - the mean of the LocalData set buffer
	// @return the standard deviation
	template <class X> inline static float 
		StandardDeviation(X set[], int size, float mean) {
		return sqrt((float)Variance(set, size, mean)); 
	}


	// finds the standard error for a distribution
	// calculates the standard deviation
	// @param set - a buffer containing a string of items
	// @param size - the size of the item buffer
	// @return the standard error
	template <class X> inline float StandardError
		(X set[], int size) {
			
		return Variance(set, size) / sqrt(size); 
	}

	// finds the standard error for a distribution
	// calculates the standard deviation
	// @param set - a buffer containing a string of items
	// @param size - the size of the item buffer
	// @param mean - the mean of the LocalData set buffer
	// @return the standard deviation
	template <class X> inline float StandardError
		(X set[], int size, float mean) {
		return Variance(set, size, mean) / sqrt(size); 
	}

	// calculates the probability of some value given
	// some Gaussian distribution
	// @param set - a buffer containing a string of items
	// @param size - the size of the item buffer
	// @param x - a number in the domain [-1, 1]
	// @return the gaussian probability
	template <class X> static float GaussianProbability
		(X set[], int size, float x) {

		float mean = Mean(set, size); 
		float variance = Variance(set, size, mean); 
		
		return 1.0f / (sqrt(2 * PIConst() * sqrt(variance))) * 
			(exp( - 0.5f * ((x - mean) * (x - mean))) / variance); 
	}

	// returns a value in the range [ - 1 1] where the 
	// probability lies
	// @param set - a buffer containing a string of items
	// @param size - the size of the item buffer
	// @param x - a number in the domain [-1, 1]
	// @return the z-score
	template <class X> inline void ZScore
		(X set[], int size, float x) {
		float mean = Mean(set, size); 
		return (mean - x) / StandardError(set, size, mean); 
	}
}; 

// Stores a position in the linked buffer 
// so it can be directly skipped to later
struct SLinkedBuffPos {
	// stores a generic pointer to the 
	// current buffer chunk
	void *buff_ptr;
	// stores the offset within the 
	// current buffer chunk;
	int buff_offset;
};

// This is just a standard linked list that is most commonly
// used as a buffer to store individual items rather than
// allocating memory explicitly for each item it can be done
// more efficiently in a linked list LocalData structure. The class
// provides some searching features and memory copying but
// is mostly limited to one way traversal. Also useful for 
// storing LocalData of undetermined size that can be accessed
// sequentiall such as a text document.
template <class X> class CLinkedBuffer {

	// This just stores a block of objects in the linked buffer
	struct SLink {
		X *buffer; 
		SLink *forward_link; 
	}; 

	// Stores the buffer size
	int m_chunk_size; 
	// Stores the offset within the buffer at the end 
	// of the list
	int m_buffer_offset;
	// Stores the offset within the buffer at the
	// current position in the list
	int m_path_offset; 
	// Stores the current size of the list
	int m_size; 
	// Stores the number of elements traversed in the
	// list when doing a linear search
	int m_traversed_length;

	// Stores a ptr to the root buffer
	SLink *m_root_node;
	// Stores a ptr to the current buffer
	SLink *m_curr_node;
	// Stores a ptr to the buffer for
	// the current list item when traversing
	SLink *m_path_node; 
	// Stores a buffer to the current element
	X *m_curr_element; 
 
	// stores the comparison function - used to check
	// if a node is contained in the linked buffer
	int (*m_compare)(const X &arg1, const X &arg2); 

	// This frees the memory used to store the linked buffer.
	// The root node is only freed in the destructor.
	void DestroyLinkedBuffer() {
		if(m_root_node == NULL) {
			return;
		}

		SLink *curr_node = m_root_node->forward_link;
		SLink *prev_node;

		while(curr_node != NULL) {
			prev_node = curr_node; 
			curr_node = curr_node->forward_link; 
			delete []prev_node->buffer; 
			delete prev_node; 
		}

		m_root_node->forward_link = NULL;
	}

public:

	CLinkedBuffer() {
		m_chunk_size = -1;
		m_root_node = NULL; 
		m_traversed_length = 0; 
	}

	// Starts the linked buffer
	// @param chunk_size - the size of each linked buffer segment
	CLinkedBuffer(int chunk_size) {
		m_chunk_size = -1;
		m_root_node = NULL; 
		Initialize(chunk_size); 
	}

	// sets the comparison funciton
	// @param compare - the comparison function
	void SetComparisonFunction(int (*compare)
		(const X &arg1, const X &arg2)) {

		m_compare = compare; 
	}

	// This returns the size of the linked buffer
	inline int BufferSize() {
		return m_chunk_size;
	}

	// This restarts the buffer with the first block of LocalData not 
	// initialized as given by a constructor. Only used when for
	// efficiency when the buffer is constantly being reset.
	// @param chunk_size - the size of each linked buffer segment
	void Restart(int chunk_size = 2048) {

		DestroyLinkedBuffer(); 
		if(m_chunk_size < 0) {
			m_chunk_size = chunk_size; 
		}

		if(m_root_node == NULL) {
			m_root_node = new SLink; 
			m_root_node->buffer = new X [chunk_size]; 
			m_root_node->forward_link = NULL; 
		}

		m_curr_node = m_root_node; 
		m_buffer_offset = 0; 
		m_size = 0; 
		
		m_path_offset = 0; 
		m_traversed_length = 0; 
		m_path_node = m_root_node; 
		m_curr_element = &m_path_node->buffer[m_path_offset]; 
	}

	// This is called to start the buffer with all elements 
	// initialized in the the constructor.
	// @param chunk_size - the size of each linked buffer segment
	void Initialize(int chunk_size = 2048) {
		FreeMemory();
		Restart(chunk_size);
	}

	// Adds a 1-byte item that may be escaped to 3-bytes
	// @param item - the item that is to be escape encoded
	template <class Y> int AddEscapedItem(Y item) {
		int bytes = 1; 
		while(item >= 128) {
			PushBack((uChar)(item | 0x80)); 
			item >>= 7; 
			bytes++; 
		}

		PushBack((uChar)item); 
		return bytes; 
	}

	// Adds an item to the linked buffer
	// @param element - the element that is being added
	void PushBack(const X &element) {
		if(m_buffer_offset >= m_chunk_size) {
			m_curr_node->forward_link = new SLink; 
			m_curr_node = m_curr_node->forward_link; 
			m_curr_node->buffer = new X [m_chunk_size]; 
			m_curr_node->forward_link = NULL; 
			m_buffer_offset = 0; 
		}

		m_curr_node->buffer[m_buffer_offset++] = element; 
		m_size++; 
	}

	// Checks if a node is in the linked buffer
	// @param node - the node that is being checked to see is there
	// @param compare - the comparison function to check for equality
	bool AskContainNode(const X &node, int (*compare)
		(const X &arg1, const X &arg2) = NULL) {

		return (FindNode(node, compare) != NULL); 
	}

	// Checks if a node is in the buffer
	// @param node - the node that is being checked to see is there
	// @param compare - the comparison function to check for equality
	X *FindNode(const X &node, int (*compare)
		(const X &arg1, const X &arg2) = NULL) {

		if(compare)m_compare = compare; 
		ResetPath(); 

		for(int i=0; i<Size(); i++) {
			X &next = NextNode(); 
			if(m_compare(next, node) == 0) {
				return &next; 
			}
		}

		return NULL; 
	}

	// Adds a 3 - byte item
	// @param item - the item that is being added
	template <class Y> inline void AddItem3Bytes(Y item) {
		PushBack(*((char *)&item)); 
		PushBack(*(((char *)&item) + 1)); 
		PushBack(*(((char *)&item) + 2)); 
	}

	// Extends the buffer size to make space for N 
	// more elements in the buffer. 
	// @param size - the size of the extension
	// @returns a pointer to the first element in
	// the extended buffer
	X *ExtendSize(int size) {
		if(size <= 0) {
			throw EIllegalArgumentException("size <= 0"); 
		}

		X *prev_elem = NULL;
		SLink *prev_link = m_curr_node;
		int prev_offset = m_buffer_offset;
		Resize(Size() + size); 

		if(prev_offset >= m_chunk_size) {
			prev_elem = (prev_link->forward_link)->buffer; 
		} else {
			prev_elem = &prev_link->buffer[prev_offset]; 
		}

		return prev_elem; 
	}

	// Resizes the buffer
	// @param size - the size of the new buffer
	void Resize(int size) {
		if(size < 0) {
			throw EIllegalArgumentException("size < 0"); 
		}

		if(size < m_size) {
			Reset(); 
		}

		size -= m_size; 
		m_size += size; 
		// returns the first element of the extened buffer
		if((m_chunk_size - m_buffer_offset) >= size) {
			m_buffer_offset += size; 
			return;
		}

		size -= m_chunk_size - m_buffer_offset; 

		while(size > m_chunk_size) {
			size -= m_chunk_size; 
			// allocates the next block
			m_curr_node->forward_link = new SLink; 
			m_curr_node = m_curr_node->forward_link; 
			m_curr_node->buffer = new X [m_chunk_size]; 
			m_curr_node->forward_link = NULL; 
			m_buffer_offset = 0; 
		}

		if(size <= 0) {
			return;
		}

		m_curr_node->forward_link = new SLink; 
		m_curr_node = m_curr_node->forward_link; 
		m_curr_node->buffer = new X [m_chunk_size]; 
		m_curr_node->forward_link = NULL; 
		m_buffer_offset = size; 
	}

	// Used to test the extend buffer function
	void TestExtendBuffer() {
		Initialize(100);
		CArray<int> comp(9999);
		for(int i=0; i<9999; i++) {
			int *ptr = ExtendSize(1);
			*ptr = rand();
			comp.PushBack(*ptr);
		}

		ResetPath();
		for(int i=0; i<9999; i++) {
			if(comp[i] != NextNode()) {
				cout<<"error "<<i;getchar();
			}
		}
	}

	// Returns the last element from the linked buffer
	inline X &LastElement() {
		if(m_size <= 0)throw EUnderflowException
			("Linked Buffer Last Element"); 
		return m_curr_node->buffer[m_buffer_offset - 1]; 
	}

	// Returns the size of the linked buffer
	inline int Size() {
		return m_size; 
	}

	// Resets the linked buffer path
	void ResetPath() {
		m_path_node = m_root_node; 
		m_path_offset = 0; 
		m_traversed_length = 0; 
		m_curr_element = &m_path_node->buffer[m_path_offset]; 
	}

	// Copy the linked buffer to an external buffer of some size.
	// @param buffer - a buffer that contains the elements that are
	// - being copied accross
	// @param size - the number of elements in the buffer
	void CopyLinkedBufferToBuffer(X buffer[], int size) {
		if(size < 0)throw EIllegalArgumentException("size < 0"); 

		if(size <= (m_chunk_size - m_path_offset)) {
			memcpy(buffer, m_path_node->buffer + m_path_offset, size * sizeof(X)); 
			m_path_offset += size; 
			return; 
		}

		memcpy(buffer, m_path_node->buffer + m_path_offset, 
			(m_chunk_size - m_path_offset) * sizeof(X)); 

		m_path_node = m_path_node->forward_link; 
		size -= m_chunk_size - m_path_offset; 
		int offset = m_chunk_size - m_path_offset; 
		m_path_offset = 0; 

		while(size > m_chunk_size) {
			size -= m_chunk_size; 
			memcpy(buffer + offset, m_path_node->buffer, sizeof(X) * m_chunk_size); 
			m_path_node = m_path_node->forward_link; 
			offset += m_chunk_size; 
		}
		memcpy(buffer + offset, m_path_node->buffer, sizeof(X) * size); 
		m_path_offset += size; 
	}

	// Copies an external buffer to the linked buffer of some size
	// to the position of the current path offset.
	// @param buffer - buffer containing the elements the need copying
	// @param size - the number of elements that need to be copied
	void CopyBufferToLinkedBuffer(const X buffer[], int size) {
		if(size < 0)throw EIllegalArgumentException("size < 0"); 

		m_size += size;
		if(size < (m_chunk_size - m_path_offset)) {
			memcpy(m_path_node->buffer + m_path_offset, 
				buffer, size * sizeof(X)); 
			m_path_offset += size; 
			return; 
		}
		
		memcpy(m_path_node->buffer + m_path_offset, buffer, 
			(m_chunk_size - m_path_offset) * sizeof(X)); 

		size -= m_chunk_size - m_path_offset; 
		int offset = m_chunk_size - m_path_offset; 

		while(size >= m_chunk_size) {
			if(m_path_node->forward_link == NULL) {
				// allocates the next block
				m_path_node->forward_link = new SLink; 
				m_path_node = m_path_node->forward_link; 
				m_path_node->buffer = new X [m_chunk_size]; 
				m_path_node->forward_link = NULL; 
			} else {
				m_path_node = m_path_node->forward_link; 
			}
			size -= m_chunk_size; 
			memcpy(m_path_node->buffer, buffer + offset, sizeof(X) * m_chunk_size); 
			offset += m_chunk_size; 
		}

		if(m_path_node->forward_link == NULL) {
			// allocates the next block
			m_path_node->forward_link = new SLink; 
			m_path_node = m_path_node->forward_link; 
			m_path_node->buffer = new X [m_chunk_size]; 
			m_path_node->forward_link = NULL; 
		} else {
			m_path_node = m_path_node->forward_link; 
		}

		memcpy(m_path_node->buffer, buffer + offset, sizeof(X) * size); 
		m_path_offset = size; 
	}

	// Copies an external buffer to the linked buffer of some size
	// to the position of the current path offset.
	// @param buffer - buffer containing the elements the need copying
	void CopyBufferToLinkedBuffer(const char buffer[]) {
		CopyBufferToLinkedBuffer(buffer, strlen(buffer));
	}

	// Returns the number of nodes traversed in the list
	inline int NumNodesTraversed() {
		return m_traversed_length; 
	}

	// Checks to see if their are more nodes not traversed in the list
	inline bool AskNextNode() {
		return Size() > m_traversed_length; 
	}

	// Checks to see if their are more nodes not traversed in the list
	inline bool AskNextNode(const SLinkedBuffPos &pos) {
		return (pos.buff_offset < m_chunk_size) || 
			(((SLink *)pos.buff_ptr)->forward_link != NULL); 
	}

	// Returns the next element after the current position provided.
	// @param pos - this is the postion that's been provided
	// @return the next element, NULL if there is no next element
	X *NextNode(SLinkedBuffPos pos) {

		if(pos.buff_offset >= m_chunk_size - 1) {
			pos.buff_ptr = ((SLink *)pos.buff_ptr)->forward_link;
			if(pos.buff_ptr == NULL) {
				return NULL;
			}
			pos.buff_offset = 0;
		}

		if(m_curr_node == pos.buff_ptr) {
			if(pos.buff_offset >= m_buffer_offset) {
				return NULL;
			}
		}

		return ((SLink *)pos.buff_ptr)->buffer + pos.buff_offset + 1;
	}

	// Returns the next element from the linked buffer in
	// the traversal path. Returns NULL if there are no 
	// more objects left to be retrieved.
	X *NextNode() {
		if(m_traversed_length >= Size()) {
			return NULL;
		}

		m_traversed_length++;
		if(m_path_offset >= m_chunk_size) {
			m_path_offset = 0; 
			m_path_node = m_path_node->forward_link; 
		}
		m_curr_element = &m_path_node->buffer[m_path_offset]; 
		return &m_path_node->buffer[m_path_offset++]; 
	}

	// This looks at the next element in the linked buffer
	// with out moving towards it
	X &PeekNextNode() {
		int path_offset = m_path_offset;
		SLink *path_node = m_path_node;
		if(path_offset >= m_chunk_size) {
			path_offset = 0; 
			if(!path_node->forward_link)throw 
				EOverflowException("Next Element Not Available"); 

			path_node = path_node->forward_link; 
		}

		return path_node->buffer[path_offset]; 
	}

	// Traverses so many places forward in the buffer
	// @param units - the number of elements to move past
	void MovePathForward(int units) {
		SLinkedBuffPos curr_pos = GetTraversalPosition();
		MovePathForward(curr_pos, units);
		SetTraversalPosition(curr_pos);
	}

	// Traverses so many places forward in the buffer
	// @param pos - the current position in the linked buffer
	// @param units - the number of elements to move past
	void MovePathForward(SLinkedBuffPos &pos, int units) {

		m_traversed_length += units;
		if(units <= (m_chunk_size - pos.buff_offset)) {
			pos.buff_offset += units;
			return;
		}

		SLink *path_node = (SLink *)pos.buff_ptr;
		units -= m_chunk_size - pos.buff_offset;

		if(path_node->forward_link == NULL) {
			throw EOverflowException("Next Element Not Available"); 
		}

		path_node = path_node->forward_link;

		while(units > m_chunk_size) {
			if(path_node->forward_link == NULL) {
				throw EOverflowException("Next Element Not Available"); 
			}

			path_node = path_node->forward_link;
			units -= m_chunk_size;
		}

		pos.buff_offset = units;
		pos.buff_ptr = path_node;
	}

	// Sets the current path traversal position
	// @param pos - the linked buffer position
	void SetTraversalPosition(const SLinkedBuffPos &pos) {
		if(pos.buff_ptr == NULL)throw 
			EIllegalArgumentException("pos NULL ptr");

		m_path_offset = pos.buff_offset;
		m_path_node = (SLink *)pos.buff_ptr;
	}

	// Returns the current path traversal position
	inline SLinkedBuffPos GetTraversalPosition() {
		SLinkedBuffPos pos;
		pos.buff_offset = m_path_offset;
		pos.buff_ptr = m_path_node;
		return pos;
	}

	// Returns the position of the curr last node in the list
	inline SLinkedBuffPos GetEndOfListPosition() {
		SLinkedBuffPos pos;
		pos.buff_offset = m_path_offset;
		pos.buff_ptr = m_path_node;
		return pos;
	}

	// Returns the node value at a particular position
	inline X &NodeValue(const SLinkedBuffPos &pos) {
		SLinkedBuffPos temp = pos;
		if(temp.buff_offset >= m_chunk_size) {
			temp.buff_offset = 0;
			temp.buff_ptr = ((SLink *)temp.buff_ptr)->forward_link;
		}
		return *(((SLink *)temp.buff_ptr)->buffer + temp.buff_offset);
	}

	// Returns the current element form the linked buffer
	// in the traversal path
	inline X &CurrentNode() {
		return *m_curr_element; 
	}

	// Resets the linked buffer
	void Reset() {
		if(m_root_node != NULL) {
			DestroyLinkedBuffer(); 
			Initialize(m_chunk_size); 
		} else {
			Initialize(); 
		}
	}

	// just a test framework
	void TestLinkedBuffer() {
		CArray<int> check(9999); 

		for(int i=0; i<100; i++) {cout<<i<<endl;
			Restart();
			check.Resize(0);
			// ReadLinkedBufferFromFile("linked"); 
			for(int i=0; i<9999; i++) {
				int element = rand(); 
				check.PushBack(element); 
				PushBack(element); 
			}

			ResetPath(); 
			for(int i=0; i<9999; i++) {
				if(check[i] != *NextNode()) {
					cout << "Error2 "<< i; 
					getchar(); 
				}
			}

			Restart();
			int offset = 0;
			while(offset < 9999) {
				int size = (rand() % 400);
				size = min(size, 9999 - offset);
				CopyBufferToLinkedBuffer(check.Buffer() + offset, size);
				offset += size;
			}


			ResetPath(); 
			for(int i=0; i<9999; i++) {
				if(check[i] != *NextNode()) {
					cout << "Error2 "<< i; 
					getchar(); 
				}
			}
		}

		// WriteLinkedBufferToFile("linked"); 
	}

	// Writes a linked buffer to file
	void WriteLinkedBufferToFile(const char str[]) {

		CHDFSFile file; 
		file.OpenWriteFile(CUtility::
			ExtendString(str, ".linked_buffer")); 

		WriteLinkedBufferToFile(file); 
	}

	// Reads a linked buffer from file
	void ReadLinkedBufferFromFile(const char str[]) {

		CHDFSFile file; 
		file.OpenReadFile(CUtility::
			ExtendString(str, ".linked_buffer")); 

		ReadLinkedBufferFromFile(file); 
	}

	// Writes a linked buffer to file
	void WriteLinkedBufferToFile(CHDFSFile &file) {

		file.WriteCompObject(m_size); 
		file.WriteCompObject(m_chunk_size); 

		SLink *path_temp = m_path_node; 
		ResetPath(); 

		int size = m_size; 
		while(size > m_chunk_size) {
			size -= m_chunk_size; 
			file.WriteCompObject(m_path_node->buffer, m_chunk_size); 
			m_path_node = m_path_node->forward_link; 
		}
		file.WriteCompObject(m_path_node->buffer, size); 
		m_path_node = path_temp; 
	}

	// Reads a linked buffer from file
	void ReadLinkedBufferFromFile(CHDFSFile &file) {

		int size;
		int chunk_size;
		file.ReadCompObject(size); 
		file.ReadCompObject(chunk_size); 

		Initialize(chunk_size); 
		m_curr_node = m_root_node; 

		m_size = size;
		while(size > m_chunk_size) {
			size -= m_chunk_size; 
			file.ReadCompObject(m_curr_node->buffer, m_chunk_size); 
			m_curr_node->forward_link = new SLink; 
			m_curr_node = m_curr_node->forward_link; 
			m_curr_node->buffer = new X [m_chunk_size]; 
		}

		m_curr_node->forward_link = NULL; 
		file.ReadCompObject(m_curr_node->buffer, size); 
		m_buffer_offset = size; 
	}

	// This frees memory associated with the linked buffer
	void FreeMemory() {
		if(m_root_node == NULL) {
			return;
		}

		DestroyLinkedBuffer(); 

		if(m_root_node != NULL) {
			// only delete the root node in the destructor
			delete []m_root_node->buffer; 
			delete m_root_node; 
		}

		m_root_node = NULL;
		m_size = 0;
	}

	~CLinkedBuffer() {
		FreeMemory();
	}
}; 

// This is a standard binary tree structure that can be used for
// searching given that a comparison function is supplied. Note
// the tree has been designed to hold multiple elements of equal
// value and simply store them in a linked list down the left branch.
template <class X> class CBinaryTree {

protected:

	// declares a binary node
	struct SNode {
		// stores the node value
		X node; 
		// stores ptrs to left and right children
		SNode *left_child, *right_child; 
	}; 

	// stores a Buffer to the root and current node
	SNode *m_root_node, *m_curr_node; 
	// stores a Buffer to the compare function
	int (*m_compare)(const X &arg1, const X &arg2); 
	// creates a buffer to store the nodex
	CLinkedBuffer<SNode> m_node_buffer; 
	// stores the list of free nodes
	SNode *m_free_node; 
	// stores the number of nodes in the tree
	int m_node_num; 
	// stores whether an element was found or not
	bool m_found; 

	// creates a new node
	SNode *CreateNewNode() {
		if(m_free_node != NULL) {
			SNode *prev_node = m_free_node; 
			m_free_node = m_free_node->left_child; 
			return prev_node; 
		}

		return m_node_buffer.ExtendSize(1); 
	}

	// deletes a node and add to the free list
	// @param node - the node being deleted
	void DeleteBinaryNode(SNode *node) {
		SNode *prev_node = m_free_node; 
		m_free_node = node; 
		m_free_node->left_child = prev_node; 
	}

	// returns the node with the next - highest value after delnode
	// goes to right child, then right child's left descendant
	// @param del_node - a ptr to the node being deleted
	SNode *GetSuccessor(SNode *del_node) {
		SNode *successor_parent = del_node; 
		SNode *successor = del_node; 
		SNode *current = del_node->right_child; 

		// go to right child until no more 
		// left children
		while(current) {
			successor_parent = successor; 
			successor = current; 
			current = current->left_child; 

		}

		// if successor not right child, make connections
		if(successor != del_node->right_child) {
			successor_parent->left_child = successor->right_child; 
			successor->right_child = del_node->right_child; 
		}
		return successor; 
	}

	// inserts a node to the right of the current position
	// taking into account an existing right node
	// @param node - the node being inserted right of the current node
	void InsertRightNode(const X &node) {
		SNode *right_tree = m_curr_node->right_child; 
		m_curr_node->right_child = CreateNewNode(); 
		m_curr_node = m_curr_node->right_child; 
		m_curr_node->right_child = right_tree; 
		m_curr_node->left_child = NULL; 
		m_curr_node->node = node; 
	}

	// inserts a node to the left of the current position
	// taking into account an existing left node
	// @param node - the node being inserted left of the current node
	void InsertLeftNode(const X &node) {
		SNode *left_tree = m_curr_node->left_child; 
		m_curr_node->left_child = CreateNewNode(); 
		m_curr_node = m_curr_node->left_child; 
		m_curr_node->left_child = left_tree; 
		m_curr_node->right_child = NULL; 
		m_curr_node->node = node; 
	}

	// adds a node to the tree when the position
	// in the tree is already been assigned
	// assumes current node has already been assigned
	// @param node - the node that is being added to the tree
	void AddInternalNode(const X &node) {
		m_found = false; 

		if(m_compare(node, m_curr_node->node) < 0) {
			if(m_curr_node->right_child) {
				InsertRightNode(node); 
				return; 
			}
			// creates a new node to store the value
			m_curr_node->right_child=CreateNewNode();
			m_curr_node=m_curr_node->right_child;

		} else if(m_compare(node, m_curr_node->node) > 0) {
			if(m_curr_node->left_child) {
				InsertLeftNode(node); 
				return; 
			}
			// creates a new node to store the value
			m_curr_node->left_child=CreateNewNode();
			m_curr_node=m_curr_node->left_child;

		} else {
			m_found = true; 
			InsertLeftNode(node); 
			return; 
		}
		m_curr_node->left_child = NULL; 
		m_curr_node->right_child = NULL; 
		m_curr_node->node = node; 
	}

	// This is used for testing
	static int TestCompareNodes(const int &arg1, const int &arg2) {

		if(arg1 < arg2) {
			return -1;
		}

		if(arg1 > arg2) {
			return 1;
		}

		return 0;
	}

public:

	CBinaryTree() {
	}

	CBinaryTree(int (*compare)(const X &arg1, const X &arg2), 
		int init_size = 2048) {
		Initialize(compare, init_size); 
	}

	// starts the binary tree
	// @param compare - the comparison function
	// @param init_size - the initial size of the tree buffer
	void Initialize(int (*compare)(const X &arg1, const X &arg2), 
		int init_size = 2048) {

		m_compare = compare; 
		m_node_buffer.Initialize(init_size); 
		m_free_node = NULL; 

		m_root_node = NULL; 
		m_node_num = 0; 
	}

	// sets the compare function used
	// @param compare - the comparison function
	void SetCompareFunction(int (*compare)
		(const X &arg1, const X &arg2)) {
		m_compare = compare; 
	}

	// copies the node buffer - assumes no nodes
	// have been deleted
	// @param buffer - a place to put all the nodes in
	// - the buffer
	void NodeBuffer(X buffer[]) {
		m_node_buffer.ResetPath(); 
		for(int i=0; i<m_node_num; i++) {
			buffer[i] = m_node_buffer.NextNode().node; 
		}
	}

	// prints out the tree
	void ShowTree() {
		if(m_root_node ==	NULL)return; 
		SNode *m_curr_node = m_root_node; 
		CArrayList<SNode *> parent(100); 
		CArrayList<CArrayList<SNode *> > tree(100); 
		CArrayList<CArrayList<SNode *> > up_node(100); 

		tree.Resize(100); 
		up_node.Resize(100); 
		for(int i=0; i<100; i++) {
			tree[i].Initialize(100); 
			up_node[i].Initialize(100); 
		}

		// now writes the binary tree structure
		CArrayList<SNode *> prev_node(100); 
		CArrayList<char> direction(100); 
		direction.PushBack(0); 

		int depth = 0; 
		m_curr_node = m_root_node; 
		SNode *before_node = m_root_node; 

		while(true) {
			if(direction.LastElement() <= 0) {
				direction.LastElement() = 1; 
				tree[depth].PushBack(m_curr_node); 
				up_node[depth].PushBack(before_node); 

				depth++; 
			}
			if(m_curr_node->left_child && (direction.LastElement() < 2)) {
				parent.PushBack(m_curr_node); 
				direction.LastElement() = 2; 
				direction.PushBack(0); 
				before_node = m_curr_node; 
				m_curr_node = m_curr_node->left_child; 
			} else if(m_curr_node->right_child && (direction.LastElement() < 3)) {
				parent.PushBack(m_curr_node); 
				direction.LastElement() = 3; 
				direction.PushBack(0); 
				before_node = m_curr_node; 
				m_curr_node = m_curr_node->right_child; 
			} else {
				depth--; 
				if(!parent.Size())break; 	// jumps to the next division
				m_curr_node = parent.PopBack(); 
				before_node = m_curr_node; 
				direction.PopBack(); 
			}
		}

		for(int i=0; i<tree.Size(); i++) {
			for(int c=0; c<tree[i].Size(); c++) {

				cout << c << ": "<< tree[i][c]->node.doc_id<<" "<<tree[i][c]->node.byte_offset << " - "; 

				if(!tree[i][c]->left_child && !tree[i][c]->right_child)
					cout << "NULLnode - "; 

				if(up_node[i][c] && i) {
					for(int d = 0; d < tree[i - 1].Size(); d++) {
						if(up_node[i][c] == tree[i - 1][d]) {
							cout << d << " "; 
							break; 
						}
					}
				}
			}

			if(tree[i].Size())cout << endl; 
			cout << " "; 
		}
	}

	// returns a reference to a particular node
	// @param position - the node value corresponding
	// - to a particular position in the tree
	inline X &NodeValue(void *position) {
		if(position == NULL) {
			throw EIllegalArgumentException
				("postion = NULL"); 
		}

		m_curr_node = (SNode *)position; 
		return m_curr_node->node; 
	}

	// adds a node to the tree at the current node
	// assumes position is a leaf node - 
	// @param position - the node value corresponding
	// - to a particular position in the tree
	// @param node - the node that is being added to the tree
	// @return a pointer to the added node
	X *AddNode(void *position, const X &node) {

		if(position == NULL) throw EIllegalArgumentException
			("Postition Argument is Null"); 

		m_curr_node = (SNode *)position; 
		AddInternalNode(node); 
		m_node_num++; 
		
		return &m_curr_node->node; 
	}

	// adds a node to the binary tree - 
	// @param node - the node that is being added to the tree
	// @return a pointer to the added node
	X *AddNode(const X &node) {

		m_node_num++; 
		if(m_root_node == NULL) {
			m_root_node = CreateNewNode(); 
			m_root_node->left_child = NULL; 
			m_root_node->right_child = NULL; 
			m_root_node->node = node; 
			return false; 
		}

		m_curr_node = m_root_node; 

		while(true) {
			int compare = m_compare(node, m_curr_node->node); 
			if(compare < 0) {
				if(!m_curr_node->right_child) 
					return AddNode(m_curr_node, node);

				m_curr_node = m_curr_node->right_child; 
			} else if(compare > 0) {
				if(!m_curr_node->left_child) 
					return AddNode(m_curr_node, node);

				m_curr_node = m_curr_node->left_child; 
			} else {
				return AddNode(m_curr_node, node);
			}
		}

		return NULL;
	}

	// retrieves a particular node corresponding to the next
	// in the tree - requires that no nodes have been deleted
	// from the tree
	inline X &NextNode() {
		return m_node_buffer.NextNode().node; 
	}

	// resets the next node to the start
	void ResetNextNode() {
		m_node_buffer.ResetPath(); 
	}

	// sorts the nodes using tree sort and copies them to a buffer
	// @param sorted_buffer - a buffer to store the sorted nodes
	void TreeSort(CMemoryChunk<X> &sorted_buffer) {

		if(m_root_node == NULL)return; 
		sorted_buffer.AllocateMemory(Size()); 

		m_curr_node = m_root_node; 
		CArrayList<SNode *> prev_node(100); 
		CArrayList<char> direction(100); 
		direction.PushBack(0); 
		int offset = 0; 
		
		while(true) {
			if(m_curr_node->right_child && (direction.LastElement() < 2)) {
				prev_node.PushBack(m_curr_node); 
				direction.LastElement() = 2; 
				direction.PushBack(0); 
				m_curr_node = m_curr_node->right_child; 
			} else if(m_curr_node->left_child && (direction.LastElement() < 3)) {
				sorted_buffer[offset++] = m_curr_node->node; 
				prev_node.PushBack(m_curr_node); 
				direction.LastElement() = 3; 
				direction.PushBack(0); 
				m_curr_node = m_curr_node->left_child; 
			} else {
				// records a null node
				if(!m_curr_node->left_child) {
					sorted_buffer[offset++] = m_curr_node->node; 
				} 

				if(prev_node.Size() <= 0)break; 	// jumps to the next division
				m_curr_node = prev_node.PopBack(); 
				direction.PopBack(); 
			}
		}

		cout<<offset<<" "<<Size();getchar();
	}

	// performs some operation on each node in the tree
	// @requries no nodes have been removed from the tree
	// @param operation - a ptr to a function that performs
	// - an operation on each node in the tree
	void PerformOperationOnNodes(void (*operation)(X &arg1)) {
		m_node_buffer.ResetPath(); 
		for(int i=0; i<m_node_buffer.Size(); i++) {
			SNode element = m_node_buffer.NextNode(); 
			operation(element.node); 
		}
	}

	// just a test framework
	void TestBinaryTree() {
	// 	ReadBinaryTreeFromFile("bin"); 

		Initialize(TestCompareNodes);
		CArrayList<int> element(100000); 
		element.Initialize(); 

		for(int i=0; i<1000; i++) {
			int insert = rand() % 100000; 
			cout << endl << "Insert Element"<< insert << endl; 
			element.PushBack(insert); 
			AddNode(insert); 
			if(!AskContainNode(insert)) {
				cout << "Cannot Find"<< endl; 
				getchar(); 
			}
		}

		for(int i=0; i<1000; i++) {
			if(!AskContainNode(element[i])) {
				cout << "Cannot Find "<< element[i] << endl; 
				getchar(); 
			}
		}

		CMemoryChunk<X> buffer; 
		TreeSort(buffer); 
		X prev_element = buffer[0]; 
		for(int i=0; i<Size(); i++) {
			cout << buffer[i] << " * "; 
			if(m_compare(prev_element, buffer[i]) > 0) {
				cout << "Sort Error"; 
				getchar(); 
			}
			prev_element = buffer[i]; 
		}

		// WriteBinaryTreeToFile("bin"); 

		for(int i=0; i<1000; i++) {
			cout << "Delete Element "<< element[i] << endl; 
			if(!DeleteNode(element[i])) {
			 cout << " Cannot Find error "<< element[i]; 
				getchar(); 
			}
		}
	}

	// returns the size of the binary tree
	inline int Size() {
		return m_node_num; 
	}

	// checks to see if the specified node was found previously
	inline bool AskFoundNode() {
		return m_found; 
	}
	
	// finds a particular node in the tree
	// @param node - the node that is being searched for
	// @return the position in the tree where the node exists
	//         or will be placed when it's added, returns NULL
	//         if the node can not be found
	void *FindNode(const X &node) {
		m_found = false; 
		if(m_root_node == NULL)return NULL; 
		m_curr_node = m_root_node; 

		while(true) {
			int compare = m_compare(node, m_curr_node->node); 
			if(compare < 0) {
				if(!m_curr_node->right_child) 
					return m_curr_node; 

				m_curr_node = m_curr_node->right_child; 
			} else if(compare > 0) {
				if(!m_curr_node->left_child) 
					return m_curr_node; 

				m_curr_node = m_curr_node->left_child; 
			} else {
				m_found = true; 
				return m_curr_node; 
			}
		}

		return NULL; 
	}

	// finds a particular node in the tree and records the
	// position of the leftmost and rightmost child
	// @param left_child - stores a ptr to the leftmost child found
	// @param right_child - stroes a ptr to the rightmost child found
	// @param node - the node that is being searched for
	// @return the position in the tree where the node exists
	//         or will be placed when it's added, returns NULL
	//         if the node can not be found
	void *FindNode(const X &node, void * &left_child, void * &right_child) {
		m_found = false; 
		if(m_root_node == NULL) {
			left_child = NULL;
			right_child = NULL;
			return NULL; 
		}

		m_curr_node = m_root_node; 
		left_child = m_curr_node->left_child;
		right_child = m_curr_node->right_child;

		while(true) {
			int compare = m_compare(node, m_curr_node->node); 
			if(compare < 0) {
				if(!m_curr_node->right_child) 
					return m_curr_node; 

				if(m_curr_node->left_child) {
					left_child = m_curr_node->left_child;
				}
				m_curr_node = m_curr_node->right_child; 
				right_child = m_curr_node;
			} else if(compare > 0) {
				if(!m_curr_node->left_child) 
					return m_curr_node; 

				if(m_curr_node->right_child) {
					right_child = m_curr_node->right_child;
				}
				m_curr_node = m_curr_node->left_child; 
				left_child = m_curr_node;
			} else {
				m_found = true; 
				return m_curr_node; 
			}
		}
		return NULL; 
	}

	// checks if a particular node exists in the tree
	// @param node - the node that is being searched for
	bool AskContainNode(const X &node) {
		FindNode(node); 
		return AskFoundNode(); 
	}

	// returns the number of equal nodes
	// @param node - the node that is being searched for
	int NumEqualNodes(const X &node) {
		if(m_root_node == NULL)return 0; 
		m_curr_node = m_root_node; 
		int equal_count = 0; 

		while(true) {
			int compare = m_compare(node, m_curr_node->node); 
			if(compare < 0) {
				if(!m_curr_node->right_child) {
					return 0; 
				}
				m_curr_node = m_curr_node->right_child; 
			} else if(compare > 0) {
				if(!m_curr_node->left_child) {
					return 0; 
				}
				m_curr_node = m_curr_node->left_child; 
			} else {
				equal_count++; 
				while(m_curr_node->left_child) {
					m_curr_node = m_curr_node->left_child; 
					// increments the number of equal nodes that have 
					// been found down the similar path
					if(!m_compare(node, m_curr_node->node))
						equal_count++; 
					else 
						return equal_count; 
				}
				return equal_count; 
			}
		}

		return 0; 
	}

	// returns an equal child if one exists
	// the nodes will need to be checked
	// for equality externally
	// @param node - a ptr to the position in the tree
	// - where the node exists
	// @return a node of equal value
	inline void *SameLink(void *node) {
		return (void *)(((SNode *)node)->left_child); 
	}

	// returns the left child
	// @param node - a ptr to the position in the tree
	// - where the node exists
	// @return the left child
	inline void *LeftChild(void *node) {
		return ((SNode *)node)->left_child; 
	}

	// returns the right child
	// @param node - a ptr to the position in the tree
	// - where the node exists
	// @return the right child
	inline void *RightChild(void *node) {
		return ((SNode *)node)->right_child; 
	}

	// returns the root node
	inline void *RootNode() {
		return m_root_node; 
	}

	// deletes a node from the tree
	// @param node - the node that is being deleted
	bool DeleteNode(const X &node) {
		if(m_root_node == NULL)return false; 
		m_curr_node = m_root_node; 
		SNode *parent_node = NULL; 
		bool prev_dir = false; 

		while(true) {
			if(m_compare(node, m_curr_node->node) < 0) {
				if(!m_curr_node->right_child) {
					return false; 
				}
				parent_node = m_curr_node; 
				prev_dir = false; 
				m_curr_node = m_curr_node->right_child; 
			} else if(m_compare(node, m_curr_node->node) > 0) {
				if(!m_curr_node->left_child) {
					return false; 
				}
				parent_node = m_curr_node; 
				prev_dir = true; 
				m_curr_node = m_curr_node->left_child; 
			} else {
				m_node_num--; 
				if(!m_curr_node->left_child && !m_curr_node->right_child) {
					if(!parent_node) {
						// delete the root node
						m_root_node = NULL; 
						DeleteBinaryNode(m_curr_node); 
						return true; 
					}

					if(prev_dir) {
						parent_node->left_child = NULL; 
						DeleteBinaryNode(m_curr_node); 
						return true; 
					}
					parent_node->right_child = NULL; 
				} else if(!m_curr_node->right_child) {
					if(!parent_node)
						m_root_node = m_curr_node->left_child; 
					else if(prev_dir)
						parent_node->left_child = m_curr_node->left_child; 
					else 
						parent_node->right_child = m_curr_node->left_child; 
				} else if(!m_curr_node->left_child) {
					if(!parent_node)
						m_root_node = m_curr_node->right_child; 
					else if(prev_dir)
						parent_node->left_child = m_curr_node->right_child; 
					else 
						parent_node->right_child = m_curr_node->right_child; 
				} else {	// two children, replace with inorder successor
					// get successor of node to delete (current)
					SNode *successor = GetSuccessor(m_curr_node); 

					// connect parent of current to successor instead
					if(m_curr_node == m_root_node)
						m_root_node = successor; 
					else if(prev_dir)
						parent_node->left_child = successor; 
					else
						parent_node->right_child = successor; 
					// connect successor to current's left child
					successor->left_child = m_curr_node->left_child; 
				}

				DeleteBinaryNode(m_curr_node); 
				return true; 
			}
		}
		return true; 
	}

	// resets the binary tree
	void Reset() {
		m_node_buffer.Reset(); 
		m_node_num = 0; 
		m_root_node = NULL; 
		m_free_node = NULL; 
	}

	// reads the binary tree from file
	void ReadBinaryTreeFromFile(const char str[]) {
		CHDFSFile file; 
		file.OpenReadFile(CUtility::ExtendString(str, ".binary_tree")); 

		ReadBinaryTreeFromFile(file); 
	}

	// writes the binary tree to file
	void WriteBinaryTreeToFile(const char str[]) {
		if(Size() <= 0)return; 

		CHDFSFile file; 
		file.OpenWriteFile(CUtility::ExtendString(str, ".binary_tree")); 

		WriteBinaryTreeToFile(file); 
	}

	// reads the binary tree from file
	void ReadBinaryTreeFromFile(CHDFSFile &file) {

		Reset(); 
		uChar back_track = 0; 
		file.ReadCompObject(m_node_num); 
		if(!m_node_num)return; 

		m_root_node = CreateNewNode(); 
		m_root_node->left_child = NULL; 
		m_root_node->right_child = NULL; 
		m_curr_node = m_root_node; 

		file.ReadCompObject(back_track); 	
		file.ReadCompObject(m_curr_node->node); 

		// reads the binary tree structure
		CArrayList<SNode *> prev_node(100); 
		prev_node.PushBack(m_curr_node); 
		X curr_element; 

		for(int i=1; i<m_node_num; i++) {
			// reads each node in from memory
			file.ReadCompObject(back_track); 	
			file.ReadCompObject(curr_element); 
			if(back_track) {
				prev_node.Resize(prev_node.Size() - back_track); 
				m_curr_node = prev_node.LastElement(); 
			}

			if(m_compare(curr_element, m_curr_node->node) < 0) {
				// creates a new node to store the value
				m_curr_node->right_child = CreateNewNode(); 
				m_curr_node = m_curr_node->right_child; 
			} else {
				// creates a new node to store the value
				m_curr_node->left_child = CreateNewNode(); 
				m_curr_node = m_curr_node->left_child; 
			}
			prev_node.PushBack(m_curr_node); 
			m_curr_node->left_child = NULL; 
			m_curr_node->right_child = NULL; 
			m_curr_node->node = curr_element; 
		}
	}

	// writes the binary tree to file
	void WriteBinaryTreeToFile(CHDFSFile &file) {
		// now writes the binary tree structure
		CArrayList<SNode *> prev_node(100); 
		CArrayList<char> direction(100); 
		direction.PushBack(0); 

		uChar back_track = 0; 
		m_curr_node = m_root_node; 
		file.WriteCompObject(m_node_num); 
		
		while(true) {
			if(direction.LastElement() <= 0) {
				direction.LastElement() = 1; 
				file.WriteCompObject(back_track); 	// writes the current backtrack
				file.WriteCompObject(m_curr_node->node); 
				back_track = 0; 
			}
			if(m_curr_node->right_child && (direction.LastElement() < 2)) {
				prev_node.PushBack(m_curr_node); 
				direction.LastElement() = 2; 
				direction.PushBack(0); 
				m_curr_node = m_curr_node->right_child; 
			} else if(m_curr_node->left_child && (direction.LastElement() < 3)) {
				prev_node.PushBack(m_curr_node); 
				direction.LastElement() = 3; 
				direction.PushBack(0); 
				m_curr_node = m_curr_node->left_child; 
			} else {
				back_track++; 
				if(prev_node.Size() <= 0)break; 	// jumps to the next division
				m_curr_node = prev_node.PopBack(); 
				direction.PopBack(); 
			}
		}
	}

}; 

// This is used a standard dictionary to store character strings
// note that all strings are terminated internally with \0 so this
// character cannot be present in any character string given for
// successful operation. Internally several bucket divisions are
// used and each string is hashed accordingly the number of buckets
// can be adjusted - but will be fitted to the closest prime.
// Each bucket division is then implemented as a binary tree to
// allow even faster searching for a character string although
// this requires an extra memory overhead and is unnecessary
// for smaller dictionaries. The standared hash dictionary is 
// more suitable for smaller size dictionaries.
template <class X> class CFastHashDictionary {
	// a linked node used to store the character
	// in the hash dictionary - this is not attatched
	// to any division
	struct SLinkedList {
		char *buffer; 
		SLinkedList* link; 
	}; 

	// used to stroe the buffer offset
	// for which a given character string starts
	// as well as the checksum for the character string
	struct SNode {
		u_short word_buffer_offset; 
		u_short check_sum; 
		SLinkedList* word_buffer; 
	}; 

	// stores the index for a given character string
	// as well as ptr to the left and right branches
	// of the binary tree stored in each bucket division
	struct SBinaryLink {
		X index; 
		SBinaryLink *forward_link; 
		SBinaryLink *prev_link; 
	}; 

	// stores pointers to the start and end of the word buffer
	SLinkedList*m_word_root, *m_word_end; 
	// stores the current word buffer offset
	int m_word_offset; 
	// stores a Buffer to the current word buffer
	SLinkedList*m_word_node; 
	// stores a Buffer to the search tree for each of the
	// different divisions
	CMemoryChunk<SBinaryLink *> m_search_root; 
	// stores the back index to the word buffer
	CVector<SNode> m_back_link; 

	// stores a Buffer to the current tree node
	SBinaryLink *m_binary_node; 
	// creates a buffer to store the binary node
	CLinkedBuffer<SBinaryLink> m_node_buffer; 

	// stores the hash division size
	int m_breadth; 
	// stores the memory block size
	int m_memory_block_size; 
	// stores the current word index size
	int m_curr_index; 
	// stores if a word is found or not 
	bool m_found; 

	// checks if a new word block needs to be created
	// @param the current word block buffer offset
	// @return true if a new word block was created, false otherwise
	bool CheckNewWordBlock(u_short &offset) {
		if(offset >= m_memory_block_size) {
			m_word_node->link = new SLinkedList; 	// puts a new link on the list
			m_word_node = m_word_node->link; 	// jumps to the next segment
			// allocates more memory for the new linked block
			m_word_node->buffer = new char [m_memory_block_size]; 
			m_word_node->link = NULL; 	// nulls the next buffer
			offset = 0; 
			return true; 
		}
		return false; 
	}

	// writes a word to the dictionary
	// @param check_sum - the check sum of the word being written
	// @param word - a buffer containing the character string being written
	// @param length - the length of the character string
	// @param start - an offset in the character string
	// @return a unique index assigned to the character string
	X WriteWord(u_short check_sum, const char word[], 
		int length, int start = 0) {

		m_found = false; 
		m_word_node = m_word_end; 

		SNode node; 
		node.check_sum = check_sum; 
		node.word_buffer = m_word_node; 
		node.word_buffer_offset = (u_short)m_word_offset; 
		m_back_link.PushBack(node); 

		for(int i=start; i<length; i++) {
			// creates a new segment if required
			CheckNewWordBlock(node.word_buffer_offset); 
			m_word_node->buffer[node.word_buffer_offset++] = word[i]; 
		}

		// creates a new segment if required
		CheckNewWordBlock(node.word_buffer_offset); 
		m_word_node->buffer[node.word_buffer_offset++] = '\0'; 

		m_curr_index++; 
		m_word_offset = node.word_buffer_offset; 
		m_word_end = m_word_node; 
		return m_curr_index - 1; 
	}

	// frees the memory used to store the word buffer
	void DeleteFastHashDictionary() {
		if(!m_search_root.OverflowSize()) {
			return; 
		}

		SLinkedList* prev_word_node; 
		m_word_node = m_word_root; 
		while(m_word_node) {	// searches through the word division
			prev_word_node = m_word_node; 
			m_word_node = m_word_node->link; 
			delete prev_word_node->buffer; 	// deletes the word buffer
			delete prev_word_node; 	// deletes the dst node
		}
	}

	// writes a word to file
	// @param file - the file being written
	// @param index - the unique index of the word being written
	void WriteCompressionWordToFile(CHDFSFile &file, int index) {
		int offset = m_back_link[index].word_buffer_offset; 
		SLinkedList* curr_node = m_back_link[index].word_buffer; 
		char *dictionary_index = curr_node->buffer; 

		if(offset >= m_memory_block_size) {
			curr_node = curr_node->link; 
			dictionary_index = curr_node->buffer; 
			offset = 0; 
		}

		dictionary_index += offset; 
		while((*dictionary_index != '\0')) {
			file.WriteCompObject(*(dictionary_index++)); 
			if(++offset >= m_memory_block_size) {
				curr_node = curr_node->link; 
				dictionary_index = curr_node->buffer; 
				offset = 0; 
			}
		}
		file.WriteCompObject(*dictionary_index); 
	}

	// reads the next word from file
	// @param file - the file being read from
	void ReadCompressionWordFromFile(CHDFSFile &file) {
		char letter = 10; 
		while(letter != '\0') {
			// checks the block overflow
			if(m_word_offset >= m_memory_block_size) {
				m_word_node->link = new SLinkedList; 
				m_word_node = m_word_node->link; 
				// creates a new block
				m_word_node->buffer = new char [m_memory_block_size]; 
				m_word_offset = 0; 
			}

			file.ReadCompObject(letter); 
			m_word_node->buffer[m_word_offset++] = letter; 
		}
	}

	// checks to see if two words are equal
	// param index - the unique index of a string that already exists
	// - in the dictionary
	// @param word - a buffer containing the character string being written
	// @param length - the length of the character string
	// @param start - an offset in the character string
	// @return true if there was a match, false otherwise
	bool CheckMatch(int index, const char word[], int length) {
		// gets an index into the word buffer for the current node
		int offset = m_back_link[index].word_buffer_offset; 
		SLinkedList* curr_node = m_back_link[index].word_buffer; 
		char *dictionary_index = curr_node->buffer; 

		if(offset >= m_memory_block_size) {
			curr_node = curr_node->link; 
			dictionary_index = curr_node->buffer; 
			offset = 0; 
		}

		dictionary_index += offset; 
		int word_size = 0; 
		while((*dictionary_index != '\0') && (word_size < length)) {
			if(*(dictionary_index++) != word[word_size++]) {
				// no match found
				return false; 
			}
			if(++offset >= m_memory_block_size) {
				curr_node = curr_node->link; 
				dictionary_index = curr_node->buffer; 
				offset = 0; 
			}
		}

		return (length == word_size); 
	}

	// checks if a node with a particular checksum 
	// exists in the tree
	// @param check_sum - the check sum of the character string -
	//  - used in find character string
	bool AskContainNode(u_short check_sum) {
		if(!m_binary_node)return false; 

		while(true) {
			u_short stored_check_sum = m_back_link
				[m_binary_node->index].check_sum; 

			if(check_sum > stored_check_sum) {
				if(!m_binary_node->forward_link) {
					return false; 
				}
				m_binary_node = m_binary_node->forward_link; 
			} else if(check_sum < stored_check_sum) {
				if(!m_binary_node->prev_link) {
					return false; 
				}
				m_binary_node = m_binary_node->prev_link; 
			} else {
				return true; 
			}
		}
		return false; 
	}

	// search a local division binary tree for an equal checksum
	// @param check_sum - the check sum of the word being added
	// @param division - the bucket division in the dictionary
	// @return true if the check sum matched an existing checksum false otherwise
	bool SearchTree(u_short check_sum, int division) {
		// checks if there is no LocalData stored in the list
		if(!m_binary_node) {
			// intilizes the root tree node
			m_node_buffer.ExtendSize(1); 
			m_search_root[division] = &m_node_buffer.LastElement(); 
			m_binary_node = m_search_root[division]; 
			return false; 
		}

		while(true) {
			u_short stored_check_sum = m_back_link
				[m_binary_node->index].check_sum; 

			if(check_sum > stored_check_sum) {
				if(m_binary_node->forward_link == NULL) {
					// no match found
					m_node_buffer.ExtendSize(1); 
					m_binary_node->forward_link = &m_node_buffer.LastElement(); 
					m_binary_node = m_binary_node->forward_link; 
					return false; 
				}
				m_binary_node = m_binary_node->forward_link; 
			} else if(check_sum < stored_check_sum) {
				if(m_binary_node->prev_link == NULL) {
					// no match found
					m_node_buffer.ExtendSize(1); 
					m_binary_node->prev_link = &m_node_buffer.LastElement(); 
					m_binary_node = m_binary_node->prev_link; 
					return false; 
				}
				m_binary_node = m_binary_node->prev_link; 
			} else {
				return true; 
			}
		}
		return false; 
	}

	// reads the node index and word from file
	void ReadTreeNodeFromFile(CHDFSFile &tree_file) {
		// reads the node index
		tree_file.ReadCompObject(m_binary_node->index); 
		// reads the node's word from file
		ReadCompressionWordFromFile(tree_file); 

		m_binary_node->forward_link = NULL; 
		m_binary_node->prev_link = NULL; 
	}

public:

	CFastHashDictionary() {
	}

	// Initializes the dictionary
	// @param breadth - the number of hash divisions to use
	// @param linked_chunk_size - the number of bytes to allocate
	//                          - in advance to store a word
	CFastHashDictionary(int breadth, 
		int linked_chunk_size = 0xFFFF) {

		Initialize(breadth, linked_chunk_size); 
	}

	// Initializes the dictionary
	// @param breadth - the number of hash divisions to use
	// @param linked_chunk_size - the number of bytes to allocate
	//                          - in advance to store a word
	void Initialize(int breadth, 
		int linked_chunk_size = 0xFFFF) {

		m_breadth = CHashFunction::FindClosestPrime(breadth); 

		if(linked_chunk_size > 0xFFFF) {
			linked_chunk_size = 0xFFFF; 
		}

		m_curr_index = 0; 
		m_memory_block_size = linked_chunk_size; 
		m_search_root.AllocateMemory(m_breadth, NULL); 
		m_node_buffer.Initialize(); 
		m_word_offset = 0; 

		// initilizes the word link
		m_word_root = new SLinkedList; 
		m_word_end = m_word_root; 
		// initilizes the word buffer
		m_word_end->buffer = new char [m_memory_block_size]; 
		// initilizes the index buffer
		m_word_end->link = NULL; 
	}

	// checks if a word was found
	inline bool AskFoundWord() {
		return m_found; 
	}

	// adds a word to the dictionary
	// @param word - a buffer containing the character string being written
	// @param length - the length of the character string
	// @param start - an offset in the character string
	int AddWord(const char word[], int length, int start = 0) {

		m_found = true; 
		// finds the local tree division
		int division = CHashFunction::
			CheckSumBaseMultiply(5, word, length, start) % m_breadth; 
		// finds the checksum for the word - different to division checksum
		u_short check_sum = (u_short)CHashFunction::
			CheckSumBaseMultiply(7, word, length, start); 

		m_binary_node = m_search_root[division]; 

		if(!SearchTree(check_sum, division)) {
			// no equal checksum found
			m_binary_node->forward_link = NULL; 
			m_binary_node->prev_link = NULL; 
			m_binary_node->index = m_curr_index; 
			// no match found - just write word to dictionary
			return WriteWord(check_sum, word, length, start); 
		}

		// no match found follow the left 
		// link to check for another equal checksum
		SBinaryLink *parent = m_binary_node; 
		while(true) {
			if(CheckMatch(m_binary_node->index, word + start, length - start)) {
				// match found return the index
				return m_binary_node->index; 
			}
			// no match found
			if(!m_binary_node->prev_link) {
				m_node_buffer.ExtendSize(1); 
				m_binary_node->prev_link = &m_node_buffer.LastElement(); 
				m_binary_node = m_binary_node->prev_link; 
				break; 
			}

			parent = m_binary_node; 
			m_binary_node = m_binary_node->prev_link; 
			// no match found
			if(check_sum != m_back_link[m_binary_node->index].check_sum) {
				// have to attatch the new node to the parent node
				m_node_buffer.ExtendSize(1); 
				parent->prev_link = &m_node_buffer.LastElement(); 
				parent = parent->prev_link; 
				parent->prev_link = m_binary_node; 
				parent->forward_link = NULL; 
				parent->index = m_curr_index; 

				// no match found - just write word to dictionary
				return WriteWord(check_sum, word, length, start); 
			}
		}

		m_binary_node->forward_link = NULL; 
		m_binary_node->prev_link = NULL; 
		m_binary_node->index = m_curr_index; 

		// no match found - just write word to dictionary
		return WriteWord(check_sum, word, length, start); 
	}

	// this is just a testing framework
	void TestFastHashDictionary() {
		CArrayList<char> word; 
		word.Initialize(); 
		CArrayList<int> word_start; 
		word_start.Initialize(); 
		word_start.PushBack(0); 

		// ReadFastHashDictionaryFromFile("check"); 

		for(int i=0; i<200000; i++) {
			if((rand() % 2)) {
				int length = (rand() % 20) + 5; 
				for(int c=0; c<length; c++) {
					char a = (rand() % 62) + '0'; 
					word.PushBack(a); 
				}

				word_start.PushBack(word.Size()); 
				AddWord(word.Buffer(), word.Size(), word.Size() - length); 
				if(AskFoundWord()) {
					cout << "error1"<< endl; 
					for(int i=word.Size() - length; i<word.Size(); i++)
						cout << word[i]; getchar(); 
				}

			} else if((word_start.Size() - 1)) {
				int index = (rand() % (word_start.Size() - 1)); 
				int out = FindWord(word.Buffer(), word_start[index+1], word_start[index]); 
				if(!AskFoundWord()) {
					cout << "error2"<< endl; 
					cout << word_start[index+1] <<" "<< word_start[index]; 
					getchar(); 
					for(int i=word_start[index]; i<word_start[index+1]; i++)
						cout << word[i]; 
					getchar(); 
				}
				if(out != index) {
					cout << "error3"; 
					getchar(); 
				}

				int length; 
				char *check = GetWord(index, length); 
				if(length != (word_start[index+1] - word_start[index])) {
					cout << "error4 "<< length <<" "<< word_start[index+1] - word_start[index]; 
					getchar(); 
				}
				for(int i=word_start[index], h = 0; i<word_start[index+1]; i++, h++) {
					if(check[h] != word[i]) {
						cout << "error5"; 
						getchar(); 
					}
				}
			}
		}
		// WriteFastHashDictionaryToFile("check"); 
	}

	// checks if a word exists in the dictionary
	// @param word - a buffer containing the character string being written
	// @param length - the length of the character string
	// @param start - an offset in the character string
	int FindWord(const char word[], int length, int start = 0) {
		m_found = false; 
		// finds the local tree division
		int division = CHashFunction::
			CheckSumBaseMultiply(5, word, length, start) % m_breadth; 
		// finds the checksum for the word - different to division checksum
		u_short check_sum = (u_short)CHashFunction::
			CheckSumBaseMultiply(7, word, length, start); 

		m_binary_node = m_search_root[division]; 

		if(!AskContainNode(check_sum)) {
			return -1; 
		}

		// no match found follow the left 
		// link to check for another equal checksum
		while(true) {
			if(CheckMatch(m_binary_node->index, word + start, length - start)) {
				m_found = true; 
				// match found return the index
				return m_binary_node->index; 
			}
			// no match found
			if(!m_binary_node->prev_link) {
				return -1; 
			}

			m_binary_node = m_binary_node->prev_link; 
			// no match found
			if(check_sum != m_back_link[m_binary_node->index].check_sum) {
				return -1; 
			}
		}

		return -1; 
	}


	// returns the number of hash divisions
	inline int GetHashSize() {
		return m_breadth; 
	}

	// returns a word for some index
	// @param index - the unique word index
	char *GetWord(X index) {
		int length; 
		GetWord(index, length); 
		CUtility::TempBuffer()[length] = '\0'; 

		return CUtility::TempBuffer(); 
	}

	// returns a word for some index
	// @param index - the unique word index
	// @param length - stores the length of the word
	char *GetWord(X index, int &length) {
		if(index < 0 || index >= Size())throw
			EIndexOutOfBoundsException("index out of bounds");

		char *word_output = CUtility::TempBuffer(); 
		int offset = m_back_link[index].word_buffer_offset; 
		SLinkedList* curr_node = m_back_link[index].word_buffer; 
		char *dictionary_index = curr_node->buffer; 
		length = 0; 

		if(offset >= m_memory_block_size) {
			curr_node = curr_node->link; 
			dictionary_index = curr_node->buffer; 
			offset = 0; 
		}

		dictionary_index += offset; 
		while((*dictionary_index != '\0')) {
			 * (word_output++) = *(dictionary_index++); 
			length++; 
			if(++offset >= m_memory_block_size) {
				curr_node = curr_node->link; 
				dictionary_index = curr_node->buffer; 
				offset = 0; 
			}
		}

		return CUtility::TempBuffer(); 
	}

	// removes the dictionary from file
	void RemoveFastHashDictionary(const char str[]) {
		CHDFSFile::Remove(CUtility::ExtendString(str, ".hash_dictionary")); 
	}

	// resets the dictionary
	void Reset() {
		DeleteFastHashDictionary(); 
		m_back_link.Resize(0); 
		m_node_buffer.Reset(); 
		Initialize(m_breadth, m_memory_block_size); 
	}

	// returns the size of the dictionary
	inline int Size() {
		return m_curr_index; 
	}

	// reads a dictionary size from file
	int ReadFastHashDictionarySizeFromFile(const char str[]) {
		CHDFSFile tree_file; 
		tree_file.OpenReadFile(CUtility::
			ExtendString(str, ".fast_hash_dictionary")); 

		DeleteFastHashDictionary(); 

		return ReadFastHashDictionarySizeFromFile(tree_file); 
	}

	// reads a dictionary size from file
	static int ReadFastHashDictionarySizeFromFile(CHDFSFile &file) {
		int temp1; 
		int temp2; 
		X size; 

		// reads the breadth
		file.ReadCompObject(temp1); 	
		// reads the memory block size
		file.ReadCompObject(temp2); 
		// reads the number of words stored
		file.ReadCompObject(size); 
		return (int)size; 
	}

	// reads a dictionary from file
	void ReadFastHashDictionaryFromFile(const char str[]) {

		CHDFSFile tree_file; 
		tree_file.OpenReadFile(CUtility::
			ExtendString(str, ".fast_hash_dictionary")); 

		DeleteFastHashDictionary(); 

		ReadFastHashDictionaryFromFile(tree_file); 
	}

	// reads a dictionary from file
	void ReadFastHashDictionaryFromFile(CHDFSFile &file) {

		file.ReadCompObject(m_breadth); 	// reads the breadth
		file.ReadCompObject(m_memory_block_size); // reads the memory block size

		// initilizes the pointers
		Initialize(m_breadth, m_memory_block_size); 
		// reads the curr_index must be done after initilization
		file.ReadCompObject(m_curr_index); 
		// resizes the back link vector to recreate the pointers
		m_back_link.Resize(m_curr_index); 

		// initilizes the word root
		m_word_root = new SLinkedList; 
		m_word_node = m_word_root; 
		m_word_node->buffer = new char [m_memory_block_size]; 
		m_word_offset = 0; 
		SLinkedList* prev_word_node = m_word_node; 
		int prev_word_offset = 0; 

		for(int i=0; i<m_breadth; i++) {

			// back tracks from the current node
			uChar back_track = 0; 
			file.ReadCompObject(back_track); 
			if(back_track == 0xFF)continue; 

			m_node_buffer.ExtendSize(1); 
			m_search_root[i] = &m_node_buffer.LastElement(); 
			m_binary_node = m_search_root[i]; 
			m_binary_node->forward_link = NULL; 
			m_binary_node->prev_link = NULL; 

			u_short check_sum = 0; 
			CArrayList< SBinaryLink *> prev_node(100); 
			prev_node.PushBack(m_binary_node); 	// stores the root node

			// reads the node's check sum from file
			file.ReadCompObject(check_sum); 
			// reads the root node of the tree in from file
			ReadTreeNodeFromFile(file); 
			// assigns the back link information
			m_back_link[m_binary_node->index].
				word_buffer = prev_word_node; 
			m_back_link[m_binary_node->index].
				word_buffer_offset = (u_short)prev_word_offset; 
			m_back_link[m_binary_node->index].
					check_sum = check_sum; 

			// stores the previous word node
			prev_word_node = m_word_node; 
			// stores the previous word offset
			prev_word_offset = m_word_offset; 	

			while(true) {
				// back tracks from the current node
				file.ReadCompObject(back_track); 
				// checks for the end of the tree
				if(back_track == 0xFF)break; 
				prev_node.Resize(prev_node.Size() - back_track); 
				m_binary_node = prev_node.LastElement(); 

				// reads the node's check sum from file
				file.ReadCompObject(check_sum); 

				if(check_sum > m_back_link[m_binary_node->index].check_sum) {
					m_node_buffer.ExtendSize(1); 
					m_binary_node->forward_link = &m_node_buffer.LastElement(); 
					m_binary_node = m_binary_node->forward_link; 
				} else {
					m_node_buffer.ExtendSize(1); 
					m_binary_node->prev_link = &m_node_buffer.LastElement(); 
					m_binary_node = m_binary_node->prev_link; 
				}

				prev_node.PushBack(m_binary_node); 
				// reads the root node of the tree in from file
				ReadTreeNodeFromFile(file); 
				// assigns the back link information
				m_back_link[m_binary_node->index].
					word_buffer = prev_word_node; 
				m_back_link[m_binary_node->index].
					word_buffer_offset = (u_short)prev_word_offset; 
				m_back_link[m_binary_node->index].
					check_sum = check_sum; 

				// stores the previous word node
				prev_word_node = m_word_node; 
				// stores the previous word offset
				prev_word_offset = m_word_offset; 	
			}
		}
		m_word_node->link = NULL; 
		m_word_end = m_word_node; 
	}

	// writes a dictionary to file
	void WriteFastHashDictionaryToFile(const char str[]) {
		if(Size() <= 0)return; 

		CHDFSFile file; 
		file.OpenWriteFile(CUtility::
			ExtendString(str, ".fast_hash_dictionary")); 

		WriteFastHashDictionaryToFile(file); 
	}

	// writes a dictionary to file
	void WriteFastHashDictionaryToFile(CHDFSFile &file) {

		file.WriteCompObject(m_breadth); 	// writes the breadth
		file.WriteCompObject(m_memory_block_size); // writes the memory block size
		file.WriteCompObject(m_curr_index); // writes the curr_index

		for(int i=0; i<m_breadth; i++) {
			m_binary_node = m_search_root[i]; 
			uChar back_track = 0; 
			if(!m_binary_node) {
				back_track = 0xFF; 
				// signifies end of tree
				file.WriteCompObject(back_track); 
				continue; 
			}

			// now writes the binary tree structure
			CArrayList< SBinaryLink *> prev_node(100); 
			CArrayList<char> direction(100); 
			direction.PushBack(0); 
			
			while(true) {
				if(direction.LastElement() <= 0) {
					direction.LastElement() = 1; 
					// writes the tree structure to file
					// writes the current backtrack
					file.WriteCompObject(back_track); 	
					// writes the nodes check sum to file
					file.WriteCompObject
						(m_back_link[m_binary_node->index].check_sum); 
					// writes the node index
					file.WriteCompObject(m_binary_node->index); 
					// writes the node's word to file
					WriteCompressionWordToFile(file, m_binary_node->index); 
					back_track = 0; 
				}
				if(m_binary_node->forward_link && (direction.LastElement() < 2)) {
					prev_node.PushBack(m_binary_node); 
					direction.LastElement() = 2; 
					direction.PushBack(0); 
					m_binary_node = m_binary_node->forward_link; 
				} else if(m_binary_node->prev_link && (direction.LastElement() < 3)) {
					prev_node.PushBack(m_binary_node); 
					direction.LastElement() = 3; 
					direction.PushBack(0); 
					m_binary_node = m_binary_node->prev_link; 
				} else {
					back_track++; 
					if(prev_node.Size() <= 0)break; 	// jumps to the next division
					m_binary_node = prev_node.PopBack(); 
					direction.PopBack(); 
				}
			}
			back_track = 0xFF; 
			// signifies end of tree
			file.WriteCompObject(back_track); 
		}
	}

	~CFastHashDictionary() {
		DeleteFastHashDictionary(); 
	}
}; 

// This is another dictionary that stores character strings. Again the character
// \0 is used internally to deliminate the strings and can therefore not be 
// present in any supplied string for successful operation. Internally the 
// dictionary is implemented as several bucket divisions that can be altered and
// each division is implemented as a linked list. When searching the hash key is 
// calculated and each division is search linearly for the given string. This 
// dictionary is suitable for smaller sizes and uses less memory. With a large 
// enough division size it is also suitable for larger dictionaries although 
// there may be some time penalty for use in large dictionaries.
template <class X> class CHashDictionary {

	// Stores a buffer segment that stores the character
	// string for a given bucket division.
	struct SLinkedWordList {
		// This stores a ptr to the buffer segment
		char *buffer; 
		// This stores a ptr to the next link in the segment
		SLinkedWordList *link; 
	}; 

	// This defines a position in the buffer
	struct SWordPos {
		// This stores a ptr to a set of ptrs that mark 
		// the beginning of a particular word
		SLinkedWordList *word_seg_ptr;
		// This stores the word id
		X id;
		// This stores the size of the word
		u_short word_length;
		// This stores the offset in the buffer
		uChar word_buff_offset;
	};

	// Stores a buffer segment that stores the character
	// string for a given bucket division.
	struct SLinkedIndexList {
		// This stores a ptr to the buffer segment
		SWordPos *buffer; 
		// This stores a ptr to the next link in the segment
		SLinkedIndexList *link; 
	}; 

	// stores the breadth of the hash table
	int m_breadth; 
	// stores the block size for the word segments
	uChar m_word_seg_size; 
	// stores the block size for the index segments
	uChar m_index_seg_size; 
	// stores the current word index
	X m_size; 

	// stores a Buffer to the word root for each division
	CMemoryChunk<SLinkedWordList *> m_word_root; 
	// stores a Buffer to the index root for each division
	CMemoryChunk<SLinkedIndexList *> m_index_root; 
	// stores the number of words in a given division
	CMemoryChunk<int> m_div_word_num;
	// stores the position of each word
	CVector<SWordPos *> m_word_pos; 
	// stores a Buffer to the current word node
	SLinkedWordList *m_word_node; 
	// stores a Buffer to the current index node
	SLinkedIndexList *m_index_node; 

	// a buffer used to store all the characters
	// associated with a given word
	CLinkedBuffer<char> m_word_buffer;
	// a buffer used to store all the indexes
	// associated with a given word
	CLinkedBuffer<SWordPos> m_index_buffer;
	// a buffer used to store all the word linked nodes
	CLinkedBuffer<SLinkedWordList> m_word_node_buffer;
	// a buffer used to store all the index linked nodes
	CLinkedBuffer<SLinkedIndexList> m_index_node_buffer;
	// stores the hash division for each word
	CLinkedBuffer<int> m_hash_div;
	// This stores the length of each word
	CLinkedBuffer<u_short> m_word_length;
	// true if the most recent lookup was found, false otherwise
	bool m_found; 

	// Creates the next sequential index for the current division link 
	// buffer when reading the next word in from file that corresponds.
	// @param div - the current hash division
	// @param index_offset - the current offset in the
	//                     - index buffer for the current div
	// @param index_end - ptr to the current end of the linked
	//                  - list for the word indexes in the current div
	void CreateSequentialIndex(int div, uChar &index_offset,
		CMemoryChunk<SLinkedIndexList *> &index_end) {

		m_index_node = index_end[div];
		if(index_offset >= m_index_seg_size) {
			m_index_node->link = m_index_node_buffer.ExtendSize(1);
			m_index_node = m_index_node->link;
			m_index_node->link = NULL;
			m_index_node->buffer = m_index_buffer.ExtendSize(m_index_seg_size);
			index_end[div] = m_index_node;
			index_offset = 0;
		}
	}
	
	// Creates a root node when reading a word in from memory
	// @param div - the current hash division
	// @param word_buff_pos - the current position in the word buffer
	void ReadRootNode(int div, SLinkedBuffPos &word_buff_pos) {

		m_word_node = m_word_node_buffer.ExtendSize(1);
		m_word_node->buffer = &m_word_buffer.NodeValue(word_buff_pos);
		m_word_buffer.MovePathForward(word_buff_pos, m_word_seg_size);
		m_word_root[div] = m_word_node;

		m_index_node = m_index_node_buffer.ExtendSize(1);
		m_index_node->buffer = m_index_buffer.ExtendSize(m_index_seg_size);
		m_index_node->link = NULL;
		m_index_root[div] = m_index_node;
	}

	// Reads in a word from file, it increments the curr pos + m_word_seg_size.
	// @param div - the current hash division
	// @param word_len - this is the word length of the next word
	// @param offset - the current word segment offset for
	//               - in the buffer corresponding to the 
	//               - current hash division
	// @param word_buff_pos - the current position in the word buffer
	// @param word_pos - this is the current word position being assigned
	void ReadWordFromFile(int div, int word_len, uChar &offset, 
		SLinkedBuffPos &word_buff_pos, SWordPos *word_pos) {

		word_pos->word_length = word_len;
		word_pos->word_seg_ptr = NULL;

		for(int i=0; i<word_len; i++) {

			if(offset >= m_word_seg_size) {
				m_word_node->link = m_word_node_buffer.ExtendSize(1);
				m_word_node = m_word_node->link;
				offset = 0;

				m_word_node->buffer = &m_word_buffer.NodeValue(word_buff_pos);
				if(m_word_buffer.AskNextNode(word_buff_pos)) {
					m_word_buffer.MovePathForward(word_buff_pos, m_word_seg_size);
				}
			}

			if(word_pos->word_seg_ptr == NULL) {
				word_pos->word_seg_ptr = m_word_node;
				word_pos->word_buff_offset = offset;
			}

			offset++;
		}
	}

	// Checks if a new word block needs to be created
	// @param offset - the offset in the character buffer
	// @return true if a new buffer segment was created
	// false otherwise
	bool CheckNewWordBlock(uChar &offset) {
		if(offset >= m_word_seg_size) {
			// puts a new link on the list
			m_word_node->link = m_word_node_buffer.ExtendSize(1); 	
			m_word_node = m_word_node->link; 	

			// allocates more memory for the new linked block
			m_word_node->buffer = m_word_buffer.ExtendSize(m_word_seg_size); 
			m_word_node->link = NULL; 	

			offset = 0; 
			return true; 
		}

		return false; 
	}

	// Checks if a new index block needs to be created
	// @param offset - the offset in the character buffer
	// @return true if a new buffer segment was created
	// false otherwise
	bool CheckNewIndexBlock(uChar &offset) {

		if(offset >= m_index_seg_size) {
			// puts a new m_index_seg_size on the list
			m_index_node->link = m_index_node_buffer.ExtendSize(1); 	
			m_index_node = m_index_node->link; 	

			// allocates more memory for the new linked block
			m_index_node->buffer = m_index_buffer.ExtendSize(m_index_seg_size); 
			m_index_node->link = NULL; 
			offset = 0; 
			return true; 
		}

		return false; 
	}

	// Writes the word to the dictionary
	// @param word_offset - the buffer offset in the word linked list
	// @param index_offset - the buffer offset in the index linked list
	// @param div - this is the hash division of the word
	// @param word - a buffer containing the character string being written
	// @param length - the length of the character string
	// @param start - an offset in the character string
	// @return the next sequential index in the dictionary (unique word id)
	X WriteWord(uChar &word_offset, uChar &index_offset, 
		int div, const char word[], int length) {

		m_found = false; 
		CheckNewIndexBlock(index_offset); 
		CheckNewWordBlock(word_offset); 
		m_div_word_num[div]++;

		m_hash_div.PushBack(div);
		m_word_length.PushBack(length);

		m_index_node->buffer[index_offset].word_seg_ptr = m_word_node;
		m_index_node->buffer[index_offset].word_buff_offset = word_offset;
		m_index_node->buffer[index_offset].word_length = length;
		m_index_node->buffer[index_offset].id = m_size; 

		m_word_pos.PushBack(&(m_index_node->buffer[index_offset]));

		for(int i=0; i<length; i++) {
			// creates a new segment if required
			CheckNewWordBlock(word_offset); 
			m_word_node->buffer[word_offset++] = word[i]; 
		}

		return m_size++; 
	}

	// This creates root word entry for one of the hash division
	// @param div - this is the current hash division
	void CreateRootHashEntry(int div) {

		m_word_root[div] = m_word_node_buffer.ExtendSize(1); 
		m_index_root[div] = m_index_node_buffer.ExtendSize(1); 

		m_word_node = m_word_root[div]; 
		m_index_node = m_index_root[div]; 

		m_word_node->buffer = m_word_buffer.ExtendSize(m_word_seg_size); 
		m_index_node->buffer = m_index_buffer.ExtendSize(m_index_seg_size); 

		m_word_node->link = NULL; 
		m_index_node->link = NULL; 
	}

public:

	CHashDictionary() {
	}

	// Initializes the buffers used in the dictionary
	// @param breadth - this is the hash breadth to use
	// @param linked_chunk_size - the number of bytes allocated at a 
	//                          - time to store a word
	CHashDictionary(int breadth, int linked_chunk_size = 64) {
		Initialize(breadth, linked_chunk_size); 
	}

	// Initializes the buffers used in the dictionary
	inline void Initialize() {
		Initialize(3333333, 32);
	}

	// Initializes the buffers used in the dictionary
	// @param breadth - this is the hash breadth to use
	// @param linked_chunk_size - the number of bytes allocated at a 
	//                          - time to store a word
	void Initialize(int breadth, int linked_chunk_size = 64) {

		m_breadth = CHashFunction::FindClosestPrime(breadth); 

		m_size = 0; 
		m_word_seg_size = linked_chunk_size; 
		m_index_seg_size = max(5, m_word_seg_size >> 3); 
		linked_chunk_size = min(linked_chunk_size, (int)0xFF);

		m_index_root.AllocateMemory(m_breadth, NULL); 
		m_word_root.AllocateMemory(m_breadth, NULL);
		m_div_word_num.AllocateMemory(m_breadth, 0);

		int word_buff_size = 4096 / m_word_seg_size;
		m_word_buffer.Initialize(word_buff_size * m_word_seg_size);
		int index_buff_size = 2048 / m_index_seg_size;
		m_index_buffer.Initialize(index_buff_size * m_index_seg_size);

		m_word_node_buffer.Initialize(1024);
		m_index_node_buffer.Initialize(1024);
		m_hash_div.Initialize(1024);
		m_word_length.Initialize(1024);
		m_word_pos.Resize(0);
	}

	// checks if a word was found previously
	inline bool AskFoundWord() {
		return m_found; 
	}

	// adds a word to the dictionary
	// @param word - a buffer containing the character string being written
	// @param length - the length of the character string
	// @param start - an offset in the character string
	X AddWord(const char word[], int length, int start = 0) {

		word += start;
		length -= start;

		int div = CHashFunction::UniversalHash
			(m_breadth, word, length); 

		m_word_node = m_word_root[div]; 
		m_index_node = m_index_root[div]; 
		uChar word_offset = 0;
		uChar index_offset = 0; 
		m_found = false;

		if(m_word_node == NULL) {
			CreateRootHashEntry(div);
			return WriteWord(word_offset, index_offset, div, word, length); 
		}

		char *word_buffer = m_word_node->buffer; 
		const char *curr_letter = word;
		int letter_offset = 0;
		int curr_word = 0;

		while(true) {	

			SWordPos *word_pos = m_index_node->buffer + index_offset;
			while(*word_buffer != *curr_letter || word_pos->word_length != length) {

				if(++curr_word >= m_div_word_num[div]) {
					m_word_node = word_pos->word_seg_ptr; 
					word_offset = word_pos->word_length + word_pos->word_buff_offset;

					while(m_word_node->link != NULL) {
						m_word_node = m_word_node->link;
						word_offset -= m_word_seg_size;
					}
					
					index_offset++;
					return WriteWord(word_offset, index_offset, div, word, length); 
				}

				if(++index_offset >= m_index_seg_size) {
					m_index_node = m_index_node->link; 
					index_offset = 0; 
				}

				word_pos = m_index_node->buffer + index_offset;
				m_word_node = m_index_node->buffer[index_offset].word_seg_ptr; 
				word_offset = m_index_node->buffer[index_offset].word_buff_offset; 
				word_buffer = m_word_node->buffer + word_offset; 

				curr_letter = word;
				letter_offset = 0;
			}

			if(++letter_offset >= length) {
				m_found = true;
				return m_index_node->buffer[index_offset].id;
			}

			word_buffer++; 
			curr_letter++;
			if(++word_offset >= m_word_seg_size) {
				m_word_node = m_word_node->link; 
				word_buffer = m_word_node->buffer; 
				word_offset = 0; 
			}
		}

		return -1; 
	}

	// checks if a word exists in the dictionary
	// @param word - a buffer containing the character string being written
	// @param length - the length of the character string
	// @param start - an offset in the character string
	X FindWord(const char word[], int length, int start = 0) {

		word += start;
		length -= start;

		int div = CHashFunction::UniversalHash
			(m_breadth, word, length); 

		m_word_node = m_word_root[div]; 
		m_index_node = m_index_root[div]; 
		uChar word_offset = 0;
		uChar index_offset = 0; 
		m_found = false;

		if(m_word_node == NULL) {
			return -1;
		}

		char *word_buffer = m_word_node->buffer; 
		const char *curr_letter = word;
		int letter_offset = 0;
		int curr_word = 0;

		while(true) {	

			while(*word_buffer != *curr_letter || m_index_node->buffer[index_offset].word_length != length) {
				if(++curr_word >= m_div_word_num[div]) {
					return -1; 
				}
		
				if(++index_offset >= m_index_seg_size) {
					m_index_node = m_index_node->link; 
					index_offset = 0; 
				}

				m_word_node = m_index_node->buffer[index_offset].word_seg_ptr; 
				word_offset = m_index_node->buffer[index_offset].word_buff_offset; 
				word_buffer = m_word_node->buffer + word_offset; 

				curr_letter = word;
				letter_offset = 0;
			}
			
			if(++letter_offset >= length) {
				m_found = true;
				return m_index_node->buffer[index_offset].id;
			}

			word_buffer++; 
			curr_letter++;
			if(++word_offset >= m_word_seg_size) {
				m_word_node = m_word_node->link; 
				word_buffer = m_word_node->buffer; 
				word_offset = 0; 
			}
		}

		return -1; 
	}

	// this is just a testing framework
	void TestHashDictionary() {
		CArrayList<char> word; 
		word.Initialize(); 
		CArrayList<int> word_start; 
		word_start.Initialize(); 
		word_start.PushBack(0); 

		Initialize(0xFFFFF);
		CHDFSFile::Initialize();

		ReadHashDictionaryFromFile("check");

		for(int i=0; i<60000; i++) {cout<<i<<endl;
			if((rand() % 2)) {
				int length = (rand() % 10) + 5; 
				for(int c=0; c<length; c++) {
					char a = (rand() % 62) + '0'; 
					word.PushBack(a); 
				}

				word_start.PushBack(word.Size()); 
			/*	AddWord(word.Buffer(), word.Size(), word.Size() - length); 
				if(AskFoundWord()) {
					cout << "error1"<< endl; 
					for(int i=word.Size() - length; i<word.Size(); i++)
					cout << word[i]; getchar(); 
				} */

			} else if((word_start.Size() - 1)) {
				int index = (rand() % (word_start.Size() - 1)); 
				int out = FindWord(word.Buffer(), word_start[index+1], word_start[index]); 
				if(!AskFoundWord()) {
					cout << "error2 "<<index<<" "<<out<<endl; 
					cout << word_start[index+1] <<" "<< word_start[index]; 
					getchar(); 
					for(int i=word_start[index]; i<word_start[index+1]; i++)
						cout << word[i]; 
					getchar(); 
				}
				if(out != index) {
					cout << "error3"; 
					getchar(); 
				}

				int length; 
				char *check = GetWord(index, length); 
				if(length != (word_start[index+1] - word_start[index])) {
					cout << "error4 "<< length <<" "<< word_start[index+1] - word_start[index]; 
					getchar(); 
				}

				for(int i=word_start[index], h = 0; i<word_start[index+1]; i++, h++) {
					if(check[h] != word[i]) {
						cout << "error5"; 
						getchar(); 
					}
				}
			}
		}
		WriteHashDictionaryToFile("check"); 
	}

	// retrieves a word from the dictionary
	// @param index - the unique index of the character string
	char *GetWord(X index, int &length) {

		if(index < 0 || index >= Size()) {
			throw EIndexOutOfBoundsException("invalid index");
		}

		uChar offset = m_word_pos[index]->word_buff_offset; 
		SLinkedWordList *curr_node = m_word_pos[index]->word_seg_ptr; 
		length = m_word_pos[index]->word_length;

		static char temp_buff[4096];
		char *dictionary_index = curr_node->buffer + offset; 

		for(int i=0; i<length; i++) {

			if(offset >= m_word_seg_size) {
				curr_node = curr_node->link; 
				dictionary_index = curr_node->buffer; 
				offset = 0; 
			}

			temp_buff[i] = *(dictionary_index++); 
			offset++;
		}

		return temp_buff; 
	}

	// retrieves a word from the dictionary
	// @param index - the unique index of the character string
	char *GetWord(X index) {
		int length; 
		char *buff = GetWord(index, length); 

		buff[length] = '\0'; 
		return buff; 
	}

	// removes the file containing the dictionary
	inline void RemoveHashDictionary(const char str[]) {
		CHDFSFile::Remove(CUtility::ExtendString(str, ".hash_dictionary"));
	}

	// resets the dictoinary
	inline void Reset() {
		Initialize(m_breadth, m_word_seg_size); 
	}

	// returns the size of the dictionary
	inline int Size() {
		return m_size; 
	}

	// returns the number of hash divisions
	inline int HashBreadth() {
		return m_breadth;
	}

	// reads the hash dictionary
	void ReadHashDictionaryFromFile(const char str[]) {
		CHDFSFile file; 
		file.OpenReadFile(CUtility::ExtendString
			(str, ".hash_dictionary"));

		ReadHashDictionaryFromFile(file);
	}

	// reads the hash dictionary
	void ReadHashDictionaryFromFile(CHDFSFile &file) {

		file.ReadCompObject(m_breadth); 	
		file.ReadCompObject(m_word_seg_size);
		Initialize(m_breadth, m_word_seg_size); 

		// reads the curr_index must be done after initilization
		file.ReadCompObject(m_size); 

		m_word_buffer.ReadLinkedBufferFromFile(file);
		m_hash_div.ReadLinkedBufferFromFile(file);
		m_word_length.ReadLinkedBufferFromFile(file);

		m_hash_div.ResetPath();
		m_word_buffer.ResetPath();
		m_word_length.ResetPath();
		// stores the current end of the word buffer
		SLinkedBuffPos word_buff_pos = m_word_buffer.GetTraversalPosition();

		// stores a ptr to the end of the word and index
		// linked lists for a particular division
		CMemoryChunk<SLinkedWordList *> word_end(HashBreadth());
		CMemoryChunk<SLinkedIndexList *> index_end(HashBreadth());

		// stores the offset in each buffer
		CMemoryChunk<uChar> index_offset_div(HashBreadth());
		CMemoryChunk<uChar> word_offset_div(HashBreadth());

		for(int i=0; i<Size(); i++) {
			int div = *m_hash_div.NextNode();
			if(m_index_root[div] == NULL) {
				ReadRootNode(div, word_buff_pos);
				index_end[div] = m_index_node; 
				word_end[div] = m_word_node; 
				index_offset_div[div] = 0;
				word_offset_div[div] = 0;
			} 

			u_short word_len = *m_word_length.NextNode();
			uChar &index_offset = index_offset_div[div];
			m_div_word_num[div]++;

			CreateSequentialIndex(div, index_offset, index_end);
			SWordPos *word_pos = m_index_node->buffer + index_offset++;
			m_word_pos.PushBack(word_pos);
			word_pos->id = (X)i;

			m_word_node = word_end[div];
			ReadWordFromFile(div, word_len, word_offset_div[div], word_buff_pos, word_pos);

			word_end[div] = m_word_node;
			m_word_node->link = NULL;
		}
	}

	// writes the hash dictionary
	void WriteHashDictionaryToFile(const char str[]) {

		CHDFSFile file; 
		file.OpenWriteFile(CUtility::ExtendString
			(str, ".hash_dictionary"));

		WriteHashDictionaryToFile(file);
	}

	// writes the hash dictionary
	void WriteHashDictionaryToFile(CHDFSFile &file) {

		file.WriteCompObject(m_breadth); 	
		file.WriteCompObject(m_word_seg_size);
		file.WriteCompObject(m_size); 

		m_word_buffer.WriteLinkedBufferToFile(file);
		m_hash_div.WriteLinkedBufferToFile(file);
		m_word_length.WriteLinkedBufferToFile(file);
	}
};

// This uses a hash map as a dictionary ADT to store arbitary
// object supplied by the user. There is only a mapping between
// an object and an index into the dictionary. All objects supplied
// as keys must be unique. Any object that already exists will not
// be added to the dictionary.
template <class X> class CObjectHashMap {

	// This is used to store a chain of linked
	// objects in the hash map
	struct SLinkObject {
		// stores the index of the object
		int index;
		// stores the hash division for an object
		int hash_div;
		// this stores a pointer to the next object in the chain
		SLinkObject *next_ptr;
		// this stores the actual object itself
		X object;
	};

	// This stores all the different objects.
	CLinkedBuffer<SLinkObject> m_obj_buff;
	// This stores a pointer to the start of a 
	// chain of objects of a given hash division
	CMemoryChunk<SLinkObject *> m_chain_ptr;
	// this is a function that calculates the hash code for an object
	int (*m_hash_code)(const X &object); 
	// this is a function used to test for equality
	bool (*m_equal)(const X &arg1, const X &arg2); 
	// true if the object already exists, false otherwise
	bool m_found;

	// This creates a new instance of an object in the dictionary
	// @param hash_div - the current hash division of the object
	// @param object - the object that is being added to the 
	//               - hash map
	// @param curr_node - the current node that is being added to the
	//                  - buffer
	int CreateObject(int hash_div, const X &object,
		SLinkObject **curr_node) {

		m_found = false;
		*curr_node = m_obj_buff.ExtendSize(1);
		(*curr_node)->object = object;
		(*curr_node)->next_ptr = NULL;
		(*curr_node)->index = m_obj_buff.Size() - 1;
		(*curr_node)->hash_div = hash_div;
		return (*curr_node)->index;
	}

	// This is used for testing
	static bool Equals(const int &arg1, const int &arg2) {
		return arg1 == arg2;
	}

	// This is used for testing
	static int HashCode(const int &object) {
		return object;
	}

public:

	CObjectHashMap() {
	}

	// This initiates an instance of an object hash map
	// @param hash_code - a function pointer that is used to 
	//                  - calculate the hash code for an object
	// @param equal - a function pointer that is used to test if
	//              - two objects are equal
	// @param div_num - the number of divisions to use in the hash map
	CObjectHashMap(int (*hash_code)(const X &object), 
		bool (*equal)(const X &arg1, const X &arg2), int div_num = 3333333) {

		Initialize(hash_code, equal, div_num);
	}

	// This initiates an instance of an object hash map
	// @param hash_code - a function pointer that is used to 
	//                  - calculate the hash code for an object
	// @param equal - a function pointer that is used to test if
	//              - two objects are equal
	// @param div_num - the number of divisions to use in the hash map
	void Initialize(int (*hash_code)(const X &object), 
		bool (*equal)(const X &arg1, const X &arg2), int div_num = 3333333) {

		div_num = CHashFunction::FindClosestPrime(div_num); 
		m_chain_ptr.AllocateMemory(div_num, NULL);
		m_obj_buff.Initialize(1024);
		m_hash_code = hash_code;
		m_equal = equal;
	}

	// This adds an object to the hash map. This is done
	// using open chaining where recently accessed elements
	// are placed at the front of the chain.
	// @param object - the object that is being added to the 
	//               - hash map
	// @return the index of the added object
	int Put(const X &object) {

		m_found = true;
		int div = m_hash_code(object) % HashBreadth();
		SLinkObject *curr_node = m_chain_ptr[div];

		if(curr_node == NULL) {
			return CreateObject(div, object, &m_chain_ptr[div]);
		}

		SLinkObject *prev_node = curr_node;
		while(curr_node != NULL) {
			if(m_equal(curr_node->object, object)) {
				return curr_node->index;
			}
			prev_node = curr_node;
			curr_node = curr_node->next_ptr;
		}

		return CreateObject(div, object, &(prev_node->next_ptr));
	}
	
	// This retrieves the index of a given object.
	// @param object - the object that is being looked up
	// @return the index of the object, -1 if the object
	//         doesn't exist
	int Get(const X &object) {

		int div = abs(m_hash_code(object)) % HashBreadth();
		SLinkObject *curr_node = m_chain_ptr[div];
		SLinkObject *prev_node = curr_node;

		while(curr_node != NULL) {
			if(m_equal(curr_node->object, object)) {
				// This places the object at the start of the
				// list for speed optimization.
				if(curr_node == m_chain_ptr[div]) {
					// already first
					return curr_node->index;
				}
				prev_node->next_ptr = curr_node->next_ptr;
				SLinkObject *prev = m_chain_ptr[div];
				m_chain_ptr[div] = curr_node;
				curr_node->next_ptr = prev;
				return curr_node->index;
			}
			prev_node = curr_node;
			curr_node = curr_node->next_ptr;
		}

		return -1;
	}

	// returns true if the object already exist during a Put
	// otherwise return false
	inline bool AskFoundWord() {
		return m_found;
	}

	// This resets the object hash map
	inline void Reset() {
		m_obj_buff.Reset();
		m_chain_ptr.InitializeMemoryChunk(NULL);
	}

	// This frees memory associated with the hash map
	inline void FreeMemory() {
		m_obj_buff.Restart();
		m_chain_ptr.FreeMemory();
	}

	// Returns the number of objects stored in the object hash map
	inline int Size() {
		return m_obj_buff.Size();
	}

	// returns the hash breadth of the dictionary
	inline int HashBreadth() {
		return m_chain_ptr.OverflowSize();
	}

	// This returns a buffer containing all the objects in the hash map
	// @param buff - this stores all the object in sequential order
	void RetrieveObjectBuffer(CMemoryChunk<X> &buff) {

		int index = 0;
		SLinkObject *object = NULL;
		m_obj_buff.ResetPath();
		buff.AllocateMemory(Size());

		while((object = m_obj_buff.NextNode()) != NULL) {
			buff[index++] = object->object;
		}
	}

	// This returns the next sequential element in the object map.
	// It's recommended that this is used over RetrieveObjectBuffer for
	// memory consumption reasons
	// @return the next sequential element
	X &NextSeqObject() {
		SLinkObject *object = m_obj_buff.NextNode();
		return object->object;
	}

	// This resets the path for the next sequential object
	inline void ResetNextObject() {
		m_obj_buff.ResetPath();
	}

	// This is just a test framework
	void TestObjectHashMap() {

		Initialize(HashCode, Equals, 100);

		CMemoryChunk<int> slot(99999, -1);

		//ReadObjectHashMap("test");

		int next = 0;
		for(int i=0; i<999999; i++) {
			int index = rand() % slot.OverflowSize();
			if(slot[index] >= 0) {
				if(Get(index) != slot[index]) {
					cout<<"Error1 "<<i;
					getchar();
				}

				if(Put(index) != slot[index]) {
					cout<<"Error5 "<<i;
					getchar();
				}
			} else {

				if(Get(index) != -1) {
					cout<<"Error2 "<<i;
					getchar();
				}

				if(Put(index) != next) {
					cout<<"Error3 "<<i;
					getchar();
				}

				slot[index] = next++;

				if(Size() != next) {
					cout<<"Error4 "<<i;
					getchar();
				}
			}
		}

		WriteObjectHashMap("test");
	}

	// This reads an ObjectHashMap from file
	// @param file - the file that's being read from
	void ReadObjectHashMap(const char str[]) {

		CHDFSFile file;
		file.OpenReadFile(CUtility::ExtendString
			(str, ".object_hash_map"));

		ReadObjectHashMap(file);
	}

	// This reads an ObjectHashMap from file
	// @param file - the file that's being read from
	void ReadObjectHashMap(CHDFSFile &file) {

		int hash_breadth;
		file.ReadCompObject(hash_breadth);

		CMemoryChunk<SLinkObject *> end_ptr(hash_breadth);
		m_chain_ptr.AllocateMemory(hash_breadth, NULL);
		m_obj_buff.Initialize();

		int buff_size;
		file.ReadCompObject(buff_size);

		SLinkObject object;
		SLinkObject *curr_ptr = NULL;
		for(int i=0; i<buff_size; i++) {
			file.ReadCompObject(object.hash_div);
			file.ReadCompObject(object.index);
			file.ReadCompObject(object.object);
			object.next_ptr = NULL;
			m_obj_buff.PushBack(object);

			if(m_chain_ptr[object.hash_div] == NULL) {
				m_chain_ptr[object.hash_div] = &m_obj_buff.LastElement();
				end_ptr[object.hash_div] = m_chain_ptr[object.hash_div];
				continue;
			}

			curr_ptr = end_ptr[object.hash_div];
			curr_ptr->next_ptr = &m_obj_buff.LastElement();
			end_ptr[object.hash_div] = curr_ptr->next_ptr;
		}
	}

	// This writes an ObjectHashMap to file
	// @param file - the file that's being written to
	void WriteObjectHashMap(const char str[]) {

		CHDFSFile file;
		file.OpenWriteFile(CUtility::ExtendString
			(str, ".object_hash_map"));

		WriteObjectHashMap(file);
	}

	// This writes an ObjectHashMap to file
	// @param file - the file that's being written to
	void WriteObjectHashMap(CHDFSFile &file) {

		SLinkObject *object = NULL;
		m_obj_buff.ResetPath();
		int hash_breadth = HashBreadth();
		file.WriteCompObject(hash_breadth);
		int buff_size = m_obj_buff.Size();
		file.WriteCompObject(buff_size);

		while((object = m_obj_buff.NextNode()) != NULL) {
			file.WriteCompObject(object->hash_div);
			file.WriteCompObject(object->index);
			file.WriteCompObject(object->object);
		}
	}
};

// This is concerned with steming a word so as 
// to remove an suffixes attatched to a base word.
// It must be preloaded with a supplied dictionary
// of suffixes for which to look for so matching
// can be performed with a trie.
class CStemWord {
	
	// This stores the suffix dictionary
	static CTrie m_suffix_dict;

	// This adds a suffix to the dictionary
	static void AddSuffix(const char str[], int length) {

		for(int i=0; i<length; i++) {
			CUtility::ThirdTempBuffer()[i] = str[length - i - 1];
		}

		m_suffix_dict.AddWord(CUtility::ThirdTempBuffer(), length);
	}

	// reads a list of suffixes from file
	static void ReadTextFile(const char dir[]) {
		// opens the file specified by the file pointer
		CHDFSFile file;
		file.OpenReadFile(dir);

		char ch; 
		int length = 0; 
		bool new_word = false; 
		char *str = CUtility::SecondTempBuffer();

		while(file.ReadObject(ch)) {	
			ch = tolower(ch); 
			if(CUtility::AskOkCharacter(ch)) {
				new_word = false; 
				str[length++] = ch; 
			} else if(!new_word && length) {
				new_word = true; 
				// adds the suffix word to dictionary
				AddSuffix(str, length); 	
				length = 0; 
			}
		}
	}

public:

	CStemWord() {
	}

	// Starts the stem words - only called once
	// @param suffix_file - the file containing suffixes, 
	static void Initialize(const char suffix_file[]) {
		m_suffix_dict.Initialize(4);
		ReadTextFile(suffix_file);
	}

	// Returns the length of the word with suffix removed
	// throws index out of bounds exception if the word
	// contains characters other than alphabet characters
	static int GetStem(const char str[], int length) {

		for(int i=0; i<min(length, 6); i++) {
			CUtility::ThirdTempBuffer()[i] = str[length - i - 1];
		}

		int closest_match;
		int suffix_length;
		m_suffix_dict.FindWord(closest_match, CUtility::ThirdTempBuffer(), min(length, 6));

		if(closest_match < 0) {
			return -1;
		}

		char *suffix = m_suffix_dict.GetWord(closest_match, suffix_length);

		if(CUtility::FindFragment(CUtility::ThirdTempBuffer(), suffix, suffix_length, 0)) {
			return length - suffix_length;
		}

		return -1;
	}

}; 
CTrie CStemWord::m_suffix_dict;

// This is the class responsible for stemming words that
// are found in the lexon. This includes storing each
// base word (no suffix) and searching for an equivalent
// base word for a word with a suffix not found in the dictionary.
class CRootWord {
	// creates an instance of a tree
	CTree<int> m_tree; 
	// stores the start of each word in the vector
	CVector<int> m_word_start; 
	// stores each word concanated together
	CVector<char> m_dictionary; 
	// true if the previous word was found, false otherise
	bool m_found;

	// insert a subtree at the current index for the child
	// held in the the child number position 0 - 26
	// @param curr_index - the child index in the three 0 - 26
	// @param word_index - the word index for the most recent word
	bool InsertSubTree(int curr_index, int word_index) {
		if(!m_tree.AskSubTree(26)) { 
			m_tree.SetSubTree(26); 
			// expand the tree at the extension point
			m_tree.ExpandTree(26, 26); 	
			m_tree[curr_index] = word_index; 
			return false; 
		}

		m_tree.TraverseTree(26); 
		if(m_tree.AskVacant(curr_index)) {
			m_tree[curr_index] = word_index; 
			return false; 
		}
		return true; 
	}

	// writes the word to the dictionary
	int WriteWord(const char str[], int length) {
		for(int i=0; i<length; i++) {
			m_dictionary.PushBack(str[i]); 
		}
		m_word_start.PushBack( m_dictionary.Size()); 
		m_found = false; 
		return m_word_start.Size() - 2; 
	}

	// used to check if a base word is valid for a 
	// given extension word that contains a suffix
	// @param curr_index - the current letter index [0-26]
	// @param letter_offset - the letter offset from the start of the word
	// @return the base word index if valid, -1 otherwise
	int CheckBaseWord(const char str[], int length, 
		int curr_index, int letter_offset) {

		if(!m_tree.AskLeaf(curr_index))return -1;
		if(m_tree.AskVacant(curr_index))return -1;
		// more than four letters matched - close enough
		if(letter_offset > 4)return m_tree[curr_index]; 

		int m_dictionary_index = m_tree[curr_index];
		int word_start = m_word_start[m_dictionary_index]; 
		int dictionary_length = m_word_start[m_dictionary_index+1] - word_start; 

		if(abs(length - dictionary_length) > 1)
			return -1;
	
		if(abs(length - dictionary_length) == 1) {
			char last_letter = m_dictionary[word_start + dictionary_length - 1];
			if(last_letter != 'e')
				return -1;
		}

		int offset = letter_offset;
		for(int c=word_start+letter_offset;offset<length;c++) {
			if(m_dictionary[c] != str[offset++])return -1;
		}

		return m_tree[curr_index]; 
	}

public:

	CRootWord() {
	}

	// starts the dictionary
	inline void Initialize() {
		m_tree.Resize(0);
		m_tree.Initialize(27); 
		m_dictionary.Resize(0);
		m_word_start.Resize(0);
		m_word_start.PushBack(0); 
	}

	// adds a root word to the dictionary to 
	// the alphabet trie - dictionary letters
	// are used to move down the trie not bits
	// @return a unique index for any given word
	int AddRootWord(const char str[], int length) {

		m_found = true;
		m_tree.ResetPath(); 
		int curr_index = 0; 
		for(int i=0; i<length; i++) {
			curr_index = str[i] - 'a'; 
			m_tree.SetReturnPoint(); 	
			// reached the end of the tree
			if(m_tree.TraverseTree(curr_index)) {	
				if(m_tree.AskVacant(curr_index)) {		
					// checks to see if something previously stored
					m_tree[curr_index] = WriteWord(str, length);
					return m_tree[curr_index]; 
				}

				int m_dictionary_index = m_tree[curr_index];
				int word_start = m_word_start[m_dictionary_index]; 
				int dictionary_length = m_word_start[m_dictionary_index+1] - word_start; 

				m_found = true;
				if(dictionary_length == length) {
					int offset = i;
					for(int c=word_start+i;offset<length;c++) {
						if(m_dictionary[c] != str[offset++]) 
							 {m_found = false; break;}
					}

					if(m_found)return m_tree[curr_index]; 
				}

				int dictionary_letter = curr_index; 
				int dictionary_offset = word_start + i; 
				int offset=0;

				while(true) {
					if(i + offset == length - 1) {
						m_tree.SetReturnPoint(); 
						InsertSubTree(curr_index, WriteWord(str, length)); 
						m_tree.BackTrack(); 
						m_tree.ExpandTree(27, curr_index); 
						dictionary_letter = m_dictionary[dictionary_offset + offset + 1] - 'a'; 	
						m_tree[dictionary_letter] = m_dictionary_index; 
						return m_word_start.Size() - 2; 
					}
					if(i + offset == dictionary_length - 1) {
						m_tree.SetReturnPoint(); 
						InsertSubTree(curr_index, m_dictionary_index); 	
						m_tree.BackTrack(); 
						m_tree.ExpandTree(27, curr_index); 
						curr_index = str[i + offset + 1] - 'a'; 
						m_tree[curr_index] = WriteWord(str, length); 
						return m_tree[curr_index]; 
					} 

					offset++; 
					m_tree.ExpandTree(27, curr_index); 	
					curr_index = str[i + offset] - 'a'; 
					dictionary_letter = m_dictionary[dictionary_offset + offset] - 'a'; 
					if(dictionary_letter != curr_index)break;
				}

				m_tree[dictionary_letter] = m_dictionary_index; 
				m_tree[curr_index] = WriteWord(str, length); 
				return m_tree[curr_index]; 
			} else if(i == (length - 1)) {
				m_tree.BackTrack(); 	// back tracks one 
				if(!InsertSubTree(curr_index, m_word_start.Size() - 1))	{
					m_tree[curr_index] = WriteWord(str, length); 
				}

				return m_tree[curr_index]; 
			}
		}

		return -1; 
	}

	// looks for a word in the dictionary
	// @return the index belonging to the word
	//         if found, -1 otherwise
	int FindRootWord(const char str[], int length) {
		m_tree.ResetPath(); 
		m_found = false; 
		int curr_index = 0; 
		for(int i=0; i<length; i++) {
			curr_index = str[i] - 'a'; 
			m_tree.SetReturnPoint(); 	
			// reached the end of the tree
			if(m_tree.TraverseTree(curr_index)) {	
				if(m_tree.AskVacant(curr_index)) {
					return -1;
				}

				int m_dictionary_index = m_tree[curr_index];
				int word_start = m_word_start[m_dictionary_index]; 
				int dictionary_length = m_word_start[m_dictionary_index+1] - word_start; 

				if(dictionary_length != length)return -1;
				int offset = i;
				for(int c=word_start+i;offset<length;c++) {
					if(m_dictionary[c] != str[offset++]) {
						return -1;
					}
				}

				m_found = true; 
				return m_tree[curr_index]; 
       		}
			if(i == (length - 1)) {
				m_tree.BackTrack(); 	// back tracks one 
				if(!m_tree.AskSubTree(26)) {
					return -1; 
				}

				m_tree.TraverseTree(26); 
				if(m_tree.AskVacant(curr_index)) {
					return -1; 
				}

				m_found = true; 
				return m_tree[curr_index]; 
			}
		}

		return -1; 
	}

	// looks for a root word - this means searching down the
	// tree matching up all the letters in the base section
	// of the word and trying to find a base word that matches
	// the word given
	// @return -1, if no base word was found, otherwise 
	//         returns the index of the base word
	int FindBaseWordPrefix(const char str[], int length) {
		m_tree.ResetPath(); 
		int base_length = CStemWord::GetStem(str, length);
		if(base_length < 6)return -1;

		int curr_index = 0; 
		int curr_level = 0;
		int next_level = 0;

		// base must be at least one less character than the extended word
		for(int i=0; i<base_length; i++) {
			curr_index = str[i] - 'a'; 
			next_level = m_tree.SetReturnPoint();

			if(m_tree.TraverseTree(curr_index)) {
				// reached the end of the tree
				if(m_tree.AskVacant(curr_index)) {
					m_tree.BackTrack(curr_level);
					int index = CheckBaseWord(str, base_length, 'e' - 'a', i);
					if(index >= 0)return index;
					return -1;
				}

				return CheckBaseWord(str, base_length, curr_index, i);

			} 

			curr_level = next_level;
			if(i >= (base_length - 1)) {
				// checks for a different letter suffix
				// ie the e or y may be concantanted from the suffix word
				m_tree.BackTrack();
				int index = CheckBaseWord(str, base_length, 'e' - 'a', i);
				if(index >= 0)return index;

				if(m_tree.AskSubTree(26)) {	
					m_tree.TraverseTree(26); 
					index = CheckBaseWord(str, base_length, curr_index, i);
					if(index >= 0)return index;
					index = CheckBaseWord(str, base_length, 'e' - 'a', i);
					if(index >= 0)return index;
				} 
				return -1;
			}
		}

		return -1; 
	}

	// this is just a testing framework
	void TestRootWord() {
		CArrayList<char> word; 
		word.Initialize(); 
		CArrayList<int> word_start; 
		word_start.Initialize(); 
		word_start.PushBack(0); 

		// ReadTrieFromFile("check"); 

		for(int i=0; i<200000; i++) {
			if((rand() % 2)) {
				int length = (rand() % 203) + 5; 
				for(int c=0; c<length; c++) {
					char a = (rand() % 26) + 'a'; 
					word.PushBack(a); 
				}

				word_start.PushBack(word.Size()); 
				int out = FindRootWord(word.Buffer() + word.Size() - length, length); 
				if(out >= 0) {cout << "new error"; getchar();}

				AddRootWord(word.Buffer() + word.Size() - length, length); 
				if(AskFoundWord()) {
					cout << "error1"<< endl; 
					for(int i=word.Size() - length; i<word.Size(); i++)
						cout << word[i]; getchar(); 
				}

			} else if((word_start.Size() - 1)) {
				int index = (rand() % (word_start.Size() - 1)); 
				int out = FindRootWord(word.Buffer()  +word_start[index], word_start[index+1] - word_start[index]); 
				if(out < 0) {
					cout << "error2"<< endl; 
					cout << word_start[index+1] <<" "<< word_start[index]; 
					getchar(); 
					for(int i=word_start[index]; i<word_start[index+1]; i++)
						cout << word[i]; 
					getchar(); 
				}
				if(out != index) {
					cout << "error3"; 
					getchar(); 
				}

				int length; 
				char *check = GetWord(index, length); 
				if(length != (word_start[index+1] - word_start[index])) {
					cout << "error4 "<< length <<" "<< word_start[index+1] - word_start[index]; 
					getchar(); 
				}
				for(int i=word_start[index], h = 0; i<word_start[index+1]; i++, h++) {
					if(check[h] != word[i]) {
						cout << "error5"; 
						getchar(); 
					}
				}
			}
		}
		// WriteTrieToFile("check"); 
	}

	// returns true if the last word was found, false othewise
	inline int AskFoundWord() {
		return m_found;
	}

	// returns the word at some index
	// @param index - the root word index
	// @param length - stores the length of the word
	char *GetWord(int index, int &length) {
		if(index < 0 || index >= Size())throw
			EIndexOutOfBoundsException("index out of bounds");

		char *word = CUtility::TempBuffer(); 
		length = m_word_start[index+1] - m_word_start[index]; 
		m_dictionary.CopyVectorToBuffer(word, length, m_word_start[index]); 

		return CUtility::TempBuffer(); 
	}

	// return the size of the dictionary
	inline int Size() {
		return m_word_start.Size() - 1; 
	}

	// reads from file
	void ReadRootWordFromFile(const char str[]) {
		CHDFSFile file; 
		file.OpenReadFile(CUtility::ExtendString(str, ".rootword")); 
		ReadRootWordFromFile(file); 
	}

	// reads from file
	void ReadRootWordFromFile(CHDFSFile &file) {
		m_tree.ReadTreeFromFile(file); 
		m_word_start.ReadVectorFromFile(file); 
		m_dictionary.ReadVectorFromFile(file); 
	}

	// writes to file
	void WriteRootWordToFile(const char str[]) {

		CHDFSFile file; 
		file.OpenWriteFile(CUtility::
			ExtendString(str, ".rootword")); 

		WriteRootWordToFile(file); 
	}

	// writes to file
	void WriteRootWordToFile(CHDFSFile &file) {
		m_tree.WriteTreeToFile(file); 
		m_word_start.WriteVectorToFile(file); 
		m_dictionary.WriteVectorToFile(file); 
	}
};

// This is an ordered binary tree class that allows
// an automatic balancing after the insertion and deletion of
// elements from the tree. Again multiple elements of equal
// value can be stored and are attatched as a linked list
// to the src node corresponding to the element's value.
template <class X> class CRedBlackTree {

	// these are the two colours used in the red black tree
	static const int BLACK = 1;
	static const int RED = 0;

	// declares a same_node node
	struct SSameNode {
		// This stores the node value
		X node; 
		// This stores a ptr to a list of
		// nodes with the same value
		SSameNode *same_link; 
	}; 

	// declares a node
	struct SNode {
		// This stores the node value
		X node; 
		// This is the color of the node
		bool color; 
		// This is the left child
		SNode *left_child;
		// This is the right child
		SNode *right_child;
		// This is the parent of the node
		SNode *parent;
		// This stores a ptr to a list of
		// nodes with the same value
		SSameNode *same_link; 
	}; 

	// stores the same node links
	CLinkedBuffer<SSameNode> m_same_node_buffer; 
	// stores a Buffer to the list of same free nodes
	SSameNode *m_same_free_node; 

	// stores a Buffer to the root and current node
	SNode *m_root_node, *m_curr_node; 
	// creates a buffer to store the nodex
	CLinkedBuffer<SNode> m_node_buffer; 
	// stores the list of free nodes
	SNode *m_free_node; 
	// stores the number of nodes in the tree
	int m_node_num; 

	// Creates a new node
	SNode *CreateNewNode() {
		if(m_free_node) {
			SNode *prev_node = m_free_node; 
			m_free_node = m_free_node->left_child; 
			return prev_node; 
		}
		m_node_buffer.ExtendSize(1); 
		return &m_node_buffer.LastElement(); 
	}

	// Creates a new same node
	SSameNode *CreateNewSameNode() {
		if(m_same_free_node != NULL) {
			SSameNode *prev_node = m_same_free_node; 
			m_same_free_node = m_same_free_node->same_link; 
			return prev_node; 
		}
		m_same_node_buffer.ExtendSize(1); 
		return &m_same_node_buffer.LastElement(); 
	}

	// Deletes a same node and add to the free list
	void DeleteSameNode(SSameNode *node) {
		SSameNode *prev_node = m_same_free_node; 
		m_same_free_node = node; 
		m_same_free_node->same_link = prev_node; 
	}


	// Rotate the binary tree node with the 
	// right_child child
	void RotateLeft(SNode *p) {
		SNode *r = p->right_child; 
		p->right_child = r->left_child; 
		if(r->left_child != NULL) {
			(r->left_child)->parent = p; 
		}

		r->parent = p->parent; 

		if(p->parent == NULL)
			m_root_node = r; 
		else if((p->parent)->left_child == p)
			(p->parent)->left_child = r; 
		else
			(p->parent)->right_child = r; 

		r->left_child = p; 
		p->parent = r; 
	}

	// Rotate the binary tree node with the 
	// left_child child
	void RotateRight(SNode *p) {
		SNode *l = p->left_child; 
		p->left_child = l->right_child; 
		if(l->right_child != NULL) 
			(l->right_child)->parent = p; 

		l->parent = p->parent; 

		if(p->parent == NULL)
			m_root_node = l; 
		else if((p->parent)->right_child == p)
			(p->parent)->right_child = l; 
		else (p->parent)->left_child = l; 
			l->right_child = p; 

		p->parent = l; 
	}


	// Sets the color of a particular node
	inline static void SetColor(SNode *p, bool c) { 
		if(p != NULL) p->color = c; 
	}

	// Returns the left_child child of the parent
	inline static SNode *LeftOf(SNode *p) { 
		return (p == NULL)? NULL: p->left_child; 
	}

	// Returns the right_child child of the parent
	inline static SNode *RightOf(SNode *p) { 
		return (p == NULL)? NULL: p->right_child; 
	}

	// Returns the parent
	inline static SNode *ParentOf(SNode *p) { 
		return (p == NULL ? NULL: p->parent); 
	}

	// Returns the color of a node
	inline static bool ColorOf(SNode *p) {
		return (p == NULL ? BLACK : p->color); 
	}

	// Returns a node in the tree
	SNode *GetEntry(const X &node) {

		SNode *curr_node = m_root_node; 

		while(curr_node != NULL) {
			int cmp = m_compare(node, curr_node->node); 

			if(cmp == 0) {
				return curr_node; 
			}
	 
			if(cmp < 0) {
				curr_node = curr_node->right_child; 
			} else {
				curr_node = curr_node->left_child; 
			}
		}

		return NULL; 
	}

	// Balances the tree after an insertion
	void FixAfterInsertion(SNode *x) {
		x->color = RED; 

		while(x != NULL && x != m_root_node && (x->parent)->color == RED) {
			if(ParentOf(x) == LeftOf(ParentOf(ParentOf(x)))) {
				SNode *y = RightOf(ParentOf(ParentOf(x))); 
				if(ColorOf(y) == RED) {
					SetColor(ParentOf(x), BLACK); 
					SetColor(y, BLACK); 
					SetColor(ParentOf(ParentOf(x)), RED); 
					x = ParentOf(ParentOf(x)); 
				} else {
					if(x == RightOf(ParentOf(x))) {
						x = ParentOf(x); 
						RotateLeft(x); 
					}
					SetColor(ParentOf(x), BLACK); 
					SetColor(ParentOf(ParentOf(x)), RED); 
					if(ParentOf(ParentOf(x)) != NULL) 
						RotateRight(ParentOf(ParentOf(x))); 
				}
			} else {
				SNode *y = LeftOf(ParentOf(ParentOf(x))); 
				if(ColorOf(y) == RED) {
					SetColor(ParentOf(x), BLACK); 
					SetColor(y, BLACK); 
					SetColor(ParentOf(ParentOf(x)), RED); 
					x = ParentOf(ParentOf(x)); 
				} else {
					if(x == LeftOf(ParentOf(x))) {
						x = ParentOf(x); 
						RotateRight(x); 
					}
					SetColor(ParentOf(x), BLACK); 
					SetColor(ParentOf(ParentOf(x)), RED); 
					if(ParentOf(ParentOf(x)) != NULL) 
						RotateLeft(ParentOf(ParentOf(x))); 
				}
			}
		}
		m_root_node->color = BLACK; 
	}

	// Rebalances the tree after a deletion
	void FixAfterDeletion(SNode *x) {
		while(x != m_root_node && ColorOf(x) == BLACK) {
			if(x == LeftOf(ParentOf(x))) {
				SNode *sib = RightOf(ParentOf(x)); 

				if(ColorOf(sib) == RED) {
					SetColor(sib, BLACK); 
					SetColor(ParentOf(x), RED); 
					RotateLeft(ParentOf(x)); 
					sib = RightOf(ParentOf(x)); 
				}

				if(ColorOf(LeftOf(sib)) == BLACK && 
					ColorOf(RightOf(sib)) == BLACK) {
					SetColor(sib, RED); 
					x = ParentOf(x); 
				} else {
					if(ColorOf(RightOf(sib)) == BLACK) {
						SetColor(LeftOf(sib), BLACK); 
						SetColor(sib, RED); 
						RotateRight(sib); 
						sib = RightOf(ParentOf(x)); 
					}
					SetColor(sib, ColorOf(ParentOf(x))); 
					SetColor(ParentOf(x), BLACK); 
					SetColor(RightOf(sib), BLACK); 
					RotateLeft(ParentOf(x)); 
					x = m_root_node; 
				}
			} else {// symmetric
				SNode *sib = LeftOf(ParentOf(x)); 

				if(ColorOf(sib) == RED) {
					SetColor(sib, BLACK); 
					SetColor(ParentOf(x), RED); 
					RotateRight(ParentOf(x)); 
					sib = LeftOf(ParentOf(x)); 
				}

				if(ColorOf(RightOf(sib)) == BLACK && 
					ColorOf(LeftOf(sib)) == BLACK) {
					SetColor(sib, RED); 
					x = ParentOf(x); 
				} else {
					if(ColorOf(LeftOf(sib)) == BLACK) {
						SetColor(RightOf(sib), BLACK); 
						SetColor(sib, RED); 
						RotateLeft(sib); 
						sib = LeftOf(ParentOf(x)); 
					}
					SetColor(sib, ColorOf(ParentOf(x))); 
					SetColor(ParentOf(x), BLACK); 
					SetColor(LeftOf(sib), BLACK); 
					RotateRight(ParentOf(x)); 
					x = m_root_node; 
				}
			}
		}

		SetColor(x, BLACK); 
	}

	// Returns the Successor to replace the deleted node
	SNode *Successor(SNode *t) {
		if(t == NULL)
			return NULL; 

		if(t->right_child != NULL) {
			SNode *p = t->right_child; 
			while(p->left_child != NULL)
				p = p->left_child; 
			return p; 
		} 

		SNode *p = t->parent; 
		SNode *ch = t; 
		while(p != NULL && ch == p->right_child) {
			ch = p; 
			p = p->parent; 
		}
		return p; 
	}

	// Delete node p, and then rebalance the tree.
	void DeleteEntry(SNode *p) {
		m_node_num--; 

		if(p->same_link != NULL) {
			SSameNode *next_node = p->same_link; 
			// disconnects the next sequential 
			// same node in the chain
			p->same_link = next_node->same_link; 
			p->node = next_node->node; 
			// removes the chained same dst node
			// not the root connected binary node
			DeleteSameNode(next_node); 
			return; 
		}
	 
		// If strictly internal, copy Successor's element to p and then make p
		// point to Successor.
		if(p->left_child != NULL && p->right_child != NULL) {
			SNode *s = Successor (p); 
			p->node = s->node; 
			p->same_link = s->same_link; 
			p = s; 
		}// p has 2 children

		// Start fixup at replacement node, if it exists.
		SNode *replacement = (p->left_child != NULL ? 
				p->left_child : p->right_child); 

		if(replacement != NULL) {
			// Link replacement to parent
			replacement->parent = p->parent; 
			if(p->parent == NULL)
				m_root_node = replacement; 
			else if(p == (p->parent)->left_child)
				(p->parent)->left_child = replacement; 
			else
				(p->parent)->right_child = replacement; 

			// NULL out links so they are OK to use by FixAfterDeletion.
			p->left_child = p->right_child = p->parent = NULL; 

			// Fix replacement
			if(p->color == BLACK)
				FixAfterDeletion(replacement); 
		} else if(p->parent == NULL) {
			// return if we are the only node.
			m_root_node = NULL; 
		} else {
			// No children. Use self as phantom replacement and unlink.
			if(p->color == BLACK)
				FixAfterDeletion(p); 

			if(p->parent != NULL) {
				if(p == (p->parent)->left_child)
					(p->parent)->left_child = NULL; 
				else if(p == (p->parent)->right_child)
					(p->parent)->right_child = NULL; 
				p->parent = NULL; 
			}
		}
	}

	// Checks the tree is correctly balanced
	bool CheckTree() {
		if(m_root_node == NULL)return true; 

		CArrayList<SNode *> parent(100); 
		// now writes the binary tree structure
		CArrayList<SNode *> prev_node(100); 
		CArrayList<char> direction(100); 
		direction.PushBack(0); 

		m_curr_node = m_root_node; 
		if(m_curr_node->color != BLACK)return false; 
		int curr_element = 0; 
		char prev_color = BLACK; 
		int black_count = 0; 
		int final_count = 0; 
		bool end_reached = false; 
		bool back_track = false; 

		while(true) {
			if(direction.LastElement() <= 0) {
				direction.LastElement() = 1; 
				if(prev_color == RED) {
					if(m_curr_node->color != BLACK)
					 {cout << "gogo"<< m_curr_node->node; return false;}
				}
				// if(m_compare(m_curr_node->node, 
				// 	item_list[curr_element++]))return false; 
			}
			prev_color = m_curr_node->color; 
			if(m_curr_node->left_child && (direction.LastElement() < 2)) {
			if(m_curr_node->color == BLACK && !back_track)black_count++; 
				parent.PushBack(m_curr_node); 
				back_track = false; 
				direction.LastElement() = 2; 
				direction.PushBack(0); 
				m_curr_node = m_curr_node->left_child; 
			} else if(m_curr_node->right_child && (direction.LastElement() < 3)) {
				if(m_curr_node->color == BLACK && !back_track)black_count++; 
				back_track = false; 
				parent.PushBack(m_curr_node); 
				direction.LastElement() = 3; 
				direction.PushBack(0); 
				m_curr_node = m_curr_node->right_child; 
			} else {
				if(!parent.Size())break; 	// jumps to the next division
				back_track = true; 

				if(m_curr_node->color == BLACK)black_count--; 
				m_curr_node = parent.PopBack(); 
				direction.PopBack(); 
			}

			if(!m_curr_node->left_child && !m_curr_node->right_child) {
				if(m_curr_node->color == BLACK)black_count++; 
				if(!end_reached) {
					end_reached = true; 
					final_count = black_count; 
				} 

				if(end_reached) {

					if(black_count != final_count)return false; 
				}
			}
		}

		return true; 
	}

	// This just creates a null node
	// @param node - the node that is being initilized
	void CreateNullNode(SNode * &node) {
		node = CreateNewNode(); 
		node->left_child = NULL; 
		node->right_child = NULL; 
		node->parent = NULL; 
		node->same_link = NULL; 
	}

	// This compares two items, this is used for testing
	static int TestCompareItems(const int &arg1, const int &arg2) {

		if(arg1 < arg2) {
			return 1;
		}

		if(arg1 > arg2) {
			return -1;
		}

		return 0;
	}

	// This copies the set of nodes that make up the tree to a buffer
	// @param buff - this is the buffer to which the nodes are copied
	void CopyNodesToBuffer(CArray<X> &buff, SNode *parent) {

		buff.PushBack(parent->node);
		SSameNode *next_node = parent->same_link;
		while(next_node != NULL) {
			buff.PushBack(next_node->node);
			next_node = next_node->same_link; 
		}
		
		if(parent->left_child != NULL) {
			CopyNodesToBuffer(buff, parent->left_child);
		}

		if(parent->right_child != NULL) {
			CopyNodesToBuffer(buff, parent->right_child);
		}
	}

protected:

	// Stores a Buffer to the compare function
	int (*m_compare)(const X &arg1, const X &arg2); 

public:

	CRedBlackTree() {
		m_root_node = NULL; 
	}

	// This initializes the Red Black Tree by allocating 
	// memory and setting up the root node.
	// @param init_size - this is the size to used for the
	//                  - buffer to contain the nodes
	// @param compare - this is the comparison function used
	//                - to order the nodes in the tree
	CRedBlackTree(int init_size, 
		int (*compare)(const X &arg1, const X &arg2)) {
		m_root_node = NULL; 
		Initialize(init_size, compare); 
	}

	// This initializes the Red Black Tree by allocating 
	// memory and setting up the root node.
	// @param init_size - this is the size to used for the
	//                  - buffer to contain the nodes
	// @param compare - this is the comparison function used
	//                - to order the nodes in the tree
	void Initialize(int init_size, 
		int (*compare)(const X &arg1, const X &arg2)) {

		m_compare = compare; 

		if(m_root_node != NULL) {
			if(init_size == m_node_buffer.BufferSize()) {
				Reset();
				return;
			}
		}

		m_node_buffer.Restart(init_size); 
		m_same_node_buffer.Restart(100); 
		m_free_node = NULL; 
		m_same_free_node = NULL; 
		m_root_node = NULL; 
		m_node_num = 0; 
	}

	// Retrieves the next node in the tree this is done in random order.
	// @return one of the nodes in the tree
	X &NextNode() {
		if(m_node_buffer.NumNodesTraversed() < m_node_buffer.Size()) {
			return m_node_buffer.NextNode().node; 
		}

		if(m_same_node_buffer.NumNodesTraversed() < m_same_node_buffer.Size()) {
			return m_same_node_buffer.NextNode().node; 
		}
		 
		throw EOverflowException("No More Nodes in the Tree"); 
	}

	// resets the both different and same next node
	inline void ResetNextNode() {
		m_same_node_buffer.ResetPath(); 
		m_node_buffer.ResetPath(); 
	}

	// Sorts the nodes using tree sort and copies them to a buffer
	void TreeSort(CMemoryChunk<X> &sorted_buffer) {
		if(m_root_node == NULL) {
			return; 
		}

		sorted_buffer.AllocateMemory(m_node_num); 

		int offset = 0; 
		m_curr_node = m_root_node; 
		CArrayList<SNode *> prev_node(100); 
		CArrayList<char> direction(100); 
		direction.PushBack(0); 
		
		while(true) {

			if(m_curr_node->right_child && (direction.LastElement() < 2)) {
				prev_node.PushBack(m_curr_node); 
				direction.LastElement() = 2; 
				direction.PushBack(0); 
				m_curr_node = m_curr_node->right_child; 
			} else if(m_curr_node->left_child && (direction.LastElement() < 3)) {
				sorted_buffer[offset++] = m_curr_node->node; 
				prev_node.PushBack(m_curr_node); 
				direction.LastElement() = 3; 
				direction.PushBack(0); 
				m_curr_node = m_curr_node->left_child; 
			} else {
				// records a null node
				if(!m_curr_node->left_child) {
					sorted_buffer[offset++] = m_curr_node->node; 
				} 

				if(prev_node.Size() <= 0)break; 	// jumps to the next division
				m_curr_node = prev_node.PopBack(); 
				direction.PopBack(); 
			}
		}
	}

	// Checks if a node exists in the tree
	inline bool AskContainNode(const X &node) {
		return GetEntry(node) != NULL;
	}

	// This copies the set of nodes that make up the tree to a buffer
	// @param buff - this is the buffer to which the nodes are copied
	inline void CopyNodesToBuffer(CArray<X> &buff) {

		buff.Resize(0);
		if(m_root_node != NULL) {
			CopyNodesToBuffer(buff, m_root_node);
		}
	}

	// Just a test framework
	void TestRedBlackTree() {
		//ReadRedBlackTreeFromFile("red"); 

		Initialize(1000, TestCompareItems);

		CArrayList<int> element(100000); 
		element.Initialize(); 

		for(int i=0; i<1000; i++) {
			int insert = rand() % 100000; 
			cout << endl << "Insert Element"<< insert << endl; 
			element.PushBack(insert); 
			AddNode(insert); 
			int *ret = FindNode(insert);
			if(*ret != insert) {
				cout<<"Element Not Found Error";getchar();
			}
			if(!AskContainNode(insert)) {
				cout << "Cannot Find"<< endl; 
				getchar(); 
			}
			if(!CheckTree()) {
				cout << "Balance Error "<< i;
				ShowTree(); 
				getchar(); 
			}
		}

		for(int i=0; i<1000; i++) {
			if(!AskContainNode(element[i])) {
				cout << "Cannot Find "<< element[i] << endl; 
				getchar(); 
			}
		}

		WriteRedBlackTreeToFile("red"); 

		for(int i=0; i<1000; i++) {
			cout << "Delete Element "<< element[i] << endl; 
			if(!DeleteNode(element[i])) {
				cout << " Cannot Find error "<< element[i]; getchar();
				ShowTree(); 
				getchar(); 
			}

			if(!CheckTree()) {
				cout << "Balance Error "<< i; getchar();
				ShowTree(); 
				getchar(); 
			}
		}
	}

	// This finds a node in the tree
	// @param node - this is the node value that's being 
	//             - searched for 
	// @return a pointer to the node if found, NULL otherwise
	X *FindNode(const X &node) {

		SNode *pos = GetEntry(node);
		if(pos == NULL) {
			return NULL;
		}

		return &pos->node;
	}

	// This finds a node in the tree
	// @param node - this is the node value that's being 
	//             - searched for 
	// @return a pointer to the node if found, NULL otherwise
	inline void *NodePosition(const X &node) {
		return GetEntry(node);
	}

	// Adds a node to the binary tree
	// @param node - this is the node value that's being 
	//             - searched for 
	// @return a pointer to the node that's been added
	X *AddNode(const X &node) {
		m_node_num++; 

		if(m_root_node == NULL) {
			CreateNullNode(m_root_node);
			m_root_node->color = BLACK; 
			m_root_node->node = node; 
			return &m_root_node->node; 
		}

		SNode *parent = NULL; 
		m_curr_node = m_root_node; 
		
		while(true) {
			// stores the value of the compare function
			int compare = m_compare(node, m_curr_node->node); 

			if(compare < 0) {
				if(m_curr_node->right_child == NULL) {
					parent = m_curr_node; 
					m_curr_node->right_child = CreateNewNode(); 
					m_curr_node = m_curr_node->right_child; 
					break; 
				}
				m_curr_node = m_curr_node->right_child; 
			} else if(compare > 0) {
				if(m_curr_node->left_child == NULL) {
					parent = m_curr_node; 
					m_curr_node->left_child = CreateNewNode(); 
					m_curr_node = m_curr_node->left_child; 
					break; 
				}
				m_curr_node = m_curr_node->left_child; 
			} else {
				// equal to
				SSameNode *prev_node = m_curr_node->same_link; 
				m_curr_node->same_link = CreateNewSameNode(); 
				SSameNode *next_node = m_curr_node->same_link; 
				next_node->same_link = prev_node; 
				next_node->node = node; 
				return &next_node->node; 
			}
		}

		m_curr_node->color = RED; 
		m_curr_node->left_child = NULL; 
		m_curr_node->right_child = NULL; 
		m_curr_node->same_link = NULL; 
		m_curr_node->node = node; 
		m_curr_node->parent = parent; 

		FixAfterInsertion(m_curr_node); 
		return &m_curr_node->node; 
	}

	// Prints out the tree
	void ShowTree() {
		if(m_root_node == NULL)return; 
		SNode *m_curr_node = m_root_node; 
		CArrayList<SNode *> parent(100); 
		CArrayList<CArrayList<SNode *> > tree(100); 
		CArrayList<CArrayList<SNode *> > up_node(100); 

		tree.Resize(100); 
		up_node.Resize(100); 
		for(int i=0; i<100; i++) {
			tree[i].Initialize(100); 
			up_node[i].Initialize(100); 
		}

		// now writes the binary tree structure
		CArrayList<SNode *> prev_node(100); 
		CArrayList<char> direction(100); 
		direction.PushBack(0); 

		int depth = 0; 
		m_curr_node = m_root_node; 
		SNode *before_node = m_root_node; 

		while(true) {
			if(direction.LastElement() <= 0) {
				direction.LastElement() = 1; 
				tree[depth].PushBack(m_curr_node); 
				up_node[depth].PushBack(before_node); 

				depth++; 
			}
			if(m_curr_node->left_child && (direction.LastElement() < 2)) {
				parent.PushBack(m_curr_node); 
				direction.LastElement() = 2; 
				direction.PushBack(0); 
				before_node = m_curr_node; 
				m_curr_node = m_curr_node->left_child; 
			} else if(m_curr_node->right_child && (direction.LastElement() < 3)) {
				parent.PushBack(m_curr_node); 
				direction.LastElement() = 3; 
				direction.PushBack(0); 
				before_node = m_curr_node; 
				m_curr_node = m_curr_node->right_child; 
			} else {
				depth--; 
				if(!parent.Size())break; 	// jumps to the next division
				m_curr_node = parent.PopBack(); 
				before_node = m_curr_node; 
				direction.PopBack(); 
			}
		}

		for(int i=0; i<tree.Size(); i++) {
			for(int c=0; c<tree[i].Size(); c++) {

				cout << c << ": "<< tree[i][c]->node << " - "; 
				if(tree[i][c]->color == RED)
					cout << "RED"<< " - "; 
				else if(tree[i][c]->color == BLACK)
					cout << "BLACK"<< " - "; 

				if(!tree[i][c]->left_child && !tree[i][c]->right_child)
					cout << "NULLnode - "; 

				if(up_node[i][c] && i) {
					for(int d = 0; d < tree[i - 1].Size(); d++) {
						if(up_node[i][c] == tree[i - 1][d]) {
							cout << d << " "; 
							break; 
						}
					}
				}
			}

			if(tree[i].Size())cout << endl; 
			cout << " "; 
		}
	}

	// Deletes a node from the tree
	// @param node - the node that's being deleted
	inline void DeleteNode(void *entry) {
		DeleteEntry((SNode *)entry); 
	}

	// Deletes a node from the tree
	// @param node - the node that's being deleted
	bool DeleteNode(const X &node) {

		m_curr_node = GetEntry(node); 
		if(m_curr_node == NULL) {
			return false; 
		}

		DeleteEntry(m_curr_node); 
		return true; 
	}

	// Returns the size of the tree
	inline int Size() {
		return m_node_num; 
	}

	// Returns the node value for a given position
	// in the red black tree
	// @param position - the position that points
	//  - to the node in the tee
	inline X &NodeValue(void *position) {
		if(position == NULL) {
			throw EIllegalArgumentException("position NULL"); 
		}

		return ((SNode *)position)->node; 
	}

	// Returns a pointer to an equal node attatched to
	// the current node pointed to by position, returns
	// null if there is no equal node
	// @param position - the current position for a 
	//  - given node in the tree
	// @return the same equal node, NULL if non exists 
	inline void *EqualNodeOf(void *position) { 
		if(position == NULL) {
			throw EIllegalArgumentException("position NULL"); 
		}

		return ((SNode *)position)->same_link; 
	}

	// Returns the parent
	inline static bool IsSameNodeAvail(void *p) { 
		return ((SNode *)p)->same_link != NULL; 
	}

	// Returns the parent for a given position
	// @param position - the current position for a 
	//  - given node in the tree
	// @return the parent of a given node, NULL if parent doesn't exists  
	inline void *ParentOf(void *position) { 
		if(position == NULL) {
			throw EIllegalArgumentException("position NULL"); 
		}

		return ((SNode *)position)->parent; 
	}

	// Returns the sibling of a given node, NULL 
	// if no sibling or root node
	// @param position - the current position for a 
	//  - given node in the tree
	void *SiblingOf(void *position) {
		if(position == NULL) {
			throw EIllegalArgumentException("position NULL"); 
		}

		SNode *parent = ((SNode *)position)->parent; 
		if(parent == NULL) {
			return NULL; 
		}

		return parent->left_child == (SNode *)position ? 
			parent->right_child : parent->left_child; 
	}

	// Returns the maximum child associated with a given node
	// @param position - the current position for a 
	//  - given node in the tree
    inline void *MaxChildOf(void *position) {
		return ((SNode *)position)->left_child; 
	}

	// Returns the minimum child associated with a given node
	// @param position - the current position for a 
	//  - given node in the tree
    inline void *MinChildOf(void *position) {
		return ((SNode *)position)->right_child; 
	}

	// Resets the tree
	void Reset() {
		m_node_buffer.Restart(); 
		m_same_node_buffer.Restart(); 
		m_node_num = 0; 
		m_root_node = NULL; 
		m_same_free_node = NULL;
		m_free_node = NULL;
	}

	// Returns the minimum element
	void *GetMax(X &item) {
		if(m_root_node == NULL) {
			throw EItemNotFoundException
			("Red Black Tree is Empty"); 
		}

		m_curr_node = m_root_node; 
		while(m_curr_node->left_child) {
			m_curr_node = m_curr_node->left_child; 
		}
		item = m_curr_node->node; 
		return m_curr_node; 
	}

	// Returns the maximum element
	void *GetMin(X &item) {
		if(m_root_node == NULL) {
			throw EItemNotFoundException
			("Red Black Tree is Empty"); 
		}

		m_curr_node = m_root_node; 
		while(m_curr_node->right_child) {
			m_curr_node = m_curr_node->right_child; 
		}
		item = m_curr_node->node; 
		return m_curr_node; 
	}
	

	// Deletes the minimum element
	bool DeleteMax(void *node = NULL) {

		if(node != NULL) {
			// deletes the node at the given
			// position if one is supplied
			DeleteEntry((SNode *)node); 
			return true; 
		}

		if(m_root_node == NULL) {
			return false; 
		}
		m_curr_node = m_root_node; 
		while(m_curr_node->left_child) {
			m_curr_node = m_curr_node->left_child; 
		}

		DeleteEntry(m_curr_node); 
		return true; 
	}

	// Deletes the maximum element
	bool DeleteMin(void *node = NULL) {

		if(node != NULL) {
			// deletes the node at the given
			// position if one is supplied
			DeleteEntry((SNode *)node); 
			return true; 
		}

		if(m_root_node == NULL) {
			return false; 
		}

		m_curr_node = m_root_node; 
		while(m_curr_node->right_child != NULL) {
			m_curr_node = m_curr_node->right_child; 
		}

		DeleteEntry(m_curr_node); 
		return true; 
	}

	// Reads the binary tree from file
	void ReadRedBlackTreeFromFile(const char str[]) {
		CHDFSFile file; 
		file.OpenReadFile(CUtility::
			ExtendString(str, ".red_black_tree")); 

		ReadRedBlackTreeFromFile(file); 
	}

	// Writes the binary tree to file
	void WriteRedBlackTreeToFile(const char str[]) {
		if(Size() <= 0)return; 

		CHDFSFile file; 
		file.OpenWriteFile(CUtility::
			ExtendString(str, ".red_black_tree")); 

		WriteRedBlackTreeToFile(file); 
	}

	// Reads the binary tree from file
	void ReadRedBlackTreeFromFile(CHDFSFile &file) {

		Reset(); 
		file.ReadCompObject(m_node_num); 
		if(m_node_num <= 0)return; 

		uChar back_track = 0; 
		CreateNullNode(m_root_node);
		m_curr_node = m_root_node; 


		file.ReadCompObject(back_track); 	
		file.ReadCompObject(m_curr_node->node); 
		file.ReadCompObject(m_curr_node->color); 

		// reads the binary tree structure
		CArrayList<SNode *> prev_node(100); 
		prev_node.PushBack(m_curr_node); 
		X curr_element; 

		for(int i=1; i<m_node_num; i++) {
			// reads each node in from memory
			file.ReadCompObject(back_track); 	
			file.ReadCompObject(curr_element); 

			// checks for the same link
			if(!m_compare(curr_element, m_curr_node->node)) {
				SSameNode *prev_node = m_curr_node->same_link; 
				m_curr_node->same_link = CreateNewSameNode(); 
				(m_curr_node->same_link)->node = curr_element;
				(m_curr_node->same_link)->same_link = prev_node; 
				continue; 
			}

			if(back_track) {
				prev_node.Resize(prev_node.Size() - back_track); 
				m_curr_node = prev_node.LastElement(); 
			}

			if(m_compare(curr_element, m_curr_node->node) < 0) {
				// creates a new node to store the value
				m_curr_node->right_child = CreateNewNode(); 
				m_curr_node = m_curr_node->right_child; 
			} else {
				// creates a new node to store the value
				m_curr_node->left_child = CreateNewNode(); 
				m_curr_node = m_curr_node->left_child; 
			}

			m_curr_node->parent = prev_node.LastElement(); 
			m_curr_node->same_link = NULL; 
			m_curr_node->left_child = NULL; 
			m_curr_node->right_child = NULL; 
			m_curr_node->node = curr_element; 
			// records the current node in the path
			prev_node.PushBack(m_curr_node); 
			file.ReadCompObject(m_curr_node->color); 
		}
	}

	// Writes the binary tree to file
	void WriteRedBlackTreeToFile(CHDFSFile &file) {

		file.WriteCompObject(m_node_num); 
		if(Size() <= 0)return; 

		// now writes the binary tree structure
		CArrayList<SNode *> prev_node(100); 
		CArrayList<char> direction(100); 
		direction.PushBack(0); 

		uChar back_track = 0; 
		m_curr_node = m_root_node; 
		
		while(true) {
			if(direction.LastElement() <= 0) {
				direction.LastElement() = 1; 
				file.WriteCompObject(back_track); 	// writes the current backtrack
				file.WriteCompObject(m_curr_node->node); 
				file.WriteCompObject(m_curr_node->color); 
				back_track = 0; 

				SSameNode *next_link = m_curr_node->same_link; 
				// cycles through the same branch checksum
				while(next_link) {
					file.WriteCompObject(back_track); 	// writes the current backtrack
					file.WriteCompObject(next_link->node); 
					// don't need to record the color 
					// for a same link since it's the same as the base
					next_link = next_link->same_link; 
				}
			}

			if(m_curr_node->right_child && (direction.LastElement() < 2)) {
				prev_node.PushBack(m_curr_node); 
				direction.LastElement() = 2; 
				direction.PushBack(0); 
				m_curr_node = m_curr_node->right_child; 
			} else if(m_curr_node->left_child && (direction.LastElement() < 3)) {
				prev_node.PushBack(m_curr_node); 
				direction.LastElement() = 3; 
				direction.PushBack(0); 
				m_curr_node = m_curr_node->left_child; 
			} else {
				back_track++; 
				if(prev_node.Size() <= 0)break; 	// jumps to the next division
				m_curr_node = prev_node.PopBack(); 
				direction.PopBack(); 
			}
		}
	}
}; 

// This class works as a priority queue with the added feature
// of dropping lowest priority elements for higher priority elements
// to ensure a fixed size queue. It is based on a red black tree
// to maintain sorted order preventing the tree from becoming 
// unbalanced and degrading performance. The minimum element is
// found by continuously searching down the left branch of the tree.
template <class X> class CLimitedPQ : public CRedBlackTree<X> {
	// Stores the maximum queue size
	int m_max_queue_size; 
	// Stores the last deleted item
	X m_last_deleted_item; 
	// This stores a pointer to the last
	// minimum deleted item
	X *m_last_deleted_ptr;

	// Stores the value of the current minimum item.
	// This is used to speed up the adding process by 
	// eleminating the need for repeating min search 
	// in the red black tree.
	X m_curr_min_item; 
	// Stores true if the current min element is set
	bool m_curr_min_set; 

	// This is used as a test function for comparisons
	static int TestCompareNodes(const int &arg1, const int &arg2) {
		if(arg1 < arg2) {
			return -1;	
        }	

		if(arg1 > arg2) {
			return 1;	
        }

		return 0;
    }

public:

	CLimitedPQ() {
		m_max_queue_size = 0;
	}

	CLimitedPQ(int max_queue_size, 
		int (*compare)(const X &item1, const X &item2)) {

		m_max_queue_size = 0;
		Initialize(max_queue_size, compare); 
	}

	// Initializes the limited PQ, this means setting the limit
	// size of the queue and the comparison function
	// @param max_queue_size - this is the limit size of the queue
	// @param compare - this is the comparison function used to 
	//                - compare different items in the queue
	void Initialize(int max_queue_size, 
		int (*compare)(const X &item1, const X &item2)) {

		m_last_deleted_ptr = NULL;
		m_max_queue_size = max_queue_size; 

		CRedBlackTree<X>::Initialize(m_max_queue_size, compare); 
		m_curr_min_set = false; 
	}

	// Returns the last deleted item
	// when adding a new item that replaced
	// the lowest priority item in the queue
	inline X *LastDeletedItem() {
		return m_last_deleted_ptr;
	}

	// Returns the current minimum item
	inline X *CurrMinItem() {
		if(m_curr_min_set == false) {
			return NULL;
		}

		return &m_curr_min_item;
	}

	// Returns the maximum size of the queue
	inline int OverflowSize() {
		return m_max_queue_size; 
	}

	// This sets the maximum overflow size
	void SetOverflowSize(int size) {

		m_curr_min_set = false;
		m_max_queue_size = size;
		if(size >= m_max_queue_size) {
			return;
		}

		for(int i=size; i<m_max_queue_size; i++) {
			PopItem();
		}
	}

	// Adds an item to the priority queue
	// @param item - this is the object being added to the 
	//             - limited priority queue
	// @return a pointer to the added item
	X *AddItem(const X &item) {
		if(this->Size() < m_max_queue_size) {
		    m_curr_min_set = false; 
			m_last_deleted_ptr = NULL;
			// just adds the item if there is room
			return AddNode(item); 
		}

		// only looks for the minimum element ptr if item
		// is greater than the current minimum value
		if(m_curr_min_set && m_compare(m_curr_min_item, item) >= 0) {
			m_last_deleted_item = item;
			m_last_deleted_ptr = &m_last_deleted_item;
			return NULL; 
		}
		
		X min; 
		m_curr_min_set = true; 
		// gets a Buffer to the minimum element in the tree
		void *min_node = GetMin(min); 
		m_curr_min_item = min; 

		// checks if the minimum element has lower priority
		if(m_compare(min, item) < 0) {
			// stores the last deleted item
			m_last_deleted_item = min; 
			m_last_deleted_ptr = &m_last_deleted_item;

			// assigns the next minimum item 
			if(!CRedBlackTree<X>::IsSameNodeAvail(min_node)) {
				// only reassign the minimum if there
				// isn't a replacement for the current minimum
				if(CRedBlackTree<X>::ParentOf(min_node)) {
					m_curr_min_item = CRedBlackTree<X>::NodeValue(CRedBlackTree<X>::ParentOf(min_node)); 
				} else {
					m_curr_min_item = item;
				}
			}
	
			// replaces the minimum priority element with
			// the new node - by first deleting the min element node
			CRedBlackTree<X>::DeleteMin(min_node); 

			return CRedBlackTree<X>::AddNode(item); 
		}
		
		// this item was not stored due to its low priority
		m_last_deleted_item = item;
		m_last_deleted_ptr = &m_last_deleted_item;

		return NULL; 
	}

	// This deletes a node from the priority quue. The minimum needs 
	// to be recalulated when a node is added back in.
	// @return true if the item was successfully deleted, false otherwise
	bool DeleteItem(const X &item) {
		
		if(!CRedBlackTree<X>::DeleteNode(item)) {
			return false;
		}

		m_curr_min_set = false;

		return true;
	}

	// a testing framework
	void TestLimitedPQ() {

	    Initialize(10090, TestCompareNodes);
		CMemoryChunk<int> element(999999, 0); 
		
		for(int i=0; i<999999; i++) {
			element[i] = (rand() % 100); 
			AddItem(element[i]); 
		}

		CSort<int> sort(element.OverflowSize(), TestCompareNodes);
		sort.HybridSort(element.Buffer());
		for(int i=0; i<OverflowSize(); i++) {
			if(element[i] != PopItem()) {
				cout<<"Priority Mismatch";getchar();
			}
		}
	}

	// This resets the queue
	inline void Reset() {
		m_curr_min_set = false;
		m_last_deleted_ptr = NULL;
		CRedBlackTree<X>::Reset();
	}

	// Removes the highest ranking item
	// from the priority queue
	X PopItem() {
		if(this->Size() <= 0) {
			throw EUnderflowException
			("No Elements In Limited Priority Queue"); 
		}

		X max; 
		// gets a Buffer to the maximum element in the tree
		void *node = this->GetMax(max); 
		m_last_deleted_ptr = NULL;
		m_curr_min_set = false; 

		this->DeleteMax(node); 

		return max; 
	}

	// Removes a limited priorityq queue file
	inline void RemoveLimitedPriorityQueue(const char str[]) {
		CHDFSFile::Remove(CUtility::ExtendString(str, ".limit_priority_queue")); 
	}

	// Reads a priority queue from file
	void ReadLimitedPQFromFile(const char str[]) {
		CHDFSFile file; 
		file.OpenReadFile(CUtility::
			ExtendString(str, ".limit_priority_queue")); 

		ReadLimitedPQFromFile(file); 
	}

	// Writes a priority queue to file
	void WriteLimitedPQToFile(const char str[]) {
		if(this->Size() == 0) {
			return; 
		}

		CHDFSFile file; 
		file.OpenWriteFile(CUtility::
			ExtendString(str, ".limit_priority_queue")); 

		WriteLimitedPQToFile(file); 
	}

	// Reads a priority queue from file
	void ReadLimitedPQFromFile(CHDFSFile &file) {
		file.ReadCompObject(m_max_queue_size); 
		this->ReadRedBlackTreeFromFile(file); 
		m_curr_min_set = false; 
		m_last_deleted_ptr = NULL;
	}

	// Writes a priority queue to file
	void WriteLimitedPQToFile(CHDFSFile &file) {
		file.WriteCompObject(m_max_queue_size); 
		this->WriteRedBlackTreeToFile(file); 
	}
}; 

// This is a standard FIFO queue, it has no finite size
template <class X> class CQueue {

	// This stores one of the items in the queue
	struct SItem {
		// This is one of the items
		X item;
		// This stores a ptr to the next item
		SItem *next_ptr;
	};
	
	// This stores the set of items
	CLinkedBuffer<SItem> m_node_buff;
	// This stores the head ptr
	SItem *m_head_ptr;
	// This stores the tail ptr
	SItem *m_tail_ptr;
	// This stores the set of free items
	SItem *m_free_ptr;
	// This returns the number of items in the queue
	int m_size;

public:

	// Starts the queue
	CQueue() {
		Initialize();
	}

	// Starts the queue
	void Initialize() {
		m_node_buff.Initialize();
		m_free_ptr = NULL;
		m_head_ptr = NULL;
		m_tail_ptr = NULL;
		m_size = 0;
	}

	// This resets the queue
	void Reset() {
		m_node_buff.Restart();
		m_free_ptr = NULL;
		m_head_ptr = NULL;
		m_tail_ptr = NULL;
		m_size = 0;
	}

	// This returns the first element in the list without removing it
	X PeekFirst() {
		if(m_size == 0) {
			throw EUnderflowException("No More Nodes");
		}

		return m_head_ptr->item;
	}

	// This returns the last element in the list without removing it
	X PeekLast() {
		if(m_size == 0) {
			throw EUnderflowException("No More Nodes");
		}

		return m_tail_ptr->item;
	}

	// Adds an item to the queue
	void AddItem(const X &item) {

		SItem *ptr = m_free_ptr;
		if(m_free_ptr != NULL) {
			ptr = m_free_ptr;
			m_free_ptr = m_free_ptr->next_ptr;
		} else {
			ptr = m_node_buff.ExtendSize(1);
		}

		ptr->item = item;
		m_size++;

		if(m_head_ptr == NULL) {
			m_tail_ptr = ptr;
			m_head_ptr = ptr;
		} else {
			m_tail_ptr->next_ptr = ptr;
			m_tail_ptr = m_tail_ptr->next_ptr;
		}

		m_tail_ptr->next_ptr = NULL;
	}

	// Removes the first item from the queue
	X PopItem() {
		if(m_size == 0) {
			throw EUnderflowException("No More Nodes");
		}

		SItem *ptr = m_head_ptr;
		m_head_ptr = m_head_ptr->next_ptr;
		m_size--;

		SItem *prev_ptr = m_free_ptr;
		m_free_ptr = ptr;
		m_free_ptr->next_ptr = prev_ptr;

		return ptr->item;
	}

	// Returns the size of the queue
	inline int Size() {
		return m_size;
	}

	// This is just a test framework
	void TestQueue() {

		CArray<int> buff(100);

		for(int i=0; i<1000; i++) {
			if(buff.Size() == 0) {
				buff.PushBack(rand());
				AddItem(buff.LastElement());
			} else if(buff.Size() == buff.OverflowSize()) {
				if(PopItem() != buff[0]) {
					cout<<"error";getchar();
				}
				buff.DeleteElement(0);
			} else if(rand() % 2) {
				int out = PopItem();
				if(out != buff[0]) {
					cout<<"error";getchar();
				}
				buff.DeleteElement(0);
			} else {
				buff.PushBack(rand());
				AddItem(buff.LastElement());
			}
		}
	}
}; 

// This class acts as a cyclic array. In particular it stores the last
// N elements added to the array and remove previous entries outside
// the N size window.
template <class X> class CCyclicArray {

	// This stores the window of N elements
	CMemoryChunk<X> m_buff;
	// This stores the offset in the window
	int m_offset;

public:

	CCyclicArray() {
	}

	// This initializes the array to some window size
	CCyclicArray(int window_size) {
		Initialize(window_size);
	}

	// This initializes the array to some window size
	void Initialize(int window_size) {
		m_buff.AllocateMemory(window_size);
		m_offset = 0;
	}

	// This adds an element to the end of the array
	void PushBack(X element) {

		int id = m_offset % m_buff.OverflowSize();
		m_buff[id] = element;
		if(++m_offset >= (m_buff.OverflowSize() << 1)) {
			m_offset = m_buff.OverflowSize();
		}
	}

	// This resets the buffer
	inline void Reset() {
		m_offset = 0;
	}

	// This returns the number of elements in the buffer
	inline int Size() {
		return min(m_offset, m_buff.OverflowSize());
	}

	// This returns the overflow size of the buffer
	inline int OverflowSize() {
		return m_buff.OverflowSize();
	}

	// This returns the last element in the buffer
	inline X &LastElement() {
		return operator[](Size() - 1);
	}

	// allocates a certain block of memory
	// and returns a pointer to it
	inline X &operator[](int index) {
		if(index >= Size() || index < 0) {
			throw EIndexOutOfBoundsException
			("memory chunk overflow "); 
		}

		if(Size() < OverflowSize()) {
			return m_buff[index]; 	
		}

		return m_buff[(index + m_offset) % m_buff.OverflowSize()]; 	
	}

	// This is just a test framework
	void TestCyclicArray() {

		CMemoryChunk<int> buff(1024);
		for(int i=0; i<buff.OverflowSize(); i++) {
			buff[i] = rand();
		}

		Initialize(5);
		for(int i=0; i<buff.OverflowSize(); i++) {
			PushBack(buff[i]);

			int offset = 0;
			for(int j=max(0, i-OverflowSize()+1); j<i+1; j++) {
				if(operator[](offset++) != buff[j]) {
					cout<<"error "<<i<<" "<<buff[j]<<" "<<j;getchar();
				}
			}
		}
	}
}; 

// This is a general purpose class for handling
// operations on a square matrix. LAPLACK 
// routines are used in mulitplication.
template <class X> class CMatrix {

	// used to store the matrix
	CMemoryChunk<X> m_matrix; 
	// stores the column size of the matrix
	int m_column_size;
	// stores the row size of the matrix
	int m_row_size;

public:

	CMatrix() {
	}

	CMatrix(CMatrix<X> &mat) {
		MakeMatrixEqualTo(mat);
	}

	CMatrix(int column_size, int row_size) {
		Initialize(column_size, row_size);
	}

	// allocates memory for the matrix
	void Initialize(int column_size, int row_size) {
		m_matrix.AllocateMemory(column_size * row_size); 
		m_column_size = column_size;
		m_row_size = row_size;
	}

	// returns a reference to a given element
	// @param i - row index
	// @param j - column index
	inline X &GetElement(int i, int j) {
		return m_matrix[(j * ColumnSize()) + i]; 
	}

	// sets all elements in the matrix to zero
	inline void SetToZero() {
		m_matrix.InitializeMemoryChunk(0);
	}

	// sets a given column in the matrix
	// @param vector - the buffer that the matrix column
	// @param j - the column index
	inline void SetColumn(X vector[], int j) {
		memcpy(m_matrix.Buffer() + (j * RowSize()), 
			vector, ColumnSize());
	}

	// returns the size of each column
	inline int ColumnSize() {
		return m_column_size; 
	}

	// returns the size of each row
	inline int RowSize() {
		return m_row_size; 
	}

	// copies a row of the matrix into a buffer
	// @param row_num - row index
	// @param buffer - used to store the row
	inline void GetRow(int row_num, X buffer[]) {
		for(int i=0; i<RowSize(); i++)
			buffer[i] = GetElement(row_num, i); 
	}

	// copies a column of the matrix into a buffer
	// @param column_num - column index
	// @param buffer - used to store the row
	inline void GetColumn(int column_num, X buffer[]) {
		for(int i=0; i<ColumnSize(); i++) {
			buffer[i] = GetElement(i, column_num); 
		}
	}

	// returns a ptr to the column in the matrix
	// @param column - column index
	inline X *GetColumn(int column) {
		return m_matrix.Buffer() + (RowSize() * column);
	}

	// returns the maximum element in the matrix
	inline X MaximumElement() {

		X max = 0;
		for(int i=0; i<ColumnSize(); i++) {
			for(int j=0; j<RowSize(); j++) {
				if(GetElement(i, j) > max) {
					max = GetElement(i, j);
				}
			}
		}

		return max;
	}

	// swaps to rows in a matrix
	// @para a, b - the two row indices
	void SwapRows(int a, int b) {
		CMemoryChunk<X> temp(RowSize()); 
		for(int i=0; i<RowSize(); i++)
			temp[i] = GetElement(a, i); 

		for(int i=0; i<RowSize(); i++)
			GetElement(a, i) = GetElement(b, i); 

		for(int i=0; i<RowSize(); i++)
			GetElement(b, i) = temp[i]; 
	}

	// multiplies all the entries in the matrix by a constant
	// @param constant - the constant multiplier
	void MultiplyByConstant(X constant) {
		for(int a = 0; a < ColumnSize(); a++) {
			for(int b = 0; b < RowSize(); b++)
				GetElement(a, b) *= constant; 
		}
	}

	// This adds a matrix to the current matrix
	void MatrixAddition(CMatrix<X> &mat) {

		for(int i=0; i<ColumnSize(); i++) {
			for(int j=0; j<RowSize(); j++) {
				GetElement(i, j) += mat.GetElement(i, j);
			}
		}
	}

	// sets a row in the matrix
	// @param vector - the buffer that holds the row
	// @param i - the row index
	inline void SetRow(X vector[], int i) {
		for(int i=0; i<RowSize(); i++) {
			GetElement(i, i) = vector[i]; 
		}
	}

	// this is just a copy operation
	// @param matrix - the matrix that's being copied
	void MakeMatrixEqualTo(CMatrix<X> &matrix) {

		Initialize(matrix.ColumnSize(), matrix.RowSize());
		for(int i=0; i<RowSize(); i++) {
			memcpy(GetColumn(i), matrix.GetColumn(i), sizeof(X) * RowSize());
		}
	}

	// this is just a copy operation
	// @param matrix - the matrix that's being copied
	inline void MakeMatrixEqualTo(CMemoryChunk<X> &matrix) {
		m_matrix.MakeMemoryChunkEqualTo(matrix);
	}

	// this normalizes each row of the matrix
	void NormalizeRows() {
		for(int i=0; i<ColumnSize(); i++) {
			X sum = 0;
			for(int j=0; j<RowSize(); j++) {
				sum += GetElement(i, j);
			}

			for(int j=0; j<RowSize(); j++) {
				GetElement(i, j) /= sum;
			}
		}
	}

	// this normalizes each column of the matrix
	inline void NormalizeColumns() {
		for(int i=0; i<RowSize(); i++) {
			CMath::NormalizeVector(GetColumn(i), RowSize());
		}
	}

	// calculates the sum of all the elements in the matrix
	X SumAllElements() {
		X sum = 0;
		for(int j=0; j<ColumnSize(); j++) {
			for(int k=0; k<RowSize(); k++) {
				sum += GetElement(j, k);
			}
		}

		return sum;
	}

	// multiplies two matrices and stores the results
	// in the left matrix (matrices must be permutable)
	// @param mat_left - the matrix on the left
	// @param mat_right - the matrix on the right
	static void MultiplyMatrices(CMatrix<float> &mat_left, CMatrix<float> &mat_right) {

		CMatrix<X> temp(mat_left.ColumnSize(), mat_right.RowSize());
		MultiplyMatrices(mat_left, mat_right, temp);
		mat_left.MakeMatrixEqualTo(temp);
	}

	// this just squares the matrix
	void SquareMatrix() {
		CMatrix<float> temp_mat(ColumnSize(), RowSize());
		MultiplyMatrices(*this, *this, temp_mat);
		MakeMatrixEqualTo(temp_mat);
	}

	// multiplies the matrix by the vector
	// @param vector - stores the vector being multiplied
	void MultiplyByVector(X vector[]) {
		CMemoryChunk<X> temp(RowSize()); 
		for(int i=0; i<RowSize(); i++)
			temp[i] = 0; 

		for(int a = 0; a < ColumnSize(); a++) {
			for(int b = 0; b < RowSize(); b++)
				temp[a] += GetElement(a, b) * vector[b]; 
		}

		for(int i=0; i<ColumnSize(); i++)
			vector[i] = temp[i]; 
	}

	// prints out the matrix
	void ShowMatrix() {
		for(int a = 0; a < ColumnSize(); a++) {
			cout <<endl; 
			for(int b = 0; b < RowSize(); b++)
				cout << GetElement(a, b) << " "; 
		}
	}

	// This writes a matrix to file
	void WriteMatrixToFile(CHDFSFile &file) {

		file.WriteCompObject(ColumnSize());
		file.WriteCompObject(RowSize());
		for(int i=0; i<RowSize(); i++) {
			file.WriteCompObject(GetColumn(i), ColumnSize());
		}
	}

	// This writes a matrix to file
	void WriteMatrixToFile(const char str[]) {

		CHDFSFile mat_file;
		mat_file.OpenWriteFile(CUtility::ExtendString(str, ".mat"));

		WriteMatrixToFile(mat_file);
	}

	// This reads a matrix from file
	void ReadMatrixFromFile(CHDFSFile &file) {

		int column_size, row_size;
		file.ReadCompObject(column_size);
		file.ReadCompObject(row_size);
		Initialize(column_size, row_size);

		for(int i=0; i<RowSize(); i++) {
			file.ReadCompObject(GetColumn(i), ColumnSize());
		}
	}
}; 

// This is a general purpose class for handling
// operations on a square matrix. LAPLACK 
// routines are used in mulitplication.
template <class X> class CSquareMatrix {

	// used to store the matrix
	CMemoryChunk<CMemoryChunk<X> > m_matrix; 
	// stores the size of the matrix
	int m_size; 

public:

	CSquareMatrix() {
	}

	CSquareMatrix(CSquareMatrix<X> &copy) {
		MakeSquareMatrixEqualTo(copy);
	}

	CSquareMatrix(int size) {
		m_matrix.AllocateMemory(size); 
		for(int i=0; i<size; i++)
			m_matrix[i].AllocateMemory(size); 
		m_size = size; 
	}

	void Initialize(int size) {
		m_matrix.AllocateMemory(size); 
		for(int i=0; i<size; i++)
			m_matrix[i].AllocateMemory(size); 
		m_size = size; 
	}

	inline X &GetElement(int i, int j) {
		return m_matrix[i][j]; 
	}

	inline void SetColumn(X *vector, int j) {
		for(int i=0; i<m_size; i++) {
			m_matrix[i][j] = vector[i]; 
		}
	}

	int Size() {
		return m_size; 
	}

	template <class Y> inline void GetRow(int row_num, Y *buffer) {
		for(int i=0; i<m_size; i++)
			buffer[i] = m_matrix[row_num][i]; 
	}

	inline X *GetRow(int row_num) {
		return m_matrix[row_num].Buffer();
	}

	template <class Y> inline void GetColumn(int row_num, Y *buffer) {
		
		for(int i=0; i<m_size; i++) {
			buffer[i] = m_matrix[i][row_num]; 
		}
	}

	inline void SwapRows(int a, int b) {
		CMemoryChunk<X> temp(m_size); 
		for(int i=0; i<m_size; i++)
			temp[i] = m_matrix[a][i]; 

		for(int i=0; i<m_size; i++)
			m_matrix[a][i] = m_matrix[b][i]; 

		for(int i=0; i<m_size; i++)
			m_matrix[b][i] = temp[i]; 
	}

	// calculates the sum of all the elements in the matrix
	X SumAllElements() {
		X sum = 0;
		for(int j=0; j<Size(); j++) {
			for(int k=0; k<Size(); k++) {
				sum += GetElement(j, k);
			}
		}

		return sum;
	}

	inline void MultiplyByConstant(X constant) {
		for(int a = 0; a < m_size; a++) {
			for(int b = 0; b < m_size; b++)
				m_matrix[a][b] *= constant; 
		}
	}

	inline void AddIdentityWithConstant(X constant) {
		for(int i=0; i<m_size; i++)
			m_matrix[i][i] += constant; 
	}

	inline void SetToIdentity() {
		for(int a = 0; a < m_size; a++) {
			for(int b = 0; b < m_size; b++) {
				if(a == b)m_matrix[a][b] = 1; 
				else m_matrix[a][b] = 0; 
			}
		}
	}

	inline void SetRow(X *vector, int i) {
		for(int i=0; i<m_size; i++) {
			m_matrix[i][i] = vector[i]; 
		}
	}

	inline void MakeSquareMatrixEqualTo(CSquareMatrix<X> &matrix) {
		Initialize(matrix.Size());
		for(int a = 0; a < m_size; a++) {
			for(int b = 0; b < m_size; b++)
				m_matrix[a][b] = matrix.GetElement(a, b); 
		}
	}

	void MultiplyBy(CSquareMatrix<X> *matrix) {
		CSquareMatrix<X> temp(m_size); 
		for(int a = 0; a < m_size; a++) {
			for(int b = 0; b < m_size; b++) {
				X sum = 0; 
				for(int c=0; c<m_size; c++) {
					sum += m_matrix[a][c] * matrix->GetElement(c, b); 
				}

				temp.GetElement(a, b) = sum; 
			}
		}
		MakeSquareMatrixEqualTo(temp);
	}

	void SetToZero() {
		for(int a = 0; a < m_size; a++) {
			for(int b = 0; b < m_size; b++) {
				m_matrix[a][b] = 0;
			}
		}
	}

	void MultiplyByTransposeOf(CSquareMatrix<X> *matrix) {
		CSquareMatrix<X> temp(m_size); 
		for(int a = 0; a < m_size; a++) {
			for(int b = 0; b < m_size; b++) {
				X sum = 0; 
				for(int c=0; c<m_size; c++)
					sum += m_matrix[a][c] * matrix->GetElement(b, c); 

				temp.GetElement(a, b) = sum; 
			}
		}
		for(int a = 0; a < m_size; a++) {
			for(int b = 0; b < m_size; b++)
				m_matrix[a][b] = temp.GetElement(a, b); 
		}
	}

	inline void TransposeMatrix() {
		for(int a = 0; a < m_size; a++) {
			for(int b = a; b < m_size; b++) {
				X temp = m_matrix[a][b]; 
				m_matrix[a][b] = m_matrix[b][a]; 
				m_matrix[b][a] = temp; 
			}
		}
	}

	void MultiplyByVector(X *vector) {
		CMemoryChunk<X> temp(m_size); 
		for(int i=0; i<m_size; i++)
			temp[i] = 0; 

		for(int a = 0; a < m_size; a++) {
			for(int b = 0; b < m_size; b++)
				temp[a] += m_matrix[a][b] * vector[b]; 
		}

		for(int i=0; i<m_size; i++)
			vector[i] = temp[i]; 
	}

	void WriteSquareMatrixToFile(CHDFSFile &file) {
		file.WriteCompObject(m_size);
		for(int i=0; i<m_size; i++) {
			file.WriteCompObject(GetRow(i), m_size);
		}
	}

	void ReadSqureMatrixFromFile(CHDFSFile &file) {
		file.ReadCompObject(m_size);
		Initialize(m_size);
		for(int i=0; i<m_size; i++) {
			file.WriteCompObject(GetRow(i), m_size);
		}
	}

	void ShowMatrix() {
		for(int a = 0; a < m_size; a++) {
			cout << endl; 
			for(int b = 0; b < m_size; b++)
				cout << m_matrix[a][b] << " "; 
		}
	}
}; 
