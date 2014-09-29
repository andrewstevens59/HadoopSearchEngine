#include "../../ProcessSet.h"

// This class is responsible for handling the compression and inflation
// of document blocks. It also is responsible for spawning each document
// parser that is used to compile each document that appears in the corpus.
class CDocumentSet {

	// This is used to spawn each document parser
	CProcessSet m_process_set;

	// This stores the set of document blocks	
	CArrayList<char> m_doc_buff;
	// This stores the offset into the doucment buffer
	CArrayList<int> m_doc_offset;
	// This is a predicate indicating whether a file is compressed or not
	CArrayList<bool> m_is_compressed;
	// This is a predicate indicating bad blocks that should
	// not been parsed because was not decompressed/uncompressed successful
	CArrayList<bool> m_is_bad_block;

	// This stores the set of parsed documents
	CArrayList<char> m_parse_buff;
	// This stores the set of parsed documents offset
	CArrayList<int> m_parse_offset;
	
	// This is the maximum number of data blocks 
	// that can be accessed at any one time
	int m_max_block_num;
	// This stores the maximum number of sets to parse
	int m_max_set_num;
	// This stores the current set id
	int m_curr_set_id;
	// This stores the set of parsed document blocks
	CTrie m_dir_map;
	// This stores the set of document blocks to be parsed
	CTrie m_parse_map;

	// This loads the set of documents into memory
	void LoadDocumentSet(const char *dir) {

		char temp_buff[1024];
		struct dirent *file_ptr = NULL;
		DIR *dir_ptr = NULL;

		dir_ptr = opendir(CUtility::ExtendString(DFS_ROOT, dir));

		if(dir_ptr == NULL) {
			cout<<"Couldn't Open Directory "<<CUtility::ExtendString(DFS_ROOT, dir);
			return;
		}

		while(file_ptr = readdir(dir_ptr)) {

			if(file_ptr->d_type == DT_DIR) {
				if(file_ptr->d_name[0] != '.') {
					strcpy(temp_buff, CUtility::ExtendString(dir, "/", file_ptr->d_name));
					LoadDocumentSet(temp_buff);
				}

				continue;
			} 

			char *block_dir = CUtility::ExtendString(dir, "/", file_ptr->d_name);
			int len = strlen(block_dir);
			if(CUtility::FindSubFragment(file_ptr->d_name, ".gz") >= 0) {
				len -= 3;
			}

			if(m_dir_map.FindWord(block_dir, len) >= 0) {
				// This file has already been parsed
				continue;
			}

			int id = m_parse_map.AddWord(block_dir, len);
			if(m_parse_map.AskFoundWord()) {
				// This block was added twice
				m_is_bad_block[id] = true;
				continue;
			}
	
			m_is_bad_block.PushBack(false);
			m_doc_offset.PushBack(m_doc_buff.Size());
			m_doc_buff.CopyBufferToArrayList(block_dir);
			m_doc_buff.PushBack('\0');
	
			if(CUtility::FindSubFragment(file_ptr->d_name, ".gz") < 0) {
				m_is_compressed.PushBack(false);
			} else {
				m_is_compressed.PushBack(true);
			}
		}

		closedir(dir_ptr);
	}

	// This deflates a given file
	void CompressFile(int file_id, bool is_compress) {
		
		if(file_id >= m_doc_offset.Size() || file_id >= m_max_set_num) {	
			return;
		}

		if(m_is_bad_block[file_id] == true) {
			return;
		}

		if(m_is_compressed[file_id] == is_compress) {
			return;
		}

		char *dir = m_doc_buff.Buffer() + m_doc_offset[file_id];

		if(is_compress == false) {
			m_process_set.CreateRemoteProcess("/bin/gunzip", 
				CUtility::ExtendString("Index ", DFS_ROOT, dir), 0);
			// remove the .gz extension now that unzipped
			dir[strlen(dir) - 3] = '\0';

		} else {
			m_process_set.CreateRemoteProcess("/bin/gzip",
				CUtility::ExtendString("Index ", DFS_ROOT, dir), 0);
		}

		m_is_compressed[file_id] = is_compress;
	}

	// This spawns one of the parse html processes
	void CreateHTMLParser(int block_id) {
		
		if(block_id >= m_doc_offset.Size() || block_id >= m_max_set_num) {
			return;
		}

		if(m_is_bad_block[block_id] == true) {
			return;
		}

		char *dir = m_doc_buff.Buffer() + m_doc_offset[block_id];
		
		m_dir_map.AddWord(dir, strlen(dir));
		if(m_dir_map.AskFoundWord()) {
			return;
		}

		m_parse_buff.CopyBufferToArrayList(dir, strlen(dir), m_parse_buff.Size());
		m_parse_offset.PushBack(m_parse_buff.Size());
	
		cout<<"Parsing "<<dir<<endl;
		for(int i=0; i<CNodeStat::GetClientNum(); i++) {
			CString arg("Index ");
			arg += i;
			arg += " ";
			arg += CNodeStat::GetClientNum();
			arg += " ";
			arg += m_curr_set_id++;
			arg += " ";
			arg += dir;

			m_process_set.CreateRemoteProcess(CUtility::ExtendString(DFS_ROOT, 
				"DyableParseTREC/DyableParseHTML/DyableParseHTML.exe"), arg.Buffer(), 0);
		}
	}

	// This stores the current parse record
	void WriteCurrentState() {

		CHDFSFile parse_file;
		parse_file.OpenWriteFile("GlobalData/CompiledAttributes/parse_file");
		parse_file.WriteCompObject(m_curr_set_id);
		m_parse_buff.WriteArrayListToFile(parse_file);
		m_parse_offset.WriteArrayListToFile(parse_file);
	}


public:

	CDocumentSet() {

		m_dir_map.Initialize(4);
		m_parse_map.Initialize(4);
		m_is_bad_block.Initialize();
		m_is_compressed.Initialize();
		m_doc_buff.Initialize();
		m_doc_offset.Initialize();

		try {
			CHDFSFile parse_file;
			parse_file.OpenReadFile("GlobalData/CompiledAttributes/parse_file");
			parse_file.ReadCompObject(m_curr_set_id);
			m_parse_buff.ReadArrayListFromFile(parse_file);
			m_parse_offset.ReadArrayListFromFile(parse_file);

			for(int i=0; i<m_parse_offset.Size()-1; i++) {
				char *dir = m_parse_buff.Buffer() + m_parse_offset[i];
				m_dir_map.AddWord(dir, m_parse_offset[i+1] - m_parse_offset[i]);
			}

		} catch(...) {
			m_curr_set_id = 0;
			m_parse_buff.Initialize(4);
			m_parse_offset.Initialize(4);
			m_parse_offset.PushBack(0);
		}

		cout<<"SET ID "<<m_curr_set_id<<" --------------------------"<<endl;
	}

	// This finds the set of bad blocks that were not parsed until completion
	void DisplayBadBlockSet() {

		CHDFSFile docs_parse_file;
		LoadDocumentSet("GlobalData/TRECData");
	
		for(int i=0; i<9000; i++) {
			char *dir = m_doc_buff.Buffer() + m_doc_offset[i];
			try {
				_int64 num;
				docs_parse_file.OpenReadFile(CUtility::ExtendString
					("GlobalData/CompiledAttributes/doc_set_size", i));

				docs_parse_file.ReadObject(num);

				if(num < 100) {
					cout<<i<<endl;
				}
			} catch(...) {
				cout<<i<<" *"<<endl;
			}
		}
	}

	// This is the entry function
	void DocumentSet(int client_num, int max_set_num, int max_block_num) {
		CNodeStat::SetClientNum(client_num);
		m_max_block_num = max_block_num;

		LoadDocumentSet("GlobalData/TRECData");
		m_max_set_num = m_doc_offset.Size();
		cout<<m_max_set_num<<" Left To Be Parsed"<<endl;

		if(max_set_num >= 0) {
			m_max_set_num = min(max_set_num, m_doc_offset.Size());
		}

		for(int i=0; i<m_max_set_num; i += m_max_block_num) {

			for(int j=i; j<i+m_max_block_num; j++) {
				CompressFile(j, false);
			}

			m_process_set.WaitForPendingProcesses();
			cout<<"----------------- Parse Set "<<i<<" Out of "<<m_max_set_num<<endl;
	
			for(int j=i; j<i+m_max_block_num; j++) {
				CreateHTMLParser(j);
			}

			m_process_set.WaitForPendingProcesses();

			for(int j=i; j<i+m_max_block_num; j++) {
				CompressFile(j, true);
			}

			m_process_set.WaitForPendingProcesses();
		}

		WriteCurrentState();

		cout<<"---------------------Finished-------------------------"<<endl;
	}
};

int main(int argc, char *argv[]) {

	//freopen("OUTPUT.txt", "w", stdout);

	//CDocumentSet se;
	//se.DisplayBadBlockSet();

	if(argc < 2)return 0;

	int client_num = atoi(argv[1]);
	int max_block_num = atoi(argv[2]);
	int max_set_num = -1;

	if(argc > 2) {
		max_set_num = atoi(argv[3]);
	}

	CDocumentSet set;
	set.DocumentSet(client_num, max_set_num, max_block_num);

	return 0;
}
