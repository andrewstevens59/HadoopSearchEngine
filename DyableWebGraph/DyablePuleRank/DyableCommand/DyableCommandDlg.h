// DyableCommandDlg.h : header file
//

#pragma once

#define LOCAL_NETWORK "127.0.0.1"


// CDyableCommandDlg dialog
class CDyableCommandDlg : public CDialog
{
	
	CSocket m_listen_socket;
// Construction
public:

	CDyableCommandDlg(CWnd* pParent = NULL);	// standard constructor

// Dialog Data
	enum { IDD = IDD_DYABLECOMMAND_DIALOG };

	protected:
	virtual void DoDataExchange(CDataExchange* pDX);	// DDX/DDV support

	// creates a given process
	// @param process_name - the name of the process to start
	// @param argc - the number of command line arguments
	// @param process_num - the number of process to create
	// @param argv - a ptr to the command line arguments
	HANDLE CreateDyableProcess(char process_name[], 
		int argc, char *argv[], long process_num = 1) {

		long length = strlen(process_name);
		long start = length;
		while(process_name[--start] != '\\');
		char command_line[1000];
		strcpy(command_line, process_name + start);
		strcat(command_line, " ");

		for(long i=0;i<argc;i++) {
			strcat(command_line, argv[i]);
			strcat(command_line, " ");
		}

		return CreateDyableProcess(process_name, command_line, process_num);
	}

	// creates a given process
	// @param process_name - the name of the process to start
	// @param command_str - the command line args separated by spaces
	// @param process_num - the number of process to create
	HANDLE CreateDyableProcess(char process_name[],
		char command_str[], long process_num = 1) {

		STARTUPINFO StartInfo;
		PROCESS_INFORMATION ProcessInfo;

		ZeroMemory(&StartInfo, sizeof(StartInfo));
		StartInfo.cb = sizeof(StartInfo);

		char temp_directory[1000];
		char temp_process_name[1000];
		strcpy(temp_process_name, process_name);
		long process_name_length = strlen(process_name);
		while(process_name[--process_name_length] != '\\');
		temp_process_name[process_name_length + 1] = '\0';

		strcpy(temp_directory, "d:\\MyDocuments\\Callum\\VisualStudioProjects\\Xitami "
			"Web Server\\xitami-25\\app\\cgi-bin\\DyableCollection\\DyableCommand\\");
		strcat(temp_directory, temp_process_name);
		
		for(long i=0;i<process_num;i++) {
			if(!CreateProcess(process_name, command_str, NULL, NULL, FALSE,
				HIGH_PRIORITY_CLASS | CREATE_NEW_CONSOLE, NULL, temp_directory, 
				&StartInfo, &ProcessInfo))return false;			
		}

		return ProcessInfo.hProcess;
	}


// Implementation
protected:
	HICON m_hIcon;

	// Generated message map functions
	virtual BOOL OnInitDialog();
	afx_msg void OnSysCommand(UINT nID, LPARAM lParam);
	afx_msg void OnPaint();
	afx_msg HCURSOR OnQueryDragIcon();
	DECLARE_MESSAGE_MAP()
public:
	afx_msg void OnBnClickedButton1();
	afx_msg void OnBnClickedButton2();
	afx_msg void OnBnClickedButton3();
	afx_msg void OnBnClickedButton4();
	afx_msg void OnBnClickedButton5();
	afx_msg void OnBnClickedButton6();
	afx_msg void OnBnClickedRadio1();
	afx_msg void PulseRank();
	afx_msg void CompileLexonWordIndexMapping();
};
