

#ifdef OS_WINDOWS
#include <windows.h> 
#include <process.h> // needed for _beginthread() 

#define ZLIB_DLL
#define _WINDOWS
#include "zlib.h"

#endif

#define _FILE_OFFSET_BITS 64

#include <stdio.h> 
#include <cstdlib> 								
#include <math.h> 
#include <iostream> 
#include <cstdlib> 						
#include <fstream> 
#include <cassert>
#include <cctype> 
#include <iomanip> 
#include <vector> 
#include <list> 
#include <map> 
#include <string> 
#include <math.h> 
#include <assert.h> 
#include <ctime> 
#include <cstring>
#include "zlib.h"

using namespace std; 

#ifdef OS_WINDOWS 

#include <strsafe.h>

// This a simple mutex semaphore that can be 
// either locked or unlocked.
class CMutex {
	CRITICAL_SECTION _critSection;

public:

	CMutex () { InitializeCriticalSection (& _critSection);}
	~CMutex () { DeleteCriticalSection (& _critSection);}

	void Acquire() {
		EnterCriticalSection (& _critSection);
	}
	void Release() {
		LeaveCriticalSection (& _critSection);
	}
};

#else 

// This a simple mutex semaphore that can be 
// either locked or unlocked.
class CMutex {
	pthread_mutex_t mp;

public:

	CMutex () { pthread_mutex_init(&mp, NULL);}
	~CMutex () { pthread_mutex_destroy(&mp);}

	void Acquire() {
		pthread_mutex_lock(&mp);
	}
	void Release() {
		pthread_mutex_unlock(&mp);
	}
};

#endif
