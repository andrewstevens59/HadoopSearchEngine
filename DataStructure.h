#include "IncludeHeadings.h"

typedef unsigned short u_short; 
typedef unsigned char uChar; 

#ifdef OS_WINDOWS

typedef void * thread_return;
typedef HANDLE pthread_t;
#define THREAD_RETURN1 unsigned 
#define THREAD_RETURN2 _stdcall

void WaitForProcess(HANDLE handle, uLong elap_time) {
	WaitForSingleObject(handle, elap_time);
}

void WaitForThread(HANDLE handle, uLong elap_time) {
	WaitForSingleObject(handle, elap_time);
}

#else

#include <sys/types.h>
#include <sys/socket.h>
#include <signal.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netdb.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <pthread.h>
#include <iostream>
#include <errno.h>    
#include <sys/un.h>

#undef MPI_ANY_TAG
#define MPI_ANY_TAG 123
#define SOCKET_ERROR -1
#define INVALID_SOCKET -1
#define abs(value) (value < 0 ? -value : value)
#define min(a, b) (a < b ? a : b)
#define max(a, b) (a > b ? a : b)
#define INFINITE 0xFFFFFFFF
#define THREAD_RETURN1 void
#define THREAD_RETURN2 *

typedef int SOCKET;
typedef void* thread_return;
typedef int HANDLE;
typedef long  _int64;
typedef unsigned int uLong;

void Sleep(int value) {
    usleep(value * 1000);
}

void WaitForProcess(HANDLE handle, uLong elap_time) {
	
	waitpid((int)handle, NULL, 0);
}

void WaitForThread(pthread_t handle, uLong elap_time) {
	pthread_join(handle, NULL);
}

static int closesocket(int fd) {
	return close(fd);
}

static inline void WSACleanup() {
}

void TerminateThread(HANDLE handle, int exit) {
}

static pthread_t _beginthreadex(void *arg1, int arg2, thread_return (*func_ptr)(void *ptr),
	 void *arg3, void *arg4, unsigned int *arg5) {
		 
	 pthread_t t;
	 pthread_create(&t, NULL, func_ptr, arg3);
	 
	 return t;
}


#endif

// Used for storing a 5-byte integer
struct S5Byte {
	char buffer[5]; 

	S5Byte() {
	}

	S5Byte(_int64 value) {
		SetValue(value); 
	}
	
	// This sets the maximum value possible
	void SetMaxValue() {
		for(int i=0; i<5; i++) {
			buffer[i] = 0xFF;
		}
	}
	
	// This returns true if the max value has been set
	inline bool IsMaxValueSet() {

		for(int i=0; i<5; i++) {
			if((uChar)buffer[i] != 0xFF) {
				return false;
			}
		}
		
		return true;
	}

	// copies 5 bytes from one byte string
	// into another byte string
	// @param buff2 - the buffer whose contents is being copied
	// @param buff1 - the buffer where the contents is being copied into
	static inline void Copy5Bytes(char buff1[], char buff2[]) {
		*((uLong *)buff1) = *((uLong *)buff2); 
		buff1[4] = buff2[4]; 
	}

	// copies 5 bytes from one byte string
	// into another byte string
	// @param buff2 - the buffer whose contents is being copied
	// @param buff1 - the buffer where the contents is being copied into
	static inline void Copy5Bytes(char buff1[], const char buff2[]) {
		*(uLong *)buff1 = *(uLong *)buff2; 
		buff1[4] = buff2[4]; 
	}

	// returns the equivalent 64-bit integer
	inline _int64 Value() {
		_int64 temp = 0; 
		memcpy((char *)&temp, Buffer(), 5); 
		return temp; 
	}

	// returns the equivalent 64-bit integer
	static inline _int64 Value(const S5Byte &value) {
		_int64 temp = 0; 
		Copy5Bytes((char *)&temp, value.buffer); 
		return temp; 
	}

	// returns the equivalent 64-bit integer
	static inline _int64 Value(char *buffer) {
		_int64 temp = 0; 
		Copy5Bytes((char *)&temp, buffer); 
		return temp; 
	}

	// returns the equivalent 64-bit integer
	static inline S5Byte Value(const _int64 &value) {
		S5Byte temp; 
		Copy5Bytes(temp.Buffer(), (char *)&value); 
		return temp; 
	}

	// converts from a 64-bit integer to a 5Byte
	template <class X> static inline S5Byte Value(X value) {
		S5Byte temp; 
		Copy5Bytes(temp.Buffer(), (char *)&value); 
		return temp; 
	}

	// returns a buffer to the value
	inline char *Buffer() {
		return buffer; 
	}

	// sets the value of the buffer
	inline void SetValue(_int64 value) {
		Copy5Bytes(Buffer(), (char *)&value); 
	}

	// assignment
	inline S5Byte &operator=(_int64 value) {
		Copy5Bytes(Buffer(), (char *)&value); 
		return *this; 
	}

	// addition
	inline S5Byte operator+(S5Byte value) {
		return Value(Value() + value.Value()); 
	}

	// subtraction
	inline S5Byte operator-(S5Byte value) {
		return Value(Value() - value.Value()); 
	}

	// addition
	template <class X> inline S5Byte operator+(X value) {
		return Value(Value() + value); 
	}

	// subtraction
	template <class X> inline S5Byte operator-(X value) {
		return Value(Value() - value); 
	}

	// addition
	template <class X> inline S5Byte &operator+=(X value) {
		SetValue(Value() + value); 
		return *this;
	}

	// subtraction
	inline S5Byte operator-(S5Byte &value) {
		return Value(Value() - value.Value()); 
	}

	// addition
	inline S5Byte &operator+=(S5Byte &value) {
		SetValue(Value() + value.Value()); 
		return *this;
	}

	// subtraction
	template <class X> inline S5Byte &operator-=(X value) {
		SetValue(Value() - value); 
		return *this;
	}

	// shift left
	template <class X> inline S5Byte &operator<<=(X value) {
		SetValue(Value() << value); 
		return *this;
	}

	// shift right
	template <class X> inline S5Byte &operator>>=(X value) {
		SetValue(Value() >> value); 
		return *this;
	}

	// multiplication
	template <class X> inline S5Byte &operator*=(X value) {
		SetValue(Value() * value); 
		return *this;
	}

	// bit wise or
	template <class X> inline S5Byte &operator|=(X value) {
		SetValue(Value() | value); 
		return *this;
	}

	// multiplication
	inline S5Byte operator*(S5Byte value) {
		return Value(Value() * value.Value()); 
	}

	// division
	inline S5Byte operator/(S5Byte value) {
		return Value(Value() / value.Value()); 
	}

	// shift left
	inline S5Byte operator<<(int value) {
		return Value(Value() << value); 
	}

	// shift right
	inline S5Byte operator>>(int value) {
		return Value(Value() >> value); 
	}

	// modulo
	template <class X> inline S5Byte operator%(X value) {
		return Value(Value() % value); 
	}

	// bitwise and
	inline S5Byte operator&(S5Byte &value) {
		return Value(Value() & value.Value()); 
	}

	// bitwise and
	inline S5Byte operator&(int value) {
		return Value(Value() & value); 
	}

	// bitwise or
	inline S5Byte operator|(S5Byte &value) {
		return Value(Value() | value.Value()); 
	}

	// equality
	inline bool operator==(S5Byte value) {
		return Value() == value.Value(); 
	}

	inline bool AskEquals(S5Byte &value) {
		return Value() == value.Value(); 
	}

	// inequality
	inline bool operator!=(S5Byte &value) {
		return Value() != value.Value(); 
	}

	// inequality
	template <class X>
	inline bool operator!=(X value) {
		return Value() != value; 
	}

	// less than 
	inline bool operator<(S5Byte &value) {
		return Value() < value.Value(); 
	}

	// less than 
	template <class X>
	inline bool operator<(X value) {
		return Value() < value; 
	}

	// greater than
	inline bool operator>(S5Byte &value) {
		return Value() > value.Value(); 
	}

	// greater than
	template <class X>
	inline bool operator>(X value) {
		return Value() > value; 
	}

	// less than or equal
	inline bool operator<=(S5Byte &value) {
		return Value() <= value.Value(); 
	}

	// less than or equal
	template <class X>
	inline bool operator<=(X value) {
		return Value() <= value; 
	}

	// greater than or equal
	inline bool operator>=(S5Byte &value) {
		return Value() >= value.Value(); 
	}

	// greater than or equal
	template <class X>
	inline bool operator>=(X value) {
		return Value() >= value; 
	}

	// logical and 
	inline bool operator&&(S5Byte &value) {
		return Value() && value.Value(); 
	}

	// logical or
	inline bool operator||(S5Byte &value) {
		return Value() || value.Value(); 
	}

	// increment
	inline S5Byte &operator++() {
		_int64 value = Value() + 1; 
		Copy5Bytes(Buffer(), (char *)&value); 
		return *this; 
	}

	// increment
	inline S5Byte operator++(int value) {
		S5Byte curr_value = *this;
		_int64 update = Value() + 1; 
		Copy5Bytes(Buffer(), (char *)&update); 
		return curr_value; 
	}

	// decrement
	inline S5Byte &operator--() {
		_int64 update = Value() - 1; 
		Copy5Bytes(Buffer(), (char *)&update); 
		return *this; 
	}

	// decrement
	inline S5Byte operator--(int value) {
		S5Byte curr_value = *this;
		_int64 update = Value() - 1; 
		Copy5Bytes(Buffer(), (char *)&update);  
		return curr_value; 
	}







}; 

// used for sorting hit items externally
struct SExternalSort {
	// stores a pointer to the start of 
	// the hit item 
	char *hit_item; 
}; 

// This defines a 64-bit bound used through the program
struct S64BitBound {
	// This defines the start bound
	_int64 start;
	// This defines the end bound
	_int64 end;

	S64BitBound() {
	}

	S64BitBound(_int64 start, _int64 end) {
		this->start = start;
		this->end = end;
	}

	// This sets the bound 
	void Set(_int64 start, _int64 end) {
		this->start = start;
		this->end = end;
	}

	// This returns the width of the bound
	inline _int64 Width() {
		return end - start;
	}
};

// stores the start and end position
struct SBoundary {
	// stores the start of the excerpt word pos
	int start; 
	// stores the end of the excerpt word pos
	int end; 

	SBoundary() {
	}

	SBoundary(int start, int end) {
		Set(start, end);
	}

	void Reset() {
		start = 0; 
		end = 0; 
	}

	inline int Width() {
		return end - start; 
	}

	void Set(int start, int end) {
		this->start = start; 
		this->end = end; 
	}
}; 

class EException {
protected:
	char m_exception[100]; 
public:

	EException() {
	}

	EException(const char str[]) {
		strcpy(m_exception, str); 
	}

	void PrintException() {
		cout << m_exception << endl; 
	}

	void PrintException(const char str[]) {
		cout << str <<" "<< m_exception << endl; 
	}
}; 


class EIndexOutOfBoundsException : public EException {
public:

	EIndexOutOfBoundsException() {
	}

	EIndexOutOfBoundsException(const char str[]) {
		strcpy(m_exception, str); 
	}
}; 

class EUnderflowException : public EException {
public:

	EUnderflowException() {
	}

	EUnderflowException(const char str[]) {
		strcpy(m_exception, str); 
	}
}; 

class EFunctionNotImplementedException : public EException {
public:

	EFunctionNotImplementedException() {
	}

	EFunctionNotImplementedException(const char str[]) {
		strcpy(m_exception, str); 
	}
}; 

class EOverflowException : public EException {
public:

	EOverflowException() {
	}

	EOverflowException(const char str[]) {
		strcpy(m_exception, str); 
	}
}; 

class EFileException : public EException {
public:

	EFileException() {
	}

	EFileException(const char str[]) {
		strcpy(m_exception, str); 
	}
}; 

class ENotFoundException : public EException {
public:

	ENotFoundException() {
	}

	ENotFoundException(const char str[]) {
		strcpy(m_exception, str); 
	}
}; 

class ECompressionException : public EException {
public:

	ECompressionException() {
	}

	ECompressionException(const char str[]) {
		strcpy(m_exception, str); 
	}
}; 

class EAllocationFailure : public EException {
public:

	EAllocationFailure() {
	}

	EAllocationFailure(const char str[]) {
		strcpy(m_exception, str); 
	}
}; 

class EItemNotFoundException : public EException {
public:

	EItemNotFoundException() {
	}

	EItemNotFoundException(const char str[]) {
		strcpy(m_exception, str); 
	}
}; 

class ENetworkException : public EException {
public:

	ENetworkException() {
	}

	ENetworkException(const char str[]) {
		strcpy(m_exception, str); 
	}
}; 

class EIllegalArgumentException : public EException {
public:

	EIllegalArgumentException() {
	}

	EIllegalArgumentException(const char str[]) {
		strcpy(m_exception, str); 
	}
}; 

// This is a general purpose class used for timing.
class CStopWatch {

public:

	CStopWatch() {
	}

	// This starts the stopwatch
	void StartTimer() {
	}

	// This stops the stopwatch
	void StopTimer() {
	}

	// This returns the net elapsed time
	double NetElapsedTime() {
		return 0;
	}

	// This returns the time elapsed in microseconds
	double GetElapsedTime() {
		return 0;
	}
};

// Converts character strings into numerical strings and vice versa
class CANConvert {
	// used to store the results
	static char str[64]; 

public:

	// converts a string to an integer
	// @param str - the buffer containing the text string
	// @param length - the length of the text string
	// @param start - an offset in the text string
	// @return the integer
	static int AlphaToNumeric(const char str[], 
		int length, int start = 0) {

		if(str == NULL)throw EIllegalArgumentException("str NULL"); 

		int number = 0; 
		int place = 1; 
		for(int i=start, c=0; i<length; i++, c++) {
			number += (int)(str[length - c - 1] - '0') * place; 
			place *= 10; 
		}

		return number; 
	}

	// converts a string to a 64-bit integer
	// @param str - the buffer containing the text string
	// @param length - the length of the text string
	// @param start - an offset in the text string
	// @return the integer
	static _int64 AlphaToNumericLong(const char str[], 
		int length, int start = 0) {

		if(str == NULL)throw EIllegalArgumentException("str NULL"); 

		_int64 number = 0; 
		int place = 1; 
		for(int i=start, c=0; i<length; i++, c++) {
			number += (int)(str[length - c - 1] - '0') * place; 
			place *= 10; 
		}

		return number; 
	}

	// converts a string to an unsigned integer
	// @param str - the buffer containing the text string
	// @param length - the length of the text string
	// @param start - an offset in the text string
	// @return the integer
	static int AlphaToNumeric(const char str[]) {
		return AlphaToNumeric(str, (int)strlen(str)); 
	}

	// converts an unsigned integer to a string
	// @param number - an integer to convert
	// @return the string representation
	template <class X>
	static char *NumericToAlpha(X number) {

		if(number < 0)throw EIllegalArgumentException("number < 0"); 
		int width = 0;
		X temp = number; 

		do {
			temp /= 10; 
			width++; 
		} while(temp); 

		temp = 0; 
		for(int d = 0; d < width; d++) {
			temp = number; 
			number /= 10; 
			str[width - d - 1] = (temp - (number * 10)) + '0'; 
		}
		str[width] = '\0'; 	// null terminates the string

		return str; 
	}
}; 
char CANConvert::str[64];

// This is a very useful general purpose utility class
// that deals mostly with text strings such as alphabets
// and numerical character determination. Also different
// types of string matching are included. Included is a
// temp buffer that can be used globally instead of having
// to allocate a separate buffer when it's needed.
class CUtility {
	// stores the numeric and alphabet characters
	static bool m_ok_character[256];

protected:

	// a general buffer used to store results
	static char m_temp_buff1[4096]; 
	// a general buffer used to store results
	static char m_temp_buff2[4096]; 
	// a general buffer used to store results
	static char m_temp_buff3[4096]; 

public:

	CUtility() {
	}

	// Sets up all the stop characters 
	static void Initialize() {
		AddTextStringTypeSet("abcdefghijklmnopqrstuvwxyz"
			"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", m_ok_character);
	}

	// Stores a text string type set
	// @param - the string of a certain type of characters
	// @param store_buffer - place to set true or false if 
	//                     - corresponding ASCII character is set
	// @param length - the length of the text string, found 
	//               - automatically if not specified
	static void AddTextStringTypeSet(const char str[], 
		bool store_buffer[], int length = 0) {

		for(int i=0; i<256; i++) {
			store_buffer[i] = false; 
		}

		if(length <= 0) {
			length = (int)strlen(str);
		}

		for(int i=0; i<length; i++) {
			store_buffer[(int)(str[i] + 128)] = true; 
		}
	}

	// Returns true if the letter is a capital
	inline static bool AskCapital(char ch) {
		return(ch >= 'A' && ch <= 'Z'); 
	}

	// Returns a temporary storage buffer
	static inline char *TempBuffer() {
		return m_temp_buff1; 
	}

	// Returns a temporary storage buffer
	static inline char *SecondTempBuffer() {
		return m_temp_buff2; 
	}

	// Returns a temporary storage buffer
	static inline char &SecondTempBuffer(int index) {
		return *(m_temp_buff2 + index); 
	}

	// Returns a temporary storage buffer
	static inline char *ThirdTempBuffer() {
		return m_temp_buff3; 
	}

	// Returns a reference to the temp buffer
	// @param index - the index of the temp buffer
	static inline char &TempBuffer(int index) {
		return *(m_temp_buff1 + index); 
	}

	// Prints a number in binary format
	template <class X> static void PrintBitStream(X number) {
		int size = sizeof(X) << 3; 
		for(int i=0; i<size; i++) {
			if(!(i % 8))cout << " "; 
			cout << (bool)(number & 1); 
			number >>= 1; 
		}
	}

	// Prints a character buffer in binary format
	static void PrintBitStream(char buffer[], int size) {
		cout<<"LSB   ";
		for(int i=0; i<size; i++) {
			PrintBitStream(buffer[i]); 
		}
	}

	// Checks to see if a character is a number
	inline static bool AskNumericCharacter(char ch) {
		return ch >= '0' && ch <= '9'; 
	}

	// Checks to see if a character belongs to the 
	// english alphabet
	inline static bool AskEnglishCharacter(char ch) {
		return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z'); 
	}

	// Returns the absolute difference between two numbers
	template <class X> inline static X 
		GetAbsoluteDifference(X value1, X value2) {

		if(value1 > value2)return value1 - value2; 
		return value2 - value1; 
	}

	// Counts the number of english characters in the buffer
	inline static int CountEnglishCharacters(char buffer[], int size) {
		int count = 0; 
		for(int i=0; i<size; i++) {
			if(AskEnglishCharacter(buffer[i]))
				count++; 
		}

		return count; 
	}

	// Converts a buffer to lower case if a character contains a capital
	inline static void ConvertBufferToLowerCase(char buffer[], int size) {
		for(int i=0; i<size; i++) {
			if(buffer[i] < 'A' || buffer[i] > 'Z') {
				continue;
			}

			ConvertToLowerCaseCharacter(buffer[i]); 
		}
	}

	// Returns true if the buffer contains a non stop character
	static bool AskBufferContainOkCharacter(char buffer[], int size) {
		for(int i=0; i<size; i++) {
			if(AskOkCharacter(buffer[i]))return true; 
		}
		return false; 
	}

	// Returns true if the buffer contains a numeric character
	static bool AskBufferContainNumericCharacter(char buffer[], int size) {
		for(int i=0; i<size; i++) {
			if(AskNumericCharacter(buffer[i]))return true; 
		}
		return false; 
	}

	// Returns true if the buffer contains a stop character
	static bool AskBufferNotContainOkCharacter(char buffer[], int size) {
		for(int i=0; i<size; i++) {
			if(!AskOkCharacter(buffer[i]))return true; 
		}
		return false; 
	}

	// Returns true if the buffer does not contain an english character
	static bool AskBufferNotContainEnglishCharacter(char buffer[], int size) {
		for(int i=0; i<size; i++) {
			if(!AskEnglishCharacter(buffer[i]))return true; 
		}
		return false; 
	}

	// Returns true if the buffer contains a space
	static bool AskBufferContainSpace(char buffer[], int size) {
		for(int i=0; i<size; i++) {
			if(buffer[i] == ' ')return true; 
		}
		return false; 
	}

	// Returns true if the character string contains an non - stop character
	// at the given index this takes into acount hyphernated words
	static bool AskOkCharacter(char buffer[], int index) {

		if(AskOkCharacter(buffer[index]))
			return true;

		if(buffer[index] == '.') {

			bool capital = (buffer[index - 1] >= 'A' 
				&& buffer[index - 1] <= 'Z'); 

			if(capital && (buffer[index+1] >= 'A' && buffer[index+1] <= 'Z'))
				return true; 

			if((buffer[index + 2] == '.') && (buffer[index+1] >= 'A'
				&& buffer[index+1] <= 'Z'))return true; 
		}

		return false; 
	}

	// Checks to see if the character is not a white
	// space character
	inline static bool AskOkCharacter(char ch) {
		return m_ok_character[ch + 128]; 
	}

	// Returns the lower case character - returns the
	// as is if not a character that belongs to the English
	// alphabet
	inline static char GetLowerCaseCharacter(char ch) {
		if(AskCapital(ch))return(ch - 'A') + 'a'; 
		return ch; 
	}

	// Convets the character to lower case - assumes that 
	// the character belongs to the English alphabet
	inline static void ConvertToLowerCaseCharacter(char &ch) {
		ch = 'a' + ch - 'A'; 
	}

	// Convets the character to upper case - assumes that 
	// the character belongs to the English alphabet
	inline static void ConvertToUpperCaseCharacter(char &ch) {
		ch = 'A' + ch - 'a'; 
	}

	// Returns the number of bytes corresponding to so many bits
	inline static int ByteCount(int bit_count) {
		if(bit_count % 8)return (bit_count >> 3) + 1; 
		return bit_count >> 3; 
	}

	// This removes any numeric characters from a string and 
	// returns a ptr to the provided string with numeric characters removed
	static char *RemoveNumericCharacters(char str[], int length = -1) {
		if(length < 0)length = (int)strlen(str);
		int offset = 0;
		for(int i=0; i<length; i++) {
			if(str[i] < '0' || str[i] > '9') {
				TempBuffer()[offset++] = str[i];
			}
		}

		TempBuffer()[offset] = '\0';
		return TempBuffer();
	}

	// Concanates the extension onto the base string
	static char *ExtendString(const char base[], const char extension[]) {
		strcpy(m_temp_buff1, base); 
		strcat(m_temp_buff1, extension); 

		return m_temp_buff1; 
	}

	// Concanates the extension onto the base string
	static char *ExtendString(const char base[], int number1) {
		strcpy(m_temp_buff1, base); 
		strcat(m_temp_buff1, CANConvert::NumericToAlpha(number1)); 

		return m_temp_buff1; 
	}

	// Concanates the extension onto the base string
	static char *ExtendString(const char base[], const char extension[], int number1) {
		strcpy(m_temp_buff1, base); 
		strcat(m_temp_buff1, extension); 
		strcat(m_temp_buff1, CANConvert::NumericToAlpha(number1)); 

		return m_temp_buff1; 
	}

	// Concanates the extension onto the base string
	static char *ExtendString(const char base[], int number1, const char extension[]) {
		strcpy(m_temp_buff1, base); 
		strcat(m_temp_buff1, CANConvert::NumericToAlpha(number1)); 
		strcat(m_temp_buff1, extension); 

		return m_temp_buff1; 
	}

	// Concanates the extension onto the base string
	static char *ExtendString(const char base[], int number1, const char extension[], int number2) {
		strcpy(m_temp_buff1, base); 
		strcat(m_temp_buff1, CANConvert::NumericToAlpha(number1)); 
		strcat(m_temp_buff1, extension); 
		strcat(m_temp_buff1, CANConvert::NumericToAlpha(number2)); 

		return m_temp_buff1; 
	}

	// Concanates the extension onto the base string
	static char *ExtendString(const char base[], int number1, 
		const char extension1[], int number2, const char extension2[], int number3) {
		strcpy(m_temp_buff1, base); 
		strcat(m_temp_buff1, CANConvert::NumericToAlpha(number1)); 
		strcat(m_temp_buff1, extension1); 
		strcat(m_temp_buff1, CANConvert::NumericToAlpha(number2)); 
		strcat(m_temp_buff1, extension2); 
		strcat(m_temp_buff1, CANConvert::NumericToAlpha(number3)); 

		return m_temp_buff1; 
	}

	// Concanates the extension onto the base string
	static char *ExtendString(const char base[], int number1, 
		const char extension1[], int number2, const char extension2[]) {
		strcpy(m_temp_buff1, base); 
		strcat(m_temp_buff1, CANConvert::NumericToAlpha(number1)); 
		strcat(m_temp_buff1, extension1); 
		strcat(m_temp_buff1, CANConvert::NumericToAlpha(number2)); 
		strcat(m_temp_buff1, extension2); 

		return m_temp_buff1; 
	}

	// Concanates the extension onto the base string
	static char *ExtendString(const char base[], const char extension1[], const char extension2[]) {
		strcpy(m_temp_buff1, base); 
		strcat(m_temp_buff1, extension1); 
		strcat(m_temp_buff1, extension2); 

		return m_temp_buff1; 
	}

	// Concanates the extension onto the base string
	static char *ExtendString(const char base[], const char extension1[],
		int number1, const char extension2[]) {

		strcpy(m_temp_buff1, base); 
		strcat(m_temp_buff1, extension1); 
		strcat(m_temp_buff1, CANConvert::NumericToAlpha(number1)); 
		strcat(m_temp_buff1, extension2); 

		return m_temp_buff1; 
	}

	// Concanates the extension onto the base string
	static char *ExtendString(const char base[], const char extension1[], 
		const char extension2[], const char extension3[]) {
		strcpy(m_temp_buff1, base); 
		strcat(m_temp_buff1, extension1); 
		strcat(m_temp_buff1, extension2); 
		strcat(m_temp_buff1, extension3); 
		return m_temp_buff1; 
	}

	// Concanates the extension onto the base string
	static char *ExtendString(const char base[], const char extension1[],
		int number1, const char extension2[], int number2) {

		strcpy(m_temp_buff1, base); 
		strcat(m_temp_buff1, extension1); 
		strcat(m_temp_buff1, CANConvert::NumericToAlpha(number1)); 
		strcat(m_temp_buff1, extension2); 
		strcat(m_temp_buff1, CANConvert::NumericToAlpha(number2)); 

		return m_temp_buff1; 
	}

	// Concanates the extension onto the base string
	static char *ExtendString(const char base[], const char extension1[], 
		const char extension2[], const char extension3[], const char extension4[]) {

		strcpy(m_temp_buff1, base); 
		strcat(m_temp_buff1, extension1); 
		strcat(m_temp_buff1, extension2); 
		strcat(m_temp_buff1, extension3); 
		strcat(m_temp_buff1, extension4); 
		return m_temp_buff1; 
	}

	// Checks if two strings are equivalent ignoring case
	template <class X> inline static bool FindUperLowerCaseFragment
		(char buffer[], char item[], X start = 0) {
		X length = (int)strlen(item) + start; 
		int c=0; 

		for(X i=start; i<length; i++, c++)
			if(GetLowerCaseCharacter(m_temp_buff1[i]) != 
				GetLowerCaseCharacter(item[c]))return false; 

		return true; 
	}
	
	// Checks to see if a given string is contained
	// in a larger string 
	template <class X> inline static bool 
		FindFragment(const char buffer[], const char item[], X &start) {

		X length = (int)strlen(item) + start; 
		int c=0; 
		for(X i=start; i<length; i++,c++) {
			if(buffer[i] != item[c])return false; 
                }

		start = length; 
		return true; 
	}

	// Checks to see if a given string is contained
	// in a larger string 
	static bool FindMatch(const char buffer[], 
		const char item[], int length, int start = 0) {

		int offset = 0;
		int size = (int)strlen(item); 
		if(size != (length - start)) {
			return false;
		}

		for(int i=start; i<length; i++) {
			if(buffer[i] != item[offset++]) {
				return false; 
			}
		}

		return true; 
	}

	// Checks to see if a given string is contained
	// in a larger string 
	static bool FindFragment(const char buffer[], const char item[], int length, int start) {

		int offset = 0; 
		for(int i=start; i<length; i++) {
			if(buffer[i] != item[offset++])return false; 
		}

		return true; 
	}

	// This finds the edit distance between two text strings
	static uChar EditDistance(const char *word1, int len1, const char *word2, int len2) {

		static uChar edit_dist[40][40];

		for(int i=0; i<len1+1; i++) {
			edit_dist[i][0] = i;
		}

		for(int i=0; i<len2+1; i++) {
			edit_dist[0][i] = i;
		}

		for(int i=0; i<len1; i++) {
			for(int j=0; j<len2; j++) {
				if(word1[i] == word2[j]) {
					edit_dist[i+1][j+1] = edit_dist[i][j];
				} else {
					edit_dist[i+1][j+1] = min(edit_dist[i][j] + 1, 
						min(edit_dist[i][j+1] + 1, edit_dist[i+1][j] + 1));
				}
			}
		}

		return edit_dist[len1][len2];
	}

	// Randomizes the seed value - used for 
	// anything that uses random
	static void RandomizeSeed() {
		time_t time_block = time(&time_block); 
		tm curr_time = *gmtime(&time_block); 
		srand(curr_time.tm_sec + clock()); 
	}

	// Extracts a number of a character string following
	// a text string marker that is given in a LocalData string
	// @param LocalData - a buffer containing the string that must
	//       - be searched
	// @param marker - a buffer containing the string that 
	//        - prepends the numerical value that must
	//        - be extracted
	// @return a numerical number, -1 if no marker was found
	static int ExtractParameter(const char LocalData[], const char marker[]) {
		int data_length = (int)strlen(LocalData); 
		int marker_length = (int)strlen(marker); 
		int index = 0; 

		for(int i=0; i<data_length; i++) {
			if(LocalData[i] != marker[index++]) {
				index = 0; 
				continue; 
			}
			if(index == marker_length) {
				
				int parameter_length = i + 1, offset = 0; 
				while((parameter_length < data_length) && (LocalData[parameter_length] != '&')
						&& (LocalData[parameter_length] != '=') && (LocalData[parameter_length] != '%'))
						CUtility::SecondTempBuffer()[offset++] = LocalData[parameter_length++]; 

				if(offset <= 0)return -1; 
				for(int j=0; j<offset; j++) {
					if(!AskNumericCharacter(CUtility::SecondTempBuffer()[j]))
						return -1; 
				}

				return CANConvert::AlphaToNumeric
					(CUtility::SecondTempBuffer(), offset); 
			}
		}

		return -1; 
	}

	// Extracts a string of text following a given
	// marker and returns a pointer to it
	// @param data - a buffer containing the string that must
	//             - be searched
	// @param marker - a buffer containing the string that 
	//               - prepends the text that is returned
	// @return the string of text that follows the marker
	static char *ExtractText(const char data[], const char marker[]) {
		int data_length = (int)strlen(data); 
		int marker_length = (int)strlen(marker); 
		int index = 0; 

		m_temp_buff2[0] = '\0';
		for(int i=0; i<data_length; i++) {
			if(data[i] != marker[index++]) {
				index = 0; 
				continue; 
			}

			if(index == marker_length) {
				int parameter_length = i + 1, offset = 0; 

				while((parameter_length < data_length) && (data[parameter_length] != '&')
					&& (data[parameter_length] != '=')) {
						m_temp_buff2[offset++] = data[parameter_length++]; 
				}

				if(offset <= 0)return m_temp_buff2; 
				m_temp_buff2[offset] = '\0'; 
				return m_temp_buff2; 
			}
		}

		return NULL; 
	}

	// Extracts a string of text following a given
	// marker and returns a pointer to it. The string is 
	// terminated once another character string finished is located
	// following the marker.
	// @param LocalData - a buffer containing the string that must
	//       - be searched
	// @param marker - a buffer containing the string that 
	//        - prepends the text that is returned
	// @param finish - the final character string which terminates the search
	// @return the string of text that follows the marker
	static char *ExtractTextUntil(char LocalData[], char marker[], char finish[]) {
		int data_length = (int)strlen(LocalData); 
		int marker_length = (int)strlen(marker); 
		int index = 0; 

		for(int i=0; i<data_length; i++) {
			if(LocalData[i] != marker[index++]) {
				index = 0; 
				continue; 
			}
			if(index == marker_length) {
				int size = FindSubFragment(LocalData, finish, i + 1); 
				int parameter_length = i + 1, offset = 0; 
				while((parameter_length < data_length) && (parameter_length < size))
					m_temp_buff1[offset++] = LocalData[parameter_length++]; 

				if(offset <= 0)return m_temp_buff1; 
				m_temp_buff1[offset] = '\0'; 
				return m_temp_buff1; 
			}
		}

		return m_temp_buff1; 
	}

	// Looks for a character string called item in 
	// at the start of another string called LocalData
	// @param LocalData - a buffer containing the string that must
	//       - be searched
	// @param item - the character string that the search is
	//       - trying to find
	// @assumes that the length of item < length of buffer
	// @return true if the item was found, false otherwise
	inline static bool FindFragment(const char buffer[], const char item[]) {

		int length = (int)strlen(item); 
		for(int i=0; i<length; i++) {
			if(buffer[i] != item[i])return false; 
		}

		return true; 
	}

	// Looks for a character string called item in 
	// a larger string string called LocalData.
	// @param LocalData - a buffer containing the string that must
	// - be searched
	// @param item - the character string that the search is
	// - trying to find
	// @return the index in the LocalData string, -1 if the item was

	// not found in the LocalData string
	static int FindSubFragment(const char LocalData[], const char item[]) {
		return FindSubFragment(LocalData, item, (int)strlen(LocalData)); 
	}

	// Finds the position of a character string called item
	// in a larger character string called LocalData (scans to the left)
	// @param LocalData - a buffer containing the string that must
	//			  - be searched
	// @param item - the character string that the search is
	//       - trying to find
	// @return the index in the LocalData string, -1 if the item was
	// not found in the LocalData string
	static int FindSubFragmentScanLeft(const char LocalData[], const char item[]) {
		int item_length = (int)strlen(item); 
		int data_length = (int)strlen(LocalData); 

		if(item_length > data_length)
			return -1; 

		for(int i=data_length - item_length; i >= 0; i++) {
			bool found = true; 
			for(int c=0; c<item_length; c++) {
				if(LocalData[i + c] != item[c]) 
				{found = false; break;}
			}

			if(found)return i; 
		}
		return -1; 
	}

	// Finds the position of a character string called item
	// in a larger character string called LocalData (scans to the right)
	// @param LocalData - a buffer containing the string that must
	// - be searched
	// @param item - the character string that the search is
	// - trying to find
	// @param data_length - the length of the LocalData string in bytes
	// @param start - an offset in the LocalData string for which the 
	// - search will commence
	// @return the index in the LocalData string, -1 if the item was
	// not found in the LocalData string
	static int FindSubFragment(const char LocalData[], const char item[], 
		int data_length, int start = 0) {

		data_length -= start; 
		LocalData += start; 

		int item_length = (int)strlen(item); 
		if(item_length > data_length) {
			return -1; 
		}

		for(int i=0; i<data_length - item_length + 1; i++) {
			bool found = true; 

			for(int c=0; c<item_length; c++) {
				if(LocalData[i + c] != item[c]) {
					found = false;  
					break;
				}
			}

			if(found == true) {
				return i + start; 
			}
		}
		return -1; 
	}

	// Replaces a text string contained in LocalData character buffer with
	// a new text string. It's assumed that the text strings are the same size
	// @param original_text - the character string that is being replaced
	// @param new_text - the character string that replaces original_text
	// @return -returns a pointer into the text string, returns the LocalData
	// - string if the text was not found
	static char *ReplaceText(char LocalData[], char original_text[], char new_text[]) {
		int new_length = (int)strlen(new_text); 
		int original_length = (int)strlen(original_text); 
		int data_length = (int)strlen(LocalData); 

		if(original_length > data_length) {
			strcat(LocalData, new_text); 
			return LocalData; 
		}

		for(int i=0; i<data_length - original_length + 1; i++) {
			bool found = true; 
			for(int c=0; c<original_length; c++) {
				if(LocalData[i + c] != original_text[c]) 
				{found = false; break;}
			}

			if(found) {
				for(int c=0; c<new_length; c++)
					LocalData[i + c] = new_text[c]; 

				LocalData[data_length] = 0; 
				return LocalData; 
			}
		}
		
		strcat(LocalData, new_text); 
		return LocalData; 
	}
}; 
bool CUtility::m_ok_character[256];
char CUtility::m_temp_buff1[4096];
char CUtility::m_temp_buff2[4096]; 
char CUtility::m_temp_buff3[4096]; 

// allocates a block of memory - used in vector
template <class X> class CMemory {

	// stores a pointer to where the buffer is stored
	X *m_memory; 

public:

	// allocates a certain block of memory
	// and returns a pointer to it
	X *AllocateMemory(int size) {
		try {
			m_memory = new X [size]; 
		} catch(bad_alloc xa) {
			throw EAllocationFailure
				("Memory Chunk Allocation Failure"); 
		}
		return m_memory; 
	}

	// allocates a certain block of memory
	// and returns a pointer to it
	X *AllocateMemory(int size, X value) {
		AllocateMemory(size); 
		for(int i=0; i<size; i++) {
			 * (m_memory + i) = value; 
		}

		return m_memory; 
	}

	
	// frees a block of memory 
	inline void FreeMemory(X *memory) {
		delete []memory; 
	}
}; 

// This class is used whenever a block of memory
// needs to be allocated to store some set of objects.
// Provides automatic deallocation and size checking
// to prevent the same block of memory being allocated 
// more than once.
template <class X> class CBaseMemoryChunk {
	
protected:
	
	// stores a pointer to where the buffer is stored
	X *m_memory; 
	// stores the current size of the buffer
	int m_size;

public:

	CBaseMemoryChunk() {
		m_memory = NULL; 
		m_size = 0; 
	}

	CBaseMemoryChunk(int size) {
		m_memory = NULL; 
		m_size = 0; 
		AllocateMemory(size); 
	}

	CBaseMemoryChunk(int size, X value) {
		m_memory = NULL; 
		m_size = 0; 
		AllocateMemory(size); 
		InitializeMemoryChunk(value); 
	}

	CBaseMemoryChunk(CBaseMemoryChunk<X> &copy) {
		m_memory = NULL; 
		m_size = 0; 
		MakeMemoryChunkEqualTo(copy); 
	}

	// fills the buffer with a certain element
	void InitializeMemoryChunk(X value, int size = -1) {
		if(size < 0) {
			size = m_size;
		}
		for(int i=0; i<size; i++) {
			m_memory[i] = value;
		}
	}

	// fills the buffer with ascending integers
	void FillAscendingOrder() {
		for(int i=0; i<OverflowSize(); i++) {
			m_memory[i] = i; 
		}
	}

	// creates a block of memory
	X *AllocateMemory(int size) {
		if(size < 0)throw EIllegalArgumentException
			("Invalid Memory Argument"); 

 		if(size == 0) {
			FreeMemory(); 
			return NULL; 
		}

		if(size != m_size)
			FreeMemory(); 
		else if(m_memory)
			return m_memory; 

		try {
			m_memory = new X [size]; 
		} catch(bad_alloc xa) {
			throw EAllocationFailure
				("Memory Chunk Allocation Failure"); 
		}
		m_size = size; 
		return m_memory; 
	}

	// allocates a block of memory and Initialize it 
	X *AllocateMemory(int size, X value) {
		AllocateMemory(size); 
		InitializeMemoryChunk(value); 
		return m_memory; 
	}

	// resizes the memory chunk and copies the old contents
	// into the new buffer
	void Resize(int size) {
		if(size < 0) {
			throw EIllegalArgumentException
			("Size Given < 0"); 
		}

		if(size == OverflowSize()) {
			return; 
		}

		CBaseMemoryChunk<X> copy(*this); 
		AllocateMemory(size); 

		if(size < copy.OverflowSize()) {
			memcpy(Buffer(), copy.Buffer(), sizeof(X) * size); 
		} else {
			memcpy(Buffer(), copy.Buffer(), sizeof(X) * copy.OverflowSize()); 
		}
	}

	// returns a reference to the last element
	X &LastElement() {
		if(OverflowSize() <= 0)throw EUnderflowException
			("No Elements Stored"); 
		return m_memory[OverflowSize() - 1]; 
	}

	// returns a reference to the second last element
	X &SecondLastElement() {
		if(OverflowSize() <= 1) {
			throw EUnderflowException
			("No Elements Stored"); 
		}

		return m_memory[OverflowSize() - 2]; 
	}

	// returns a reference to an element from the end
	X &ElementFromEnd(int index) {
		if(index >= OverflowSize() || index < 0) {
			throw EUnderflowException
			("No Elements Stored"); 
		}

		return m_memory[OverflowSize() - 1 - index]; 
	}


	// finds the maximum element from an buffer of elements
	// @return the maximum element
	X MaxElement() {
		X max = m_memory[0]; 
		for(int i=0; i<OverflowSize(); i++) {
			if(max < m_memory[i]) {
				max = m_memory[i]; 
			}
		}
		return max; 
	}

	// finds the minimum element from an buffer of elements
	// @return the minimum element
	X MinElement() {
		X min = m_memory[0]; 
		for(int i=0; i<OverflowSize(); i++) {
			if(min > m_memory[i]) {
				min = m_memory[i]; 
			}
		}
		return min; 
	}

	// shifts the enitre contents of the buffer
	// left some number of places the values at
	// the start will be removed and undefined
	// values will be added at the end
	// @shift_num - the number of places to shift left
	void ShiftBufferLeft(int shift_num) {

		if(shift_num > OverflowSize()) {
			throw EIllegalArgumentException("shift exceeds buffer boundary");
		}

		CBaseMemoryChunk<X> temp(OverflowSize() - shift_num);
		memcpy(temp.Buffer(), Buffer() + shift_num, temp.OverflowSize() * sizeof(X));
		memcpy(Buffer(), temp.Buffer(), temp.OverflowSize() * sizeof(X));
	}

	// shifts the enitre contents of the buffer
	// right some number of places the values at
	// the end will be removed and undefined
	// values will be added at the start
	// @shift_num - the number of places to shift left
	void ShiftBufferRight(int shift_num) {
		if(shift_num > OverflowSize())throw
			EIllegalArgumentException("shift exceeds buffer boundary");

		CBaseMemoryChunk<X> temp(OverflowSize() - shift_num);
		memcpy(temp.Buffer(), Buffer(), temp.OverflowSize() * sizeof(X));
		memcpy(Buffer() + shift_num, temp.Buffer(), temp.OverflowSize() * sizeof(X));
	}

	// copies an existing memory chunk
	void MakeMemoryChunkEqualTo(CBaseMemoryChunk<X> &copy) {
		AllocateMemory(copy.OverflowSize()); 
		memcpy(Buffer(), copy.Buffer(), copy.OverflowSize() * sizeof(X)); 
	}

	// copies an existing memory chunk
	void MakeMemoryChunkEqualTo(X buffer[], int size) {
		AllocateMemory(size); 
		memcpy(m_memory, buffer, size * sizeof(X)); 
	}

	// retrieves an element
	inline X &operator[](int index) {
		if(index >= OverflowSize() || index < 0) {
			throw EIndexOutOfBoundsException
				("memory chunk overflow "); 
		}
		return m_memory[index]; 
	}

	// retrieves an element
	inline X &Element(int index) {
		if(index >= OverflowSize() || index < 0) {
			throw EIndexOutOfBoundsException
				("memory chunk overflow "); 
		}

		return m_memory[index]; 
	}

	// returns a pointer to the buffer
	inline X *Buffer() {
		return m_memory; 
	}

	// returns a reference to the buffer
	inline X &Reference() {
		return *m_memory; 
	}

	// frees the block of memory 
	inline void FreeMemory() {
		if(m_memory != NULL)delete []m_memory; 
		m_size = 0; 
		m_memory = NULL; 
	}

	// returns the amount of memory allocated
	inline int OverflowSize() {
		return m_size; 
	}

	// returns the net sum of all the elements
	inline X SumElements() {
		X sum = 0;
		for(int i=0; i<OverflowSize(); i++)
			sum += Element(i);

		return sum;
	}

	// sets a block of memory to zero
	inline void SetToZero() {
		for(int i=0; i<OverflowSize(); i++)
			m_memory[i] = 0; 
	}
	
	~CBaseMemoryChunk() {
		if(m_memory) {
			delete []m_memory; 
		}
	}
};

// This is just the base class of Array.
template <class X> class CBaseArray {
protected:

	// stores the current size of the array
	int m_size; 
	// creates a buffer to store the array
	CBaseMemoryChunk<X> m_memory; 

public:

	CBaseArray() {
		m_size = 0; 
	}
	
	// returns a given element
	inline X &operator[](int index) {
		if(index >= Size() || index < 0) {
			throw EIndexOutOfBoundsException
				("Array Index Out Of Bounds"); 
		}

		return m_memory[index]; 	
	}

	// returns a given element
	inline X &Element(int index) {
		return operator[](index); 	
	}

	// allocates memory for the array
	void Initialize(int size) {
		m_size = 0; 
		m_memory.AllocateMemory(size); 
	}

	// allocates memory for the array
	void Initialize(int size, X value) {
		Initialize(size); 
		m_memory.InitializeMemoryChunk(value); 
	}

	// adds an element to the array
	inline void PushBack(X element) {
		if(Size() >= OverflowSize()) {
			throw EOverflowException("Array Overflow"); 
		}
		m_memory[m_size++] = element; 
	}

	// resizes the buffer to accomadate for more
	// elements to be stored
	// @param size - the new number of bytes requested
	inline void ResizeBuffer(int size) {
		m_memory.Resize(size); 
	}

	// removes the last element from the array
	inline X PopBack() {
		if(m_size <= 0) {
			throw EUnderflowException("Array UnderFlow"); 
		}

		return m_memory[--m_size]; 
	}

	// returns the size of the array
	inline int Size() {
		return m_size; 
	}

	// resizes the array
	inline void Resize(int size) {
		if(size < 0 || size > OverflowSize()) {
			throw EIllegalArgumentException("Resize Out of Bounds"); 
		}
		m_size = size; 
	}

	// returns the last element
	inline X &LastElement() {
		if(Size() > 0) {
			return m_memory[m_size - 1]; 
		}

		throw EUnderflowException("Array UnderFlow Last Element"); 
	}

	// increases the size of the array by some offset
	inline X *ExtendSize(int offset) {

		int prev_size = m_size;
		m_size += offset; 
		if(Size() > OverflowSize()) {
			throw EOverflowException("Bounds Exceeded"); 
		}

		return m_memory.Buffer() + prev_size;
	}

	// retreives the encoding for a 4 - byte integer and places
	// the results in the buffer
	static void GetEscapedEncoding(uLong item, char buffer[]) {
		while(item >= 128) {
			(*buffer++) = (uChar)(item|0x80); 
			item >>= 7; 
		}

		*buffer = (uChar)item; 
	}

	// returns the second last element
	inline X &SecondLastElement() {
		if(Size() > 1)return m_memory[m_size - 2]; 
		throw EUnderflowException("Array UnderFlow Second Last Element"); 
	}

	// returns the third last element
	inline X &ThirdLastElement() {
		if(Size() > 2)return m_memory[m_size - 3]; 
		throw EUnderflowException("Array UnderFlow Third Last Element"); 
	}

	// returns the fourth last element
	inline X &FourthLastElement() {
		if(Size() > 3)return m_memory[m_size - 4]; 
		throw EUnderflowException("Array UnderFlow Fourth Last Element"); 
	}

	// returns the fifth last element
	inline X &FifthLastElement() {
		if(Size() > 4)return m_memory[m_size - 5]; 
		throw EUnderflowException("Array UnderFlow Fifth Last Element"); 
	}

	// returns an element indexed from the end of the array
	inline X &ElementFromEnd(int index) {
		if(index < 0 || index >= Size()) {
			throw EUnderflowException("Array UnderFlow Fifth Last Element"); 
		}
		return m_memory[m_size - index - 1]; 
	}

	// returns a 5 - byte representation of the buffer
	inline static void Get5ByteItem( _int64 & item, char buffer[]) {
		 * ((char *)&item) = buffer[0]; 
		 * (((char *)&item) + 1) = buffer[1]; 
		 * (((char *)&item) + 2) = buffer[2]; 
		 * (((char *)&item) + 3) = buffer[3]; 
		 * (((char *)&item) + 4) = buffer[4]; 
	}

	// returns a 4 - byte representation of the buffer
	template <class Y> inline static void Get4ByteItem(Y &item, char buffer[]) {
		 * ((char *)&item) = buffer[0]; 
		 * (((char *)&item) + 1) = buffer[1]; 
		 * (((char *)&item) + 2) = buffer[2]; 
		 * (((char *)&item) + 3) = buffer[3]; 
	}

	// returns a 3 - byte representation of the buffer
	template <class Y> inline static void Get3ByteItem(Y &item, char buffer[]) {
		 * ((char *)&item) = buffer[0]; 
		 * (((char *)&item) + 1) = buffer[1]; 
		 * (((char *)&item) + 2) = buffer[2]; 
	}

	// returns a 2 - byte representation of the buffer
	template <class Y> inline static void Get2ByteItem(Y &item, char buffer[]) {
		 * ((char *)&item) = buffer[0]; 
		 * (((char *)&item) + 1) = buffer[1]; 
	}

	// adds a 5 - byte item to the buffer
	inline void AddItem5Bytes(_int64 & item) {
		PushBack(*((char *)&item)); 
		PushBack(*(((char *)&item) + 1)); 
		PushBack(*(((char *)&item) + 2)); 
		PushBack(*(((char *)&item) + 3)); 
		PushBack(*(((char *)&item) + 5)); 
	}

	// adds a 4 - byte item to the buffer
	template <class Y> inline void AddItem4Bytes(Y item) {
		PushBack(*((char *)&item)); 
		PushBack(*(((char *)&item) + 1)); 
		PushBack(*(((char *)&item) + 2)); 
		PushBack(*(((char *)&item) + 3)); 
	}

	// adds a 3 - byte item to the buffer
	template <class Y> inline void AddItem3Bytes(Y item) {
		PushBack(*((char *)&item)); 
		PushBack(*(((char *)&item) + 1)); 
		PushBack(*(((char *)&item) + 2)); 
	}

	// adds a 2 - byte item to the buffer
	template <class Y> inline void AddItem2Bytes(Y item) {
		PushBack(*((char *)&item)); 
		PushBack(*(((char *)&item) + 1)); 
	}

	// returns a Buffer to the buffer
	inline X *Buffer() {
		return m_memory.Buffer(); 
	}

	// returns a reference to the buffer
	inline X &Reference() {
		return m_memory.Reference(); 
	}


	// returns the maximum size of the buffer
	inline int OverflowSize() {
		return m_memory.OverflowSize(); 
	}

	// forcefully frees memory
	inline void FreeMemory() {
		m_memory.FreeMemory(); 
		m_size = 0; 
	}

	// deletes an element from the array
	// @param index - the index of the element to delete
	void DeleteElement(int index) {
		if(Size() <= 0)throw EUnderflowException
			("size exeeds buffer boundary");

		if(index >= Size() || index < 0)
			throw EIndexOutOfBoundsException
			("Array Index Out Of Bounds"); 

		for(int i=index; i<Size()-1; i++) 
			m_memory[i] = m_memory[i+1]; 

		m_size--; 
	}

	// insert an element into the array
	// @param index - the index of where to insert the element
	// @param element - the element that's being inserted
	void InsertElement(X element, int index) {
		if(Size() >= OverflowSize())throw EOverflowException
			("size exeeds buffer boundary");

		for(int i=0; i<m_size-index; i++) {
			m_memory[m_size - i] = m_memory[m_size - i - 1]; 
		}

		m_memory[index] = element; 
		m_size++; 
	}
}; 

// This is really a utility class that handles common 
// tasks associated with a hash function. It is mostly
// used to calculate hash functions using different methods
// aswell as finding prime numbers.
class CHashFunction {

public:

	// Only applicable to 8 - bit modulo sizes
	static inline int CheckSumExlusiveOr(const char str[], int length, int start = 0) {

		uChar check_sum = 0; 
		for(int i=start; i<length; i++) {
			check_sum ^= str[i]; 
		}
		return check_sum; 
	}

	// Finds the checks using an incremental
	// approach by adding a certain factor
	// @param base - the base increment
	static uLong CheckSumBaseIncrement(int base, 
		const char str[], int length, int start = 0) {

		int factor = 1; 
		uLong check_sum = 0; 
		for(int i=start; i<length; i++) {
			check_sum += str[i] * factor; 
			factor += base; 
		}
		return (int)check_sum; 
	}

	// Finds the checks using an multiple
	// approach by multiplying by a certain factor
	// @param base - the base multiple
	static uLong CheckSumBaseMultiply(int base, 
		const char str[], int length, int start = 0) {

		int factor = 1; 
		uLong check_sum = 0; 
		for(int i=start; i<length; i++) {
			check_sum += str[i] * factor; 
			factor *= base; 
		}
		return (int)check_sum; 
	}

	// General purpose hash function
	// @param mod - the hashing modulus
	static int UniversalHash(const char str[], int length) {

		uLong h = 0, a = 31415, b = 27183; 
		for(int i=0; i<length; i++) {
			h = (a * h + str[i]); 
			a = (a * b); 
		}
		return (int)h; 
	}

	// General purpose hash function
	// @param mod - the hashing modulus
	static int UniversalHash(int mod, const char str[], int length) {
		uLong h = 0, a = 31415, b = 27183; 
		for(int i=0; i<length; i++) {
			h = (a * h + str[i]) % mod; 
			a = (a * b) % (mod - 1); 
		}
		return (int)h; 
	}

	// Finds the closest prime corresponding to some division size
	// @param mod - the number for which a closest prime equal to 
	// - or greater is to be found
	static uLong FindClosestPrime(int mod) {
		static int prime_num[] = {3, 5, 19, 39, 57, 251, 509, 1021, 2039, 4093, 
			8191, 16381, 32749, 65521, 131071, 262139, 524287, 1048573, 
			2097143, 4194301, 8388593, 0xFFFFFF, 0x4FFFFFF}; 

		for(int i=0; i<23; i++) {
			if(prime_num[i] >= mod) {
				return prime_num[i]; 
			}
		}
		return prime_num[22]; 
	}

	// Used for simple hashing - fast
	// @param mod - the hashing modulus
	static int SimpleHash(const char str[], int length) {

		int sum = 0; 
		for(int i=0; i<length; i++) {
			sum += abs(str[i]);
		}

		return sum; 
	}

	// Used for simple hashing - fast
	// @param mod - the hashing modulus
	static inline int SimpleHash(int mod, const char str[], int length) {
		return SimpleHash(str, length) % mod; 
	}

	// This returns an equal segment size for some interval
	// that needs to be partioned (used if not divisible)
	// @param interval_size - the total that needs to be divided
	// @param set_num - the number of divisions in a given set
	// @return the size of the equal partion
	static void EqualSegmentSize(int interval_size, int &set_id, int set_num) {
		int block_size = interval_size / set_num;
		int left_over = interval_size % set_num;
	}

	// This function evenely distributes a 
	// given boundary into equal spaced partions
	// @param set_id - the set in the range [0 set_num]
	// @param set_num - the number of divisions in a given set
	// @param div_end - the global end of the division ->
	//                - this will be changed to reflect the set
	//                - id end of the division
	// @param div_start - the global start of the division ->
	//                - this will be changed to reflect the set
	//                - id start of the division
	template <class X>
	static void BoundaryPartion(int set_id, int set_num, 
		X &div_end, X &div_start) {

		X div_width = (div_end - div_start) / set_num;
		X left_over =  (div_end - div_start) % set_num;

		if(set_id < left_over) {
			div_end = (div_width + 1) * (set_id + 1);
		} else {
			div_end = ((div_width + 1) * left_over) + 
				((set_id - left_over + 1) * div_width);
		}

		div_end += div_start;

		// increments the div width size if there
		// is a leftover div available
		if(set_id < left_over) {
			div_start += (div_width + 1) * set_id;
		} else {
			div_start += ((div_width + 1) * left_over) + 
				((set_id - left_over) * div_width);
		}
	}
}; 

// This is used for the write comp block thread
struct SCompBlockThread {
	// This stores a ptr to the comp buffer
	CBaseMemoryChunk<char> comp_buffer;
	// This stores the compression size
	int compress_size;
	// This stores the normal uncompressed size
	int norm_size;
};

// This defines the root DFS directory
const char *DFS_ROOT = "/work1/s4141073/DyableCollection/";

// This is a very useful file class that handles
// all access to an external file. Besides the
// standard write and read functions there is also
// the equivalent compression functions that compress
// an object or several groups of objects.
class CHDFSFile {

	// This defines the number of hash divisions for the LocalData directory
	static const int HASH_DIV_NUM = 8192;

	// This stores the number of bytes stored
	_int64 m_bytes_stored;

	// This writes the compressed block to external storage 
	void WriteCompressedBlock() {

		WriteObject(m_write_thread.compress_size); 
		WriteObject(m_write_thread.norm_size); 
		WriteObject(m_write_thread.comp_buffer.Buffer(), m_write_thread.compress_size);
	}

	// This returns the hash directory for a given file
	const char *HashDirectory(const char *dir) {

		char *temp_buff = m_directory.Buffer() + strlen(m_directory.Buffer()) + 5;
		strcpy(temp_buff, DFS_ROOT);

		if(CUtility::FindFragment(dir, "LocalData/") == false) {
			strcat(temp_buff, dir);
			return temp_buff;
		}

		int div = CHashFunction::UniversalHash(HASH_DIV_NUM, dir, strlen(dir));
		strcat(temp_buff, "LocalData/Div");
		strcat(temp_buff, CANConvert::NumericToAlpha(div));
		strcat(temp_buff, "/");
		strcat(temp_buff, dir + strlen("LocalData/"));

		return temp_buff;
	}

protected:

	// This stores a ptr to the open file stream
	FILE *m_file_ptr;

	// stores the directory of the file
	CBaseMemoryChunk<char> m_directory;
	// used to store a single compression block
	CBaseArray<char> m_comp_buffer; 
	// This is used to store the results to a file system
	SCompBlockThread m_write_thread;
	// stores the current compression offset within the file
	int m_comp_offset;
	// This stores the total number of bytes read
	_int64 m_bytes_read;

	// Writes the compression buffer to file
	// @param last_block - false if the last block is being written
	int FlushCompressionBuffer(bool last_block = false) {

		if(m_comp_buffer.OverflowSize() == 0) {
			InitializeCompression();
			return 0;
		}

		int compress_size = CompressBuffer(m_comp_buffer.Buffer(), 
			m_write_thread.comp_buffer, m_comp_buffer.Size()); 

		m_write_thread.compress_size = compress_size;
		m_write_thread.norm_size = m_comp_buffer.Size();

		m_comp_buffer.Resize(0); 
		if(compress_size <= 0) {
			return -1; 
		}

		WriteCompressedBlock();

		return compress_size + (sizeof(int) << 1); 
	}

	// reads the next compression block from file
	bool GetNextCompressionBlock() {

		if(m_bytes_read < 0) {
			return false;
		}

		int norm_size; 
		int compress_size; 
		if(ReadObject(compress_size) == false) {
			compress_size = -1;
		}

		if(compress_size < 0) {
			m_bytes_read = -1;
			return false;
		}

		if(m_comp_buffer.OverflowSize() == 0) {
			InitializeCompression();
		}
		
		m_comp_offset = 0; 
		ReadObject(norm_size); 

		if(norm_size > m_comp_buffer.OverflowSize()) {
			m_comp_buffer.Initialize(norm_size);
		}

		CBaseMemoryChunk<char> decompress(compress_size); 
		ReadObject(decompress.Buffer(), compress_size); 

		try {
			DecompressBuffer(decompress.Buffer(), 
				m_comp_buffer.Buffer(), compress_size, norm_size); 
		} catch(...) {
			cout<<"Could Not Decompress "<<m_directory.Buffer()<<endl;
			throw EException("");
		}

		m_bytes_read += compress_size;
		m_comp_buffer.Resize(norm_size); 
		return true;
	}

public:

	CHDFSFile() {
		m_bytes_stored = -1;
		m_file_ptr = NULL;
	}

	// This just sets the directory name of the file
	// @param dir - this is the directory of the file
	CHDFSFile(const char dir[]) {
		SetFileName(dir);
		m_bytes_stored = -1;
		m_file_ptr = NULL;
	}

	// This opens a read file, it closes any previous files that 
	// @param str - a buffer containing the name of the file
	//            - default NULL means use previously set directory
	void OpenReadFile(const char str[] = NULL) {

		CloseFile(); 

		if(str != NULL) {
			SetFileName(str);
		}
	
		for(int i=0; i<4; i++) {
			m_file_ptr = fopen64(HashDirectory(m_directory.Buffer()), "r");
	
			if(m_file_ptr != NULL) {	
				break;
			}

			if(i >= 3) {
				cout<<"Could Not Open "<<HashDirectory(m_directory.Buffer())<<endl;
				throw EFileException("Could Not Open File"); 
			} else {
				Sleep(75);
			}
		}
	   
	        fseeko(m_file_ptr, 0L, SEEK_END);	
		m_bytes_stored = (_int64)ftell(m_file_ptr);
		fseeko(m_file_ptr, 0L, SEEK_SET);
		m_bytes_read = 0;
		m_comp_offset = 0;
	}

	// This opens a write file, it just stores the directory of the file
	// @param str - a buffer containing the name of the file
	//            - default NULL means use previously set directory
	void OpenWriteFile(const char str[] = NULL) {

/*
		CloseFile();

		if(str != NULL) {
			SetFileName(str);
		}

		m_bytes_stored = -1;
		m_file_ptr = fopen64(HashDirectory(m_directory.Buffer()), "w");
		if(m_file_ptr == NULL) {
			cout<<"Could Not Write "<<HashDirectory(m_directory.Buffer())<<endl;
			throw EFileException("Could Not Open File"); 
		}

*/
	}

	// Starts the compression buffer before reading and writing to a file
	// @param buffer_size - this is the size of the comp buffer
	void InitializeCompression(int buffer_size = 960000) {

		buffer_size = max((int)1024, buffer_size);
		buffer_size = min(buffer_size, (int)960000);
		if((buffer_size % 8) != 0) {
			buffer_size += 8 - (buffer_size % 8);
		}
		// must be a multiple of 8
		m_comp_buffer.Initialize(buffer_size); 
		m_comp_offset = 0; 
	}

	// This resests the readfile at the beginning
	inline void ResetReadFile() {
		m_comp_buffer.Initialize(m_comp_buffer.OverflowSize()); 
		fseeko(m_file_ptr, 0L, SEEK_SET);
		m_comp_offset = 0; 
		m_bytes_read = 0;
	}

	static void Initialize() {
	}

	// Returns the size of a read file
	// @return the size of the read file in bytes
	inline _int64 ReadFileSize() {
		return m_bytes_stored;
	}

	// This replaces a file
	static int Rename(const char *dir1, const char *dir2) {
		static char temp_buff1[100];
		static char temp_buff2[100];
		strcpy(temp_buff1, DFS_ROOT);
		strcat(temp_buff1, dir1);

		strcpy(temp_buff2, DFS_ROOT);
		strcat(temp_buff2, dir2);

		return rename(temp_buff1, temp_buff2);
	}

	// This removes a file
	static int Remove(const char *dir) {
		static char temp_buff[100];
		strcpy(temp_buff, DFS_ROOT);
		strcat(temp_buff, dir);
		return remove(temp_buff);
	}

	// This returns the percent of the file read
	inline float PercentageFileRead() {
		return (float)m_bytes_read / ReadFileSize();
	}

	// This checks if there is enough space to store a given byte stream
	// @param ovf_bytes - this is the total number of bytes being verified
	void AskBufferOverflow(int ovf_bytes) {
		if(m_comp_buffer.Size() + ovf_bytes > m_comp_buffer.OverflowSize()) {
			FlushCompressionBuffer(); 
		}
	}

	// This removes a file - closes if necessary
	inline void RemoveFile() {
		CloseFile();
		remove(GetFullFileName());
	}

	// Returns the reference name of the file
	inline const char *GetFileName() {
		return m_directory.Buffer();
	}

	// Returns the root file name excluding anyl client extensions
	inline const char *GetRootFileName() {
		int offset = strlen(m_directory.Buffer()) - 1;
		while(m_directory[offset] >= '0' && m_directory[offset] <= '9') {
			offset--;
		}

		offset++;
		memcpy(m_directory.Buffer() + (m_directory.OverflowSize() >> 1),
			m_directory.Buffer(), offset);
		char *ret = m_directory.Buffer() + (m_directory.OverflowSize() >> 1);
		ret[offset] = '\0';

		return ret;
	}

	// Returns the complete directory of the file
	inline const char *GetFullFileName() {
		return HashDirectory(m_directory.Buffer());
	}

	// This sets the reference name for the file
	// @param str - a buffer containing the name of the file
	inline void SetFileName(const char str[]) {
		m_directory.AllocateMemory(2048);
		strcpy(m_directory.Buffer(), str);
	}

	// This copies the contents of this file to another file
	void CopyCompFile(CHDFSFile &file) {

		OpenReadFile();

		int norm_size;
		int compress_size;
		CBaseMemoryChunk<char> compress_buff(compress_size);

		while(true) {

			ReadObject(compress_size);
			if(compress_size < 0) {
				break;
			}

			ReadObject(norm_size); 
			ReadObject(compress_buff.Buffer(), compress_size);

			file.WriteObject(compress_size);
			file.WriteObject(norm_size);
			file.WriteObject(compress_buff.Buffer(), compress_size);
		}
	}

	// This replaces one file with another and stores
	// the new file under the name of the old file
	// @param replace_file - the file that is replacing the 
	//                     - instance of this file 
	//                     - this will be renamed to this instance
	//                     - directory name
	void SubsumeFile(CHDFSFile &replace_file) {
		CloseFile();
		replace_file.CloseFile();
		Sleep(50);
		if(remove(this->GetFullFileName()) != 0) {
			cout<<"Could Not Remove "<<this->GetFullFileName();
		}
		Sleep(5);
		if(rename(replace_file.GetFullFileName(), this->GetFullFileName()) != 0) {
			cout<<"Could Not Rename "<<replace_file.GetFullFileName()
				<<" "<<this->GetFullFileName();getchar();
		}
	}

	// returns true if not more bytes available
	// in the comp file, false otherwise
	inline bool AskEndOfCompFile() {
		return m_comp_offset < 0;
	}

	// Writes a buffer of type X items size times
	// @param size - the number of items to write
	// @param object - a buffer containing the items that need to be writted
	template <class X> void WriteCompObject(X object[], int size) {
		int offset = 0; 
		size *= sizeof(X); 

		while((m_comp_buffer.Size() + (size - offset)) > m_comp_buffer.OverflowSize()) {
			// copies the external buffer into the hit buffer
			memcpy(m_comp_buffer.Buffer() + m_comp_buffer.Size(), (char *)object + offset, 
				(m_comp_buffer.OverflowSize() - m_comp_buffer.Size())); 
			// resizes the buffer
			offset += m_comp_buffer.OverflowSize() - m_comp_buffer.Size(); 
			// resizes the buffer
			m_comp_buffer.Resize(m_comp_buffer.OverflowSize()); 
			FlushCompressionBuffer(); 
		}

		memcpy(m_comp_buffer.Buffer() + m_comp_buffer.Size(),
			(char *)object + offset, size - offset); 
		m_comp_buffer.Resize(m_comp_buffer.Size() + (size - offset)); 
	}

	// writes a single compressoin object of type X
	// @param object - the object of type X to write
	template <class X> inline void WriteCompObject(X object) {
		WriteCompObject(&object, 1);
	}

	// writes a singular object to file of any type
	inline void WriteCompObject5Byte(_int64 &object) {
		WriteCompObject((char *)&object, 5);	
	}

	// writes a singular object to file of any type
	template <class X> inline void WriteObject(X &object) {
		fwrite((const char *)&object, sizeof(X), 1, m_file_ptr); 	
	}
	// writes a buffer of objects to file of a given number 
	template <class X> inline void WriteObject(X *object, int size) {
		fwrite((const char *)object, sizeof(X), size, m_file_ptr); 
	}

	// Reads a buffer of compression objects of type X size times
	// @param object - a buffer containing the items that need to be writted
	// @return false if the object could not be retrieved (end of file)
	template <class X> bool ReadCompObject(X object[], int size) {
		int offset = 0; 
		size *= sizeof(X); 

		while((m_comp_offset + (size - offset)) > m_comp_buffer.Size()) {
			// copies the external buffer into the hit buffer
			memcpy((char *)object + offset, (char *)m_comp_buffer.Buffer() + m_comp_offset, 
				m_comp_buffer.Size() - m_comp_offset); 
			offset += m_comp_buffer.Size() - m_comp_offset; 

			// loads the next compression block in from memory
			if(!GetNextCompressionBlock()) {
				return false;
			}
		}

		memcpy((char *)object + offset, (char *)m_comp_buffer.Buffer() 
			+ m_comp_offset, size - offset); 

		m_comp_offset += size - offset; 
		return true;
	}

	// Reads a compression object of type X
	// @param object - the object of type X to write
	// @return false if the object could not be retrieved (end of file)
	template <class X> inline bool ReadCompObject(X &object) {
		return ReadCompObject(&object, 1);
	}

	// reads a singular object to file of any type
	template <class X> inline bool ReadObject(X &object) {
		return fread((char *)&object, sizeof(X), 1, m_file_ptr) > 0; 
	}

	// reads a buffer of objects to file of a given number 
	template <class X> inline bool ReadObject(X *object, int size) {
		return fread((char *)object, sizeof(X), size, m_file_ptr) > 0; 	
	}

	// seeks a number of bytes from the current position in the file
	inline void SeekReadFileCurrentPosition(_int64 offset) {
		fseeko(m_file_ptr, offset, SEEK_CUR); 
	}
	
	// seeks a number of bytes from the beginning of the file
	inline void SeekReadFileFromBeginning(_int64 offset) {
		fseeko(m_file_ptr, offset, SEEK_SET); 
	}

	// gets the coding associated with an
	// escaped four byte integer
	// @returns the size of the integer in bytes
	int GetEscapedEncoding(char buffer[]) {
		int byte_count = 1;

		while(true) {
			if(!ReadCompObject(*buffer))return -1;
			// checks if finished
			if(((uChar)*buffer & 0x80) == 0)return byte_count; 
			byte_count++; 
			buffer++; 
		}
		return byte_count; 
	}

	// gets an escaped four byte integer
	// @returns the number of bytes used to store it
	int GetEscapedItem(uLong &item) {
		item = 0; 
		int offset = 0; 
		int byte_count = 1; 
		static char hit; 

		while(true) {
			if(!ReadCompObject(hit)) {
				return -1;
			}

			item |= (uLong)(((uChar)(hit & 0x7F)) << offset); 
			// checks if finished
			if(!((uChar)hit & 0x80)) {
				return byte_count; 
			}
			byte_count++; 
			offset += 7; 
		}

		return byte_count; 
	}

	// gets an escaped 56-bit integer and places
	// the results in the buffer
	// @returns the number of bytes used to store it
	int GetEscapedItem(char buffer[]) {
		int offset = 0; 
		uLong item = 0; 
		int byte_count = 1; 
		static char hit; 

		while(offset < 28) {
			if(!ReadCompObject(hit))return -1;
			item |= (uLong)(hit & 0x7F) << offset; 
			// checks if finished
			if(((uChar)hit & 0x80) == 0) {
				*(uLong *)buffer = item;
				return byte_count; 
			}
			byte_count++; 
			offset += 7; 
		}

		*(uLong *)buffer = item;
		offset = 0;
		item = 0;

		while(true) {
			ReadCompObject(hit);
			item |= (uLong)(hit & 0x7F) << offset; 
			if(((uChar)hit & 0x80) == 0)break;
			byte_count++; 
			offset += 7; 
		}

		*(uChar *)(buffer + 3) |= item << 4;
		*(uLong *)(buffer + 4) |= item >> 4;
		return byte_count; 
	}

	// Retrieves next 5-bytes and places in a 8-byte integer
	// @param item - used to store the 8-byte item
	// @return true if more bytes available, false otherwise
	inline bool Get5ByteCompItem(_int64 &item) {
		item = 0; 
		return ReadCompObject((char *)&item, 5); 
	}

	// gets an escaped n-byte integer and places
	// the results in the buffer
	// @returns the number of bytes used to store it
	inline int GetEscapedItem(_int64 &item) {
		item = 0; 
		return GetEscapedItem((char *)&item); 
	}

	// gets an escaped n-byte integer and places
	// the results in the buffer
	// @returns the number of bytes used to store it
	int GetEscapedItem(S5Byte &item) {
		_int64 temp = 0; 
		int bytes = GetEscapedItem(temp); 
		item.SetValue(temp); 
		return bytes; 
	}

	// adds a X byte escaped integer
	// @return the number of bytes added
	template <class X> int AddEscapedItem(X item) {
		if(item < 0)throw EIllegalArgumentException("item < 0"); 

		int bytes = 1; 
		while(item >= 128) {
			WriteCompObject((uChar)(item|0x80)); 
			item >>= 7; 
			bytes++; 
		}

		WriteCompObject((uChar)item); 
		return bytes; 
	}

	// compresses a buffer takes an array of items and compresses 
	// them to the output buffer
	// @param input - an input buffer to compress
	// @param output - the compressed character buffer
	// @param input_size - the size of the input buffer - counts
	// - the number of items not bytes
	template <class X> static int CompressBuffer(X input[], 
		CBaseMemoryChunk<char> &output, int input_size) {

		input_size *= sizeof(X); 
		uLongf length = (uLongf)((input_size * 1.1f) + 13); 
		output.AllocateMemory(length); 

		if(compress2(reinterpret_cast<Bytef *> (output.Buffer()), &length, 
			reinterpret_cast < const Bytef *> (input), input_size, 
			Z_DEFAULT_COMPRESSION) != Z_OK) {
				throw ECompressionException("Could Not Compress Buffer"); 
			}

		return (int)length; 
	}

	// decompresses a buffer taken from input and puts the results in output
	// @param input - the compressed input buffer
	// @param output - the uncompressed output buffer
	// @param input_size - the size of the input buffer in bytes
	// @param output_size - the size of the output buffer in bytes
	template <class X> static void DecompressBuffer(char input[], X output[], 
		int input_size, int output_size) {
		uLongf length = output_size * sizeof(X); 

		if(uncompress(reinterpret_cast<Bytef *> (output), &length, 
			reinterpret_cast<const Bytef *> (input), input_size) != Z_OK) {
				throw ECompressionException("Could Not Decompress Buffer"); 
		}
	}

	// used for testing compression 
	void TestFile() {

		OpenWriteFile((char *)"test_file"); 
		InitializeCompression(10000); 
		CBaseArray<int> buffer; 
		buffer.Initialize(1000); 
		CBaseArray<int> temp; 
		temp.Initialize(99999999); 
		CBaseArray<int> size; 
		size.Initialize(99999999); 

		for(int i=0; i<9999; i++) {
			int length = rand() % 1000; 
			buffer.Resize(0); 
			for(int c=0; c<length; c++) {
				int ge = rand(); 
				buffer.PushBack(ge); 
				temp.PushBack(ge); 
			}

			size.PushBack(length); 
			WriteCompObject(buffer.Buffer(), buffer.Size()); 
		}

		CloseFile(); 
		OpenReadFile((char *)"test_file"); 
		int offset = 0; 

		for(int i=0; i<9999; i++) {
			int length = size[i]; 
			buffer.Resize(length); 
			ReadCompObject(buffer.Buffer(), length); 
			for(int c=0; c<length; c++) {

				if(buffer[c] != temp[offset++]) {
					cout<<"error"; getchar(); 
				}
			}
		}
	}

	// forces the closing of the file
	void CloseFile() {

		m_comp_offset = -1; 
		if(m_bytes_stored < 0 && m_comp_buffer.OverflowSize() > 0) {
			if(m_file_ptr != NULL) {
				FlushCompressionBuffer(true); 
				WriteObject(m_comp_offset);
			}
		}
		
		if(m_file_ptr != NULL) {	
			fclose(m_file_ptr);
			m_file_ptr = NULL;
		}

		m_bytes_stored = -1;
		m_comp_buffer.FreeMemory();
		m_comp_offset = 0;
	}

	virtual ~CHDFSFile() {
		CloseFile(); 
	}
}; 



// This stores the size of comp blocks as they are created 
// so individual comp blocks can be retrieved randomly. It's
// important that serialized objects are stored in an isolated
// comp block to allow of the correct decompression of the object.
class CSegFile : public CHDFSFile {

	// This stores the set of compression block sizes
	CHDFSFile m_comp_block_size;
	// This stores the current byte offset in the comp buffer
	_int64 m_comp_byte_offset;

	// This stores the size of the comp block, compressed and uncompressed
	void StoreCompBlock() {

		if(m_comp_buffer.OverflowSize() == 0) {
			CSegFile::InitializeCompression();
			return;
		}
		
		int norm_size = m_comp_buffer.Size();
		int comp_size = FlushCompressionBuffer();
		m_comp_block_size.WriteCompObject(comp_size);
		m_comp_block_size.WriteCompObject(norm_size);
	}

public:

	CSegFile() {
	}

	CSegFile(const char str[]) {
		SetFileName(str);
	}

	// This opens a read file, it closes any previous files that 
	// @param str - a buffer containing the name of the file
	//            - default NULL means use previously set directory
	void OpenReadFile(const char str[] = NULL) {

		CloseFile();
		CHDFSFile::OpenReadFile(str);

		m_comp_byte_offset = 0;
		m_comp_block_size.OpenReadFile(CUtility::ExtendString
			(GetFileName(), ".comp_size"));
	}

	// This opens a write file, it just stores the directory of the file
	// @param str - a buffer containing the name of the file
	//            - default NULL means use previously set directory
	void OpenWriteFile(const char str[] = NULL) {
		CloseFile();
		CHDFSFile::OpenWriteFile(str);
		m_comp_block_size.OpenWriteFile(CUtility::ExtendString
			(GetFileName(), ".comp_size"));
	}

	// This skips forward to a particualr comp block
	// @param comp_block_num - this is the number of comp blocks
	//                       - to skip forward from
	void SkipForward(int comp_block_num) {

		InitializeCompression();
		SeekReadFileFromBeginning(m_comp_byte_offset);

		int compress_size;
		for(int i=0; i<comp_block_num; i++) {
			m_comp_block_size.ReadCompObject(compress_size);
			m_comp_byte_offset += compress_size;
		}
	}

	// This checks if the byte stream exceeds the comp buffer size 
	// @param buff_size - this stores the byte stream size
	inline void AskBufferOverflow(int buff_size) {

		if((m_comp_buffer.Size() + buff_size) > m_comp_buffer.OverflowSize()) {
			StoreCompBlock();
		}
	}

	// Writes a buffer of compression objects of type X size times
	// @param object - a buffer containing the items that need to be writted
	// @return false if the object could not be retrieved (end of file)
	template <class X> void WriteCompObject(X object[], int size) {

		size *= sizeof(X); 
		if((m_comp_buffer.Size() + size) > m_comp_buffer.OverflowSize()) {
			StoreCompBlock();
		}

		memcpy(m_comp_buffer.Buffer() + m_comp_buffer.Size(),
			(char *)object, size); 
		m_comp_buffer.Resize(m_comp_buffer.Size() + size); 
	}

	// Writes a buffer of compression objects of type X size times
	// @param object - a buffer containing the items that need to be writted
	// @return false if the object could not be retrieved (end of file)
	template <class X> inline void WriteCompObject(X object) {
		WriteCompObject((char *)&object, sizeof(X)); 
	}

	// This closes the seg file
	void CloseFile() {
		StoreCompBlock();
		CHDFSFile::CloseFile();
		m_comp_block_size.CloseFile();
	}

	// This tests the sef file to check that the sum of comp blocks matches
	// the entire size of the file.
	void TestSegFile() {
		CloseFile();

		int norm_size;
		int comp_size;
		m_comp_block_size.OpenReadFile(CUtility::ExtendString
			(GetFileName(), ".comp_size"));

		_int64 sum = 0;
		while(m_comp_block_size.ReadCompObject(comp_size)) {
			m_comp_block_size.ReadCompObject(norm_size);
			sum += norm_size;
		}

		char ch;
		_int64 sum2 = 0;
		CHDFSFile::OpenReadFile();
		while(ReadCompObject(ch)) {
			sum2++;
		}

		if(sum != sum2) {
			cout<<"File Size Mismatch "<<sum<<" "<<sum2;getchar();
		}
	}

	~CSegFile() {
		CloseFile();
	}
};

// This class is used whenever a block of memory
// needs to be allocated to store some set of objects.
// Provides automatic deallocation and size checking
// to prevent the same block of memory being allocated 
// more than once.
template <class X> class CMemoryChunk : public CBaseMemoryChunk<X> {
	
public:

	CMemoryChunk() {
		this->m_memory = NULL; 
		this->m_size = 0; 
	}
	
	CMemoryChunk(const char dir[]) {
		this->m_memory = NULL; 
		this->m_size = 0; 
		ReadMemoryChunkFromFile(dir);
	}

	CMemoryChunk(int size) {
		this->m_memory = NULL; 
		this->m_size = 0; 
		this->AllocateMemory(size); 
	}

	CMemoryChunk(int size, X value) {
		this->m_memory = NULL; 
		this->m_size = 0; 
		this->AllocateMemory(size); 
		this->InitializeMemoryChunk(value); 
	}

	CMemoryChunk(CMemoryChunk<X> &copy) {
		this->m_memory = NULL; 
		this->m_size = 0; 
		this->MakeMemoryChunkEqualTo(copy); 
	}


	// removes the file associated with a memory chunk
	void RemoveMemoryChunk(const char str[]) {
		remove(CUtility::ExtendString(str, ".memory_chunk")); 
	}

	// reads a memory element in from file
	// @param index - the index of the element to retrieve
	// @param element - stores the retrieved element
	// @param item_num - the number of elements to retrieve
	inline void ReadMemoryElementFromFile(const char str[], int index, X &element) {
		ReadMemoryChunkFromFile(str);
		element = this->Element(index);
		this->FreeMemory();
	}

	// reads a memory chunk in from file
	void ReadMemoryChunkFromFile(const char str[]) {
		CHDFSFile file; 
		file.OpenReadFile(CUtility::ExtendString(str, ".memory_chunk")); 
		file.InitializeCompression();
		ReadMemoryChunkFromFile(file);
	}

	// reads a memory chunk in from file
	void ReadMemoryChunkFromFile(CHDFSFile &file) {
		int size;
		file.ReadCompObject(size); 
		this->AllocateMemory(size); 
		file.ReadCompObject(this->Buffer(), size); 
	}

	// writes a memory chunk out to file
	void WriteMemoryChunkToFile(const char str[]) {
		if(this->OverflowSize() <= 0) {
			return; 
		}

		CHDFSFile file; 
		file.OpenWriteFile(CUtility::ExtendString(str, ".memory_chunk")); 
		WriteMemoryChunkToFile(file);
	}

	// writes a memory chunk out to file
	void WriteMemoryChunkToFile(CHDFSFile &file) {
		file.WriteCompObject(this->OverflowSize()); 
		file.WriteCompObject(this->Buffer(), this->OverflowSize()); 
	}
}; 

// this is the single bit version of MemoryChunk
class CBitStringChunk : public CMemoryChunk<uChar> {

public:

	CBitStringChunk() {
	}

	CBitStringChunk(int num_bits) {
		AllocateMemory(num_bits);
	}

	CBitStringChunk(int num_bits, bool value) {
		AllocateMemory(num_bits, value);
	}

	CBitStringChunk(CBitStringChunk &copy) {
		CMemoryChunk<uChar>::MakeMemoryChunkEqualTo(copy);
	}

	// creates a block of memory (in bits)
	inline uChar *AllocateMemory(int num_bits) {
		return CMemoryChunk<uChar>::AllocateMemory((num_bits >> 3) + 1);
	}

	// creates a block of memory (in bits)
	inline uChar *AllocateMemory(int num_bits, bool value) {
		return CMemoryChunk<uChar>::AllocateMemory((num_bits >> 3) + 1, value);
	}

	// retrieves an element
	inline bool operator[](int index) {
		int byte_num = index >> 3;
		if(byte_num >= OverflowSize() || byte_num < 0) {
			throw EIndexOutOfBoundsException("memory chunk overflow"); 
		}

		return (*(Buffer() + byte_num) & (1 << (index % 8))) != 0; 
	}

	// returns the number of bits (rounded to neares byte)
	inline int OverflowSize() {
		return CMemoryChunk<uChar>::OverflowSize() << 3;
	}

	// sets an element to true in the buffer
	void SetBit(int index) {
		int byte_num = index >> 3;
		if(byte_num >= OverflowSize() || byte_num < 0) {
			throw EIndexOutOfBoundsException("memory chunk overflow"); 
		}

		*(Buffer() + byte_num) |= 1 << (index % 8);
	}

	// clear an element to false in the buffer
	void ClearBit(int index) {
		int byte_num = index >> 3;
		if(byte_num >= OverflowSize() || byte_num < 0) {
			throw EIndexOutOfBoundsException("memory chunk overflow"); 
		}

		*(Buffer() + byte_num) &= (uChar)~(1 << (index % 8));
	}

	// this is just a test framework
	void TestBitStringChunk() {

		AllocateMemory(99999, false);
		CMemoryChunk<uChar> compare(99999);

		for(int i=0; i<99999; i++) {
			compare[i] = ((rand() % 2) == 1);
			if(compare[i]) {
				SetBit(i);
			}
		}

		for(int i=0; i<99999; i++) {
			if(compare[i] != operator[](i)) {
				cout<<"error "<<i;getchar();
			}
		}
	}
};

// allocates memory for a single element
// deallocation is handled automatically
template <class X> class CMemoryElement {
	X *p_memory; 
public:

	CMemoryElement() {
		AllocateMemory();
	}

	// returns the memory element
	inline X *operator->() {
		return p_memory; 	
	}

	// This allocates a new element
	void AllocateMemory() {
		try {
			p_memory = new X; 
		} catch(bad_alloc xa) {
			throw EAllocationFailure
				("Memory Chunk Allocation Failure"); 
		}
	}

	// deletes the memory element
	void DeleteMemoryElement() {
		if(p_memory)delete p_memory; 
		p_memory = NULL; 
	}

	~CMemoryElement() {
		if(p_memory)delete p_memory; 
	}
};  

#ifdef OS_WINDOWS 

// This is a general purpose class used for timing.
class CStopWatch {

	// This stores the start and stop time
	struct SStopWatch {
		LARGE_INTEGER start;
		LARGE_INTEGER stop;
	};

	SStopWatch timer;
	LARGE_INTEGER frequency;
	//	This stores the net elapsed time
	double m_net_elapased;

	double LIToSecs(LARGE_INTEGER & L) {
		return ((double)L.QuadPart / (double)frequency.QuadPart);
	}

public:

	CStopWatch() {
		timer.start.QuadPart=0;
		timer.stop.QuadPart=0;	
		QueryPerformanceFrequency( &frequency );
		m_net_elapased = 0;
	}

	// This starts the stopwatch
	void StartTimer() {
		QueryPerformanceCounter(&timer.start);
	}

	// This stops the stopwatch
	void StopTimer() {
		QueryPerformanceCounter(&timer.stop);
		m_net_elapased += GetElapsedTime();
	}

	// This returns the net elapsed time
	double NetElapsedTime() {
		double time = m_net_elapased;
		m_net_elapased = 0;
		return time;
	}

	// This returns the time elapsed in microseconds
	double GetElapsedTime() {
		LARGE_INTEGER time;
		time.QuadPart = timer.stop.QuadPart - timer.start.QuadPart;
		return LIToSecs( time) ;
	}
};

#endif

