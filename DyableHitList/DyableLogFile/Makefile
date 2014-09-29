all: DyableLogFile

CFLAGS=         -g -O0 -c
INCLUDES=       -I /home/Desktop/src/c++/libhdfs \
                -I /usr/lib/jvm/java-6-openjdk/include/ \
                -I /usr/lib/jvm/java-6-openjdk/include/linux/
LDPATH=         -L /home/Desktop/c++/Linux-i386-32/lib/ \
                -L /usr/lib/jvm/java-6-openjdk/jre/lib/i386/client
LIBS=           -lz -pthread

DyableLogFile: DyableLogFile.o 
	g++ -o DyableLogFile DyableLogFile.o  $(LDPATH) $(LIBS)

DyableLogFile.o: IndexLogFile.h WordLogEntry.h ../../DyableDocument/DyableIndex/LogFile.h DyableLogFile.cpp
	g++ $(CFLAGS) $(INCLUDES) -c DyableLogFile.cpp


clean:
	rm -rf *o DyableLogFile
