all: DyableIndex

CFLAGS=         -g -O0 -c
INCLUDES=       -I /home/Desktop/src/c++/libhdfs \
                -I /usr/lib/jvm/java-6-openjdk/include/ \
                -I /usr/lib/jvm/java-6-openjdk/include/linux/
LDPATH=         -L /home/Desktop/c++/Linux-i386-32/lib/ \
                -L /usr/lib/jvm/java-6-openjdk/jre/lib/i386/client
LIBS=           -lz -pthread

DyableIndex: DyableIndex.o 
	g++ -o DyableIndex DyableIndex.o  $(LDPATH) $(LIBS)

DyableIndex.o: ../../FileStorage.h ../../MyStuff.h DocumentInstance.h Webpage.h LogFile.h ImageStorage.h DyableIndex.cpp
	g++ $(CFLAGS) $(INCLUDES) -c DyableIndex.cpp


clean:
	rm -rf *o DyableIndex
