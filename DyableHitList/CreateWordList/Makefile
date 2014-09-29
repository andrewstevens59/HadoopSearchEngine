all: CreateWordList

CFLAGS=         -g -O0 -c
INCLUDES=       -I /home/Desktop/src/c++/libhdfs \
                -I /usr/lib/jvm/java-6-openjdk/include/ \
                -I /usr/lib/jvm/java-6-openjdk/include/linux/
LDPATH=         -L /home/Desktop/c++/Linux-i386-32/lib/ \
                -L /usr/lib/jvm/java-6-openjdk/jre/lib/i386/client
LIBS=           -lz -pthread

CreateWordList: CreateWordList.o 
	g++ -o CreateWordList CreateWordList.o  $(LDPATH) $(LIBS)

CreateWordList.o: CreateWordList.cpp
	g++ $(CFLAGS) $(INCLUDES) -c CreateWordList.cpp


clean:
	rm -rf *o CreateWordList
