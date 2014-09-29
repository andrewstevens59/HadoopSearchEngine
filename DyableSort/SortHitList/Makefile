all: SortHitList

CFLAGS=         -g -O0 -c
INCLUDES=       -I /home/Desktop/src/c++/libhdfs \
                -I /usr/lib/jvm/java-6-openjdk/include/ \
                -I /usr/lib/jvm/java-6-openjdk/include/linux/
LDPATH=         -L /home/Desktop/c++/Linux-i386-32/lib/ \
                -L /usr/lib/jvm/java-6-openjdk/jre/lib/i386/client
LIBS=           -lz -pthread

SortHitList: SortHitList.o 
	g++ -o SortHitList SortHitList.o  $(LDPATH) $(LIBS)

SortHitList.o: SortHitList.cpp
	g++ $(CFLAGS) $(INCLUDES) -c SortHitList.cpp


clean:
	rm -rf *o SortHitList
