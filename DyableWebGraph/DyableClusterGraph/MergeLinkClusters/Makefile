all: MergeLinkClusters

CFLAGS=         -g -O0 -c
INCLUDES=       -I /home/Desktop/src/c++/libhdfs \
                -I /usr/lib/jvm/java-6-openjdk/include/ \
                -I /usr/lib/jvm/java-6-openjdk/include/linux/
LDPATH=         -L /home/Desktop/c++/Linux-i386-32/lib/ \
                -L /usr/lib/jvm/java-6-openjdk/jre/lib/i386/client
LIBS=           -lz -pthread

MergeLinkClusters: MergeLinkClusters.o 
	g++ -o MergeLinkClusters MergeLinkClusters.o  $(LDPATH) $(LIBS)

MergeLinkClusters.o: MergeLinkClusters1/MergeLinkClusters.cpp
	g++ $(CFLAGS) $(INCLUDES) -c MergeLinkClusters1/MergeLinkClusters.cpp


clean:
	rm -rf *o MergeLinkClusters
