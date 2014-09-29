all: AccumulateClusterNodes

CFLAGS=         -g -O0 -c
INCLUDES=       -I /home/Desktop/src/c++/libhdfs \
                -I /usr/lib/jvm/java-6-openjdk/include/ \
                -I /usr/lib/jvm/java-6-openjdk/include/linux/
LDPATH=         -L /home/Desktop/c++/Linux-i386-32/lib/ \
                -L /usr/lib/jvm/java-6-openjdk/jre/lib/i386/client
LIBS=           -lz -pthread

AccumulateClusterNodes: AccumulateClusterNodes.o 
	g++ -o AccumulateClusterNodes AccumulateClusterNodes.o  $(LDPATH) $(LIBS)

AccumulateClusterNodes.o: AccumulateClusterNodes.cpp
	g++ $(CFLAGS) $(INCLUDES) -c AccumulateClusterNodes.cpp


clean:
	rm -rf *o AccumulateClusterNodes
