all: DistributeClusterNodes

CFLAGS=         -g -O0 -c
INCLUDES=       -I /home/Desktop/src/c++/libhdfs \
                -I /usr/lib/jvm/java-6-openjdk/include/ \
                -I /usr/lib/jvm/java-6-openjdk/include/linux/
LDPATH=         -L /home/Desktop/c++/Linux-i386-32/lib/ \
                -L /usr/lib/jvm/java-6-openjdk/jre/lib/i386/client
LIBS=           -lz -pthread

DistributeClusterNodes: DistributeClusterNodes.o 
	g++ -o DistributeClusterNodes DistributeClusterNodes.o  $(LDPATH) $(LIBS)

DistributeClusterNodes.o: DistributeClusterNodes.cpp
	g++ $(CFLAGS) $(INCLUDES) -c DistributeClusterNodes.cpp


clean:
	rm -rf *o DistributeClusterNodes
