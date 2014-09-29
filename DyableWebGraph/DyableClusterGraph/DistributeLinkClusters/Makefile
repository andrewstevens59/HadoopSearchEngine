all: DistributeLinkClusters

CFLAGS=         -g -O0 -c
INCLUDES=       -I /home/Desktop/src/c++/libhdfs \
                -I /usr/lib/jvm/java-6-openjdk/include/ \
                -I /usr/lib/jvm/java-6-openjdk/include/linux/
LDPATH=         -L /home/Desktop/c++/Linux-i386-32/lib/ \
                -L /usr/lib/jvm/java-6-openjdk/jre/lib/i386/client
LIBS=           -lz -pthread

DistributeLinkClusters: DistributeLinkClusters.o 
	g++ -o DistributeLinkClusters DistributeLinkClusters.o  $(LDPATH) $(LIBS)

DistributeLinkClusters.o: DistributeLinkClusters1/DistributeLinkClusters.cpp
	g++ $(CFLAGS) $(INCLUDES) -c DistributeLinkClusters1/DistributeLinkClusters.cpp


clean:
	rm -rf *o DistributeLinkClusters
