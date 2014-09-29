all: CreateClusterNodeMap

CFLAGS=         -g -O0 -c
INCLUDES=       -I /home/Desktop/src/c++/libhdfs \
                -I /usr/lib/jvm/java-6-openjdk/include/ \
                -I /usr/lib/jvm/java-6-openjdk/include/linux/
LDPATH=         -L /home/Desktop/c++/Linux-i386-32/lib/ \
                -L /usr/lib/jvm/java-6-openjdk/jre/lib/i386/client
LIBS=           -lz -pthread

CreateClusterNodeMap: CreateClusterNodeMap.o 
	g++ -o CreateClusterNodeMap CreateClusterNodeMap.o  $(LDPATH) $(LIBS)

CreateClusterNodeMap.o: CreateClusterNodeMap1/CreateClusterNodeMap.cpp
	g++ $(CFLAGS) $(INCLUDES) -c CreateClusterNodeMap1/CreateClusterNodeMap.cpp


clean:
	rm -rf *o CreateClusterNodeMap
