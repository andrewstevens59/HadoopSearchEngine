all: CreateClusteredLinkSet

CFLAGS=         -g -O0 -c
INCLUDES=       -I /home/Desktop/src/c++/libhdfs \
                -I /usr/lib/jvm/java-6-openjdk/include/ \
                -I /usr/lib/jvm/java-6-openjdk/include/linux/
LDPATH=         -L /home/Desktop/c++/Linux-i386-32/lib/ \
                -L /usr/lib/jvm/java-6-openjdk/jre/lib/i386/client
LIBS=           -lz -pthread

CreateClusteredLinkSet: CreateClusteredLinkSet.o 
	g++ -o CreateClusteredLinkSet CreateClusteredLinkSet.o  $(LDPATH) $(LIBS)

CreateClusteredLinkSet.o: CreateClusteredLinkSet.cpp
	g++ $(CFLAGS) $(INCLUDES) -c CreateClusteredLinkSet.cpp


clean:
	rm -rf *o CreateClusteredLinkSet
