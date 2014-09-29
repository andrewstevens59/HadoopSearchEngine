all: DistributeClusterHiearchies

CFLAGS=         -g -O0 -c
INCLUDES=       -I /home/Desktop/src/c++/libhdfs \
                -I /usr/lib/jvm/java-6-openjdk/include/ \
                -I /usr/lib/jvm/java-6-openjdk/include/linux/
LDPATH=         -L /home/Desktop/c++/Linux-i386-32/lib/ \
                -L /usr/lib/jvm/java-6-openjdk/jre/lib/i386/client
LIBS=           -lz -pthread

DistributeClusterHiearchies: DistributeClusterHiearchies.o 
	g++ -o DistributeClusterHiearchies DistributeClusterHiearchies.o  $(LDPATH) $(LIBS)

DistributeClusterHiearchies.o: DistributeClusterHiearchies1/DistributeClusterHiearchies.cpp
	g++ $(CFLAGS) $(INCLUDES) -c DistributeClusterHiearchies1/DistributeClusterHiearchies.cpp


clean:
	rm -rf *o DistributeClusterHiearchies
