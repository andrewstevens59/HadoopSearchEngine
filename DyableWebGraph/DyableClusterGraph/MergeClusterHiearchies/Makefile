all: MergeClusterHiearchies

CFLAGS=         -g -O0 -c
INCLUDES=       -I /home/Desktop/src/c++/libhdfs \
                -I /usr/lib/jvm/java-6-openjdk/include/ \
                -I /usr/lib/jvm/java-6-openjdk/include/linux/
LDPATH=         -L /home/Desktop/c++/Linux-i386-32/lib/ \
                -L /usr/lib/jvm/java-6-openjdk/jre/lib/i386/client
LIBS=           -lz -pthread

MergeClusterHiearchies: MergeClusterHiearchies.o 
	g++ -o MergeClusterHiearchies MergeClusterHiearchies.o  $(LDPATH) $(LIBS)

MergeClusterHiearchies.o: MergeClusterHiearchies1/MergeClusterHiearchies.cpp
	g++ $(CFLAGS) $(INCLUDES) -c MergeClusterHiearchies1/MergeClusterHiearchies.cpp


clean:
	rm -rf *o MergeClusterHiearchies
