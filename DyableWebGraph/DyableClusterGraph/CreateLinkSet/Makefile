all: CreateLinkSet

CFLAGS=         -g -O0 -c
INCLUDES=       -I /home/Desktop/src/c++/libhdfs \
                -I /usr/lib/jvm/java-6-openjdk/include/ \
                -I /usr/lib/jvm/java-6-openjdk/include/linux/
LDPATH=         -L /home/Desktop/c++/Linux-i386-32/lib/ \
                -L /usr/lib/jvm/java-6-openjdk/jre/lib/i386/client
LIBS=           -lz -pthread

CreateLinkSet: CreateLinkSet.o 
	g++ -o CreateLinkSet CreateLinkSet.o  $(LDPATH) $(LIBS)

CreateLinkSet.o: CreateLinkSet.cpp
	g++ $(CFLAGS) $(INCLUDES) -c CreateLinkSet.cpp


clean:
	rm -rf *o CreateLinkSet
