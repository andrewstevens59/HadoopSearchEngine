all: CreateJoinLinks

CFLAGS=         -g -O0 -c
INCLUDES=       -I /home/Desktop/src/c++/libhdfs \
                -I /usr/lib/jvm/java-6-openjdk/include/ \
                -I /usr/lib/jvm/java-6-openjdk/include/linux/
LDPATH=         -L /home/Desktop/c++/Linux-i386-32/lib/ \
                -L /usr/lib/jvm/java-6-openjdk/jre/lib/i386/client
LIBS=           -lz -pthread

CreateJoinLinks: CreateJoinLinks.o 
	g++ -o CreateJoinLinks CreateJoinLinks.o  $(LDPATH) $(LIBS)

CreateJoinLinks.o: CreateJoinLinks.cpp
	g++ $(CFLAGS) $(INCLUDES) -c CreateJoinLinks.cpp


clean:
	rm -rf *o CreateJoinLinks
