all: GroupTerms

CFLAGS=         -g -O0 -c
INCLUDES=       -I /home/Desktop/src/c++/libhdfs \
                -I /usr/lib/jvm/java-6-openjdk/include/ \
                -I /usr/lib/jvm/java-6-openjdk/include/linux/
LDPATH=         -L /home/Desktop/c++/Linux-i386-32/lib/ \
                -L /usr/lib/jvm/java-6-openjdk/jre/lib/i386/client
LIBS=           -lz -pthread

GroupTerms: GroupTerms.o 
	g++ -o GroupTerms GroupTerms.o  $(LDPATH) $(LIBS)

GroupTerms.o: GroupTerms1/GroupTerms.cpp
	g++ $(CFLAGS) $(INCLUDES) -c GroupTerms1/GroupTerms.cpp


clean:
	rm -rf *o GroupTerms
