all: DocumentQuery

CFLAGS=         -g -O0 -c
INCLUDES=       -I /home/Desktop/src/c++/libhdfs \
                -I /usr/lib/jvm/java-6-openjdk/include/ \
                -I /usr/lib/jvm/java-6-openjdk/include/linux/
LDPATH=         -L /home/Desktop/c++/Linux-i386-32/lib/ \
                -L /usr/lib/jvm/java-6-openjdk/jre/lib/i386/client
LIBS=           -lz -pthread -lsocket -lnsl

DocumentQuery: DocumentQuery.o 
	g++ -o DocumentQuery DocumentQuery.o  $(LDPATH) $(LIBS)

DocumentQuery.o: DocumentQuery.cpp DocumentInstance.h
	g++ $(CFLAGS) $(INCLUDES) -c DocumentQuery.cpp


clean:
	rm -rf *o DocumentQuery
