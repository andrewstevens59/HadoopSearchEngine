all: SearchHitItems

CFLAGS=         -g -O0 -c
INCLUDES=       -I /home/Desktop/src/c++/libhdfs \
                -I /usr/lib/jvm/java-6-openjdk/include/ \
                -I /usr/lib/jvm/java-6-openjdk/include/linux/
LDPATH=         -L /home/Desktop/c++/Linux-i386-32/lib/ \
                -L /usr/lib/jvm/java-6-openjdk/jre/lib/i386/client
LIBS=           -lz -pthread -lsocket -lnsl

SearchHitItems: SearchHitItems.o 
	g++ -o SearchHitItems SearchHitItems.o  $(LDPATH) $(LIBS)

SearchHitItems.o: SearchHitItems.cpp 
	g++ $(CFLAGS) $(INCLUDES) -c SearchHitItems.cpp


clean:
	rm -rf *o SearchHitItems
