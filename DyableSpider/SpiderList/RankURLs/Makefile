all: RankURLs

CFLAGS=         -g -O0 -c
INCLUDES=       -I /home/Desktop/src/c++/libhdfs \
                -I /usr/lib/jvm/java-6-openjdk/include/ \
                -I /usr/lib/jvm/java-6-openjdk/include/linux/
LDPATH=         -L /home/Desktop/c++/Linux-i386-32/lib/ \
                -L /usr/lib/jvm/java-6-openjdk/jre/lib/i386/client
LIBS=           -lz -pthread

RankURLs: RankURLs.o 
	g++ -o RankURLs RankURLs.o  $(LDPATH) $(LIBS)

RankURLs.o: RankURLs1/RankURLs.cpp
	g++ $(CFLAGS) $(INCLUDES) -c RankURLs1/RankURLs.cpp


clean:
	rm -rf *o RankURLs
