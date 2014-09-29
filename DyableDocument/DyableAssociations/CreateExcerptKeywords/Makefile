all: CreateExcerptKeywords

CFLAGS=         -g -O0 -c
INCLUDES=       -I /home/Desktop/src/c++/libhdfs \
                -I /usr/lib/jvm/java-6-openjdk/include/ \
                -I /usr/lib/jvm/java-6-openjdk/include/linux/
LDPATH=         -L /home/Desktop/c++/Linux-i386-32/lib/ \
                -L /usr/lib/jvm/java-6-openjdk/jre/lib/i386/client
LIBS=           -lz -pthread

CreateExcerptKeywords: CreateExcerptKeywords.o 
	g++ -o CreateExcerptKeywords CreateExcerptKeywords.o  $(LDPATH) $(LIBS)

CreateExcerptKeywords.o: CreateExcerptKeywords1/CreateExcerptKeywords.cpp
	g++ $(CFLAGS) $(INCLUDES) -c CreateExcerptKeywords1/CreateExcerptKeywords.cpp


clean:
	rm -rf *o CreateExcerptKeywords
