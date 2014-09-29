all: ReduceExcerptKeywordSet

CFLAGS=         -g -O0 -c
INCLUDES=       -I /home/Desktop/src/c++/libhdfs \
                -I /usr/lib/jvm/java-6-openjdk/include/ \
                -I /usr/lib/jvm/java-6-openjdk/include/linux/
LDPATH=         -L /home/Desktop/c++/Linux-i386-32/lib/ \
                -L /usr/lib/jvm/java-6-openjdk/jre/lib/i386/client
LIBS=           -lz -pthread

ReduceExcerptKeywordSet: ReduceExcerptKeywordSet.o 
	g++ -o ReduceExcerptKeywordSet ReduceExcerptKeywordSet.o  $(LDPATH) $(LIBS)

ReduceExcerptKeywordSet.o: ReduceExcerptKeywordSet1/ReduceExcerptKeywordSet.cpp
	g++ $(CFLAGS) $(INCLUDES) -c ReduceExcerptKeywordSet1/ReduceExcerptKeywordSet.cpp


clean:
	rm -rf *o ReduceExcerptKeywordSet
