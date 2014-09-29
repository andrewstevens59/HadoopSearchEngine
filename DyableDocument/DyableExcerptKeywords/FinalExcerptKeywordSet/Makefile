all: FinalExcerptKeywordSet

CFLAGS=         -g -O0 -c
INCLUDES=       -I /home/Desktop/src/c++/libhdfs \
                -I /usr/lib/jvm/java-6-openjdk/include/ \
                -I /usr/lib/jvm/java-6-openjdk/include/linux/
LDPATH=         -L /home/Desktop/c++/Linux-i386-32/lib/ \
                -L /usr/lib/jvm/java-6-openjdk/jre/lib/i386/client
LIBS=           -lz -pthread

FinalExcerptKeywordSet: FinalExcerptKeywordSet.o 
	g++ -o FinalExcerptKeywordSet FinalExcerptKeywordSet.o  $(LDPATH) $(LIBS)

FinalExcerptKeywordSet.o: FinalExcerptKeywordSet1/FinalExcerptKeywordSet.cpp
	g++ $(CFLAGS) $(INCLUDES) -c FinalExcerptKeywordSet1/FinalExcerptKeywordSet.cpp


clean:
	rm -rf *o FinalExcerptKeywordSet
