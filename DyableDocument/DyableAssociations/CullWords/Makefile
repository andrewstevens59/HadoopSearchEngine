all: CullWords

CFLAGS=         -g -O0 -c
INCLUDES=       -I /home/Desktop/src/c++/libhdfs \
                -I /usr/lib/jvm/java-6-openjdk/include/ \
                -I /usr/lib/jvm/java-6-openjdk/include/linux/
LDPATH=         -L /home/Desktop/c++/Linux-i386-32/lib/ \
                -L /usr/lib/jvm/java-6-openjdk/jre/lib/i386/client
LIBS=           -lz -pthread

CullWords: CullWords.o 
	g++ -o CullWords CullWords.o  $(LDPATH) $(LIBS)

CullWords.o: CullWords.cpp
	g++ $(CFLAGS) $(INCLUDES) -c CullWords.cpp


clean:
	rm -rf *o CullWords
