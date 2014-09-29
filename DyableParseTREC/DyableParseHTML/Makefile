all: DyableParseHTML

CFLAGS=         -g -O0 -c
INCLUDES=       -I /home/Desktop/src/c++/libhdfs \
                -I /usr/lib/jvm/java-6-openjdk/include/ \
                -I /usr/lib/jvm/java-6-openjdk/include/linux/
LDPATH=        
LIBS=           -lz -pthread

DyableParseHTML: DyableParseHTML.o 
	g++ -o DyableParseHTML DyableParseHTML.o  $(LDPATH) $(LIBS)

DyableParseHTML.o: DyableParseHTML.cpp
	g++ $(CFLAGS) $(INCLUDES) -c DyableParseHTML.cpp


clean:
	rm -rf *o DyableParseHTML
