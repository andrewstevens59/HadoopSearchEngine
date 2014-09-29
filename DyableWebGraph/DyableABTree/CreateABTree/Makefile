all: CreateABTree

CFLAGS=         -g -O0 -c
INCLUDES=       -I /home/Desktop/src/c++/libhdfs \
                -I /usr/lib/jvm/java-6-openjdk/include/ \
                -I /usr/lib/jvm/java-6-openjdk/include/linux/
LDPATH=         -L /home/Desktop/c++/Linux-i386-32/lib/ \
                -L /usr/lib/jvm/java-6-openjdk/jre/lib/i386/client
LIBS=           -lz -pthread

CreateABTree: TransformHiearchy.o 
	g++ -o CreateABTree TransformHiearchy.o  $(LDPATH) $(LIBS)

TransformHiearchy.o: TransformHiearchy.cpp
	g++ $(CFLAGS) $(INCLUDES) -c TransformHiearchy.cpp


clean:
	rm -rf *o CreateABTree
