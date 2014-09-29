all: FilterAssociations

CFLAGS=         -g -O0 -c
INCLUDES=       -I /home/Desktop/src/c++/libhdfs \
                -I /usr/lib/jvm/java-6-openjdk/include/ \
                -I /usr/lib/jvm/java-6-openjdk/include/linux/
LDPATH=         -L /home/Desktop/c++/Linux-i386-32/lib/ \
                -L /usr/lib/jvm/java-6-openjdk/jre/lib/i386/client
LIBS=           -lz -pthread

FilterAssociations: FilterAssociations.o 
	g++ -o FilterAssociations FilterAssociations.o  $(LDPATH) $(LIBS)

FilterAssociations.o: FilterAssociations1/FilterAssociations.cpp
	g++ $(CFLAGS) $(INCLUDES) -c FilterAssociations1/FilterAssociations.cpp


clean:
	rm -rf *o FilterAssociations
