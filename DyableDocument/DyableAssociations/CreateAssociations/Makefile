all: CreateAssociations

CFLAGS=         -g -O0 -c
INCLUDES=       -I /home/Desktop/src/c++/libhdfs \
                -I /usr/lib/jvm/java-6-openjdk/include/ \
                -I /usr/lib/jvm/java-6-openjdk/include/linux/
LDPATH=         -L /home/Desktop/c++/Linux-i386-32/lib/ \
                -L /usr/lib/jvm/java-6-openjdk/jre/lib/i386/client
LIBS=           -lz -pthread

CreateAssociations: CreateAssociations.o 
	g++ -o CreateAssociations CreateAssociations.o  $(LDPATH) $(LIBS)

CreateAssociations.o: CreateAssociations1/CreateAssociations.cpp
	g++ $(CFLAGS) $(INCLUDES) -c CreateAssociations1/CreateAssociations.cpp


clean:
	rm -rf *o CreateAssociations
