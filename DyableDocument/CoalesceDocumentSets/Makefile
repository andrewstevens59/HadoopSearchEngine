all: CoalesceDocumentSets

CFLAGS=         -g -O0 -c
INCLUDES=       -I /home/Desktop/src/c++/libhdfs \
                -I /usr/lib/jvm/java-6-openjdk/include/ \
                -I /usr/lib/jvm/java-6-openjdk/include/linux/
LDPATH=         -L /home/Desktop/c++/Linux-i386-32/lib/ \
                -L /usr/lib/jvm/java-6-openjdk/jre/lib/i386/client
LIBS=           -lz -pthread

CoalesceDocumentSets: CoalesceDocumentSets.o 
	g++ -o CoalesceDocumentSets CoalesceDocumentSets.o  $(LDPATH) $(LIBS)

CoalesceDocumentSets.o: CoalesceDocumentSets1/CoalesceDocumentSets.cpp
	g++ $(CFLAGS) $(INCLUDES) -c CoalesceDocumentSets1/CoalesceDocumentSets.cpp


clean:
	rm -rf *o CoalesceDocumentSets
