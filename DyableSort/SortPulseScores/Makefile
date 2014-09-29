all: SortPulseScores

CFLAGS=         -g -O0 -c
INCLUDES=       -I /home/Desktop/src/c++/libhdfs \
                -I /usr/lib/jvm/java-6-openjdk/include/ \
                -I /usr/lib/jvm/java-6-openjdk/include/linux/
LDPATH=         -L /home/Desktop/c++/Linux-i386-32/lib/ \
                -L /usr/lib/jvm/java-6-openjdk/jre/lib/i386/client
LIBS=           -lz -pthread

SortPulseScores: SortPulseScores.o 
	g++ -o SortPulseScores SortPulseScores.o  $(LDPATH) $(LIBS)

SortPulseScores.o: SortPulseScores1/SortPulseScores.cpp
	g++ $(CFLAGS) $(INCLUDES) -c SortPulseScores1/SortPulseScores.cpp


clean:
	rm -rf *o SortPulseScores
