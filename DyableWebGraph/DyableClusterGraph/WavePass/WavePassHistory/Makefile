all: WavePassHistory

CFLAGS=         -g -O0 -c
INCLUDES=       -I /home/Desktop/src/c++/libhdfs \
                -I /usr/lib/jvm/java-6-openjdk/include/ \
                -I /usr/lib/jvm/java-6-openjdk/include/linux/
LDPATH=         -L /home/Desktop/c++/Linux-i386-32/lib/ \
                -L /usr/lib/jvm/java-6-openjdk/jre/lib/i386/client
LIBS=           -lz -pthread

WavePassHistory: WavePassHistory.o 
	g++ -o WavePassHistory WavePassHistory.o  $(LDPATH) $(LIBS)

WavePassHistory.o: WavePassHistory1/WavePassHistory.cpp
	g++ $(CFLAGS) $(INCLUDES) -c WavePassHistory1/WavePassHistory.cpp


clean:
	rm -rf *o WavePassHistory
