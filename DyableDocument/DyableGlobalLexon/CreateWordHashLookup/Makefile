all: CreateWordHashLookup

CFLAGS=         -g -O0 -c
INCLUDES=       -I /home/Desktop/src/c++/libhdfs \
                -I /usr/lib/jvm/java-6-openjdk/include/ \
                -I /usr/lib/jvm/java-6-openjdk/include/linux/
LDPATH=         -L /home/Desktop/c++/Linux-i386-32/lib/ \
                -L /usr/lib/jvm/java-6-openjdk/jre/lib/i386/client
LIBS=           -lz -pthread

CreateWordHashLookup: CreateWordHashLookup.o 
	g++ -o CreateWordHashLookup CreateWordHashLookup.o  $(LDPATH) $(LIBS)

CreateWordHashLookup.o: CreateWordHashLookup.cpp
	g++ $(CFLAGS) $(INCLUDES) -c CreateWordHashLookup.cpp


clean:
	rm -rf *o CreateWordHashLookup
