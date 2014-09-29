all: DistributeWavePass

CFLAGS=         -g -O0 -c
INCLUDES=       -I /home/Desktop/src/c++/libhdfs \
                -I /usr/lib/jvm/java-6-openjdk/include/ \
                -I /usr/lib/jvm/java-6-openjdk/include/linux/
LDPATH=         -L /home/Desktop/c++/Linux-i386-32/lib/ \
                -L /usr/lib/jvm/java-6-openjdk/jre/lib/i386/client
LIBS=           -lz -pthread

DistributeWavePass: DistributeWavePass.o 
	g++ -o DistributeWavePass DistributeWavePass.o  $(LDPATH) $(LIBS)

DistributeWavePass.o: DistributeWavePass.cpp
	g++ $(CFLAGS) $(INCLUDES) -c DistributeWavePass.cpp


clean:
	rm -rf *o DistributeWavePass
