all: DyableMPISpawn TestMaster TestSlave

CFLAGS=         -g -O0 -c
INCLUDES=       -I /home/Desktop/src/c++/libhdfs \
                -I /usr/lib/jvm/java-6-openjdk/include/ \
                -I /usr/lib/jvm/java-6-openjdk/include/linux/
LDPATH=         -L /home/Desktop/c++/Linux-i386-32/lib/ \
                -L /usr/lib/jvm/java-6-openjdk/jre/lib/i386/client
LIBS=           -lz -pthread

DyableMPISpawn: DyableMPISpawn.o 
	mpicxx -o DyableMPISpawn DyableMPISpawn.o  $(LDPATH) $(LIBS)

DyableMPISpawn.o: DyableMPISpawn.cpp
	mpicxx $(CFLAGS) $(INCLUDES) -c DyableMPISpawn.cpp

TestMaster: TestMaster.o
	mpicxx -o TestMaster TestMaster.o $(LDPATH) $(LIBS)

TestMaster.o: TestMaster.cpp
	mpicxx $(CFLAGS) $(INCLUDES) -c TestMaster.cpp

TestSlave: TestSlave.o
	mpicxx -o TestSlave TestSlave.o $(LDPATH) $(LIBS)

TestSlave.o: TestSlave.cpp
	mpicxx $(CFLAGS) $(INCLUDES) -c TestSlave.cpp 
clean:
	rm -rf *o DyableMPISpawn
