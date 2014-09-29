all: DistributeDomainName

CFLAGS=         -g -O0 -c
INCLUDES=       -I /home/Desktop/src/c++/libhdfs \
                -I /usr/lib/jvm/java-6-openjdk/include/ \
                -I /usr/lib/jvm/java-6-openjdk/include/linux/
LDPATH=         -L /home/Desktop/c++/Linux-i386-32/lib/ \
                -L /usr/lib/jvm/java-6-openjdk/jre/lib/i386/client
LIBS=           -lz -pthread

DistributeDomainName: DistributeDomainName.o 
	g++ -o DistributeDomainName DistributeDomainName.o  $(LDPATH) $(LIBS)

DistributeDomainName.o: DistributeDomainName.cpp
	g++ $(CFLAGS) $(INCLUDES) -c DistributeDomainName.cpp


clean:
	rm -rf *o DistributeDomainName
