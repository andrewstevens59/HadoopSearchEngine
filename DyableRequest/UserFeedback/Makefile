all: UserFeedback

CFLAGS=         -g -O0 -c
INCLUDES=       -I /home/Desktop/src/c++/libhdfs \
                -I /usr/lib/jvm/java-6-openjdk/include/ \
                -I /usr/lib/jvm/java-6-openjdk/include/linux/
LDPATH=         -L /home/Desktop/c++/Linux-i386-32/lib/ \
                -L /usr/lib/jvm/java-6-openjdk/jre/lib/i386/client
LIBS=           -lz -pthread -lsocket -lnsl

UserFeedback: Query.o 
	g++ -o UserFeedback Query.o  $(LDPATH) $(LIBS)

Query.o: Query.cpp 
	g++ $(CFLAGS) $(INCLUDES) -c Query.cpp


clean:
	rm -rf *o UserFeedback
