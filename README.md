    $ gradle fatjar

    start worker
    $ java -jar ./build/libs/findprimes.jar

    start master
    $ java -jar -Dconfig.resource=server.conf ./build/libs/findprimes.jar
    
    master will send a message to worker to find a prime