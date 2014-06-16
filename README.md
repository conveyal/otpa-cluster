    $ gradle fatjar

    start master
    $ java -jar -Dconfig.resource=server.conf ./build/libs/findprimes.jar

    start worker
    $ java -jar ./build/libs/findprimes.jar

   	register worker with master
   	http://localhost:8000/addworker/MySystem@127.0.0.1:2553/user/tester

   	start a job
   	http://localhost:8000/find/1/100

   	get results of the job
   	http://localhost:8000/jobresult/0 
    
    master will send a message to worker to find a prime