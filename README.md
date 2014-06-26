    $ gradle shadowjar (version 1.12+ required)

    start master
    $ java -jar ./build/libs/otpa-cluster-all.jar

    start worker
    $ java -jar -Dconfig.resource=worker.conf ./build/libs/otpa-cluster-all.jar

   	register worker with master
   	http://localhost:8000/addworker/MySystem@127.0.0.1:2553/user/tester

   	start a job
   	http://localhost:8000/find?gtfs=/otpac/austin/capitalmetro.zip&osm=/otpac/austin/austin.osm.pbf

    get status of jobs
    http://localhost:8000/jobstatus

   	get results of the job
   	http://localhost:8000/jobresult/0 
