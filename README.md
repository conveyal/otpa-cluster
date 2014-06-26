    $ gradle shadowjar (version 1.12+ required)

    start master
    $ java -jar ./build/libs/otpa-cluster-all.jar

    start worker
    $ java -jar -Dconfig.resource=worker.conf ./build/libs/otpa-cluster-all.jar

   	register worker with master
   	http://localhost:8080/addworker/MySystem@127.0.0.1:2553/user/tester

   	start a job
   	http://localhost:8080/find?gtfs=/otpac/austin/capitalmetro.zip&osm=/otpac/austin/austin.osm.pbf

    or alternatively start a job with the web UI:
    http://localhost:8080/index.html

    Convenient defaults are already in place. Just click 'submit' and result show up on the map as they're returned from workers.