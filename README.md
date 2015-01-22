# OTPA Cluster: Run OpenTripPlanner Analyst queries on distributed compute infrastructure

Graphs and pointsets are stored in S3.

## Build

    ./gradlew build shadowJar

## Use
### Start an executive

    java -Xmx[several]G -Dotpac.bucket.pointsets=POINTSET_BUCKET -Dotpac.bucket.graphs=GRAPH_BUCKET [-Ds3.credentials.filename=/path/to/s3credentials] -jar build/libs/otpa-cluster-all.jar -h [hostname] -p port

### Start a worker

  java -Xmx[several]G -Dotpac.bucket.pointsets=POINTSET_BUCKET -Dotpac.bucket.graphs=GRAPH_BUCKET [-Ds3.credentials.filename=/path/to/s3credentials] -jar build/libs/otpa-cluster-all.jar -w akka.tcp://<executive>/user/executive

### Use over a network

Make sure to pass in `-Dakka.remote.netty.tcp.hostname=ip.address.of.machine` so that Akka will bind to the correct host.
Unfortunately there is no good way to autodetect this. You can also pass in a custom port (useful for testing, when using
  on a single machine): `-Dakka.remote.netty.tcp.port=port`.

### Use with analyst-server

Generally you'll use this with [analyst-server](https://github.com/conveyal/analyst-server). In that case, the executive is
started by analyst server. See the documentation there for how to start the cluster worker.
