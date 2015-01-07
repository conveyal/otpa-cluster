# OTPA Cluster: Run OpenTripPlanner Analyst queries on distributed compute infrastructure

Graphs and pointsets are stored in S3.

## Build

    $ gradle build fatJar

## Use
### Start an executive

    java -Xmx[several]G -Dotpac.bucket.pointsets=POINTSET_BUCKET -Dotpac.bucket.graphs=GRAPH_BUCKET [-Ds3.credentials.filename=/path/to/s3credentials] -jar build/libs/otpa-cluster-all.jar -h [hostname] -p port

### Start a worker

  java -Xmx[several]G -Dotpac.bucket.pointsets=POINTSET_BUCKET -Dotpac.bucket.graphs=GRAPH_BUCKET [-Ds3.credentials.filename=/path/to/s3credentials] -jar build/libs/otpa-cluster-all.jar -w akka.tcp://<executive>/user/executive

### Use with analyst-server

Generally you'll use this with [analyst-server](https://github.com/conveyal/analyst-server). In that case, the executive is
started by analyst server. See the documentation there for how to start the cluster worker.
