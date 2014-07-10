Diary of kicking off an instance on ec2

-go to spot instances
-ubuntu server
-m3.medium
-0.25/hr
-make sure to select the cheapest availability zone
-click "review and launch"
-click "edit security group"
-add rule; HTTP
-click "review and launch"
-click "launch"
-pick a key pair you have, or make one, or whatever
-click request spot instance
-wait a few minutes for the spot request to be fulfilled

connect to the instance

$ssh -i ~/.ssh/your-key.pem ubuntu@ec2-xx-xx-xx-xx.compute-1.amazonaws.com

get the instance set up

$ sudo add-apt-repository ppa:webupd8team/java
$ sudo apt-get update
$ sudo apt-get install oracle-java7-installer
$ sudo apt-get install oracle-java7-set-default

$ sudo apt-get install git

- get gradle 1.12, which isn't in the ubuntu repo
$ mkdir downloads
$ cd downloads
$ wget https://services.gradle.org/distributions/gradle-1.12-bin.zip
$ unzip gradle-1.12-bin.zip

- <set up a new ssh key on github for the cluster instance>

$ cd ~
$ git clone git@github.com:conveyal/otpa-cluster.git
$ cd otpa-cluster

- install AWS credentials

$ vim s3credentials

the credentials file takes this form:

[default]
aws_access_key_id=YOUR_ACCESS_KEY_ID
aws_secret_access_key=YOUR_SECRET_ACCESS_KEY

- test otpa-cluster

$ ~/downloads/gradle-1.12/bin/gradle test

- package it up

$ ~/downloads/gradle-1.12/bin/gradle shadowjar

- get the public DNS domain name for the instance, for example "ec2-54-89-61-120.compute-1.amazonaws.com"

$ java -jar -Dconfig.resource=worker.conf ./build/libs/otpa-cluster-all.jar ec2-54-89-61-120.compute-1.amazonaws.com

- on the master machine:

$ java -jar ./build/libs/otpa-cluster-all.jar

- associate the master with the worker:

http://localhost:8080/addworker/?path=MySystem@ec2-54-89-61-120.compute-1.amazonaws.com:2553/user/manager

==Starting from an image==

Start from AMI ID ami-945e9efc / AMI name "otpac-worker-v001"

log in

$ cd otpa-cluster
$ java -jar -Dconfig.resource=worker.conf ./build/libs/otpa-cluster-all.jar <INSTANCE PUBLIC DNS>

