mesos-docker-tutorial
=====================

Tutorial on building a Mesos framework in Java to launch Docker containers on slaves.

The full tutorial will be available on the CodeFutures web site shortly.

Here are some brief instructions to running this code.

First, install mesos 0.20.1 and Docker 1.0.0 or greater. This code was tested with Docker 1.2.0.

Start the Mesos master and slave:

    nohup mesos-master --ip=127.0.0.1 --work_dir=/tmp >mesos-master.log 2>&1 &

    nohup mesos-slave --master=127.0.0.1:5050 --containerizers=docker,mesos >mesos-slave.log 2>&1 &

Build the framework:

    mvn package

Run the framework:

    java -classpath target/cf-tutorial-mesos-docker-1.0-SNAPSHOT-jar-with-dependencies.jar com.codefutures.tutorial.mesos.docker.ExampleFramework 127.0.0.1:5050 fedora/apache 2

