## All the code is in src\main\java\com\virtualpairprogrammers\Main.java


## Streaming code is under Streaming package in Virtual Programming

### Imp - everytime before running log analysis run loggingServer and stop it after stopping the analysis


Here every time we are creating a RDD but inreality the rdd is created only at the end of the code


uses a master-slave architecture that consists of a driver, which runs as a master node, and many executors that run across as worker nodes in the cluster


The driver is the program or process responsible for coordinating the execution of the Spark application. It runs the main function and creates the SparkContext, which connects to the cluster manager.

Executors are worker processes responsible for executing tasks in Spark applications. They are launched on worker nodes and communicate with the driver program and cluster manager. Executors run tasks concurrently and store data in memory or disk for caching and intermediate storage.

The cluster manager is responsible for allocating resources and managing the cluster on which the Spark application runs


### working

Driver Program in the Apache Spark architecture executes, it calls the real program of an application and creates a SparkContext which are responsible for translating user-written code into jobs that are actually executed on the cluster

SparkContext receives task information from the Cluster Manager and enqueues it on worker nodes. The executor is in charge of carrying out these duties. The lifespan of executors is the same as that of the Spark Application