/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.codefutures.tutorial.mesos.docker;

import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/** Example scheduler to launch Docker containers. */
public class ExampleScheduler implements Scheduler {

  /** Logger. */
  private static final Logger logger = LoggerFactory.getLogger(ExampleScheduler.class);

  /** Docker image name e..g. "fedora/apache". */
  private final String imageName;

  /** Number of instances to run. */
  private final int desiredInstances;

  /** Number of instances pending . */
  private final List<String> pendingInstances = new ArrayList<>();

  /** Number of instances running. */
  private final List<String> runningInstances = new ArrayList<>();

  /** Task ID generator. */
  private final AtomicInteger taskIDGenerator = new AtomicInteger();

  /** Constructor. */
  public ExampleScheduler(String imageName, int desiredInstances) {
    this.imageName = imageName;
    this.desiredInstances = desiredInstances;
  }

  @Override
  public void registered(SchedulerDriver schedulerDriver, Protos.FrameworkID frameworkID, Protos.MasterInfo masterInfo) {
    logger.info("registered() master={}:{}, framework={}", masterInfo.getIp(), masterInfo.getPort(), frameworkID);
  }

  @Override
  public void reregistered(SchedulerDriver schedulerDriver, Protos.MasterInfo masterInfo) {
    logger.info("reregistered()");
  }

  @Override
  public void resourceOffers(SchedulerDriver schedulerDriver, List<Protos.Offer> offers) {

    logger.info("resourceOffers() with {} offers", offers.size());

    for (Protos.Offer offer : offers) {

      List<Protos.TaskInfo> tasks = new ArrayList<>();
      if (runningInstances.size() + pendingInstances.size() < desiredInstances) {

        // generate a unique task ID
        Protos.TaskID taskId = Protos.TaskID.newBuilder()
            .setValue(Integer.toString(taskIDGenerator.incrementAndGet())).build();

        logger.info("Launching task {}", taskId.getValue());
        pendingInstances.add(taskId.getValue());

        // docker image info
        Protos.ContainerInfo.DockerInfo.Builder dockerInfoBuilder = Protos.ContainerInfo.DockerInfo.newBuilder();
        dockerInfoBuilder.setImage(imageName);
        dockerInfoBuilder.setNetwork(Protos.ContainerInfo.DockerInfo.Network.BRIDGE);

        // container info
        Protos.ContainerInfo.Builder containerInfoBuilder = Protos.ContainerInfo.newBuilder();
        containerInfoBuilder.setType(Protos.ContainerInfo.Type.DOCKER);
        containerInfoBuilder.setDocker(dockerInfoBuilder.build());

        // create task to run
        Protos.TaskInfo task = Protos.TaskInfo.newBuilder()
            .setName("task " + taskId.getValue())
            .setTaskId(taskId)
            .setSlaveId(offer.getSlaveId())
            .addResources(Protos.Resource.newBuilder()
                .setName("cpus")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(1)))
            .addResources(Protos.Resource.newBuilder()
                .setName("mem")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(128)))
            .setContainer(containerInfoBuilder)
            .setCommand(Protos.CommandInfo.newBuilder().setShell(false))
            .build();

        tasks.add(task);
      }
      Protos.Filters filters = Protos.Filters.newBuilder().setRefuseSeconds(1).build();
      schedulerDriver.launchTasks(offer.getId(), tasks, filters);
    }
  }

  @Override
  public void offerRescinded(SchedulerDriver schedulerDriver, Protos.OfferID offerID) {
    logger.info("offerRescinded()");
  }

  @Override
  public void statusUpdate(SchedulerDriver driver, Protos.TaskStatus taskStatus) {

    final String taskId = taskStatus.getTaskId().getValue();

    logger.info("statusUpdate() task {} is in state {}",
        taskId, taskStatus.getState());

    switch (taskStatus.getState()) {
      case TASK_RUNNING:
        pendingInstances.remove(taskId);
        runningInstances.add(taskId);
        break;
      case TASK_FAILED:
      case TASK_FINISHED:
        pendingInstances.remove(taskId);
        runningInstances.remove(taskId);
        break;
    }

    logger.info("Number of instances: pending={}, running={}",
        pendingInstances.size(), runningInstances.size());
  }

  @Override
  public void frameworkMessage(SchedulerDriver schedulerDriver, Protos.ExecutorID executorID, Protos.SlaveID slaveID, byte[] bytes) {
    logger.info("frameworkMessage()");
  }

  @Override
  public void disconnected(SchedulerDriver schedulerDriver) {
    logger.info("disconnected()");
  }

  @Override
  public void slaveLost(SchedulerDriver schedulerDriver, Protos.SlaveID slaveID) {
    logger.info("slaveLost()");
  }

  @Override
  public void executorLost(SchedulerDriver schedulerDriver, Protos.ExecutorID executorID, Protos.SlaveID slaveID, int i) {
    logger.info("executorLost()");
  }

  @Override
  public void error(SchedulerDriver schedulerDriver, String s) {
    logger.error("error() {}", s);
  }

}
