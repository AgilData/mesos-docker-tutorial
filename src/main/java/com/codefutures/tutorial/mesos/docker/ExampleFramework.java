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

import com.google.protobuf.ByteString;

import org.apache.mesos.*;
import org.apache.mesos.Protos.*;

/**
 * Example framework that will run a scheduler that in turn will cause
 * Docker containers to be launched.
 *
 * Source code adapted from the example that ships with Mesos.
 */
public class ExampleFramework {

  /** Show command-line usage. */
  private static void usage() {
    String name = ExampleFramework.class.getName();
    System.err.println("Usage: " + name + " master-ip-and-port docker-image-name number-of-instances");
  }

  /**
   * Command-line entry point.
   * <br/>
   * Example usage: java ExampleFramework 127.0.0.1:5050 fedora/apache 2
   */
  public static void main(String[] args) throws Exception {

    // check command-line args
    if (args.length != 3) {
      usage();
      System.exit(1);
    }

    // If the framework stops running, mesos will terminate all of the tasks that
    // were initiated by the framework but only once the fail-over timeout period
    // has expired. Using a timeout of zero here means that the tasks will
    // terminate immediately when the framework is terminated. For production
    // deployments this probably isn't the desired behavior, so a timeout can be
    // specified here, allowing another instance of the framework to take over.
    final int frameworkFailoverTimeout = 0;

    FrameworkInfo.Builder frameworkBuilder = FrameworkInfo.newBuilder()
        .setName("CodeFuturesExampleFramework")
        .setUser("") // Have Mesos fill in the current user.
        .setFailoverTimeout(frameworkFailoverTimeout); // timeout in seconds

    if (System.getenv("MESOS_CHECKPOINT") != null) {
      System.out.println("Enabling checkpoint for the framework");
      frameworkBuilder.setCheckpoint(true);
    }

    // parse command-line args
    final String imageName = args[1];
    final int totalTasks = Integer.parseInt(args[2]);

    // create the scheduler
    final Scheduler scheduler = new ExampleScheduler(
        imageName,
        totalTasks
    );

    // create the driver
    MesosSchedulerDriver driver;
    if (System.getenv("MESOS_AUTHENTICATE") != null) {
      System.out.println("Enabling authentication for the framework");

      if (System.getenv("DEFAULT_PRINCIPAL") == null) {
        System.err.println("Expecting authentication principal in the environment");
        System.exit(1);
      }

      if (System.getenv("DEFAULT_SECRET") == null) {
        System.err.println("Expecting authentication secret in the environment");
        System.exit(1);
      }

      Credential credential = Credential.newBuilder()
          .setPrincipal(System.getenv("DEFAULT_PRINCIPAL"))
          .setSecret(ByteString.copyFrom(System.getenv("DEFAULT_SECRET").getBytes()))
          .build();

      frameworkBuilder.setPrincipal(System.getenv("DEFAULT_PRINCIPAL"));

      driver = new MesosSchedulerDriver(scheduler, frameworkBuilder.build(), args[0], credential);
    } else {
      frameworkBuilder.setPrincipal("test-framework-java");

      driver = new MesosSchedulerDriver(scheduler, frameworkBuilder.build(), args[0]);
    }

    int status = driver.run() == Status.DRIVER_STOPPED ? 0 : 1;

    // Ensure that the driver process terminates.
    driver.stop();

    System.exit(status);
  }
}
