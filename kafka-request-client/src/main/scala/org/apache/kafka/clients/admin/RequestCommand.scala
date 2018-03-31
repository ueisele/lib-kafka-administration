package org.apache.kafka.clients.admin

import java.util.Properties

object RequestCommand {

  def main(args: Array[String]): Unit = {
    println("Hello World :)")

    var client = RequestClient.create(new Properties());
    client.request(null, client.toControllerNode);
  }
}
