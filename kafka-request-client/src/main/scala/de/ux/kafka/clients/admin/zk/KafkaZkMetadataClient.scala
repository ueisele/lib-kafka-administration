package de.ux.kafka.clients.admin.zk

import java.time.Duration

import kafka.zk.KafkaZkClient
import org.apache.kafka.common.utils.Time

class KafkaZkMetadataClient(val zkConnectionString: String,
                            val sessionTimeout: Duration,
                            val connectionTimeout: Duration,
                            val isSecurityEnabled: Boolean) extends AutoCloseable {

  val kafkaZkClient = KafkaZkClient.apply(zkConnectionString, isSecurityEnabled, sessionTimeout.toMillis.toInt, connectionTimeout.toMillis.toInt, 5, Time.SYSTEM);

  def getBrokerEpoch(brokerId: Int): Option[Long] = {
    Some(getAllBrokerIdAndEpochsInCluster(brokerId))
  }

  def getAllBrokerIdAndEpochsInCluster: Map[Int, Long] = {
    kafkaZkClient.getAllBrokerAndEpochsInCluster
      .map(brokerAndEpoch => (brokerAndEpoch._1.id, brokerAndEpoch._2))
  }

  /*
  def getControllerIdAndEpoch: Option[(Int, Int)] = {
    val controllerId = kafkaZkClient.getControllerId
    val controllerEpoch = kafkaZkClient.getControllerEpoch
  }
  */

  override def close(): Unit = kafkaZkClient.close()
}
