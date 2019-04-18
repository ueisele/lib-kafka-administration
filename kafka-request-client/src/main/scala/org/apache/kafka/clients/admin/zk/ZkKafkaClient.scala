package org.apache.kafka.clients.admin.zk

import java.time.Duration
import java.util.Optional

import kafka.admin.AdminUtils.debug
import kafka.cluster.Broker
import kafka.utils.ZkUtils
import kafka.utils.ZkUtils.getTopicPath
import org.apache.kafka.common.Node
import org.apache.kafka.common.errors.{LeaderNotAvailableException, ReplicaNotAvailableException}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.MetadataResponse
import org.apache.kafka.common.security.auth.SecurityProtocol

import scala.collection.JavaConverters._
import scala.collection.Seq

class ZkKafkaClient(val zkConnectionString: String,
                    val sessionTimeout: Duration,
                    val connectionTimeout: Duration,
                    val isSecurityEnabled: Boolean) extends AutoCloseable {

  val zkUtils = ZkUtils.apply(zkConnectionString, sessionTimeout.toMillis.toInt, connectionTimeout.toMillis.toInt, isSecurityEnabled)

  def metadataRequest(): MetadataResponse = metadataRequest(null)

  def metadataRequest(topics: java.util.Set[String]): MetadataResponse = metadataRequest(topics, SecurityProtocol.PLAINTEXT)

  def metadataRequest(topics: java.util.Set[String], securityProtocol: SecurityProtocol): MetadataResponse = {
    var metadata: java.util.Set[MetadataResponse.TopicMetadata] = null
    if(topics != null) {
      metadata = topicMetadata(topics, securityProtocol)
    } else {
      metadata = topicMetadata(securityProtocol)
    }
    new MetadataResponse(liveBrokers(securityProtocol).asScala.toList.asJava, clusterId(), zkUtils.getController(), metadata.asScala.toList.asJava)
  }

  def clusterId(): String = zkUtils.getClusterId.orNull

  def liveBrokers(): java.util.Set[Node] = liveBrokers(SecurityProtocol.PLAINTEXT)

  def liveBrokers(securityProtocol: SecurityProtocol): java.util.Set[Node] = zkUtils.getAllBrokersInCluster()
    .map(broker => broker.node(ListenerName.forSecurityProtocol(securityProtocol))).toSet.asJava

  def controller(): Node = controller(SecurityProtocol.PLAINTEXT)

  def controller(securityProtocol: SecurityProtocol): Node = zkUtils.getBrokerInfo(zkUtils.getController())
    .map(broker => broker.node(ListenerName.forSecurityProtocol(securityProtocol)))
    .orNull

  def topicMetadata(): java.util.Set[MetadataResponse.TopicMetadata] =
    topicMetadata(SecurityProtocol.PLAINTEXT)

  def topicMetadata(securityProtocol: SecurityProtocol): java.util.Set[MetadataResponse.TopicMetadata] = {
    topicMetadata(zkUtils.getAllTopics().toSet.asJava, securityProtocol)
  }

  def topicMetadata(topics: java.util.Set[String]): java.util.Set[MetadataResponse.TopicMetadata] =
    topicMetadata(topics, SecurityProtocol.PLAINTEXT)

  def topicMetadata(topics: java.util.Set[String], securityProtocol: SecurityProtocol): java.util.Set[MetadataResponse.TopicMetadata] =
    topics.asScala.map(topic => topicMetadata(topic, ListenerName.forSecurityProtocol(securityProtocol))).asJava

  def topicMetadata(topic: String): MetadataResponse.TopicMetadata =
    topicMetadata(topic, SecurityProtocol.PLAINTEXT)

  def topicMetadata(topic: String, securityProtocol: SecurityProtocol): MetadataResponse.TopicMetadata =
    topicMetadata(topic, ListenerName.forSecurityProtocol(securityProtocol))

  private def topicMetadata(topic: String, listenerName: ListenerName): MetadataResponse.TopicMetadata = {
    if (zkUtils.pathExists(getTopicPath(topic))) {
      val topicPartitionAssignment = zkUtils.getPartitionAssignmentForTopics(List(topic))(topic)
      val sortedPartitions = topicPartitionAssignment.toList.sortWith((m1, m2) => m1._1 < m2._1)
      val partitionMetadata = sortedPartitions.map { partitionMap =>
        val partition = partitionMap._1
        val replicas = partitionMap._2
        var leaderAndIsr = zkUtils.getLeaderAndIsrForPartition(topic, partition)
        //val inSyncReplicas = zkUtils.getInSyncReplicasForPartition(topic, partition)
        //val leader = zkUtils.getLeaderForPartition(topic, partition)
        debug("replicas = " + replicas + ", " + leaderAndIsr)

        var leaderInfo: Node = Node.noNode()
        var replicaInfo: Seq[Node] = Nil
        var isrInfo: Seq[Node] = Nil
        var offlineReplicas: Seq[Node] = Nil
        try {
          leaderInfo = leaderAndIsr match {
            case Some(l) =>
              try {
                getBrokerInfo(List(l.leader)).head.getNode(listenerName).getOrElse(Node.noNode())
              } catch {
                case e: Throwable => throw new LeaderNotAvailableException("Leader not available for partition [%s,%d]".format(topic, partition), e)
              }
            case None => throw new LeaderNotAvailableException("No leader exists for partition " + partition)
          }
          try {
            replicaInfo = getBrokerInfo(replicas).map(_.getNode(listenerName).getOrElse(Node.noNode()))
            isrInfo = getBrokerInfo(leaderAndIsr.get.isr).map(_.getNode(listenerName).getOrElse(Node.noNode()))
          } catch {
            case e: Throwable => throw new ReplicaNotAvailableException(e)
          }
          if (replicaInfo.size < replicas.size)
            throw new ReplicaNotAvailableException("Replica information not available for following brokers: " +
              replicas.filterNot(replicaInfo.map(_.id).contains(_)).mkString(","))
          if (isrInfo.size < leaderAndIsr.get.isr.size)
            throw new ReplicaNotAvailableException("In Sync Replica information not available for following brokers: " +
              leaderAndIsr.get.isr.filterNot(isrInfo.map(_.id).contains(_)).mkString(","))
          new MetadataResponse.PartitionMetadata(Errors.NONE, partition, leaderInfo, Optional.ofNullable(leaderAndIsr.get.leaderEpoch), replicaInfo.asJava, isrInfo.asJava, offlineReplicas.asJava)
        } catch {
          case e: Throwable =>
            debug("Error while fetching metadata for partition [%s,%d]".format(topic, partition), e)
            new MetadataResponse.PartitionMetadata(Errors.forException(e), partition, leaderInfo, Optional.ofNullable(leaderAndIsr.get.leaderEpoch), replicaInfo.asJava, isrInfo.asJava, offlineReplicas.asJava)
        }
      }
      new MetadataResponse.TopicMetadata(Errors.NONE, topic, Topic.isInternal(topic), partitionMetadata.asJava)
    } else {
      // topic doesn't exist, send appropriate error code
      new MetadataResponse.TopicMetadata(Errors.UNKNOWN_TOPIC_OR_PARTITION, topic, Topic.isInternal(topic), java.util.Collections.emptyList())
    }
  }

  private def getBrokerInfo(brokerIds: Seq[Int]): Seq[Broker] = {
    val brokerMetadata = brokerIds.map { id =>
        zkUtils.getBrokerInfo(id) match {
          case Some(brokerInfo) =>
            Some(brokerInfo)
          case None =>
            None
        }
    }
    brokerMetadata.filter(_.isDefined).map(_.get)
  }

  override def close(): Unit = zkUtils.close()

}