package de.ux.kafka.clients.admin

import java.util.concurrent.Future
import java.util.concurrent.TimeUnit.SECONDS

import de.ux.kafka.clients.admin.mapper.Mapper.mapperByFormat
import de.ux.kafka.clients.admin.metadata.{KafkaMetadataClient, MetadataDescription, ZkMetadataClient}
import de.ux.kafka.clients.admin.request.RequestClient
import de.ux.kafka.clients.admin.request.RequestClient.NodeProvider
import de.ux.kafka.clients.admin.utils.{CommandLineUtils, Logging}
import joptsimple.OptionParser
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.utils.{Exit, Utils}

import scala.collection.JavaConverters._
import scala.collection.Map

object MetadataCommand extends Logging {

  def main(args: Array[String]): Unit = {

    val opts = new MetadataCommandOptions(args)

    if(args.length == 0)
      CommandLineUtils.printUsageAndDie(opts.parser, "Describe cluster metadata.")

    var exitCode = 0

    var kafkaRequestClient: RequestClient = null
    var zkMetadataClient: ZkMetadataClient = null

    try {
      var responseFutures: List[Future[MetadataDescription]] = List()

      if(opts.options.has(opts.bootstrapServerOpt)) {
        kafkaRequestClient = RequestClient.create(adminClientConfigs(opts).asJava)
        val metadataClient = new KafkaMetadataClient(kafkaRequestClient)
        val nodeProviders = nodeProvidersByOpts(metadataClient, opts)
        responseFutures ++= metadataClient.describe(nodeProviders.asJava).asScala.toList
      }

      if(opts.options.has(opts.zookeeperServerOpt)) {
        val zkMetadataClient = new ZkMetadataClient(opts.options.valueOf(opts.zookeeperServerOpt))
        responseFutures ++= List(zkMetadataClient.describe())
      }

      val allResponses = responseFutures.map(_.get(1000, SECONDS)).sortBy(r => r.source().toString).map(_.toMap())
      val mapper = mapperByFormat(opts.options.valueOf(opts.formatOpt))
      println(mapper.map(allResponses.asJava))
    } catch {
      case e: Throwable =>
        println("Error while executing metadata command : " + e.getMessage)
        error(Utils.stackTrace(e))
        exitCode = 1
    } finally {
      if(kafkaRequestClient!=null) {
        kafkaRequestClient.close()
      }
      if(zkMetadataClient!=null) {
        zkMetadataClient.close()
      }
      Exit.exit(exitCode)
    }
  }

  def adminClientConfigs(opts: MetadataCommandOptions): Map[String, _] = {
    Map(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> opts.options.valueOf(opts.bootstrapServerOpt))
  }

  def nodeProvidersByOpts(client: KafkaMetadataClient, opts: MetadataCommandOptions): List[NodeProvider] = {
    opts.options.valuesOf(opts.atKafkaNodesOpt).asScala
        .flatMap(value => nodeProviderByType(client, value))
        .toList
  }

  def nodeProviderByType(client: KafkaMetadataClient, value: String): List[NodeProvider] = {
    val node = """node#(\d+)""".r
    value match {
      case "any" => List(client.atAnyNode())
      case "controller" => List(client.atControllerNode())
      case "all" => client.atAllNodes().asScala.toList
      case node(id) => List(client.atNode(Integer.valueOf(id)))
      case _ => throw new IllegalArgumentException("The value " + value + " is not supported.")
    }
  }

  class MetadataCommandOptions(args: Array[String]) {
    val parser = new OptionParser(false)
    val helpOpt = parser.accepts("help", "Print usage information.")
    val bootstrapServerOpt = parser.accepts("bootstrap-server", "The connection string for the kafka connection in the form host:port. " +
      "Multiple hosts can be given to allow fail-over.")
      .withRequiredArg
      .describedAs("Kafka hosts")
      .ofType(classOf[String])
    val atKafkaNodesOpt = parser.accepts("at-kafka", "The nodes from which the metadata should be queried. " +
      "Allowed values are 'any', 'controller', 'all' and 'node#[id]', for e.g. 'node#1,node#2'. Default value is 'any'.")
      .withRequiredArg
      .describedAs("nodes")
      .defaultsTo("any")
      .withValuesSeparatedBy(",")
      .ofType(classOf[String])
    val zookeeperServerOpt = parser.accepts("zookeeper", "The connection string for the zookeeper connection in the form host:port. " +
      "Multiple hosts can be given to allow fail-over.")
      .withRequiredArg
      .describedAs("Zookeeper hosts")
      .ofType(classOf[String])
    val formatOpt = parser.accepts("format", "The output format. Supported values are 'json' or 'yaml'. Default value is 'json'.")
      .withRequiredArg()
      .defaultsTo("json")
      .ofType(classOf[String])

    val options = parser.parse(args : _*)

  }

}
