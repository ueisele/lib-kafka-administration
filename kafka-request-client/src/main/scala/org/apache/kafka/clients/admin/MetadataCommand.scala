package org.apache.kafka.clients.admin

import java.util.concurrent.Future
import java.util.concurrent.TimeUnit.SECONDS
import java.util.stream.Collectors

import joptsimple.OptionParser
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.mapper.Mapper.mapperByFormat
import org.apache.kafka.clients.admin.metadata.MetadataClient
import org.apache.kafka.clients.admin.request.RequestClient
import org.apache.kafka.clients.admin.request.RequestClient.NodeProvider
import org.apache.kafka.clients.admin.utils.{CommandLineUtils, Logging}
import org.apache.kafka.common.utils.{Exit, Utils}

import scala.collection.JavaConverters._
import scala.collection.Map

object MetadataCommand extends Logging{

  def main(args: Array[String]): Unit = {

    val opts = new MetadataCommandOptions(args)

    if(args.length == 0)
      CommandLineUtils.printUsageAndDie(opts.parser, "Describe cluster metadata.")

    opts.checkArgs()

    var exitCode = 0
    val requestClient = RequestClient.create(adminClientConfigs(opts).asJava)
    try {
      val metadataClient = new MetadataClient(requestClient)
      val nodeProviders = nodeProvidersByOpts(metadataClient, opts)
      val responseFutures = metadataClient.describe(nodeProviders.asJava)
      val allResponses = responseFutures.asScala.map(_.get()).map(_.toMap()).toList
      val mapper = mapperByFormat(opts.options.valueOf(opts.formatOpt))
      println(mapper.map(allResponses.asJava))
    } catch {
      case e: Throwable =>
        println("Error while executing metadata command : " + e.getMessage)
        error(Utils.stackTrace(e))
        exitCode = 1
    } finally {
      requestClient.close()
      Exit.exit(exitCode)
    }
  }

  def adminClientConfigs(opts: MetadataCommandOptions): Map[String, _] = {
    Map(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> opts.options.valueOf(opts.bootstrapServerOpt))
  }

  def nodeProvidersByOpts(client: MetadataClient, opts: MetadataCommandOptions): List[NodeProvider] = {
    opts.options.valuesOf(opts.atKafkaNodesOpt).stream()
      .flatMap(value => nodeProviderByType(client, value).asJava.stream())
      .collect(Collectors.toList()).asScala.toList
  }

  def nodeProviderByType(client: MetadataClient, value: String): List[NodeProvider] = {
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
    val bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED: The connection string for the kafka connection in the form host:port. " +
      "Multiple hosts can be given to allow fail-over.")
      .withRequiredArg
      .describedAs("hosts")
      .ofType(classOf[String])
    val atKafkaNodesOpt = parser.accepts("at-kafka", "The nodes from which the metadata should be queried. " +
      "Allowed values are 'any', 'controller', 'all' and 'node#[id]', for e.g. 'node#1,node#2'. Default value is 'any'.")
      .withRequiredArg
      .describedAs("nodes")
      .defaultsTo("any")
      .ofType(classOf[String])
    val formatOpt = parser.accepts("format", "The output format. Supported values are 'json' or 'yaml'. Default value is 'yaml'.")
      .withRequiredArg()
      .defaultsTo("yaml")
      .ofType(classOf[String])

    val options = parser.parse(args : _*)

    def checkArgs() {
      CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServerOpt)
    }
  }

}
