package de.ux.kafka.clients.admin

import java.util.concurrent.Future
import java.util.concurrent.TimeUnit.SECONDS

import de.ux.kafka.clients.admin.mapper.Mapper.mapperByFormat
import de.ux.kafka.clients.admin.request.RequestClient
import de.ux.kafka.clients.admin.stopreplica.{KafkaStopReplicaClient, StopReplicaStatus}
import de.ux.kafka.clients.admin.utils.{CommandLineUtils, Logging}
import joptsimple.OptionParser
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.utils.{Exit, Utils}

import scala.collection.JavaConverters._
import scala.collection.Map

object StopReplicaCommand extends Logging {

  def main(args: Array[String]): Unit = {

    val opts = new StopReplicaCommandOptions(args)

    if(args.length == 0)
      CommandLineUtils.printUsageAndDie(opts.parser, "Send StopReplica Requests.")

    var exitCode = 0

    var kafkaRequestClient: RequestClient = null

    try {
      var responseFutures: List[Future[StopReplicaStatus]] = List()

      if(opts.options.has(opts.bootstrapServerOpt)) {
        kafkaRequestClient = RequestClient.create(adminClientConfigs(opts).asJava)
        val stopReplicaClient = new KafkaStopReplicaClient(kafkaRequestClient)
        responseFutures = stopReplicaClient.stopReplica(opts.options.valueOf(opts.brokerId).intValue(), opts.options.valuesOf(opts.topicPartitions).asScala.toSet.asJava) :: responseFutures
      }

      val allResponses = responseFutures.map(_.get(1000, SECONDS)).sortBy(r => r.source().toString).map(_.toMap())
      val mapper = mapperByFormat(opts.options.valueOf(opts.formatOpt))
      println(mapper.map(allResponses.asJava))
    } catch {
      case e: Throwable =>
        println("Error while executing ControlledShutdown command : " + e.getMessage)
        error(Utils.stackTrace(e))
        exitCode = 1
    } finally {
      if(kafkaRequestClient!=null) {
        kafkaRequestClient.close()
      }
      Exit.exit(exitCode)
    }
  }

  def adminClientConfigs(opts: StopReplicaCommandOptions): Map[String, _] = {
    Map(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> opts.options.valueOf(opts.bootstrapServerOpt))
  }

  class StopReplicaCommandOptions(args: Array[String]) {
    val parser = new OptionParser(false)
    val helpOpt = parser.accepts("help", "Print usage information.")
    val bootstrapServerOpt = parser.accepts("bootstrap-server", "The connection string for the kafka connection in the form host:port. " +
      "Multiple hosts can be given to allow fail-over.")
      .withRequiredArg
      .describedAs("hosts")
      .ofType(classOf[String])
    val brokerId = parser.accepts("brokerId", "The id of the broker to  which a StopReplica request should be sent.")
      .withRequiredArg
      .ofType(classOf[Integer])
    val topicPartitions = parser.accepts("topicPartitions", "List of Topic/Partitions e.g. 'topic-a/0,topic-a/1,topic-b/0'")
      .withRequiredArg
      .withValuesSeparatedBy(",")
      .withValuesConvertedBy(new TopicPartitionValueConverter)
    val formatOpt = parser.accepts("format", "The output format. Supported values are 'json' or 'yaml'. Default value is 'json'.")
      .withRequiredArg()
      .defaultsTo("json")
      .ofType(classOf[String])

    val options = parser.parse(args : _*)

  }

}
