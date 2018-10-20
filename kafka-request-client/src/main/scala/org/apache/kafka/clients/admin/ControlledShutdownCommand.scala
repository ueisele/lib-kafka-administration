package org.apache.kafka.clients.admin

import java.util.concurrent.Future
import java.util.concurrent.TimeUnit.SECONDS

import joptsimple.OptionParser
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.controlledshutdown.{ControlledShutdownStatus, KafkaControlledShutdownClient}
import org.apache.kafka.clients.admin.mapper.Mapper.mapperByFormat
import org.apache.kafka.clients.admin.request.RequestClient
import org.apache.kafka.clients.admin.utils.{CommandLineUtils, Logging}
import org.apache.kafka.common.utils.{Exit, Utils}

import scala.collection.JavaConverters._
import scala.collection.Map

object ControlledShutdownCommand extends Logging{

  def main(args: Array[String]): Unit = {

    val opts = new ControlledShutdownCommandOptions(args)

    if(args.length == 0)
      CommandLineUtils.printUsageAndDie(opts.parser, "Send ControlledShutdown Requests.")

    var exitCode = 0

    var kafkaRequestClient: RequestClient = null

    try {
      var responseFutures: List[Future[ControlledShutdownStatus]] = List()

      if(opts.options.has(opts.bootstrapServerOpt)) {
        kafkaRequestClient = RequestClient.create(adminClientConfigs(opts).asJava)
        val controlledShutdownClient = new KafkaControlledShutdownClient(kafkaRequestClient)
        responseFutures ++= controlledShutdownClient.shutdown(opts.options.valuesOf(opts.brokerIds)).asScala.toList
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

  def adminClientConfigs(opts: ControlledShutdownCommandOptions): Map[String, _] = {
    Map(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> opts.options.valueOf(opts.bootstrapServerOpt))
  }
  class ControlledShutdownCommandOptions(args: Array[String]) {
    val parser = new OptionParser(false)
    val helpOpt = parser.accepts("help", "Print usage information.")
    val bootstrapServerOpt = parser.accepts("bootstrap-server", "The connection string for the kafka connection in the form host:port. " +
      "Multiple hosts can be given to allow fail-over.")
      .withRequiredArg
      .describedAs("hosts")
      .ofType(classOf[String])
    val brokerIds = parser.accepts("brokerIds", "The ids of the brokers to which to send a ControlledShutdown request.")
      .withRequiredArg
      .withValuesSeparatedBy(",")
      .ofType(classOf[Integer])
    val formatOpt = parser.accepts("format", "The output format. Supported values are 'json' or 'yaml'. Default value is 'json'.")
      .withRequiredArg()
      .defaultsTo("json")
      .ofType(classOf[String])

    val options = parser.parse(args : _*)

  }

}
