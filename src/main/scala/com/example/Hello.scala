package com.example

import java.io.File
import java.net.InetAddress
import java.nio.file.Paths
import com.datastax.driver.core._
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}
import scala.language.implicitConversions
import com.google.common.util.concurrent.{FutureCallback, Futures}
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.Try

object Main extends App {
  case class Stat(successTimes: List[Long] = List.empty, failureTimes: List[Long] = List.empty) {
    def success(time: Long) = copy(successTimes = successTimes :+ time)
    def failure(time: Long) = copy(failureTimes = failureTimes :+ time)

    def +(s: Stat) = copy(successTimes = successTimes ++ s.successTimes, failureTimes = failureTimes ++ s.failureTimes)

    def totalCount: Int = successTimes.size + failureTimes.size
    def errorPercent: Double = failureTimes.size.toDouble / (successTimes.size + failureTimes.size).toDouble * 100

    def successCount: Int = successTimes.size
    def successMax: Long = Try(successTimes.max).getOrElse(-1)
    def successMin: Long = Try(successTimes.min).getOrElse(-1)
    def successAvg: Long = Try(successTimes.sum / successTimes.size).getOrElse(-1)
    def successMedian: Long = Try(successTimes.sorted.apply(math.floor(successTimes.size / 2).toInt)).getOrElse(-1)

    def failureCount: Int = failureTimes.size
    def failureMax: Long = Try(failureTimes.max).getOrElse(-1)
    def failureMin: Long = Try(failureTimes.min).getOrElse(-1)
    def failureAvg: Long = Try(failureTimes.sum / failureTimes.size).getOrElse(-1)
    def failureMedian: Long = Try(failureTimes.sorted.apply(math.floor(failureTimes.size / 2).toInt)).getOrElse(-1)

    override def toString: String = {
    s"""
    | Total: $totalCount
    | Error %: $errorPercent
    | Success: $successCount
    | Success max: $successMax
    | Success min: $successMin
    | Success avg: $successAvg
    | Success median: $successMedian
    | Failures: $failureCount
    | Failure max: $failureMax
    | Failure min: $failureMin
    | Failure avg: $failureAvg
    | Failure median: $failureMedian
    | RAW success: ${successTimes.mkString(", ")}
    | RAW failures: ${failureTimes.mkString(", ")}
      """.stripMargin
    }
  }

  implicit def future(future: ResultSetFuture): Future[ResultSet] = {
    val promise = Promise[ResultSet]()

    val callback = new FutureCallback[ResultSet] {
      def onSuccess(result: ResultSet): Unit = {
        promise success result
      }

      def onFailure(err: Throwable): Unit = {
        promise failure err
      }
    }

    Futures.addCallback(future, callback)
    promise.future
  }

  def now() = scala.compat.Platform.currentTime

  val testCassandraHost = "10.110.0.10"

  val endpoints = Array[String](InetAddress.getByName(testCassandraHost).getHostAddress)
  val port = ProtocolOptions.DEFAULT_PORT
  val cluster = Cluster.builder().addContactPoints(endpoints : _*)
    .withPort(port)
    .withPoolingOptions(new PoolingOptions()
      .setConnectionsPerHost(HostDistance.LOCAL, 5, 10)
      .setNewConnectionThreshold(HostDistance.LOCAL, 10)
      .setHeartbeatIntervalSeconds(30)
    )
    .withSocketOptions(new SocketOptions()
      .setConnectTimeoutMillis(10000)
      .setReadTimeoutMillis(30000)
    )
    .withReconnectionPolicy(new ExponentialReconnectionPolicy(300,3000))
    .build()

  val session = cluster.newSession()

  session.init()
  session.executeAsync("CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true")
  session.executeAsync("""
    CREATE TABLE IF NOT EXISTS test.chats (
    chat_id bigint PRIMARY KEY,
    avatar blob,
    chat_type blob,
    name blob
    ) WITH bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';
  """)

  val loadPolicy = session.getCluster.getConfiguration.getPolicies.getLoadBalancingPolicy
  val hosts = session.getState.getConnectedHosts.toList
  hosts.foreach(h => println("HostDistance is " + session.getCluster.getConfiguration.getPolicies.getLoadBalancingPolicy.distance(h)))

  val num = 1000
  val subnum = 32

  var stat = Stat(List.empty, List.empty)

  for (i <- 1 to num) {
    def req() = {
      val start = now()
      (for {
        _ <- session.executeAsync("select * from test.chats")
        _ <- session.executeAsync(s"insert into test.chats (chat_id, avatar, chat_type, name) values (${now()}, textAsBlob('dsds'), textAsBlob('dsdsa'), textAsBlob('dsds'))")
        _ <- session.executeAsync("select * from test.chats")
      } yield Stat().success(now() - start)).recover { case e: Exception =>
        println(e.getMessage())
        Stat().failure(now() - start) }
    }
    val fs = (1 to subnum).map(_ => req())
    val res = Await.result(Future.sequence(fs), 31.seconds)

    stat += res.foldLeft(Stat())(_ + _)
  }
  println("====================================")
  println(stat)
}

  // for (i <- 1 to 10) {
  //   println(s"\n====================================$i STARTED========================================\n\n\n")
  //   val state = session.getState
  //   val hosts = state.getConnectedHosts
  //   println(
  //     s"""
  //        | state = $state
  //        | state.connectedHosts = ${state.getConnectedHosts.toList.mkString("  |<>|  ")}
  //        | Hosts = ${hosts.mkString("  |<>|  ")}
  //        | state.inFlightQueries = ${hosts.map(h => h -> state.getInFlightQueries(h)).mkString("  |<>|  ")}
  //        | state.trashedConnections = ${hosts.map(h => h -> state.getTrashedConnections(h)).mkString("  |<>|  ")}
  //        | session.isClosed = ${state.getSession.isClosed}
  //      """.stripMargin)
  //   val req = session.executeAsync("select * from test.chats").map(_ => "Success").recover{case e: Exception => "Failure " + e}
  //   println (Await.result(req, 10.seconds))
  //   println(s"\n\n\n====================================$i ENDED========================================\n")
  //   Thread.sleep(5000)
  // }
