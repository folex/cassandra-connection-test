package com.example

import java.io.File
import java.net.{InetAddress,InetSocketAddress}
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
import com.datastax.driver.core.policies.{RoundRobinPolicy, TokenAwarePolicy, WhiteListPolicy, ExponentialReconnectionPolicy}
import scala.collection.JavaConverters._



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
    | Success max: $successMax ms
    | Success min: $successMin ms
    | Success avg: $successAvg ms
    | Success median: $successMedian ms
    | Failures: $failureCount
    | Failure max: $failureMax ms
    | Failure min: $failureMin ms
    | Failure avg: $failureAvg ms
    | Failure median: $failureMedian ms
    | RAW success (ms): ${successTimes.mkString(", ")}
    | RAW failures (ms): ${failureTimes.mkString(", ")}
      """.stripMargin
    }
  }

  def printState(session: Session) = {
    val state = session.getState
    val hosts = state.getConnectedHosts
    println(
      s"""
         | session.isClosed = ${state.getSession.isClosed}
         | state = $state
         | state.connectedHosts = ${state.getConnectedHosts.toList.mkString("  |<>|  ")}
         | Hosts = ${hosts.mkString("  |<>|  ")}
         | state.inFlightQueries = ${hosts.map(h => h -> state.getInFlightQueries(h)).mkString("  |<>|  ")}
         | state.trashedConnections = ${hosts.map(h => h -> state.getTrashedConnections(h)).mkString("  |<>|  ")}
         | state.openConnections = ${hosts.map(h => h -> state.getOpenConnections(h)).mkString("  |<>|  ")}
         | metrics.openConnections = ${session.getCluster.getMetrics.getOpenConnections.getValue}
         | metrics.knownHosts = ${session.getCluster.getMetrics.getKnownHosts.getValue}
         | metrics.connectedToHosts = ${session.getCluster.getMetrics.getConnectedToHosts.getValue}
       """.stripMargin)
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

  val testCassandraHost = "52.8.109.8"

  val endpoints = Array[String](InetAddress.getByName(testCassandraHost).getHostAddress)
  val endpointsA = Seq(new InetSocketAddress(InetAddress.getByName(testCassandraHost), 9042))
  val port = ProtocolOptions.DEFAULT_PORT
  val connectionMin = 1
  val connectionsMax = 10
  val threshold = 256
  val readTimeout = 10.seconds
  val cluster = Cluster.builder().addContactPoints(endpoints : _*)
    .withPort(port)
    .withPoolingOptions(new PoolingOptions()
      .setConnectionsPerHost(HostDistance.LOCAL, connectionMin, connectionsMax)
      .setNewConnectionThreshold(HostDistance.LOCAL, threshold)
      .setHeartbeatIntervalSeconds(30)
      .setIdleTimeoutSeconds(60)
    )
    .withSocketOptions(new SocketOptions()
      .setConnectTimeoutMillis(10000)
      .setReadTimeoutMillis(readTimeout.toMillis.toInt)
    )
    .withReconnectionPolicy(new ExponentialReconnectionPolicy(300,3000))
    .withLoadBalancingPolicy(new WhiteListPolicy(new TokenAwarePolicy(new RoundRobinPolicy()), endpointsA.asJavaCollection))
    .build()

  val keyspace = "folex_test"
  val session = cluster.newSession()

  session.init()
  session.executeAsync(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true")
  session.executeAsync(s"""
    CREATE TABLE IF NOT EXISTS ${keyspace}.chats (
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

  val num = 5
  /*
  * driver will add a connection when the number of concurrent requests is more than
  * (n - 1) * 128 + PoolingOptions.setNewConnectionThreshold
  */
  val subnum = 100

  var stat = Stat(List.empty, List.empty)

  printState(session)
  println(s"Starting test")

  for (i <- 1 to num) {
    def req() = {
      val start = now()
      (for {
        _ <- session.executeAsync(s"select * from ${keyspace}.chats")
        _ <- session.executeAsync(s"insert into ${keyspace}.chats (chat_id, avatar, chat_type, name) values (${now()}, textAsBlob('dsds'), textAsBlob('dsdsa'), textAsBlob('dsds'))")
        _ <- session.executeAsync(s"select * from ${keyspace}.chats")
      } yield Stat().success(now() - start)).recover { case e: Exception =>
        println(e.getMessage())
        Stat().failure(now() - start) }
    }
    val fs = (1 to subnum).map(_ => req())
    val res = Await.result(Future.sequence(fs), readTimeout + 2.seconds)

    stat += res.foldLeft(Stat())(_ + _)

    printState(session)
  }
  println("====================================")
  println(stat)
}
