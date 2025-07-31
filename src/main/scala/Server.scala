import cats.effect._
import cats.implicits.catsSyntaxTuple4Parallel
import configuration.{Configuration, ConfigurationUtils, FileAndSystemPropertyReader}
import http.HttpClient
import org.slf4j.LoggerFactory

import java.nio.channels.ClosedChannelException
import scala.concurrent.duration.DurationInt

object Server extends IOApp {

  private val logger = LoggerFactory.getLogger(getClass)

  override protected def reportFailure(err: Throwable): IO[Unit] = err match {
    case _: ClosedChannelException => IO.pure(logger.debug("Suppressing ClosedChannelException error", err))
    case _                         => IO.pure(logger.error("Failure caught and handled by IOApp", err))
  }

  def run(args: List[String]): IO[ExitCode] = {
    val configReader = FileAndSystemPropertyReader
    val httpClient   = new HttpClient

    for {
      initialConfig <- ConfigurationUtils.create(configReader, httpClient)
      configRef     <- Ref.of[IO, Configuration](initialConfig)
      result <- (
        pingTokenSync(configRef, httpClient),
        plexRssSync(configRef, httpClient),
        plexTokenDeleteSync(configRef, httpClient),
        plexFullSync(configRef, httpClient)
      ).parTupled.as(ExitCode.Success)
    } yield result
  }

  private def fetchLatestConfig(configRef: Ref[IO, Configuration]): IO[Configuration] =
    configRef.get

  private def pingTokenSync(configRef: Ref[IO, Configuration], httpClient: HttpClient): IO[Unit] =
    for {
      config <- fetchLatestConfig(configRef)
      _      <- PingTokenSync.run(config, httpClient)
      _      <- IO.sleep(24.hours)
      _      <- pingTokenSync(configRef, httpClient).handleErrorWith { err =>
        logger.error("Error in pingTokenSync, retrying", err)
        IO.sleep(1.minute) >> pingTokenSync(configRef, httpClient)
      }
    } yield ()

  private def plexRssSync(
      configRef: Ref[IO, Configuration],
      httpClient: HttpClient
  ): IO[Unit] =
    for {
      config <- fetchLatestConfig(configRef)
      _      <- IO(logger.info("Starting PlexRssSync (quick sync)"))
      _      <- PlexTokenSync.run(config, httpClient, runFullSync = false)
      _      <- IO(logger.info(s"PlexRssSync completed, sleeping for ${config.refreshInterval.toSeconds} seconds"))
      _      <- IO.sleep(config.refreshInterval)
      _      <- IO(logger.info("PlexRssSync woke up, starting next iteration"))
      _      <- plexRssSync(configRef, httpClient).handleErrorWith { err =>
        logger.error("Error in plexRssSync, retrying", err)
        IO.sleep(1.minute) >> plexRssSync(configRef, httpClient)
      }
    } yield ()

  private def plexFullSync(
      configRef: Ref[IO, Configuration],
      httpClient: HttpClient
  ): IO[Unit] =
    for {
      config <- fetchLatestConfig(configRef)
      _      <- PlexTokenSync.run(config, httpClient, runFullSync = true)
      _      <- IO.sleep(19.minutes)
      _      <- plexFullSync(configRef, httpClient).handleErrorWith { err =>
        logger.error("Error in plexFullSync, retrying", err)
        IO.sleep(1.minute) >> plexFullSync(configRef, httpClient)
      }
    } yield ()

  private def plexTokenDeleteSync(configRef: Ref[IO, Configuration], httpClient: HttpClient): IO[Unit] =
    for {
      config <- fetchLatestConfig(configRef)
      _      <- IO(logger.info("Starting PlexTokenDeleteSync"))
      _      <- PlexTokenDeleteSync.run(config, httpClient)
      _      <- IO(logger.info(s"PlexTokenDeleteSync completed, sleeping for ${config.deleteConfiguration.deleteInterval.toDays} days"))
      _      <- IO.sleep(config.deleteConfiguration.deleteInterval)
      _      <- IO(logger.info("PlexTokenDeleteSync woke up, starting next iteration"))
      _      <- plexTokenDeleteSync(configRef, httpClient).handleErrorWith { err =>
        logger.error("Error in plexTokenDeleteSync, retrying", err)
        IO.sleep(1.minute) >> plexTokenDeleteSync(configRef, httpClient)
      }
    } yield ()
}
