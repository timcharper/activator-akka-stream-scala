package sample.stream

import akka.actor.ActorSystem
import java.io.{ FileOutputStream, PrintWriter }
import akka.stream.FlowMaterializer
import akka.stream.scaladsl.Source
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import akka.stream.impl.ActorBasedFlowMaterializer
import akka.stream.scaladsl.BlackholeSink
import akka.stream.scaladsl.OnCompleteSink
import akka.stream.scaladsl.SimpleActorFlowSink
import akka.stream.scaladsl.{ Sink, Source }
import akka.stream.stage.Context
import akka.stream.stage.Directive
import akka.stream.stage.PushStage
import akka.stream.stage.TerminationDirective
import akka.stream.{ FlowMaterializer, OverflowStrategy }
import java.io.{ FileOutputStream, PrintWriter }
import org.reactivestreams.Publisher
import scala.util.{Failure, Success, Try}
import akka.stream.scaladsl.OperationAttributes._

case class OnAllCompleteSink[In](callback: Try[Unit] ⇒ Unit)(implicit executionContext: ExecutionContext) extends SimpleActorFlowSink[Future[In]] {

  override def attach(flowPublisher: Publisher[Future[In]], materializer: ActorBasedFlowMaterializer, flowName: String) = {
    var futureAgg = Future.successful()
    val section = (s: Source[Future[In]]) => s.transform(() => new PushStage[Future[In], Unit] {
      override def onPush(elem: Future[In], ctx: Context[Unit]): Directive = {
        futureAgg = futureAgg.flatMap(_ => elem.mapTo[Unit])
        ctx.pull()
      }
      override def onUpstreamFailure(cause: Throwable, ctx: Context[Unit]): TerminationDirective = {
        callback(Failure(cause))
        ctx.fail(cause)
      }
      override def onUpstreamFinish(ctx: Context[Unit]): TerminationDirective = {
        futureAgg.onComplete(callback)
        ctx.finish()
      }
    })

    Source(flowPublisher).
      section(name("onAllCompleteSink"))(section).
      to(BlackholeSink).
      run()(materializer.withNamePrefix(flowName))
  }
}

object GroupLogFile {

  def main(args: Array[String]): Unit = {
    // actor system and implicit materializer
    implicit val system = ActorSystem("Sys")
    // execution context
    import system.dispatcher

    implicit val materializer = FlowMaterializer()

    val LoglevelPattern = """.*\[(DEBUG|INFO|WARN|ERROR)\].*""".r

    // read lines from a log file
    val logFile = io.Source.fromFile("src/main/resources/logfile.txt", "utf-8")

    Source(() => logFile.getLines()).
      // group them by log level
      groupBy {
        case LoglevelPattern(level) => level
        case other                  => "OTHER"
      }.
      // write lines of each group to a separate file
      map {
        case (level, groupFlow) =>
          val output = new PrintWriter(new FileOutputStream(s"target/log-$level.txt"), true)
          // close resource when the group stream is completed
          // foreach returns a future that we can key the close() off of
          groupFlow.
            foreach(line => output.println(line)).
            andThen { case t => output.close(); t.get }
      }.
      runWith(OnAllCompleteSink { _ =>
        Try(logFile.close())
        system.shutdown()
      })
  }
}
