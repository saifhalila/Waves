package com.wavesplatform.it

import com.wavesplatform.it.api._
import org.scalatest._
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}

import scala.concurrent.Await.result
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future.traverse
import scala.concurrent.duration._
import scala.util.Random

class ValidChainGenerationSpec(override val nodes: Seq[Node]) extends FreeSpec with ScalaFutures with IntegrationPatience
  with Matchers with TransferSending with MultipleNodesApi {

  "Generate more blocks and resynchronise after rollback" - {
    "1 of N" in test(1)
    "N-1 of N" in test(nodes.size - 1)

    def test(n: Int): Unit = result(for {
      height <- traverse(nodes)(_.height).map(_.max)
      baseHeight = height + 5
      _ <- traverse(nodes)(_.waitForHeight(baseHeight))

      rollbackNodes = Random.shuffle(nodes).take(n)
      _ <- traverse(rollbackNodes)(_.rollback(1))
      _ <- waitForSameBlocksAt(nodes)(baseHeight, 5.seconds)
    } yield (), 7.minutes)
  }
}
