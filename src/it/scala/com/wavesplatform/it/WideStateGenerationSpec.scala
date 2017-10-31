package com.wavesplatform.it

import com.wavesplatform.it.api._
import org.scalatest._
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}

import scala.concurrent.Await.result
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future.traverse
import scala.concurrent.duration._

class WideStateGenerationSpec(override val nodes: Seq[Node]) extends FreeSpec with ScalaFutures with IntegrationPatience
  with Matchers with TransferSending {

  private val requestsCount = 10000

  "Generate a lot of transactions and synchronise" in result(for {
    b <- traverse(nodes)(balanceForNode).map(_.toMap)
    _ <- processRequests(generateTransfersToRandomAddresses(requestsCount/2, b) ++ generateTransfersBetweenAccounts(requestsCount/2, b))

    height <- traverse(nodes)(_.height).map(_.max)
    _ <- traverse(nodes)(_.waitForHeight(height + 30))

    _ <- waitForSameBlocksAt(nodes)(height + 10, 5.seconds)
  } yield (), 10.minutes)

}
