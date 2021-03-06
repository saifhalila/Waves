package scorex.block

import java.nio.ByteBuffer

import cats._
import com.google.common.primitives.{Bytes, Ints, Longs}
import com.wavesplatform.settings.GenesisSettings
import com.wavesplatform.state2.{ByteStr, LeaseInfo, Portfolio}
import monix.eval.Coeval
import play.api.libs.json.{JsObject, Json}
import scorex.account.{Address, PrivateKeyAccount, PublicKeyAccount}
import scorex.block.fields.FeaturesBlockField
import scorex.consensus.nxt.{NxtConsensusBlockField, NxtLikeConsensusBlockData}
import scorex.crypto.EllipticCurveImpl
import scorex.crypto.hash.FastCryptographicHash.DigestSize
import scorex.transaction.TransactionParser._
import scorex.transaction.ValidationError.GenericError
import scorex.transaction._
import scorex.utils.ScorexLogging

import scala.util.{Failure, Try}

class BlockHeader(val timestamp: Long,
                  val version: Byte,
                  val reference: ByteStr,
                  val signerData: SignerData,
                  val consensusData: NxtLikeConsensusBlockData,
                  val transactionCount: Int,
                  val featureVotes: Set[Short]) {

  protected val versionField: ByteBlockField = ByteBlockField("version", version)
  protected val timestampField: LongBlockField = LongBlockField("timestamp", timestamp)
  protected val referenceField: BlockIdField = BlockIdField("reference", reference.arr)
  protected val signerField: SignerDataBlockField = SignerDataBlockField("signature", signerData)
  protected val consensusField = NxtConsensusBlockField(consensusData)
  protected val supportedFeaturesField = FeaturesBlockField(version, featureVotes)

  val headerJson: Coeval[JsObject] = Coeval.evalOnce(
    versionField.json() ++
      timestampField.json() ++
      referenceField.json() ++
      consensusField.json() ++
      supportedFeaturesField.json() ++
      signerField.json())
}

object BlockHeader extends ScorexLogging {
  def parseBytes(bytes: Array[Byte]): Try[(BlockHeader, Array[Byte])] = Try {

    val version = bytes.head

    var position = 1

    val timestamp = Longs.fromByteArray(bytes.slice(position, position + 8))
    position += 8

    val reference = ByteStr(bytes.slice(position, position + SignatureLength))
    position += SignatureLength

    val cBytesLength = Ints.fromByteArray(bytes.slice(position, position + 4))
    position += 4
    val cBytes = bytes.slice(position, position + cBytesLength)
    val consData = NxtLikeConsensusBlockData(Longs.fromByteArray(cBytes.take(Block.BaseTargetLength)), ByteStr(cBytes.takeRight(Block.GeneratorSignatureLength)))
    position += cBytesLength

    val tBytesLength = Ints.fromByteArray(bytes.slice(position, position + 4))
    position += 4
    val tBytes = bytes.slice(position, position + tBytesLength)

    val txCount = version match {
      case 1 | 2 => tBytes.head
      case 3 => ByteBuffer.wrap(tBytes, 0, 4).getInt()
    }

    position += tBytesLength

    var supportedFeaturesIds = Set.empty[Short]

    if (version > 2) {
      val featuresCount = Ints.fromByteArray(bytes.slice(position, position + 4))
      position += 4

      val buffer = ByteBuffer.wrap(bytes.slice(position, position + featuresCount * 2)).asShortBuffer
      val arr = new Array[Short](featuresCount)
      buffer.get(arr)
      position += featuresCount * 2
      supportedFeaturesIds = arr.toSet
    }

    val genPK = bytes.slice(position, position + KeyLength)
    position += KeyLength

    val signature = ByteStr(bytes.slice(position, position + SignatureLength))
    position += SignatureLength

    val blockHeader = new BlockHeader(timestamp, version, reference, SignerData(PublicKeyAccount(genPK), signature), consData, txCount, supportedFeaturesIds)
    (blockHeader, tBytes)
  }.recoverWith { case t: Throwable =>
    log.error("Error when parsing block", t)
    Failure(t)
  }

  def json(bh: BlockHeader, blockSize: Int): JsObject = bh.headerJson() ++
    Json.obj(
      "blocksize" -> blockSize,
      "transactionCount" -> bh.transactionCount
    )

}

case class Block private(override val timestamp: Long,
                         override val version: Byte,
                         override val reference: ByteStr,
                         override val signerData: SignerData,
                         override val consensusData: NxtLikeConsensusBlockData,
                         transactionData: Seq[Transaction],
                         override val featureVotes: Set[Short]) extends
  BlockHeader(timestamp,
    version,
    reference,
    signerData,
    consensusData,
    transactionData.length,
    featureVotes) with Signed {

  import Block._

  private val transactionField = TransactionsBlockField(version.toInt, transactionData)

  val uniqueId: ByteStr = signerData.signature

  val bytes: Coeval[Array[Byte]] = Coeval.evalOnce {
    val txBytesSize = transactionField.bytes().length
    val txBytes = Bytes.ensureCapacity(Ints.toByteArray(txBytesSize), 4, 0) ++ transactionField.bytes()

    val cBytesSize = consensusField.bytes().length
    val cBytes = Bytes.ensureCapacity(Ints.toByteArray(cBytesSize), 4, 0) ++ consensusField.bytes()

    versionField.bytes() ++
      timestampField.bytes() ++
      referenceField.bytes() ++
      cBytes ++
      txBytes ++
      supportedFeaturesField.bytes() ++
      signerField.bytes()
  }

  val json: Coeval[JsObject] = Coeval.evalOnce(BlockHeader.json(this, bytes().length) ++
    Json.obj("fee" -> transactionData.filter(_.assetFee._1.isEmpty).map(_.assetFee._2).sum) ++
    transactionField.json())

  val bytesWithoutSignature: Coeval[Array[Byte]] = Coeval.evalOnce(bytes().dropRight(SignatureLength))

  val blockScore: Coeval[BigInt] = Coeval.evalOnce((BigInt("18446744073709551616") / consensusData.baseTarget).ensuring(_ > 0))

  val feesPortfolio: Coeval[Portfolio] = Coeval.evalOnce(Monoid[Portfolio].combineAll({
    val assetFees: Seq[(Option[AssetId], Long)] = transactionData.map(_.assetFee)
    assetFees
      .map { case (maybeAssetId, vol) => maybeAssetId -> vol }
      .groupBy(a => a._1)
      .mapValues((records: Seq[(Option[ByteStr], Long)]) => records.map(_._2).sum)
  }.toList.map {
    case (maybeAssetId, feeVolume) =>
      maybeAssetId match {
        case None => Portfolio(feeVolume, LeaseInfo.empty, Map.empty)
        case Some(assetId) => Portfolio(0L, LeaseInfo.empty, Map(assetId -> feeVolume))
      }
  }))

  val prevBlockFeePart: Coeval[Portfolio] = Coeval.evalOnce(Monoid[Portfolio].combineAll(transactionData.map(tx => tx.feeDiff().minus(tx.feeDiff().multiply(CurrentBlockFeePart)))))

  protected val signatureValid: Coeval[Boolean] = Coeval.evalOnce(EllipticCurveImpl.verify(signerData.signature.arr, bytesWithoutSignature(), signerData.generator.publicKey))
  protected override val signedDescendants: Coeval[Seq[Transaction]] = Coeval.evalOnce(transactionData)

  override def toString: String =
    s"Block(${signerData.signature} -> ${reference.trim}, txs=${transactionData.size}, features=$featureVotes) "

}

object Block extends ScorexLogging {

  case class Fraction(dividend: Int, divider: Int) {
    def apply(l: Long): Long = l / divider * dividend
  }

  val CurrentBlockFeePart: Fraction = Fraction(2, 5)

  type BlockIds = Seq[ByteStr]
  type BlockId = ByteStr

  val MaxTransactionsPerBlockVer1Ver2: Int = 100
  val MaxTransactionsPerBlockVer3: Int = 65535
  val MaxFeaturesInBlock: Int = 64
  val BaseTargetLength: Int = 8
  val GeneratorSignatureLength: Int = 32

  val BlockIdLength = SignatureLength

  val TransactionSizeLength = 4

  def transParseBytes(version: Int, bytes: Array[Byte]): Try[Seq[Transaction]] = Try {
    if (bytes.isEmpty) {
      Seq.empty
    } else {
      val v: (Array[Byte], Int) = version match {
        case 1 | 2 => (bytes.tail, bytes.head) //  127 max, won't work properly if greater
        case 3 =>
          val size = ByteBuffer.wrap(bytes, 0, 4).getInt()
          (bytes.drop(4), size)
        case _ => ???
      }

      (1 to v._2).foldLeft((0: Int, Seq[Transaction]())) { case ((pos, txs), _) =>
        val transactionLengthBytes = v._1.slice(pos, pos + TransactionSizeLength)
        val transactionLength = Ints.fromByteArray(transactionLengthBytes)
        val transactionBytes = v._1.slice(pos + TransactionSizeLength, pos + TransactionSizeLength + transactionLength)
        val transaction = TransactionParser.parseBytes(transactionBytes).get

        (pos + TransactionSizeLength + transactionLength, txs :+ transaction)
      }._2
    }
  }

  def parseBytes(bytes: Array[Byte]): Try[Block] =
    for {
      (blockHeader, transactionBytes) <- BlockHeader.parseBytes(bytes)
      transactionsData <- transParseBytes(blockHeader.version, transactionBytes)
    } yield
      Block(blockHeader.timestamp,
        blockHeader.version,
        blockHeader.reference,
        blockHeader.signerData,
        blockHeader.consensusData,
        transactionsData,
        blockHeader.featureVotes)

  def buildAndSign(version: Byte,
                   timestamp: Long,
                   reference: ByteStr,
                   consensusData: NxtLikeConsensusBlockData,
                   transactionData: Seq[Transaction],
                   signer: PrivateKeyAccount,
                   featureVotes: Set[Short]): Either[GenericError, Block] = (for {
    _ <- Either.cond(transactionData.size <= MaxTransactionsPerBlockVer3, (), s"Too many transactions in Block: allowed: $MaxTransactionsPerBlockVer3, actual: ${transactionData.size}")
    _ <- Either.cond(reference.arr.length == SignatureLength, (), "Incorrect reference")
    _ <- Either.cond(consensusData.generationSignature.arr.length == GeneratorSignatureLength, (), "Incorrect consensusData.generationSignature")
    _ <- Either.cond(signer.publicKey.length == KeyLength, (), "Incorrect signer.publicKey")
    _ <- Either.cond(version > 2 || featureVotes.isEmpty, (), s"Block version $version could not contain feature votes")
    _ <- Either.cond(featureVotes.size <= MaxFeaturesInBlock, (), s"Block could not contain more than $MaxFeaturesInBlock feature votes")
  } yield {
    val nonSignedBlock = Block(timestamp, version, reference, SignerData(signer, ByteStr.empty), consensusData, transactionData, featureVotes)
    val toSign = nonSignedBlock.bytes
    val signature = EllipticCurveImpl.sign(signer, toSign())
    nonSignedBlock.copy(signerData = SignerData(signer, ByteStr(signature)))
  }).left.map(GenericError)

  def genesisTransactions(gs: GenesisSettings): Seq[GenesisTransaction] = {
    gs.transactions.map { ts =>
      val acc = Address.fromString(ts.recipient).right.get
      GenesisTransaction.create(acc, ts.amount, gs.timestamp).right.get
    }
  }

  def genesis(genesisSettings: GenesisSettings): Either[ValidationError, Block] = {

    val genesisSigner = PrivateKeyAccount(Array.empty)

    val transactionGenesisData = genesisTransactions(genesisSettings)
    val transactionGenesisDataField = TransactionsBlockFieldVersion1or2(transactionGenesisData)
    val consensusGenesisData = NxtLikeConsensusBlockData(genesisSettings.initialBaseTarget, ByteStr(Array.fill(DigestSize)(0: Byte)))
    val consensusGenesisDataField = NxtConsensusBlockField(consensusGenesisData)
    val txBytesSize = transactionGenesisDataField.bytes().length
    val txBytes = Bytes.ensureCapacity(Ints.toByteArray(txBytesSize), 4, 0) ++ transactionGenesisDataField.bytes()
    val cBytesSize = consensusGenesisDataField.bytes().length
    val cBytes = Bytes.ensureCapacity(Ints.toByteArray(cBytesSize), 4, 0) ++ consensusGenesisDataField.bytes()

    val reference = Array.fill(SignatureLength)(-1: Byte)

    val timestamp = genesisSettings.blockTimestamp
    val toSign: Array[Byte] = Array(GenesisBlockVersion) ++
      Bytes.ensureCapacity(Longs.toByteArray(timestamp), 8, 0) ++
      reference ++
      cBytes ++
      txBytes ++
      genesisSigner.publicKey

    val signature = genesisSettings.signature.fold(EllipticCurveImpl.sign(genesisSigner, toSign))(_.arr)

    if (EllipticCurveImpl.verify(signature, toSign, genesisSigner.publicKey))
      Right(Block(timestamp = timestamp,
        version = GenesisBlockVersion,
        reference = ByteStr(reference),
        signerData = SignerData(genesisSigner, ByteStr(signature)),
        consensusData = consensusGenesisData,
        transactionData = transactionGenesisData,
        featureVotes = Set.empty))
    else Left(GenericError("Passed genesis signature is not valid"))
  }

  val GenesisBlockVersion: Byte = 1
  val PlainBlockVersion: Byte = 2
  val NgBlockVersion: Byte = 3
}
