package kmeans

import java.util.concurrent._
import scala.collection.{mutable, Map, Seq}
import scala.collection.parallel.{ParMap, ParSeq}
import scala.collection.parallel.CollectionConverters._
import scala.math._

class KMeansSuite extends munit.FunSuite {
  object KM extends KMeans
  import KM._

  def checkParClassify(points: ParSeq[Point], means: ParSeq[Point], expected: ParMap[Point, ParSeq[Point]]): Unit = {
    assertEquals(classify(points, means), expected, s"classify($points, $means) should equal to $expected")
  }

  test("'classify' should work for empty 'points' and empty 'means'") {
    val points: ParSeq[Point] = IndexedSeq().par
    val means: ParSeq[Point] = IndexedSeq().par
    val expected = ParMap[Point, ParSeq[Point]]()
    checkParClassify(points, means, expected)
  }

  import scala.concurrent.duration._
  override val munitTimeout = 10.seconds
}


