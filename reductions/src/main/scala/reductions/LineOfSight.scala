package reductions

import org.scalameter._

object LineOfSightRunner {

    val standardConfig = config(
        Key.exec.minWarmupRuns := 40,
        Key.exec.maxWarmupRuns := 80,
        Key.exec.benchRuns := 100,
        Key.verbose := false
    ) withWarmer(new Warmer.Default)

    def main(args: Array[String]): Unit = {
        val length = 10000000
        val input = (0 until length).map(_ % 100 * 1.0f).toArray
        val output = new Array[Float](length + 1)
        val seqtime = standardConfig measure {
            LineOfSight.lineOfSight(input, output)
        }
        println(s"sequential time: $seqtime")

        val partime = standardConfig measure {
            LineOfSight.parLineOfSight(input, output, 10000)
        }
        println(s"parallel time: $partime")
        println(s"speedup: ${seqtime.value / partime.value}")
    }
}

sealed abstract class Tree {
    def maxPrevious: Float
}

case class Node(left: Tree, right: Tree) extends Tree {
    val maxPrevious = left.maxPrevious.max(right.maxPrevious)
}

case class Leaf(from: Int, until: Int, maxPrevious: Float) extends Tree

object LineOfSight extends LineOfSightInterface {

    def lineOfSight(input: Array[Float], output: Array[Float]): Unit = {
        var max: Float = 0
        for (i <- 0 until input.length) {
            if (i == 0) {
                output(0) = 0
            } else {
                max = Math.max(input(i) / i, max)
                output(i) = max
            }
        }



    }

    /** Traverses the specified part of the array and returns the maximum angle.
     */
    def upsweepSequential(input: Array[Float], from: Int, until: Int): Float = {
        var max = input(from) / from
        for (i <- from+1 until until) {
            max = Math.max(max, input(i) / i)
        }
        return max
    }

    /** Traverses the part of the array starting at `from` and until `end`, and
     *  returns the reduction tree for that part of the array.
     *
     *  The reduction tree is a `Leaf` if the length of the specified part of the
     *  array is smaller or equal to `threshold`, and a `Node` otherwise.
     *  If the specified part of the array is longer than `threshold`, then the
     *  work is divided and done recursively in parallel.
     */
    def upsweep(input: Array[Float], from: Int, end: Int,
                threshold: Int): Tree = {

        if (end - from <= threshold) {
            return Leaf(from, end, upsweepSequential(input, from, end))
        } else {
            val mid = (from + end) / 2
            val (left, right) = parallel(
                upsweep(input, from, mid, threshold),
                upsweep(input, mid, end, threshold))
            return Node(left, right)
        }

    }

    /** Traverses the part of the `input` array starting at `from` and until
     *  `until`, and computes the maximum angle for each entry of the output array,
     *  given the `startingAngle`.
     */
    def downsweepSequential(input: Array[Float], output: Array[Float],
                            startingAngle: Float, from: Int, until: Int): Unit = {
        var max = startingAngle
        for (i <- from until until) {
            max = Math.max(input(i) / i, max)
            output(i) = max
        }

    }

    /** Pushes the maximum angle in the prefix of the array to each leaf of the
     *  reduction `tree` in parallel, and then calls `downsweepSequential` to write
     *  the `output` angles.
     */
    def downsweep(input: Array[Float], output: Array[Float], startingAngle: Float,
                  tree: Tree): Unit = {
        tree match {
            case Leaf(from, until, maxPrevious) => downsweepSequential(input, output, startingAngle, from, until)
            case Node(left, right) => parallel(
                downsweep(input, output, startingAngle, left),
                downsweep(input, output, Math.max(left.maxPrevious, startingAngle), right)
            )
        }

    }

    /** Compute the line-of-sight in parallel. */
    def parLineOfSight(input: Array[Float], output: Array[Float],
                       threshold: Int): Unit = {
        val t = upsweep(input, 0, input.length, threshold)
        downsweep(input, output, 0f, t)
    }
}
