package reductions

import scala.annotation._
import org.scalameter._

object ParallelParenthesesBalancingRunner {

    @volatile var seqResult = false

    @volatile var parResult = false

    val standardConfig = config(
        Key.exec.minWarmupRuns := 40,
        Key.exec.maxWarmupRuns := 80,
        Key.exec.benchRuns := 120,
        Key.verbose := false
    ) withWarmer(new Warmer.Default)

    def main(args: Array[String]): Unit = {
        val length = 100000000
        val chars = new Array[Char](length)
        val threshold = 10000
        val seqtime = standardConfig measure {
            seqResult = ParallelParenthesesBalancing.balance(chars)
        }
        println(s"sequential result = $seqResult")
        println(s"sequential balancing time: $seqtime")

        val fjtime = standardConfig measure {
            parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
        }
        println(s"parallel result = $parResult")
        println(s"parallel balancing time: $fjtime")
        println(s"speedup: ${seqtime.value / fjtime.value}")
    }
}

object ParallelParenthesesBalancing extends ParallelParenthesesBalancingInterface {

    /** Returns `true` iff the parentheses in the input `chars` are balanced.
     */
    def balance(chars: Array[Char]): Boolean = {
        var stack: Int = 0
        for (c <- chars) {
            if (c == '(') {stack = stack + 1}
            else if (c == ')') {stack = stack - 1}
            if (stack < 0) {return false}
        }

        if (stack != 0) {
            return false
        } else {
            return true
        }
    }

    /** Returns `true` iff the parentheses in the input `chars` are balanced.
     */
    def parBalance(chars: Array[Char], threshold: Int): Boolean = {

        def traverse(idx: Int, until: Int): (Int, Int) = {
            var active_left, front_right = 0
            var i = idx
            var first_left = false

            while (i < until) {
                if (first_left) {
                    if (chars(i) == '(') {
                        active_left = active_left + 1
                    } else if (chars(i) == ')') {
                        active_left = active_left - 1
                    }
                } else {
                    if (chars(i) == '(') {
                        first_left = true
                        active_left = active_left + 1
                    } else if (chars(i) == ')') {
                        front_right = front_right + 1
                    }
                }

                i = i + 1
            }

            return (active_left, front_right)

        }


        def reduce(from: Int, until: Int): (Int, Int)  = {

            // parallel
            if ((until - from) < threshold) {
                return traverse(from, until)
            } else {
                val mid: Int = from + (until - from) / 2
                val (left, right) = parallel(reduce(from, mid), reduce(mid, until))

                // combine two subtrees
                return (left._1 - right._2 + right._1, left._2)
            }

        }

        if (chars.isEmpty) {
            return true
        } else {
            val result = reduce(0, chars.length)
            return result._1 == 0 & result._2 == 0
        }


    }


}
