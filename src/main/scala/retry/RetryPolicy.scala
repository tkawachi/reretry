package retry

import scala.annotation.tailrec

case class RetryPolicy(getDelay: Int => Option[Long]) {
  def append(another: => RetryPolicy) = new RetryPolicy(
    n => for {
      a <- this.getDelay(n)
      b <- another.getDelay(n)
    } yield a.max(b)
  )

  /** alias to append */
  def |+|(policy: => RetryPolicy) = append(policy)
}

object RetryPolicy {
  /** Retry immediately, but only up to n times */
  def limitRetries(i: Int): RetryPolicy =
    new RetryPolicy(n => if (n >= i) None else Some(0L))

  /**
   * Add an upperbound to a policy such that once the given time-delay
   * amount has been reached or exceeded, the policy will stop retrying
   * and fail.
   */
  def limitRetriesByDelay(i: Int, p: RetryPolicy): RetryPolicy = {
    def limit(delay: Long) = if (delay >= i) None else Some(delay)
    new RetryPolicy(n => p.getDelay(n).flatMap(limit))
  }

  /** a constant delay with unlimited retries. */
  def constantDelay(delay: Long): RetryPolicy = new RetryPolicy(_ => Some(delay))

  /** Grow delay exponentially each iteration. */
  def exponentialBackoff(base: Int): RetryPolicy = new RetryPolicy(n => Some(math.pow(2, n).toLong * base))

  /** Fibonacci backoff. */
  def fibonacciBackoff(base: Int): RetryPolicy = {
    @tailrec
    def fib(m: Int, a: Long, b: Long): Long =
      if (m == 0) a
      else fib(m - 1, b, a + b)
    new RetryPolicy(n => Some(fib(n + 1, 0, base)))
  }

  /**
   * Set a time-upperbound for any delays that may be directed by the
   * given policy.  This function does not terminate the retrying.  The policy
   * `capDelay(maxDelay, exponentialBackoff(n))` will never stop retrying.  It
   * will reach a state where it retries forever with a delay of `maxDelay`
   * between each one.  To get termination you need to use one of the
   * 'limitRetries' function variants.
   * @param limit A maximum delay in microseconds
   */
  def capDelay(limit: Long, p: RetryPolicy): RetryPolicy =
    new RetryPolicy( n => p.getDelay(n).map(_.min(limit)))

  def defaultPolicy = constantDelay(50000) |+| limitRetries(5)

  def retrying[A](policy: RetryPolicy)(chk: (Int, A) => Boolean)(f: => A): A = {
    @tailrec
    def go(n: Int): A = {
      val res = f
      chk(n, res) match {
        case true =>
          policy.getDelay(n) match {
            case Some(delay) =>
              Thread.sleep(delay)
              go(n + 1)
            case None => res
          }
        case false => res
      }
    }
    go(0)
  }
}
