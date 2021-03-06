/**
 * A token bucket based rate-limiter.
 *
 * This class should implement a "soft" rate limiter by adding maxBytesPerSecond tokens to the bucket every second,
 * or a "hard" rate limiter by resetting the bucket to maxBytesPerSecond tokens every second.
 */
public class RateLimiter implements Runnable {

    private static final String MODULE_NAME = "RateLimiter";
    private static final int RATE_IN_MILLISECOND = 1000;
    private final TokenBucket tokenBucket;
    private final Long maxBytesPerSecond;

    RateLimiter(TokenBucket tokenBucket, Long maxBytesPerSecond) {
        this.tokenBucket = tokenBucket;
        this.maxBytesPerSecond = maxBytesPerSecond;
    }

    @Override
    public void run() {
        // While token bucket is alive, add maxBytesPerSecond tokens each quanta of time
        while (!tokenBucket.terminated()) {
            try {
                Thread.sleep(RATE_IN_MILLISECOND);
                tokenBucket.add(maxBytesPerSecond);
            } catch (InterruptedException e) {
                System.err.println( "Interrupted Exception while sleep");
            }
        }
    }
}
