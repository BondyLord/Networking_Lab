import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A Token Bucket (https://en.wikipedia.org/wiki/Token_bucket)
 *
 * This thread-safe bucket should support the following methods:
 *
 * - take(n): remove n tokens from the bucket (blocks until n tokens are available and taken)
 * - set(n): set the bucket to contain n tokens (to allow "hard" rate limiting)
 * - add(n): add n tokens to the bucket (to allow "soft" rate limiting)
 * - terminate(): mark the bucket as terminated (used to communicate between threads)
 * - terminated(): return true if the bucket is terminated, false otherwise
 *
 */
class TokenBucket {
    
    private AtomicLong pf_maxNumberOfTokens;
    private AtomicLong pm_availableNumberOfTokens;
    private boolean infinitTokens;
    private boolean pm_bucketIsTerminated= false;
    
    protected TokenBucket(long i_maxNumberOfTokens, boolean infinitTokens) {
    	this.infinitTokens = infinitTokens;
    	this.pf_maxNumberOfTokens = new AtomicLong(i_maxNumberOfTokens);
    	this.pm_availableNumberOfTokens = new AtomicLong(0);
    }

    protected void take(long tokens) {
    	if(!infinitTokens)
    	{
	    	while(pm_availableNumberOfTokens.updateAndGet(value -> value >= tokens ? value - tokens : value) < tokens)
	    	{
	    		try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
	    		
	    	}
    	}
    }

    protected void terminate() {
        pm_bucketIsTerminated = true;
    }

    boolean terminated() {
        return pm_bucketIsTerminated;
    }
    
    void add(long tokens)
    {
    	pm_availableNumberOfTokens.getAndAdd(tokens);
    	}
}
