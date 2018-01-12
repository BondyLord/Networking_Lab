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

    //private final long pf_maxNumberOfTokens;
    //private long pm_availableNumberOfTokens;
    
    private AtomicLong pf_maxNumberOfTokens;
    private AtomicLong pm_availableNumberOfTokens;
    private boolean pm_bucketIsTerminated= false;

    protected TokenBucket(long i_maxNumberOfTokens) {
    	this.pf_maxNumberOfTokens = new AtomicLong(i_maxNumberOfTokens);
    	this.pm_availableNumberOfTokens = new AtomicLong(i_maxNumberOfTokens);
        //this.pf_maxNumberOfTokens = i_maxNumberOfTokens;
        //this.pm_availableNumberOfTokens= i_maxNumberOfTokens;
    }

    protected void take(long tokens) {
        //TODO
    	System.out.println("Number of aviliable tokens - " + pm_availableNumberOfTokens.get());
        while(pm_availableNumberOfTokens.get() - tokens < 0){
            try {
                Thread.sleep(500); // Maybe implement the block in a different way
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        
        pm_availableNumberOfTokens.set(pm_availableNumberOfTokens.get() - tokens);

    }

    protected void terminate() {
        pm_bucketIsTerminated = true;
    }

    boolean terminated() {
        return pm_bucketIsTerminated;
    }

    void set(long tokens) {
        if(tokens <= pf_maxNumberOfTokens.get())
            pm_availableNumberOfTokens.set(tokens);
        else
            pm_availableNumberOfTokens.set(pf_maxNumberOfTokens.get());
    }
    
    void add(long tokens)
    {
        if(pm_availableNumberOfTokens.get() + tokens <= pf_maxNumberOfTokens.get())
        	pm_availableNumberOfTokens.getAndAdd(tokens);
        else
        	pm_availableNumberOfTokens.set(pf_maxNumberOfTokens.get());
    }
}
