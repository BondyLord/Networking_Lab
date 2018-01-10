
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

    private final long pf_maxNumberOfTokens;
    private long pm_availableNumberOfTokens;
    private boolean pm_bucketIsTerminated= false;

    protected TokenBucket(long i_maxNumberOfTokens) {
        this.pf_maxNumberOfTokens = i_maxNumberOfTokens;
        this.pm_availableNumberOfTokens= i_maxNumberOfTokens;
    }

    protected void take(long tokens) {
        //TODO
        synchronized (this){
            while(pm_availableNumberOfTokens- tokens < 0){
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            pm_availableNumberOfTokens-= tokens;
        }
    }

    protected void terminate() {
        pm_bucketIsTerminated= true;
    }

    boolean terminated() {
        return pm_bucketIsTerminated;
    }

    void set(long tokens) {
        if(tokens <= pf_maxNumberOfTokens)
            pm_availableNumberOfTokens = tokens;
        else
            pm_availableNumberOfTokens = pf_maxNumberOfTokens;
    }
}
