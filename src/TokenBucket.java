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

    TokenBucket() {
        //TODO
    }

    void take(long tokens) {
        //TODO
    }

    void terminate() {
        //TODO
    }

    boolean terminated() {
        //TODO
    }

    void set(long tokens) {
        //TODO
    }
}
