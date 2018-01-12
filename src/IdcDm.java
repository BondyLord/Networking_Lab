import Utill.Utilities;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.concurrent.*;

public class IdcDm {
    static final String MODULE_NAME = "IdcDm";
    static int fileSize;

    /**
     * Receive arguments from the command-line, provide some feedback and start the download.
     *
     * @param args command-line arguments
     */
    public static void main(String[] args) {
        int numberOfWorkers = 1;
        Long maxBytesPerSecond = null;

        if (args.length < 1 || args.length > 3) {
            System.err.printf("usage:\n\tjava IdcDm URL [MAX-CONCURRENT-CONNECTIONS] [MAX-DOWNLOAD-LIMIT]\n");
            System.exit(1);
        } else if (args.length >= 2) {
            numberOfWorkers = Integer.parseInt(args[1]);
            if (args.length == 3)
                maxBytesPerSecond = Long.parseLong(args[2]);
        }

        String url = args[0];

        System.err.printf("Downloading");
        if (numberOfWorkers > 1)
            System.err.printf(" using %d connections", numberOfWorkers);
        if (maxBytesPerSecond != null)
            System.err.printf(" limited to %d Bps", maxBytesPerSecond);
        System.err.printf("...\n");

        DownloadURL(url, numberOfWorkers, maxBytesPerSecond);
    }

    /**
     * Initiate the file's metadata, and iterate over missing ranges. For each:
     * 1. Setup the Queue, TokenBucket, DownloadableMetadata, FileWriter, RateLimiter, and a pool of HTTPRangeGetters
     * 2. Join the HTTPRangeGetters, send finish marker to the Queue and terminate the TokenBucket
     * 3. Join the FileWriter and RateLimiter
     * <p>
     * Finally, print "Download succeeded/failed" and delete the metadata as needed.
     *
     * @param url               URL to download
     * @param numberOfWorkers   number of concurrent connections
     * @param maxBytesPerSecond limit on download bytes-per-second
     */
    private static void DownloadURL(String url, int numberOfWorkers, Long maxBytesPerSecond) {
        // Initiate the file's metadata, and iterate over missing ranges. For each:
        DownloadableMetadata downloadableMetadata = new DownloadableMetadata(url);
        File metadataFile = new File(downloadableMetadata.getFilename());
        if (metadataFile.exists()){
            //<TODO iterate over missing ranges>
        }

        String downloadStatus = "failed";
        //1. Setup the Queue, TokenBucket, DownloadableMetadata, FileWriter, RateLimiter, and a pool of HTTPRangeGetters
        // FileSize is in Bytes.
        fileSize = getFileSize(url);
        int chunkQueueSize = (int) Math.ceil((double) fileSize / HTTPRangeGetter.CHUNK_SIZE); //round up
        Utilities.Log(MODULE_NAME, "chunkQueueSize is: " + chunkQueueSize);

        BlockingQueue<Chunk> chunkQueue = new ArrayBlockingQueue<>(chunkQueueSize);
        FileWriter fileWriter = new FileWriter(downloadableMetadata, chunkQueue);
        Thread fileWriterThread = new Thread(fileWriter);
        Utilities.Log(MODULE_NAME, "starting fileWriterThread");
        fileWriterThread.start();

        TokenBucket tokenBucket = null;
        RateLimiter rateLimiter = null;

        if(maxBytesPerSecond != null)
        {
        	tokenBucket = new TokenBucket(maxBytesPerSecond);
        	rateLimiter = new RateLimiter(tokenBucket, maxBytesPerSecond);
        }
        else
        {
        	tokenBucket = new TokenBucket(Integer.MAX_VALUE);
            rateLimiter = new RateLimiter(tokenBucket, Long.MAX_VALUE);
        }

        Thread rateLimiterThread = new Thread(rateLimiter);
        Utilities.Log(MODULE_NAME, "starting rateLimiterThread");
        rateLimiterThread.start();

        ScheduledExecutorService httpRangeGetterTPExecutor = Executors.newScheduledThreadPool(numberOfWorkers);
        long startRange = 0L;
        long rangeChunkSize = (int) Math.ceil(((double)fileSize / numberOfWorkers));
        long endRange = rangeChunkSize; // Shister, your very smart for thinking about this
        Utilities.Log(MODULE_NAME, "rangeChunkSize is: " + chunkQueueSize);

        //<TODO this code is duplicate (implemented in HTTPRangeGetter) - we should decide where it needs to be implemented>
        for (int i = 0; i < numberOfWorkers; i++) {

            Utilities.Log(MODULE_NAME, "Starting a HTTPRangeGetter thread with ranges:");
            Utilities.Log(MODULE_NAME, "startRange: " + startRange);
            Utilities.Log(MODULE_NAME, "endRange: " + endRange);
            Range range = new Range(startRange, endRange);
            startRange = endRange + 1;
            endRange += rangeChunkSize;
            HTTPRangeGetter httpRangeGetter = new HTTPRangeGetter(url, range, chunkQueue, tokenBucket);
            Utilities.Log(MODULE_NAME, "Executing a HTTPRangeGetter thread with ranges:");
            httpRangeGetterTPExecutor.execute(httpRangeGetter);

        }
        Utilities.Log(MODULE_NAME, "Starting: Join the HTTPRangeGetters, send finish marker to the Queue and terminate the TokenBucket");
        // 2. Join the HTTPRangeGetters, send finish marker to the Queue and terminate the TokenBucket
        try {
            httpRangeGetterTPExecutor.shutdown();
            while (!httpRangeGetterTPExecutor.awaitTermination(24L, TimeUnit.HOURS)) {
                System.out.println("Not yet. Still waiting for termination");
            }
            // -1 offset in a chunk marks end of queue
            chunkQueue.put(new Chunk(new byte[0],-1,0));
            tokenBucket.terminate();
            Utilities.Log(MODULE_NAME, "Finished: Join the HTTPRangeGetters, send finish marker to the Queue and terminate the TokenBucket");

            // 3. Join the FileWriter and RateLimiter
            Utilities.Log(MODULE_NAME, "Starting: Join the FileWriter and RateLimiter");
            fileWriterThread.join();
            rateLimiterThread.join();
            Utilities.Log(MODULE_NAME, "Finished: Join the FileWriter and RateLimiter");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Finally, print "Download succeeded/failed" and delete the metadata as needed.
        if (downloadableMetadata.isCompleted()){
            downloadStatus = "succeeded";
            downloadableMetadata.delete();
        }
        System.out.println("Download " + downloadStatus);
    }


    protected static int getFileSize(String urlString) {
        URL url = null;
        HttpURLConnection httpConnection = null;
        try {
            url = new URL(urlString);
            httpConnection = (HttpURLConnection) url.openConnection();
            // can ask for content length only...
            httpConnection.setRequestMethod("HEAD");
            httpConnection.getInputStream();
            return httpConnection.getContentLength();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            httpConnection.disconnect();
        }
        return -1;
    }
}
