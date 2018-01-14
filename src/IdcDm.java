import Utill.Utilities;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.concurrent.*;

public class IdcDm {

    static long fileSize;
    private static final String MODULE_NAME = "IdcDm";
    private static final int SMALLEST_RANGE_SIZE = HTTPRangeGetter.CHUNK_SIZE * 10;
    private static final int MAX_DOWNLOAD_ATTEMPTS = 3;
    private static final int TIME_BETWEEN_ATTEMPTS = 4000;
    private static int numberOfWorkers;
    private static Long maxBytesPerSecond;
    private static String url;
    private static int numberOfDownloadAttempts;
    private static DownloadableMetadata downloadableMetadata;

    private static final String END_OF_DOWNLOAD_MESSAGE = "Download %s \n";
    private static final String METADATA_FILE_WAS_FOUND_MESSAGE = "Metadata file was found. Resume downloading... \n";
    private static final String RETRIEVE_DATA_MESSAGE = "Retrieving failed data - attempt number: %d \n";


    /**
     * Receive arguments from the command-line, provide some feedback and start the download.
     *
     * @param args command-line arguments
     * @throws InterruptedException thrown interrupted in the case of Ctl+C
     */
    public static void main(String[] args) throws InterruptedException {
        numberOfWorkers = 1;
        maxBytesPerSecond = null;
        numberOfDownloadAttempts = 0;


        if (args.length < 1 || args.length > 3) {
            System.err.printf("usage:\n\tjava IdcDm URL [MAX-CONCURRENT-CONNECTIONS] [MAX-DOWNLOAD-LIMIT]\n");
            System.exit(1);
        } else if (args.length >= 2) {
            numberOfWorkers = Integer.parseInt(args[1]);
            if (args.length == 3)
                maxBytesPerSecond = Long.parseLong(args[2]);
        }

        url = args[0];

        System.err.printf("Downloading");
        if (numberOfWorkers > 1)
            System.err.printf(" using %d connections", numberOfWorkers);
        if (maxBytesPerSecond != null)
            System.err.printf(" limited to %d Bps", maxBytesPerSecond);
        System.err.printf("...\n");

        downloadFile();
    }

    private static void downloadFile() throws InterruptedException{

        String downloadStatus = "failed";
        File metaDataFile;

        fileSize = getFileSize(url);
        downloadableMetadata = new DownloadableMetadata(url);
        metaDataFile = new File(downloadableMetadata.getMetadataFilename());
        if (metaDataFile.exists()) {
            try {
                getAndSetMeteDataFromFile(metaDataFile);
                System.out.printf(METADATA_FILE_WAS_FOUND_MESSAGE);
            } catch (IOException e) {
                Utilities.ErrorLog(MODULE_NAME,"Could not read metadata file!");
            }
        }else{
            if(fileSize == -1){
                Utilities.ErrorLog(MODULE_NAME,"Could not get file size...");
            }
        }
        /* Try to download a file for max number of attempts attempts.
           In case of missing data, due to timeout connection or any other
           error in one of the concurrent connections will try to recover
           these missing ranges.
        */
        while (fileSize != -1
                && !downloadableMetadata.isCompleted()
                && numberOfDownloadAttempts <= MAX_DOWNLOAD_ATTEMPTS) {

            numberOfDownloadAttempts++;
            if (numberOfDownloadAttempts > 1) {
                System.out.printf(RETRIEVE_DATA_MESSAGE, numberOfDownloadAttempts);
            }
            try {
                Download();
            } catch (Exception e) {
                Utilities.Log(MODULE_NAME, "There was an exception during attempt number " + numberOfDownloadAttempts);
                Thread.sleep(TIME_BETWEEN_ATTEMPTS);
            }
        }

        if (downloadableMetadata.isCompleted()) {
            FileWriter.renameTmp(downloadableMetadata.getFilename());
            downloadableMetadata.delete();
            downloadStatus = "succeeded";
        } else {
            Utilities.Log(MODULE_NAME, "Exceeded number of max tries - " + numberOfDownloadAttempts);
        }
        System.out.printf(END_OF_DOWNLOAD_MESSAGE, downloadStatus);
    }

    /**
     * Initiate the file's metadata, and iterate over missing ranges. For each:
     * 1. Setup the Queue, TokenBucket, DownloadableMetadata, FileWriter, RateLimiter, and a pool of HTTPRangeGetters
     * 2. Join the HTTPRangeGetters, send finish marker to the Queue and terminate the TokenBucket
     * 3. Join the FileWriter and RateLimiter
     * <p>
     * Finally, print "Download succeeded/failed" and delete the metadata as needed.
     */
    private static void Download(){

        ArrayList<Range> ranges;
        //1. Setup the Queue, TokenBucket, DownloadableMetadata, FileWriter, RateLimiter, and a pool of HTTPRangeGetters
        ranges = downloadableMetadata.getMissingRanges();

        if (fileSize != -1) {

            int chunkQueueSize = (int) fileSize;
            TokenBucket tokenBucket;
            RateLimiter rateLimiter;
            FileWriter fileWriter;
            Thread fileWriterThread;

            Utilities.Log(MODULE_NAME, "chunkQueueSize is: " + chunkQueueSize);
            BlockingQueue<Chunk> chunkQueue = new ArrayBlockingQueue<>(chunkQueueSize);
            if (maxBytesPerSecond != null) {
                tokenBucket = new TokenBucket(false);
                rateLimiter = new RateLimiter(tokenBucket, maxBytesPerSecond);
            } else {
                tokenBucket = new TokenBucket(true);
                rateLimiter = new RateLimiter(tokenBucket, Long.MAX_VALUE);
            }

            fileWriter = new FileWriter(downloadableMetadata, chunkQueue);
            fileWriterThread = new Thread(fileWriter);
            Utilities.Log(MODULE_NAME, "starting fileWriterThread");
            fileWriterThread.start();
            Thread rateLimiterThread = new Thread(rateLimiter);
            Utilities.Log(MODULE_NAME, "starting rateLimiterThread");
            rateLimiterThread.start();

            // Split the ranges between workers
            ExecutorService httpRangeGetterTPExecutor =
                    executeHttpRangeGetterThreadPool(
                            url,
                            numberOfWorkers,
                            chunkQueueSize,
                            chunkQueue,
                            tokenBucket,
                            ranges
                    );

            // 2. Join the HTTPRangeGetters, send finish marker to the Queue and terminate the TokenBucket
            joinThreads(chunkQueue, fileWriterThread, tokenBucket, rateLimiterThread, httpRangeGetterTPExecutor);
        }
    }

    private static void getAndSetMeteDataFromFile(File metadataFile) throws IOException {

        // Read and set metaData serialized object

        try (InputStream readMetaDateFile = new FileInputStream(metadataFile);
             ObjectInput metaData = new ObjectInputStream(readMetaDateFile)){

            Utilities.Log(MODULE_NAME, "Resume Download");
            Utilities.Log(MODULE_NAME, "Reading meta data file...");
            downloadableMetadata = (DownloadableMetadata) metaData.readObject();
        } catch (FileNotFoundException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static ExecutorService executeHttpRangeGetterThreadPool(
            String url,
            int numberOfWorkers,
            int chunkQueueSize,
            BlockingQueue<Chunk> chunkQueue, TokenBucket tokenBucket,
            ArrayList<Range> ranges) {

        ExecutorService httpRangeGetterTPExecutor = Executors.newFixedThreadPool(numberOfWorkers);
        int relevantNumberOfWorkers;
        int maxNumberOfWorkers;
        long rangeChunkSize;
        long startRange;
        long endRange;
        Utilities.Log(MODULE_NAME, "rangeChunkSize is: " + chunkQueueSize);

        // loop over missing ranges and split work between workers
        for (Range mainRange : ranges) {
            // calculate the max number of workers needed for this range
            maxNumberOfWorkers = rangeMaximalNumberOfConnections(mainRange);
            relevantNumberOfWorkers = Math.min(numberOfWorkers, maxNumberOfWorkers);
            Utilities.Log(MODULE_NAME, "Set relevant number of workers: " + relevantNumberOfWorkers);
            rangeChunkSize = (int) Math.ceil(((double) mainRange.getLength() / relevantNumberOfWorkers));
            startRange = mainRange.getStart();
            endRange = startRange + rangeChunkSize;

            for (int i = 0; i < relevantNumberOfWorkers; i++) {

                Utilities.Log(MODULE_NAME, "Starting a HTTPRangeGetter thread with ranges:");
                Utilities.Log(MODULE_NAME, "startRange: " + startRange);
                Utilities.Log(MODULE_NAME, "endRange: " + endRange);

                Range range = new Range(startRange, endRange);
                HTTPRangeGetter httpRangeGetter = new HTTPRangeGetter(url, range, chunkQueue, tokenBucket);
                httpRangeGetterTPExecutor.execute(httpRangeGetter);
                startRange = endRange + 1;
                // final process range should end at the end of the main range
                if (i == relevantNumberOfWorkers - 2) {
                    endRange = mainRange.getEnd();
                }
                else
                {
                    endRange += rangeChunkSize;
                }
                Utilities.Log(MODULE_NAME, "Executing a HTTPRangeGetter thread with ranges:");
            }

        }
        return httpRangeGetterTPExecutor;
    }

    private static void joinThreads(
            BlockingQueue<Chunk> chunkQueue,
            Thread fileWriterThread,
            TokenBucket tokenBucket,
            Thread rateLimiterThread,
            ExecutorService httpRangeGetterTPExecutor) {
        try {
            // join httpRangeGetter thread pool
            httpRangeGetterTPExecutor.shutdown();
            while (!httpRangeGetterTPExecutor.awaitTermination(24L, TimeUnit.HOURS)) {
                Utilities.Log(MODULE_NAME, "Not yet. Still waiting for termination");
            }

            // -1 offset in a chunk marks end of queue
            chunkQueue.put(new Chunk(new byte[0], -1, 0));
            tokenBucket.terminate();

            // 3. Join the FileWriter and RateLimiter
            fileWriterThread.join();
            rateLimiterThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static int getFileSize(String urlString) {
        URL url;
        HttpURLConnection httpConnection = null;
        try{
            url = new URL(urlString);
            httpConnection = (HttpURLConnection) url.openConnection();
            httpConnection.setRequestMethod("HEAD");
            return httpConnection.getContentLength();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            httpConnection.disconnect();
        }
        return -1;
    }

    private static int rangeMaximalNumberOfConnections(Range i_range) {
        return (int) Math.ceil((((double) i_range.getLength()) / SMALLEST_RANGE_SIZE));
    }
}
