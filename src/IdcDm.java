import Utill.Utilities;
//<TODO remove import>
//import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.concurrent.*;

public class IdcDm {
    static final String MODULE_NAME = "IdcDm";
    static long fileSize;
    static int numberOfWorkers;
    static Long maxBytesPerSecond;
    static String url;
    static int numberOfDownloadAttempts;
    static DownloadableMetadata downloadableMetadata;

    /**
     * Receive arguments from the command-line, provide some feedback and start the download.
     *
     * @param args command-line arguments
     * @throws InterruptedException 
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

        downloadableMetadata = new DownloadableMetadata(url);
        
        while(!downloadableMetadata.isCompleted() && numberOfDownloadAttempts <= 3)
        {
        	numberOfDownloadAttempts++;
        	
        	try{
            	Utilities.Log(MODULE_NAME, "Starting Download number - " + numberOfDownloadAttempts);
            	DownloadURL(url, numberOfWorkers, maxBytesPerSecond);
        	} catch (Exception e) {
        		Utilities.Log(MODULE_NAME, "There was an exception during attempt number " + numberOfDownloadAttempts);
        		Thread.sleep(4000);
			}

        }
        
        if(downloadableMetadata.isCompleted())
        {
        	FileWriter.renameTmp(downloadableMetadata.getFilename());
            downloadableMetadata.delete();
            System.out.println("Download success!");
            
        }
            
        else
        	Utilities.Log(MODULE_NAME, "Exceded number of max trys - " + numberOfDownloadAttempts);
        	
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
     * @throws Exception 
     */
    private static void DownloadURL(String url, int numberOfWorkers, Long maxBytesPerSecond) throws Exception {
        // Initiate the file's metadata, and iterate over missing ranges. For each:
        File metadataFile = new File(downloadableMetadata.getMetadataFilename());
        if (metadataFile.exists()) {
            try {
                Utilities.Log(MODULE_NAME, "Resume Download");
                Utilities.Log(MODULE_NAME, "Reading meta data file...");
                InputStream readMetaDateFile = new FileInputStream(metadataFile);
                ObjectInput metaData = new ObjectInputStream(readMetaDateFile);
                downloadableMetadata = (DownloadableMetadata) metaData.readObject();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }

        String downloadStatus = "failed";
        //1. Setup the Queue, TokenBucket, DownloadableMetadata, FileWriter, RateLimiter, and a pool of HTTPRangeGetters
        // FileSize is in Bytes.
        fileSize = getFileSize(url);
        
        if(fileSize != -1)
        {
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
	        	tokenBucket = new TokenBucket(fileSize, false);
	        	rateLimiter = new RateLimiter(tokenBucket, maxBytesPerSecond);
	        }
	        else
	        {
	        	tokenBucket = new TokenBucket(Integer.MAX_VALUE, true);
	            rateLimiter = new RateLimiter(tokenBucket, Long.MAX_VALUE);
	        }
	
	        Thread rateLimiterThread = new Thread(rateLimiter);
	        Utilities.Log(MODULE_NAME, "starting rateLimiterThread");
	        rateLimiterThread.start();
	
	        ExecutorService httpRangeGetterTPExecutor =
	                executeHttpRangeGetterThreadPool(
	                        url,
	                        numberOfWorkers,
	                        chunkQueueSize,
	                        chunkQueue,
	                        tokenBucket,
	                        downloadableMetadata);
	
	        // 2. Join the HTTPRangeGetters, send finish marker to the Queue and terminate the TokenBucket
	        joinThreads(chunkQueue, fileWriterThread, tokenBucket, rateLimiterThread, httpRangeGetterTPExecutor);
        }
        else
        {
        	throw new Exception("this is an exception");
        }

        // Finally, print "Download succeeded/failed" and delete the metadata as needed.
        //printStatusAndDeleteMeta(downloadableMetadata, downloadStatus);
    }


    //@NotNull
    private static ExecutorService executeHttpRangeGetterThreadPool(
            String url,
            int numberOfWorkers,
            int chunkQueueSize,
            BlockingQueue<Chunk> chunkQueue, TokenBucket tokenBucket,
            DownloadableMetadata downloadableMetadata) {
    	
        ArrayList<Range> ranges;
        ExecutorService httpRangeGetterTPExecutor = Executors.newFixedThreadPool(numberOfWorkers);
        long rangeChunkSize = 0;
        long startRange = 0L;
        long endRange = 0L;
        Utilities.Log(MODULE_NAME, "rangeChunkSize is: " + chunkQueueSize);
    	ranges = downloadableMetadata.getMissingRanges();
    
        for (Range mainRange: ranges) {
            rangeChunkSize = (int) Math.ceil(((double)mainRange.getLength() / numberOfWorkers));
            startRange = mainRange.getStart();
            endRange = startRange + rangeChunkSize;

            for (int i = 0; i < numberOfWorkers; i++) {
                Utilities.Log(MODULE_NAME, "Starting a HTTPRangeGetter thread with ranges:");
                Utilities.Log(MODULE_NAME, "startRange: " + startRange);
                Utilities.Log(MODULE_NAME, "endRange: " + endRange);

                Range range = new Range(startRange, endRange);
                HTTPRangeGetter httpRangeGetter = new HTTPRangeGetter(url, range, chunkQueue, tokenBucket);

                startRange = endRange + 1;
                if( i == numberOfWorkers - 2){
                    endRange = mainRange.getEnd();
                }
                else{
                    endRange += rangeChunkSize;
                }
                Utilities.Log(MODULE_NAME, "Executing a HTTPRangeGetter thread with ranges:");
                httpRangeGetterTPExecutor.execute(httpRangeGetter);
            }

        }
        
        return httpRangeGetterTPExecutor;
    }

    private static void printStatusAndDeleteMeta(DownloadableMetadata downloadableMetadata) 
    {
            downloadableMetadata.delete();
            System.out.println("Download success!");        
    }

    private static void joinThreads(
            BlockingQueue<Chunk> chunkQueue,
            Thread fileWriterThread,
            TokenBucket tokenBucket,
            Thread rateLimiterThread,
            ExecutorService httpRangeGetterTPExecutor) {
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
        } catch (Exception e) {
            //e.printStackTrace();
        } finally {
            httpConnection.disconnect();
        }
        return -1;
    }
}
