

import Utill.Utilities;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.BlockingQueue;

/**
 * A runnable class which downloads a given url.
 * It reads CHUNK_SIZE at a time and writs it into a BlockingQueue.
 * It supports downloading a range of data, and limiting the download rate using a token bucket.
 */
public class HTTPRangeGetter implements Runnable {

    static final String MODULE_NAME = "HTTPRangeGetter";
    static final int CHUNK_SIZE = 4096;
    private static final int CONNECT_TIMEOUT = 500;
    private static final int READ_TIMEOUT = 2000;
    private final String url;
    private final Range range;
    private final BlockingQueue<Chunk> outQueue;
    private TokenBucket tokenBucket;

    HTTPRangeGetter(
            String url,
            Range range,
            BlockingQueue<Chunk> outQueue,
            TokenBucket tokenBucket) {
        this.url = url;
        this.range = range;
        this.outQueue = outQueue;
        this.tokenBucket = tokenBucket;
    }


    private void downloadRange() throws IOException, InterruptedException {

        long startRange = this.range.getStart();
        long endRange;
        int downloadResponse;
        long numberOfNeededChunks= (long) Math.ceil((double)range.getLength()/ CHUNK_SIZE);
        URL url = new URL(this.url);
        HttpURLConnection httpConnection = (HttpURLConnection) url.openConnection();
        httpConnection.setRequestMethod("GET");
        httpConnection.setReadTimeout(READ_TIMEOUT);
        httpConnection.setConnectTimeout(CONNECT_TIMEOUT);

        Utilities.Log(MODULE_NAME,"start downloading in "
                + numberOfNeededChunks
                + "iterations");

        for( int i = 0; i < numberOfNeededChunks; i++){
            startRange += (i * CHUNK_SIZE);
            endRange = startRange + CHUNK_SIZE - 1;
            downloadResponse = downloadData(startRange, endRange, httpConnection);
            if (downloadResponse == 0 ){
                // In the case response code is not 200 (OK)
                return;
            }
        }
    }

    private int downloadData(long startRange, long endRange, HttpURLConnection httpConnection) throws IOException {
        String rangRequestProperty;
        int resCode;
        rangRequestProperty = String.format("bytes=%d-%d", startRange, endRange);
        httpConnection.setRequestProperty("Range", rangRequestProperty);

        Utilities.Log(MODULE_NAME,"range request - " + rangRequestProperty);
        resCode = httpConnection.getResponseCode();
        Utilities.Log(MODULE_NAME,"Response code - " +  resCode);

        if ( resCode == HttpURLConnection.HTTP_OK){
            byte[] data = new byte[CHUNK_SIZE];
            Utilities.Log(MODULE_NAME,"getting data from request");
            httpConnection.getInputStream().read(data);
            Chunk chunk = new Chunk(data, startRange, CHUNK_SIZE);
            outQueue.add(chunk);
            return 1;
        }else{
            Utilities.Log(MODULE_NAME,"houston we have a problem");
            Utilities.Log(MODULE_NAME,"terminating....");
            return 0;
        }
    }

    @Override
    public void run() {
        try {
            this.downloadRange();
        } catch (IOException | InterruptedException e) {
            Utilities.ErrorLog(MODULE_NAME, e.getMessage());
            //TODO
        }
    }
}
