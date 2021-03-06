
import Utill.Utilities;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.BlockingQueue;

/**
 * A runnable class which downloads a given url.
 * It reads CHUNK_SIZE at a time and writs it into a BlockingQueue.
 * It supports downloading a range of data, and limiting the download rate using a token bucket.
 */
public class HTTPRangeGetter implements Runnable {
    private static final String MODULE_NAME = "HTTPRangeGetter";
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

    // Download the given worker range
    private void downloadRange() throws IOException, InterruptedException {
        String rangRequestProperty;
        long startRange = this.range.getStart();
        long endRange = this.range.getEnd();

        // Open the url connection
        URL url = new URL(this.url);
        HttpURLConnection httpConnection = (HttpURLConnection) url.openConnection();

        // Set HTTP headers
        httpConnection.setRequestMethod("GET");
        httpConnection.setReadTimeout(READ_TIMEOUT);
        httpConnection.setConnectTimeout(CONNECT_TIMEOUT);

        // Set the range property
        rangRequestProperty = String.format("bytes=%d-%d", startRange, endRange);
        httpConnection.setRequestProperty("Range", rangRequestProperty);
        Utilities.Log(MODULE_NAME, "range request - " + rangRequestProperty);
        httpConnection.connect();

        // Download the data in the given range
        downloadData(httpConnection, startRange);
    }

    private void downloadData(
            HttpURLConnection httpConnection,
            long offset)
            throws
            IOException,
            InterruptedException {

        int resCode;
        int dataSize;
        InputStream in = null;

        try {
            // Check tokens availability before opening a network connection
            tokenBucket.take(CHUNK_SIZE);

            // Get the request response code
            resCode = httpConnection.getResponseCode();
            Utilities.Log(MODULE_NAME, "Response code - " + resCode);

            // Check the http response code(200 or 206)
            if (resCode == HttpURLConnection.HTTP_OK || resCode == HttpURLConnection.HTTP_PARTIAL) {

                byte[] data = new byte[CHUNK_SIZE];
                Utilities.Log(MODULE_NAME, "getting data from request");

                in = httpConnection.getInputStream();

                // Loop over the response data
                while ((dataSize = in.read(data)) != -1) {
                    tokenBucket.take(dataSize); // Token availability
                    Chunk chunk = new Chunk(data, offset, dataSize); // A chunk of data read
                    outQueue.put(chunk); // Put the data in the queue
                    offset += dataSize; // Change the next data offset
                }
            } else{
                System.err.println("Unable to download data, Response code from server was - " + resCode);
            }

        } catch (Exception e) {
            System.err.println("There was an exception during reading data from stream: " + e.getMessage());
            throw (e);
        } finally {
            if (in != null) {
                in.close();
            }

            httpConnection.disconnect();
        }
    }

    @Override
    public void run() {
        try {
            this.downloadRange();
        } catch (IOException | InterruptedException e) {
            System.err.println("There was an exception while getting data from the network: " + e.getMessage());
        }
    }
}
