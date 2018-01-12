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
        Utilities.Log(MODULE_NAME,"range request - " + rangRequestProperty);
        
        // Download the data in the given range
        downloadData(httpConnection, startRange);
    }

    private int downloadData(HttpURLConnection httpConnection, long offset) throws IOException {
        
        int resCode = 0;
        int dataSize = 0;
        InputStream in = null;
        
        try{ 
        	// Get the request response code
            resCode = httpConnection.getResponseCode();
            Utilities.Log(MODULE_NAME,"Response code - " +  resCode);
            
            // Check the http response code(200 or 206)
            if (resCode == HttpURLConnection.HTTP_OK || resCode == HttpURLConnection.HTTP_PARTIAL){

                byte[] data = new byte[CHUNK_SIZE];
                Utilities.Log(MODULE_NAME,"getting data from request");

                in = httpConnection.getInputStream();
                
                // Loop over the response data
                while((dataSize = in.read(data))!= -1)
                {
                	System.out.println("Trying to take tokens in size - " + dataSize);
                	tokenBucket.take(dataSize);
                	System.out.println("After taking");
                    Chunk chunk = new Chunk(data, offset, dataSize); // A chunk of data read
                    outQueue.put(chunk); // Put the data in the queue
                    offset += dataSize; // Change the next data offset
                }
            }
            
            
            return 1;
            
        } catch (Exception e) {
        	Utilities.Log(MODULE_NAME,"There was an exception during reading data from stream - " + e.getMessage());
        	return 0;
		} finally {
			in.close();
			httpConnection.disconnect();
		}
    }

    @Override
    public void run() {
        try {
            this.downloadRange();
        } catch (IOException | InterruptedException e) {
            Utilities.ErrorLog(MODULE_NAME, e.getMessage());
        }
    }
}
