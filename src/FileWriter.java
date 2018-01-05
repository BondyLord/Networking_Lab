import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.BlockingQueue;

/**
 * This class takes chunks from the queue, writes them to disk and updates the file's metadata.
 * <p>
 * NOTE: make sure that the file interface you choose writes every update to the file's content or metadata
 * synchronously to the underlying storage device.
 */
public class FileWriter implements Runnable {

    //<TODO give a real name for file>
    private static final String FILE_NAME = "file name";
    private final BlockingQueue<Chunk> chunkQueue;
    private DownloadableMetadata downloadableMetadata;

    FileWriter(DownloadableMetadata downloadableMetadata, BlockingQueue<Chunk> chunkQueue) {
        this.chunkQueue = chunkQueue;
        this.downloadableMetadata = downloadableMetadata;
    }

    private void writeChunks() throws IOException {
        //TODO
        FileOutputStream out = new FileOutputStream(FILE_NAME);
        try {
            synchronized (chunkQueue) {
                while (!chunkQueue.isEmpty()) {
                    Chunk chunk = chunkQueue.take();
                    if (chunk.getOffset()== -1) {
                        System.out.println("Exiting FileWriter thread, " +
                                "end of data reached.");
                        break;
                    }
                    out.write(chunk.getData());
                    //<TODO write to metaData>
                }
                //<TODO choose sleep time>
                Thread.sleep(100);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        try {
            this.writeChunks();
        } catch (IOException e) {
            e.printStackTrace();
            //TODO
        }
    }
}
