import Utill.Utilities;

import java.io.*;
import java.util.concurrent.BlockingQueue;

/**
 * This class takes chunks from the queue, writes them to disk and updates the file's metadata.
 * <p>
 * NOTE: make sure that the file interface you choose writes every update to the file's content or metadata
 * synchronously to the underlying storage device.
 */
public class FileWriter implements Runnable {

    static final String MODULE_NAME = "FileWriter";

    private final BlockingQueue<Chunk> chunkQueue;
    private DownloadableMetadata downloadableMetadata;

    FileWriter(DownloadableMetadata downloadableMetadata, BlockingQueue<Chunk> chunkQueue) {
        this.chunkQueue = chunkQueue;
        this.downloadableMetadata = downloadableMetadata;
    }

    private void writeChunks() throws IOException {
        //TODO

        String tempFileName = downloadableMetadata.getFilename() + ".tmp";
        File tempFile = new File(tempFileName);
        if (!tempFile.exists()){
            tempFile.createNewFile();
        }
        Utilities.Log(MODULE_NAME, "open FileOutputStream for Writing to file: " + tempFileName);
        RandomAccessFile randomAccessFile = new RandomAccessFile(tempFile.getPath(), "rw");

        String metadataFilename = downloadableMetadata.getMetadataFilename();
        Utilities.Log(MODULE_NAME, "open FileOutputStream for Writing to file: " + metadataFilename);

        try {
            System.out.println("entering while loop");

            long fileSize = IdcDm.fileSize;

            double progressPercentage = (int) (((double)downloadableMetadata.get_sizeInBytes() / fileSize) * 100);

            while (true) {
                Chunk chunk = chunkQueue.take();
                if (chunk.getOffset() == -1) {
                    System.out.println("Exiting FileWriter thread, " +
                            "end of data reached.");
                    break;
                }

                byte[] byteArray = chunk.getData();
                randomAccessFile.seek(chunk.getOffset());
                long chunkSize = chunk.getSize_in_bytes();
                for (int i = 0; i < chunkSize; i++) {
                    randomAccessFile.write(byteArray[i]);
                }

                progressPercentage = getUpdatedProgress(progressPercentage, chunkSize, fileSize);
                Range range = new Range(chunk.getOffset(), chunk.getOffset() + chunkSize);
                downloadableMetadata.addRange(range);
                Utilities.Log(MODULE_NAME, "Range: " +range.getStart() + ":" + range.getEnd() + " was added");
                FileOutputStream metadataFileOut = new FileOutputStream(metadataFilename);
                ObjectOutput metadataObjectOut = new ObjectOutputStream(metadataFileOut);
                metadataObjectOut.writeObject(downloadableMetadata);
                //<TODO think of a better solution>
                metadataObjectOut.close();
                metadataFileOut.close();
            }
            randomAccessFile.close();

            // rename
            String fileName = downloadableMetadata.getFilename();
            File file = new File(fileName);
            if (tempFile.renameTo(file)) {
                System.out.println("File: " + tempFile.getName() + " renamed to: " + file.getName());
            } else {
                System.out.println("Sorry! the file: " + tempFile.getName() + " can't be renamed");
            }

            //<TODO choose sleep time>
            Thread.sleep(100);
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }

    private double getUpdatedProgress(double progressPercentage, long chunkSize, long fileSize) {
        double progressPercentageBefore = progressPercentage;
        progressPercentage += ((double) chunkSize / fileSize) * 100;
        int progressPercentageAfter = (int) progressPercentage;
        if ((Math.floor(progressPercentageBefore) != progressPercentageAfter) || progressPercentageBefore == 0.0) {
            System.out.println("Downloaded " + progressPercentageAfter + "%");
        }
        return progressPercentage;
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
