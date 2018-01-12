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
        FileOutputStream metadataFileOut = new FileOutputStream(metadataFilename);

        try {
            System.out.println("entering while loop");

            int fileSize = IdcDm.fileSize;

//            int progressPercentage = downloadableMetadata.getPrecentageSoFar();
            double progressPercentage = 0;

            while (true) {
                Chunk chunk = chunkQueue.take();
                if (chunk.getOffset() == -1) {
                    System.out.println("Exiting FileWriter thread, " +
                            "end of data reached.");
                    break;
                }


                byte[] byteArray = chunk.getData();

                randomAccessFile.seek(chunk.getOffset());
                int chunkSize = chunk.getSize_in_bytes();
                for (int i = 0; i < chunkSize; i++) {
                    randomAccessFile.write(byteArray[i]);
                }

                progressPercentage = getUpdatedProgress(progressPercentage, chunkSize, fileSize);

                Range range = new Range(chunk.getOffset(), (long) chunkSize);

                downloadableMetadata.addRange(range);
                //<TODO write to metaData>
//                outMetadataFile.write(downloadableMetadata.);
            }

            metadataFileOut.close();
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

    private double getUpdatedProgress(double progressPercentage, int chunkSize, int fileSize) {
        progressPercentage += ((double)chunkSize / fileSize) * 100;

        System.out.println("Downloaded " + Math.floor(progressPercentage));
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
