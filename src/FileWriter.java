import Utill.Utilities;

import java.io.*;
import java.nio.file.Files;
import java.util.concurrent.BlockingQueue;

/**
 * This class takes chunks from the queue, writes them to disk and updates the file's metadata.
 *
 * NOTE: make sure that the file interface you choose writes every update to the file's content or metadata
 * synchronously to the underlying storage device.
 */
public class FileWriter implements Runnable {

    private static final String MODULE_NAME = "FileWriter";

    private final BlockingQueue<Chunk> chunkQueue;
    private DownloadableMetadata downloadableMetadata;


    FileWriter(DownloadableMetadata downloadableMetadata, BlockingQueue<Chunk> chunkQueue) {
        this.chunkQueue = chunkQueue;
        this.downloadableMetadata = downloadableMetadata;
    }

    private void writeChunks() throws IOException {
        // create tempFile
        String tempFileName = downloadableMetadata.getFilename() + ".tmp";
        File tempFile = new File(tempFileName);

        if (!tempFile.createNewFile()) {
            Utilities.Log(MODULE_NAME, "Temp file exists... ");
        }

        RandomAccessFile randomAccessFile = new RandomAccessFile(tempFile.getPath(), "rw");
        Utilities.Log(MODULE_NAME, "open RandomAccessFile for Writing to file: " + tempFileName);

        String metadataFilename = downloadableMetadata.getMetadataFilename();
        Utilities.Log(MODULE_NAME, "open FileOutputStream for Writing to file: " + metadataFilename);

        // using downloadableMetadata object to get current downloaded percentage
        long fileSize = IdcDm.fileSize;
        double progressPercentage = (int) (((double) downloadableMetadata.get_sizeInBytes() / fileSize) * 100);
        try {
            // Write all available data till the end(represented by offset -1)
            while (true) {
                Chunk chunk = chunkQueue.take();

                // stopping while at end of data
                if (chunk.getOffset() == -1) {
                    Utilities.Log(MODULE_NAME, "Exiting FileWriter thread, " +
                            "end of data reached.");
                    break;
                }

                long chunkSize = chunk.getSize_in_bytes();
                writeDataToFile(randomAccessFile, chunk, chunkSize); // Write data to file
                progressPercentage = getUpdatedProgress(progressPercentage, chunkSize, fileSize); // Show progress
                addDownloadedRange(chunk, chunkSize); // Add downloaded range in metadata object
                updateMetadata(metadataFilename); // Update downloaded range in metadata file
            }

            Thread.sleep(500);
            randomAccessFile.close();
        } catch (InterruptedException | IOException e) {
            System.err.println( "There was an exception while writing chunk data " + e.getMessage());
        }
    }

    // Update downloaded range in the metadata file
    private void updateMetadata(String metadataFilename) throws IOException {
        FileOutputStream metadataFileOut = null;
        ObjectOutput metadataObjectOut = null;

        try {
            metadataFileOut = new FileOutputStream(metadataFilename + ".tmp");
            metadataObjectOut = new ObjectOutputStream(metadataFileOut);
            metadataObjectOut.writeObject(downloadableMetadata);
            metadataObjectOut.close();

            metadataFileOut.close();

            // to handle corrupted temp file - renaming metadata.tmp file to metadata after writing
            renameTmp(metadataFilename);

        } catch (Exception e) {
            if (metadataObjectOut != null)
                metadataObjectOut.close();
            if (metadataFileOut != null)
                metadataFileOut.close();
        }
    }

    // Add downloaded range in metadata object
    private void addDownloadedRange(Chunk chunk, long chunkSize) {
        Range range = new Range(chunk.getOffset(), chunk.getOffset() + chunkSize);
        downloadableMetadata.addRange(range);
    }

    // Write the given data into the output downloaded file
    private void writeDataToFile(RandomAccessFile randomAccessFile, Chunk chunk, long chunkSize) throws IOException {
        long chunkOffset = chunk.getOffset();
        long chunkEndSet = chunkOffset + chunkSize;
        byte[] byteArray = chunk.getData();

        randomAccessFile.seek(chunkOffset); // Seek the right data offset
        Utilities.Log(MODULE_NAME, "Writing chunk to file - chunk range - "
                + chunkOffset + " - " + chunkEndSet);

        // Write the given chunk data
        for (int i = 0; i < chunkSize; i++) {
            randomAccessFile.write(byteArray[i]);
        }
    }

    // File rename operation
    static void renameTmp(String fileName) {
        File tmpFile = new File(fileName + ".tmp");
        if (tmpFile.exists()) {
            File file = new File(fileName);
            try {
                Files.move(tmpFile.toPath(), file.toPath(), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException ex) {
                System.err.println( "Sorry! the file: " + tmpFile + " can't be renamed");
            }
        }
    }

    // Update and write the download progress percentage
    private double getUpdatedProgress(double progressPercentage, long chunkSize, long fileSize) {
        double progressPercentageBefore = progressPercentage;
        progressPercentage += ((double) chunkSize / fileSize) * 100;
        int progressPercentageAfter = (int) progressPercentage;

        // Check for a percentage change and update the user accordingly
        if ((Math.floor(progressPercentageBefore) != progressPercentageAfter) || progressPercentageBefore == 0.0) {
            System.err.println("Downloaded " + progressPercentageAfter + "%");
        }

        return progressPercentage;
    }

    @Override
    public void run() {
        try {
            this.writeChunks();
        } catch (IOException e) {
            System.err.println( "FileWriter error " + e.getMessage());
        }
    }
}
