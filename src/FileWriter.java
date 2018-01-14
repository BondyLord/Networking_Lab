import Utill.Utilities;

import java.io.*;
import java.nio.file.Files;
import java.util.concurrent.BlockingQueue;

/**
 * This class takes chunks from the queue, writes them to disk and updates the file's metadata.
 * <p>
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
            Utilities.Log(MODULE_NAME,"Temp file exists... ");
        }
        Utilities.Log(MODULE_NAME, "open FileOutputStream for Writing to file: " + tempFileName);
        RandomAccessFile randomAccessFile = new RandomAccessFile(tempFile.getPath(), "rw");

        String metadataFilename = downloadableMetadata.getMetadataFilename();
        Utilities.Log(MODULE_NAME, "open FileOutputStream for Writing to file: " + metadataFilename);
        long fileSize = IdcDm.fileSize;
        double progressPercentage = (int) (((double) downloadableMetadata.get_sizeInBytes() / fileSize) * 100);
        try {
            while (true) {
                Chunk chunk = chunkQueue.take();
                // stopping while at end of data
                if (chunk.getOffset() == -1) {
                    Utilities.Log(MODULE_NAME, "Exiting FileWriter thread, " +
                            "end of data reached.");
                    break;
                }

                long chunkSize = chunk.getSize_in_bytes();
                writeDataToFile(randomAccessFile, chunk, chunkSize);
                progressPercentage = getUpdatedProgress(progressPercentage, chunkSize, fileSize);
                addDownloadedRange(chunk, chunkSize);
                //Utilities.Log(MODULE_NAME, "Range: " +range.getStart() + ":" + range.getEnd() + " was added");
                updateMetadata(metadataFilename);
            }
            Thread.sleep(500);
            randomAccessFile.close();
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }

    private void updateMetadata(String metadataFilename) throws IOException {
    	FileOutputStream metadataFileOut = null;
    	ObjectOutput metadataObjectOut = null;
    	
    	try{
            metadataFileOut = new FileOutputStream(metadataFilename + ".tmp");
            metadataObjectOut = new ObjectOutputStream(metadataFileOut);
            metadataObjectOut.writeObject(downloadableMetadata);
            metadataObjectOut.close();
            metadataFileOut.close();
            // to handle corrupted temp file - renaming metadata.tmp file to metadata after writing
            renameTmp(metadataFilename);
    		
    	}catch (Exception e) {
			if(metadataObjectOut != null)
				metadataObjectOut.close();
			if(metadataFileOut != null)
				metadataFileOut.close();
				
		}

    }

    private void addDownloadedRange(Chunk chunk, long chunkSize) {
        Range range = new Range(chunk.getOffset(), chunk.getOffset() + chunkSize);
        downloadableMetadata.addRange(range);
    }

    private void writeDataToFile(RandomAccessFile randomAccessFile, Chunk chunk, long chunkSize) throws IOException {
        byte[] byteArray = chunk.getData();
        randomAccessFile.seek(chunk.getOffset());
        for (int i = 0; i < chunkSize; i++) {
            randomAccessFile.write(byteArray[i]);
        }
    }

    static void renameTmp(String fileName) {
        File tmpFile = new File(fileName + ".tmp");
        if (tmpFile.exists()) {
            File file = new File(fileName);
            try {
                Files.move(tmpFile.toPath(), file.toPath(), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                Utilities.Log(MODULE_NAME, "File: " + tmpFile + " renamed to: " + file.getName());
            } catch (IOException ex) {
                Utilities.Log(MODULE_NAME, "Sorry! the file: " + tmpFile + " can't be renamed");
            }
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
        }
    }
}
