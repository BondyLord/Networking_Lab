import java.io.File;
import java.util.List;

/**
 * Describes a file's metadata: URL, file name, size, and which parts already downloaded to disk.
 *
 * The metadata (or at least which parts already downloaded to disk) is constantly stored safely in disk.
 * When constructing a new metadata object, we first check the disk to load existing metadata.
 *
 * CHALLENGE: try to avoid metadata disk footprint of O(n) in the average case
 * HINT: avoid the obvious bitmap solution, and think about ranges...
 */
class DownloadableMetadata {
    private final String metadataFilename;
    private String filename;
    private String url;
    private int size;
    //<TODO decide about a data structure for the ranges>
    private List<Range> ranges;

    DownloadableMetadata(String url) {
        this.url = url;
        this.filename = getName(url);
        this.metadataFilename = getMetadataName(filename);
        //TODO
    }

    private static String getMetadataName(String filename) {
        return filename + ".metadata";
    }

    private static String getName(String path) {
        return path.substring(path.lastIndexOf('/') + 1, path.length());
    }

    void addRange(Range range) {
        //TODO
    }

    String getFilename() {
        return filename;
    }

    boolean isCompleted() {
        //TODO
        if (this.size == IdcDm.getFileSize(url)){
            return true;
        }
        return false;

    }

    void delete() {
        //TODO
        try{

            File file = new File(metadataFilename);

            if(file.delete()){
                System.out.println(file.getName() + " is deleted!");
            }else{
                System.out.println("Delete operation is failed.");
            }

        }catch(Exception e){

            e.printStackTrace();

        }
    }

    Range getMissingRange() {
        //TODO
        return null;
    }

    String getUrl() {
        return url;
    }
}
