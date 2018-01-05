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

    DownloadableMetadata(String url) {
        this.url = url;
        this.filename = getName(url);
        this.metadataFilename = getMetadataName(filename);
        //TODO
    }}

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
    }

    void delete() {
        //TODO
    }

    Range getMissingRange() {
        //TODO
    }

    String getUrl() {
        return url;
    }
}
