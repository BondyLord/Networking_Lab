import Utill.Utilities;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Describes a file's metadata: URL, file name, size, and which parts already downloaded to disk.
 *
 * The metadata (or at least which parts already downloaded to disk) is constantly stored safely in disk.
 * When constructing a new metadata object, we first check the disk to load existing metadata.
 *
 * CHALLENGE: try to avoid metadata disk footprint of O(n) in the average case
 * HINT: avoid the obvious bitmap solution, and think about ranges...
 */
class DownloadableMetadata implements Serializable {
	private static final long serialVersionUID = 1L;
    private final String metadataFilename;
    private static final String MODULE_NAME="DownloadableMetadata";
    private String filename;
    private ArrayList<Range> m_downLoadedRanges;
    private ArrayList<Range> m_missingRanges;
    private long m_sizeInBytes;
    private AtomicBoolean m_updateFlag;

    DownloadableMetadata(String url) {

        this.filename = getName(url);
        this.metadataFilename = getMetadataName(filename);
        this.m_downLoadedRanges = new ArrayList<>();
        this.m_missingRanges = null;
        this.m_sizeInBytes = 0;
        this.m_updateFlag = new AtomicBoolean(false);
    }

    private static String getMetadataName(String filename) {
        return filename + ".metadata";
    }

    private static String getName(String path) {
        return path.substring(path.lastIndexOf('/') + 1, path.length());
    }

    private void addRangeToList(Range range){
        int unionResponseCode;
        Range currentRange;

        // adding first range
        if (m_downLoadedRanges.size() == 0) {
            m_downLoadedRanges.add(range);
            return;
        }

        for (int i = 0; i < m_downLoadedRanges.size(); i++) {
            currentRange = m_downLoadedRanges.get(i);
            Range.UnionResponse unionResponse = Range.unionRanges(currentRange, range);
            unionResponseCode = unionResponse.get_res();

            switch (unionResponseCode) {
                // range is bigger than current
                case 1:
                    // last index of ranges. add range as last range
                    if (i == m_downLoadedRanges.size() - 1) {
                        m_downLoadedRanges.add(range);
                        return;
                    }
                    break;
                // current Range is bigger than range
                case -1:
                    m_downLoadedRanges.add(i, range);
                    return;
                case 0:
                    // remove the old range
                    m_downLoadedRanges.remove(i);
                    // add the new range
                    addRangeToList(unionResponse.get_updatedRange());
                    return;
            }
        }
    }

    void addRange(Range range) {
        m_sizeInBytes += range.getLength();
        addRangeToList(range);
    }

    long get_sizeInBytes() {
        return m_sizeInBytes;
    }

    String getFilename() {
        return filename;
    }

    String getMetadataFilename() {
        return metadataFilename;
    }

    boolean isCompleted() {
        // Return true without computing missing ranges in the case
        // Missing Ranges was already computed.
        long total = 0;
        for (Range range:
                m_downLoadedRanges
             ) {
            total += range.getLength();
        }
        return total == IdcDm.fileSize;
    }

    void delete() {
        try {
            File metadataFile = new File(metadataFilename);
            if (metadataFile.delete()) {
                Utilities.Log(MODULE_NAME,metadataFile.getName() + " is deleted!");
            } else {
                Utilities.Log(MODULE_NAME,"Delete operation has failed.");
            }

        } catch (Exception e) {
        	Utilities.Log(MODULE_NAME,"There was an error while deleting metadata file " + e.getMessage());
        }
    }

    ArrayList<Range> getMissingRanges() {

        ArrayList<Range> ranges;
        long fileSize = IdcDm.fileSize;
        long indexOfLastByteInTheFile = fileSize;
        // if no metadata exist we initiate the one first range as the full file size
        if (m_downLoadedRanges.size() == 0) {
            ranges = new ArrayList<>();
            ranges.add(new Range(0L, indexOfLastByteInTheFile));
            m_missingRanges = ranges;
        }
        else if(!m_updateFlag.get()){
            Utilities.Log(MODULE_NAME,"Computing Missing Ranges");
            m_missingRanges = new ArrayList<>();
            Range currentMissingRange;
            long lastDownloadedByte;
            int index;
            
            // find missing ranges
            for (index = 1; index < m_downLoadedRanges.size(); index++) {
                // holding both the range in the current index and the one before it
                // take into count that the range array list is sorted
                if (m_downLoadedRanges.get(index).getStart() - m_downLoadedRanges.get(index - 1).getEnd() > 1) {
                    currentMissingRange = new Range(
                            m_downLoadedRanges.get(index - 1).getEnd(),
                            m_downLoadedRanges.get(index).getStart());
                    m_missingRanges.add(currentMissingRange);
                }
            }

            // get last missing range
            lastDownloadedByte = m_downLoadedRanges.get(index - 1).getEnd();
            if (lastDownloadedByte != indexOfLastByteInTheFile) {
                currentMissingRange = new Range(
                        lastDownloadedByte,
                        indexOfLastByteInTheFile);
                m_missingRanges.add(currentMissingRange);
            }
        }
        m_updateFlag.set(false);
        return m_missingRanges;
    }
}
