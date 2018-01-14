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

    void addRange(Range range) {
        int unionResponseCode;
        Range currentRange;
        m_sizeInBytes += range.getLength();

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
                case -1:
                    if (i == m_downLoadedRanges.size() - 1) {
                        m_downLoadedRanges.add(range);
                        return;
                    }
                    break;
                // current Range is bigger than range
                case 1:
                    m_downLoadedRanges.add(i, range);
                    return;
                case 0:
                    // update the range to a new range
                    m_downLoadedRanges.add(i, unionResponse.get_updatedRange());
                    m_downLoadedRanges.remove(i + 1);
                    return;
            }
        }
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
        int numberOfMissingRanges;
        if(m_updateFlag.get() || m_missingRanges == null){
            numberOfMissingRanges = getMissingRanges().size();
        }else{
            numberOfMissingRanges = m_missingRanges.size();
        }

        return numberOfMissingRanges == 0;
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
            e.printStackTrace();
        }
    }

    ArrayList<Range> getMissingRanges() {

        ArrayList<Range> ranges;

        // if no metadata exist we initiate the one first range as the full file size
        if (m_downLoadedRanges.size() == 0) {
            ranges = new ArrayList<>();
            ranges.add(new Range(0L, IdcDm.fileSize));
            m_missingRanges = ranges;
        }else if(!m_updateFlag.get()){
            System.out.println("Computing Missing Ranges");
            long fileSize = IdcDm.fileSize;
            m_missingRanges = new ArrayList<>();
            Range currentMissingRange;
            long lastDownloadedByte;
            int index;
            // finding missing ranges
            for (index = 1; index < m_downLoadedRanges.size(); index++) {
                if (m_downLoadedRanges.get(index - 1).getEnd() < m_downLoadedRanges.get(index).getStart()) {
                    currentMissingRange = new Range(m_downLoadedRanges.get(index - 1).getEnd(),
                            m_downLoadedRanges.get(index).getStart());
                    m_missingRanges.add(currentMissingRange);
                }
            }
            // making sure we have all missing ranges
            lastDownloadedByte = m_downLoadedRanges.get(index - 1).getEnd();
            if (lastDownloadedByte != fileSize) {
                currentMissingRange = new Range(lastDownloadedByte,
                        fileSize);
                m_missingRanges.add(currentMissingRange);
            }
        }
        m_updateFlag.set(false);
        return m_missingRanges;
    }
}
