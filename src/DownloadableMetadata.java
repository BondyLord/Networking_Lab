import com.sun.xml.internal.bind.v2.model.core.ID;
import javafx.scene.control.RadioMenuItem;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
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
class DownloadableMetadata implements Serializable {

    private final String metadataFilename;
    private String filename;
    private String url;
    private ArrayList<Range> m_downLoadedRanges;
    private ArrayList<Range> m_missingRanges;
    private long m_sizeInBytes;

    DownloadableMetadata(String url) {

        this.filename = getName(url);
        this.metadataFilename = getMetadataName(filename);
        this.m_downLoadedRanges = new ArrayList<Range>();
        this.m_missingRanges = null;
        this.m_sizeInBytes = 0;
    }

    private static String getMetadataName(String filename) {
        return filename + ".metadata";
    }

    private static String getName(String path) {
        return path.substring(path.lastIndexOf('/') + 1, path.length());
    }

    protected void addRange(Range range) {
        int resCode;
        Range currentRange;
        m_sizeInBytes += range.getLength();

        if(m_downLoadedRanges.size() == 0){
            m_downLoadedRanges.add(range);
            return;
        }

        for(int i=0; i < m_downLoadedRanges.size(); i++){

            currentRange = m_downLoadedRanges.get(i);
            Range.UnionResponse res = Range.unionRanges(currentRange, range);
            resCode = res.get_res();

            switch (resCode){
                // range is bigger than current
                case -1:
                    break;
                // current Range is bigger than range
                case 1:
                    m_downLoadedRanges.add(i, range);
                    return;
                case 0:
                    // update the range to a new range
                    m_downLoadedRanges.add(i,res.get_updatedRange());
                    m_downLoadedRanges.remove(i+1);
                    return;
            }
        }
    }

    protected long get_sizeInBytes(){
        return m_sizeInBytes;
    }

    protected String getFilename() {
        return filename;
    }

    protected String getMetadataFilename() {
        return metadataFilename;
    }

    protected boolean isCompleted() {
        if (IdcDm.fileSize == m_sizeInBytes){
            return true;
        }
        return false;
    }

    protected void delete() {
        try{
            File metadataFile = new File(metadataFilename);

            if(metadataFile.delete()){
                System.out.println(metadataFile.getName() + " is deleted!");
            }else{
                System.out.println("Delete operation has failed.");
            }

        }catch(Exception e){
            e.printStackTrace();
        }
    }

    protected ArrayList<Range> getMissingRanges() {
        if(m_missingRanges != null){
            return m_missingRanges;
        }

        long fileSize = IdcDm.fileSize;
        m_missingRanges = new ArrayList<Range>();
        Range currentMissingRange;
        long lastDownloadedByte;
        int index;
        for( index= 1; index < m_downLoadedRanges.size(); index++){
            currentMissingRange = new Range(m_downLoadedRanges.get(index-1).getEnd(),
                                            m_downLoadedRanges.get(index).getStart());
            m_missingRanges.add(currentMissingRange);
        }
        lastDownloadedByte = m_downLoadedRanges.get(index-1).getEnd();
        if( lastDownloadedByte != fileSize){
            currentMissingRange = new Range(lastDownloadedByte,
                                            fileSize);
        }
        return getMissingRanges();
    }

    String getUrl() {
        return url;
    }
}
