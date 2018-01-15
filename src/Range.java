import java.io.Serializable;

/**
 * Describes a simple range, with a start, an end, and a length
 */
class Range implements Serializable {
    private static final long serialVersionUID = 1L;
    private long start;
    private long end;

    Range(Long start, Long end) {
        this.start = start;
        this.end = end;
    }

    long getStart() {
        return start;
    }

    long getEnd() {
        return end;
    }

    static UnionResponse unionRanges(Range i_rangeOne, Range i_rangeTwo) {
        UnionResponse unionResponse;
        Range unitedRange = null;
        int resCode = 0;
        long rangeOneOffset = i_rangeOne.getStart();
        long rangeOneEndIndex = i_rangeOne.getEnd();
        long rangeTwoOffset = i_rangeTwo.getStart();
        long rangeTwoEndIndex = i_rangeTwo.getEnd();

        // Get the distance between start point of the two ranges
        long rangeOffsetDifferences = rangeOneOffset - rangeTwoOffset;
        long rangeEndDifferences =  rangeOneEndIndex - rangeTwoEndIndex;
        // uniting ranges

        if (rangeOffsetDifferences < 0) {
            if( rangeTwoOffset  - rangeOneEndIndex > 1){
                // range two is bigger than range one
                resCode = 1;
            }
            else if ( rangeEndDifferences >= 0 ) {
                // range two is inside range one
                unitedRange = i_rangeOne;
            } else{
                // range one and two overlap
                // |------ range one ----|
                //        |------- range two ------|
                unitedRange = new Range(rangeOneOffset, rangeTwoEndIndex);
            }
        } else {
            if (rangeOneOffset - rangeTwoEndIndex > 1) {
                //range one is bigger than range two
                resCode = -1;
            }
            else if (rangeEndDifferences <= 0 ){
                // range one is inside range two
                unitedRange = i_rangeTwo;
            }else{
                // range two and one overlap
                // |------ range two ----|
                //        |------- range one ------|
                unitedRange = new Range(rangeTwoOffset, rangeOneEndIndex);

            }
        }
        unionResponse = new UnionResponse(unitedRange, resCode);
        return unionResponse;
    }

    long getLength() {
        return (end - start);
    }

    static class UnionResponse implements Serializable {
        private static final long serialVersionUID = 1L;
        private int m_resCode;
        private Range m_updatedRange;

        UnionResponse(Range i_updatedRange, int i_res) {
            this.m_resCode = i_res;
            this.m_updatedRange = i_updatedRange;
        }

        int get_res() {
            return m_resCode;
        }

        Range get_updatedRange() {
            return m_updatedRange;
        }
    }

}
