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
        Long rangeDifferences = i_rangeOne.getStart() - i_rangeTwo.getStart();
        // uniting ranges
        if (rangeDifferences < 0) {
            if (i_rangeOne.getEnd() >= i_rangeTwo.getStart()) {
                unitedRange = new Range(i_rangeOne.getStart(), i_rangeTwo.getEnd());
            } else {
                // range two is bigger than range one
                resCode = -1;
            }
        } else {
            if (i_rangeOne.getStart() <= i_rangeTwo.getEnd()) {
                unitedRange = new Range(i_rangeTwo.getStart(), i_rangeOne.getEnd());
            } else {
                // range one is bigger than range two
                resCode = 1;
            }
        }
        unionResponse = new UnionResponse(unitedRange, resCode);
        return unionResponse;
    }

    long getLength() {
        return end - start;
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
