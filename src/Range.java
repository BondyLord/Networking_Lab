/**
 * Describes a simple range, with a start, an end, and a length
 */
class Range {
    private Long start;
    private Long end;

    Range(Long start, Long end) {
        this.start = start;
        this.end = end;
    }

    Long getStart() {
        return start;
    }

    Long getEnd() {
        return end;
    }

    public static UnionResponse unionRanges(Range i_rangeOne, Range i_rangeTwo){

        UnionResponse unionResponse;
        Range resRange = null;
        int resCode = 0;
        Long diff = i_rangeOne.getStart() - i_rangeTwo.getStart();

        if(diff < 0){
            if(i_rangeOne.getStart() == i_rangeTwo.getEnd()){
                resRange = new Range(i_rangeTwo.start, i_rangeOne.getEnd());
            }else{
                // range two is bigger than range one
                resCode = -1;
            }
        }else{
            if( i_rangeOne.getEnd() == i_rangeTwo.getStart()){
                resRange = new Range(i_rangeOne.start, i_rangeTwo.end);
            }else{
                // range one is bigger than range two
                resCode = 1;
            }
        }
        unionResponse = new UnionResponse(resRange, resCode);
        return unionResponse;

    }

    Long getLength() {
        return end - start + 1;
    }

    static class UnionResponse{

        private int m_resCode;
        private Range m_updatedRange;


        UnionResponse( Range i_updatedRange, int i_res){
            this.m_resCode = i_res;
            this.m_updatedRange = i_updatedRange;
        }

        public int get_res() {
            return m_resCode;
        }

        public Range get_updatedRange() {
            return m_updatedRange;
        }
    }

}
