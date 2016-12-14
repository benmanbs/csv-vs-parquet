import com.google.common.base.Objects;

/**
 * Created by bshai on 12/14/16.
 */
public class Record {

    private final int timePeriodId;
    private final int trackedSearchId;
    private final Integer rank;

    public Record(int timePeriodId, int trackedSearchId, Integer rank) {
        this.timePeriodId = timePeriodId;
        this.trackedSearchId = trackedSearchId;
        this.rank = rank;
    }

    public int getTimePeriodId() {
        return timePeriodId;
    }

    public int getTrackedSearchId() {
        return trackedSearchId;
    }

    public Integer getRank() {
        return rank;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("timePeriodId", timePeriodId)
                .add("trackedSearchId", trackedSearchId)
                .add("rank", rank)
                .toString();
    }
}
