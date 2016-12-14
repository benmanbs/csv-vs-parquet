/**
 * Created by bshai on 12/14/16.
 */
public enum VisibilityDistributionZone {
    HYPER_TRAFFIC,
    TRAFFIC,
    STRIKING_DISTANCE,
    EMERGING,
    DEVELOPMENTAL,
    DID_NOT_RANK;

    public static VisibilityDistributionZone fromRank(final Integer rank) {
        if (rank == null) {
            return VisibilityDistributionZone.DID_NOT_RANK;
        } else if (rank >= 1 && rank <= 3) {
            return VisibilityDistributionZone.HYPER_TRAFFIC;
        } else if (rank >= 4 && rank <= 10) {
            return VisibilityDistributionZone.TRAFFIC;
        } else if (rank >= 11 && rank <= 20) {
            return VisibilityDistributionZone.STRIKING_DISTANCE;
        } else if (rank >= 21 && rank <= 40) {
            return VisibilityDistributionZone.EMERGING;
        } else if (rank >= 41 && rank <= 100) {
            return VisibilityDistributionZone.DEVELOPMENTAL;
        } else {
            return VisibilityDistributionZone.DID_NOT_RANK;
        }
    }
}