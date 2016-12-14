import com.google.common.base.Suppliers;
import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Stream;

/**
 * Created by bshai on 12/14/16.
 */
public class Tramsformer {

    public static Map<VisibilityDistributionZone, int[]> getZones(Stream<Record> stream) {
        Map<VisibilityDistributionZone, int[]> map = Maps.newEnumMap(VisibilityDistributionZone.class);
        for (VisibilityDistributionZone zone : VisibilityDistributionZone.values()) {
            int[] arr = new int[384 - 284 + 1];
            map.put(zone, arr);
        }

        stream
                .filter((record) -> record.getTimePeriodId() >= 284)
                .forEach((record) -> {
                    VisibilityDistributionZone zone = VisibilityDistributionZone.fromRank(record.getRank());
                    map.get(zone)[record.getTimePeriodId() - 284]++;
                });

        return map;
    }

}
