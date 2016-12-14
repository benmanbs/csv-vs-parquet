import com.google.common.collect.Lists;
import org.apache.commons.lang.ArrayUtils;
import org.xerial.snappy.SnappyFramedInputStream;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * Created by bshai on 12/14/16.
 */
public class CsvStreamer {

    public static void main(String[] args) throws Exception {
        long startTime = System.nanoTime();
        final String dir = "/Users/bshai/Documents/conductor_data/search-channel-derivative-reports/visibility_distribution/v1/time_period_id=384/account_id=6128/web_property_id=18717/rank_source_id=16/rank_type=TRUE_RANK/device_id=1/locale_id=14/";
        InputStream is = new FileInputStream(new File(dir + "visibilityDistribution.csv.sz"));

        final InputStream fileStream = new SnappyFramedInputStream(new BufferedInputStream(is));
        BufferedReader br = new BufferedReader(new InputStreamReader(fileStream));

        final Stream<Record> recordStream = br.lines()
                .map(line -> {
                    final List<Integer> ints = Lists.newArrayList(line.split(",")).stream()
                            .map(cell -> {
                                final String cellVal = cell.substring(1, cell.length() - 1);
                                return cellVal.length() == 0 ? null : Integer.parseInt(cellVal);
                            })
                            .collect(toList());
                    return new Record(ints.get(0), ints.get(1), ints.get(2));
                });

        final Map<VisibilityDistributionZone, int[]> zones = Transformer.getZones(recordStream);
        zones.entrySet().forEach(entry -> System.out.println(entry.getKey() + ":" + ArrayUtils.toString(entry.getValue())));
        long endTime = System.nanoTime();

        System.out.println("\nTime to execute (in ms): " + ((endTime - startTime) / 1000000));
    }
}
