import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Created by bshai on 12/14/16.
 */
public class ParquetStreamer {

    public ParquetStreamer() {}

    public Map<VisibilityDistributionZone, int[]> go() throws IOException {
        final String dir = "/Users/bshai/Documents/conductor_data/search-channel-derivative-reports/visibility_distribution/v1/time_period_id=384/account_id=6128/web_property_id=18717/rank_source_id=16/rank_type=TRUE_RANK/device_id=1/locale_id=14/";
        File parquetFile = new File(dir + "visibilityDistribution.parquet");

        Path parquetFilePath = new Path(parquetFile.toURI());

        Configuration configuration = new Configuration(true);

        GroupReadSupport readSupport = new GroupReadSupport();
        ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, parquetFilePath);
        MessageType schema = readFooter.getFileMetaData().getSchema();

        readSupport.init(configuration, null, schema);

        ParquetReader<Group> reader = new ParquetReader<Group>(parquetFilePath, readSupport);

        final Stream<Record> recordStream = StreamSupport.stream(((Iterable<Group>) () -> new Iterator<Group>() {
            private Group group;

            @Override
            public boolean hasNext() {
                try {
                    group = reader.read();
                    return group != null;
                } catch (IOException e) {
                    return false;
                }
            }

            @Override
            public Group next() {
                return group;
            }
        }).spliterator(), false)
                .map(group -> new Record(group.getInteger(0, 0), group.getInteger(1, 0), group.toString().contains("rank") ? group.getInteger(2, 0) : null));

        return Transformer.getZones(recordStream);
    }
    public static void main(String[] args) throws Exception {
        long startTime = System.nanoTime();
        ParquetStreamer streamer = new ParquetStreamer();
        final Map<VisibilityDistributionZone, int[]> zones = streamer.go();
        zones.entrySet().forEach(entry -> System.out.println(entry.getKey() + ":" + ArrayUtils.toString(entry.getValue())));
        long endTime = System.nanoTime();

        System.out.println("\nTime to execute (in ms): " + ((endTime - startTime) / 1000000));
    }
}
