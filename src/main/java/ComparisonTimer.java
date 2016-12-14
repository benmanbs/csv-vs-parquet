import java.io.IOException;

/**
 * Created by bshai on 12/14/16.
 */
public class ComparisonTimer {

    public static final int TIMES = 100;

    public static void main(String[] args) throws IOException {
        ParquetStreamer parquetStreamer = new ParquetStreamer();
        CsvStreamer csvStreamer = new CsvStreamer();

        int parquetTimeAggregate = 0, csvTimeAggregate = 0;

        for (int i = 0; i < TIMES; i++) {
            long startTime = System.nanoTime();
            parquetStreamer.go();
            long endTime = System.nanoTime();
            parquetTimeAggregate+=(endTime - startTime) / 1000000;
        }
        for (int i = 0; i < TIMES; i++) {
            long startTime = System.nanoTime();
            csvStreamer.go();
            long endTime = System.nanoTime();
            csvTimeAggregate+=(endTime - startTime) / 1000000;
        }

        System.out.println("Average execution time (in ms):");
        System.out.println("Parquet: " + parquetTimeAggregate / TIMES);
        System.out.println("CSV: " + csvTimeAggregate / TIMES);
    }
}
