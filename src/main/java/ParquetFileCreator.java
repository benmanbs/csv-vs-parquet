import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.parquet.Log;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

/**
 * Created by bshai on 12/14/16.
 */
public class ParquetFileCreator {

    private static final Log LOG = Log.getLog(ParquetFileCreator.class);

    public static final String CSV_DELIMITER= ",";

    public static class CsvParquetWriter extends ParquetWriter<List<String>> {

        public CsvParquetWriter(Path file, MessageType schema) throws IOException {
            this(file, schema, false);
        }

        public CsvParquetWriter(Path file, MessageType schema, boolean enableDictionary) throws IOException {
            this(file, schema, CompressionCodecName.SNAPPY, enableDictionary);
        }

        public CsvParquetWriter(Path file, MessageType schema, CompressionCodecName codecName, boolean enableDictionary) throws IOException {
            super(file, (WriteSupport<List<String>>) new CsvWriteSupport(schema), codecName, DEFAULT_BLOCK_SIZE, DEFAULT_PAGE_SIZE, enableDictionary, false);
        }
    }

    public static class CsvWriteSupport extends WriteSupport<List<String>> {
        MessageType schema;
        RecordConsumer recordConsumer;
        List<ColumnDescriptor> cols;

        // TODO: support specifying encodings and compression
        public CsvWriteSupport(MessageType schema) {
            this.schema = schema;
            this.cols = schema.getColumns();
        }

        @Override
        public WriteContext init(Configuration config) {
            return new WriteContext(schema, new HashMap<String, String>());
        }

        @Override
        public void prepareForWrite(RecordConsumer r) {
            recordConsumer = r;
        }

        @Override
        public void write(List<String> values) {
            if (values.size() != cols.size()) {
                throw new ParquetEncodingException("Invalid input data. Expecting " +
                        cols.size() + " columns. Input had " + values.size() + " columns (" + cols + ") : " + values);
            }

            recordConsumer.startMessage();
            for (int i = 0; i < cols.size(); ++i) {
                String val = values.get(i);
                // Since our CSV file is wrapped in quotes, omit the first and last char
                val = val.substring(1, val.length()-1);
                // val.length() == 0 indicates a NULL value.
                if (val.length() > 0) {
                    recordConsumer.startField(cols.get(i).getPath()[0], i);
                    switch (cols.get(i).getType()) {
                        case BOOLEAN:
                            recordConsumer.addBoolean(Boolean.parseBoolean(val));
                            break;
                        case FLOAT:
                            recordConsumer.addFloat(Float.parseFloat(val));
                            break;
                        case DOUBLE:
                            recordConsumer.addDouble(Double.parseDouble(val));
                            break;
                        case INT32:
                            recordConsumer.addInteger(Integer.parseInt(val));
                            break;
                        case INT64:
                            recordConsumer.addLong(Long.parseLong(val));
                            break;
                        case BINARY:
                            recordConsumer.addBinary(stringToBinary(val));
                            break;
                        default:
                            throw new ParquetEncodingException(
                                    "Unsupported column type: " + cols.get(i).getType());
                    }
                    recordConsumer.endField(cols.get(i).getPath()[0], i);
                }
            }
            recordConsumer.endMessage();
        }

        private Binary stringToBinary(Object value) {
            return Binary.fromString(value.toString());
        }
    }

    public static void convertCsvToParquet(File csvFile, File outputParquetFile) throws IOException {
        convertCsvToParquet(csvFile, outputParquetFile, false);
    }

    private static String readFile(String path) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(path));
        StringBuilder stringBuilder = new StringBuilder();

        try {
            String line = null;
            String ls = System.getProperty("line.separator");

            while ((line = reader.readLine()) != null ) {
                stringBuilder.append(line);
                stringBuilder.append(ls);
            }
        } finally {
            closeQuietly(reader);
        }

        return stringBuilder.toString();
    }

    public static String getSchema(File csvFile) throws IOException {
        String fileName = csvFile.getName().substring(
                0, csvFile.getName().length() - ".csv".length()) + ".schema";
        File schemaFile = new File(csvFile.getParentFile(), fileName);
        return readFile(schemaFile.getAbsolutePath());
    }

    public static void convertCsvToParquet(File csvFile, File outputParquetFile, boolean enableDictionary) throws IOException {
        LOG.info("Converting " + csvFile.getName() + " to " + outputParquetFile.getName());
        String rawSchema = getSchema(csvFile);
        if(outputParquetFile.exists()) {
            throw new IOException("Output file " + outputParquetFile.getAbsolutePath() +
                    " already exists");
        }

        Path path = new Path(outputParquetFile.toURI());

        MessageType schema = MessageTypeParser.parseMessageType(rawSchema);
        CsvParquetWriter writer = new CsvParquetWriter(path, schema, enableDictionary);

        BufferedReader br = new BufferedReader(new FileReader(csvFile));
        String line;
        int lineNumber = 0;
        try {
            while ((line = br.readLine()) != null) {
                String[] fields = line.split(Pattern.quote(CSV_DELIMITER));
                writer.write(Arrays.asList(fields));
                ++lineNumber;
            }

            writer.close();
        } finally {
            LOG.info("Number of lines: " + lineNumber);
            closeQuietly(br);
        }
    }

    public static void closeQuietly(Closeable res) {
        try {
            if(res != null) {
                res.close();
            }
        } catch (IOException ioe) {
            LOG.warn("Exception closing reader " + res + ": " + ioe.getMessage());
        }
    }

    public static void main(String[] args) throws IOException {
        final String dir = "/Users/bshai/Documents/conductor_data/search-channel-derivative-reports/visibility_distribution/v1/time_period_id=384/account_id=6128/web_property_id=18717/rank_source_id=16/rank_type=TRUE_RANK/device_id=1/locale_id=14/";
        convertCsvToParquet(new File(dir + "visibilityDistribution.csv"), new File(dir + "visibilityDistribution.parquet"));
    }

}
