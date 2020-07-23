package org.embulk.input.gcs.parquet;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Storage;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.NoWrappingJsonEncoder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.embulk.spi.Exec;
import org.embulk.spi.unit.LocalFile;
import org.embulk.spi.util.InputStreamFileInput;
import org.embulk.spi.util.InputStreamFileInput.InputStreamWithHints;
import org.embulk.spi.util.ResumableInputStream;
import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static java.lang.String.format;

@SuppressWarnings("unchecked")
public class SingleFileProvider implements InputStreamFileInput.Provider {
    private static final Logger LOGGER = Exec.getLogger(SingleFileProvider.class);
    private final Storage client;
    private final String bucket;
    private final Iterator<String> iterator;
    private boolean opened = false;
    private final LocalFile jsonFile;

    SingleFileProvider(PluginTask task, int taskIndex) {
        this.client = AuthUtils.newClient(task);
        this.bucket = task.getBucket();
        this.iterator = task.getFiles().get(taskIndex).iterator();
        this.jsonFile = task.getJsonKeyfile().get();
    }

    @Override
    public InputStreamWithHints openNextWithHints() {
        if (opened) {
            return null;
        }
        opened = true;
        if (!iterator.hasNext()) {
            return null;
        }
        String key = iterator.next();
        InputStream inputStream = null;
        Configuration config = getConfig(jsonFile);
        String path = String.format("gs://%s/%s", bucket, key);
        ParquetReader reader = null;
        List<GenericRecord> records = new ArrayList<>();
        Schema schema;
        try {
            reader = AvroParquetReader.builder(new Path(path)).withConf(config).build();
            Object obj = reader.read();

            while (obj != null) {
                if (obj instanceof GenericRecord) {
                    records.add(((GenericRecord) obj));
                }
                obj = reader.read();
            }
            if (records.size() == 0) {
                schema =
                        SchemaBuilder.record("default") // source's name
                                .namespace("default") // source's namespace
                                .fields() // empty fields
                                .endRecord();
            } else {
                schema = records.get(0).getSchema();
                LOGGER.info("Schema is {}", schema);
            }

            ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
            DatumWriter<GenericRecord> writer = new GenericDatumWriter(schema);
            NoWrappingJsonEncoder jsonEncoder = new NoWrappingJsonEncoder(schema, byteStream);

            for (GenericRecord genericRecord : records) {
                writer.write(genericRecord, jsonEncoder);
            }
            jsonEncoder.flush();
            byteStream.flush();
            inputStream = new ByteArrayInputStream(byteStream.toString().getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return new InputStreamWithHints(
                new ResumableInputStream(
                        inputStream, new InputStreamReopener(client, bucket, key, jsonFile)),
                String.format("gcs://%s/%s", bucket, key));
    }

    @Override
    public void close() {
    }

    static class InputStreamReopener implements ResumableInputStream.Reopener {
        private Logger logger = Exec.getLogger(getClass());
        private final Storage client;
        private final String bucket;
        private final String key;
        private final LocalFile jsonFile;

        InputStreamReopener(Storage client, String bucket, String key, LocalFile jsonFile) {
            this.client = client;
            this.bucket = bucket;
            this.key = key;
            this.jsonFile = jsonFile;
        }

        @Override
        public InputStream reopen(long offset, Exception closedCause) throws IOException {
            logger.warn(
                    format("GCS read failed. Retrying GET request with %,d bytes offset", offset),
                    closedCause);
            ReadChannel ch = client.get(bucket, key).reader();
            ch.seek(offset);
            InputStream inputStream = null;
            Configuration config = getConfig(jsonFile);
            String path = String.format("gs://%s/%s", bucket, key);
            ParquetReader reader = null;
            List<GenericRecord> records = new ArrayList<>();
            Schema schema;
            try {
                reader = AvroParquetReader.builder(new Path(path)).withConf(config).build();
                Object obj = reader.read();

                while (obj != null) {
                    if (obj instanceof GenericRecord) {
                        records.add(((GenericRecord) obj));
                    }
                    obj = reader.read();
                }
                if (records.size() == 0) {
                    schema =
                            SchemaBuilder.record("default") // source's name
                                    .namespace("default") // source's namespace
                                    .fields() // empty fields
                                    .endRecord();
                } else {
                    schema = records.get(0).getSchema();
                    LOGGER.info("Schema is {}", schema);
                }
                ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
                DatumWriter<GenericRecord> writer = new GenericDatumWriter(schema);
                NoWrappingJsonEncoder jsonEncoder = new NoWrappingJsonEncoder(schema, byteStream, true);

                for (GenericRecord genericRecord : records) {
                    writer.write(genericRecord, jsonEncoder);
                }
                jsonEncoder.flush();
                byteStream.flush();
                inputStream = new ByteArrayInputStream(byteStream.toString().getBytes());
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (reader != null) {
                    try {
                        reader.close();

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            return inputStream;
        }
    }

    private static Configuration getConfig(LocalFile jsonFile) {
        Configuration config = new Configuration();
        config.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
        config.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
        config.set("google.cloud.auth.service.account.enable", "true");
        config.set("fs.gs.auth.service.account.enable", "true");
        config.set("fs.gs.working.dir", "/");
        config.set("fs.gs.auth.service.account.json.keyfile", jsonFile.getPath().toAbsolutePath().toString());
        config.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        config.set("fs.file.impl", LocalFileSystem.class.getName());

        return config;
    }
}
