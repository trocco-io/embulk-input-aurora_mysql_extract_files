package org.embulk.input.aurora_mysql_extract_files;

import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.TransactionalFileInput;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.TaskMapper;
import org.embulk.util.file.InputStreamFileInput;
import org.embulk.util.file.ResumableInputStream;
import org.embulk.util.retryhelper.RetryExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.embulk.util.retryhelper.RetryExecutor.builder;

public class AuroraMySQLExtractFilesFileInputPlugin implements FileInputPlugin
{
    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory.builder().addDefaultModules().build();
    private static final Logger log = LoggerFactory.getLogger(AuroraMySQLExtractFilesFileInputPlugin.class);

    @Override
    public ConfigDiff transaction(ConfigSource config, FileInputPlugin.Control control)
    {
        ConfigMapper configMapper = CONFIG_MAPPER_FACTORY.createConfigMapper();
        PluginTask task = configMapper.map(config, PluginTask.class);
        // delete s3
        try {
            if (task.getAllowBeforeCleanUp()) {
                deleteS3Dump(task);
            }
            else {
                log.info("Skip S3 Clean up");
            }
        }
        catch (Exception e) {
            log.error("delete error: ", e);
            return null;
        }

        if (!task.getSkipQuery()) {
            executeAuroraQuery(task);
        }
        else {
            log.info("Skip query execution");
        }
        setFiles(task);
        // run() method is called for this number of times in parallel.
        int taskCount = task.getFiles().size();

        return resume(task.toTaskSource(), taskCount, control);
    }

    public String selectIntoQuery(String query, String s3Bucket, String s3PathPrefix)
    {
        String s3Path = String.format("s3://%s/%s", s3Bucket, s3PathPrefix);
        return String.format(
                "%s INTO OUTFILE S3 '%s' CHARACTER SET UTF8 FIELDS TERMINATED BY '\\t' ENCLOSED BY '\"' MANIFEST OFF OVERWRITE ON;",
                query, s3Path);
    }

    public void executeAuroraQuery(PluginTask task)
    {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        }
        catch (ClassNotFoundException ex) {
            log.info("error");
            return;
        }

        String url = String.format("jdbc:mysql://%s:%d/%s", task.getHost(), task.getPort(), task.getDatabase());
        Properties props = new Properties();
        props.setProperty("user", task.getUser());
        props.setProperty("password", task.getPassword());
        props.setProperty("socketTimeout", String.valueOf(task.getSocketTimeout() * 1000)); // specified in milliseconds

        switch (task.getSsl()) {
            case DISABLE:
                props.setProperty("useSSL", "false");
                break;
            case ENABLE:
                props.setProperty("useSSL", "true");
                props.setProperty("requireSSL", "true");
                props.setProperty("verifyServerCertificate", "false");
                break;
            case VERIFY:
                props.setProperty("useSSL", "true");
                props.setProperty("requireSSL", "true");
                props.setProperty("verifyServerCertificate", "true");
                break;
        }

        try {
            Connection con = DriverManager.getConnection(url, props);
            String query = selectIntoQuery(task.getQuery(), task.getS3Bucket(), task.getS3PathPrefix());
            log.info(query);
            Statement stmt = con.createStatement();
            Thread cancelQuery = new Thread(() -> {
                try {
                    log.info("canceling query");
                    stmt.cancel();
                    if (!stmt.isClosed()) {
                        stmt.close();
                    }
                }
                catch (SQLException e) {
                    log.error(e.getMessage());
                }
            });

            // interrupt query when embulk process is shutdown
            Runtime.getRuntime().addShutdownHook(cancelQuery);
            try {
                stmt.executeQuery(query);
                Runtime.getRuntime().removeShutdownHook(cancelQuery);
                log.info("query succeeded");
            }
            finally {
                log.info("query finished");
                stmt.close();
            }
        }
        catch (Exception e) {
            // TODO: handle exception
            log.error("connection error", e);
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource, int taskCount, FileInputPlugin.Control control)
    {
        control.run(taskSource, taskCount);

        ConfigDiff configDiff = CONFIG_MAPPER_FACTORY.newConfigDiff();

        return configDiff;
    }

    @Override
    public void cleanup(TaskSource taskSource, int taskCount, List<TaskReport> successTaskReports)
    {
        TaskMapper taskMapper = CONFIG_MAPPER_FACTORY.createTaskMapper();
        final PluginTask task = taskMapper.map(taskSource, PluginTask.class);
        if (task.getAllowAfterCleanUp()) {
            deleteS3Dump(task);
        }
    }

    @Override
    public TransactionalFileInput open(TaskSource taskSource, int taskIndex)
    {
        TaskMapper taskMapper = CONFIG_MAPPER_FACTORY.createTaskMapper();
        final PluginTask task = taskMapper.map(taskSource, PluginTask.class);

        log.info(task.getFiles().get(taskIndex));
        return new S3FileInput(task, taskIndex);
    }

    public static InputStream openInputStream(PluginTask task, String file)
    {
        S3Client client = newS3Client(task);
        final GetObjectRequest request = GetObjectRequest.builder()
                .bucket(task.getS3Bucket())
                .key(file)
                .build();
        ResponseInputStream<GetObjectResponse> object = client.getObject(request);

        log.info("Start download : {} ", file);
        return object;
    }

    private static S3Client newS3Client(PluginTask task)
    {
        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(
                task.getAwsAccessKey().orElse(null),
                task.getAwsSecretAccessKey().orElse(null));

        ApacheHttpClient.Builder httpClientBuilder = ApacheHttpClient.builder()
                .maxConnections(50)
                .socketTimeout(Duration.ofMillis(8 * 60 * 1000));

        ClientOverrideConfiguration clientConfig = ClientOverrideConfiguration.builder()
                .retryPolicy(RetryPolicy.none())
                .build();

        return S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .region(Region.AP_NORTHEAST_1)
                .httpClientBuilder(httpClientBuilder)
                .overrideConfiguration(clientConfig)
                .build();
    }

    private List<String> getS3Keys(PluginTask task)
    {
        S3Client client = newS3Client(task);
        ListObjectsRequest request = ListObjectsRequest.builder()
                .bucket(task.getS3Bucket())
                .prefix(String.format("%s.part", task.getS3PathPrefix()))
                .build();
        ListObjectsResponse list = client.listObjects(request);
        List<S3Object> objects = list.contents();
        return objects.stream().map(S3Object::key).collect(Collectors.toList());
    }

    private void deleteS3Dump(PluginTask task)
    {
        S3Client client = newS3Client(task);
        List<String> s3Keys = getS3Keys(task);

        if (s3Keys.isEmpty()) {
            log.info("no objects are detected for delete");
        }
        else {
            log.info("deleting objects");
            List<String> s3Paths = s3Keys.stream().map(k -> String.format("s3://%s/%s", task.getS3Bucket(), k))
                    .collect(Collectors.toList());
            log.info("following files will be deleted\n{}", String.join("\n", s3Paths));
            client.deleteObject(DeleteObjectRequest.builder()
                    .bucket(task.getS3Bucket())
                    .key(task.getS3PathPrefix())
                    .build());

            List<ObjectIdentifier> keys = s3Keys.stream()
                    .map(key -> ObjectIdentifier.builder().key(key).build())
                    .collect(Collectors.toList());

            DeleteObjectsRequest multiObjectDeleteRequest = DeleteObjectsRequest.builder()
                    .bucket(task.getS3Bucket())
                    .delete(Delete.builder().objects(keys).build())
                    .build();
            client.deleteObjects(multiObjectDeleteRequest);
        }
    }

    private void setFiles(PluginTask task)
    {
        // TODO: read manifest
        List<String> s3Keys = getS3Keys(task);
        log.info(String.format("%d files", s3Keys.size()));
        task.setFiles(s3Keys);
    }

    static class S3InputStreamReopener implements ResumableInputStream.Reopener
    {
        private final Logger log = LoggerFactory.getLogger(S3InputStreamReopener.class);

        private final S3Client client;
        private final GetObjectRequest.Builder requestBuilder;
        private final long contentLength;
        private final RetryExecutor retryExec;

        public S3InputStreamReopener(S3Client client, GetObjectRequest.Builder requestBuilder, long contentLength)
        {
            this(client, requestBuilder, contentLength, null);
        }

        public S3InputStreamReopener(S3Client client, GetObjectRequest.Builder requestBuilder, long contentLength, RetryExecutor retryExec)
        {
            this.client = client;
            this.requestBuilder = requestBuilder;
            this.contentLength = contentLength;
            this.retryExec = retryExec;
        }

        @Override
        public InputStream reopen(final long offset, final Exception closedCause) throws IOException
        {
            log.warn(format("S3 read failed. Retrying GET request with %,d bytes offset", offset), closedCause);
            GetObjectRequest request = requestBuilder
                    .range(String.format("bytes=%d-%d", offset, contentLength - 1))
                    .build();

            return new DefaultRetryable<ResponseInputStream<GetObjectResponse>>(format("Getting object '%s'", request.key())) {
                @Override
                public ResponseInputStream<GetObjectResponse> call()
                {
                    return client.getObject(request);
                }
            }.executeWithCheckedException(retryExec, IOException.class);
        }
    }

    public static class S3FileInput extends InputStreamFileInput implements TransactionalFileInput
    {
        public S3FileInput(PluginTask task, int taskIndex)
        {
            super(Exec.getBufferAllocator(), new SingleFileProvider(task, taskIndex));
        }

        public void abort()
        {
        }

        public TaskReport commit()
        {
            return CONFIG_MAPPER_FACTORY.newTaskReport();
        }

        @Override
        public void close()
        {
        }
    }

    private static RetryExecutor retryExecutorFrom(RetrySupportPluginTask task)
    {
        return builder().withRetryLimit(task.getMaximumRetries())
                .withInitialRetryWaitMillis(task.getInitialRetryIntervalMillis())
                .withMaxRetryWaitMillis(task.getMaximumRetryIntervalMillis()).build();
    }

    // TODO create single-file InputStreamFileInput utility
    private static class SingleFileProvider implements InputStreamFileInput.Provider
    {
        private final S3Client client;
        private final String bucket;
        private final RetryExecutor retryExec;
        private final String key;
        private boolean provided = false;

        public SingleFileProvider(PluginTask task, int taskIndex)
        {
            this.client = newS3Client(task);
            this.bucket = task.getS3Bucket();
            this.key = task.getFiles().get(taskIndex);
            this.retryExec = retryExecutorFrom(task);
        }

        @Override
        public InputStreamFileInput.InputStreamWithHints openNextWithHints() throws IOException
        {
            if (provided) {
                return null;
            }
            provided = true;
            final GetObjectRequest.Builder requestBuilder = GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key);

            ResponseInputStream<GetObjectResponse> object = new DefaultRetryable<ResponseInputStream<GetObjectResponse>>(format("Getting object '%s'", key)) {
                @Override
                public ResponseInputStream<GetObjectResponse> call()
                {
                    return client.getObject(requestBuilder.build());
                }
            }.executeWithCheckedException(retryExec, IOException.class);

            long objectSize = object.response().contentLength();
            // Some plugin users are parsing this output to get file list.
            // Keep it for now but might be removed in the future.
            log.info("Open S3Object with bucket [{}], key [{}], with size [{}]", bucket, key, objectSize);
            InputStream inputStream = new ResumableInputStream(object,
                    new S3InputStreamReopener(client, requestBuilder, objectSize, retryExec));
            return new InputStreamFileInput.InputStreamWithHints(inputStream, String.format("s3://%s/%s", bucket, key));
        }

        @Override
        public void close()
        {
        }
    }
}
