package org.embulk.input.aurora_mysql_extract_files;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.services.s3.model.*;

import com.google.common.annotations.VisibleForTesting;
import org.embulk.config.*;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.TransactionalFileInput;
import org.embulk.spi.util.InputStreamFileInput;
import org.embulk.spi.util.InputStreamTransactionalFileInput;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import org.embulk.spi.util.ResumableInputStream;
import org.embulk.spi.util.RetryExecutor;
import org.slf4j.Logger;

import static java.lang.String.format;
import static org.embulk.spi.util.RetryExecutor.retryExecutor;

public class AuroraMySQLExtractFilesFileInputPlugin implements FileInputPlugin {

    private static final Logger log = Exec.getLogger(AuroraMySQLExtractFilesFileInputPlugin.class);

    @Override
    public ConfigDiff transaction(ConfigSource config, FileInputPlugin.Control control) {
        PluginTask task = config.loadConfig(PluginTask.class);

        // delete s3
        try {
            if (task.getAllowBeforeCleanUp()){
                deleteS3Dump(task);
            }else{
                log.info("Skip S3 Clean up");
            }
        } catch(Exception e){
            log.error("delete error: ", e);
            return null;
        }

        if (!task.getSkipQuery()){
            executeAuroraQuery(task);
        }else{
            log.info("Skip query execution");
        }
        setFiles(task);
        // run() method is called for this number of times in parallel.
        int taskCount = task.getFiles().size();

        return resume(task.dump(), taskCount, control);
    }

    public String selectIntoQuery(String query, String s3Bucket, String s3PathPrefix) {
        String s3Path = String.format("s3://%s/%s", s3Bucket, s3PathPrefix);
        return String.format("%s INTO OUTFILE S3 '%s' CHARACTER SET UTF8 FIELDS TERMINATED BY '\\t' ENCLOSED BY '\"' MANIFEST OFF OVERWRITE ON;", query, s3Path);
    }

    public void executeAuroraQuery(PluginTask task) {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException ex) {
            log.info("error");
            return;
        }

        String url = String.format("jdbc:mysql://%s:%d/%s", task.getHost(), task.getPort(), task.getDatabase());
        Properties props = new Properties();
        props.setProperty("user", task.getUser());
        props.setProperty("password", task.getPassword());

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
            Connection con =  DriverManager.getConnection(url, props);
            String query = selectIntoQuery(task.getQuery(), task.getS3Bucket(), task.getS3PathPrefix());
            log.info(query);
            Statement stmt = con.createStatement();

            // interrupt query when embulk process is shutdown
            Runtime.getRuntime().addShutdownHook(new Thread(){
                @Override
                public void run(){
                    try {
                        stmt.cancel();
                        if(!stmt.isClosed()){
                            stmt.close();
                        }
                    } catch (SQLException e){
                        log.error(e.getMessage());
                    }
                }
            });
            try {
                stmt.executeQuery(query);
                log.info("query succeeded");
            } finally {
                log.info("query finished");
                stmt.close();
            }
        } catch (Exception e) {
            // TODO: handle exception
            log.error("connection error", e);
        }
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource, int taskCount, FileInputPlugin.Control control) {
        control.run(taskSource, taskCount);

        ConfigDiff configDiff = Exec.newConfigDiff();

        return configDiff;
    }

    @Override
    public void cleanup(TaskSource taskSource, int taskCount, List<TaskReport> successTaskReports) {
    }

    @Override
    public TransactionalFileInput open(TaskSource taskSource, int taskIndex) {
        final PluginTask task = taskSource.loadTask(PluginTask.class);

        log.info(task.getFiles().get(taskIndex));
        return new S3FileInput(task, taskIndex);
    }

    public static InputStream openInputStream(PluginTask task, String file)
    {
        AmazonS3 client = newS3Client(task);
        final GetObjectRequest request = new GetObjectRequest(task.getS3Bucket(), file);
        S3Object object = client.getObject(request);

        log.info("Start download : {} ", file);
        return object.getObjectContent();
    }

    private static AmazonS3 newS3Client(PluginTask task) {
        BasicAWSCredentials awsCreds = new BasicAWSCredentials(task.getAwsAccessKey().orNull(),
                task.getAwsSecretAccessKey().orNull());
        ClientConfiguration clientConfiguration = getClientConfiguration(task);
        return AmazonS3ClientBuilder
            .standard()
            .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
            .withClientConfiguration(clientConfiguration)
            .withRegion(Regions.AP_NORTHEAST_1)
            .build();
    }

    private static ClientConfiguration getClientConfiguration(PluginTask task)
    {
        ClientConfiguration clientConfig = new ClientConfiguration();

        clientConfig.setMaxConnections(50); // SDK default: 50
        clientConfig.setMaxErrorRetry(3); // SDK default: 3
        clientConfig.setSocketTimeout(8 * 60 * 1000); // SDK default: 50*1000
        clientConfig.setRetryPolicy(PredefinedRetryPolicies.NO_RETRY_POLICY);

        return clientConfig;
    }

    private List<String> getS3Keys(PluginTask task){
        AmazonS3 client = newS3Client(task);
        ListObjectsRequest request = new ListObjectsRequest().withBucketName(task.getS3Bucket())
                .withPrefix(String.format("%s.part",task.getS3PathPrefix()));
        ObjectListing list = client.listObjects(request);
        List<S3ObjectSummary> objects = list.getObjectSummaries();
        return objects.stream().map(S3ObjectSummary::getKey).collect(Collectors.toList());
    }

    private void deleteS3Dump(PluginTask task){
        AmazonS3 client = newS3Client(task);
        List<String> s3Keys = getS3Keys(task);

        if (s3Keys.isEmpty()){
            log.info("no objects are detected for delete");
        }else{
            log.info("deleting objects");
            List<String> s3Paths = s3Keys.stream().map(k -> String.format("s3://%s/%s", task.getS3Bucket(),k)).collect(Collectors.toList());
            log.info("following files will be deleted\n{}", String.join("\n",s3Paths));
            client.deleteObject(new DeleteObjectRequest(task.getS3Bucket(), task.getS3PathPrefix()));
            DeleteObjectsRequest multiObjectDeleteRequest = new DeleteObjectsRequest(task.getS3Bucket());
            List<DeleteObjectsRequest.KeyVersion> keys = s3Keys.stream()
                    .map(DeleteObjectsRequest.KeyVersion::new)
                    .collect(Collectors.toList());
            multiObjectDeleteRequest.setKeys(keys);
            client.deleteObjects(multiObjectDeleteRequest);
        }
    }

    private void setFiles(PluginTask task){
        // TODO: read manifest
        List<String> s3Keys = getS3Keys(task);
        log.info(String.format("%d files", s3Keys.size()));
        task.setFiles(s3Keys);
    }

    @VisibleForTesting
    static class S3InputStreamReopener
            implements ResumableInputStream.Reopener
    {
        private final Logger log = Exec.getLogger(S3InputStreamReopener.class);

        private final AmazonS3 client;
        private final GetObjectRequest request;
        private final long contentLength;
        private final RetryExecutor retryExec;

        public S3InputStreamReopener(AmazonS3 client, GetObjectRequest request, long contentLength)
        {
            this(client, request, contentLength, null);
        }

        public S3InputStreamReopener(AmazonS3 client, GetObjectRequest request, long contentLength, RetryExecutor retryExec)
        {
            this.client = client;
            this.request = request;
            this.contentLength = contentLength;
            this.retryExec = retryExec;
        }

        @Override
        public InputStream reopen(final long offset, final Exception closedCause) throws IOException
        {
            log.warn(format("S3 read failed. Retrying GET request with %,d bytes offset", offset), closedCause);
            request.setRange(offset, contentLength - 1);  // [first, last]

            return new DefaultRetryable<S3ObjectInputStream>(format("Getting object '%s'", request.getKey())) {
                @Override
                public S3ObjectInputStream call()
                {
                    return client.getObject(request).getObjectContent();
                }
            }.executeWithCheckedException(retryExec, IOException.class);
        }
    }

    public class S3FileInput
            extends InputStreamFileInput
            implements TransactionalFileInput
    {
        public S3FileInput(PluginTask task, int taskIndex)
        {
            super(task.getBufferAllocator(), new SingleFileProvider(task, taskIndex));
        }

        public void abort()
        {
        }

        public TaskReport commit()
        {
            return Exec.newTaskReport();
        }

        @Override
        public void close()
        {
        }
    }

    private static RetryExecutor retryExecutorFrom(RetrySupportPluginTask task)
    {
        return retryExecutor()
                .withRetryLimit(task.getMaximumRetries())
                .withInitialRetryWait(task.getInitialRetryIntervalMillis())
                .withMaxRetryWait(task.getMaximumRetryIntervalMillis());
    }

    // TODO create single-file InputStreamFileInput utility
    private class SingleFileProvider
            implements InputStreamFileInput.Provider
    {
        private AmazonS3 client;
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
            if (provided){
                return null;
            }
            provided = true;
            final GetObjectRequest request = new GetObjectRequest(bucket, key);

            S3Object object = new DefaultRetryable<S3Object>(format("Getting object '%s'", request.getKey())) {
                @Override
                public S3Object call()
                {
                    return client.getObject(request);
                }
            }.executeWithCheckedException(retryExec, IOException.class);

            long objectSize = object.getObjectMetadata().getContentLength();
            // Some plugin users are parsing this output to get file list.
            // Keep it for now but might be removed in the future.
            log.info("Open S3Object with bucket [{}], key [{}], with size [{}]", bucket, key, objectSize);
            InputStream inputStream = new ResumableInputStream(object.getObjectContent(), new S3InputStreamReopener(client, request, objectSize, retryExec));
            return new InputStreamFileInput.InputStreamWithHints(inputStream, String.format("s3://%s/%s", bucket, key));
        }

        @Override
        public void close()
        {
        }
    }
}
