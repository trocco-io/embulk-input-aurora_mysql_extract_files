package org.embulk.input.aurora_mysql_extract_files;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;

import com.amazonaws.services.s3.model.DeleteObjectsRequest;

import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.TransactionalFileInput;
import org.embulk.spi.util.InputStreamTransactionalFileInput;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import org.slf4j.Logger;

public class AuroraMySQLExtractFilesFileInputPlugin implements FileInputPlugin {

    private static final Logger log = Exec.getLogger(AuroraMySQLExtractFilesFileInputPlugin.class);
    private AmazonS3 client;

    @Override
    public ConfigDiff transaction(ConfigSource config, FileInputPlugin.Control control) {
        PluginTask task = config.loadConfig(PluginTask.class);

        // delete s3
        try {
            if (task.getAllowBeforeCleanUp()){
                deleteS3Dump(task);
            }
        } catch(Exception e){
            log.error("delete error: ", e);
            return null;
        }

        if (!task.getSkipQuery()){
            executeAuroraQuery(task);
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
        try {
            Connection con = DriverManager.getConnection(url, task.getUser(), task.getPassword());
            String query = selectIntoQuery(task.getQuery(), task.getS3Bucket(), task.getS3PathPrefix());
            log.info(query);
            Statement stmt = con.createStatement();
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
        // TODO: retry
        final PluginTask task = taskSource.loadTask(PluginTask.class);

        log.info(task.getFiles().get(taskIndex));
        InputStream input = openInputStream(task, task.getFiles().get(taskIndex));
        try {

            return new InputStreamTransactionalFileInput(task.getBufferAllocator(), input) {
                @Override
                public void abort() {
                }

                @Override
                public TaskReport commit() {
                    return Exec.newTaskReport();
                }
            };
        } catch (Exception e) {
            return null;
        }
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
        return AmazonS3ClientBuilder
            .standard()
            .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
            .withRegion(Regions.AP_NORTHEAST_1).build();
    }

    private List<String> getS3Keys(PluginTask task){
        ListObjectsRequest request = new ListObjectsRequest().withBucketName(task.getS3Bucket())
                .withPrefix(String.format("%s.part",task.getS3PathPrefix()));
        ObjectListing list = client.listObjects(request);
        List<S3ObjectSummary> objects = list.getObjectSummaries();
        return objects.stream().map(S3ObjectSummary::getKey).collect(Collectors.toList());
    }

    private void deleteS3Dump(PluginTask task){
        client = newS3Client(task);
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
}
