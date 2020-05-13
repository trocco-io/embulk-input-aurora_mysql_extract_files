package org.embulk.input.aurora_mysql_extract_files;

import com.google.common.base.Optional;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigInject;
import org.embulk.config.Task;
import org.embulk.spi.BufferAllocator;

import java.util.List;

public interface PluginTask extends Task, RetrySupportPluginTask {
    @Config("aws_access_key_id")
    public Optional<String> getAwsAccessKey();

    @Config("aws_secret_access_key")
    public Optional<String> getAwsSecretAccessKey();

    @Config("host")
    public String getHost();

    @Config("port")
    @ConfigDefault("3306")
    public int getPort();

    @Config("database")
    public String getDatabase();

    @Config("user")
    public String getUser();

    @Config("password")
    @ConfigDefault("\"\"")
    public String getPassword();

    @Config("query")
    public String getQuery();

    @Config("s3_bucket")
    public String getS3Bucket();

    @Config("s3_path_prefix")
    public String getS3PathPrefix();

    @Config("ssl")
    @ConfigDefault("\"disable\"")
    public Ssl getSsl();

    public List<String> getFiles();

    public void setFiles(List<String> files);

    @ConfigInject
    public BufferAllocator getBufferAllocator();

    // debug option
    @Config("allow_before_clean_up")
    @ConfigDefault("true")
    public boolean getAllowBeforeCleanUp();

    @Config("skip_query")
    @ConfigDefault("false")
    public boolean getSkipQuery();
}
