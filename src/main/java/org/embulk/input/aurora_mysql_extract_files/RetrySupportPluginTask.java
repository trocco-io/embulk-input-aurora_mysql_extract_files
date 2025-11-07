package org.embulk.input.aurora_mysql_extract_files;

import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.Task;

public interface RetrySupportPluginTask extends Task
{
    @Config("maximum_retries")
    @ConfigDefault("7")
    int getMaximumRetries();

    @Config("initial_retry_interval_millis")
    @ConfigDefault("2000")
    int getInitialRetryIntervalMillis();

    @Config("maximum_retry_interval_millis")
    @ConfigDefault("480000")
    int getMaximumRetryIntervalMillis();
}
