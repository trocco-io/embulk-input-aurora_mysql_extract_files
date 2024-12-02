package org.embulk.input.aurora_mysql_extract_files;

// ref: https://github.com/embulk/embulk-input-s3/blob/master/embulk-input-s3/src/main/java/org/embulk/input/s3/DefaultRetryable.java

import com.amazonaws.AmazonServiceException;
import org.apache.http.HttpStatus;
import org.embulk.util.retryhelper.RetryExecutor;
import org.embulk.util.retryhelper.RetryGiveupException;
import org.embulk.util.retryhelper.Retryable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

import static java.lang.String.format;

/**
 * Retryable utility, regardless the occurred exceptions,
 * Also provide a default approach for exception propagation.
 */
public class DefaultRetryable<T> implements Retryable<T>
{
    private static final Logger log = LoggerFactory.getLogger(DefaultRetryable.class);
    private static final Set<Integer> NONRETRYABLE_STATUS_CODES = new HashSet<>(2);
    private static final Set<String> NONRETRYABLE_ERROR_CODES = new HashSet<>(1);
    private final String operationName;
    private Callable<T> callable;

    static {
        NONRETRYABLE_STATUS_CODES.add(HttpStatus.SC_FORBIDDEN);
        NONRETRYABLE_STATUS_CODES.add(HttpStatus.SC_METHOD_NOT_ALLOWED);
        NONRETRYABLE_ERROR_CODES.add("ExpiredToken");
    }

    /**
     * @param operationName the name that will be referred on logging
     */
    public DefaultRetryable(String operationName)
    {
        this.operationName = operationName;
    }

    /**
     * @param operationName the name that will be referred on logging
     * @param callable the operation, either define this at construction time or override the call() method
     */
    public DefaultRetryable(String operationName, Callable<T> callable)
    {
        this.operationName = operationName;
        this.callable = callable;
    }

    public DefaultRetryable()
    {
        this("Anonymous operation");
    }

    public DefaultRetryable(Callable<T> callable)
    {
        this("Anonymous operation", callable);
    }

    @Override
    public T call() throws Exception
    {
        if (callable != null) {
            return callable.call();
        }
        else {
            throw new IllegalStateException("Either override call() or construct with a Runnable");
        }
    }

    @Override
    public boolean isRetryableException(Exception exception)
    {
        // No retry on a subset of service exceptions
        if (exception instanceof AmazonServiceException) {
            AmazonServiceException ase = (AmazonServiceException) exception;
            return !NONRETRYABLE_STATUS_CODES.contains(ase.getStatusCode()) && !NONRETRYABLE_ERROR_CODES.contains(ase.getErrorCode());
        }
        return true;
    }

    @Override
    public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait)
    {
        String message = format("%s failed. Retrying %d/%d after %d seconds. Message: %s",
                operationName, retryCount, retryLimit, retryWait / 1000, exception.getMessage());
        if (retryCount % retryLimit == 0) {
            log.warn(message, exception);
        }
        else {
            log.warn(message);
        }
    }

    @Override
    public void onGiveup(Exception firstException, Exception lastException)
    {
        // Exceptions would be propagated, so it's up to the caller to handle, this is just warning
        log.warn("Giving up on retrying for {}, first exception is [{}], last exception is [{}]",
                operationName, firstException.getMessage(), lastException.getMessage());
    }

    /**
     * Run itself by the supplied executor,
     *
     * This propagates all exceptions (as unchecked) and unwrap RetryGiveupException for the original cause.
     * If the original exception already is a RuntimeException, it will be propagated as is. If not, it will
     * be wrapped around with a RuntimeException.
     *
     * For convenient, it execute normally without retrying when executor is null.
     *
     * @throws RuntimeException the original cause
     */
    public T executeWith(RetryExecutor executor)
    {
        if (executor == null) {
            try {
                return this.call();
            }
            catch (Exception e) {
                throw propagate(e);
            }
        }

        try {
            return executor.runInterruptible(this);
        }
        catch (RetryGiveupException e) {
            throw propagate(e.getCause());
        }
        catch (InterruptedException e) {
            throw propagate(e);
        }
    }

    /**
     * Run itself by the supplied executor,
     *
     * Same as `executeWith`, this propagates all original exceptions. But `propagateAsIsException` will
     * be re-throw without being wrapped on a RuntimeException, whether it is a checked or unchecked exception.
     *
     * For convenient, it execute normally without retrying when executor is null.
     *
     * @throws X whatever checked exception that you decided to propagate directly
     * @throws RuntimeException wrap around whatever the original cause of failure (potentially thread interruption)
     */
    public <X extends Throwable> T executeWithCheckedException(RetryExecutor executor,
                                                               Class<X> propagateAsIsException) throws X
    {
        if (executor == null) {
            try {
                return this.call();
            }
            catch (Exception e) {
                throw propagate(e);
            }
        }

        try {
            return executor.runInterruptible(this);
        }
        catch (RetryGiveupException e) {
            propagateIfInstanceOf(e.getCause(), propagateAsIsException);
            throw propagate(e.getCause());
        }
        catch (InterruptedException e) {
            throw propagate(e);
        }
    }

    @Deprecated
    // https://github.com/google/guava/blob/v18.0/guava/src/com/google/common/base/Preconditions.java#L208-L213
    // https://github.com/google/guava/blob/v18.0/guava/src/com/google/common/base/Throwables.java#L158-L161
    // https://github.com/google/guava/blob/v18.0/guava/src/com/google/common/base/Throwables.java#L82-L85
    private static RuntimeException propagate(Throwable throwable)
    {
        if (throwable == null) {
            throw new NullPointerException();
        }
        propagateIfInstanceOf(throwable, Error.class);
        propagateIfInstanceOf(throwable, RuntimeException.class);
        throw new RuntimeException(throwable);
    }

    @Deprecated
    // https://github.com/google/guava/blob/v18.0/guava/src/com/google/common/base/Throwables.java#L60-L66
    private static <X extends Throwable> void propagateIfInstanceOf(Throwable throwable, Class<X> declaredType) throws X
    {
        if (throwable != null && declaredType.isInstance(throwable)) {
            throw declaredType.cast(throwable);
        }
    }
}
