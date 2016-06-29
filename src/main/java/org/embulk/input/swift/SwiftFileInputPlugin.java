package org.embulk.input.swift;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import org.embulk.config.*;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.TransactionalFileInput;
import org.embulk.spi.util.InputStreamFileInput;
import org.embulk.spi.util.ResumableInputStream;
import org.embulk.spi.util.RetryExecutor;
import org.javaswift.joss.client.factory.AccountConfig;
import org.javaswift.joss.client.factory.AccountFactory;
import org.javaswift.joss.client.factory.AuthenticationMethod;
import org.javaswift.joss.headers.object.range.ExcludeStartRange;
import org.javaswift.joss.headers.object.range.LastPartRange;
import org.javaswift.joss.headers.object.range.MidPartRange;
import org.javaswift.joss.instructions.DownloadInstructions;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.StoredObject;
import org.slf4j.Logger;

import static org.embulk.spi.util.RetryExecutor.retryExecutor;

public class SwiftFileInputPlugin
        implements FileInputPlugin {
    public interface PluginTask
            extends FileList.Task, Task {
        @Config("username")
        public String getUsername();

        @Config("password")
        public String getPassword();

        @Config("auth_url")
        public String getAuthUrl();

        @Config("auth_type")
        public String getAuthType();

        @Config("tenant_id")
        @ConfigDefault("null")
        public Optional<String> getTenantId();

        @Config("tenant_name")
        @ConfigDefault("null")
        public Optional<String> getTenantName();

        @Config("container")
        public String getContainer();

        @Config("path_prefix")
        public String getPathPrefix();

        @Config("last_path")
        @ConfigDefault("null")
        public Optional<String> getLastPath();

        @Config("incremental")
        @ConfigDefault("true")
        public boolean getIncremental();

        public FileList getFiles();

        public void setFiles(FileList files);

        @ConfigInject
        public BufferAllocator getBufferAllocator();
    }

    /**
     * Logger
     */
    private static final Logger LOGGER = Exec.getLogger(SwiftFileInputPlugin.class);

    private Account getAccount(PluginTask task) {
        AccountConfig accountConfig = new AccountConfig();

        String auth_type = task.getAuthType();
        accountConfig.setAuthUrl(task.getAuthUrl());
        accountConfig.setUsername(task.getUsername());
        accountConfig.setPassword(task.getPassword());

        Optional<String> tenant_id = task.getTenantId();
        if (tenant_id.isPresent()) {
            accountConfig.setTenantId(tenant_id.get());
        }
        Optional<String> tenant_name = task.getTenantName();
        if (tenant_name.isPresent()) {
            accountConfig.setTenantName(tenant_name.get());
        }

        if (auth_type.equals("keystone")) {
            if (!tenant_id.isPresent() && !tenant_name.isPresent()) {
                throw new ConfigException("if you choose keystone auth, you must specify to either tenant_id or tenant_name.");
            }
            accountConfig.setAuthenticationMethod(AuthenticationMethod.KEYSTONE);
        } else if (auth_type.equals("tempauth")) {
            accountConfig.setAuthenticationMethod(AuthenticationMethod.TEMPAUTH);
        } else if (auth_type.equals("basic")) {
            accountConfig.setAuthenticationMethod(AuthenticationMethod.BASIC);
        } else {
            throw new ConfigException("auth_type has to be either keystone, tempauth or basic.");
        }

        return new AccountFactory(accountConfig).createAccount();
    }

    /**
     * retrieve target objects with specified prefix
     * @param task PluginTsak
     * @return List of Target Objects
     */
    private FileList listFiles(PluginTask task) {
        FileList.Builder builder = new FileList.Builder(task);
        Account account = this.getAccount(task);
        Container container = account.getContainer(task.getContainer());

        // if the container is not exist, cannot input.
        if (container.exists() == false) {
            throw new ConfigException("Container not found");
        }

        String marker = task.getLastPath().orNull();

        do {
            Collection<StoredObject> objectList = container.list(task.getPathPrefix(), marker, 1024);
            if (objectList.size() > 0) {
                for (StoredObject obj : objectList) {
                    if (obj.getContentLength() > 0) {
                        LOGGER.info("add {}", obj.getName());
                        builder.add(obj.getName(), obj.getContentLength());
                        if (!builder.needsMore()) {
                            return builder.build();
                        }
                    }
                    marker = obj.getName();
                }
            } else {
                break;
            }
        } while (marker != null);

        return builder.build();
    }

    @Override
    public ConfigDiff transaction(ConfigSource config, FileInputPlugin.Control control) {
        PluginTask task = config.loadConfig(PluginTask.class);

        //set input files
        task.setFiles(this.listFiles(task));
        int taskCount = task.getFiles().getTaskCount();

        return resume(task.dump(), taskCount, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
                             int taskCount,
                             FileInputPlugin.Control control) {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        //validate
        this.getAccount(task);

        control.run(taskSource, taskCount);

        ConfigDiff configDiff = Exec.newConfigDiff();

        if (task.getIncremental()) {
            configDiff.set("last_path", task.getFiles().getLastPath(task.getLastPath()));
        }

        return configDiff;
    }

    @Override
    public void cleanup(TaskSource taskSource,
                        int taskCount,
                        List<TaskReport> successTaskReports) {
    }

    @Override
    public TransactionalFileInput open(TaskSource taskSource, int taskIndex) {
        final PluginTask task = taskSource.loadTask(PluginTask.class);

        return new SwiftFileInput(task, taskIndex);
    }

    //private static InputStream openInputStream(PluginTask task, String path)
    //{
    //    return new MyInputStream(file);
    //}


    @VisibleForTesting
    static class SwiftInputStreamReopener
            implements ResumableInputStream.Reopener {
        private final Logger LOGGER = Exec.getLogger(SwiftInputStreamReopener.class);

        private final StoredObject obj;

        public SwiftInputStreamReopener(StoredObject obj) {
            this.obj = obj;
        }

        @Override
        public InputStream reopen(final long offset, final Exception closedCause) throws IOException {
            try {
                return retryExecutor()
                        .withRetryLimit(3)
                        .withInitialRetryWait(500)
                        .withMaxRetryWait(30 * 1000)
                        .runInterruptible(new RetryExecutor.Retryable<InputStream>() {
                            @Override
                            public InputStream call() throws InterruptedIOException {
                                LOGGER.warn(String.format("Swift read failed. Retrying GET request with %,d bytes offset", offset), closedCause);
                                return obj.downloadObjectAsInputStream(new DownloadInstructions().setRange(new ExcludeStartRange((int) offset)));
                            }

                            @Override
                            public boolean isRetryableException(Exception exception) {
                                return true;  // TODO
                            }

                            @Override
                            public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait)
                                    throws RetryExecutor.RetryGiveupException {
                                String message = String.format("Swift GET request failed. Retrying %d/%d after %d seconds. Message: %s",
                                        retryCount, retryLimit, retryWait / 1000, exception.getMessage());
                                if (retryCount % 3 == 0) {
                                    LOGGER.warn(message, exception);
                                } else {
                                    LOGGER.warn(message);
                                }
                            }

                            @Override
                            public void onGiveup(Exception firstException, Exception lastException)
                                    throws RetryExecutor.RetryGiveupException {
                            }
                        });
            } catch (RetryExecutor.RetryGiveupException ex) {
                Throwables.propagateIfInstanceOf(ex.getCause(), IOException.class);
                throw Throwables.propagate(ex.getCause());
            } catch (InterruptedException ex) {
                throw new InterruptedIOException();
            }
        }
    }

    public class SwiftFileInput
            extends InputStreamFileInput
            implements TransactionalFileInput {
        public SwiftFileInput(PluginTask task, int taskIndex) {
            super(task.getBufferAllocator(), new SingleFileProvider(task, taskIndex));
        }

        public void abort() {
        }

        public TaskReport commit() {
            return Exec.newTaskReport();
        }

        @Override
        public void close() {
        }
    }

    private class SingleFileProvider
            implements InputStreamFileInput.Provider {
        private Account account;
        private final String containerName;
        private final Iterator<String> iterator;

        public SingleFileProvider(PluginTask task, int taskIndex) {
            this.account = getAccount(task);
            this.containerName = task.getContainer();
            this.iterator = task.getFiles().get(taskIndex).iterator();
        }

        @Override
        public InputStream openNext() throws IOException {
            if (!iterator.hasNext()) {
                return null;
            }
            StoredObject obj = this.account.getContainer(this.containerName).getObject(iterator.next());
            return new ResumableInputStream(obj.downloadObjectAsInputStream(), new SwiftInputStreamReopener(obj));
        }

        @Override
        public void close() {
        }
    }
}
