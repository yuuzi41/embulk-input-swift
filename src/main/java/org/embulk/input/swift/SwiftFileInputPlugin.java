package org.embulk.input.swift;

import static org.embulk.spi.util.RetryExecutor.retryExecutor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.TransactionalFileInput;
import org.embulk.spi.util.InputStreamFileInput;
import org.embulk.spi.util.ResumableInputStream;
import org.embulk.spi.util.RetryExecutor;
import org.javaswift.joss.client.factory.AccountConfig;
import org.javaswift.joss.client.factory.AccountFactory;
import org.javaswift.joss.client.factory.AuthenticationMethod;
import org.javaswift.joss.headers.object.range.ExcludeStartRange;
import org.javaswift.joss.instructions.DownloadInstructions;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.StoredObject;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class SwiftFileInputPlugin
        implements FileInputPlugin {
  public interface PluginTask
      extends FileList.Task, Task {
    @Config("username")
    @ConfigDefault("null")
    public Optional<String> getUsername();

    @Config("password")
    @ConfigDefault("null")
    public Optional<String> getPassword();

    @Config("auth_url")
    @ConfigDefault("null")
    public Optional<String> getAuthUrl();

    @Config("auth_type")
    public String getAuthType();

    @Config("tenant_id")
    @ConfigDefault("null")
    public Optional<String> getTenantId();

    @Config("tenant_name")
    @ConfigDefault("null")
    public Optional<String> getTenantName();

    @Config("endpoint_url")
    @ConfigDefault("null")
    public Optional<String> getEndpointUrl();

    @Config("account")
    @ConfigDefault("null")
    public Optional<String> getAccount();


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
   * Logger.
   */
  private static final Logger LOGGER = Exec.getLogger(SwiftFileInputPlugin.class);

  private Account getAccount(PluginTask task) {
    AccountConfig accountConfig = new AccountConfig();

    final String authType = task.getAuthType();

    Optional<String> authUrl = task.getAuthUrl();
    if (authUrl.isPresent()) {
      accountConfig.setAuthUrl(authUrl.get());
    }

    Optional<String> username = task.getUsername();
    if (username.isPresent()) {
      accountConfig.setUsername(username.get());
    }

    Optional<String> password = task.getPassword();
    if (password.isPresent()) {
      accountConfig.setPassword(password.get());
    }

    Optional<String> tenantId = task.getTenantId();
    if (tenantId.isPresent()) {
      accountConfig.setTenantId(tenantId.get());
    }
    Optional<String> tenantName = task.getTenantName();
    if (tenantName.isPresent()) {
      accountConfig.setTenantName(tenantName.get());
    }

    Optional<String> endpointUrl = task.getEndpointUrl();
    Optional<String> account = task.getAccount();

    switch (authType) {
      case "keystone":
        if (authUrl.isPresent() && username.isPresent() && password.isPresent()) {
          if (tenantId.isPresent() || tenantName.isPresent()) {
            accountConfig.setAuthenticationMethod(AuthenticationMethod.KEYSTONE);
          } else {
            throw new ConfigException("if you choose keystone auth, "
                + "you must specify to either tenant_id or tenant_name.");
          }
        } else {
          throw new ConfigException("if you choose keystone auth, "
              + "you must specify auth_url, username and password.");
        }
        break;
      case "tempauth":
        if (authUrl.isPresent() && username.isPresent() && password.isPresent()) {
          accountConfig.setAuthenticationMethod(AuthenticationMethod.TEMPAUTH);
        } else {
          throw new ConfigException("if you choose tempauth, "
              + "you must specify auth_url, username and password.");
        }
        break;
      case "basic":
        if (authUrl.isPresent() && username.isPresent() && password.isPresent()) {
          accountConfig.setAuthenticationMethod(AuthenticationMethod.BASIC);
        } else {
          throw new ConfigException("if you choose basic auth, "
              + "you must specify auth_url, username and password.");
        }
        break;
      case "noauth":
        if (endpointUrl.isPresent() && account.isPresent()) {
          accountConfig.setAuthenticationMethod(AuthenticationMethod.EXTERNAL);
          accountConfig.setAuthUrl(endpointUrl.get()); // in JOSS, AuthUrl is necessary.(NPE if no AuthUrl)
          accountConfig.setAccessProvider(
              new JossNoauthAccessProvider(endpointUrl.get(), account.get())
          );
        } else {
          throw new ConfigException("if you choose noauth, "
              + "you must specify endpoint_url and account.");
        }
        break;
      default:
        throw new ConfigException("auth_type has to be either "
            + "keystone, tempauth, basic or noauth.");
    }

    return new AccountFactory(accountConfig).createAccount();
  }

  /**
   * retrieve target objects with specified prefix.
   *
   * @param task PluginTask
   * @return List of Target Objects
   */
  private FileList listFiles(PluginTask task) {
    FileList.Builder builder = new FileList.Builder(task);
    Account account = this.getAccount(task);
    Container container = account.getContainer(task.getContainer());

    // if the container is not exist, cannot input.
    if (!container.exists()) {
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
            .runInterruptible(
                new RetryExecutor.Retryable<InputStream>() {
                  @Override
                  public InputStream call() throws InterruptedIOException {
                    LOGGER.warn(
                        String.format(
                            "Swift read failed. Retrying GET request with %,d bytes offset", offset
                        ), closedCause);
                    return obj.downloadObjectAsInputStream(
                        new DownloadInstructions().setRange(new ExcludeStartRange((int) offset))
                    );
                  }

                  @Override
                  public boolean isRetryableException(Exception exception) {
                    return true;  // TODO
                  }

                  @Override
                  public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait)
                      throws RetryExecutor.RetryGiveupException {
                    String message = String.format(
                        "Swift GET request failed. Retrying %d/%d after %d seconds. Message: %s",
                        retryCount, retryLimit, retryWait / 1000, exception.getMessage()
                    );
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

      return new ResumableInputStream(
          obj.downloadObjectAsInputStream(), new SwiftInputStreamReopener(obj)
      );
    }

    @Override
    public void close() {
    }
  }
}
