package org.embulk.input.swift;

import org.javaswift.joss.client.factory.AuthenticationMethod;
import org.javaswift.joss.client.factory.TempUrlHashPrefixSource;
import org.javaswift.joss.command.shared.identity.access.AccessBasic;
import org.javaswift.joss.model.Access;

import java.net.URL;

/**
 * Access Provider for Swift with no authentication.
 */
public class JossNoauthAccessProvider implements AuthenticationMethod.AccessProvider {
  private String url;

  /**
   * constructor.
   *
   * @param endpoint   The Endpoint URL of Swift. e.g. &quot;http://10.0.0.1:8080/v1&quot; .
   * @param account    Account name.
   */
  public JossNoauthAccessProvider(String endpoint, String account) {
    if (endpoint.endsWith("/")) {
      this.url = String.format("%s%s", endpoint, account);
    } else {
      this.url = String.format("%s/%s", endpoint, account);
    }
  }

  @Override
  public Access authenticate() {
    AccessBasic access = new AccessBasic();
    access.setToken("");
    access.setUrl(this.url);

    return access;
  }
}
