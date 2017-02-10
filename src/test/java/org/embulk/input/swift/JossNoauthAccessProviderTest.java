package org.embulk.input.swift;

import static org.junit.Assert.*;

import org.javaswift.joss.client.factory.TempUrlHashPrefixSource;
import org.javaswift.joss.model.Access;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test code for JossNoauthAccessProvider.
 *
 * this test code checks "Is JossNoauthAccessProvider generating the expected Access instance?"
 * at some parameters.
 */
public class JossNoauthAccessProviderTest {
  @Test
  public void testAuthenticate() throws Exception {
    final JossNoauthAccessProvider providerWithoutSlash =
        new JossNoauthAccessProvider("http://hoge:8080/v1", "account1");
    final Access accessWithoutSlash = providerWithoutSlash.authenticate();
    assertEquals("http://hoge:8080/v1/account1", accessWithoutSlash.getInternalURL());
    assertEquals("http://hoge:8080/v1/account1", accessWithoutSlash.getPublicURL());
    assertEquals("", accessWithoutSlash.getToken());
    assertEquals("http://hoge:8080/v1/account1",accessWithoutSlash.getTempUrlPrefix(TempUrlHashPrefixSource.ADMIN_URL_PATH));

    final JossNoauthAccessProvider providerWithlash =
        new JossNoauthAccessProvider("http://hoge:8080/v1/", "account1");
    final Access accessWithSlash = providerWithlash.authenticate();
    assertEquals("http://hoge:8080/v1/account1", accessWithSlash.getInternalURL());
    assertEquals("http://hoge:8080/v1/account1", accessWithSlash.getPublicURL());
    assertEquals("", accessWithSlash.getToken());
    assertEquals("http://hoge:8080/v1/account1",accessWithSlash.getTempUrlPrefix(TempUrlHashPrefixSource.ADMIN_URL_PATH));
  }
}
