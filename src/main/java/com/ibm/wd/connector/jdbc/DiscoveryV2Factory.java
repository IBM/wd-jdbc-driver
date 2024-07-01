package com.ibm.wd.connector.jdbc;

import static com.ibm.wd.connector.jdbc.WDProperties.WD_PASSWORD;
import static com.ibm.wd.connector.jdbc.WDProperties.WD_USER;
import static com.ibm.wd.connector.jdbc.WDProperties.WD_IAM_API;
import static com.ibm.wd.connector.jdbc.model.WDConstants.USERNAME_BEARER;
import static com.ibm.wd.connector.jdbc.model.WDConstants.USERNAME_IAMAPIKEY;

import com.ibm.cloud.sdk.core.http.HttpConfigOptions;
import com.ibm.cloud.sdk.core.security.Authenticator;
import com.ibm.cloud.sdk.core.security.BearerTokenAuthenticator;
import com.ibm.cloud.sdk.core.security.IamAuthenticator;
import com.ibm.watson.discovery.v2.Discovery;

import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.util.Properties;

public class DiscoveryV2Factory {

    private String serviceUrl;
    private Properties properties;

    public DiscoveryV2Factory(String serviceUrl) {
        setServiceUrl(serviceUrl);
    }

    public void setServiceUrl(String url) {
        if (!url.startsWith("https://")) {
            url = "https://" + url;
        }
        this.serviceUrl = url;
    }

    public Discovery create(Properties properties) throws SQLException {
        if (StringUtils.isEmpty(serviceUrl)) {
            throw new SQLException("Watson Discovery service URL is empty.");
        }

        // parse user credentials
        String user = WD_USER.get(properties);
        final String password = WD_PASSWORD.get(properties);
        if (StringUtils.isEmpty(user) || StringUtils.isEmpty(password)) {
            throw new SQLException(String.format("User credential is not set. user=%s", user));
        }
        user = user.toLowerCase();
        if (!user.equals(USERNAME_IAMAPIKEY) && !user.equals(USERNAME_BEARER)) {
            throw new SQLException("Invalid user name. Specify iamapikey for IAM auth, "
                    + "bearer for standard Bearer auth. Specified user="
                    + user);
        }

        Authenticator authenticator = user.equals(USERNAME_BEARER)
                ? new BearerTokenAuthenticator(password)
                : new IamAuthenticator.Builder()
                        .url(WD_IAM_API.get(properties))
                        .apikey(password)
                        .build();

        Discovery service = new Discovery("2020-08-30", authenticator);
        service.setServiceUrl(serviceUrl);
        // Disable SSL validation
        HttpConfigOptions httpConfigOptions =
                new HttpConfigOptions.Builder().disableSslVerification(true).build();
        service.configureClient(httpConfigOptions);

        return service;
    }
}
