package org.apache.nifi.processors.webdav;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.net.ssl.SSLContext;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.NTCredentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.ProxyAuthenticationStrategy;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.NiFiProperties;

import com.github.sardine.Sardine;
import com.github.sardine.impl.SardineImpl;

public abstract class AbstractWebDAVProcessor extends AbstractProcessor {

    public static final Relationship RELATIONSHIP_SUCCESS = new Relationship.Builder().name("success").description("Relationship for successfully received FlowFiles").build();
    public static final Relationship RELATIONSHIP_FAILURE = new Relationship.Builder().name("failure").description("Relationship for failed FlowFiles").build();

    public static final PropertyDescriptor URL = new PropertyDescriptor.Builder().name("URL").description("A resource URL on a WebDAV server").required(true).expressionLanguageSupported(true).build();

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder().name("SSL Context Service")
            .description("The Controller Service to use in order to obtain an SSL Context").required(false).identifiesControllerService(SSLContextService.class).build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder().name("Username").description("Username").addValidator(StandardValidators.NON_EMPTY_VALIDATOR).required(false)
            .expressionLanguageSupported(true).build();
    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder().name("Password").description("Password for the user account").addValidator(Validator.VALID).required(false)
            .expressionLanguageSupported(true).sensitive(true).build();
    public static final PropertyDescriptor NTLM_AUTH = new PropertyDescriptor.Builder().name("NTLM Authentication").description("Use NTLM authentication")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR).required(false).expressionLanguageSupported(true).allowableValues("true", "false").defaultValue("false").build();

    public static final PropertyDescriptor PROXY_HOST = new PropertyDescriptor.Builder().name("Proxy Host").description("The fully qualified hostname or IP address of the proxy server")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(true).build();
    public static final PropertyDescriptor PROXY_PORT = new PropertyDescriptor.Builder().name("Proxy Port").description("The port of the proxy server").addValidator(StandardValidators.PORT_VALIDATOR)
            .expressionLanguageSupported(true).build();
    public static final PropertyDescriptor HTTP_PROXY_USERNAME = new PropertyDescriptor.Builder().name("Http Proxy Username").description("Http Proxy Username")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).required(false).expressionLanguageSupported(true).build();
    public static final PropertyDescriptor HTTP_PROXY_PASSWORD = new PropertyDescriptor.Builder().name("Http Proxy Password").description("Http Proxy Password")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).required(false).expressionLanguageSupported(true).sensitive(true).build();
    public static final PropertyDescriptor NTLM_PROXY_AUTH = new PropertyDescriptor.Builder().name("Proxy NTLM Authentication").description("Use NTLM authentication for proxy connection")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR).required(false).expressionLanguageSupported(true).allowableValues("true", "false").defaultValue("false").build();

    private final static List<PropertyDescriptor> properties;
    private final static Set<Relationship> relationships;

    // need to expose the credentials provider for NTLM
    protected final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

    static {
        final List<PropertyDescriptor> _properties = new ArrayList<>();
        _properties.add(URL);
        
        _properties.add(SSL_CONTEXT_SERVICE);
        _properties.add(USERNAME);
        _properties.add(PASSWORD);
        _properties.add(NTLM_AUTH);
        
        _properties.add(PROXY_HOST);
        _properties.add(PROXY_PORT);
        _properties.add(HTTP_PROXY_USERNAME);
        _properties.add(HTTP_PROXY_PASSWORD);
        _properties.add(NTLM_PROXY_AUTH);
        
        properties = Collections.unmodifiableList(_properties);

        final Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(RELATIONSHIP_SUCCESS);
        _relationships.add(RELATIONSHIP_FAILURE);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    private HttpClientBuilder clientBuilder;

    @OnScheduled
    protected void init(ProcessContext context) throws GeneralSecurityException, IOException {
        clientBuilder = HttpClientBuilder.create();

        // if we're using NTLM we have to do this by flow file because it required the hostname for the resource.
        if (!context.getProperty(NTLM_AUTH).asBoolean()) {
            String username = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
            String password = context.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
        }

        clientBuilder.useSystemProperties();
        clientBuilder.setDefaultCredentialsProvider(credentialsProvider);

        // add proxy bits
        String proxyHost = context.getProperty(PROXY_HOST).evaluateAttributeExpressions().getValue();
        if (proxyHost != null) {
            Integer proxyPort = context.getProperty(PROXY_PORT).evaluateAttributeExpressions().asInteger();
            HttpHost proxyHttpHost = new HttpHost(proxyHost, proxyPort);

            String proxyUser = context.getProperty(HTTP_PROXY_USERNAME).evaluateAttributeExpressions().getValue();
            String proxyPass = context.getProperty(HTTP_PROXY_USERNAME).evaluateAttributeExpressions().getValue();

            if (proxyUser != null) {
                clientBuilder.setProxyAuthenticationStrategy(new ProxyAuthenticationStrategy());
                if (context.getProperty(NTLM_PROXY_AUTH).asBoolean()) {
                    NTCredentials proxyCreds = new NTCredentials(proxyUser, proxyPass, InetAddress.getLocalHost().getHostName(), domain(proxyHost));
                    credentialsProvider.setCredentials(new AuthScope(proxyHttpHost), proxyCreds);
                } else {
                    credentialsProvider.setCredentials(new AuthScope(proxyHttpHost), new UsernamePasswordCredentials(proxyUser, proxyPass));
                }
            }
            clientBuilder.setProxy(proxyHttpHost);
        }

        // using SSL?
        SSLContextService sslContextService = (SSLContextService) context.getProperty(SSL_CONTEXT_SERVICE).asControllerService();

        // add ssl support
        if (sslContextService != null) {
            final boolean needClientAuth = sslContextService == null ? false : sslContextService.getTrustStoreFile() != null;
            final String keystorePath = sslContextService == null ? null : sslContextService.getKeyStoreFile();
            final String keystorePassword = sslContextService.getKeyStorePassword();

            SSLContextBuilder sslContextBuilder = new SSLContextBuilder();
            if (keystorePath != null) {
                sslContextBuilder.loadKeyMaterial(new File(keystorePath), keystorePassword.toCharArray(), keystorePassword.toCharArray());
            }
            if (needClientAuth) {
                sslContextBuilder.loadTrustMaterial(new File(sslContextService.getTrustStoreFile()), sslContextService.getTrustStorePassword().toCharArray());
            }
            final SSLContext sslContext = sslContextBuilder.build();

            clientBuilder.setSslcontext(sslContext);
        }
    }

    protected Sardine buildSardine(ProcessContext context) {
        return new SardineImpl(clientBuilder);
    }

    protected final String workstation = workstation();

    protected static String workstation() {
        String res;
        try {
            res = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            NiFiProperties properties = NiFiProperties.getInstance();
            res = properties.getProperty(NiFiProperties.WEB_HTTP_HOST);
            if (res == null) {
                res = "localhost";
            }
        }
        return res;
    }

    protected String domain(String hostname) {
        return hostname.substring(hostname.indexOf("."));
    }

    /**
     * Adds authentication credentials
     * 
     * If the authentication is NTLM adds those credentials to a scope for the destination, otherwise no need, since credentials will already be in the provider from the scheduled method.
     * 
     * @param context
     * @param url
     * @throws URISyntaxException
     */
    protected void addAuth(ProcessContext context, String url) {
        if (context.getProperty(NTLM_AUTH).asBoolean()) {
            URI uri;
            try {
                uri = new URI(url);
                String username = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
                String password = context.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();
                String domain = domain(uri.getHost());
                credentialsProvider.setCredentials(new AuthScope(new HttpHost(uri.getHost(), uri.getPort())), new NTCredentials(username, password, workstation, domain));
            } catch (URISyntaxException e) {
                getLogger().warn("Invalid URL for authentication, webdav will probably fail", e);
            }
        }
    }
}
