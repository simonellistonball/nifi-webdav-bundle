package org.apache.nifi.processors.webdav;

import com.github.sardine.Sardine;
import com.github.sardine.impl.SardineImpl;
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
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxySpec;
import org.apache.nifi.ssl.SSLContextService;

import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.IOException;
import java.net.*;
import java.security.*;
import java.security.cert.CertificateException;
import java.util.List;
import java.util.Set;

import static org.apache.commons.lang3.StringUtils.trimToEmpty;

public abstract class AbstractWebDAVProcessor extends AbstractProcessor {
    protected final static AllowableValue TRUE_VALUE= new AllowableValue("true","true","True boolean value");
    protected final static AllowableValue FALSE_VALUE= new AllowableValue("false","false","False boolean value");
    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("Relationship for successfully received FlowFiles").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description("Relationship for failed FlowFiles").build();

    public static final PropertyDescriptor URL = new PropertyDescriptor.Builder()
            .name("URL")
            .description("A resource URL on a WebDAV server")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("The Controller Service to use in order to obtain an SSL Context")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .description("Username")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();
    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("Password for the user account")
            .addValidator(Validator.VALID)
            .required(false)
            .sensitive(true)
            .build();
    public static final PropertyDescriptor NTLM_AUTH = new PropertyDescriptor.Builder()
            .name("NTLM Authentication")
            .description("Use NTLM authentication")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .required(false)
            .allowableValues(TRUE_VALUE,FALSE_VALUE)
            .defaultValue("false")
            .build();
    private static final ProxySpec[] PROXY_SPECS = {ProxySpec.HTTP_AUTH};
    public static final PropertyDescriptor PROXY_CONFIGURATION_SERVICE
            = ProxyConfiguration.createProxyConfigPropertyDescriptor(true, PROXY_SPECS);

    public static final PropertyDescriptor PROXY_HOST = new PropertyDescriptor.Builder()
            .name("Proxy Host")
            .description("The fully qualified hostname or IP address of the proxy server")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build();
    public static final PropertyDescriptor PROXY_PORT = new PropertyDescriptor.Builder()
            .name("Proxy Port")
            .description("The port of the proxy server")
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor HTTP_PROXY_USERNAME = new PropertyDescriptor.Builder()
            .name("Http Proxy Username")
            .description("Http Proxy Username")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build();
    public static final PropertyDescriptor HTTP_PROXY_PASSWORD = new PropertyDescriptor.Builder()
            .name("Http Proxy Password")
            .description("Http Proxy Password")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .sensitive(true).build();
    public static final PropertyDescriptor NTLM_PROXY_AUTH = new PropertyDescriptor.Builder()
            .name("Proxy NTLM Authentication")
            .description("Use NTLM authentication for proxy connection")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .required(false)
            .allowableValues(TRUE_VALUE,FALSE_VALUE)
            .defaultValue("false")
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {

        properties = List.of(URL, SSL_CONTEXT_SERVICE, USERNAME, PASSWORD, NTLM_AUTH, PROXY_CONFIGURATION_SERVICE,
                PROXY_HOST, PROXY_PORT, HTTP_PROXY_USERNAME, HTTP_PROXY_PASSWORD, NTLM_PROXY_AUTH);

        relationships = Set.of(REL_SUCCESS, REL_FAILURE);
    }

    // need to expose the credentials provider for NTLM
    protected final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    protected final String workstation = workstation();
    private HttpClientBuilder clientBuilder;

    protected static String workstation() {
        String res;
        try {
            res = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            res = "localhost";
        }
        return res;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnScheduled
    public void onSchedule(ProcessContext context) throws GeneralSecurityException, IOException {
        createClientBuilder(context);
    }

    protected void createClientBuilder(ProcessContext context) throws CertificateException, UnrecoverableKeyException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException, IOException {
        clientBuilder = HttpClientBuilder.create();
        clientBuilder.useSystemProperties();
        configureNTLM(context);
        configureProxy(context, getProxyConfig(context));
        configureSSL(context);

    }

    private void configureSSL(ProcessContext context) throws UnrecoverableKeyException, CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException, KeyManagementException {
        // using SSL?
        SSLContextService sslContextService = (SSLContextService) context.getProperty(SSL_CONTEXT_SERVICE).asControllerService();

        // add ssl support
        if (sslContextService != null) {
            final boolean needClientAuth = sslContextService.getTrustStoreFile() != null;
            final String keystorePath = sslContextService.getKeyStoreFile();
            final String keystorePassword = sslContextService.getKeyStorePassword();

            SSLContextBuilder sslContextBuilder = new SSLContextBuilder();
            if (keystorePath != null) {
                sslContextBuilder.loadKeyMaterial(new File(keystorePath), keystorePassword.toCharArray(), keystorePassword.toCharArray());
            }
            if (needClientAuth) {
                sslContextBuilder.loadTrustMaterial(new File(sslContextService.getTrustStoreFile()), sslContextService.getTrustStorePassword().toCharArray());
            }
            final SSLContext sslContext = sslContextBuilder.build();

            clientBuilder.setSSLContext(sslContext);
        }
    }

    private void configureProxy(ProcessContext context, ProxyConfiguration proxyConfig) throws UnknownHostException {
        if (proxyConfig.getProxyType() != Proxy.Type.DIRECT) {
            HttpHost proxyHost = new HttpHost(proxyConfig.getProxyServerHost(), proxyConfig.getProxyServerPort());
            if (proxyConfig.hasCredential()) {
                clientBuilder.setProxyAuthenticationStrategy(new ProxyAuthenticationStrategy());
                if (context.getProperty(NTLM_PROXY_AUTH).asBoolean()) {
                    NTCredentials proxyCreds = new NTCredentials(proxyConfig.getProxyUserName(), proxyConfig.getProxyUserPassword(),
                            InetAddress.getLocalHost().getHostName(), domain(proxyConfig.getProxyServerHost()));
                    credentialsProvider.setCredentials(new AuthScope(proxyHost), proxyCreds);
                } else {
                    credentialsProvider.setCredentials(new AuthScope(proxyHost),
                            new UsernamePasswordCredentials(proxyConfig.getProxyUserName(),
                                    proxyConfig.getProxyUserPassword()));
                }
            }
            clientBuilder.setProxy(proxyHost);
        }
    }

    private ProxyConfiguration getProxyConfig(ProcessContext context) {
        return ProxyConfiguration.getConfiguration(context, () -> {
            final ProxyConfiguration componentProxyConfig = new ProxyConfiguration();
            final String proxyHost = context.getProperty(PROXY_HOST).evaluateAttributeExpressions().getValue();
            final Integer proxyPort = context.getProperty(PROXY_PORT).evaluateAttributeExpressions().asInteger();
            if (proxyHost != null && proxyPort != null) {
                componentProxyConfig.setProxyType(Proxy.Type.HTTP);
                componentProxyConfig.setProxyServerHost(proxyHost);
                componentProxyConfig.setProxyServerPort(proxyPort);
                final String proxyUsername = trimToEmpty(context.getProperty(HTTP_PROXY_USERNAME).evaluateAttributeExpressions().getValue());
                final String proxyPassword = context.getProperty(HTTP_PROXY_PASSWORD).evaluateAttributeExpressions().getValue();
                componentProxyConfig.setProxyUserName(proxyUsername);
                componentProxyConfig.setProxyUserPassword(proxyPassword);
            }
            return componentProxyConfig;
        });
    }

    protected void configureNTLM(ProcessContext context) {
        // if we're using NTLM we have to do this by flow file because it required the hostname for the resource.
        if (!context.getProperty(NTLM_AUTH).asBoolean()) {
            String username = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
            String password = context.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
        }
        clientBuilder.setDefaultCredentialsProvider(credentialsProvider);
    }

    protected Sardine buildSardine() {
        return new SardineImpl(clientBuilder);
    }

    protected String domain(String hostname) {
        return hostname.substring(hostname.indexOf("."));
    }

    /**
     * Adds authentication credentials
     * <p>
     * If the authentication is NTLM adds those credentials to a scope for the destination, otherwise no need, since credentials will already be in the provider from the scheduled method.
     *
     * @param context Process Context
     * @param url     url of address
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
