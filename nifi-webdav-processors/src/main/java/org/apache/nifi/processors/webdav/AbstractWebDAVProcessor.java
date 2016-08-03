package org.apache.nifi.processors.webdav;

import java.io.File;
import java.io.IOException;
import java.net.ProxySelector;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.net.ssl.SSLContext;

import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;

import com.github.sardine.Sardine;
import com.github.sardine.SardineFactory;
import com.github.sardine.impl.SardineImpl;

public abstract class AbstractWebDAVProcessor extends AbstractProcessor {

    public static final Relationship RELATIONSHIP_SUCCESS = new Relationship.Builder().name("success").description("Relationship for successfully received FlowFiles").build();
    public static final Relationship RELATIONSHIP_FAILURE = new Relationship.Builder().name("failure").description("Relationship for failed FlowFiles").build();
    
    public static final PropertyDescriptor URL = new PropertyDescriptor.Builder().name("URL")
            .description("A resource URL on a WebDAV server").required(true).expressionLanguageSupported(true).build();
    
    
    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder().name("SSL Context Service")
            .description("The Controller Service to use in order to obtain an SSL Context").required(false).identifiesControllerService(SSLContextService.class).build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder().name("Username").description("Username").addValidator(StandardValidators.NON_EMPTY_VALIDATOR).required(false).expressionLanguageSupported(true)
            .build();
    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder().name("Password").description("Password for the user account").addValidator(Validator.VALID).required(false).expressionLanguageSupported(true)
            .sensitive(true).build();

    public static final PropertyDescriptor PROXY_HOST = new PropertyDescriptor.Builder().name("Proxy Host").description("The fully qualified hostname or IP address of the proxy server")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(true).build();
    public static final PropertyDescriptor PROXY_PORT = new PropertyDescriptor.Builder().name("Proxy Port").description("The port of the proxy server").addValidator(StandardValidators.PORT_VALIDATOR)
            .expressionLanguageSupported(true).build();
    public static final PropertyDescriptor HTTP_PROXY_USERNAME = new PropertyDescriptor.Builder().name("Http Proxy Username").description("Http Proxy Username")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).required(false).expressionLanguageSupported(true).build();
    public static final PropertyDescriptor HTTP_PROXY_PASSWORD = new PropertyDescriptor.Builder().name("Http Proxy Password").description("Http Proxy Password")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).required(false).expressionLanguageSupported(true).sensitive(true).build();

    
    private final static List<PropertyDescriptor> properties;
    private final static Set<Relationship> relationships;
    
    static{
        final List<PropertyDescriptor> _properties = new ArrayList<>();
        _properties.add(URL);
        _properties.add(SSL_CONTEXT_SERVICE);
        _properties.add(USERNAME);
        _properties.add(PASSWORD);
        
        _properties.add(PROXY_HOST);
        _properties.add(PROXY_PORT);
        _properties.add(HTTP_PROXY_USERNAME);
        _properties.add(HTTP_PROXY_PASSWORD);
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
    
    
    protected Sardine buildSardine(ProcessContext context) throws GeneralSecurityException, IOException {
        // get auth properties
        ProxySelector proxySelector = createProxy(context);
        String password = context.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();
        String user = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();

        // using SSL?
        SSLContextService sslContextService = (SSLContextService) context.getProperty(SSL_CONTEXT_SERVICE).asControllerService();

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

            return new SardineImpl(user, password, proxySelector) {
                @Override
                protected ConnectionSocketFactory createDefaultSecureSocketFactory() {
                    return new SSLConnectionSocketFactory(sslContext);
                }
            };
        }
        return SardineFactory.begin(user, password, proxySelector);
    }

    @SuppressWarnings("unused")
    private ProxySelector createProxy(ProcessContext context) {
        String proxyHost = context.getProperty(PROXY_HOST).evaluateAttributeExpressions().getValue();
        Integer proxtPort = context.getProperty(PROXY_PORT).evaluateAttributeExpressions().asInteger();

        String proxyUser = context.getProperty(HTTP_PROXY_USERNAME).evaluateAttributeExpressions().getValue();
        String proxyPass = context.getProperty(HTTP_PROXY_USERNAME).evaluateAttributeExpressions().getValue();

        // TODO - actually add proxy support
        return ProxySelector.getDefault();
    }

}
