package org.apache.nifi.processors.webdav.MockSardine;

import com.github.sardine.DavResource;

import javax.xml.namespace.QName;
import java.net.URISyntaxException;
import java.util.*;

public class MockDavResource extends DavResource {
    protected MockDavResource(String href, Date creation, Date modified, String contentType, Long contentLength, String etag,
                              String displayName, List<QName> resourceTypes, String contentLanguage,
                              List<QName> supportedReports, Map<QName, String> customProps) throws URISyntaxException {
        super(href, creation, modified, contentType, contentLength, etag, displayName, resourceTypes, contentLanguage, supportedReports, customProps);
    }
    public MockDavResource(String href,Date created,String contentType,Long contentLength) throws URISyntaxException {
        super(href,created,created,contentType,contentLength,null,null,null,null,null,new HashMap<>());
    }
}
