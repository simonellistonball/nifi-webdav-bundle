package org.apache.nifi.processors.webdav;

import static org.mockito.Mockito.mock;

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AbstractWebDAVTest {

    @Before
    public void init() {

    }

    @Test
    public void domainStringProcessing() {
        AbstractWebDAVProcessor processors = new AbstractWebDAVProcessor() {

            @Override
            public void onTrigger(ProcessContext arg0, ProcessSession arg1) throws ProcessException {
                // TODO Auto-generated method stub
                
            }
            
        };
            
        String domain = processors.domain("domain\\user");
        String username = processors.userpart("domain\\user");

        Assert.assertEquals("domain", domain);
        Assert.assertEquals("user", username);
    }

}
