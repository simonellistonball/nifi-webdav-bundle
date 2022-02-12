package org.apache.nifi.processors.webdav.MockSardine;

import com.github.sardine.*;
import com.github.sardine.report.SardineReport;
import org.w3c.dom.Element;

import javax.xml.namespace.QName;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MockSardine implements Sardine {
    List<DavResource> davResources;
    public int putCount = 0;
    public int deleteCount=0;

    public void addResource(MockDavResource resource) {
        davResources.add(resource);
    }

    public MockSardine() {
        davResources = new ArrayList<>();
    }

    @Override
    public void setCredentials(String s, String s1) {

    }

    @Override
    public void setCredentials(String s, String s1, String s2, String s3) {

    }

    @Override
    @Deprecated
    public List<DavResource> getResources(String s) {
        return null;
    }

    @Override
    public List<DavResource> list(String s) {
        return davResources;
    }

    @Override
    public List<DavResource> list(String s, int i) {
        return davResources;
    }

    @Override
    public List<DavResource> list(String s, int i, Set<QName> set) {
        return davResources;
    }

    @Override
    public List<DavResource> list(String s, int i, boolean b) {
        return davResources;
    }

    @Override
    public List<DavResource> propfind(String s, int i, Set<QName> set) {
        return null;
    }

    @Override
    public <T> T report(String s, int i, SardineReport<T> sardineReport) {
        return null;
    }

    @Override
    public List<DavResource> search(String s, String s1, String s2) {
        return null;
    }

    @Override
    @Deprecated
    public void setCustomProps(String s, Map<String, String> map, List<String> list) {

    }

    @Override
    public List<DavResource> patch(String s, Map<QName, String> map) {
        return null;
    }

    @Override
    public List<DavResource> patch(String s, Map<QName, String> map, List<QName> list) {
        return null;
    }

    @Override
    public List<DavResource> patch(String s, List<Element> list, List<QName> list1) {
        return null;
    }

    @Override
    public InputStream get(String s) {
        return new ByteArrayInputStream("test".getBytes());
    }

    @Override
    public InputStream get(String s, Map<String, String> map) {
        return new ByteArrayInputStream("test".getBytes());
    }

    @Override
    public void put(String s, byte[] bytes) {
        this.putCount++;

    }

    @Override
    public void put(String s, InputStream inputStream) {
        this.putCount++;
    }

    @Override
    public void put(String s, byte[] bytes, String s1) {
        this.putCount++;
    }

    @Override
    public void put(String s, InputStream inputStream, String s1) {
        this.putCount++;
    }

    @Override
    public void put(String s, InputStream inputStream, String s1, boolean b) {
        this.putCount++;
    }

    @Override
    public void put(String s, InputStream inputStream, String s1, boolean b, long l) {
        this.putCount++;
    }

    @Override
    public void put(String s, InputStream inputStream, Map<String, String> map) {
        this.putCount++;
    }

    @Override
    public void put(String s, File file, String s1) {
        this.putCount++;
    }

    @Override
    public void put(String s, File file, String s1, boolean b) {
        this.putCount++;
    }

    @Override
    public void delete(String s) {
        this.deleteCount++;
    }

    @Override
    public void createDirectory(String s) {

    }

    @Override
    public void move(String s, String s1) {

    }

    @Override
    public void move(String s, String s1, boolean b) {

    }

    @Override
    public void copy(String s, String s1) {

    }

    @Override
    public void copy(String s, String s1, boolean b) {

    }

    @Override
    public boolean exists(String s) {
        return false;
    }

    @Override
    public String lock(String s) {
        return null;
    }

    @Override
    public String refreshLock(String s, String s1, String s2) {
        return null;
    }

    @Override
    public void unlock(String s, String s1) {

    }

    @Override
    public DavAcl getAcl(String s) {
        return null;
    }

    @Override
    public DavQuota getQuota(String s) {
        return null;
    }

    @Override
    public void setAcl(String s, List<DavAce> list) {

    }

    @Override
    public List<DavPrincipal> getPrincipals(String s) {
        return null;
    }

    @Override
    public List<String> getPrincipalCollectionSet(String s) {
        return null;
    }

    @Override
    public void enableCompression() {

    }

    @Override
    public void disableCompression() {

    }

    @Override
    public void ignoreCookies() {

    }

    @Override
    public void enablePreemptiveAuthentication(String s) {

    }

    @Override
    public void enablePreemptiveAuthentication(URL url) {

    }

    @Override
    public void enablePreemptiveAuthentication(String s, int i, int i1) {

    }

    @Override
    public void disablePreemptiveAuthentication() {

    }

    @Override
    public void shutdown() {

    }
}
