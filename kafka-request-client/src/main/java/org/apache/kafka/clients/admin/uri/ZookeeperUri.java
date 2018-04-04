package org.apache.kafka.clients.admin.uri;

import java.net.URI;
import java.net.URISyntaxException;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ZookeeperUri extends Uri {

    public static final Scheme SCHEME = new Scheme("zk");

    private static final String DEFAULT_ROOT_PATH = "/";
    private static final int DEFAULT_PORT = 2181;

    public ZookeeperUri(URI uri) {
        super(validate(uri));
    }

    private static URI validate(URI uri) {
        if(SCHEME.name().isPresent() && !SCHEME.name().get().equals(uri.getScheme())) {
            throw new UriSyntaxException(format("Not a valid Zookeeper uri. Requires scheme '%s', but was '%s'.", SCHEME.name(), uri.getScheme()));
        }
        if(uri.getHost() == null) {
            throw new UriSyntaxException("Not a valid Zookeeper uri. Requires a host name.");
        }
        if(uri.getPath() != null && !uri.getPath().isEmpty()  && !uri.getPath().startsWith("/")) {
            throw new UriSyntaxException(format("Not a valid Zookeeper uri. Requires a absolute root path, but was '%s'.", uri.getPath()));
        }
        return uri;
    }

    public ZookeeperUri(String host, Integer port) {
        this(host, port, null);
    }

    public ZookeeperUri(String host, Integer port, String rootPath) {
        super(SCHEME, new Authority(requireNonNull(host), port), new Path(rootPath), new Query());
    }

    public static ZookeeperUri parse(String uriString) throws UriSyntaxException {
        try {
            return new ZookeeperUri(new URI(uriString));
        } catch (URISyntaxException e) {
            throw new UriSyntaxException(e.getMessage(), e);
        }
    }

    public String host() {
        return authority().host().get();
    }

    public Integer port() {
        return authority().port().orElse(DEFAULT_PORT);
    }

    public String rootPath() {
        return path().name().orElse(DEFAULT_ROOT_PATH);
    }
}
