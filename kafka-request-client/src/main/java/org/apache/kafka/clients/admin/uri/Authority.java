package org.apache.kafka.clients.admin.uri;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;
import java.util.Optional;

import static java.util.Optional.ofNullable;

public class Authority {

    private final String user;
    private final String host;
    private final Integer port;

    private final URI uri;

    public Authority(URI uri) {
        this(uri.getUserInfo(), uri.getHost(), uri.getPort()>=0 ? uri.getPort() : null);
    }

    public Authority() {
        this(null, null, null);
    }

    public Authority(String host) {
        this(host, null, null);
    }

    public Authority(String host, Integer port) {
        this(null, host, port);
    }

    public Authority(String user, String host) {
        this(user, host, null);
    }

    public Authority(String user, String host, Integer port) {
        this.user = user;
        this.host = host;
        this.port = port;
        this.uri = toURI(user, host, port);
    }

    public static Authority parse(String authorityString) throws UriSyntaxException {
        if(authorityString.isEmpty()) {
            return new Authority();
        }
        try {
            return new Authority(new URI("//" + authorityString));
        } catch (URISyntaxException e) {
            throw new UriSyntaxException(e.getMessage(), e);
        }
    }

    public boolean isDefined() {
        return user != null || host !=null || port != null;
    }

    public Optional<String> user() {
        return ofNullable(user);
    }

    public Optional<String> host() {
        return ofNullable(host);
    }

    public Optional<Integer> port() {
        return ofNullable(port);
    }

    public URI toURI() {
        return uri;
    }

    private static URI toURI(String user, String host, Integer port) throws UriSyntaxException {
        try {
            return new URI(null, user, host, port != null ? port : -1, null ,null, null);
        } catch (URISyntaxException e) {
            throw new UriSyntaxException(e.getMessage(), e);
        }
    }

    @Override
    public String toString() {
        return toURI().getAuthority();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Authority authority = (Authority) o;
        return Objects.equals(user, authority.user) &&
                Objects.equals(host, authority.host) &&
                Objects.equals(port, authority.port);
    }

    @Override
    public int hashCode() {
        return Objects.hash(user, host, port);
    }

}
