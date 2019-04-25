package de.ux.kafka.clients.admin.uri;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;

public class Uri {

    protected final Scheme scheme;
    protected final Authority authority;
    protected final Path path;
    protected final Query query;

    protected final URI uri;

    public Uri(URI uri) {
        this(ofNullable(uri.getScheme()).map(Scheme::new).orElse(new Scheme()),
             ofNullable(uri.getAuthority()).map(Authority::parse).orElse(new Authority()),
             ofNullable(uri.getPath()).map(Path::new).orElse(new Path()),
             ofNullable(uri.getQuery()).map(Query::parse).orElse(new Query()));
    }

    public Uri(Scheme scheme, Authority authority, Path path, Query query) throws UriSyntaxException {
        this.scheme = requireNonNull(scheme);
        this.authority = requireNonNull(authority);
        this.path = requireNonNull(path);
        this.query = requireNonNull(query);
        this.uri = toURI(scheme, authority, path, query);
    }

    public static Uri parse(String uriString) throws UriSyntaxException {
        try {
            return new Uri(new URI(uriString));
        } catch (URISyntaxException e) {
            throw new UriSyntaxException(e.getMessage(), e);
        }
    }

    public Scheme schema() {
        return scheme;
    }

    public Authority authority() {
        return authority;
    }

    public Path path() {
        return path;
    }

    public Query query() {
        return query;
    }

    public URI toURI() {
        return uri;
    }

    private static URI toURI(Scheme scheme, Authority authority, Path path, Query query) throws UriSyntaxException {
        try {
            return new URI(
                    scheme.isDefined() ? scheme.toString() : null,
                    authority.isDefined() ? authority.toString(): null,
                    path.isDefined() ? path.toString() : null,
                    query.isDefined() ? query.toString() : null,
                    null);
        } catch (URISyntaxException e) {
            throw new UriSyntaxException(e.getMessage(), e);
        }
    }

    @Override
    public String toString() {
        return toURI().toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Uri uri = (Uri) o;
        return Objects.equals(scheme, uri.scheme) &&
                Objects.equals(authority, uri.authority) &&
                Objects.equals(path, uri.path) &&
                Objects.equals(query, uri.query);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scheme, authority, path, query);
    }

}
