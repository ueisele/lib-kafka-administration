package de.ux.kafka.clients.admin.uri;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;

public class KafkaUri extends Uri {

    private static final Scheme SCHEME = new Scheme("kafka");

    private static final int DEFAULT_PORT = 2181;

    private static final String QUERY_PARAM_ID = "id";
    private static final String QUERY_PARAM_RACK = "rack";

    public KafkaUri(URI uri) {
        super(validate(uri));
    }

    private static URI validate(URI uri) {
        if(SCHEME.name().isPresent() && !SCHEME.name().get().equals(uri.getScheme())) {
            throw new UriSyntaxException(format("Not a valid Kafka uri. Requires scheme '%s', but was '%s'.", SCHEME.name(), uri.getScheme()));
        }
        if(uri.getHost() == null) {
            throw new UriSyntaxException("Not a valid Kafka uri. Requires a host name.");
        }
        return uri;
    }

    public KafkaUri(String host) {
        this(host, null, null, null);
    }

    public KafkaUri(String host, Integer port) {
        this(host, port, null, null);
    }

    public KafkaUri(String host, Integer port, Integer id) {
        this(host, port, id, null);
    }

    public KafkaUri(String host, Integer port, Integer id, String rack) {
        super(SCHEME, new Authority(requireNonNull(host), port), new Path(), buildQuery(id, rack));
    }

    private static Query buildQuery(Integer id, String rack) {
        Query query = new Query();
        ofNullable(id).ifPresent(theId -> query.with(QUERY_PARAM_ID, String.valueOf(theId)));
        ofNullable(rack).ifPresent(theRack -> query.with(QUERY_PARAM_RACK, theRack));
        return query;
    }

    public static KafkaUri parse(String uriString) throws UriSyntaxException {
        try {
            return new KafkaUri(new URI(uriString));
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

    public Optional<Integer> id() {
        return query().getValue(QUERY_PARAM_ID).map(Integer::valueOf);
    }

    public Optional<String> rack() {
        return query().getValue(QUERY_PARAM_RACK);
    }

}
