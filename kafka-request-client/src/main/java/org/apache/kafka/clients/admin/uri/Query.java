package org.apache.kafka.clients.admin.uri;

import org.apache.commons.lang3.tuple.Pair;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.Arrays.stream;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

public class Query {

    private Map<String, String> params;

    public Query() {
        this(new HashMap<>());
    }

    public Query(Map<String, String> params) {
        this.params = requireNonNull(params);
    }

    public static Query parse(String queryString) {
        return new Query(stream(queryString.split("&")).map(Query::parseEntry).collect(toMap(Pair::getLeft, Pair::getRight)));
    }

    private static Pair<String, String> parseEntry(String entryString) {
        String[] entryArray = entryString.split("=");
        return Pair.of(entryArray[0], entryArray.length>1 ? entryArray[1] : null);
    }

    public boolean isDefined() {
        return !params.isEmpty();
    }

    public Map<String, String> params() {
        return unmodifiableMap(params);
    }

    public Optional<String> getValue(String key) {
        return Optional.ofNullable(params.get(key));
    }

    Query with(String key, String value) {
        params.put(requireNonNull(key), requireNonNull(value));
        return this;
    }

    @Override
    public String toString() {
        return params.entrySet().stream()
                .map(entry -> format("%s=%s", entry.getKey(), entry.getValue()))
                .collect(joining("&"));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Query query = (Query) o;
        return Objects.equals(params, query.params);
    }

    @Override
    public int hashCode() {
        return Objects.hash(params);
    }
}
