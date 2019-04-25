package de.ux.kafka.clients.admin.uri;

import java.util.Objects;
import java.util.Optional;

import static java.util.Optional.ofNullable;

public class Scheme {

    private final String name;

    public Scheme() {
        this(null);
    }

    public Scheme(String name) {
        this.name = name;
    }

    public boolean isDefined() {
        return name!=null;
    }

    public Optional<String> name() {
        return ofNullable(name);
    }

    @Override
    public String toString() {
        return name!=null ? name : "";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Scheme scheme = (Scheme) o;
        return Objects.equals(name, scheme.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

}
