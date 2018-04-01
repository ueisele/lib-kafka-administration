package org.apache.kafka.clients.admin.metadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ErrorDescription {

    private String name;
    private Integer id;
    private String description;

    public ErrorDescription() {
        this(null, null, null);
    }

    public ErrorDescription(String name, Integer id, String description) {
        this.name = name;
        this.id = id;
        this.description = description;
    }

    public String name() {
        return name;
    }

    public ErrorDescription withName(String name) {
        this.name = name;
        return this;
    }

    public Integer id() {
        return id;
    }

    public ErrorDescription withId(Integer id) {
        this.id = id;
        return this;
    }

    public String description() {
        return description;
    }

    public ErrorDescription withDescription(String description) {
        this.description = description;
        return this;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        if(name != null) {
            map.put("name", name);
        }
        if(id != null) {
            map.put("id", id);
        }
        if(description != null) {
            map.put("description", description);
        }
        return map;
    }

    @Override
    public String toString() {
        return "ErrorDescription{" +
                "name='" + name + '\'' +
                ", id=" + id +
                ", description='" + description + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ErrorDescription that = (ErrorDescription) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(id, that.id) &&
                Objects.equals(description, that.description);
    }

    @Override
    public int hashCode() {

        return Objects.hash(name, id, description);
    }
}
