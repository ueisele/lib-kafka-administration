package de.ux.kafka.clients.admin.metadata;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class ErrorDescription {

    private Integer id;
    private String name;
    private String exceptionName;
    private String message;

    public ErrorDescription() {
        this(null, null, null, null);
    }

    public ErrorDescription(Integer id, String name, String exceptionName, String message) {
        this.id = id;
        this.name = name;
        this.exceptionName = exceptionName;
        this.message = message;
    }

    public Integer id() {
        return id;
    }

    public ErrorDescription withId(Integer id) {
        this.id = id;
        return this;
    }

    public String name() {
        return name;
    }

    public ErrorDescription withName(String name) {
        this.name = name;
        return this;
    }

    public String exceptionName() {
        return exceptionName;
    }

    public ErrorDescription withExceptionName(String exceptionName) {
        this.exceptionName = exceptionName;
        return this;
    }

    public String message() {
        return message;
    }

    public ErrorDescription withMessage(String message) {
        this.message = message;
        return this;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        if(id != null) {
            map.put("id", id);
        }
        if(name != null) {
            map.put("name", name);
        }
        if(exceptionName != null) {
            map.put("exceptionName", name);
        }
        if(message != null) {
            map.put("message", message);
        }
        return map;
    }

    @Override
    public String toString() {
        return "ErrorDescription{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", exceptionName='" + exceptionName + '\'' +
                ", message='" + message + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ErrorDescription that = (ErrorDescription) o;
        return Objects.equals(id, that.id) &&
                Objects.equals(name, that.name) &&
                Objects.equals(exceptionName, that.exceptionName) &&
                Objects.equals(message, that.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, exceptionName, message);
    }
}
