package org.apache.kafka.clients.admin.mapper;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.util.Arrays;

import static java.lang.String.format;

public enum Mapper {

    YAML(new YAMLFactory()),
    JSON(new JsonFactory());

    private final ObjectMapper objectMapper;

    Mapper(JsonFactory factory) {
        this.objectMapper = new ObjectMapper(factory);
        this.objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
    }

    public String map(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new MappingException(e.getMessage(), e);
        }
    }

    public static Mapper mapperByFormat(String format) {
        return Arrays.stream(values())
                .filter(mapper -> mapper.name().equalsIgnoreCase(format))
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException(format("Unsupported format: %s", format)));
    }
}
