package org.apache.kafka.clients.admin.metadata;

import org.apache.kafka.clients.admin.uri.KafkaUri;

import java.util.*;

import static java.util.stream.Collectors.toList;

public class ClusterDescription {

    private KafkaUri controller;
    private Set<KafkaUri> nodes;

    public ClusterDescription() {
        this(null, null);
    }

    public ClusterDescription(KafkaUri controller, Set<KafkaUri> nodes) {
        this.controller = controller;
        this.nodes = nodes;
    }

    public KafkaUri controller() {
        return controller;
    }

    public ClusterDescription withController(KafkaUri controller) {
        this.controller = controller;
        return this;
    }

    public Set<KafkaUri> nodes() {
        return nodes;
    }

    public ClusterDescription withNodes(Set<KafkaUri> nodes) {
        this.nodes = nodes;
        return this;
    }

    public ClusterDescription addNode(KafkaUri node) {
        if(nodes == null) {
            nodes = new HashSet<>();
        }
        nodes.add(node);
        return this;
    }

    public Map<String, Object> toMap()  {
        Map<String, Object> map = new HashMap<>();
        if(controller != null) {
            map.put("controller", controller.toString());
        }
        if (nodes != null) {
            map.put("nodes", nodes.stream().map(KafkaUri::toString).collect(toList()));
        }
        return map;
    }

    @Override
    public String toString() {
        return "ClusterDescription{" +
                "controller=" + controller +
                ", nodes=" + nodes +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterDescription that = (ClusterDescription) o;
        return Objects.equals(controller, that.controller) &&
                Objects.equals(nodes, that.nodes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(controller, nodes);
    }
}
