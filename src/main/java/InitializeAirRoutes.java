import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.janusgraph.core.*;
import org.janusgraph.core.schema.JanusGraphManagement;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @version 1.0
 * @description InitializeAirRoutes
 * @date 2025/1/10 16:09
 *
 *
 * Notes:
 * Basic concepts:
 * 1. TinkerPop:  graph computing framework
 * 2. JanusGraph: support the processing of graphs so large that they require storage and computational
 * capacities beyond what a single machine can provide.
 * 3. Gremlin: a JanusGraph’s query language used to retrieve data from and modify data in the graph,
 * a component of Apache TinkerPop
 */

public class InitializeAirRoutes {
    public static void main(String[] args) {
//         Create an in-memory JanusGraph
//         InmemoryLoader();
        try (JanusGraph graph = JanusGraphFactory.build().set("storage.backend", "inmemory").open()) {
            Loader(graph);
            query(graph, "Jill");
            query(graph, "Bob");
            query(graph, "Kate");
            query(graph, "Jane");
            query(graph, "Mike");
        }

    }

    public static void Loader(JanusGraph graph) {
        // reference https://docs.janusgraph.org/v1.0/storage-backend/inmemorybackend/

        // Initialize schema
        initializeSchema(graph);
        System.out.println("Schema initialized.");

        // Load sample data
        long startTime = System.currentTimeMillis();
        try {
            loadSampleData(graph);
        } catch (Exception e) {
            System.out.println(e);
        }
        long endTime = System.currentTimeMillis();
        System.out.println("Sample data loaded, takes: " + (endTime - startTime) + "milliseconds");
        long vertexCount = graph.traversal().V().count().next();
        System.out.println("Total vertices: " + vertexCount);
        long edgeCount = graph.traversal().E().count().next();
        System.out.println("Total edges: " + edgeCount);
    }


    public static void initializeSchema(JanusGraph graph) {
        // reference: https://docs.janusgraph.org/v1.0/schema/
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            // read JSON file
            JsonNode schema = objectMapper.readTree(new File("./src/data/schema.json"));
            JanusGraphManagement mgmt = graph.openManagement();

            // create Vertex Labels
            for (JsonNode vertexLabel : schema.get("vertexLabels")) {
                String name = vertexLabel.get("name").asText();
                mgmt.makeVertexLabel(name).make();
            }

            // create Edge Labels
            for (JsonNode edgeLabel : schema.get("edgeLabels")) {
                String name = edgeLabel.get("name").asText();
                // multi: Allows multiple edges of the same label between any pair of vertices.
                Multiplicity multiplicity = Multiplicity.valueOf(edgeLabel.get("multiplicity").asText());
                mgmt.makeEdgeLabel(name).multiplicity(multiplicity).make();
            }

            // create Property Keys
            for (JsonNode propertyKey : schema.get("propertyKeys")) {
                String name = propertyKey.get("name").asText();
                String dataType = propertyKey.get("dataType").asText();
                Cardinality cardinality = Cardinality.valueOf(propertyKey.get("cardinality").asText());
                Class<?> clazz = getClassForDataType(dataType);

                mgmt.makePropertyKey(name).dataType(clazz).cardinality(cardinality).make();
            }
            mgmt.commit();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static Class<?> getClassForDataType(String dataType) {
        // helper function to get data types
        switch (dataType) {
            case "String":
                return String.class;
            case "Double":
                return Double.class;
            case "Integer":
                return Integer.class;
            case "Boolean":
                return Boolean.class;
            default:
                throw new IllegalArgumentException("Unsupported data type: " + dataType);
        }
    }

    public static void loadSampleData(JanusGraph graph) {
        try {
            loadNodes(graph);
            loadEdges(graph);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void loadNodes(JanusGraph graph) {
        try {
            JanusGraphTransaction tx = graph.newTransaction();
            Vertex vertex1 = tx.addVertex("member");
            vertex1.property("userid", "user1");
            vertex1.property("username", "Bob");
            vertex1.property("pw", "password123");
            vertex1.property("firstname", "Bob");
            vertex1.property("lastname", "Doe");
            vertex1.property("gender", "male");
            vertex1.property("dob", "1990-01-01");
            vertex1.property("jdate", "2022-01-01");
            vertex1.property("ldate", "2025-01-01");
            vertex1.property("address", "123 Main St, Los Angeles, CA");
            vertex1.property("email", "john.doe@example.com");
            vertex1.property("tel", "123-456-7890");
            vertex1.property("imageid", "img1");
            vertex1.property("thumbnailid", "thumb1");

            Vertex vertex2 = tx.addVertex("member");
            vertex2.property("userid", "user2");
            vertex2.property("username", "Jill");
            vertex2.property("pw", "password456");
            vertex2.property("firstname", "Jill");
            vertex2.property("lastname", "Doe");
            vertex2.property("gender", "female");
            vertex2.property("dob", "1992-05-10");
            vertex2.property("jdate", "2023-01-01");
            vertex2.property("ldate", "2025-05-10");
            vertex2.property("address", "456 Elm St, Los Angeles, CA");
            vertex2.property("email", "jane.doe@example.com");
            vertex2.property("tel", "987-654-3210");
            vertex2.property("imageid", "img2");
            vertex2.property("thumbnailid", "thumb2");

            Vertex vertex3 = tx.addVertex("member");
            vertex3.property("userid", "user3");
            vertex3.property("username", "Kate");
            vertex3.property("pw", "password789");
            vertex3.property("firstname", "Kate");
            vertex3.property("lastname", "Smith");
            vertex3.property("gender", "male");
            vertex3.property("dob", "1985-07-15");
            vertex3.property("jdate", "2021-07-01");
            vertex3.property("ldate", "2025-07-15");
            vertex3.property("address", "789 Maple St, Los Angeles, CA");
            vertex3.property("email", "alex.smith@example.com");
            vertex3.property("tel", "555-555-5555");
            vertex3.property("imageid", "img3");
            vertex3.property("thumbnailid", "thumb3");

            Vertex vertex4 = tx.addVertex("member");
            vertex4.property("userid", "user4");
            vertex4.property("username", "Jane");
            vertex4.property("pw", "password101");
            vertex4.property("firstname", "Jane");
            vertex4.property("lastname", "Brown");
            vertex4.property("gender", "female");
            vertex4.property("dob", "1995-03-25");
            vertex4.property("jdate", "2024-03-01");
            vertex4.property("ldate", "2025-03-25");
            vertex4.property("address", "321 Oak St, Los Angeles, CA");
            vertex4.property("email", "emma.brown@example.com");
            vertex4.property("tel", "444-444-4444");
            vertex4.property("imageid", "img4");
            vertex4.property("thumbnailid", "thumb4");

            Vertex vertex5 = tx.addVertex("member");
            vertex5.property("userid", "user5");
            vertex5.property("username", "Mike");
            vertex5.property("pw", "password202");
            vertex5.property("firstname", "Mike");
            vertex5.property("lastname", "Jones");
            vertex5.property("gender", "male");
            vertex5.property("dob", "1988-10-20");
            vertex5.property("jdate", "2020-10-01");
            vertex5.property("ldate", "2025-10-20");
            vertex5.property("address", "654 Pine St, Los Angeles, CA");
            vertex5.property("email", "mike.jones@example.com");
            vertex5.property("tel", "666-666-6666");
            vertex5.property("imageid", "img5");
            vertex5.property("thumbnailid", "thumb5");
            tx.commit();
            System.out.println("Node Data loaded successfully.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void loadEdges(JanusGraph graph) {
        try {
            JanusGraphTransaction tx = graph.newTransaction();
            Map<String, Vertex> vertexCache = new HashMap<>();
            addVertex(tx, vertexCache, "Bob", "Jill", "friendship");
            addVertex(tx, vertexCache, "Jill", "Kate", "friendship");
            addVertex(tx, vertexCache, "Jill", "Mike", "pendingFriendship");
            addVertex(tx, vertexCache, "Jane", "Kate", "friendship");
            addVertex(tx, vertexCache, "Mike", "Jane", "friendship");
            addVertex(tx, vertexCache, "Jane", "Bob", "pendingFriendship");
            tx.commit();
            System.out.println("Edge Data loaded successfully.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void addVertex(JanusGraphTransaction tx, Map<String, Vertex> vertexCache, String name1, String name2, String relationship) {
        Vertex fromVertex2 = vertexCache.computeIfAbsent(name1, name ->
                tx.traversal().V().has("username", name).tryNext().orElse(null)
        );
        Vertex toVertex2 = vertexCache.computeIfAbsent(name2, name ->
                tx.traversal().V().has("username", name).tryNext().orElse(null)
        );
        if (fromVertex2 != null && toVertex2 != null) {
            fromVertex2.addEdge(relationship, toVertex2);}
    }

    public static void query(Graph graph, String username){
        try{
            List<Object> numFriend = graph.traversal().V().hasLabel("member").has("username", username).inE("friendship").outV().values("username").toList();
            List<Object> numPendingFriend = graph.traversal().V().hasLabel("member").has("username", username).inE("pendingFriendship").outV().values("username").toList();
            System.out.println(username + " has friendship:" + numFriend + " count:" + numFriend.size());
            System.out.println(username + " has pending_friendship:" + numPendingFriend + " count:" + numPendingFriend.size());
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}

