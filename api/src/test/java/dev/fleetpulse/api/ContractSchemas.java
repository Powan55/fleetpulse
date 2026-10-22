package dev.fleetpulse.api;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.networknt.schema.InputFormat;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SpecificationVersion;
import java.nio.file.Files;
import java.nio.file.Path;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;
import tools.jackson.dataformat.yaml.YAMLMapper;

final class ContractSchemas {
    private static final Path CONTRACTS = Path.of("..", "contracts");
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final SchemaRegistry REGISTRY = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12);

    private ContractSchemas() {}

    /** Validates body against components.schemas.{name} of the OpenAPI file. */
    static void assertApi(String name, String body) throws Exception {
        JsonNode components = new YAMLMapper().readTree(Files.readString(CONTRACTS.resolve("api.openapi.yaml"))).get("components");
        ObjectNode root = JSON.createObjectNode();
        root.put("$schema", "https://json-schema.org/draft/2020-12/schema");
        root.put("$ref", "#/components/schemas/" + name);
        root.set("components", components);
        check(REGISTRY.getSchema(root.toString(), InputFormat.JSON), name, body);
    }

    /** Validates an SSE data line against $defs.{name} of the SSE schema. */
    static void assertSse(String name, String data) throws Exception {
        JsonNode schema = JSON.readTree(Files.readString(CONTRACTS.resolve("sse-events.schema.json"))).deepCopy();
        ((ObjectNode) schema).put("$ref", "#/$defs/" + name);
        check(REGISTRY.getSchema(schema.toString(), InputFormat.JSON), name, data);
    }

    /** Validates an outbox payload against the webhook schema. */
    static void assertWebhook(String body) throws Exception {
        check(REGISTRY.getSchema(Files.readString(CONTRACTS.resolve("webhook.schema.json")), InputFormat.JSON), "webhook", body);
    }

    private static void check(Schema schema, String name, String body) {
        var errors = schema.validate(body, InputFormat.JSON);
        assertTrue(errors.isEmpty(), name + " invalid: " + errors + " body=" + body);
    }
}
