package org.gautelis.durga.tools;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CharlotteTargetGeneratorTest {

    @Test
    public void generatesPortableContractsAndBlockedDeploymentPlan() throws Exception {
        System.out.println("TC: Charlotte target generates portable contracts and an honest blocked deployment plan");
        Path output = Files.createTempDirectory("durga-charlotte-target-");

        generate("e2e_pipeline.bpmn", output);

        assertTrue(Files.exists(output.resolve("model.bpmn")));
        assertTrue(Files.exists(output.resolve("Cargo.toml")));
        assertTrue(Files.exists(output.resolve("src/lib.rs")));
        assertTrue(Files.exists(output.resolve("src/transform_order.rs")));
        assertTrue(Files.exists(output.resolve("src/route_by_amount.rs")));

        Map<String, Object> bundle = readYaml(output.resolve("charlotte/bundle.yaml"));
        assertEquals("durga.gautelis.org/charlotte-v1alpha1", bundle.get("apiVersion"));
        assertEquals("CharlotteProcessBundle", bundle.get("kind"));
        Map<String, Object> bundleSpec = map(bundle.get("spec"));
        assertEquals("aarch64-unknown-none-catten", map(bundleSpec.get("target")).get("triple"));
        assertEquals(7, list(bundleSpec.get("components")).size());
        assertTrue(list(bundleSpec.get("requiredPlatformContracts")).stream()
                .map(CharlotteTargetGeneratorTest::map)
                .anyMatch(requirement -> "kafka-transactional-step-runner".equals(requirement.get("id"))));

        for (Object value : list(bundleSpec.get("components"))) {
            String logicalName = (String) map(map(value).get("artifact")).get("logicalName");
            assertTrue("CLS2 artifact name exceeds Charlotte's 48-byte capacity", logicalName.length() <= 48);
        }

        Map<String, Object> deployment = readYaml(output.resolve("charlotte/deployment.yaml"));
        Map<String, Object> deploymentSpec = map(deployment.get("spec"));
        assertEquals(Boolean.FALSE, map(deploymentSpec.get("status")).get("deployable"));
        Map<String, Object> firstDeployment = map(list(deploymentSpec.get("components")).get(0));
        Map<String, Object> placement = map(firstDeployment.get("placement"));
        assertEquals(1, placement.get("replicas"));
        assertEquals(1, placement.get("maxInstancesPerNode"));
        assertEquals(1, placement.get("minDistinctNodes"));
        assertEquals("requires-explicit-single-node-assignment",
                map(firstDeployment.get("currentManifestAdapter")).get("status"));
        assertEquals("kafka-step",
                map(firstDeployment.get("transactionalStep")).get("platformArtifact"));
        assertEquals("transform_order-connector",
                map(firstDeployment.get("transactionalStep")).get("kafkaConnector"));

        Map<String, Object> capabilities = readYaml(output.resolve("charlotte/capabilities.yaml"));
        Map<String, Object> capabilitySpec = map(capabilities.get("spec"));
        Map<String, Object> firstPrincipal = map(list(capabilitySpec.get("principals")).get(0));
        assertEquals(Boolean.FALSE, map(firstPrincipal.get("kafka")).get("granted"));
        assertEquals(7, list(capabilitySpec.get("transactionalSteps")).size());
        Map<String, Object> firstStep = map(list(capabilitySpec.get("transactionalSteps")).get(0));
        Map<String, Object> kafka = map(firstStep.get("kafka"));
        assertEquals(2, kafka.get("profileFormatVersion"));
        assertEquals("read-only-launch-capability", kafka.get("profileDelivery"));
        assertEquals("sha256", kafka.get("profileDigest"));
        assertEquals(64, kafka.get("maxProduceRoutes"));
        Map<String, Object> connector = map(kafka.get("connector"));
        assertEquals("kafka", connector.get("platformArtifact"));
        assertEquals("launcher-only", connector.get("profileMaterial"));
        List<Object> routes = list(kafka.get("produceRoutes"));
        assertEquals(3, routes.size());
        assertEquals(1, map(routes.get(0)).get("index"));
        assertEquals("process-output", map(routes.get(0)).get("purpose"));
        assertEquals("lifecycle", map(routes.get(1)).get("purpose"));
        assertEquals("dead-letter", map(routes.get(2)).get("purpose"));

        String handler = Files.readString(output.resolve("src/transform_order.rs"));
        assertTrue(handler.contains("Err(ActivityError::NotImplemented)"));
        assertFalse(handler.contains("unsafe"));
        String readme = Files.readString(output.resolve("README.md"));
        assertTrue(readme.contains("not yet a deployable Charlotte application"));
        assertTrue(readme.contains("catten_rt::owned"));
        assertTrue(readme.contains("business code does not receive Kafka authority"));
    }

    @Test
    public void preservesS3DataStoresAsExplicitCapabilityProfiles() throws Exception {
        System.out.println("TC: Charlotte target preserves BPMN S3 stores as reviewed capability profiles");
        Path output = Files.createTempDirectory("durga-charlotte-s3-");

        generate("data_pipeline_demo.bpmn", output);

        Map<String, Object> capabilities = readYaml(output.resolve("charlotte/capabilities.yaml"));
        Map<String, Object> spec = map(capabilities.get("spec"));
        List<Object> stores = list(spec.get("dataStoreProfiles"));
        assertTrue(stores.stream().map(CharlotteTargetGeneratorTest::map)
                .anyMatch(store -> "s3".equals(store.get("kind"))
                        && "s3".equals(store.get("charlotteService"))
                        && "launch-profile-secret-reference".equals(store.get("credentials"))));

        Map<String, Object> bundle = readYaml(output.resolve("charlotte/bundle.yaml"));
        Map<String, Object> bundleSpec = map(bundle.get("spec"));
        assertFalse(list(bundleSpec.get("dataStores")).isEmpty());
        assertFalse(list(bundleSpec.get("dataAssociations")).isEmpty());
    }

    private static void generate(String fixture, Path output) {
        BpmnScaffolder.main(new String[]{
                fixturePath(fixture).toAbsolutePath().toString(),
                "--out", output.toAbsolutePath().toString(),
                "--target", "charlotte"
        });
    }

    private static Path fixturePath(String fixture) {
        Path modulePath = Path.of("src/test/resources/bpmn", fixture);
        if (Files.exists(modulePath)) {
            return modulePath;
        }
        return Path.of("durga-tools/src/test/resources/bpmn", fixture);
    }

    private static Map<String, Object> readYaml(Path path) throws Exception {
        return new ObjectMapper(new YAMLFactory()).readValue(path.toFile(), new TypeReference<>() {
        });
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> map(Object value) {
        return (Map<String, Object>) value;
    }

    @SuppressWarnings("unchecked")
    private static List<Object> list(Object value) {
        return (List<Object>) value;
    }
}
