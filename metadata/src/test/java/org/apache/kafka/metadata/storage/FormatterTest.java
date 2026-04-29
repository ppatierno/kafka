/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.metadata.storage;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.VotersRecord;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.metadata.UserScramCredentialRecord;
import org.apache.kafka.common.record.internal.ControlRecordUtils;
import org.apache.kafka.common.record.internal.FileRecords;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.security.scram.internals.ScramFormatter;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.metadata.bootstrap.BootstrapDirectory;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.metadata.properties.MetaProperties;
import org.apache.kafka.metadata.properties.MetaPropertiesEnsemble;
import org.apache.kafka.raft.DynamicVoters;
import org.apache.kafka.raft.VoterSet;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.EligibleLeaderReplicasVersion;
import org.apache.kafka.server.common.Feature;
import org.apache.kafka.server.common.GroupVersion;
import org.apache.kafka.server.common.KRaftVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.OffsetAndEpoch;
import org.apache.kafka.server.common.ShareVersion;
import org.apache.kafka.server.common.StreamsVersion;
import org.apache.kafka.server.common.TestFeatureVersion;
import org.apache.kafka.server.common.TransactionVersion;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import static org.apache.kafka.metadata.storage.ScramParserTest.TEST_SALT;
import static org.apache.kafka.metadata.storage.ScramParserTest.TEST_SALTED_PASSWORD;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 40)
public class FormatterTest {
    private static final Logger LOG = LoggerFactory.getLogger(FormatterTest.class);

    private static final int DEFAULT_NODE_ID = 1;

    private static final Uuid DEFAULT_CLUSTER_ID = Uuid.fromString("b3dGE68sQQKzfk80C_aLZw");

    static class TestEnv implements AutoCloseable {
        final List<String> directories;

        TestEnv(int numDirs) {
            this.directories = new ArrayList<>(numDirs);
            for (int i = 0; i < numDirs; i++) {
                this.directories.add(TestUtils.tempDirectory().getAbsolutePath());
            }
        }

        FormatterContext newFormatter() {
            Formatter formatter = new Formatter().
                setNodeId(DEFAULT_NODE_ID).
                setClusterId(DEFAULT_CLUSTER_ID.toString());
            directories.forEach(formatter::addDirectory);
            formatter.setMetadataLogDirectory(directories.get(0));
            return new FormatterContext(formatter);
        }

        String directory(int i) {
            return this.directories.get(i);
        }

        void deleteDirectory(int i) throws IOException {
            Utils.delete(new File(directories.get(i)));
        }

        @Override
        public void close() throws Exception {
            for (int i = 0; i < directories.size(); i++) {
                try {
                    deleteDirectory(i);
                } catch (Exception e) {
                    LOG.error("Error deleting directory " + directories.get(i), e);
                }
            }
        }
    }

    static class FormatterContext {
        final Formatter formatter;
        final ByteArrayOutputStream stream;

        FormatterContext(Formatter formatter) {
            this.formatter = formatter;
            this.stream = new ByteArrayOutputStream();
            this.formatter.setPrintStream(new PrintStream(stream));
            this.formatter.setControllerListenerName("CONTROLLER");
        }

        String output() {
            return stream.toString();
        }

        List<String> outputLines() {
            return List.of(stream.toString().trim().split("\\r*\\n"));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    public void testDirectories(int numDirs) throws Exception {
        try (TestEnv testEnv = new TestEnv(numDirs)) {
            testEnv.newFormatter().formatter.run();
            MetaPropertiesEnsemble ensemble = new MetaPropertiesEnsemble.Loader().
                addLogDirs(testEnv.directories).
                load();
            assertEquals(OptionalInt.of(DEFAULT_NODE_ID), ensemble.nodeId());
            assertEquals(Optional.of(DEFAULT_CLUSTER_ID.toString()), ensemble.clusterId());
            assertEquals(new HashSet<>(testEnv.directories), ensemble.logDirProps().keySet());
            BootstrapMetadata bootstrapMetadata =
                new BootstrapDirectory(testEnv.directory(0)).read();
            assertEquals(MetadataVersion.latestProduction(), bootstrapMetadata.metadataVersion());
        }
    }

    @Test
    public void testFormatterFailsOnAlreadyFormatted() throws Exception {
        try (TestEnv testEnv = new TestEnv(1)) {
            testEnv.newFormatter().formatter.run();
            assertEquals("Log directory " + testEnv.directory(0) + " is already formatted. " +
                "Use --ignore-formatted to ignore this directory and format the others.",
                    assertThrows(FormatterException.class,
                        () -> testEnv.newFormatter().formatter.run()).getMessage());
        }
    }

    @Test
    public void testFormatterFailsOnUnwritableDirectory() throws Exception {
        try (TestEnv testEnv = new TestEnv(1)) {
            new File(testEnv.directory(0)).setReadOnly();
            FormatterContext formatter1 = testEnv.newFormatter();
            String expectedPrefix = "Error while writing meta.properties file";
            assertEquals(expectedPrefix,
                assertThrows(FormatterException.class,
                    formatter1.formatter::run).
                        getMessage().substring(0, expectedPrefix.length()));
        }
    }

    @Test
    public void testIgnoreFormatted() throws Exception {
        try (TestEnv testEnv = new TestEnv(1)) {
            FormatterContext formatter1 = testEnv.newFormatter();
            formatter1.formatter.run();
            assertEquals("Bootstrap metadata: " + formatter1.formatter.bootstrapMetadata() +
                    "\nFormatting metadata directory " + testEnv.directory(0) +
                    " with metadata.version " + MetadataVersion.latestProduction() + ".",
                formatter1.output().trim());

            FormatterContext formatter2 = testEnv.newFormatter();
            formatter2.formatter.setIgnoreFormatted(true);
            formatter2.formatter.run();
            assertTrue(formatter2.output().trim().contains("All of the log directories are already formatted."));
        }
    }

    @Test
    public void testStandaloneWithIgnoreFormatted() throws Exception {
        try (TestEnv testEnv = new TestEnv(1)) {
            FormatterContext formatter1 = testEnv.newFormatter();
            String originalDirectoryId = Uuid.randomUuid().toString();
            String newDirectoryId = Uuid.randomUuid().toString();
            formatter1.formatter
                .setInitialControllers(DynamicVoters.parse("1@localhost:8020:" + originalDirectoryId))
                .setHasDynamicQuorum(true)
                .run();
            assertEquals("Bootstrap metadata: " + formatter1.formatter.bootstrapMetadata() +
                    "\nFormatting dynamic metadata voter directory " + testEnv.directory(0) +
                    " with metadata.version " + MetadataVersion.latestProduction() + ".",
                formatter1.output().trim());
            assertMetadataDirectoryId(testEnv, Uuid.fromString(originalDirectoryId));

            FormatterContext formatter2 = testEnv.newFormatter();
            formatter2.formatter
                .setIgnoreFormatted(true)
                .setInitialControllers(DynamicVoters.parse("1@localhost:8020:" + newDirectoryId))
                .run();
            assertEquals("All of the log directories are already formatted.",
                formatter2.output().trim());
            assertMetadataDirectoryId(testEnv, Uuid.fromString(originalDirectoryId));
        }
    }

    private void assertMetadataDirectoryId(TestEnv testEnv, Uuid expectedDirectoryId) throws Exception {
        MetaPropertiesEnsemble ensemble = new MetaPropertiesEnsemble.Loader().
            addLogDirs(testEnv.directories).
            load();
        MetaProperties logDirProps0 = ensemble.logDirProps().get(testEnv.directory(0));
        assertEquals(expectedDirectoryId, logDirProps0.directoryId().get());
    }

    @Test
    public void testOneDirectoryFormattedAndOthersNotFormatted() throws Exception {
        try (TestEnv testEnv = new TestEnv(2)) {
            testEnv.newFormatter().formatter.setDirectories(List.of(testEnv.directory(0))).run();
            assertEquals("Log directory " + testEnv.directory(0) + " is already formatted. " +
                "Use --ignore-formatted to ignore this directory and format the others.",
                    assertThrows(FormatterException.class,
                        () -> testEnv.newFormatter().formatter.run()).getMessage());
        }
    }

    @Test
    public void testOneDirectoryFormattedAndOthersNotFormattedWithIgnoreFormatted() throws Exception {
        try (TestEnv testEnv = new TestEnv(2)) {
            testEnv.newFormatter().formatter.setDirectories(List.of(testEnv.directory(0))).run();

            FormatterContext formatter2 = testEnv.newFormatter();
            formatter2.formatter.setIgnoreFormatted(true);
            formatter2.formatter.run();
            assertEquals("Bootstrap metadata: " + formatter2.formatter.bootstrapMetadata() +
                    "\nFormatting data directory " + testEnv.directory(1) + " with metadata.version " +
                    MetadataVersion.latestProduction() + ".",
                formatter2.output().trim());
        }
    }

    @Test
    public void testFormatWithOlderReleaseVersion() throws Exception {
        try (TestEnv testEnv = new TestEnv(1)) {
            FormatterContext formatter1 = testEnv.newFormatter();
            formatter1.formatter.setReleaseVersion(MetadataVersion.IBP_3_5_IV0);
            formatter1.formatter.run();
            assertEquals("Bootstrap metadata: " + formatter1.formatter.bootstrapMetadata() +
                    "\nFormatting metadata directory " + testEnv.directory(0) +
                    " with metadata.version " + MetadataVersion.IBP_3_5_IV0 + ".",
                formatter1.output().trim());
            BootstrapMetadata bootstrapMetadata =
                new BootstrapDirectory(testEnv.directory(0)).read();
            assertEquals(MetadataVersion.IBP_3_5_IV0, bootstrapMetadata.metadataVersion());
            assertEquals(1, bootstrapMetadata.records().size());
        }
    }

    @Test
    public void testFormatWithUnstableReleaseVersionFailsWithoutEnableUnstable() throws Exception {
        try (TestEnv testEnv = new TestEnv(1)) {
            FormatterContext formatter1 = testEnv.newFormatter();
            formatter1.formatter.setReleaseVersion(MetadataVersion.latestTesting());
            assertEquals("metadata.version " + MetadataVersion.latestTesting() + " is not yet stable.",
                assertThrows(FormatterException.class, formatter1.formatter::run).getMessage());
        }
    }

    @Test
    public void testFormatWithUnstableReleaseVersion() throws Exception {
        try (TestEnv testEnv = new TestEnv(1)) {
            FormatterContext formatter1 = testEnv.newFormatter();
            formatter1.formatter.setReleaseVersion(MetadataVersion.latestTesting());
            formatter1.formatter.setUnstableFeatureVersionsEnabled(true);
            formatter1.formatter.run();
            assertEquals("Bootstrap metadata: " + formatter1.formatter.bootstrapMetadata() +
                    "\nFormatting metadata directory " + testEnv.directory(0) +
                    " with metadata.version " + MetadataVersion.latestTesting() + ".",
                formatter1.output().trim());
            BootstrapMetadata bootstrapMetadata =
                    new BootstrapDirectory(testEnv.directory(0)).read();
            assertEquals(MetadataVersion.latestTesting(), bootstrapMetadata.metadataVersion());
        }
    }

    @Test
    public void testFormattingCreatesLogDirId() throws Exception {
        try (TestEnv testEnv = new TestEnv(1)) {
            FormatterContext formatter1 = testEnv.newFormatter();
            formatter1.formatter.run();
            MetaPropertiesEnsemble ensemble = new MetaPropertiesEnsemble.Loader().
                addLogDirs(testEnv.directories).
                load();
            MetaProperties logDirProps = ensemble.logDirProps().get(testEnv.directory(0));
            assertNotNull(logDirProps);
            assertTrue(logDirProps.directoryId().isPresent());
        }
    }

    @Test
    public void testFormatWithScramFailsOnUnsupportedReleaseVersions() throws Exception {
        try (TestEnv testEnv = new TestEnv(1)) {
            FormatterContext formatter1 = testEnv.newFormatter();
            formatter1.formatter.setReleaseVersion(MetadataVersion.IBP_3_4_IV0);
            formatter1.formatter.setScramArguments(List.of(
                "SCRAM-SHA-256=[name=alice,salt=\"MWx2NHBkbnc0ZndxN25vdGN4bTB5eTFrN3E=\"," +
                    "saltedpassword=\"mT0yyUUxnlJaC99HXgRTSYlbuqa4FSGtJCJfTMvjYCE=\"]",
                "SCRAM-SHA-512=[name=alice,salt=\"MWx2NHBkbnc0ZndxN25vdGN4bTB5eTFrN3E=\"," +
                    "saltedpassword=\"mT0yyUUxnlJaC99HXgRTSYlbuqa4FSGtJCJfTMvjYCE=\"]"));
            assertEquals("SCRAM is only supported in metadata.version 3.5-IV2 or later.",
                assertThrows(FormatterException.class,
                    formatter1.formatter::run).getMessage());
        }
    }

    @Test
    public void testFormatWithScram() throws Exception {
        try (TestEnv testEnv = new TestEnv(1)) {
            FormatterContext formatter1 = testEnv.newFormatter();
            formatter1.formatter.setReleaseVersion(MetadataVersion.IBP_3_8_IV0);
            formatter1.formatter.setScramArguments(List.of(
                "SCRAM-SHA-256=[name=alice,salt=\"MWx2NHBkbnc0ZndxN25vdGN4bTB5eTFrN3E=\"," +
                    "saltedpassword=\"mT0yyUUxnlJaC99HXgRTSYlbuqa4FSGtJCJfTMvjYCE=\"]",
                "SCRAM-SHA-512=[name=alice,salt=\"MWx2NHBkbnc0ZndxN25vdGN4bTB5eTFrN3E=\"," +
                    "saltedpassword=\"mT0yyUUxnlJaC99HXgRTSYlbuqa4FSGtJCJfTMvjYCE=\"]"));
            formatter1.formatter.run();
            assertEquals("Bootstrap metadata: " + formatter1.formatter.bootstrapMetadata() +
                    "\nFormatting metadata directory " + testEnv.directory(0) +
                    " with metadata.version " + MetadataVersion.IBP_3_8_IV0 + ".",
                formatter1.output().trim());
            BootstrapMetadata bootstrapMetadata =
                new BootstrapDirectory(testEnv.directory(0)).read();
            assertEquals(MetadataVersion.IBP_3_8_IV0, bootstrapMetadata.metadataVersion());
            List<ApiMessageAndVersion> scramRecords = bootstrapMetadata.records().stream().
                filter(r -> r.message() instanceof UserScramCredentialRecord).
                    toList();
            ScramFormatter scram256 = new ScramFormatter(ScramMechanism.SCRAM_SHA_256);
            ScramFormatter scram512 = new ScramFormatter(ScramMechanism.SCRAM_SHA_512);
            assertEquals(List.of(
                new ApiMessageAndVersion(new UserScramCredentialRecord().
                    setName("alice").
                    setMechanism(ScramMechanism.SCRAM_SHA_256.type()).
                    setSalt(TEST_SALT).
                    setStoredKey(scram256.storedKey(scram256.clientKey(TEST_SALTED_PASSWORD))).
                    setServerKey(scram256.serverKey(TEST_SALTED_PASSWORD)).
                    setIterations(4096), (short) 0),
                new ApiMessageAndVersion(new UserScramCredentialRecord().
                    setName("alice").
                    setMechanism(ScramMechanism.SCRAM_SHA_512.type()).
                    setSalt(TEST_SALT).
                    setStoredKey(scram512.storedKey(scram512.clientKey(TEST_SALTED_PASSWORD))).
                    setServerKey(scram512.serverKey(TEST_SALTED_PASSWORD)).
                    setIterations(4096), (short) 0)),
                scramRecords);
        }
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testFeatureFlag(short version) throws Exception {
        try (TestEnv testEnv = new TestEnv(1)) {
            FormatterContext formatter1 = testEnv.newFormatter();
            formatter1.formatter.setSupportedFeatures(Feature.TEST_AND_PRODUCTION_FEATURES);
            formatter1.formatter.setFeatureLevel(TestFeatureVersion.FEATURE_NAME, version);
            formatter1.formatter.run();
            BootstrapMetadata bootstrapMetadata =
                new BootstrapDirectory(testEnv.directory(0)).read();
            List<ApiMessageAndVersion> expected = new ArrayList<>();
            expected.add(new ApiMessageAndVersion(new FeatureLevelRecord().
                setName(MetadataVersion.FEATURE_NAME).
                setFeatureLevel(MetadataVersion.latestProduction().featureLevel()),
                    (short) 0));
            expected.add(new ApiMessageAndVersion(new FeatureLevelRecord().
                setName(EligibleLeaderReplicasVersion.FEATURE_NAME).
                setFeatureLevel(EligibleLeaderReplicasVersion.ELRV_1.featureLevel()), (short) 0));
            expected.add(new ApiMessageAndVersion(new FeatureLevelRecord().
                setName(GroupVersion.FEATURE_NAME).
                setFeatureLevel(GroupVersion.GV_1.featureLevel()), (short) 0));
            expected.add(new ApiMessageAndVersion(new FeatureLevelRecord().
                setName(ShareVersion.FEATURE_NAME).
                setFeatureLevel(ShareVersion.SV_1.featureLevel()), (short) 0));
            expected.add(new ApiMessageAndVersion(new FeatureLevelRecord().
                setName(StreamsVersion.FEATURE_NAME).
                setFeatureLevel(StreamsVersion.SV_1.featureLevel()), (short) 0));
            if (version > 0) {
                expected.add(new ApiMessageAndVersion(new FeatureLevelRecord().
                    setName(TestFeatureVersion.FEATURE_NAME).
                    setFeatureLevel(version), (short) 0));
            }
            expected.add(new ApiMessageAndVersion(new FeatureLevelRecord().
                setName(TransactionVersion.FEATURE_NAME).
                setFeatureLevel(TransactionVersion.TV_2.featureLevel()), (short) 0));
            assertEquals(expected, bootstrapMetadata.records());
        }
    }

    @Test
    public void testInvalidFeatureFlag() throws Exception {
        try (TestEnv testEnv = new TestEnv(2)) {
            FormatterContext formatter1 = testEnv.newFormatter();
            formatter1.formatter.setSupportedFeatures(Feature.TEST_AND_PRODUCTION_FEATURES);
            formatter1.formatter.setFeatureLevel("nonexistent.feature", (short) 1);
            assertEquals("Unsupported feature: nonexistent.feature. Supported features " +
                    "are: eligible.leader.replicas.version, group.version, kraft.version, " +
                    "share.version, streams.version, test.feature.version, transaction.version",
                assertThrows(FormatterException.class,
                    formatter1.formatter::run).
                        getMessage());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testFormatWithInitialVoters(boolean specifyKRaftVersion) throws Exception {
        try (TestEnv testEnv = new TestEnv(2)) {
            FormatterContext formatter1 = testEnv.newFormatter();
            if (specifyKRaftVersion) {
                formatter1.formatter.setFeatureLevel(KRaftVersion.FEATURE_NAME, (short) 1);
            }
            formatter1.formatter.setUnstableFeatureVersionsEnabled(true);
            formatter1.formatter.setInitialControllers(DynamicVoters.
                parse("1@localhost:8020:4znU-ou9Taa06bmEJxsjnw"));
            formatter1.formatter.setHasDynamicQuorum(true);
            formatter1.formatter.run();
            assertEquals((short) 1, formatter1.formatter.featureLevels.get(KRaftVersion.FEATURE_NAME));
            assertEquals(List.of(
                "Bootstrap metadata: " + formatter1.formatter.bootstrapMetadata(),
                String.format("Formatting data directory %s with %s %s.",
                    testEnv.directory(1),
                    MetadataVersion.FEATURE_NAME,
                    MetadataVersion.latestTesting()),
                String.format("Formatting dynamic metadata voter directory %s with %s %s.",
                    testEnv.directory(0),
                    MetadataVersion.FEATURE_NAME,
                    MetadataVersion.latestTesting())),
                formatter1.outputLines().stream().sorted().toList());
            MetaPropertiesEnsemble ensemble = new MetaPropertiesEnsemble.Loader().
                addLogDirs(testEnv.directories).
                load();
            MetaProperties logDirProps0 = ensemble.logDirProps().get(testEnv.directory(0));
            assertNotNull(logDirProps0);
            assertEquals(Uuid.fromString("4znU-ou9Taa06bmEJxsjnw"), logDirProps0.directoryId().get());
            MetaProperties logDirProps1 = ensemble.logDirProps().get(testEnv.directory(1));
            assertNotNull(logDirProps1);
            assertNotEquals(Uuid.fromString("4znU-ou9Taa06bmEJxsjnw"), logDirProps1.directoryId().get());
        }
    }

    @Test
    public void testFormatWithInitialVotersFailsWithOlderKraftVersion() throws Exception {
        try (TestEnv testEnv = new TestEnv(2)) {
            FormatterContext formatter1 = testEnv.newFormatter();
            formatter1.formatter.setFeatureLevel(KRaftVersion.FEATURE_NAME, (short) 0);
            formatter1.formatter.setUnstableFeatureVersionsEnabled(true);
            formatter1.formatter.setInitialControllers(DynamicVoters.
                    parse("1@localhost:8020:4znU-ou9Taa06bmEJxsjnw"));
            formatter1.formatter.setHasDynamicQuorum(true);
            assertTrue(formatter1.formatter.hasDynamicQuorum());
            assertEquals(
                "Cannot set kraft.version to 0 if controller.quorum.voters is empty " +
                "and one of the flags --standalone, --initial-controllers, or --no-initial-controllers is used. " +
                "For dynamic controllers support, try removing the --feature flag for kraft.version.",
                assertThrows(FormatterException.class, formatter1.formatter::run).getMessage()
            );
        }
    }

    @Test
    public void testFormatWithStaticQuorumFailsWithNewerKraftVersion() throws Exception {
        try (TestEnv testEnv = new TestEnv(2)) {
            FormatterContext formatter1 = testEnv.newFormatter();
            formatter1.formatter.setFeatureLevel(KRaftVersion.FEATURE_NAME, (short) 1);
            formatter1.formatter.setUnstableFeatureVersionsEnabled(true);
            assertFalse(formatter1.formatter.hasDynamicQuorum());
            assertEquals(
                "Cannot set kraft.version to 1 unless controller.quorum.voters is empty and " +
                "one of the flags --standalone, --initial-controllers, or --no-initial-controllers is used. " +
                "For dynamic controllers support, try using one of --standalone, --initial-controllers, " +
                "or --no-initial-controllers and removing controller.quorum.voters.",
                assertThrows(FormatterException.class, formatter1.formatter::run).getMessage()
            );
        }
    }

    @Test
    public void testFormatWithInitialVotersWithOlderMetadataVersion() throws Exception {
        try (TestEnv testEnv = new TestEnv(2)) {
            FormatterContext formatter1 = testEnv.newFormatter();
            formatter1.formatter.setReleaseVersion(MetadataVersion.IBP_3_8_IV0);
            formatter1.formatter.setFeatureLevel(KRaftVersion.FEATURE_NAME, (short) 1);
            formatter1.formatter.setInitialControllers(DynamicVoters.
                    parse("1@localhost:8020:4znU-ou9Taa06bmEJxsjnw"));
            formatter1.formatter.setUnstableFeatureVersionsEnabled(true);
            formatter1.formatter.setHasDynamicQuorum(true);
            formatter1.formatter.run();
            assertEquals((short) 1, formatter1.formatter.featureLevels.get(KRaftVersion.FEATURE_NAME));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testFormatWithNoInitialControllersWithOlderMetadataVersion(boolean hasDynamicQuorum) throws Exception {
        try (TestEnv testEnv = new TestEnv(2)) {
            FormatterContext formatter1 = testEnv.newFormatter();
            formatter1.formatter.setReleaseVersion(MetadataVersion.IBP_3_8_IV0);
            formatter1.formatter.setHasDynamicQuorum(hasDynamicQuorum);
            formatter1.formatter.run();
            if (hasDynamicQuorum) {
                assertEquals((short) 1, formatter1.formatter.featureLevels.get(KRaftVersion.FEATURE_NAME));
            } else {
                assertEquals((short) 0, formatter1.formatter.featureLevels.get(KRaftVersion.FEATURE_NAME));
            }
        }
    }

    private static Stream<Arguments> elrTestMetadataVersions() {
        return Stream.of(
            MetadataVersion.IBP_3_9_IV0,
            MetadataVersion.IBP_4_0_IV0,
            MetadataVersion.IBP_4_0_IV1 // ELR minimal MV
        ).map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("elrTestMetadataVersions")
    public void testFormatElrEnabledWithMetadataVersions(MetadataVersion metadataVersion) throws Exception {
        try (TestEnv testEnv = new TestEnv(2)) {
            FormatterContext formatter1 = testEnv.newFormatter();
            formatter1.formatter.setReleaseVersion(metadataVersion);
            formatter1.formatter.setFeatureLevel(EligibleLeaderReplicasVersion.FEATURE_NAME, (short) 1);
            formatter1.formatter.setInitialControllers(DynamicVoters.
                parse("1@localhost:8020:4znU-ou9Taa06bmEJxsjnw"));
            formatter1.formatter.setHasDynamicQuorum(true);
            if (metadataVersion.isAtLeast(MetadataVersion.IBP_4_0_IV1)) {
                assertDoesNotThrow(formatter1.formatter::run);
            } else {
                assertEquals("eligible.leader.replicas.version could not be set to 1 because it depends on " +
                    "metadata.version level 23",
                    assertThrows(IllegalArgumentException.class,
                        formatter1.formatter::run).getMessage());
            }
        }
    }

    @Test
    public void testFormatWithNoInitialControllers() throws Exception {
        try (TestEnv testEnv = new TestEnv(2)) {
            FormatterContext formatter1 = testEnv.newFormatter();
            formatter1.formatter.setUnstableFeatureVersionsEnabled(true);
            assertFalse(formatter1.formatter.hasDynamicQuorum());
            formatter1.formatter.run();
            assertEquals((short) 0, formatter1.formatter.featureLevels.get(KRaftVersion.FEATURE_NAME));
            assertEquals(List.of(
                    "Bootstrap metadata: " + formatter1.formatter.bootstrapMetadata(),
                    String.format("Formatting data directory %s with %s %s.",
                        testEnv.directory(1),
                        MetadataVersion.FEATURE_NAME,
                        MetadataVersion.latestTesting()),
                    String.format("Formatting metadata directory %s with %s %s.",
                        testEnv.directory(0),
                        MetadataVersion.FEATURE_NAME,
                        MetadataVersion.latestTesting())),
                formatter1.outputLines().stream().sorted().toList());
            MetaPropertiesEnsemble ensemble = new MetaPropertiesEnsemble.Loader().
                addLogDirs(testEnv.directories).
                load();
            MetaProperties logDirProps0 = ensemble.logDirProps().get(testEnv.directory(0));
            assertNotNull(logDirProps0);
            MetaProperties logDirProps1 = ensemble.logDirProps().get(testEnv.directory(1));
            assertNotNull(logDirProps1);
        }
    }

    @Test
    public void testOverrideFlagCanBeSet() throws Exception {
        try (TestEnv testEnv = new TestEnv(1)) {
            FormatterContext formatter = testEnv.newFormatter();
            formatter.formatter
                .setInitialControllers(DynamicVoters.parse("1@localhost:8020:4znU-ou9Taa06bmEJxsjnw"))
                .setHasDynamicQuorum(true)
                .setOverride(true);

            assertTrue(true);
        }
    }

    @Test
    public void testOverrideRequiresInitialControllers() throws Exception {
        try (TestEnv testEnv = new TestEnv(1)) {
            FormatterContext formatter = testEnv.newFormatter();
            formatter.formatter.setOverride(true);

            assertEquals(
                "--override requires --initial-controllers to specify the new voter endpoints.",
                assertThrows(FormatterException.class, formatter.formatter::run).getMessage()
            );
        }
    }

    @Test
    public void testReadPersistedVoterSetFromSnapshot() throws Exception {
        try (TestEnv testEnv = new TestEnv(1)) {
            // Format with dynamic quorum (creates snapshot with VotersRecord)
            DynamicVoters voters = DynamicVoters.parse("1@localhost:9093:4znU-ou9Taa06bmEJxsjnw,2@localhost:9094:5znU-ou9Taa06bmEJxsjnx,3@localhost:9095:6znU-ou9Taa06bmEJxsjny");

            FormatterContext formatter = testEnv.newFormatter();
            formatter.formatter
                .setUnstableFeatureVersionsEnabled(true)
                .setInitialControllers(voters)
                .setHasDynamicQuorum(true)
                .setFeatureLevel(KRaftVersion.FEATURE_NAME, KRaftVersion.KRAFT_VERSION_1.featureLevel())
                .run();

            // Read VoterSetWriteInfo
            Formatter.VoterSetWriteInfo writeInfo = formatter.formatter.readVoterSetWriteInfo(testEnv.directory(0));

            assertNotNull(writeInfo, "Should read VoterSetWriteInfo");
            assertNotNull(writeInfo.voterSet(), "Should have VoterSet");
            assertEquals(3, writeInfo.voterSet().voterIds().size(), "Should have 3 voters");
            assertTrue(writeInfo.voterSet().voterIds().contains(1), "Should contain voter 1");
            assertTrue(writeInfo.voterSet().voterIds().contains(2), "Should contain voter 2");
            assertTrue(writeInfo.voterSet().voterIds().contains(3), "Should contain voter 3");
            // Bootstrap snapshot contains: Header(0), KRaftVersion(1), Voters(2), Footer(3)
            // The actual last offset is 3, not the snapshot ID (0, 0) from the filename
            assertEquals(new OffsetAndEpoch(3, 0), writeInfo.lastOffsetAndEpoch(), "Bootstrap snapshot should have last offset 3");
            assertEquals(KRaftVersion.KRAFT_VERSION_1.featureLevel(), writeInfo.kraftVersion(), "Should have kraft.version = 1");
        }
    }

    @Test
    public void testReadPersistedVoterSetWithNoSnapshots() throws Exception {
        try (TestEnv testEnv = new TestEnv(1)) {
            // Create a directory without formatting it (no snapshots)
            FormatterContext formatter = testEnv.newFormatter();

            // Should throw exception when metadata log directory doesn't exist
            FormatterException exception = assertThrows(FormatterException.class,
                () -> formatter.formatter.readVoterSetWriteInfo(testEnv.directory(0)));

            assertTrue(exception.getMessage().contains("Metadata log directory not found"),
                "Should indicate metadata log directory not found");
        }
    }

    @Test
    public void testReadPersistedVoterSetFromLog() throws Exception {
        try (TestEnv testEnv = new TestEnv(1)) {
            // Format with initial VoterSet at original endpoints (snapshot created)
            DynamicVoters initialVoters = DynamicVoters.parse(
                "1@localhost:9093:4znU-ou9Taa06bmEJxsjnw,2@localhost:9094:5znU-ou9Taa06bmEJxsjnx,3@localhost:9095:6znU-ou9Taa06bmEJxsjny");

            FormatterContext formatter = testEnv.newFormatter();
            formatter.formatter
                .setUnstableFeatureVersionsEnabled(true)
                .setInitialControllers(initialVoters)
                .setHasDynamicQuorum(true)
                .setFeatureLevel(KRaftVersion.FEATURE_NAME, KRaftVersion.KRAFT_VERSION_1.featureLevel())
                .run();

            // Create a log segment with updated VotersRecord (simulating DNS change)
            // Same voter IDs and directory IDs, but different endpoints (port changes)
            // NOTE: changing the hostname slows down the test because trying to resolve a non-existing host
            DynamicVoters updatedVoters = DynamicVoters.parse(
                "1@localhost:9096:4znU-ou9Taa06bmEJxsjnw,2@localhost:9097:5znU-ou9Taa06bmEJxsjnx,3@localhost:9098:6znU-ou9Taa06bmEJxsjny");
            VotersRecord updatedVotersRecord = updatedVoters.toVoterSet("CONTROLLER")
                .toVotersRecord(ControlRecordUtils.KRAFT_VOTERS_CURRENT_VERSION);

            Path metadataLogPath = Paths.get(testEnv.directory(0), "__cluster_metadata-0");
            Path logFile = metadataLogPath.resolve("00000000000000000000.log");

            // Write a control batch with VotersRecord to the log file
            writeVotersRecordToLog(logFile, updatedVotersRecord, 1L);

            // Read VoterSetWriteInfo, should find the updated one from the log
            Formatter.VoterSetWriteInfo writeInfo = formatter.formatter.readVoterSetWriteInfo(testEnv.directory(0));

            // Verify we read the updated VoterSet from the log (not the snapshot)
            assertNotNull(writeInfo, "Should read VoterSetWriteInfo");
            assertNotNull(writeInfo.voterSet(), "Should have VoterSet from log");
            assertEquals(3, writeInfo.voterSet().voterIds().size(), "Should have 3 voters");
            assertEquals(new OffsetAndEpoch(1, 1), writeInfo.lastOffsetAndEpoch(), "Should have found offset and epoch");
            assertEquals(KRaftVersion.KRAFT_VERSION_1.featureLevel(), writeInfo.kraftVersion(), "Should have kraft.version = 1");

            VoterSet persistedVoterSet = writeInfo.voterSet();

            // Use VoterSetDiff to verify only endpoints changed
            VoterSet initialVoterSet = initialVoters.toVoterSet("CONTROLLER");
            VoterSet updatedVoterSet = updatedVoters.toVoterSet("CONTROLLER");
            VoterSetDiff diff = VoterSetDiff.compare(initialVoterSet, updatedVoterSet, "CONTROLLER");

            assertTrue(diff.onlyEndpointsChanged(), "Should only have endpoint changes");
            assertFalse(diff.hasVoterIdChanges(), "Should not have voter ID changes");
            assertFalse(diff.hasDirectoryIdChanges(), "Should not have directory ID changes");
            assertEquals(3, diff.endpointChanges().size(), "All 3 endpoints should change");

            // Verify the persisted VoterSet matches the updated one
            assertEquals(updatedVoterSet.voterIds(), persistedVoterSet.voterIds(), "VoterSet IDs should match");
        }
    }

    @Test
    public void testReadPersistedVoterSetFromLogWithMultipleUpdates() throws Exception {
        try (TestEnv testEnv = new TestEnv(1)) {
            // Format with initial VoterSet at original endpoints (snapshot created)
            DynamicVoters initialVoters = DynamicVoters.parse(
                "1@localhost:9093:4znU-ou9Taa06bmEJxsjnw,2@localhost:9094:5znU-ou9Taa06bmEJxsjnx,3@localhost:9095:6znU-ou9Taa06bmEJxsjny");

            FormatterContext formatter = testEnv.newFormatter();
            formatter.formatter
                .setUnstableFeatureVersionsEnabled(true)
                .setInitialControllers(initialVoters)
                .setHasDynamicQuorum(true)
                .setFeatureLevel(KRaftVersion.FEATURE_NAME, KRaftVersion.KRAFT_VERSION_1.featureLevel())
                .run();

            Path metadataLogPath = Paths.get(testEnv.directory(0), "__cluster_metadata-0");
            Path logFile = metadataLogPath.resolve("00000000000000000000.log");

            // Simulate multiple DNS changes by writing multiple VotersRecords to the same segment
            // First update: change ports to 9096-9098
            DynamicVoters firstUpdate = DynamicVoters.parse(
                "1@localhost:9096:4znU-ou9Taa06bmEJxsjnw,2@localhost:9097:5znU-ou9Taa06bmEJxsjnx,3@localhost:9098:6znU-ou9Taa06bmEJxsjny");
            VotersRecord firstUpdateRecord = firstUpdate.toVoterSet("CONTROLLER")
                .toVotersRecord(ControlRecordUtils.KRAFT_VOTERS_CURRENT_VERSION);
            writeVotersRecordToLog(logFile, firstUpdateRecord, 1L);

            // Second update: change ports to 9099-9101 (this is the latest and should be returned)
            DynamicVoters secondUpdate = DynamicVoters.parse(
                "1@localhost:9099:4znU-ou9Taa06bmEJxsjnw,2@localhost:9100:5znU-ou9Taa06bmEJxsjnx,3@localhost:9101:6znU-ou9Taa06bmEJxsjny");
            VotersRecord secondUpdateRecord = secondUpdate.toVoterSet("CONTROLLER")
                .toVotersRecord(ControlRecordUtils.KRAFT_VOTERS_CURRENT_VERSION);
            writeVotersRecordToLog(logFile, secondUpdateRecord, 2L);

            // Read VoterSetWriteInfo which should return the LAST one (secondUpdate), not the first
            Formatter.VoterSetWriteInfo writeInfo = formatter.formatter.readVoterSetWriteInfo(testEnv.directory(0));

            // Verify we read the latest VoterSet from the log (second update, not first)
            assertNotNull(writeInfo, "Should read VoterSetWriteInfo");
            assertNotNull(writeInfo.voterSet(), "Should have VoterSet from log");
            assertEquals(3, writeInfo.voterSet().voterIds().size(), "Should have 3 voters");
            assertEquals(new OffsetAndEpoch(2, 1), writeInfo.lastOffsetAndEpoch(), "Should have found offset and epoch");
            assertEquals(KRaftVersion.KRAFT_VERSION_1.featureLevel(), writeInfo.kraftVersion(), "Should have kraft.version = 1");

            VoterSet persistedVoterSet = writeInfo.voterSet();

            // Verify the persisted VoterSet matches the second update (not the first)
            VoterSet initialVoterSet = initialVoters.toVoterSet("CONTROLLER");
            VoterSet secondVoterSet = secondUpdate.toVoterSet("CONTROLLER");

            // Compare initial with second to verify only endpoints changed
            VoterSetDiff diff = VoterSetDiff.compare(initialVoterSet, secondVoterSet, "CONTROLLER");
            assertTrue(diff.onlyEndpointsChanged(), "Should only have endpoint changes from initial to second");
            assertFalse(diff.hasVoterIdChanges(), "Should not have voter ID changes");
            assertFalse(diff.hasDirectoryIdChanges(), "Should not have directory ID changes");
            assertEquals(3, diff.endpointChanges().size(), "All 3 endpoints should change");

            // Verify the persisted VoterSet matches the second update (using VoterSetDiff to handle resolved IPs)
            VoterSetDiff persistedDiff = VoterSetDiff.compare(secondVoterSet, persistedVoterSet, "CONTROLLER");
            assertFalse(persistedDiff.hasVoterIdChanges(), "Persisted should have same voter IDs as second update");
            assertFalse(persistedDiff.hasDirectoryIdChanges(), "Persisted should have same directory IDs as second update");
            assertTrue(persistedDiff.endpointChanges().isEmpty(), "Persisted should have same endpoints as second update");
        }
    }

    @Test
    public void testOverrideDoesNotFailOnFormattedStorage() throws Exception {
        try (TestEnv testEnv = new TestEnv(1)) {
            // Step 1: Format storage initially
            DynamicVoters initialVoters = DynamicVoters.parse("1@localhost:9093:4znU-ou9Taa06bmEJxsjnw");
            FormatterContext formatter1 = testEnv.newFormatter();
            formatter1.formatter
                    .setUnstableFeatureVersionsEnabled(true)
                    .setInitialControllers(initialVoters)
                    .setHasDynamicQuorum(true)
                    .setFeatureLevel(KRaftVersion.FEATURE_NAME, KRaftVersion.KRAFT_VERSION_1.featureLevel())
                    .run();

            // Step 2: Run format --override on already-formatted storage
            // This should NOT throw "already formatted" error
            DynamicVoters newVoters = DynamicVoters.parse("1@localhost:9094:4znU-ou9Taa06bmEJxsjnw");
            FormatterContext formatter2 = testEnv.newFormatter();
            formatter2.formatter
                    .setUnstableFeatureVersionsEnabled(true)
                    .setInitialControllers(newVoters)
                    .setHasDynamicQuorum(true)
                    .setFeatureLevel(KRaftVersion.FEATURE_NAME, KRaftVersion.KRAFT_VERSION_1.featureLevel())
                    .setOverride(true);

            // This should not throw an exception
            assertDoesNotThrow(() -> formatter2.formatter.run(),
                    "Format with --override should not fail on already-formatted storage");

            // Verify output contains expected messages
            String output = formatter2.output();
            assertTrue(output.contains("Override mode enabled"), "Should show override mode message");
            assertTrue(output.contains("Validation: PASSED"), "Should show validation passed");
        }
    }

    @Test
    public void testOverrideWithEndpointChanges() throws Exception {
        try (TestEnv testEnv = new TestEnv(1)) {
            // Step 1: Format storage with initial endpoints
            DynamicVoters initialVoters = DynamicVoters.parse(
                "1@localhost:9093:4znU-ou9Taa06bmEJxsjnw,2@localhost:9094:5znU-ou9Taa06bmEJxsjnx,3@localhost:9095:6znU-ou9Taa06bmEJxsjny");
            FormatterContext formatter1 = testEnv.newFormatter();
            formatter1.formatter
                .setUnstableFeatureVersionsEnabled(true)
                .setInitialControllers(initialVoters)
                .setHasDynamicQuorum(true)
                .setFeatureLevel(KRaftVersion.FEATURE_NAME, KRaftVersion.KRAFT_VERSION_1.featureLevel())
                .run();

            // Step 2: Run format --override with new endpoints (only port changes)
            DynamicVoters newVoters = DynamicVoters.parse(
                "1@localhost:9096:4znU-ou9Taa06bmEJxsjnw,2@localhost:9097:5znU-ou9Taa06bmEJxsjnx,3@localhost:9098:6znU-ou9Taa06bmEJxsjny");
            FormatterContext formatter2 = testEnv.newFormatter();
            formatter2.formatter
                .setUnstableFeatureVersionsEnabled(true)
                .setInitialControllers(newVoters)
                .setHasDynamicQuorum(true)
                .setFeatureLevel(KRaftVersion.FEATURE_NAME, KRaftVersion.KRAFT_VERSION_1.featureLevel())
                .setOverride(true)
                .run();

            // Verify output messages
            String output = formatter2.output();
            assertTrue(output.contains("Override mode enabled"), "Should show override mode enabled");
            assertTrue(output.contains("Persisted VoterSet"), "Should show persisted VoterSet");
            assertTrue(output.contains("Provided VoterSet"), "Should show provided VoterSet");
            assertTrue(output.contains("Changes detected"), "Should show changes detected");
            assertTrue(output.contains("Validation: PASSED"), "Should pass validation for endpoint changes");
            assertTrue(output.contains("only endpoints changed"), "Should indicate only endpoint changes");
        }
    }

    @Test
    public void testOverrideIdempotence() throws Exception {
        try (TestEnv testEnv = new TestEnv(1)) {
            // Step 1: Format storage
            DynamicVoters voters = DynamicVoters.parse("1@localhost:9093:4znU-ou9Taa06bmEJxsjnw");
            FormatterContext formatter1 = testEnv.newFormatter();
            formatter1.formatter
                .setUnstableFeatureVersionsEnabled(true)
                .setInitialControllers(voters)
                .setHasDynamicQuorum(true)
                .setFeatureLevel(KRaftVersion.FEATURE_NAME, KRaftVersion.KRAFT_VERSION_1.featureLevel())
                .run();

            // Step 2: Run format --override with SAME endpoints (idempotence test)
            FormatterContext formatter2 = testEnv.newFormatter();
            formatter2.formatter
                .setUnstableFeatureVersionsEnabled(true)
                .setInitialControllers(voters)
                .setHasDynamicQuorum(true)
                .setFeatureLevel(KRaftVersion.FEATURE_NAME, KRaftVersion.KRAFT_VERSION_1.featureLevel())
                .setOverride(true)
                .run();

            // Verify idempotence - should skip with no changes
            String output = formatter2.output();
            assertTrue(output.contains("Override mode enabled"), "Should show override mode enabled");
            assertTrue(output.contains("No changes detected (VoterSets are equivalent). Override operation skipped, already up to date."), "Should detect no changes and skip override");
        }
    }

    @Test
    public void testOverrideRejectsVoterIdChanges() throws Exception {
        try (TestEnv testEnv = new TestEnv(1)) {
            // Step 1: Format storage with voters 1,2,3
            DynamicVoters initialVoters = DynamicVoters.parse(
                "1@localhost:9093:4znU-ou9Taa06bmEJxsjnw,2@localhost:9094:5znU-ou9Taa06bmEJxsjnx,3@localhost:9095:6znU-ou9Taa06bmEJxsjny");
            FormatterContext formatter1 = testEnv.newFormatter();
            formatter1.formatter
                .setUnstableFeatureVersionsEnabled(true)
                .setInitialControllers(initialVoters)
                .setHasDynamicQuorum(true)
                .setFeatureLevel(KRaftVersion.FEATURE_NAME, KRaftVersion.KRAFT_VERSION_1.featureLevel())
                .run();

            // Step 2: Try to override with different voter IDs (1,2,4 instead of 1,2,3)
            DynamicVoters newVoters = DynamicVoters.parse(
                "1@localhost:9093:4znU-ou9Taa06bmEJxsjnw,2@localhost:9094:5znU-ou9Taa06bmEJxsjnx,4@localhost:9096:7znU-ou9Taa06bmEJxsjnz");
            FormatterContext formatter2 = testEnv.newFormatter();
            formatter2.formatter
                .setUnstableFeatureVersionsEnabled(true)
                .setInitialControllers(newVoters)
                .setHasDynamicQuorum(true)
                .setFeatureLevel(KRaftVersion.FEATURE_NAME, KRaftVersion.KRAFT_VERSION_1.featureLevel())
                .setOverride(true);

            // Should throw exception for voter ID changes
            FormatterException exception = assertThrows(FormatterException.class,
                () -> formatter2.formatter.run());
            assertTrue(exception.getMessage().contains("--override cannot be used for changing node IDs or directory IDs."),
                "Should reject voter ID changes");
        }
    }

    @Test
    public void testOverrideRejectsDirectoryIdChanges() throws Exception {
        try (TestEnv testEnv = new TestEnv(1)) {
            // Step 1: Format storage with original directory IDs
            DynamicVoters initialVoters = DynamicVoters.parse("1@localhost:9093:4znU-ou9Taa06bmEJxsjnw");
            FormatterContext formatter1 = testEnv.newFormatter();
            formatter1.formatter
                .setUnstableFeatureVersionsEnabled(true)
                .setInitialControllers(initialVoters)
                .setHasDynamicQuorum(true)
                .setFeatureLevel(KRaftVersion.FEATURE_NAME, KRaftVersion.KRAFT_VERSION_1.featureLevel())
                .run();

            // Step 2: Try to override with DIFFERENT directory ID (critical safety check)
            DynamicVoters newVoters = DynamicVoters.parse("1@localhost:9093:5znU-ou9Taa06bmEJxsjnx");
            FormatterContext formatter2 = testEnv.newFormatter();
            formatter2.formatter
                .setUnstableFeatureVersionsEnabled(true)
                .setInitialControllers(newVoters)
                .setHasDynamicQuorum(true)
                .setFeatureLevel(KRaftVersion.FEATURE_NAME, KRaftVersion.KRAFT_VERSION_1.featureLevel())
                .setOverride(true);

            // Should throw exception for directory ID changes (prevents data loss)
            FormatterException exception = assertThrows(FormatterException.class,
                () -> formatter2.formatter.run());
            assertTrue(exception.getMessage().contains("--override cannot be used for changing node IDs or directory IDs."),
                "Should reject directory ID changes");
        }
    }

    /**
     * Writes a VotersRecord as a control record to a log file.
     * Uses MemoryRecords.withVotersRecord() factory method (similar to RecordsIteratorTest.buildControlRecords).
     */
    private void writeVotersRecordToLog(Path logFile, VotersRecord votersRecord, long offset) throws Exception {
        MemoryRecords memoryRecords = MemoryRecords.withVotersRecord(
            offset,
            System.currentTimeMillis(),
            1, // leader epoch
            ByteBuffer.allocate(256), // 256 bytes is sufficient for VotersRecord with 3 voters
            votersRecord
        );

        // Write to log file
        try (FileRecords fileRecords = FileRecords.open(logFile.toFile())) {
            fileRecords.append(memoryRecords);
            fileRecords.flush();
        }
    }
}
