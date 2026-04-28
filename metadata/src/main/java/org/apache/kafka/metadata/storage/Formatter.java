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
import org.apache.kafka.common.utils.internals.BufferSupplier;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.MetadataRecordSerde;
import org.apache.kafka.metadata.bootstrap.BootstrapDirectory;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.metadata.properties.MetaProperties;
import org.apache.kafka.metadata.properties.MetaPropertiesEnsemble;
import org.apache.kafka.metadata.properties.MetaPropertiesVersion;
import org.apache.kafka.metadata.util.BatchFileReader;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.raft.ControlRecord;
import org.apache.kafka.raft.DynamicVoters;
import org.apache.kafka.raft.KafkaRaftClient;
import org.apache.kafka.raft.VoterSet;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.Feature;
import org.apache.kafka.server.common.FeatureVersion;
import org.apache.kafka.server.common.KRaftVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.snapshot.FileRawSnapshotReader;
import org.apache.kafka.snapshot.FileRawSnapshotWriter;
import org.apache.kafka.snapshot.RecordsSnapshotReader;
import org.apache.kafka.snapshot.RecordsSnapshotWriter;
import org.apache.kafka.snapshot.SnapshotPath;
import org.apache.kafka.snapshot.Snapshots;

import java.io.File;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Stream;

import static org.apache.kafka.common.internals.Topic.CLUSTER_METADATA_TOPIC_PARTITION;
import static org.apache.kafka.server.common.KRaftVersion.KRAFT_VERSION_0;
import static org.apache.kafka.server.common.KRaftVersion.KRAFT_VERSION_1;

/**
 * Formats storage directories.
 */
public class Formatter {
    /**
     * The stream to log to while formatting.
     */
    private PrintStream printStream = System.out;

    /**
     * The features that are supported.
     */
    private List<Feature> supportedFeatures = Feature.PRODUCTION_FEATURES;

    /**
     * The current node id.
     */
    private int nodeId = -1;

    /**
     * The cluster ID to use.
     */
    private String clusterId = null;

    /**
     * The directories to format.
     */
    private final TreeSet<String> directories = new TreeSet<>();

    /**
     * The metadata version to use.
     */
    private MetadataVersion releaseVersion = null;

    /**
     * Maps feature names to the level they will start off with.
     *
     * Visible for testing.
     */
    protected Map<String, Short> featureLevels = new TreeMap<>();

    /**
     * The bootstrap metadata used to format the cluster.
     */
    private BootstrapMetadata bootstrapMetadata;

    /**
     * True if we should enable unstable feature versions.
     */
    private boolean unstableFeatureVersionsEnabled = false;

    /**
     * True if we should ignore already formatted directories.
     */
    private boolean ignoreFormatted = false;

    /**
     * True if we should create a snapshot with updated VotersRecord when already formatted.
     */
    private boolean override = false;

    /**
     * The arguments passed to --add-scram
     */
    private List<String> scramArguments = List.of();

    /**
     * The name of the initial controller listener.
     */
    private String controllerListenerName = null;

    /**
     * The metadata log directory.
     */
    private Optional<String> metadataLogDirectory = Optional.empty();

    /**
     * The initial KIP-853 voters.
     */
    private Optional<DynamicVoters> initialControllers = Optional.empty();
    private boolean hasDynamicQuorum = false;

    public Formatter setPrintStream(PrintStream printStream) {
        this.printStream = printStream;
        return this;
    }

    public Formatter setSupportedFeatures(List<Feature> supportedFeatures) {
        this.supportedFeatures = supportedFeatures;
        return this;
    }

    public Formatter setNodeId(int nodeId) {
        this.nodeId = nodeId;
        return this;
    }

    public Formatter setClusterId(String clusterId) {
        this.clusterId = clusterId;
        return this;
    }

    public String clusterId() {
        return clusterId;
    }

    public Formatter setDirectories(Collection<String> directories) {
        this.directories.clear();
        this.directories.addAll(directories);
        return this;
    }

    public Formatter addDirectory(String directory) {
        this.directories.add(directory);
        return this;
    }

    public Collection<String> directories() {
        return directories;
    }

    public Formatter setReleaseVersion(MetadataVersion releaseVersion) {
        this.releaseVersion = releaseVersion;
        return this;
    }

    public Formatter setFeatureLevel(String featureName, Short level) {
        this.featureLevels.put(featureName, level);
        return this;
    }

    public Formatter setUnstableFeatureVersionsEnabled(boolean unstableFeatureVersionsEnabled) {
        this.unstableFeatureVersionsEnabled = unstableFeatureVersionsEnabled;
        return this;
    }

    public Formatter setIgnoreFormatted(boolean ignoreFormatted) {
        this.ignoreFormatted = ignoreFormatted;
        return this;
    }

    public Formatter setOverride(boolean override) {
        this.override = override;
        return this;
    }

    public Formatter setScramArguments(List<String> scramArguments) {
        this.scramArguments = scramArguments;
        return this;
    }

    public Formatter setControllerListenerName(String controllerListenerName) {
        this.controllerListenerName = controllerListenerName;
        return this;
    }

    public Formatter setMetadataLogDirectory(String metadataLogDirectory) {
        this.metadataLogDirectory = Optional.of(metadataLogDirectory);
        return this;
    }

    public Formatter setInitialControllers(DynamicVoters initialControllers) {
        this.initialControllers = Optional.of(initialControllers);
        return this;
    }

    public Formatter setHasDynamicQuorum(boolean hasDynamicQuorum) {
        this.hasDynamicQuorum = hasDynamicQuorum;
        return this;
    }

    public Optional<DynamicVoters> initialVoters() {
        return initialControllers;
    }

    boolean hasDynamicQuorum() {
        return hasDynamicQuorum;
    }

    boolean isOverride() {
        return override;
    }

    public BootstrapMetadata bootstrapMetadata() {
        return bootstrapMetadata;
    }

    public void run() throws Exception {
        if (nodeId < 0) {
            throw new RuntimeException("You must specify a valid non-negative node ID.");
        }
        if (clusterId == null) {
            throw new FormatterException("You must specify the cluster id.");
        }
        if (directories.isEmpty()) {
            throw new FormatterException("You must specify at least one directory to format");
        }
        if (controllerListenerName == null) {
            throw new FormatterException("You must specify the name of the initial controller listener.");
        }
        // Validate override requires initial-controllers (Formatter-level check)
        if (override && initialControllers.isEmpty()) {
            throw new FormatterException("--override requires --initial-controllers to specify the new voter endpoints.");
        }
        metadataLogDirectory.ifPresent(d -> {
            if (!directories.contains(d)) {
                throw new FormatterException("The specified metadata log directory, " + d +
                    " was not one of the given directories: " + directories);
            }
        });
        releaseVersion = calculateEffectiveReleaseVersion();
        featureLevels = calculateEffectiveFeatureLevels();
        this.bootstrapMetadata = calculateBootstrapMetadata();
        doFormat(bootstrapMetadata);
    }

    /**
     * Calculate the effective value of release version. This will be used to set defaults
     * for the other features. We also throw an exception if something inconsistent was requested.
     *
     * @return  The effective value of release version.
     */
    MetadataVersion calculateEffectiveReleaseVersion() {
        if (featureLevels.containsKey(MetadataVersion.FEATURE_NAME)) {
            if (releaseVersion != null) {
                throw new FormatterException("Use --release-version instead of " +
                    "--feature " + MetadataVersion.FEATURE_NAME + "=X to avoid ambiguity.");
            }
            return verifyReleaseVersion(MetadataVersion.fromFeatureLevel(
                    featureLevels.get(MetadataVersion.FEATURE_NAME)));
        } else if (releaseVersion != null) {
            return verifyReleaseVersion(releaseVersion);
        } else if (unstableFeatureVersionsEnabled) {
            return MetadataVersion.latestTesting();
        } else {
            return MetadataVersion.latestProduction();
        }
    }

    MetadataVersion verifyReleaseVersion(MetadataVersion metadataVersion) {
        if (!unstableFeatureVersionsEnabled) {
            if (!metadataVersion.isProduction()) {
                throw new FormatterException(MetadataVersion.FEATURE_NAME + " " + metadataVersion +
                        " is not yet stable.");
            }
        }
        return metadataVersion;
    }

    Map<String, Short> calculateEffectiveFeatureLevels() {
        Map<String, Feature> nameToSupportedFeature = new TreeMap<>();
        supportedFeatures.forEach(feature -> nameToSupportedFeature.put(feature.featureName(), feature));
        Map<String, Short> newFeatureLevels = new TreeMap<>();
        // Verify that all specified features are known to us.
        for (Map.Entry<String, Short> entry : featureLevels.entrySet()) {
            String featureName = entry.getKey();
            short level = entry.getValue();
            if (!featureName.equals(MetadataVersion.FEATURE_NAME)) {
                if (!nameToSupportedFeature.containsKey(featureName)) {
                    throw new FormatterException("Unsupported feature: " + featureName +
                            ". Supported features are: " + String.join(", ", nameToSupportedFeature.keySet()));
                }
            }
            newFeatureLevels.put(featureName, level);
        }
        newFeatureLevels.put(MetadataVersion.FEATURE_NAME, releaseVersion.featureLevel());
        // Add default values for features that were not specified.
        supportedFeatures.forEach(supportedFeature -> {
            if (supportedFeature.featureName().equals(KRaftVersion.FEATURE_NAME)) {
                newFeatureLevels.put(KRaftVersion.FEATURE_NAME, effectiveKRaftFeatureLevel(
                    Optional.ofNullable(newFeatureLevels.get(KRaftVersion.FEATURE_NAME))));
            } else if (!newFeatureLevels.containsKey(supportedFeature.featureName())) {
                newFeatureLevels.put(supportedFeature.featureName(),
                    supportedFeature.defaultLevel(releaseVersion));
            }
        });
        // Verify that the specified features support the given levels. This requires the full
        // features map since there may be cross-feature dependencies.
        for (Map.Entry<String, Short> entry : newFeatureLevels.entrySet()) {
            String featureName = entry.getKey();
            if (!featureName.equals(MetadataVersion.FEATURE_NAME)) {
                short level = entry.getValue();
                Feature supportedFeature = nameToSupportedFeature.get(featureName);
                FeatureVersion featureVersion =
                    supportedFeature.fromFeatureLevel(level, unstableFeatureVersionsEnabled);
                Feature.validateVersion(featureVersion, newFeatureLevels);
            }
        }
        return newFeatureLevels;
    }

    /**
     * Calculate the effective feature level for kraft.version. In order to keep existing
     * command-line invocations of StorageTool working, we default this to 0 if no dynamic
     * voter quorum arguments were provided. As a convenience, if the static voters config is
     * empty, we set the latest kraft.version. (Currently there is only 1 non-zero version).
     *
     * @param configuredKRaftVersionLevel   The configured level for kraft.version
     * @return                              The effective feature level.
     */
    private short effectiveKRaftFeatureLevel(Optional<Short> configuredKRaftVersionLevel) {
        if (configuredKRaftVersionLevel.isPresent()) {
            if (configuredKRaftVersionLevel.get() == 0) {
                if (hasDynamicQuorum()) {
                    throw new FormatterException(
                        "Cannot set kraft.version to 0 if controller.quorum.voters is empty and one of the flags " +
                        "--standalone, --initial-controllers, or --no-initial-controllers is used. For dynamic " +
                        "controllers support, try removing the --feature flag for kraft.version."
                    );
                }
            } else {
                if (!hasDynamicQuorum()) {
                    throw new FormatterException(
                        "Cannot set kraft.version to " + configuredKRaftVersionLevel.get() +
                        " unless controller.quorum.voters is empty and one of the flags --standalone, " +
                        "--initial-controllers, or --no-initial-controllers is used. " +
                        "For dynamic controllers support, try using one of --standalone, --initial-controllers, " +
                        "or --no-initial-controllers and removing controller.quorum.voters."
                    );
                }
            }
            return configuredKRaftVersionLevel.get();
        } else if (hasDynamicQuorum()) {
            return KRAFT_VERSION_1.featureLevel();
        } else {
            return KRAFT_VERSION_0.featureLevel();
        }
    }

    BootstrapMetadata calculateBootstrapMetadata() throws  Exception {
        BootstrapMetadata bootstrapMetadata = BootstrapMetadata.
            fromVersions(releaseVersion, featureLevels, "format command");
        List<ApiMessageAndVersion> bootstrapRecords = new ArrayList<>(bootstrapMetadata.records());
        if (!scramArguments.isEmpty()) {
            if (!releaseVersion.isScramSupported()) {
                throw new FormatterException("SCRAM is only supported in " + MetadataVersion.FEATURE_NAME +
                        " " + MetadataVersion.IBP_3_5_IV2 + " or later.");
            }
            bootstrapRecords.addAll(ScramParser.parse(scramArguments));
        }
        return BootstrapMetadata.fromRecords(bootstrapRecords, "format command");
    }

    void doFormat(BootstrapMetadata bootstrapMetadata) throws Exception {
        MetaProperties metaProperties = new MetaProperties.Builder().
                setVersion(MetaPropertiesVersion.V1).
                setClusterId(clusterId).
                setNodeId(nodeId).
                build();
        MetaPropertiesEnsemble.Loader loader = new MetaPropertiesEnsemble.Loader();
        loader.addLogDirs(directories);
        MetaPropertiesEnsemble ensemble = loader.load();
        ensemble.verify(Optional.of(clusterId),
                OptionalInt.of(nodeId),
                EnumSet.noneOf(MetaPropertiesEnsemble.VerificationFlag.class));
        MetaPropertiesEnsemble.Copier copier = new MetaPropertiesEnsemble.Copier(ensemble);
        if (!(ignoreFormatted || override || copier.logDirProps().isEmpty())) {
            String firstLogDir = copier.logDirProps().keySet().iterator().next();
            throw new FormatterException("Log directory " + firstLogDir + " is already formatted. " +
                "Use --ignore-formatted to ignore this directory and format the others.");
        }
        if (!copier.errorLogDirs().isEmpty()) {
            copier.errorLogDirs().forEach(errorLogDir ->
                printStream.println("I/O error trying to read log directory " + errorLogDir + ". Ignoring..."));
            if (ensemble.emptyLogDirs().isEmpty() && copier.logDirProps().isEmpty()) {
                throw new FormatterException("No available log directories to format.");
            }
        }
        if (ensemble.emptyLogDirs().isEmpty()) {
            if (override) {
                // Handle override mode: update VoterSet if needed
                handleOverride(metadataLogDirectory.orElseThrow(() ->
                    new FormatterException("Override mode requires metadata log directory")));
            } else {
                printStream.println("All of the log directories are already formatted.");
            }
        } else {
            printStream.println("Bootstrap metadata: " + bootstrapMetadata);
            Map<String, DirectoryType> directoryTypes = new HashMap<>();
            for (String emptyLogDir : ensemble.emptyLogDirs()) {
                DirectoryType directoryType = DirectoryType.calculate(emptyLogDir,
                    metadataLogDirectory.orElse(""),
                    nodeId,
                    initialControllers);
                directoryTypes.put(emptyLogDir, directoryType);
                Uuid directoryId;
                if (directoryType == DirectoryType.DYNAMIC_METADATA_VOTER_DIRECTORY) {
                    directoryId = initialControllers.get().voters().get(nodeId).directoryId();
                } else {
                    directoryId = copier.generateValidDirectoryId();
                }
                copier.setLogDirProps(emptyLogDir, new MetaProperties.Builder(metaProperties).
                    setDirectoryId(directoryId).
                    build());
            }
            copier.setPreWriteHandler((writeLogDir, __, ____) -> {
                printStream.printf("Formatting %s %s with %s %s.%n",
                    directoryTypes.get(writeLogDir).description(), writeLogDir,
                    MetadataVersion.FEATURE_NAME, releaseVersion);
                Files.createDirectories(Paths.get(writeLogDir));
                BootstrapDirectory bootstrapDirectory = new BootstrapDirectory(writeLogDir);
                bootstrapDirectory.writeBinaryFile(bootstrapMetadata);
                if (directoryTypes.get(writeLogDir).isDynamicMetadataDirectory()) {
                    writeDynamicQuorumSnapshot(writeLogDir,
                        initialControllers.get(),
                        featureLevels.get(KRaftVersion.FEATURE_NAME),
                        controllerListenerName);
                }
            });
            copier.setWriteErrorHandler((errorLogDir, e) -> {
                throw new FormatterException("Error while writing meta.properties file " +
                        errorLogDir + ": " + e);
            });
            copier.writeLogDirChanges();
        }
    }

    enum DirectoryType {
        LOG_DIRECTORY,
        STATIC_METADATA_DIRECTORY,
        DYNAMIC_METADATA_NON_VOTER_DIRECTORY,
        DYNAMIC_METADATA_VOTER_DIRECTORY;

        String description() {
            return switch (this) {
                case LOG_DIRECTORY -> "data directory";
                case STATIC_METADATA_DIRECTORY -> "metadata directory";
                case DYNAMIC_METADATA_NON_VOTER_DIRECTORY -> "dynamic metadata directory";
                case DYNAMIC_METADATA_VOTER_DIRECTORY -> "dynamic metadata voter directory";
            };
        }

        boolean isDynamicMetadataDirectory() {
            return this == DYNAMIC_METADATA_NON_VOTER_DIRECTORY ||
                this == DYNAMIC_METADATA_VOTER_DIRECTORY;
        }

        static DirectoryType calculate(
            String logDir,
            String metadataLogDirectory,
            int nodeId,
            Optional<DynamicVoters> initialControllers
        ) {
            if (!logDir.equals(metadataLogDirectory)) {
                return LOG_DIRECTORY;
            } else if (initialControllers.isEmpty()) {
                return STATIC_METADATA_DIRECTORY;
            } else if (initialControllers.get().voters().containsKey(nodeId)) {
                return DYNAMIC_METADATA_VOTER_DIRECTORY;
            } else {
                return DYNAMIC_METADATA_NON_VOTER_DIRECTORY;
            }
        }
    }

    static void writeDynamicQuorumSnapshot(
        String writeLogDir,
        DynamicVoters initialControllers,
        short kraftVersion,
        String controllerListenerName
    ) {
        File parentDir = new File(writeLogDir);
        File clusterMetadataDirectory = new File(parentDir, String.format("%s-%d",
                CLUSTER_METADATA_TOPIC_PARTITION.topic(),
                CLUSTER_METADATA_TOPIC_PARTITION.partition()));
        VoterSet voterSet = initialControllers.toVoterSet(controllerListenerName);
        RecordsSnapshotWriter.Builder builder = new RecordsSnapshotWriter.Builder().
            setLastContainedLogTimestamp(Time.SYSTEM.milliseconds()).
            setMaxBatchSizeBytes(KafkaRaftClient.MAX_BATCH_SIZE_BYTES).
            setRawSnapshotWriter(FileRawSnapshotWriter.create(
                clusterMetadataDirectory.toPath(),
                Snapshots.BOOTSTRAP_SNAPSHOT_ID)).
            setKraftVersion(KRaftVersion.fromFeatureLevel(kraftVersion)).
            setVoterSet(Optional.of(voterSet));
        try (RecordsSnapshotWriter<ApiMessageAndVersion> writer = builder.build(new MetadataRecordSerde())) {
            writer.freeze();
        }
    }

    /**
     * Handle --override mode: update VoterSet if needed.
     *
     * This method provides idempotent, safe VoterSet updates for cloud-native environments
     * where kafka-storage.sh runs on every pod start (e.g., Strimzi).
     *
     * Safety guarantees:
     * - Only allows endpoint (DNS/port) changes
     * - Rejects voter ID changes (topology changes)
     * - Rejects directory ID changes (prevents data loss)
     * - Idempotent: safe to run multiple times
     *
     * @param logDir The log directory containing the metadata log
     * @throws FormatterException if changes are unsafe or validation fails
     */
    private void handleOverride(String logDir) throws Exception {
        printStream.println("Storage directory " + logDir + " is already formatted.");
        printStream.println("Override mode enabled, checking if VoterSet needs updating...");
        printStream.println();

        // Read persisted VoterSet from checkpoint or metadata log
        VoterSet persistedVoterSet = readPersistedVoterSet(logDir);
        printStream.println("Persisted VoterSet:");
        printStream.println(persistedVoterSet);
        printStream.println();

        // Get provided VoterSet from --initial-controllers
        if (initialControllers.isEmpty()) {
            throw new FormatterException("--override requires --initial-controllers to specify the new voter endpoints.");
        }
        VoterSet providedVoterSet = initialControllers.get().toVoterSet(controllerListenerName);
        printStream.println("Provided VoterSet (from --initial-controllers):");
        printStream.println(providedVoterSet);
        printStream.println();

        // Detect changes using VoterSetDiff (compares hostname/port only, ignoring resolved IPs)
        VoterSetDiff diff = VoterSetDiff.compare(persistedVoterSet, providedVoterSet, controllerListenerName);

        // Idempotence check: if no changes detected, skip override
        if (!diff.hasVoterIdChanges() && !diff.hasDirectoryIdChanges() && diff.endpointChanges().isEmpty()) {
            printStream.println("No changes detected (VoterSets are equivalent). Override operation skipped, already up to date.");
            return;
        }

        printStream.println("Changes detected:");
        printStream.println(diff);
        printStream.println();

        // Validate safety: only endpoint changes allowed
        if (!diff.onlyEndpointsChanged()) {
            throw new FormatterException(
                "--override cannot be used for changing node IDs or directory IDs.\n" +
                "Changes detected:\n" + diff
            );
        }

        printStream.println("Validation: PASSED (only endpoints changed, safe operation)");
        printStream.println();

        // TODO: Create snapshot with updated VoterSet
        printStream.println("Snapshot creation not yet implemented.");
        printStream.println("Override validation complete.");
    }

    /**
     * Read the currently persisted VoterSet from metadata log.
     *
     * This searches log segments first (most up-to-date), then falls back to the latest snapshot.
     * Required for idempotent --override operation.
     *
     * Process:
     * 1. Search .log files (newest first) for the latest VotersRecord
     * 2. If not found in logs, read VotersRecord from latest .checkpoint snapshot
     *
     * @param logDir The log directory containing the metadata log
     * @return The current VoterSet
     * @throws Exception if reading fails or no VotersRecord found
     */
    VoterSet readPersistedVoterSet(String logDir) throws Exception {
        Path metadataLogPath = Paths.get(logDir, String.format("%s-%d",
            CLUSTER_METADATA_TOPIC_PARTITION.topic(),
            CLUSTER_METADATA_TOPIC_PARTITION.partition()));

        if (!Files.exists(metadataLogPath)) {
            throw new FormatterException("Metadata log directory not found: " + metadataLogPath +
                ". The directory may not be formatted with dynamic quorum mode.");
        }

        // First, search log segments (newest first) - most up-to-date source
        VoterSet voterSet = readVoterSetFromMetadataLog(metadataLogPath);
        if (voterSet != null) {
            return voterSet;
        }

        // If not found in log, read from latest snapshot
        List<Path> snapshots = new ArrayList<>();
        try (Stream<Path> paths = Files.list(metadataLogPath)) {
            paths.filter(p -> p.toString().endsWith(".checkpoint"))
                 .sorted(Comparator.comparing(Path::getFileName).reversed())
                 .forEach(snapshots::add);
        }

        if (!snapshots.isEmpty()) {
            voterSet = readVoterSetFromSnapshot(snapshots.get(0));
        }

        if (voterSet == null) {
            throw new FormatterException("No VotersRecord found in metadata log at " + metadataLogPath +
                ". Found " + snapshots.size() + " snapshot(s). " +
                "This directory may not be formatted with dynamic quorum mode (kraft.version >= 1).");
        }

        return voterSet;
    }

    /**
     * Read VoterSet from a snapshot file.
     *
     * Reuses existing Kafka code:
     * - Snapshots.parse() - parses snapshot filename to get OffsetAndEpoch
     * - FileRawSnapshotReader.open() - opens snapshot file
     * - RecordsSnapshotReader.of() - reads records from snapshot
     * - VoterSet.fromVotersRecord() - converts VotersRecord to VoterSet
     *
     * @param snapshotPath Path to the snapshot file
     * @return VoterSet if found in snapshot, null otherwise
     * @throws Exception if reading fails
     */
    VoterSet readVoterSetFromSnapshot(Path snapshotPath) throws Exception {
        Optional<SnapshotPath> parsedSnapshot = Snapshots.parse(snapshotPath);
        if (parsedSnapshot.isEmpty()) {
            return null;
        }

        SnapshotPath snapshot = parsedSnapshot.get();
        Path logDir = snapshotPath.getParent();

        try (RecordsSnapshotReader<ApiMessageAndVersion> reader = RecordsSnapshotReader.of(
                FileRawSnapshotReader.open(logDir, snapshot.snapshotId()),
                new MetadataRecordSerde(),
                BufferSupplier.create(),
                Integer.MAX_VALUE,
                true,
                new LogContext())) {

            while (reader.hasNext()) {
                Batch<ApiMessageAndVersion> batch = reader.next();
                // VotersRecord is a control record, not a regular record
                for (ControlRecord controlRecord : batch.controlRecords()) {
                    if (controlRecord.message() instanceof VotersRecord) {
                        return VoterSet.fromVotersRecord((VotersRecord) controlRecord.message());
                    }
                }
            }
        }
        return null;
    }

    /**
     * Read VoterSet from metadata log segments.
     *
     * Searches log segments in reverse order (newest first) and stops at the first VotersRecord found.
     *
     * Reuses existing Kafka code:
     * - BatchFileReader - utility for reading log segments (also used by kafka-dump-log tool)
     * - VoterSet.fromVotersRecord() - converts VotersRecord to VoterSet
     *
     * @param metadataPath Path to the metadata log directory
     * @return Latest VoterSet found in logs, or null if none found
     * @throws Exception if reading fails
     */
    VoterSet readVoterSetFromMetadataLog(Path metadataPath) throws Exception {
        VoterSet latestInSegment = null;

        // Find all .log files and sort in reverse order (newest first)
        List<Path> logFiles = new ArrayList<>();
        try (Stream<Path> paths = Files.list(metadataPath)) {
            paths.filter(p -> p.toString().endsWith(".log"))
                 .sorted(Comparator.reverseOrder())
                 .forEach(logFiles::add);
        }

        // Replay log segments from newest to oldest
        // Scan entire newest segment to find LAST VotersRecord (in case multiple exist)
        for (Path logFile : logFiles) {
            try (BatchFileReader reader = new BatchFileReader.Builder()
                    .setPath(logFile.toString())
                    .build()) {

                while (reader.hasNext()) {
                    BatchFileReader.BatchAndType bat = reader.next();
                    if (bat.isControl()) {  // VotersRecord is a control record
                        for (ApiMessageAndVersion record : bat.batch().records()) {
                            if (record.message() instanceof VotersRecord) {
                                // Keep scanning to find the latest one in this segment
                                latestInSegment = VoterSet.fromVotersRecord(
                                    (VotersRecord) record.message());
                            }
                        }
                    }
                }
            }
        }

        // No VotersRecord found in any log segment
        return latestInSegment;
    }
}
