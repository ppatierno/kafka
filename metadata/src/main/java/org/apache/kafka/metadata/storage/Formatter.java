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
import org.apache.kafka.common.message.KRaftVersionRecord;
import org.apache.kafka.common.message.VotersRecord;
import org.apache.kafka.common.utils.internals.BufferSupplier;
import org.apache.kafka.common.utils.internals.LogContext;
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
import org.apache.kafka.server.common.OffsetAndEpoch;
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
                        controllerListenerName,
                        Snapshots.BOOTSTRAP_SNAPSHOT_ID);
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
        String controllerListenerName,
        OffsetAndEpoch snapshotId
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
                snapshotId)).
            setKraftVersion(KRaftVersion.fromFeatureLevel(kraftVersion)).
            setVoterSet(Optional.of(voterSet));
        try (RecordsSnapshotWriter<ApiMessageAndVersion> writer = builder.build(new MetadataRecordSerde())) {
            writer.freeze();
        }
    }

    /**
     * Handle --override mode: update VoterSet if needed.
     *
     * This method allows to override the VoterSet in case of endpoints change (DNS/port) within the current KRaft quorum.
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

        // Read persisted state from metadata log or snapshot
        VoterSetWriteInfo writeInfo = readVoterSetWriteInfo(logDir);
        printStream.println("VoterSetWriteInfo:");
        printStream.println(writeInfo);
        printStream.println();

        VoterSet persistedVoterSet = writeInfo.voterSet();
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

        // Create snapshot with updated VoterSet at next offset
        createSnapshotWithUpdatedVoters(logDir, writeInfo, providedVoterSet);
    }

    /**
     * Create a snapshot with updated VoterSet at the next log offset.
     *
     * This method is used by --override to create a new snapshot containing
     * the updated VotersRecord with new DNS endpoints. The snapshot is created
     * at the NEXT offset after the current log end, allowing controllers to load
     * the updated VoterSet on restart without requiring quorum.
     *
     * @param logDir The log directory containing the metadata log
     * @param writeInfo VoterSet write information including last offset, epoch, and kraftVersion
     * @param updatedVoterSet The new VoterSet with updated endpoints to write to the snapshot
     * @throws Exception if snapshot creation fails
     */
    private void createSnapshotWithUpdatedVoters(String logDir, VoterSetWriteInfo writeInfo, VoterSet updatedVoterSet) throws Exception {
        // Calculate next offset for the new snapshot
        long nextOffset = writeInfo.lastOffsetAndEpoch().offset() + 1;
        int currentEpoch = writeInfo.lastOffsetAndEpoch().epoch();

        printStream.println("Creating snapshot at offset " + nextOffset +
                           ", epoch " + currentEpoch + " with updated VotersRecord...");

        // Get the metadata log directory
        File parentDir = new File(logDir);
        File clusterMetadataDirectory = new File(parentDir, String.format("%s-%d",
                CLUSTER_METADATA_TOPIC_PARTITION.topic(),
                CLUSTER_METADATA_TOPIC_PARTITION.partition()));

        // Create snapshot at next offset with updated VoterSet
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(nextOffset, currentEpoch);
        RecordsSnapshotWriter.Builder builder = new RecordsSnapshotWriter.Builder()
            .setLastContainedLogTimestamp(Time.SYSTEM.milliseconds())
            .setMaxBatchSizeBytes(KafkaRaftClient.MAX_BATCH_SIZE_BYTES)
            .setRawSnapshotWriter(FileRawSnapshotWriter.create(
                clusterMetadataDirectory.toPath(),
                snapshotId))
            .setKraftVersion(KRaftVersion.fromFeatureLevel(writeInfo.kraftVersion()))
            .setVoterSet(Optional.of(updatedVoterSet));

        try (RecordsSnapshotWriter<ApiMessageAndVersion> writer = builder.build(new MetadataRecordSerde())) {
            writer.freeze();
        }

        // Log snapshot creation details for operators
        String snapshotFilename = String.format("%020d-%010d.checkpoint", nextOffset, currentEpoch);
        printStream.println("Snapshot created: " + snapshotFilename);
        printStream.println();
        printStream.println("Override complete. Kafka will load updated VoterSet on startup.");
    }

    /**
     * Read all information needed for VoterSet update from metadata log or snapshot.
     *
     * This method orchestrates reading from multiple sources:
     * 1. Try reading from metadata log segments (most up-to-date)
     * 2. If log data incomplete or missing, supplement/fallback to snapshot
     *
     * @param logDir The log directory containing the metadata log
     * @return VoterSetWriteInfo with all necessary information
     * @throws Exception if reading fails or no data found
     */
    VoterSetWriteInfo readVoterSetWriteInfo(String logDir) throws Exception {
        Path metadataLogPath = Paths.get(logDir, String.format("%s-%d",
            CLUSTER_METADATA_TOPIC_PARTITION.topic(),
            CLUSTER_METADATA_TOPIC_PARTITION.partition()));

        if (!Files.exists(metadataLogPath)) {
            throw new FormatterException("Metadata log directory not found: " + metadataLogPath +
                ". The directory may not be formatted with dynamic quorum mode.");
        }

        // Try reading from metadata log first
        VoterSetWriteInfo writeInfo = readVoterSetWriteInfoFromMetadataLog(metadataLogPath);

        // If no log data or missing VoterSet, read from snapshot
        if (writeInfo == null || writeInfo.voterSet() == null) {
            // Find latest snapshot
            List<Path> snapshots = new ArrayList<>();
            try (Stream<Path> paths = Files.list(metadataLogPath)) {
                paths.filter(p -> p.toString().endsWith(".checkpoint"))
                     .sorted(Comparator.comparing(Path::getFileName).reversed())
                     .forEach(snapshots::add);
            }

            if (snapshots.isEmpty()) {
                throw new FormatterException("No metadata log data or snapshots found in " + metadataLogPath +
                    ". The directory may not be formatted with dynamic quorum mode.");
            }

            VoterSetWriteInfo snapshotInfo = readVoterSetWriteInfoFromSnapshot(snapshots.get(0));

            // If we have log info but no VoterSet, supplement with VoterSet from snapshot
            if (writeInfo != null) {
                writeInfo = new VoterSetWriteInfo(
                    writeInfo.lastOffsetAndEpoch(),
                    writeInfo.kraftVersion(),
                    snapshotInfo.voterSet()
                );
            } else {
                // No log data at all, use snapshot entirely
                writeInfo = snapshotInfo;
            }
        }

        return writeInfo;
    }

    /**
     * Holds all the information needed to write a snapshot with updated VoterSet.
     * Extracted in a single pass through log segments or snapshot for efficiency.
     *
     * @param lastOffsetAndEpoch Last offset and epoch in the metadata log/snapshot (where the new snapshot will be created)
     * @param kraftVersion kraft.version to include in snapshot
     * @param voterSet Most recent VoterSet found while reading to lastOffsetAndEpoch (may be null if not found in log)
     */
    record VoterSetWriteInfo(OffsetAndEpoch lastOffsetAndEpoch, short kraftVersion, VoterSet voterSet) { }

    /**
     * Read information needed for VoterSet update in a single pass through log segments.
     *
     * This method extracts from log segments ONLY:
     * - Offset and epoch (where to create new snapshot)
     * - kraft.version (to include in snapshot)
     * - Latest VoterSet (may be null if not found in log segments)
     *
     * Returns null if no usable log data exists (no segments or empty segments).
     * Caller should fall back to reading from snapshot.
     *
     * @param metadataLogPath Path to the metadata log directory
     * @return VoterSetWriteInfo with extracted information, or null if no log data
     * @throws Exception if reading fails
     */
    VoterSetWriteInfo readVoterSetWriteInfoFromMetadataLog(Path metadataLogPath) throws Exception {
        if (!Files.exists(metadataLogPath)) {
            return null;
        }

        // Find all .log segment files (sorted newest first for VoterSet search)
        List<Path> logSegments = new ArrayList<>();
        try (Stream<Path> paths = Files.list(metadataLogPath)) {
            paths.filter(p -> p.toString().endsWith(".log"))
                 .sorted(Comparator.reverseOrder())  // Newest first
                 .forEach(logSegments::add);
        }

        long maxOffset = -1;
        int maxEpoch = 0;
        short kraftVersion = KRAFT_VERSION_1.featureLevel(); // Default to version 1
        VoterSet latestVoterSet = null;

        // Single pass through log segments: extract everything
        for (Path segmentPath : logSegments) {
            try (BatchFileReader reader = new BatchFileReader.Builder()
                    .setPath(segmentPath.toString())
                    .build()) {

                while (reader.hasNext()) {
                    BatchFileReader.BatchAndType batchAndType = reader.next();
                    Batch<ApiMessageAndVersion> batch = batchAndType.batch();

                    // Track the highest offset and epoch
                    long lastOffset = batch.lastOffset();
                    if (lastOffset > maxOffset) {
                        maxOffset = lastOffset;
                        maxEpoch = batch.epoch();
                    }

                    // Extract control records (VotersRecord and KRaftVersionRecord)
                    if (batchAndType.isControl()) {
                        for (ApiMessageAndVersion record : batch.records()) {
                            if (record.message() instanceof VotersRecord) {
                                // Keep scanning to find the latest one in this segment
                                latestVoterSet = VoterSet.fromVotersRecord((VotersRecord) record.message());
                            } else if (record.message() instanceof KRaftVersionRecord) {
                                kraftVersion = ((KRaftVersionRecord) record.message()).kRaftVersion();
                            }
                        }
                    }
                }
            }

            // Optimization: If we found a VoterSet in the newest segment, we can stop
            // (segments are sorted newest-first)
            if (latestVoterSet != null) {
                break;
            }
        }

        // No usable log data - caller should read from snapshot
        if (maxOffset < 0) {
            return null;
        }

        // Return with potentially null voterSet (caller will handle fallback to snapshot)
        return new VoterSetWriteInfo(
            new OffsetAndEpoch(maxOffset, maxEpoch),
            kraftVersion,
            latestVoterSet
        );
    }

    /**
     * Read information needed for VoterSet update from a snapshot file.
     *
     * Extracts:
     * - Offset and epoch (from snapshot filename)
     * - kraft.version (from KRaftVersionRecord in snapshot)
     * - VoterSet (from VotersRecord in snapshot)
     *
     * @param snapshotPath Path to the snapshot file
     * @return VoterSetWriteInfo with all information from snapshot
     * @throws Exception if reading fails or snapshot is invalid
     */
    VoterSetWriteInfo readVoterSetWriteInfoFromSnapshot(Path snapshotPath) throws Exception {
        Optional<SnapshotPath> parsedSnapshot = Snapshots.parse(snapshotPath);
        if (parsedSnapshot.isEmpty()) {
            throw new FormatterException("Invalid snapshot file: " + snapshotPath);
        }

        SnapshotPath snapshot = parsedSnapshot.get();
        Path logDir = snapshotPath.getParent();

        short kraftVersion = KRAFT_VERSION_1.featureLevel(); // Default
        VoterSet voterSet = null;
        long lastOffset = -1;
        int lastEpoch = 0;

        try (RecordsSnapshotReader<ApiMessageAndVersion> reader = RecordsSnapshotReader.of(
                FileRawSnapshotReader.open(logDir, snapshot.snapshotId()),
                new MetadataRecordSerde(),
                BufferSupplier.create(),
                Integer.MAX_VALUE,
                true,
                new LogContext())) {

            while (reader.hasNext()) {
                Batch<ApiMessageAndVersion> batch = reader.next();

                // Track the last offset in the snapshot
                // For bootstrap snapshots, this will be 3 (SnapshotFooter), not 0 (from filename)
                lastOffset = batch.lastOffset();
                lastEpoch = batch.epoch();

                // Extract control records
                for (ControlRecord controlRecord : batch.controlRecords()) {
                    if (controlRecord.message() instanceof VotersRecord) {
                        voterSet = VoterSet.fromVotersRecord((VotersRecord) controlRecord.message());
                    } else if (controlRecord.message() instanceof KRaftVersionRecord) {
                        kraftVersion = ((KRaftVersionRecord) controlRecord.message()).kRaftVersion();
                    }
                }
            }
        }

        if (voterSet == null) {
            throw new FormatterException("No VotersRecord found in snapshot: " + snapshotPath);
        }

        // Use the actual last offset from the snapshot contents, not snapshot.snapshotId()
        // For bootstrap snapshots: filename shows (0,0) but actual last offset is 3
        return new VoterSetWriteInfo(
            new OffsetAndEpoch(lastOffset, lastEpoch),
            kraftVersion,
            voterSet
        );
    }
}
