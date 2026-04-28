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

import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.raft.VoterSet;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Represents differences between two VoterSets.
 * Used for validating --override safety: only endpoint changes are allowed.
 */
public class VoterSetDiff {
    private final boolean hasVoterIdChanges;
    private final boolean hasDirectoryIdChanges;
    private final Map<Integer, InetSocketAddress> endpointChanges;

    private VoterSetDiff(
        boolean hasVoterIdChanges,
        boolean hasDirectoryIdChanges,
        Map<Integer, InetSocketAddress> endpointChanges
    ) {
        this.hasVoterIdChanges = hasVoterIdChanges;
        this.hasDirectoryIdChanges = hasDirectoryIdChanges;
        this.endpointChanges = endpointChanges;
    }

    /**
     * Returns true if only endpoints changed (safe for --override).
     * Voter ID or directory ID changes are rejected.
     */
    public boolean onlyEndpointsChanged() {
        return !hasVoterIdChanges && !hasDirectoryIdChanges && !endpointChanges.isEmpty();
    }

    public boolean hasVoterIdChanges() {
        return hasVoterIdChanges;
    }

    public boolean hasDirectoryIdChanges() {
        return hasDirectoryIdChanges;
    }

    public Map<Integer, InetSocketAddress> endpointChanges() {
        return endpointChanges;
    }

    @Override
    public String toString() {
        return "VoterSetDiff{" +
               "hasVoterIdChanges=" + hasVoterIdChanges +
               ", hasDirectoryIdChanges=" + hasDirectoryIdChanges +
               ", endpointChanges=" + endpointChanges +
               '}';
    }

    /**
     * Compare two VoterSets and return detailed diff.
     *
     * Only compares the specified listener (controller listener) since --initial-controllers
     * only creates one listener per voter.
     *
     * @param persisted The VoterSet currently persisted in the metadata log
     * @param provided The VoterSet provided via --initial-controllers
     * @param controllerListenerName The listener name to use for endpoint comparison
     * @return VoterSetDiff containing detected changes
     */
    public static VoterSetDiff compare(
        VoterSet persisted,
        VoterSet provided,
        String controllerListenerName
    ) {
        Set<Integer> persistedIds = persisted.voterIds();
        Set<Integer> providedIds = provided.voterIds();

        // Check voter ID changes (additions/removals)
        boolean hasVoterIdChanges = !persistedIds.equals(providedIds);

        // Build maps from voter ID to VoterNode for easy lookup
        Map<Integer, VoterSet.VoterNode> persistedMap = buildVoterMap(persisted);
        Map<Integer, VoterSet.VoterNode> providedMap = buildVoterMap(provided);

        // Check directory ID and endpoint changes for common voters
        Set<Integer> commonIds = new HashSet<>(persistedIds);
        commonIds.retainAll(providedIds);

        boolean hasDirectoryIdChanges = false;
        Map<Integer, InetSocketAddress> endpointChanges = new HashMap<>();

        for (int voterId : commonIds) {
            VoterSet.VoterNode persistedNode = persistedMap.get(voterId);
            VoterSet.VoterNode providedNode = providedMap.get(voterId);

            // Check directory ID (CRITICAL: must not change - prevents data loss)
            if (!persistedNode.voterKey().directoryId().equals(providedNode.voterKey().directoryId())) {
                hasDirectoryIdChanges = true;
            }

            // Check endpoints (DNS/port changes) for the controller listener only
            InetSocketAddress persistedAddr = persistedNode.listeners()
                .address(new ListenerName(controllerListenerName)).orElse(null);
            InetSocketAddress providedAddr = providedNode.listeners()
                .address(new ListenerName(controllerListenerName)).orElse(null);

            // Compare hostname and port only, not resolved IP address
            // This prevents false positives when one address is resolved and the other is not
            if (persistedAddr != null && providedAddr != null && !endpointsMatch(persistedAddr, providedAddr)) {
                endpointChanges.put(voterId, providedAddr);
            }
        }

        return new VoterSetDiff(hasVoterIdChanges, hasDirectoryIdChanges, endpointChanges);
    }

    /**
     * Compare two InetSocketAddress objects by hostname and port only, ignoring resolved IP.
     *
     * This prevents false positives when comparing endpoints where one has been DNS-resolved
     * and the other hasn't. For example:
     *   - localhost/<unresolved>:9093
     *   - localhost/127.0.0.1:9093
     * These should be considered the same endpoint.
     *
     * @param addr1 First address
     * @param addr2 Second address
     * @return true if hostname and port match, false otherwise
     */
    private static boolean endpointsMatch(InetSocketAddress addr1, InetSocketAddress addr2) {
        return addr1.getHostString().equals(addr2.getHostString()) && addr1.getPort() == addr2.getPort();
    }

    /**
     * Build a map from voter ID to VoterNode for easy lookup.
     */
    private static Map<Integer, VoterSet.VoterNode> buildVoterMap(VoterSet voterSet) {
        Map<Integer, VoterSet.VoterNode> map = new HashMap<>();
        for (VoterSet.VoterNode node : voterSet.voterNodes()) {
            map.put(node.voterKey().id(), node);
        }
        return map;
    }
}
