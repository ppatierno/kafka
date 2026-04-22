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

import org.apache.kafka.raft.DynamicVoters;
import org.apache.kafka.raft.VoterSet;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 40)
public class VoterSetDiffTest {

    @Test
    public void testCompareWithOnlyEndpointChanges() {
        // Create two VoterSets with same IDs and directory IDs but different endpoints
        DynamicVoters voters1 = DynamicVoters.parse("0@host1:9093:4znU-ou9Taa06bmEJxsjnw,1@host2:9094:5znU-ou9Taa06bmEJxsjnx,2@host3:9095:6znU-ou9Taa06bmEJxsjny");
        DynamicVoters voters2 = DynamicVoters.parse("0@host1-new:9093:4znU-ou9Taa06bmEJxsjnw,1@host2-new:9094:5znU-ou9Taa06bmEJxsjnx,2@host3-new:9095:6znU-ou9Taa06bmEJxsjny");

        VoterSet voterSet1 = voters1.toVoterSet("CONTROLLER");
        VoterSet voterSet2 = voters2.toVoterSet("CONTROLLER");

        VoterSetDiff diff = VoterSetDiff.compare(voterSet1, voterSet2, "CONTROLLER");

        // Should detect only endpoint changes
        assertTrue(diff.onlyEndpointsChanged(), "Should only have endpoint changes");
        assertFalse(diff.hasVoterIdChanges(), "No voter ID changes");
        assertFalse(diff.hasDirectoryIdChanges(), "No directory ID changes");
        assertEquals(3, diff.endpointChanges().size(), "All 3 endpoints should change");
    }

    @Test
    public void testCompareRejectsVoterIdChanges() {
        // Voter 2 removed, voter 3 added
        DynamicVoters voters1 = DynamicVoters.parse("0@host1:9093:4znU-ou9Taa06bmEJxsjnw,1@host2:9094:5znU-ou9Taa06bmEJxsjnx,2@host3:9095:6znU-ou9Taa06bmEJxsjny");
        DynamicVoters voters2 = DynamicVoters.parse("0@host1:9093:4znU-ou9Taa06bmEJxsjnw,1@host2:9094:5znU-ou9Taa06bmEJxsjnx,3@host4:9096:7znU-ou9Taa06bmEJxsjnz");

        VoterSet voterSet1 = voters1.toVoterSet("CONTROLLER");
        VoterSet voterSet2 = voters2.toVoterSet("CONTROLLER");

        VoterSetDiff diff = VoterSetDiff.compare(voterSet1, voterSet2, "CONTROLLER");

        // Should NOT be considered "only endpoints changed"
        assertFalse(diff.onlyEndpointsChanged(), "Should reject voter ID changes");
        assertTrue(diff.hasVoterIdChanges(), "Should detect voter ID changes");
        assertFalse(diff.hasDirectoryIdChanges(), "No directory ID changes");
    }

    @Test
    public void testCompareRejectsDirectoryIdChanges() {
        // Same voter IDs, but directory ID changed for voter 1 (DANGEROUS!)
        DynamicVoters voters1 = DynamicVoters.parse("0@host1:9093:4znU-ou9Taa06bmEJxsjnw,1@host2:9094:5znU-ou9Taa06bmEJxsjnx,2@host3:9095:6znU-ou9Taa06bmEJxsjny");
        DynamicVoters voters2 = DynamicVoters.parse("0@host1:9093:4znU-ou9Taa06bmEJxsjnw,1@host2:9094:8znU-ou9Taa06bmEJxsjnX,2@host3:9095:6znU-ou9Taa06bmEJxsjny");

        VoterSet voterSet1 = voters1.toVoterSet("CONTROLLER");
        VoterSet voterSet2 = voters2.toVoterSet("CONTROLLER");

        VoterSetDiff diff = VoterSetDiff.compare(voterSet1, voterSet2, "CONTROLLER");

        // Should NOT be considered "only endpoints changed" - directory ID changed!
        assertFalse(diff.onlyEndpointsChanged(), "Should reject directory ID changes");
        assertFalse(diff.hasVoterIdChanges(), "No voter ID changes");
        assertTrue(diff.hasDirectoryIdChanges(), "Should detect directory ID changes");
    }

    @Test
    public void testCompareWithNoChanges() {
        // Same VoterSets
        DynamicVoters voters1 = DynamicVoters.parse("0@host1:9093:4znU-ou9Taa06bmEJxsjnw,1@host2:9094:5znU-ou9Taa06bmEJxsjnx,2@host3:9095:6znU-ou9Taa06bmEJxsjny");
        DynamicVoters voters2 = DynamicVoters.parse("0@host1:9093:4znU-ou9Taa06bmEJxsjnw,1@host2:9094:5znU-ou9Taa06bmEJxsjnx,2@host3:9095:6znU-ou9Taa06bmEJxsjny");

        VoterSet voterSet1 = voters1.toVoterSet("CONTROLLER");
        VoterSet voterSet2 = voters2.toVoterSet("CONTROLLER");

        VoterSetDiff diff = VoterSetDiff.compare(voterSet1, voterSet2, "CONTROLLER");

        // No changes at all
        assertFalse(diff.onlyEndpointsChanged(), "No endpoint changes");
        assertFalse(diff.hasVoterIdChanges(), "No voter ID changes");
        assertFalse(diff.hasDirectoryIdChanges(), "No directory ID changes");
        assertEquals(0, diff.endpointChanges().size(), "No endpoints changed");
    }

    @Test
    public void testCompareWithEndpointAndPortChanges() {
        // Both hostname and port changed
        DynamicVoters voters1 = DynamicVoters.parse("0@host1:9093:4znU-ou9Taa06bmEJxsjnw");
        DynamicVoters voters2 = DynamicVoters.parse("0@host1-new:9094:4znU-ou9Taa06bmEJxsjnw");

        VoterSet voterSet1 = voters1.toVoterSet("CONTROLLER");
        VoterSet voterSet2 = voters2.toVoterSet("CONTROLLER");

        VoterSetDiff diff = VoterSetDiff.compare(voterSet1, voterSet2, "CONTROLLER");

        assertTrue(diff.onlyEndpointsChanged(), "Should only have endpoint changes");
        assertFalse(diff.hasVoterIdChanges(), "No voter ID changes");
        assertFalse(diff.hasDirectoryIdChanges(), "No directory ID changes");
        assertEquals(1, diff.endpointChanges().size(), "One endpoint changed");
    }
}
