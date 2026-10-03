# drivesync 6420600 confirm review (Opus)

Imported confirmation proofs. Every test fails on 6420600; the before-runs and
fixture adaptations are documented in ../README.md.

    go test -race ./reviewtests/confirmopus -count=1 -timeout 180s

| Test | Finding |
| --- | --- |
| TestGCScalesQuadraticallyWithOwners, TestGCNeverCompletesWithSixThousandOwners, TestRunGCStallsServerAtModestOwnerCount | Finish recomputes contributions per owner key: GC is O(owners x folders x (tickets+garbage)) under the writer lock |
| TestListFoldersQuadraticInSharingOwners | Same cause, for a principal shared into many owners' folders |
| TestRecreateAfterCompactionOnCurrentReplica | Stale tombstone base after compaction: a recreated path always becomes a conflict copy |
| TestVictimLatencyGrowsWithOtherTenantRows | Global writer lock + full-folder decode: victim latency tracks another tenant's row count |
| TestFreshAttachNeverSyncsBusyFolder, TestFreshAttachBusyFolderHTTP | Reconcile demands a quiescent folder 3 times; fresh replica never syncs a busy folder |
| TestDeletedDirectoryResurrectedByIgnoredFile, TestCaseOnlyDirectoryRenameObservedMidway | Remote directory deletes are marked applied while the directory remains, then re-uploaded |
| TestSamePrincipalReserveCancelsSiblingUpload, TestSamePrincipalReplicasLivelockOnLargeUpload | Same-principal Reserve retires a sibling replica's in-flight ticket |
| TestPullIgnoresFullDelta, TestHTTPChangesFullFlipsMidPagination | Pull ignores Delta.Full; ChangesPage decides Full per page |
| TestDirectoryMoveRenameDetectionIsQuadratic | Moving an N-file directory is O(N^2) |
| TestReconcileDeleteFailureBlocksAllSync | One failing local delete in reconcile aborts every sync |
| TestExistingRowDeletesExceedAllocatedCharge | Documented delete bookkeeping overshoot; persistent while blob deletes fail |

Size knobs: GC_OWNERS, GC_GARBAGE, LIST_OWNERS, BIG_ROWS, MOVE_FILES.
