Phase 1: Parallel Feature Development (Isolation)
Create Feature Branch: Start from the currently deployed release branch (e.g., branch-release/7.5.1).

git checkout -b feature/A branch-release/7.5.1

Daily Development: Developers pull their own sub-branches from feature/A. Once development is complete, changes are merged back into feature/A via Pull Request (PR).

Initial QA Testing: Build the test package (e.g., Build 101) directly from feature/A. QA verifies the logical correctness of the specific feature in isolation.

Phase 2: Release Integration (Deterministic Merging)
When the scope for the next Release (7.5.2) is decided (e.g., including only Feature B):

Create Release Branch: Use the production environment (7.5.1) as the baseline to pull a brand-new release branch.

git checkout -b branch-release/7.5.2 branch-release/7.5.1

Cherry-pick Features: * Switch to your local branch-release/7.5.2.

Cherry-pick the relevant commits from feature/B. (Recommendation: If there are many commits, perform a Squash on the feature/B branch first.)

Conflict Resolution & Synchronization (PR): * Push the merged code to a temporary branch: sync-B-to-7.5.2.

Open a PR to merge into branch-release/7.5.2. This step maintains an audit trail and allows colleagues to review that conflicts were resolved correctly.

Regression Testing: Generate a build from branch-release/7.5.2 for QA to perform final regression testing before the go-live.

Phase 3: Deployment & Solidification (Production Snapshot)
Execute Deployment: Deploy the code from branch-release/7.5.2 to the production environment.

Confirm Stability: Monitor production performance (e.g., ensure smoke tests pass and there is no risk of rollback within the first 24 hours).

Apply Retroactive Tag (Permanent Milestone): Once stability is confirmed, apply a formal tag to the specific commit used for deployment.

git checkout branch-release/7.5.2

git tag -a v7.5.2 -m "Production release of 7.5.2 - Feature B included"

git push origin v7.5.2

Phase 4: Post-Release Maintenance (Synchronization)
Rebasing Future Features: If Feature A is postponed to the next version, its "base" must now be updated to 7.5.2.

git checkout feature/A

git rebase v7.5.2 (This aligns the development foundation of Feature A with the latest production code).
