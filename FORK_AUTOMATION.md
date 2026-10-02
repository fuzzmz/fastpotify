# Fork synchronization and feature builds

This fork can keep `main` identical to `crmne/fastpotify:main`, rebase
`feature/new-releases` on top of it, and publish installable feature builds.
The automation lives in `.github/workflows/fork-sync.yml`.

## One-time GitHub setup

1. Push this workflow to `feature/new-releases`.
2. In **Settings > Branches > Default branch**, make
   `feature/new-releases` the fork's default branch. GitHub only runs scheduled
   workflows from the default branch. `main` cannot host the workflow because
   the automation deliberately makes it an exact copy of upstream.
3. In **Settings > Actions > General > Workflow permissions**, select
   **Read and write permissions**.
4. Add `OPENAI_API_KEY` in **Settings > Secrets and variables > Actions > New
   repository secret**. It is used only when a rebase has conflicts. The
   OpenAI action keeps it behind its API proxy and gives Codex write access
   only to the checked-out workspace, without network access. Codex runs as a
   dedicated unprivileged account on the disposable runner.
5. If branch or tag rules are enabled, permit GitHub Actions to update `main`
   and force-update `feature/new-releases`, and to force-update the rolling
   `new-releases-latest` tag. Do not make that tag immutable.

The schedule runs at 02:17, 08:17, 14:17, and 20:17 Europe/Bucharest time.
GitHub may delay scheduled jobs during busy periods. For a public repository,
GitHub can disable scheduled workflows after 60 days without repository
activity; re-enable the workflow from the Actions page if that happens.

## What each run does

The workflow fetches the two branch tips it expects to replace and records
their exact object IDs. It then:

1. Rebases `feature/new-releases` onto upstream `main`.
2. Invokes Codex only if Git reports real file conflicts.
3. Rejects unfinished rebases, merge commits, conflict markers, modifications
   to the automation workflow, formatting differences, Clippy warnings, or
   failing tests.
4. Stores a Git bundle containing the old feature tip, the rebased feature
   tip, and upstream `main` for 14 days.
5. In a separate job that has no OpenAI credential, atomically pushes both
   branches with exact `--force-with-lease` expectations. If either branch
   changed during the run, neither branch is pushed.

A run with no upstream or feature-base change stops before the build jobs. Use
**Actions > Sync upstream and publish feature builds > Run workflow** and
enable **force_build** to rebuild the current feature tip anyway.

## Published files

Successful runs replace the assets on the rolling
`new-releases-latest` GitHub prerelease:

- Linux x86_64 portable archive
- Linux ARM64 portable archive
- Windows x86_64 portable archive and installer
- Windows ARM64 portable archive and installer (without MilkDrop, matching
  the upstream release matrix)
- universal Intel/Apple Silicon macOS app archive
- x86_64 Flatpak bundle, when the optional Flatpak job succeeds
- SHA-256 checksum manifest

The macOS bundle has the ad-hoc signature required to run on Apple Silicon but
is not Developer ID signed or notarized. Users may have to explicitly approve
it in macOS Privacy & Security. Add Apple credentials and the upstream native
packaging step if notarized fork builds are required.

These are feature prereleases, not official releases. Their tag does not begin
with `v`, and their checksums are not signed with Spotifast's update key, so
the in-app updater will not offer them. The rolling filenames remain stable so
the latest binaries are easy to link and download.

## Recovering a failed rebase

No branch is updated when conflict resolution or validation fails. Download
the `rebased-source` artifact from the workflow run to inspect a successful
rebase before or after its push. The bundle also retains
`bundle-old-feature`, which can be fetched into a local recovery branch:

```bash
git fetch feature-new-releases.bundle \
  refs/heads/bundle-old-feature:refs/heads/recovery/new-releases-before-sync
```

Review failed Codex resolutions rather than weakening the validation. Manual
conflict fixes can still be pushed to `feature/new-releases`; the next run will
retry against the current upstream tip.
