# Releasing

A JDBC release is triggered by pushing a version tag to GitHub. The [Release](.github/workflows/release.yml) workflow then publishes artifacts to Maven Central and creates a GitHub Release.

## Prerequisites

Repository secrets used by the release workflow:

| Secret | Purpose |
| --- | --- |
| `ENCRYPTION_KEY` / `ENCRYPTION_IV` | Decrypt the GPG signing key in `ci/secring.asc.enc` |
| `SONATYPE_USER` / `SONATYPE_PASSWORD` | Authenticate to Maven Central (Sonatype) |

## Using AI release skills

The `ai/skills/` folder has agent skills that automate the manual steps below. Ask an AI coding agent (for example Cursor) to follow the skill and pass the version as the argument:

| Skill | File | What it does |
| --- | --- | --- |
| Prepare release | [`ai/skills/prepare-release.md`](ai/skills/prepare-release.md) | Bumps version files, updates the changelog, opens a `release-<version>` PR |
| Publish release | [`ai/skills/publish-release.md`](ai/skills/publish-release.md) | After the PR is approved and green, merges it and pushes the `v<version>` tag |

Example prompts:

```text
Follow ai/skills/prepare-release.md for version 1.2.13
```

```text
Follow ai/skills/publish-release.md for version 1.2.13
```

Run prepare first, wait for the PR to be reviewed and CI to pass, then run publish. Pushing the tag is what triggers the Release workflow.

## 1. Prepare the release

Use the [prepare-release](ai/skills/prepare-release.md) skill or follow the steps below.

Create a `release-<version>` branch and bump the project version. For example, for `1.2.13`:

1. Update `pom.xml`:
   - Set `<version>` to the new version.
   - Set `<scm><tag>` to `singlestore-jdbc-client-<version>`.
2. Add a section to `CHANGELOG.md` for the new version, summarizing changes since the previous release tag.
3. Update the version in `.circleci/config.yml` `store_artifacts` paths for the JAR files.
4. Update the version in `README.md` (`## Version` and the Maven dependency example).
5. Open a PR, wait for CI and review, then merge.


## 2. Publish by pushing a tag

After the version bump is on `master`, use the [publish-release](ai/skills/publish-release.md) skill, which merges the release PR (when checks and approval are in place) and pushes the tag, or do this manually:

```bash
git checkout master
git pull
git tag -a v<version> -m "Release <version>"
git push origin v<version>
```

Tag format: `v` followed by the exact `pom.xml` version (for example `v1.2.13` or `v1.2.13-beta`).

Pushing the tag starts the Release workflow, which:

1. Builds and deploys signed artifacts to Maven Central.
2. Creates a GitHub Release named `SingleStore JDBC Driver <version>`.
3. Attaches:
   - `singlestore-jdbc-client-<version>.jar`
   - `singlestore-jdbc-client-<version>-browser-sso-uber.jar`

## 3. Verify

1. Confirm the [Release](https://github.com/memsql/S2-JDBC-Connector/actions/workflows/release.yml) workflow succeeded.
2. Confirm the [GitHub Release](https://github.com/memsql/S2-JDBC-Connector/releases) exists with the expected JARs.
3. Confirm the artifact appears on [Maven Central](https://central.sonatype.com/artifact/com.singlestore/singlestore-jdbc-client) (propagation can take some time).
