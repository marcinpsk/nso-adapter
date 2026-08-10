# CHANGELOG

<!-- version list -->

## v0.3.0 (2026-08-10)

### Bug Fixes

- **api**: Bind the settlement cursor and the status filter to their page shapes
  ([`c4a1a64`](https://github.com/marcinpsk/nso-adapter/commit/c4a1a64c4e90f454b2d6241ea1dc6ec09c23bff9))

- **apply**: Keep exception text out of the persisted failure items
  ([`8d76852`](https://github.com/marcinpsk/nso-adapter/commit/8d76852f91c9cc51c1fb3d80f99c2b4b781ee526))

- **core**: Persist client-safe job errors, never raw exception text
  ([`3170d80`](https://github.com/marcinpsk/nso-adapter/commit/3170d80e7132bacc7bcee47e6ef0e89ada12304a))

- **jobs**: Branch on the terminal CAS in the sync and connect success paths
  ([`7c43b82`](https://github.com/marcinpsk/nso-adapter/commit/7c43b82296b657cd623a59525f7ee3abf4c507a4))

- **jobs**: Discard the transaction when the terminal CAS is refused
  ([`fbecb7f`](https://github.com/marcinpsk/nso-adapter/commit/fbecb7f0ff32edf2df51591f7de237f38890e936))

- **jobs**: Name the execution on provision's device-busy failure
  ([`ab05ec4`](https://github.com/marcinpsk/nso-adapter/commit/ab05ec43049ada6fb17339ec10db917b7c16e8ed))

- **jobs**: Route a failed settlement allocation through the no-second-write path
  ([`2dd1629`](https://github.com/marcinpsk/nso-adapter/commit/2dd16293ddf6b051dba6c3017efa677c6c9b302d))

- **scripts**: Keep secret-adjacent text out of validation output
  ([`3bf8083`](https://github.com/marcinpsk/nso-adapter/commit/3bf8083e99abaae2edf693d68bf35a8d98420ecc))

### Chores

- **deps**: Bump the python-minor-patch group across 1 directory with 7 updates
  ([`29adc17`](https://github.com/marcinpsk/nso-adapter/commit/29adc17bf6cf52269ddcf793d0f7607bce19bb5c))

- **deps**: Update setuptools requirement from >=74 to >=83.0.0
  ([`754f3cf`](https://github.com/marcinpsk/nso-adapter/commit/754f3cf12c9f27ef0360f66c0ee7d3978fc1b35c))

- **deps**: Update sqlalchemy[asyncio] requirement
  ([`647e2f4`](https://github.com/marcinpsk/nso-adapter/commit/647e2f47840c02bf67d4eb71236f01a14edb2b52))

### Continuous Integration

- **release**: Bound the release uv install to the 0.12 series
  ([`f06fb8b`](https://github.com/marcinpsk/nso-adapter/commit/f06fb8b44df4658ef3eed78ae1f09c0eb388ba55))

- **release**: Publish sdist and wheel to PyPI via trusted publishing
  ([`e3736b2`](https://github.com/marcinpsk/nso-adapter/commit/e3736b292778e18a87397d8e384212a6caa5e95a))

- **release**: Sync uv.lock's own version into the release commit
  ([`c622c53`](https://github.com/marcinpsk/nso-adapter/commit/c622c53b353b23bd5e150365c37be06009e67b92))

### Documentation

- **api-contract**: Document the ordered settlement feed and its cursor rules (Appendix S, S7)
  ([`f5538ca`](https://github.com/marcinpsk/nso-adapter/commit/f5538cab3ed434f09c11c93a5ab5c2ee3404eb7e))

- **guidelines**: State the notification-lane exception and the error-surface layering
  ([`2760e20`](https://github.com/marcinpsk/nso-adapter/commit/2760e20ed639d80e18a7208b7d61d19c995948e4))

- **guidelines**: The notification payload is one documented identifier
  ([`59f2ffa`](https://github.com/marcinpsk/nso-adapter/commit/59f2ffa65a94d98162ddf1939b03a86054fef579))

### Features

- **jobs**: Add the run-attempt token and one terminal writer (Appendix S, S1)
  ([`4ed0f9b`](https://github.com/marcinpsk/nso-adapter/commit/4ed0f9bcc60fcb4f7f9fdc02329b8710b7659e35))

- **jobs**: Allocate a per-device settlement sequence on every terminal write (Appendix S, S2)
  ([`77cdb65`](https://github.com/marcinpsk/nso-adapter/commit/77cdb65e0393d7ad0b1ec4dace7f0efd73f428e5))

- **jobs**: Serve the ordered settlement feed and validate it (Appendix S, S3)
  ([`ea77c7e`](https://github.com/marcinpsk/nso-adapter/commit/ea77c7e11cb963f68d80a7d0d3f666d1ed5ab633))

### Refactoring

- **worker**: Route the crash envelope through error_envelope
  ([`9e305db`](https://github.com/marcinpsk/nso-adapter/commit/9e305db501c27ee5eb9f78dbcd78296f927b7ca2))

### Testing

- **api**: Walk nested included routers when asserting get_read_db injection
  ([`11b1f2c`](https://github.com/marcinpsk/nso-adapter/commit/11b1f2c35831cce33272c639b881cd595d1d7253))

- **apply**: Pin the sanitized crash envelope on last_apply_error
  ([`d73d836`](https://github.com/marcinpsk/nso-adapter/commit/d73d836e72cc05e4b1da8731136f843e8bcbed33))

- **guards**: Close str.format repr and values-mapping analyzer gaps
  ([`6dfb2c9`](https://github.com/marcinpsk/nso-adapter/commit/6dfb2c960c11aec89da16ed559ea3c962a309679))

- **guards**: Close the mapping, f-string repr, and locale-encoding gaps
  ([`70d1c95`](https://github.com/marcinpsk/nso-adapter/commit/70d1c95a610182c61a231bde61ddf55c5674e191))

- **settlement**: Tighten predicate fidelity, bound the alembic subprocess, fix a dead assertion
  message
  ([`5bbff85`](https://github.com/marcinpsk/nso-adapter/commit/5bbff85285a3b4289e58d4683505b01c7ae000a0))


## v0.2.1 (2026-08-04)

### Bug Fixes

- **tests**: Mask the release version in the OpenAPI snapshot gate
  ([`a2543fb`](https://github.com/marcinpsk/nso-adapter/commit/a2543fb338f474a224e179bfd83ea721c7ed4daa))

### Chores

- **deps**: Bump postgres from 16-alpine to 18-alpine
  ([`4c9abde`](https://github.com/marcinpsk/nso-adapter/commit/4c9abdeba66b26b77c6a84e70a7df0f4b71b4635))

- **deps**: Bump python from 3.12-slim to 3.14-slim
  ([`c0808d5`](https://github.com/marcinpsk/nso-adapter/commit/c0808d5d43a9867c7769c7dea0ebd90f8b5c0436))

- **deps**: Bump structlog from 25.5.0 to 26.1.0
  ([`067bb9b`](https://github.com/marcinpsk/nso-adapter/commit/067bb9b3775526e8ab295526f35ac86a79410ddc))

### Continuous Integration

- Pin all actions to latest release SHAs with accurate version comments
  ([`e39c913`](https://github.com/marcinpsk/nso-adapter/commit/e39c9130c5d4d6c1a922cd0bf29950a439162ff8))


## v0.2.0 (2026-08-04)

### Bug Fixes

- **ci**: Harden workflows and validate image runtime
  ([`b682926`](https://github.com/marcinpsk/nso-adapter/commit/b682926f616356dfe7316c595d63fdc995540e1c))

- **core**: Close the codex findings on the static-route PUT
  ([`635253c`](https://github.com/marcinpsk/nso-adapter/commit/635253c11a179a4500c8cc0edcd37ef9e085a11d))

- **core**: Close the ownership gaps codex found in the provision guard
  ([`df35c2a`](https://github.com/marcinpsk/nso-adapter/commit/df35c2ab38bc192b640e4a0b0c1d6df5b24bf9a9))

- **core**: Coalesce a requeue with its queued successor; grant shutdown grace
  ([`8470d28`](https://github.com/marcinpsk/nso-adapter/commit/8470d28c83df624a336a690aa44dfca872797867))

- **core**: Drop the authorityless removal PUT and the racing reclaim kick
  ([`364822b`](https://github.com/marcinpsk/nso-adapter/commit/364822bae262bcf3a0d0df9013e585f2ea5a59a5))

- **core**: Keep the excluded static-route scope's capability bookkeeping
  ([`78ade3c`](https://github.com/marcinpsk/nso-adapter/commit/78ade3c97f3f2204798c1751f1198dc33cf3414b))

- **core**: Keep the terminal apply transaction whole and its lock order right
  ([`99fee50`](https://github.com/marcinpsk/nso-adapter/commit/99fee50cd7ee510da379f9dc5b5d0392fea8aa6b))

- **core**: Make the claim cutoff strictly clear the lifecycle, and sharpen three proofs
  ([`725148e`](https://github.com/marcinpsk/nso-adapter/commit/725148ef4f98be4694c1254c1b053463c1dbd74d))

- **core**: Re-raise ClaimLostError through every broad catch on a claimed path
  ([`1385c30`](https://github.com/marcinpsk/nso-adapter/commit/1385c30fe843ca8ec88565e7231d4e991b415901))

- **core**: Resolve a cancellation delivered at the acquisition COMMIT
  ([`bc1262c`](https://github.com/marcinpsk/nso-adapter/commit/bc1262cd1bc9e4701a67475978afd0b7ecc3ace6))

- **core**: Roll back the guard before releasing; widen the failover bound; tolerate a vanished
  device
  ([`1310586`](https://github.com/marcinpsk/nso-adapter/commit/1310586469311fe3e9d05b27bb20261ce4b9d812))

- **db**: Validate the database URL before any migration work
  ([`856a223`](https://github.com/marcinpsk/nso-adapter/commit/856a223256d11ae831feb9acc4571570f966850b))

- **dev**: Restart adapter database with Docker
  ([`19d2725`](https://github.com/marcinpsk/nso-adapter/commit/19d2725b8030c74124f122cbb42de2ba60879af1))

- **devices**: Enforce device identity in the database and resolve onboard races
  ([`f0b0f87`](https://github.com/marcinpsk/nso-adapter/commit/f0b0f8720863a76734582a6c850ae40addd42be3))

- **static-route**: Scope the per-route error to the verdict this apply produced
  ([`c10d77e`](https://github.com/marcinpsk/nso-adapter/commit/c10d77eeb85276e47a66fe619e1419619a26a253))

- **store**: Bind aware-UTC instants everywhere and run tests on per-test PostgreSQL clones
  ([`62630e7`](https://github.com/marcinpsk/nso-adapter/commit/62630e78b6652a9adf744756d0b1198ed1b6766f))

- **store**: Normalize timestamps to timestamptz and one wire serializer
  ([`4aefeb4`](https://github.com/marcinpsk/nso-adapter/commit/4aefeb4de4dbcef88a63825e4244283be9dba278))

### Chores

- **ci**: Add dependabot for uv, actions, docker and compose
  ([`4916881`](https://github.com/marcinpsk/nso-adapter/commit/491688175b3e21bdfe0974f496076625e3fbd3b1))

- **db**: PostgreSQL is the only engine — reject other URLs at startup
  ([`a39a4b0`](https://github.com/marcinpsk/nso-adapter/commit/a39a4b0904caf49c7c6107c893de2c65b9a95700))

### Continuous Integration

- Enable the PostgreSQL-only test job
  ([`6d88f40`](https://github.com/marcinpsk/nso-adapter/commit/6d88f40939c209da1858a7125448a8497a6fd4e8))

- Probe the TCP listener, not the init-time socket server
  ([`746125b`](https://github.com/marcinpsk/nso-adapter/commit/746125b7fd6d29a1e354c2cdd3ea5114cd11dccb))

- **release**: Publish from conventional commits
  ([`9fd5a98`](https://github.com/marcinpsk/nso-adapter/commit/9fd5a982400cc13033a18395977f86628f80c627))

- **release**: Release-please to GHCR with semver tags on merge to main
  ([`2118393`](https://github.com/marcinpsk/nso-adapter/commit/21183934b7ae4c6192f9b5af0d68367f5c5f3b92))

### Documentation

- **api**: Describe what the static-route replacement path actually ships
  ([`3c7cf6a`](https://github.com/marcinpsk/nso-adapter/commit/3c7cf6a7ed8e66825b294b33096f1497ca98bbde))

- **api-contract**: Document the static-route settlement carriage
  ([`470faa4`](https://github.com/marcinpsk/nso-adapter/commit/470faa49da1809c2d32100ea46dbae71ee9bdf0a))

- **config**: Document the intent claim wait knob
  ([`98aea34`](https://github.com/marcinpsk/nso-adapter/commit/98aea34dad65acf023f2e1a38e4076bd6073c2d4))

### Features

- **api**: Claim the device before reading the plan it mutates
  ([`f59fa25`](https://github.com/marcinpsk/nso-adapter/commit/f59fa254b26fba3542fcc5170178f314195ccf0d))

- **api**: Fold the static-route removal job into the endpoint transaction
  ([`9ffa908`](https://github.com/marcinpsk/nso-adapter/commit/9ffa908fc5571cd312803209fc29916add2487d5))

- **api**: Route_id-first static-route matching, payload refusals, rollout fence
  ([`1fcaedd`](https://github.com/marcinpsk/nso-adapter/commit/1fcaedd87a3a5a3098ba22602b317b15ee0e273b))

- **core**: Acquire the device claim for the failover tick
  ([`a03daf1`](https://github.com/marcinpsk/nso-adapter/commit/a03daf145f06c0bffb462d9ef434c3ba8e5db21f))

- **core**: Add the static-route replacement classifier and clear carrier
  ([`dc3c88b`](https://github.com/marcinpsk/nso-adapter/commit/dc3c88bf239a2f05b40d247e7d2723748352ad29))

- **core**: Admit one provision per node at the database
  ([`f49f277`](https://github.com/marcinpsk/nso-adapter/commit/f49f27779108e5191d225275a696211ca43d3261))

- **core**: Atomic, handoff-safe job admission
  ([`9130e6b`](https://github.com/marcinpsk/nso-adapter/commit/9130e6b24030bdf19f04d7bac121ba76b1f904c7))

- **core**: Claim guard, three-state commit outcome, staleness reaper
  ([`80bc463`](https://github.com/marcinpsk/nso-adapter/commit/80bc4630807d3cc88d83e57e0aa2130528f00ce1))

- **core**: Claim-first worker — acquire the device, then its exact queued head
  ([`d3c06a1`](https://github.com/marcinpsk/nso-adapter/commit/d3c06a1a5306e9f454a5bdd45d7153f685213e7b))

- **core**: Deliver a static-route replacement with a guarded PUT
  ([`82db7e9`](https://github.com/marcinpsk/nso-adapter/commit/82db7e96f10f44e2147f237dd24e62cd60b07ec3))

- **core**: Deliver the static-route replacement an atomic apply cannot stage
  ([`f8b41e3`](https://github.com/marcinpsk/nso-adapter/commit/f8b41e389e9a161d5272f1bf9362743cbf19ebee))

- **core**: Hold the device claim across provision's post-map phase
  ([`f935e76`](https://github.com/marcinpsk/nso-adapter/commit/f935e76e4bc9a96d38252608cbae0a26d9392d0d))

- **core**: Prove a static-route apply before recording what it deployed
  ([`f4eefe3`](https://github.com/marcinpsk/nso-adapter/commit/f4eefe3144181bd043963bebc54e7193e126c275))

- **core**: Retract static routes against the live service and prove it
  ([`67fbbc2`](https://github.com/marcinpsk/nso-adapter/commit/67fbbc249107bd5fcf3749cb2d8a1ba5a28b18ba))

- **core**: Single recovery clock — retire the legacy job-status reaper
  ([`aba5a77`](https://github.com/marcinpsk/nso-adapter/commit/aba5a77ae5729f20e5ea3f7edcfd3c3d20b3be61))

- **core**: Split the active-job lookup three ways and narrow dedupe to queued same-type
  ([`9ec5367`](https://github.com/marcinpsk/nso-adapter/commit/9ec5367ee98063a409046605b2c38a02a2d62afb))

- **core**: Sweep uncarried tombstones into removal jobs
  ([`6de8370`](https://github.com/marcinpsk/nso-adapter/commit/6de8370afb33c90dd7d47a6b87983c17967241f8))

- **core**: Tear a device down under its claim, intent roots before jobs
  ([`4535e4b`](https://github.com/marcinpsk/nso-adapter/commit/4535e4b36ae2274fbb0c5534e77c0024716273fc))

- **core**: Two-phase worker cancellation, bounded drain, fail-stop
  ([`e33e9c4`](https://github.com/marcinpsk/nso-adapter/commit/e33e9c4994be3ba6dd96b2220ef2b1fe37f687bf))

- **static-route**: Carry the intent generation, echo the settlement triple, report per-route errors
  ([`10137be`](https://github.com/marcinpsk/nso-adapter/commit/10137be398b70d3bde03e0bd0caa6af302463379))

- **store**: Static-route route_id, deployed_key and deletion tombstones
  ([`1e245aa`](https://github.com/marcinpsk/nso-adapter/commit/1e245aae562db088b51db1ac64b8d68bd01d0f35))

- **store**: The exclusive per-device execution claim table
  ([`1ad71ec`](https://github.com/marcinpsk/nso-adapter/commit/1ad71ec52adb9d2b1dac736333522aa9d69ebdb5))

### Performance Improvements

- **test**: Parallelize local pytest runs
  ([`611a3cd`](https://github.com/marcinpsk/nso-adapter/commit/611a3cd642f8453ea61c3980543ee352274dfccf))

### Testing

- **core**: Pin the lock order, the endpoint/worker handoff and Path A
  ([`3ef76de`](https://github.com/marcinpsk/nso-adapter/commit/3ef76de628a2d6ffb83a3cc73a4d687c197fab93))

- **db**: Add PG clone fixtures and a deterministic session helper
  ([`d19ac81`](https://github.com/marcinpsk/nso-adapter/commit/d19ac8125ecb6a6006f64867d27651edcc0da691))

- **db**: Declare the throwaway PostgreSQL test server
  ([`69a6e02`](https://github.com/marcinpsk/nso-adapter/commit/69a6e027dbb1ccdb3a884bce9a66de76c550deb9))

- **db**: Make the read-snapshot unconditional and retire the sqlite read lane
  ([`fcd5e14`](https://github.com/marcinpsk/nso-adapter/commit/fcd5e143b4f9f2edcbbffda298833838515bc1d1))

- **db**: Retire the write-path sqlite branches and pin the caller-session contract
  ([`a739f39`](https://github.com/marcinpsk/nso-adapter/commit/a739f39e9b4d5ebf98e582c82882c449434bda78))

- **store**: Pin migration revisions and strengthen R1a assertions
  ([`49013ac`](https://github.com/marcinpsk/nso-adapter/commit/49013acfe77234cfa1bf3a92a48e4d6d4f5022d0))


## v0.1.0 (2026-07-27)

- Initial Release
