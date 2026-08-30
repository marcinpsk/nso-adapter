# CHANGELOG

<!-- version list -->

## v1.2.0 (2026-08-30)

### Bug Fixes

- Address adapter review findings
  ([`c3b579b`](https://github.com/marcinpsk/nso-adapter/commit/c3b579b7986e6bb3c08100a5d7e1a99506cbe0ca))

- Cover queue review findings
  ([`774a642`](https://github.com/marcinpsk/nso-adapter/commit/774a642f96d69ee2aefe50c1bd6fb8d4d0ddd330))

- **api**: Bound raw deployment-evidence id traversal
  ([`11fc7b8`](https://github.com/marcinpsk/nso-adapter/commit/11fc7b869921ff5f4b7867c7cef8ef628b7f9e00))

- **api**: Document both apply conflicts
  ([`4306040`](https://github.com/marcinpsk/nso-adapter/commit/4306040c0e1481b5d6a3e2ed5b8ca38427fdd13a))

- **api**: Split the deployment-evidence raw and distinct bounds
  ([`39be9a6`](https://github.com/marcinpsk/nso-adapter/commit/39be9a6448bfa1cec250eed00a40aa6a861232ed))

- **apply**: Refuse carriers without generations
  ([`2d259cf`](https://github.com/marcinpsk/nso-adapter/commit/2d259cf191f9fd2a368a98c9fed6110ce4f127de))

- **core**: Take the device row before the projection counter everywhere
  ([`65e9411`](https://github.com/marcinpsk/nso-adapter/commit/65e9411f2cbebf637ca193f50e578f119892dbd4))

- **evidence**: Deduplicate attempt IDs in linear time
  ([`7064555`](https://github.com/marcinpsk/nso-adapter/commit/7064555c7e6b6df1079df38590950f5d8dc74bfb))

- **evidence**: Resolve the PR #33 review wave
  ([`b3f52e1`](https://github.com/marcinpsk/nso-adapter/commit/b3f52e19d14f58df1d294613d957582681c017c6))

- **generation**: Isolate corrupt recovery heads
  ([`dbef048`](https://github.com/marcinpsk/nso-adapter/commit/dbef0489cdc975b9e8f2e47608c10e8a86578ca6))

- **generation**: Isolate recovery database failures
  ([`afee5e4`](https://github.com/marcinpsk/nso-adapter/commit/afee5e49e2797ed2631e59d048ce016e3bda2e39))

- **generation**: Isolate recovery-domain failures
  ([`e1c0372`](https://github.com/marcinpsk/nso-adapter/commit/e1c0372c38d564019557c33547b456d9ae7e4256))

- **generation**: Lock the carrier before trusting its status
  ([`b08cc30`](https://github.com/marcinpsk/nso-adapter/commit/b08cc30a4b98f16c7a77db8dab7d8ab095697b02))

- **generation**: Take the projection device lock as NO KEY UPDATE
  ([`221c2db`](https://github.com/marcinpsk/nso-adapter/commit/221c2dbae6c03f429747a5916df2caebe5f823e6))

- **generation**: Yield instead of blocking on a carrier a worker holds
  ([`92be73e`](https://github.com/marcinpsk/nso-adapter/commit/92be73e63b5cf4938ad07e0a885b7523404d114e))

- **onboarding**: Route offboard orphan terminalization through the claim module
  ([`7d1d294`](https://github.com/marcinpsk/nso-adapter/commit/7d1d294720c612f08ba18ebc0199e71516e1343a))

- **queue**: Reject invalid removal carriers
  ([`b2e0bbd`](https://github.com/marcinpsk/nso-adapter/commit/b2e0bbde3088c2cdce42743f2e577b9bc1cc6822))

- **review**: Resolve recovery review gate
  ([`1f97569`](https://github.com/marcinpsk/nso-adapter/commit/1f97569c602fe85c219a6f8765b92fe8038d87c4))

- **store**: The queue-class downgrade validates before recreating the old index
  ([`482c894`](https://github.com/marcinpsk/nso-adapter/commit/482c89441f9bc09599ac4e3e649ba1bf89381c9f))

- **test**: Handle schemas without job trigger
  ([`343b7fd`](https://github.com/marcinpsk/nso-adapter/commit/343b7fddcb71b60f64ea26c479dd1931fb79c9a2))

- **types**: Route the offboard bulk terminalize through execute_dml
  ([`978fc06`](https://github.com/marcinpsk/nso-adapter/commit/978fc06644efc831cda45f7a12bf7d79816bac01))

- **types**: Satisfy the mypy gate after rebasing onto develop
  ([`cc8314d`](https://github.com/marcinpsk/nso-adapter/commit/cc8314d7a772f0bca5613684583aef1ed129413e))

### Documentation

- **api**: State the heterogeneous admission rules the adapter really has
  ([`cf663df`](https://github.com/marcinpsk/nso-adapter/commit/cf663df4fe3fbcad7e32e13e8f308c7faa455afe))

- **api**: State which generations carry the apply attempt id
  ([`8f342f1`](https://github.com/marcinpsk/nso-adapter/commit/8f342f12630900d651bdfaaadc3dbd31f0ebd234))

- **store**: State the projection lock's real strength
  ([`12f1a40`](https://github.com/marcinpsk/nso-adapter/commit/12f1a40d43951140dd810564a8a45c096bf62ac9))

### Features

- **actions**: Barrier actions take the generation they act on
  ([`23cd830`](https://github.com/marcinpsk/nso-adapter/commit/23cd830266e925f5043866f2c153848447e2c440))

- **apply**: The Apply POST carries its durable attempt identity
  ([`bce5486`](https://github.com/marcinpsk/nso-adapter/commit/bce54865690562db3797dcd3fbded0b78896b6e9))

- **core**: Head-coverage advancement and truthful abandon identity
  ([`e3b55a6`](https://github.com/marcinpsk/nso-adapter/commit/e3b55a6587a9dc93cf65f3a167fb82f68f42e1b0))

- **core**: One job-construction seam and no recovery takeover
  ([`425df74`](https://github.com/marcinpsk/nso-adapter/commit/425df74307533a6f6d6257e1cfda3e0eb3093f46))

- **evidence**: Durable apply attempts and generation carrier snapshots
  ([`4e19909`](https://github.com/marcinpsk/nso-adapter/commit/4e1990958617e22a0a63ae36bb6997a765f104b3))

- **store**: Job queue classes - coalescible column, checks, index, trigger
  ([`87f1489`](https://github.com/marcinpsk/nso-adapter/commit/87f1489a445c0edea14fddf67e4f44015a3d0386))

### Performance Improvements

- **store**: Index deployment_apply_attempt.device_id
  ([`60a8dd7`](https://github.com/marcinpsk/nso-adapter/commit/60a8dd79fbbd28f34f5d2296c5d0a72113720815))

### Refactoring

- **sweep**: Attach reissue generations through the job choke point
  ([`efa10ac`](https://github.com/marcinpsk/nso-adapter/commit/efa10acce9c9703a7f34206e5fdb4afed1af9460))

### Testing

- Classify every Job fixture and reconcile shapes the schema now forbids
  ([`859767c`](https://github.com/marcinpsk/nso-adapter/commit/859767c1b9e3f63ea547672dbdf64f4d704383f1))

- Probe the projection writer's NO KEY UPDATE wait
  ([`d55470b`](https://github.com/marcinpsk/nso-adapter/commit/d55470baaca1975d74f361a73e7e24b5c858cde7))

- Tighten adapter review coverage
  ([`17bdb31`](https://github.com/marcinpsk/nso-adapter/commit/17bdb31c3fa28ca1e566ba1253099010d67092d4))

- **api**: Enforce canonical barrier responses
  ([`6b4d871`](https://github.com/marcinpsk/nso-adapter/commit/6b4d871bb1996895f8929e1dc5e7497f9c8e3fc1))

- **api**: Pin the json_invalid envelope for syntactically invalid bodies
  ([`03589c5`](https://github.com/marcinpsk/nso-adapter/commit/03589c563701c7103c10905dc68a58ce60eb5d9d))

- **apply**: Seed immutable worker generations
  ([`6e2e2b5`](https://github.com/marcinpsk/nso-adapter/commit/6e2e2b5de9f8e8a4ca54d1dc6acf694acef280da))

- **generation**: Cancel the starting task when the yield assertion fails
  ([`ee22866`](https://github.com/marcinpsk/nso-adapter/commit/ee22866894e380788e69b363c13dd8cf39012e0f))

- **generation**: Classify settlement carriers
  ([`5fd6f05`](https://github.com/marcinpsk/nso-adapter/commit/5fd6f05ce408d76308e622f06d0b8703fec033bb))

- **generation**: Clean up concurrent recovery tasks
  ([`0e991d7`](https://github.com/marcinpsk/nso-adapter/commit/0e991d788d28ff63b91fe79d64dc83204bd175de))

- **generation**: Pin the yield test's failure-path cleanup
  ([`5d01a8d`](https://github.com/marcinpsk/nso-adapter/commit/5d01a8d691b2ba0a09315d297042ea68bc372abc))

- **store**: Coalescible-immutability trigger suite
  ([`bac901e`](https://github.com/marcinpsk/nso-adapter/commit/bac901eff77094285cefa865e086625437c3d678))

- **store**: Finish the pg_provisioner fixture rename at this level
  ([`1c14613`](https://github.com/marcinpsk/nso-adapter/commit/1c146131c6a416c05ea4182bbe97cd60794d5868))

- **store**: Finish the pg_provisioner fixture rename at this level
  ([`0fa2500`](https://github.com/marcinpsk/nso-adapter/commit/0fa25003b050b8d12bcfdfe54ac51bfacdfdba92))


## v1.1.0 (2026-08-29)

### Bug Fixes

- **api**: Declare ActionApplyGenerationOut.job_id non-null
  ([`8a63c66`](https://github.com/marcinpsk/nso-adapter/commit/8a63c663dd02c1b38509f660e7c8dc600beb2134))

- **api**: Route promotion refusal through error factory
  ([`5acf1ee`](https://github.com/marcinpsk/nso-adapter/commit/5acf1ee8e76c8c696a3b56318515398a5a193144))

- **api**: Skipped_detail is required and serialized on the Apply response
  ([`8c857c3`](https://github.com/marcinpsk/nso-adapter/commit/8c857c39a18bfdc3563937fe3f8f1cf1697eb08c))

- **apply**: Order the backfill skip after sequence match
  ([`84c45de`](https://github.com/marcinpsk/nso-adapter/commit/84c45de672ab2b357776a5653b34749e7926c461))

- **apply**: Preserve error response contract
  ([`0883596`](https://github.com/marcinpsk/nso-adapter/commit/0883596cb282497ac0f51689c9f4abb5db85773b))

- **compose**: Mount postgres volumes at the 18+ parent directory
  ([`4fba8c8`](https://github.com/marcinpsk/nso-adapter/commit/4fba8c891f557a455c290f072d5c247467c041c4))

- **contract**: Include provenance refusal reason
  ([`e76c97d`](https://github.com/marcinpsk/nso-adapter/commit/e76c97dae091f241825175399782e667d4128f44))

- **generation**: Carry interface execution into successors
  ([`c0167cf`](https://github.com/marcinpsk/nso-adapter/commit/c0167cffd7ffdb0303c3f6d7942922e12ba3f50d))

- **generation**: Close document execution gaps
  ([`08ef84a`](https://github.com/marcinpsk/nso-adapter/commit/08ef84adf9c276745753dddadd58e58d0256a705))

- **generation**: Complete writer settlement admission
  ([`e85b5c6`](https://github.com/marcinpsk/nso-adapter/commit/e85b5c635261756ff84adf4a02671c2e5fde540d))

- **generation**: Drop the stale authorized_streams export
  ([`ecfdca3`](https://github.com/marcinpsk/nso-adapter/commit/ecfdca3979d279b517e35066c15249bb95ca7de6))

- **generation**: Harden document boundaries
  ([`7cfd932`](https://github.com/marcinpsk/nso-adapter/commit/7cfd93212823a104a3882f886d4c9b971ed5529c))

- **generation**: Preserve empty document sections
  ([`2bf61fb`](https://github.com/marcinpsk/nso-adapter/commit/2bf61fb05c66fa705ca5b6f378eb92ec8eed7f67))

- **generation**: Scope interface eligibility to its promotion
  ([`81bb121`](https://github.com/marcinpsk/nso-adapter/commit/81bb12191bac7b7b46ce73f1756efa55f018e32b))

- **l2**: Count removal generations without the retired metadata-clear signal
  ([`264dd9b`](https://github.com/marcinpsk/nso-adapter/commit/264dd9bd1c78250bb913a92c0d7627626f4bd9d0))

- **openapi**: Preserve push sequence bound
  ([`c99ec77`](https://github.com/marcinpsk/nso-adapter/commit/c99ec778c84b92ba1e3e2dd9487fc60f045aca84))

- **projection**: Keep correlation-only edits out of the apply delta
  ([`b6d9b67`](https://github.com/marcinpsk/nso-adapter/commit/b6d9b6704c1e8f9c70308c9789d384fa7b4aba68))

- **promotion**: Harden manual apply boundaries
  ([`b92e608`](https://github.com/marcinpsk/nso-adapter/commit/b92e608bbdb8e63cc29989225f651f3b64199c88))

- **removal**: Fail closed on a generationless static-route force job
  ([`ee758c6`](https://github.com/marcinpsk/nso-adapter/commit/ee758c606b08597e796adc8dc9a109a10347b348))

- **removal**: Refuse force promotes before any store work
  ([`dbef8f4`](https://github.com/marcinpsk/nso-adapter/commit/dbef8f43cb2204bdb90df44c2a51343d94545420))

- **removal**: Refuse generation-only arguments on a force removal
  ([`5c28853`](https://github.com/marcinpsk/nso-adapter/commit/5c28853aba58115b2bc1cfc23a256d89fcf63f9b))

- **removal**: Reject cohort on force reissue
  ([`b93810b`](https://github.com/marcinpsk/nso-adapter/commit/b93810bd7ca6b0eb81276d26fb15529560f1faa5))

- **removal**: Tighten promotion-context invariants and the lock probe
  ([`a86cd2b`](https://github.com/marcinpsk/nso-adapter/commit/a86cd2b96a59a7a7a7dc48cd38f0b0634f6f2bb4))

- **review**: Resolve branch 24 findings
  ([`9f01870`](https://github.com/marcinpsk/nso-adapter/commit/9f01870dd41ffc403a342febfc80b1473ce196e7))

- **review**: Resolve branch 26 findings
  ([`dbc6e15`](https://github.com/marcinpsk/nso-adapter/commit/dbc6e15af123b2f4e70274e8b9ec7b4c51734c94))

- **stack**: Preserve final helper seams
  ([`c64679d`](https://github.com/marcinpsk/nso-adapter/commit/c64679dfba3aae7a2224bb2c91d1880c3af97c5e))

- **static-route**: Normalize replay identity
  ([`e7225b8`](https://github.com/marcinpsk/nso-adapter/commit/e7225b827fe744adf26e8d709decdcc119c9ff4d))

- **static-route**: Refuse a generationless removal job on every scope
  ([`182a1cd`](https://github.com/marcinpsk/nso-adapter/commit/182a1cd0d83d6d7bbe838bca5d01c3f9805bdffd))

- **types**: Satisfy the mypy gate after rebasing onto develop
  ([`54e6071`](https://github.com/marcinpsk/nso-adapter/commit/54e6071dc85467790eca2a2121334be27f27939f))

- **types**: Satisfy the mypy gate after rebasing onto develop
  ([`0fde500`](https://github.com/marcinpsk/nso-adapter/commit/0fde500564ca0b7d73ccf3b3dfc06919f6922155))

### Code Style

- **api**: Format promotion error regression
  ([`e407c86`](https://github.com/marcinpsk/nso-adapter/commit/e407c865a9301761ade628bb63c2157eb8e06306))

### Documentation

- **api**: Clarify explicit apply boundary
  ([`1e243bd`](https://github.com/marcinpsk/nso-adapter/commit/1e243bda29990d6d3ed9b8eb4511df60bd88363e))

- **api**: Distinguish document-executed removals from live-store reissues; unambiguous
  skipped_detail presence
  ([`ecf7070`](https://github.com/marcinpsk/nso-adapter/commit/ecf7070a82dd166c6921e55a01fe09c368167610))

- **api**: Name the backfill removal exception
  ([`32bf196`](https://github.com/marcinpsk/nso-adapter/commit/32bf196237b070a46ad54fb44eb86fa23177bc48))

- **api**: Name the static-route store-only removal exception
  ([`753cd60`](https://github.com/marcinpsk/nso-adapter/commit/753cd60daefbbd77d1956409dca28b8708df1481))

- **api**: Update explicit apply sections
  ([`1338699`](https://github.com/marcinpsk/nso-adapter/commit/1338699a71be8e03c149a0067740b9eab682a6c1))

- **contract**: Document the skipped_detail response member
  ([`02819ef`](https://github.com/marcinpsk/nso-adapter/commit/02819efba93e23276606e663f5af6d8e92c2aeb1))

- **contract**: Name all three removal-execution outcomes
  ([`46bac11`](https://github.com/marcinpsk/nso-adapter/commit/46bac11599334a7cc237a2979f16576452309533))

- **contract**: Scope the empty-list detach rule to non-backfill pushes
  ([`cdc7f58`](https://github.com/marcinpsk/nso-adapter/commit/cdc7f5832c297cc5c517e73281ac2d45467df71c))

- **contract**: State the real action-apply chain order
  ([`18dba93`](https://github.com/marcinpsk/nso-adapter/commit/18dba93cf6d4b985e9c44c520a51a8b4aef5cfe8))

- **projection**: Correct manual apply set relation
  ([`0d4075f`](https://github.com/marcinpsk/nso-adapter/commit/0d4075f28d541f0d74a48186666e66c80a5b4e50))

### Features

- **1558**: Atomic action-apply promotion with generation chains (slice 2a)
  ([`47590bc`](https://github.com/marcinpsk/nso-adapter/commit/47590bc281a5fb1becd11cd999091c6b86f775cc))

- **1558**: Device generations listing for chain-aware consumers
  ([`73ee4e1`](https://github.com/marcinpsk/nso-adapter/commit/73ee4e1f1010e9911ff5bc0eeac8e45788d732ab))

- **1558**: Document execution for bgp
  ([`b279abc`](https://github.com/marcinpsk/nso-adapter/commit/b279abc86fffce3937c368ff6e487ad36699b9a8))

- **1558**: Document execution for eight sections + request-atomic cohorts
  ([`c63a45c`](https://github.com/marcinpsk/nso-adapter/commit/c63a45cc59cfc678112cbf4174547f89104a2684))

- **1558**: Document execution for interface_config and the ip lane
  ([`62d32f9`](https://github.com/marcinpsk/nso-adapter/commit/62d32f967734457357eee105a8e5af069cbf75ab))

- **1558**: Document execution for snmp and logging
  ([`cedb524`](https://github.com/marcinpsk/nso-adapter/commit/cedb5243be27b55e0284f3ca502acb93d18ae40e))

- **1558**: Document execution for static_route completes the partition
  ([`786f5d4`](https://github.com/marcinpsk/nso-adapter/commit/786f5d4fade8240bb965b9158e967b4571838dbb))

- **apply**: Report a backfill-only receipt with its own skip code
  ([`3c12001`](https://github.com/marcinpsk/nso-adapter/commit/3c120013aebfbc072cf32c2983d3b6cac8394835))

- **static-route**: Require deleted_routes on the intent PUT
  ([`dc3405c`](https://github.com/marcinpsk/nso-adapter/commit/dc3405c718b72fd1c994a18e7feeb342f50c6a24))

### Performance Improvements

- **receipt**: Index deletion-restore lookups once per table
  ([`63fc23a`](https://github.com/marcinpsk/nso-adapter/commit/63fc23ad46482a11e0f28845d097bc91c14aac8a))

### Refactoring

- **intent**: Export require_attach_to_job and narrow its pragma
  ([`b7e4fff`](https://github.com/marcinpsk/nso-adapter/commit/b7e4fffb3c9b67e9b86dadbc4b347c22e02389ce))

- **removal**: Collapse the live-store accepted reads
  ([`f19fed7`](https://github.com/marcinpsk/nso-adapter/commit/f19fed7b45c9fa818412b7d04307522faf59a01d))

- **removal**: Route _replacement_rows through _accepted_rows
  ([`26b6e70`](https://github.com/marcinpsk/nso-adapter/commit/26b6e70086aa1bd24a2a907a60f35b13d2ddbcdd))

- **static-route**: Share one clear-candidate rule
  ([`bad2b95`](https://github.com/marcinpsk/nso-adapter/commit/bad2b9579a33fd8e816a5f32d9694fdfd26bb557))

### Testing

- Drop the time import left unused by the lock-wait resolution
  ([`cdeffbd`](https://github.com/marcinpsk/nso-adapter/commit/cdeffbd4f7458370889cf23ace662ae5220485e0))

- Replace the remaining iteration-count waits with deadlines
  ([`cd8f969`](https://github.com/marcinpsk/nso-adapter/commit/cd8f9695f7e7f1e7c5ac29861e0082b95cc51844))

- **api**: Exercise promotion refusal dispatch
  ([`3f0192a`](https://github.com/marcinpsk/nso-adapter/commit/3f0192af3b2fd4a10e263444aa8f407bf0d11ea7))

- **apply**: Pin both interface apply_unexecutable reasons
  ([`75c276f`](https://github.com/marcinpsk/nso-adapter/commit/75c276f8f941c930dc30b4c48ef3ee3427838a17))

- **apply**: Wait on a deadline, not an iteration count
  ([`03c86bd`](https://github.com/marcinpsk/nso-adapter/commit/03c86bdee61b4605ccda8de27eb5d2cdf1405e00))

- **apply**: Widen the live-read gate at its source module
  ([`6060947`](https://github.com/marcinpsk/nso-adapter/commit/6060947954aad5ad9922f635168c2edbfc905098))

- **generation**: Drop superseded apply cases
  ([`0e866bd`](https://github.com/marcinpsk/nso-adapter/commit/0e866bd1adf44031a4c0a05614a089ae81c408eb))

- **generation**: Refresh lock probe snapshot
  ([`c59687f`](https://github.com/marcinpsk/nso-adapter/commit/c59687ff67fda1864ec3930bc690bfe4e7a7ea4e))

- **projection**: Refuse a well-formed keyless vault reference
  ([`eb1b61a`](https://github.com/marcinpsk/nso-adapter/commit/eb1b61a54a91d65eb330f2eb11188c4359097fc6))

- **protocol**: Stop shadowing the push_seq helper
  ([`b621af9`](https://github.com/marcinpsk/nso-adapter/commit/b621af98f94f7be5933fe696d0e91a9f06e28ae8))

- **receipts**: Assert the transmitted push sequence
  ([`97fa96a`](https://github.com/marcinpsk/nso-adapter/commit/97fa96abf69f24ff0c76c58a56fa8626b8f95d93))

- **removal**: Adapt predicate case to documents
  ([`788bc5f`](https://github.com/marcinpsk/nso-adapter/commit/788bc5f6084c124c09c8c341cbc80c312a40544c))

- **removal**: Pin generation ownership predicate
  ([`c1987c6`](https://github.com/marcinpsk/nso-adapter/commit/c1987c6369a86fbda5741311133dfe5113a0fa60))

- **removal**: State force promotion disposition
  ([`c981885`](https://github.com/marcinpsk/nso-adapter/commit/c981885564e4f9a775abab4676464ffa73286274))

- **removal**: State reissue promotion disposition
  ([`5b0b9bb`](https://github.com/marcinpsk/nso-adapter/commit/5b0b9bbfe60b80b5b83535e156ed0331241103a9))

- **review**: Separate the document slot from the stamp slot
  ([`3af124d`](https://github.com/marcinpsk/nso-adapter/commit/3af124dc18d24aecb6656ea59a58d5a0b4811b55))

- **static-route**: Cover null deleted routes
  ([`72ba063`](https://github.com/marcinpsk/nso-adapter/commit/72ba063cdee0d16bc75c2c044f657fdd11f9bdb6))

- **static-route**: Cover per-object deletion rollback
  ([`b8d10c1`](https://github.com/marcinpsk/nso-adapter/commit/b8d10c17e4b7f52ee77117e580c3ddf74f20455c))

- **static-route**: Record removal execution fixture
  ([`de633e9`](https://github.com/marcinpsk/nso-adapter/commit/de633e9a05f2370a3988d0bccb72ac1b8e6b90cb))

- **store**: Assert the cohort index returns on re-upgrade
  ([`368e261`](https://github.com/marcinpsk/nso-adapter/commit/368e2618ff705e4094c3f5e3ad8d5ed6b7beac41))

- **store**: Finish the pg_provisioner fixture rename at this level
  ([`65f20eb`](https://github.com/marcinpsk/nso-adapter/commit/65f20eb920a528e42bcb6cffff5d8aa872c8081e))


## v1.0.0 (2026-08-29)

### Bug Fixes

- Resolve develop review findings
  ([`4b04499`](https://github.com/marcinpsk/nso-adapter/commit/4b0449915168bb5387dd119ba4c1af2bfeab3977))

- **1503**: Bound a deleted_routes lineage at two triples
  ([`083a801`](https://github.com/marcinpsk/nso-adapter/commit/083a801fa3f5578829c1c5e1939634775d350209))

- **1503**: Cohort every promoted generation of one intent request
  ([`6bd24e8`](https://github.com/marcinpsk/nso-adapter/commit/6bd24e858109860b2cc3ad49e7902ccee6922df5))

- **1503**: Scope the settlement barrier to the marking-split cohort
  ([`b3111e6`](https://github.com/marcinpsk/nso-adapter/commit/b3111e6cba680e1199d523520bf66664ffc0ca76))

- **1503**: Stamp a stream revision only when every carrying generation settled
  ([`74647c4`](https://github.com/marcinpsk/nso-adapter/commit/74647c438fdb9fc2d83a9923a0f71c045857bd83))

- **api**: Answer 422 for undecodable intent bodies
  ([`7df09de`](https://github.com/marcinpsk/nso-adapter/commit/7df09deb5c7e748be51568d422092b3c3b9f7851))

- **api**: Redact malformed request modes
  ([`2956e9c`](https://github.com/marcinpsk/nso-adapter/commit/2956e9c57a093c2ea92880b4d8b576e0789db72f))

- **api**: Reject malformed request modes
  ([`8e1a254`](https://github.com/marcinpsk/nso-adapter/commit/8e1a254b0522a115643718f28d8e21eb438e8477))

- **apply**: Hydrate only carried sections
  ([`3b34ca0`](https://github.com/marcinpsk/nso-adapter/commit/3b34ca0848b5017cb2030e68012da25d74be4f0e))

- **backfill**: Reject residual uncorrelated routes
  ([`d2c2ed2`](https://github.com/marcinpsk/nso-adapter/commit/d2c2ed28c38838525266c0c7994fb835d84e8047))

- **db**: Close internal sessions before returning
  ([`77f9999`](https://github.com/marcinpsk/nso-adapter/commit/77f9999e082788bdba09a1e849b86f26957d535e))

- **db**: Preserve pending-clear revision during migration
  ([`1c61085`](https://github.com/marcinpsk/nso-adapter/commit/1c61085846f561eb8c4cc7331565fc8bc09d82cc))

- **db**: Refuse the irreversible pending-clear uniqueness downgrade
  ([`146d5d4`](https://github.com/marcinpsk/nso-adapter/commit/146d5d4a4a367e5af9003dbe5d601392e2a64a13))

- **docs**: Validate the receipt example
  ([`8e23c33`](https://github.com/marcinpsk/nso-adapter/commit/8e23c331666ab431ece9eaa462839850a3c3a5c0))

- **failover**: Never start a flip whose way back is unknown
  ([#1630](https://github.com/marcinpsk/nso-adapter/pull/1630),
  [`b3aed29`](https://github.com/marcinpsk/nso-adapter/commit/b3aed29fcd3ca4d5f77e9e0cf2ed3f6b7b4e70e1))

- **failover**: Stale stuck-markers clear on recovery evidence
  ([`057ac90`](https://github.com/marcinpsk/nso-adapter/commit/057ac9006c2dc1752792dcd1baea792d0245fd2b))

- **generation**: Settle abandoned cohorts
  ([`01aa742`](https://github.com/marcinpsk/nso-adapter/commit/01aa7427b843081a2a90d698544e58d0aee2604a))

- **intent**: Preserve sequence through admission seam
  ([`9049570`](https://github.com/marcinpsk/nso-adapter/commit/90495701130e1b70be02ec32e3c50fbe8685976b))

- **migration**: Chain the settlement-cohort revision off the failback column
  ([`48fe068`](https://github.com/marcinpsk/nso-adapter/commit/48fe068acfa854c5e3417b9c346454ccd82e38d3))

- **migration**: Chain the settlement-cohort revision off the pending-clear table
  ([`bfe437d`](https://github.com/marcinpsk/nso-adapter/commit/bfe437d431757b237ddeaf147d4dd1b4b461b6ca))

- **migration**: Chain the settlement-cohort revision off the pending-clear uniqueness constraint
  ([`4e1e434`](https://github.com/marcinpsk/nso-adapter/commit/4e1e43478a2bd064e448a3deb0a2fd9475e8b50d))

- **migrations**: Freeze the settlement-cohort trigger DDL in its revision
  ([`92142b9`](https://github.com/marcinpsk/nso-adapter/commit/92142b98e3b8a5ae91ddb3bcf145f61994035a3e))

- **removal**: Reject unmarked deleting removals
  ([`3803e33`](https://github.com/marcinpsk/nso-adapter/commit/3803e33d735bea3d4234b39004d858f34b5c544f))

- **removal**: Resolve the single pending-clear row and refuse deferred delete-origin retracts
  ([`0c46c9b`](https://github.com/marcinpsk/nso-adapter/commit/0c46c9be4c0dbf787b2bcec1716e759ea3bbe0ba))

- **review**: Resolve branch 22 findings
  ([`3d327da`](https://github.com/marcinpsk/nso-adapter/commit/3d327da0b22c90c86c1af105876e093bfcf59ce1))

- **static-route**: Acknowledge only identity-carrying backfill rows
  ([`f150483`](https://github.com/marcinpsk/nso-adapter/commit/f1504837b504673df6db251bcff2972546609d7a))

- **static-route**: One acknowledgement renderer and a documented backfill refusal
  ([`f7ba056`](https://github.com/marcinpsk/nso-adapter/commit/f7ba056ac1bf3aff0b39c730dc2de8101d4efbff))

- **store**: Enforce one pending clear per stream
  ([`ddee6b6`](https://github.com/marcinpsk/nso-adapter/commit/ddee6b6647d0b31146b2b08057e67ee1226bb1ef))

- **types**: Pin the settlement-target tuple set for the mypy gate
  ([`2352b2a`](https://github.com/marcinpsk/nso-adapter/commit/2352b2a355bc553c9c7fd18134d8578e99a4d606))

- **types**: Satisfy the mypy gate after rebasing onto develop
  ([`b4a4cb0`](https://github.com/marcinpsk/nso-adapter/commit/b4a4cb0808f1cec00c7a13b9c5fd385b9de322eb))

### Code Style

- Restore the blank-line separation lost in a restack resolution
  ([`6fe6f35`](https://github.com/marcinpsk/nso-adapter/commit/6fe6f351fc13521a8d5c7a047082ab89441e9d61))

### Documentation

- **api**: Correct the receipt route example
  ([`308b0b0`](https://github.com/marcinpsk/nso-adapter/commit/308b0b05f3e7b7346ddeae660b5687c5f9fbcbd7))

- **api**: Define nullable deletion authority
  ([`df0526f`](https://github.com/marcinpsk/nso-adapter/commit/df0526f1642fd259f2318823523ff21e73e27dd2))

- **api**: Exempt store_only pushes from the removal-job rule
  ([`18675b1`](https://github.com/marcinpsk/nso-adapter/commit/18675b140c6002ba8d847b9274597be814cc7095))

- **api**: Name the barrier-action nullable job_id as the common-rule exception
  ([`0873b5c`](https://github.com/marcinpsk/nso-adapter/commit/0873b5cb99808df13ac617885ee5b2d2a2a03f43))

- **api**: State delete_origin in the four-field rule and the order-sensitive receipt identity
  ([`8995e5e`](https://github.com/marcinpsk/nso-adapter/commit/8995e5e27e7013c212bc76194e331ba3bec0fb76))

- **api): pending_clear map is always present; test(generation**: Discriminating auto-apply restore
  ([`cc87500`](https://github.com/marcinpsk/nso-adapter/commit/cc87500b241a092a9ddad1dc2fb9c9d85f4cb27f))

- **contract**: State strict mode parsing and rehome intent-receipts
  ([`3c5ae1f`](https://github.com/marcinpsk/nso-adapter/commit/3c5ae1fde82406676c726eb65898a64c10e92e30))

- **db**: Record teardown leak diagnosis
  ([`4a7f272`](https://github.com/marcinpsk/nso-adapter/commit/4a7f272c2a6fc5f7b064b6d9285fecd0145a8adb))

- **deleted-routes**: Receipts are order-exact, responses order-free
  ([`f3a41e2`](https://github.com/marcinpsk/nso-adapter/commit/f3a41e262c9f87d5c2a8ab511a21b6a538801b4b))

- **static-route**: Separate the recovery GET from a pass receipt
  ([`fbef479`](https://github.com/marcinpsk/nso-adapter/commit/fbef47985ca5367129e067f39e3d5a9d1cbb63df))

- **store**: Correct the settlement_cohort membership comment
  ([`b08f876`](https://github.com/marcinpsk/nso-adapter/commit/b08f87696472d9df057f47b2cf937ecf77af577d))

### Features

- **1503**: GET /api/v1/intent-receipts, the restore path's read surface
  ([`0d8f6e6`](https://github.com/marcinpsk/nso-adapter/commit/0d8f6e6e5141fac1d2b6696e6322c9ed666ef729))

- **1503**: Marking-homogeneous removal jobs (Appendix O chunk O2)
  ([`b653ffd`](https://github.com/marcinpsk/nso-adapter/commit/b653ffdad1cd5b7baed86e4947cfe52fba6cbf37))

- **1503**: Per-object deletion authority and the backfill-only fence pass
  ([`4ffd7de`](https://github.com/marcinpsk/nso-adapter/commit/4ffd7de0180e650d2cfa71e6bf34f57fe8ba4024))

- **1503**: Require X-Push-Seq on every in-protocol intent PUT
  ([`66cf57d`](https://github.com/marcinpsk/nso-adapter/commit/66cf57dcd2fd6f2f14489d3a29c43837690d7d33))

- **removal**: Record deferred pending clears durably per stream
  ([`2113d63`](https://github.com/marcinpsk/nso-adapter/commit/2113d63137e44f3d4378e44a31f2fb144d20d75f))

### Refactoring

- **removal**: Extract the unmarked-deletion refusal helper
  ([`39d9788`](https://github.com/marcinpsk/nso-adapter/commit/39d9788fbe1fd5fd3885cecef333cab20c86ba0a))

- **store**: Align pending-clear column widths
  ([`fa71aef`](https://github.com/marcinpsk/nso-adapter/commit/fa71aefd50cbbf5c5b0bd7ac46a86aa5bf632c4b))

- **tests**: Finish the provisioner rename in test helpers
  ([`4f09783`](https://github.com/marcinpsk/nso-adapter/commit/4f09783db4af2671c58af5781fda9de8ace141af))

### Testing

- Use the shared auth and sequence helpers in the backfill test
  ([`f603fe1`](https://github.com/marcinpsk/nso-adapter/commit/f603fe17e06fb548a35df90efd8455f8c1166c17))

- **api**: Pin authentication precedence over a malformed X-Push-Seq
  ([`c426909`](https://github.com/marcinpsk/nso-adapter/commit/c42690901900b66db1a9d2d0600c17c246436e9c))

- **backfill**: Assert the rolled-back push answers a 500
  ([`ff7578f`](https://github.com/marcinpsk/nso-adapter/commit/ff7578f0fb5184d090a4cb6f3605cc8008beeddc))

- **db**: Identify connections that survive teardown
  ([`2df1176`](https://github.com/marcinpsk/nso-adapter/commit/2df1176a0dd228977f171594e9f61f55ac51b5f3))

- **db**: Scope the timestamp downgrade test below the irreversible migration
  ([`1622565`](https://github.com/marcinpsk/nso-adapter/commit/1622565c7834ca6cdc47112f36a260cf865424a1))

- **fold**: Correct the cohorts-by-job key annotation
  ([`5be7dd5`](https://github.com/marcinpsk/nso-adapter/commit/5be7dd52ce80b03b4429a80259a50f1f44713b18))

- **generation**: Expect no released successor job
  ([`9be7d40`](https://github.com/marcinpsk/nso-adapter/commit/9be7d40e5052d0aff0876b063ff313f65626b686))

- **guard**: Pin the fail-closed flag on a mapping passed to any call
  ([`30ca934`](https://github.com/marcinpsk/nso-adapter/commit/30ca934291be92737e664907283511f39e48d02c))

- **intent**: Key inherited protocol writes
  ([`5934969`](https://github.com/marcinpsk/nso-adapter/commit/593496984c81e5531fd3b32ecca04229ee38ac35))

- **intent**: Pin the always-present pending_clear field in the summary golden
  ([`7f69b76`](https://github.com/marcinpsk/nso-adapter/commit/7f69b76c7fbe9b65c7ad1d12b0e94968b43b95da))

- **receipts**: Pin the stamped generation_id on the wire
  ([`eb2d8ee`](https://github.com/marcinpsk/nso-adapter/commit/eb2d8ee77742a02bde2b315374bd9db9e3dc2d2c))

- **removal**: Pin unmatched carrier rejection
  ([`de72509`](https://github.com/marcinpsk/nso-adapter/commit/de725099d7a33d7173aaa485797b2b3c397ebde7))

- **removal**: Track reissued tombstone authority
  ([`e91e1f2`](https://github.com/marcinpsk/nso-adapter/commit/e91e1f20f411e7b0f60d6f04e300148ff77cec0c))

- **removal**: Use the shared marking constant
  ([`3f65114`](https://github.com/marcinpsk/nso-adapter/commit/3f651142de9cef7be19c31c8609e645cf03e2a38))

- **store**: Assert the backfill column returns on the re-upgrade
  ([`17ec778`](https://github.com/marcinpsk/nso-adapter/commit/17ec778edf034b2f1353d7b2bba634f2820a1209))

- **store**: Assert the cohort column and its allocator return on the re-upgrade
  ([`2f7ed3e`](https://github.com/marcinpsk/nso-adapter/commit/2f7ed3e7e47d1addfa53cf523a0a48efee2298d4))

- **store**: Finish the pg_provisioner fixture rename at this level
  ([`846a43b`](https://github.com/marcinpsk/nso-adapter/commit/846a43b8556e053c2ae93ec8313ac01847f311cc))

- **store**: Prove the sitecustomize injection reached the alembic subprocess
  ([`7b336d1`](https://github.com/marcinpsk/nso-adapter/commit/7b336d1e9ede34647f02d144910e8f3c6b5bd433))


## v0.4.1 (2026-08-28)

### Bug Fixes

- Handle nullable refresh timestamps
  ([`c3e5feb`](https://github.com/marcinpsk/nso-adapter/commit/c3e5feb5eb42348bed86805dd09db9b3f10a87f0))

- Isolate scheduled intent reconciliation errors
  ([`9456b3b`](https://github.com/marcinpsk/nso-adapter/commit/9456b3b486d4024a7e2368e47bbd19355a71f197))

- Resolve mypy-branch review findings
  ([`473c9dd`](https://github.com/marcinpsk/nso-adapter/commit/473c9dd12bc9c70bd96c3d87f0838bde4f542221))

- **failover**: Keep active-OOB liveness running without a primary address
  ([`c391e35`](https://github.com/marcinpsk/nso-adapter/commit/c391e3520ff2a02b39febea6aab5f2db3e199e75))

- **failover**: Read the pre-flip address and refuse an unrevertable failback flip
  ([`8390d99`](https://github.com/marcinpsk/nso-adapter/commit/8390d99f9db6b29d8e32f737cdefd823ff29c8a3))

### Chores

- Add mypy quality gate
  ([`028f7a6`](https://github.com/marcinpsk/nso-adapter/commit/028f7a60ec612a0081f188474db06f547a2ad030))

- Resolve mypy findings
  ([`d6ecfc6`](https://github.com/marcinpsk/nso-adapter/commit/d6ecfc6f98cac29847d95858d70ac7d33fced4ea))

- Widen mypy gate to alembic
  ([`3056217`](https://github.com/marcinpsk/nso-adapter/commit/3056217275326ce36be734726a3503735b4086a7))

- **hooks**: Refuse a stale uv.lock in the pre-push mypy gate
  ([`3ad3fd1`](https://github.com/marcinpsk/nso-adapter/commit/3ad3fd1dbadba3d79341039ff325e554bf62ce9b))

### Refactoring

- Simplify mypy type boundaries
  ([`9097111`](https://github.com/marcinpsk/nso-adapter/commit/90971114a7f8c0680e05a2acaa29d0b8a4cac01d))

### Testing

- Strengthen mypy adoption regressions
  ([`b43afee`](https://github.com/marcinpsk/nso-adapter/commit/b43afee1613e84a131d5dad2d90452968d46787f))


## v0.4.0 (2026-08-25)

### Bug Fixes

- **api**: Answer an unexpected exception with the error envelope
  ([`60b2c8a`](https://github.com/marcinpsk/nso-adapter/commit/60b2c8a9900bb61c0c78f34355d5ee7ff7c64c05))

- **api**: Close validation and logging review gaps
  ([`ecb64fd`](https://github.com/marcinpsk/nso-adapter/commit/ecb64fdcc6c9c1f892467ec07c347685e07fbfed))

- **api**: Correct the static-route replay annotation
  ([`eb5a0df`](https://github.com/marcinpsk/nso-adapter/commit/eb5a0dfca76effbca588d786b08f6fd0466c2f69))

- **api**: Derive runtime error codes from schema
  ([`4f2e98f`](https://github.com/marcinpsk/nso-adapter/commit/4f2e98fd4923bb39691a173fe7d60c7799001270))

- **api**: Keep sequence exceptions out of responses
  ([`b21f51f`](https://github.com/marcinpsk/nso-adapter/commit/b21f51fe666adf22961249e0fe34f1c6446191a4))

- **api**: Log only safe metadata for an unhandled exception
  ([`88edb8f`](https://github.com/marcinpsk/nso-adapter/commit/88edb8f99c12dc9f1c1ca340d316d869df3c3f52))

- **api**: Normalize framework HTTP errors
  ([`e124ee8`](https://github.com/marcinpsk/nso-adapter/commit/e124ee84fb71a998753be98943dbb68bdae9a093))

- **api**: Point error additions to schema
  ([`f25e19c`](https://github.com/marcinpsk/nso-adapter/commit/f25e19ced4f44a80a6a1cc1850ca88604f9a0188))

- **api**: Redact validation inputs
  ([`b3e0966`](https://github.com/marcinpsk/nso-adapter/commit/b3e096601b0bf92091af1323d690548d443d164b))

- **api**: Return only the admitted barrier job
  ([`aa52008`](https://github.com/marcinpsk/nso-adapter/commit/aa5200866ddd49bcf34720dba468a1090b81a62b))

- **api**: Return the released successor job
  ([`a1f4b94`](https://github.com/marcinpsk/nso-adapter/commit/a1f4b944e91f649472297efd9a87cb8089d859ae))

- **api**: Seal the unhandled-exception traceback at the outermost middleware
  ([`7e03945`](https://github.com/marcinpsk/nso-adapter/commit/7e039455979cb01da2b126db07c5ad7eef97c2a6))

- **apply**: Count document send outcomes
  ([`4f87006`](https://github.com/marcinpsk/nso-adapter/commit/4f87006eb8dd43cfb5becd51794236bcb268db0c))

- **apply**: Hydrate only carried document sections
  ([`edaa0da`](https://github.com/marcinpsk/nso-adapter/commit/edaa0da55a5471782e7722d6ac962e61393a2178))

- **bgp**: Enqueue the retracting removal when redistribution fields clear
  ([`4bfca20`](https://github.com/marcinpsk/nso-adapter/commit/4bfca2092a945ff7418250bd315e48db83b72a44))

- **generation**: Clear a terminal job binding before attaching a fresh job
  ([`5fa8d6c`](https://github.com/marcinpsk/nso-adapter/commit/5fa8d6cc20a2b800a0b138e440ca21b85101c5de))

- **generation**: Close admission and outcome gaps
  ([`1f636a4`](https://github.com/marcinpsk/nso-adapter/commit/1f636a4076286ebbf4b49d94d208d59cebb0b836))

- **generation**: Close review admission gaps
  ([`2dd69c7`](https://github.com/marcinpsk/nso-adapter/commit/2dd69c7c6cd703a60ce9f6d4c9570f351d455833))

- **generation**: Harden advancement and immutable DDL
  ([`43b089a`](https://github.com/marcinpsk/nso-adapter/commit/43b089ac51617c54eb0ea84bf575afa1b2a96d73))

- **generation**: Keep queued conflicts truthful
  ([`d93201f`](https://github.com/marcinpsk/nso-adapter/commit/d93201f76f7798cf478c0ad180577d6cdeee591a))

- **generation**: Keep takeover off generation-less queued removals
  ([`fd3f6bc`](https://github.com/marcinpsk/nso-adapter/commit/fd3f6bc35d105c6a7a0d167eb14c7a52b1d377d0))

- **generation**: Reject non-head operator exits
  ([`320b42f`](https://github.com/marcinpsk/nso-adapter/commit/320b42f7cfe72dbd76a142c1cdfe970c86ceb5a0))

- **guard**: Track values mapping provenance
  ([`e713828`](https://github.com/marcinpsk/nso-adapter/commit/e713828c5c0bacc2b3e1003306da3a923df32219))

- **intent**: Centralize delivery admission and job ordering
  ([`a7b4e68`](https://github.com/marcinpsk/nso-adapter/commit/a7b4e685fbfd6438eeb9f05e01859d2e556e4604))

- **migration**: Serialize the removal-quiescence gate with jobs writers
  ([`82d21cd`](https://github.com/marcinpsk/nso-adapter/commit/82d21cd4cee16602926ec061688e61d20c7960df))

- **migrations**: Freeze the deployment-generation trigger DDL in its revision
  ([`fd8ddde`](https://github.com/marcinpsk/nso-adapter/commit/fd8ddde3053947ec05d41a63dc06a92ccd3ac741))

- **projection**: Give section_models the siblings' error contract
  ([`0cbdd72`](https://github.com/marcinpsk/nso-adapter/commit/0cbdd724d3c3cf88f7038c35cd6aade62b9ec68b))

- **projection**: Refuse a document row without its primary key
  ([`0d60bce`](https://github.com/marcinpsk/nso-adapter/commit/0d60bce48174b4e33d851104ccb7b35aa9f2f51a))

- **removal**: Preserve tombstone authority across retries
  ([`4e1c99e`](https://github.com/marcinpsk/nso-adapter/commit/4e1c99eaba93760d6bb29b724ec40460f6b8169a))

- **review**: Harden generation boundaries
  ([`6c25129`](https://github.com/marcinpsk/nso-adapter/commit/6c25129df06c01fd11f1fada32800eee73e15e3c))

- **review**: Pin the injected rejection and scope the action contract
  ([`fe88dcd`](https://github.com/marcinpsk/nso-adapter/commit/fe88dcd1b59e0067d4676c9a8fd33855e2c66e9c))

- **review**: Truthful trace-context doc and deterministic test fixtures
  ([`342f03d`](https://github.com/marcinpsk/nso-adapter/commit/342f03d36dcbfe75e0a88553f37fdbc6cfa44c52))

- **route-policy**: Enqueue auto-apply generation
  ([`d9ca81b`](https://github.com/marcinpsk/nso-adapter/commit/d9ca81bffb1d8cd861894124914205cc114af15a))

- **tests**: Omit SQL from teardown diagnostics
  ([`c2c5926`](https://github.com/marcinpsk/nso-adapter/commit/c2c5926d9e24413bfa53f857452afc5826c24618))

- **tombstone**: Include a divergent deployed_key in the sweep fallback removal set
  ([`5a64663`](https://github.com/marcinpsk/nso-adapter/commit/5a646634f1d7bc74ce7b45e92887cfda16f1de8b))

- **worker**: Log the exception type, not its text
  ([`a2aa856`](https://github.com/marcinpsk/nso-adapter/commit/a2aa8562e34d3bd8ece4fdbe6729b5a5180129ff))

### Chores

- **ci**: Bump astral-sh/setup-uv in the actions group
  ([`3a4a62a`](https://github.com/marcinpsk/nso-adapter/commit/3a4a62af3f86fec4d3d44d23cbf9b76d5163ab08))

- **ci**: Bump the actions group with 2 updates
  ([`1891c52`](https://github.com/marcinpsk/nso-adapter/commit/1891c52232e535732eab1c4a0d981c1a201618e7))

- **deps**: Bump the python-minor-patch group with 2 updates
  ([`a607e93`](https://github.com/marcinpsk/nso-adapter/commit/a607e93b5f0976712c41c5d2ccf5d50d65d83431))

- **deps**: Bump the python-minor-patch group with 4 updates
  ([`fc5fec0`](https://github.com/marcinpsk/nso-adapter/commit/fc5fec07730c9873a59724ec2135c7eadb70405f))

- **test**: Cap auto-detected xdist workers at 8
  ([`659ab0e`](https://github.com/marcinpsk/nso-adapter/commit/659ab0e2dfdd09eebf8e0efbe9c3c6e0f1eef38c))

- **test**: Run the suite on xdist workers by default
  ([`1da2ad6`](https://github.com/marcinpsk/nso-adapter/commit/1da2ad6176e175199ff590c63cd31d8e8123a5ef))

### Continuous Integration

- Audit GitHub Actions with zizmor in pre-commit and CI
  ([`b2146c8`](https://github.com/marcinpsk/nso-adapter/commit/b2146c87369aa7d0d7f98d734204ad947b7d9853))

- Fail on lockfile drift instead of silently rewriting it
  ([`c40592b`](https://github.com/marcinpsk/nso-adapter/commit/c40592b7fdc9d6967c0390ccf5e9cc5010ef3223))

### Features

- **1558**: Deployment-generation and promotion foundation
  ([`50bb078`](https://github.com/marcinpsk/nso-adapter/commit/50bb078d435a51dfa7529ce4740c52288f01ba41))

- **receipts**: Stamp the generation each intent push authorized
  ([`2e67874`](https://github.com/marcinpsk/nso-adapter/commit/2e6787425bf6af9b2e66b1ae0d09e1dded616a2e))

### Refactoring

- **generation**: Export the blocked-status set
  ([`2a81379`](https://github.com/marcinpsk/nso-adapter/commit/2a8137994e8e405410e15d23c2579097f826c74b))

- **isis**: Reuse the auto-apply helper in the flex-algo route
  ([`f977459`](https://github.com/marcinpsk/nso-adapter/commit/f9774598b75fb888c9f45bad103d9897f32d5aaf))

### Testing

- Drop the dead _vlan name and clear every projection cache
  ([`3b4c198`](https://github.com/marcinpsk/nso-adapter/commit/3b4c19886a0fbc1d564775ceb799f20f4a8729c9))

- Enforce the zizmor parity claim and three review-driven guards
  ([`cf6204c`](https://github.com/marcinpsk/nso-adapter/commit/cf6204cbe903ae3ec1881c29d5ace05dba951616))

- Resolve PR #18 review findings
  ([`fffae05`](https://github.com/marcinpsk/nso-adapter/commit/fffae05c2a5e50ebb8e81d570a76d6d32c28dcd2))

- **api**: Pin generation action responses
  ([`b3464e9`](https://github.com/marcinpsk/nso-adapter/commit/b3464e9d13c4ad00a477d1b75a388c364ea3f0d1))

- **db**: Let the teardown guard wait out disposal latency
  ([`eefef15`](https://github.com/marcinpsk/nso-adapter/commit/eefef1502bc23d55e4a12ae5ac815992a185d90f))

- **flake**: Remove the two timing flakes that CPU contention decides
  ([`8d3c678`](https://github.com/marcinpsk/nso-adapter/commit/8d3c678133d57070d59e89399e3facec4cb21c83))

- **generation**: Bound race below lock timeout
  ([`4ef7403`](https://github.com/marcinpsk/nso-adapter/commit/4ef740394daefa55aef330d99ffcbe32796d2cc6))

- **generation**: Make the lock-overlap windows deterministic
  ([`90d7c4f`](https://github.com/marcinpsk/nso-adapter/commit/90d7c4fc98a7deda01720d6a4db58a1454ad5d64))

- **generation**: Prove the contention window without a timing sleep
  ([`413e77b`](https://github.com/marcinpsk/nso-adapter/commit/413e77bda3cdeb09118ecbbad72f229d842cab24))

- **generation**: Strengthen protocol review coverage
  ([`2fe8af5`](https://github.com/marcinpsk/nso-adapter/commit/2fe8af5d1da6e9223d76365d5e0cb30ee21d649a))

- **guards**: Detect named-expression mappings in terminal-write scan
  ([`48dabf5`](https://github.com/marcinpsk/nso-adapter/commit/48dabf53d740301c08717a1b426bf706de4ea2a4))

- **guards**: Flag a walrus mapping bound outside the values call
  ([`77ed342`](https://github.com/marcinpsk/nso-adapter/commit/77ed342a7b22b10a705e2ee0d1180a46ac10e34a))

- **interface-ip**: Assert every seeding PUT lands
  ([`f386395`](https://github.com/marcinpsk/nso-adapter/commit/f386395fcc9fb9af3cca44c99be0a36fde1c99ad))

- **receipts**: Cover unpacked terminal writes
  ([`2d1d991`](https://github.com/marcinpsk/nso-adapter/commit/2d1d991415d67b16ed1f1cc188286d6154f6aea1))

- **release**: Parse the workflow instead of substring-matching build: true
  ([`bc36808`](https://github.com/marcinpsk/nso-adapter/commit/bc368087df0c418c8667a0aae371b0212d0c4149))

- **removal**: Isolate the vault fixture from collection order
  ([`abaeaac`](https://github.com/marcinpsk/nso-adapter/commit/abaeaac1d1c973dc1b4de17a69770689c4d1bf3b))

- **removal**: Seed force jobs through the reissue generation
  ([`6532bac`](https://github.com/marcinpsk/nso-adapter/commit/6532bace8ecd570ec7edd2f41fc0f36733c9572f))

- **review**: Check the outer static-route handler for note_write too
  ([`605961f`](https://github.com/marcinpsk/nso-adapter/commit/605961f1b6666de67fd6d2c07877e1a2e446be06))

- **settle-token**: Exercise the write barrier with a device-writing job
  ([`f5590e8`](https://github.com/marcinpsk/nso-adapter/commit/f5590e86d8f7b06e94b34507eb8c169855cca1f6))

- **static-route**: Prove the claim guard precedes the body
  ([`1dcc28c`](https://github.com/marcinpsk/nso-adapter/commit/1dcc28cf523f043404237532095c572d37d27f52))

- **store**: Assert every dropped object returns on the re-upgrade
  ([`f307014`](https://github.com/marcinpsk/nso-adapter/commit/f30701498484ad8e80db5e7339fa00144db65188))

- **store**: Drop the dead sitecustomize from the historical DDL-freeze test
  ([`c510b62`](https://github.com/marcinpsk/nso-adapter/commit/c510b62417ee83d9974e0d199a288c34c80ddae9))

- **store**: Make the historical DDL-freeze test discriminating at this level
  ([`c12d27c`](https://github.com/marcinpsk/nso-adapter/commit/c12d27c8fcf0767ec9009b9ff8b01f61b36a65d5))

- **store**: State both freeze discriminators in the docstring
  ([`db6fac3`](https://github.com/marcinpsk/nso-adapter/commit/db6fac34c785fa20a561f79e3aefa57bfd314142))


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
