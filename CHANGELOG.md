# Changelog

## [1.3.5](https://github.com/thesentinella/hub-kubernetes-agent/compare/v1.3.4...v1.3.5) (2026-07-10)


### Bug Fixes

* harden drain_node controls ([#172](https://github.com/thesentinella/hub-kubernetes-agent/issues/172)) ([7be2b4f](https://github.com/thesentinella/hub-kubernetes-agent/commit/7be2b4f98761a8383a38dc40e1fe98aff6aa6f38))

## [1.3.4](https://github.com/thesentinella/hub-kubernetes-agent/compare/v1.3.3...v1.3.4) (2026-07-09)


### Bug Fixes

* gate drain_node by action policy ([#170](https://github.com/thesentinella/hub-kubernetes-agent/issues/170)) ([24efcc6](https://github.com/thesentinella/hub-kubernetes-agent/commit/24efcc6442c3e2ccc815767ff2a390d1393e16e7))

## [1.3.3](https://github.com/thesentinella/hub-kubernetes-agent/compare/v1.3.2...v1.3.3) (2026-07-09)


### Bug Fixes

* add scale subresource rbac ([#168](https://github.com/thesentinella/hub-kubernetes-agent/issues/168)) ([d207d2c](https://github.com/thesentinella/hub-kubernetes-agent/commit/d207d2c1a967fbace93b3fde4625018eb4f219f9))

## [1.3.2](https://github.com/thesentinella/hub-kubernetes-agent/compare/v1.3.1...v1.3.2) (2026-07-09)


### Bug Fixes

* improve action compatibility and preflight RBAC ([#166](https://github.com/thesentinella/hub-kubernetes-agent/issues/166)) ([bb8bb10](https://github.com/thesentinella/hub-kubernetes-agent/commit/bb8bb1058ef6a12ab7fe9e6eb7b5313f31f6756a))

## [1.3.1](https://github.com/thesentinella/hub-kubernetes-agent/compare/v1.3.0...v1.3.1) (2026-07-08)


### Bug Fixes

* allow update_agent for sentinella namespace ([#164](https://github.com/thesentinella/hub-kubernetes-agent/issues/164)) ([f7a2f64](https://github.com/thesentinella/hub-kubernetes-agent/commit/f7a2f6408e3c215c2897d0604e34e891913ecbcb))

## [1.3.0](https://github.com/thesentinella/hub-kubernetes-agent/compare/v1.2.0...v1.3.0) (2026-07-08)


### Features

* add operator exclusions and command contracts ([#161](https://github.com/thesentinella/hub-kubernetes-agent/issues/161)) ([ecd6f7d](https://github.com/thesentinella/hub-kubernetes-agent/commit/ecd6f7dc7505f8c17ab93e776ee661036d2ed49a))

## [1.2.0](https://github.com/thesentinella/hub-kubernetes-agent/compare/v1.1.0...v1.2.0) (2026-07-08)


### Features

* sen 361 resource yaml ([#159](https://github.com/thesentinella/hub-kubernetes-agent/issues/159)) ([c233fea](https://github.com/thesentinella/hub-kubernetes-agent/commit/c233fea6e32d5aba88b1b0a93a8f2941a85647e7))

## [1.1.0](https://github.com/thesentinella/hub-kubernetes-agent/compare/v1.0.3...v1.1.0) (2026-07-08)


### Features

* add resource yaml fetch command ([#157](https://github.com/thesentinella/hub-kubernetes-agent/issues/157)) ([ca5572b](https://github.com/thesentinella/hub-kubernetes-agent/commit/ca5572b077ce6f2944253014eadc0c5cf7e8b894))

## [1.0.3](https://github.com/thesentinella/hub-kubernetes-agent/compare/v1.0.2...v1.0.3) (2026-07-08)


### Bug Fixes

* finish action policy operator support ([#155](https://github.com/thesentinella/hub-kubernetes-agent/issues/155)) ([9c2a9ad](https://github.com/thesentinella/hub-kubernetes-agent/commit/9c2a9ad6eb32a3fcfc7a58c6cc0088fb7382a996))

## [1.0.2](https://github.com/thesentinella/hub-kubernetes-agent/compare/v1.0.1...v1.0.2) (2026-07-08)


### Bug Fixes

* action policy readiness and remote actions ([#153](https://github.com/thesentinella/hub-kubernetes-agent/issues/153)) ([1659f6c](https://github.com/thesentinella/hub-kubernetes-agent/commit/1659f6c71b8e92407cee6c9a47b4953d5f7158a6))

## [1.0.1](https://github.com/thesentinella/hub-kubernetes-agent/compare/v1.0.0...v1.0.1) (2026-07-08)


### Bug Fixes

* finish action operator verification ([#151](https://github.com/thesentinella/hub-kubernetes-agent/issues/151)) ([d35e3d4](https://github.com/thesentinella/hub-kubernetes-agent/commit/d35e3d4fb53962ccbe8ca289ed5a50b2b99d96fc))

## [1.0.0](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.37.0...v1.0.0) (2026-07-08)


### chore

* release 1.0.0 ([eb226ed](https://github.com/thesentinella/hub-kubernetes-agent/commit/eb226ed924c868b5ea005f4ac529e0302fe59348))

## [0.37.0](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.36.3...v0.37.0) (2026-06-30)


### Features

* add workload app metrics scraping ([#145](https://github.com/thesentinella/hub-kubernetes-agent/issues/145)) ([09d3b11](https://github.com/thesentinella/hub-kubernetes-agent/commit/09d3b11036006172f2cdcc0ce70b261dd7ee00c6))

## [0.36.3](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.36.2...v0.36.3) (2026-06-30)


### Bug Fixes

* harden agent health probes ([#143](https://github.com/thesentinella/hub-kubernetes-agent/issues/143)) ([b5900a9](https://github.com/thesentinella/hub-kubernetes-agent/commit/b5900a9b53d92c3f6277cc499a58ab7d11e60a2d))

## [0.36.2](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.36.1...v0.36.2) (2026-06-30)


### Bug Fixes

* pod label tech detection ([#141](https://github.com/thesentinella/hub-kubernetes-agent/issues/141)) ([f8277ef](https://github.com/thesentinella/hub-kubernetes-agent/commit/f8277efef4272bf0f2699cddc3a08041f9c15531))

## [0.36.1](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.36.0...v0.36.1) (2026-06-29)


### Bug Fixes

* detect workload tech from pod labels ([#139](https://github.com/thesentinella/hub-kubernetes-agent/issues/139)) ([951b3f4](https://github.com/thesentinella/hub-kubernetes-agent/commit/951b3f45a93a64a5b8e7ac720b508ffbdcd04a77))

## [0.36.0](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.35.0...v0.36.0) (2026-06-27)


### Features

* add read-only postgres diagnostics ([#137](https://github.com/thesentinella/hub-kubernetes-agent/issues/137)) ([60f5662](https://github.com/thesentinella/hub-kubernetes-agent/commit/60f56624a727ab38cb7cb7d23c2c7d4b4a26b953))

## [0.35.0](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.34.0...v0.35.0) (2026-06-26)


### Features

* add secret-backed postgres probe ([#135](https://github.com/thesentinella/hub-kubernetes-agent/issues/135)) ([bca9864](https://github.com/thesentinella/hub-kubernetes-agent/commit/bca98646f0fecaa870325f90995c9002ad655702))

## [0.34.0](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.33.0...v0.34.0) (2026-06-26)


### Features

* refine postgres discovery heuristics ([#133](https://github.com/thesentinella/hub-kubernetes-agent/issues/133)) ([4fec2cf](https://github.com/thesentinella/hub-kubernetes-agent/commit/4fec2cf5bd7bdbad089e6ca8829ed01894c7aadd))

## [0.33.0](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.32.0...v0.33.0) (2026-06-26)


### Features

* add live postgres probe ([#131](https://github.com/thesentinella/hub-kubernetes-agent/issues/131)) ([1586587](https://github.com/thesentinella/hub-kubernetes-agent/commit/1586587c17a221f8f0b18da5b34b064a3aee6a80))

## [0.32.0](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.31.0...v0.32.0) (2026-06-26)


### Features

* add postgresql monitoring plugin ([#129](https://github.com/thesentinella/hub-kubernetes-agent/issues/129)) ([e1761c9](https://github.com/thesentinella/hub-kubernetes-agent/commit/e1761c9077bfa58f3a09615fdb978ee9ce4cc997))

## [0.31.0](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.30.0...v0.31.0) (2026-06-25)


### Features

* add openshift version payload ([#127](https://github.com/thesentinella/hub-kubernetes-agent/issues/127)) ([f08c227](https://github.com/thesentinella/hub-kubernetes-agent/commit/f08c227fdf8ea8a7d8e206717b55b699899af68d))

## [0.30.0](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.29.3...v0.30.0) (2026-06-24)


### Features

* add workload monitoring log tails ([#125](https://github.com/thesentinella/hub-kubernetes-agent/issues/125)) ([adb18f6](https://github.com/thesentinella/hub-kubernetes-agent/commit/adb18f64683e6d9082d27a7bb08d8272fcbe1be4))

## [0.29.3](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.29.2...v0.29.3) (2026-06-24)


### Bug Fixes

* add postgres tech subtype ([#123](https://github.com/thesentinella/hub-kubernetes-agent/issues/123)) ([0aca926](https://github.com/thesentinella/hub-kubernetes-agent/commit/0aca9269ff76385e5fcf702bead2a6fee704a3ba))

## [0.29.2](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.29.1...v0.29.2) (2026-06-23)


### Bug Fixes

* document release-please title casing ([#120](https://github.com/thesentinella/hub-kubernetes-agent/issues/120)) ([77c5e1e](https://github.com/thesentinella/hub-kubernetes-agent/commit/77c5e1e8ce2836149600b33f6daea6a55946c14b))

## [0.29.1](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.29.0...v0.29.1) (2026-06-22)


### Bug Fixes

* add tetragon readiness flag ([#117](https://github.com/thesentinella/hub-kubernetes-agent/issues/117)) ([1f3b5ed](https://github.com/thesentinella/hub-kubernetes-agent/commit/1f3b5ed1619aaa00709d6704cf9cbd3588d47f20))

## [0.29.0](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.28.2...v0.29.0) (2026-06-22)


### Features

* sen-331 aarch64 support ([#115](https://github.com/thesentinella/hub-kubernetes-agent/issues/115)) ([83b4e09](https://github.com/thesentinella/hub-kubernetes-agent/commit/83b4e090a94cbc76223bf9bcab8d69ebc3fe30ad))

## [0.28.2](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.28.1...v0.28.2) (2026-06-22)


### Bug Fixes

* report CSI snapshot API availability state in snapshot ([#112](https://github.com/thesentinella/hub-kubernetes-agent/issues/112)) ([2c2f81e](https://github.com/thesentinella/hub-kubernetes-agent/commit/2c2f81e9e338eb197f3488b3b5c6ef58e5dcc640))

## [0.28.1](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.28.0...v0.28.1) (2026-06-15)


### Bug Fixes

* report pod-metrics availability state in snapshot ([#109](https://github.com/thesentinella/hub-kubernetes-agent/issues/109)) ([fa9a574](https://github.com/thesentinella/hub-kubernetes-agent/commit/fa9a5748bba878f8657cc22a184f93fbf4d6a4ac))

## [0.28.0](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.27.0...v0.28.0) (2026-06-11)


### ⚠ BREAKING CHANGES

* add process-level technology detection ([#106](https://github.com/thesentinella/hub-kubernetes-agent/issues/106))

### Features

* add process-level technology detection ([#106](https://github.com/thesentinella/hub-kubernetes-agent/issues/106)) ([953bb3d](https://github.com/thesentinella/hub-kubernetes-agent/commit/953bb3d31dc79234fd8949cf9b488d2bd6df0272))

## [0.27.0](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.26.0...v0.27.0) (2026-06-11)


### Features

* add operational maturity inventory ([#104](https://github.com/thesentinella/hub-kubernetes-agent/issues/104)) ([cdeae72](https://github.com/thesentinella/hub-kubernetes-agent/commit/cdeae7249cfa7ead43e9c462021375043e0f0e4f))

## [0.26.0](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.25.0...v0.26.0) (2026-06-11)


### Features

* add security inventory ([#102](https://github.com/thesentinella/hub-kubernetes-agent/issues/102)) ([020b80f](https://github.com/thesentinella/hub-kubernetes-agent/commit/020b80f5f8245165caafa2141dd5c2fe3a99518e))

## [0.25.0](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.24.0...v0.25.0) (2026-06-11)


### Features

* add pod usage metrics ([#100](https://github.com/thesentinella/hub-kubernetes-agent/issues/100)) ([531bab6](https://github.com/thesentinella/hub-kubernetes-agent/commit/531bab6ca6e543ce822c4460c6de03b5698f5797))

## [0.24.0](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.23.0...v0.24.0) (2026-06-11)


### Features

* expose agent config drift env ([#98](https://github.com/thesentinella/hub-kubernetes-agent/issues/98)) ([5c581a4](https://github.com/thesentinella/hub-kubernetes-agent/commit/5c581a494efbfc9ceb08455feab2da0b46773313))

## [0.23.0](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.22.0...v0.23.0) (2026-06-11)


### Features

* telemetry metrics ([#96](https://github.com/thesentinella/hub-kubernetes-agent/issues/96)) ([d8a3c0a](https://github.com/thesentinella/hub-kubernetes-agent/commit/d8a3c0a95a5716698db59038a1813c6b44d2661d))

## [0.22.0](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.21.0...v0.22.0) (2026-06-11)


### Features

* tetragon grpc clean ([#93](https://github.com/thesentinella/hub-kubernetes-agent/issues/93)) ([02a3d24](https://github.com/thesentinella/hub-kubernetes-agent/commit/02a3d24d670b0f94e7b3ad40faaaa5627bc44fd8))


### Bug Fixes

* remove stale sidecar remnants ([#95](https://github.com/thesentinella/hub-kubernetes-agent/issues/95)) ([d1ff2c1](https://github.com/thesentinella/hub-kubernetes-agent/commit/d1ff2c13a4ccd499ad08922a7542fb3fc7e1e6bb))

## [0.21.0](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.20.0...v0.21.0) (2026-06-11)


### Features

* implement direct tetragon grpc ingestion ([#91](https://github.com/thesentinella/hub-kubernetes-agent/issues/91)) ([811b76d](https://github.com/thesentinella/hub-kubernetes-agent/commit/811b76d15aef9d60cd603ebeaabff94309a7d841))

## [0.20.0](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.19.8...v0.20.0) (2026-06-10)


### Features

* add Tetragon gRPC sidecar ingestion ([#89](https://github.com/thesentinella/hub-kubernetes-agent/issues/89)) ([072c4a0](https://github.com/thesentinella/hub-kubernetes-agent/commit/072c4a05f3395239444ebbc0179ced6b6ca274d0))

## [0.19.8](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.19.7...v0.19.8) (2026-06-09)


### Bug Fixes

* tetragon TCP tracing policy ([#87](https://github.com/thesentinella/hub-kubernetes-agent/issues/87)) ([4df31fc](https://github.com/thesentinella/hub-kubernetes-agent/commit/4df31fcb5c1e2a7f122732d2520ab9545e28fbb2))

## [0.19.7](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.19.6...v0.19.7) (2026-06-09)


### Bug Fixes

* install openshift integrity ([#85](https://github.com/thesentinella/hub-kubernetes-agent/issues/85)) ([9184fed](https://github.com/thesentinella/hub-kubernetes-agent/commit/9184fed9240d3b7f2ba33737593a03b81f6b3f14))

## [0.19.6](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.19.5...v0.19.6) (2026-06-09)


### Bug Fixes

* install openshift integrity ([#83](https://github.com/thesentinella/hub-kubernetes-agent/issues/83)) ([089653a](https://github.com/thesentinella/hub-kubernetes-agent/commit/089653ad85fbfec3a6aeef6f0622449b3d2f7cde))

## [0.19.5](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.19.4...v0.19.5) (2026-06-09)


### Bug Fixes

* install openshift integrity ([#81](https://github.com/thesentinella/hub-kubernetes-agent/issues/81)) ([631f33d](https://github.com/thesentinella/hub-kubernetes-agent/commit/631f33d10c2f5d95b8d646d7b8118b25c4e4149a))

## [0.19.4](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.19.3...v0.19.4) (2026-06-09)


### Bug Fixes

* make manifest verification opt-in ([#79](https://github.com/thesentinella/hub-kubernetes-agent/issues/79)) ([e5e3d5d](https://github.com/thesentinella/hub-kubernetes-agent/commit/e5e3d5d9e11325f8ec4a31dcdc7d0e69941be067))

## [0.19.3](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.19.2...v0.19.3) (2026-06-07)


### Bug Fixes

* install openshift integrity ([#76](https://github.com/thesentinella/hub-kubernetes-agent/issues/76)) ([f70c4e8](https://github.com/thesentinella/hub-kubernetes-agent/commit/f70c4e8c70266becbb7e78ace8f3da7c7b98e82b))

## [0.19.2](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.19.1...v0.19.2) (2026-06-07)


### Bug Fixes

* **install:** pin manifest integrity ([745b798](https://github.com/thesentinella/hub-kubernetes-agent/commit/745b798865d87a10bbe08bb85c897fb36e8bf560))
* **install:** support openshift auto-detect ([cfd1e5f](https://github.com/thesentinella/hub-kubernetes-agent/commit/cfd1e5f32505b2c46aa796589eddcf6400ecf494))

## [0.19.1](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.19.0...v0.19.1) (2026-05-29)


### Bug Fixes

* remove problematic pod logs collection ([#73](https://github.com/thesentinella/hub-kubernetes-agent/issues/73)) ([3e7f570](https://github.com/thesentinella/hub-kubernetes-agent/commit/3e7f570458694092f5174da6aa03c96ea4408462))

## [0.19.0](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.18.2...v0.19.0) (2026-05-29)


### Features

* add k8s uid duplicate-cluster detection ([#69](https://github.com/thesentinella/hub-kubernetes-agent/issues/69)) ([ed54b43](https://github.com/thesentinella/hub-kubernetes-agent/commit/ed54b43c9f9e0f99a65c32124ac42e6726c82c37))

## [0.18.2](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.18.1...v0.18.2) (2026-05-29)


### Bug Fixes

* **install:** detect physical cluster conflicts via k8s_uid and show last_seen_at ([#67](https://github.com/thesentinella/hub-kubernetes-agent/issues/67)) ([ba52775](https://github.com/thesentinella/hub-kubernetes-agent/commit/ba52775843a2138bfde649cf83ff9169eb0aff1d))

## [0.18.1](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.18.0...v0.18.1) (2026-05-28)


### Bug Fixes

* **ci:** quote if condition to prevent YAML tag parsing of ! ([#65](https://github.com/thesentinella/hub-kubernetes-agent/issues/65)) ([cfc1894](https://github.com/thesentinella/hub-kubernetes-agent/commit/cfc18947da0bf0157fffac6358a875309997b7a1))

## [0.18.0](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.17.0...v0.18.0) (2026-05-28)


### Features

* warn when cluster_id already registered before agent install ([#63](https://github.com/thesentinella/hub-kubernetes-agent/issues/63)) ([3a8a819](https://github.com/thesentinella/hub-kubernetes-agent/commit/3a8a819f4a11861b4d0ec888a4f161cd5af048f0))

## [0.17.0](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.16.0...v0.17.0) (2026-05-26)


### Features

* ebpf tracing ([#61](https://github.com/thesentinella/hub-kubernetes-agent/issues/61)) ([a4a7c23](https://github.com/thesentinella/hub-kubernetes-agent/commit/a4a7c2382df862fd6b0573dd4fd8a796dcfe1df1))

## [0.16.0](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.15.0...v0.16.0) (2026-05-09)


### Features

* improve container technology classification from image metadata ([#56](https://github.com/thesentinella/hub-kubernetes-agent/issues/56)) ([6b19910](https://github.com/thesentinella/hub-kubernetes-agent/commit/6b199103a7ef934026e77210aa83207e14d463c1))

## [0.15.0](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.14.0...v0.15.0) (2026-05-09)


### Features

* collect configuration resource metadata in snapshots ([#54](https://github.com/thesentinella/hub-kubernetes-agent/issues/54)) ([be1e555](https://github.com/thesentinella/hub-kubernetes-agent/commit/be1e5553a99f6990231c94e65f123f2989851c63))

## [0.14.0](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.13.2...v0.14.0) (2026-05-09)


### Features

* add bounded problematic pod logs to snapshots ([#52](https://github.com/thesentinella/hub-kubernetes-agent/issues/52)) ([8b8aaa7](https://github.com/thesentinella/hub-kubernetes-agent/commit/8b8aaa724d5480df92dfcbbecab9111e84141edf))

## [0.13.2](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.13.1...v0.13.2) (2026-05-09)


### Bug Fixes

* support AGENT_VERSION_OVERRIDE for snapshot agent version ([#50](https://github.com/thesentinella/hub-kubernetes-agent/issues/50)) ([d23b096](https://github.com/thesentinella/hub-kubernetes-agent/commit/d23b096df617b5d04b2b9c28bacef6ce6197a7ac))

## [0.13.1](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.13.0...v0.13.1) (2026-05-08)


### Bug Fixes

* separate ConfigMap and DaemonSet YAML documents ([#48](https://github.com/thesentinella/hub-kubernetes-agent/issues/48)) ([c7a2979](https://github.com/thesentinella/hub-kubernetes-agent/commit/c7a297953dd73109604d6e1d14f731d9b7d8623f))

## [0.13.0](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.12.1...v0.13.0) (2026-05-08)


### Features

* add network resources to snapshot inventory ([#46](https://github.com/thesentinella/hub-kubernetes-agent/issues/46)) ([af02fa2](https://github.com/thesentinella/hub-kubernetes-agent/commit/af02fa2165f44061a0f0c436b7e2f194a7851a61))

## [0.12.1](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.12.0...v0.12.1) (2026-05-08)


### Bug Fixes

* add guarded update_agent image rollout command ([#44](https://github.com/thesentinella/hub-kubernetes-agent/issues/44)) ([160849b](https://github.com/thesentinella/hub-kubernetes-agent/commit/160849b6f8b95cfbd56d86eb37a5b379237768c8))

## [0.12.0](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.11.1...v0.12.0) (2026-05-08)


### Features

* add bounded Kubernetes events to inventory snapshots ([#42](https://github.com/thesentinella/hub-kubernetes-agent/issues/42)) ([c1518ed](https://github.com/thesentinella/hub-kubernetes-agent/commit/c1518ed05612f5b86d807e1033c329f5eedcdf73))

## [0.11.1](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.11.0...v0.11.1) (2026-05-08)


### Bug Fixes

* send actions_enabled state in agent snapshot metadata ([#40](https://github.com/thesentinella/hub-kubernetes-agent/issues/40)) ([9ee8353](https://github.com/thesentinella/hub-kubernetes-agent/commit/9ee83537f789b90f952db602a71d71f5127884b5))

## [0.11.0](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.10.0...v0.11.0) (2026-05-07)


### Features

* add self_update command for immediate agent restart ([#38](https://github.com/thesentinella/hub-kubernetes-agent/issues/38)) ([c034757](https://github.com/thesentinella/hub-kubernetes-agent/commit/c03475767f9dd99451cce68df1f1b28b4e3f336f))

## [0.10.0](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.9.0...v0.10.0) (2026-05-07)


### Features

* include pod age seconds in inventory snapshots ([#36](https://github.com/thesentinella/hub-kubernetes-agent/issues/36)) ([b93cb7e](https://github.com/thesentinella/hub-kubernetes-agent/commit/b93cb7e2019bb4eb9e2a3434d78f9c4f4f9576cc))

## [0.9.0](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.8.1...v0.9.0) (2026-05-07)


### Features

* add storage inventory signals to snapshot payload ([#34](https://github.com/thesentinella/hub-kubernetes-agent/issues/34)) ([b978cfd](https://github.com/thesentinella/hub-kubernetes-agent/commit/b978cfdd965c1bd593ceea314c225fc915e2ffee))

## [0.8.1](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.8.0...v0.8.1) (2026-05-07)


### Bug Fixes

* externalize agent auth secret from deploy manifest ([#32](https://github.com/thesentinella/hub-kubernetes-agent/issues/32)) ([f69c494](https://github.com/thesentinella/hub-kubernetes-agent/commit/f69c494d7cda0665f9873015da42cb79043a812b))

## [0.8.0](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.7.0...v0.8.0) (2026-05-07)


### Features

* implement live apply for workload resource patches ([#30](https://github.com/thesentinella/hub-kubernetes-agent/issues/30)) ([92032a5](https://github.com/thesentinella/hub-kubernetes-agent/commit/92032a592bba2950064d81821f7708605eb88838))

## [0.7.0](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.6.2...v0.7.0) (2026-05-07)


### Features

* add preflight warning signals for preview resource patches ([#28](https://github.com/thesentinella/hub-kubernetes-agent/issues/28)) ([a8103e2](https://github.com/thesentinella/hub-kubernetes-agent/commit/a8103e2f7be69388bae96d860c0677dbd0902bcb))

## [0.6.2](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.6.1...v0.6.2) (2026-05-07)


### Bug Fixes

* harden hub route fallback logging behavior ([#26](https://github.com/thesentinella/hub-kubernetes-agent/issues/26)) ([7dd688e](https://github.com/thesentinella/hub-kubernetes-agent/commit/7dd688e1d7ad905758edbf379f677d54235c35c0))

## [0.6.1](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.6.0...v0.6.1) (2026-05-07)


### Bug Fixes

* set default HUB_URL to api.hub.sentinel.la ([#24](https://github.com/thesentinella/hub-kubernetes-agent/issues/24)) ([a7bdb21](https://github.com/thesentinella/hub-kubernetes-agent/commit/a7bdb210867dcc3f6fd43069945bd9a5dfc917db))

## [0.6.0](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.5.0...v0.6.0) (2026-05-07)


### Features

* add POST request body previews in HTTP debug logs ([#22](https://github.com/thesentinella/hub-kubernetes-agent/issues/22)) ([07b3e4b](https://github.com/thesentinella/hub-kubernetes-agent/commit/07b3e4b3e99a388c0a8b8e256528ebbcd4042f15))

## [0.5.0](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.4.0...v0.5.0) (2026-05-07)


### Features

* add bounded hub HTTP debug previews and warn suppression ([#20](https://github.com/thesentinella/hub-kubernetes-agent/issues/20)) ([6a8f226](https://github.com/thesentinella/hub-kubernetes-agent/commit/6a8f226201ea40eccf500e768de2d163b0537ffc))

## [0.4.0](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.3.1...v0.4.0) (2026-05-05)


### Features

* add workload resource preview action ([#16](https://github.com/thesentinella/hub-kubernetes-agent/issues/16)) ([9337df0](https://github.com/thesentinella/hub-kubernetes-agent/commit/9337df0c7368d6fd5dd497c8be092f4131605735))

## [0.3.1](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.3.0...v0.3.1) (2026-05-01)


### Bug Fixes

* replace unwrap() with expect() in health.rs for better diagnostics ([#13](https://github.com/thesentinella/hub-kubernetes-agent/issues/13)) ([f61db48](https://github.com/thesentinella/hub-kubernetes-agent/commit/f61db480b242b697e2248d21cf58d89c100022ff))

## [0.3.0](https://github.com/thesentinella/hub-kubernetes-agent/compare/v0.2.0...v0.3.0) (2026-05-01)


### Features

* add language detection to container technology ([7f49142](https://github.com/thesentinella/hub-kubernetes-agent/commit/7f49142a6c6a415e43d4e9116dee3a860dd76704))
* add language detection to container technology ([37f1a75](https://github.com/thesentinella/hub-kubernetes-agent/commit/37f1a758d7abb448d7f047ca8fc1bc16cc9e2a0e))


### Bug Fixes

* adapt to kube-leader-election 0.43 enum API ([842c7ed](https://github.com/thesentinella/hub-kubernetes-agent/commit/842c7ed005a9198c8b9244d8a99f5d6806d1f06c))
* copy .rs files from root into src/ for docker build ([534692a](https://github.com/thesentinella/hub-kubernetes-agent/commit/534692a9c4008db0f053ba5210df4873233bfd27))
* reset env vars ([6deac44](https://github.com/thesentinella/hub-kubernetes-agent/commit/6deac4480cd0687df4e4727d296ab7399b0e523b))
* resolve clippy and dead_code warnings ([8db2e9b](https://github.com/thesentinella/hub-kubernetes-agent/commit/8db2e9b48c682e3882559fd99971b3145d760f1c))
* unsafe to reset env ([854e24e](https://github.com/thesentinella/hub-kubernetes-agent/commit/854e24e4311dd3d9163437081cb85e598c13655c))
* upgrade rust to 1.88 for kube crate compatibility ([d081e52](https://github.com/thesentinella/hub-kubernetes-agent/commit/d081e5252956128e382d5ebd0f6f6eda97d49c8f))

## [0.2.0](https://github.com/thesentinella/hub-kubernetes-agent/compare/sentinella-hub-k8s-agent-v0.1.0...sentinella-hub-k8s-agent-v0.2.0) (2026-05-01)


### Features

* add language detection to container technology ([7f49142](https://github.com/thesentinella/hub-kubernetes-agent/commit/7f49142a6c6a415e43d4e9116dee3a860dd76704))
* add language detection to container technology ([37f1a75](https://github.com/thesentinella/hub-kubernetes-agent/commit/37f1a758d7abb448d7f047ca8fc1bc16cc9e2a0e))


### Bug Fixes

* adapt to kube-leader-election 0.43 enum API ([842c7ed](https://github.com/thesentinella/hub-kubernetes-agent/commit/842c7ed005a9198c8b9244d8a99f5d6806d1f06c))
* copy .rs files from root into src/ for docker build ([534692a](https://github.com/thesentinella/hub-kubernetes-agent/commit/534692a9c4008db0f053ba5210df4873233bfd27))
* reset env vars ([6deac44](https://github.com/thesentinella/hub-kubernetes-agent/commit/6deac4480cd0687df4e4727d296ab7399b0e523b))
* resolve clippy and dead_code warnings ([8db2e9b](https://github.com/thesentinella/hub-kubernetes-agent/commit/8db2e9b48c682e3882559fd99971b3145d760f1c))
* unsafe to reset env ([854e24e](https://github.com/thesentinella/hub-kubernetes-agent/commit/854e24e4311dd3d9163437081cb85e598c13655c))
* upgrade rust to 1.88 for kube crate compatibility ([d081e52](https://github.com/thesentinella/hub-kubernetes-agent/commit/d081e5252956128e382d5ebd0f6f6eda97d49c8f))
