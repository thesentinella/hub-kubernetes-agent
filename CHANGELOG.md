# Changelog

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
