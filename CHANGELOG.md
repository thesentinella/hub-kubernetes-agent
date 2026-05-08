# Changelog

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
