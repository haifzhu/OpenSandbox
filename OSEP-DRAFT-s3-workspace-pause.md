# OSEP Draft: S3-Compatible Workspace Pause(基于 S3 协议对象存储的工作区持久化暂停)

> 状态:Draft(未编号)
> 关联:[issue #1169](https://github.com/opensandbox-group/OpenSandbox/issues/1169)、OSEP-0008(rootfs snapshot)、OSEP-0003(volumes)、OSEP-0015(operatingMode/SnapshotClass,draft)
> 代码基线:本仓库 master(75f4c267)

## 1. 背景与动机

现有 pause(OSEP-0008)把整个容器 rootfs commit 成 OCI 镜像推送到 registry。生产实践暴露的问题:

| 问题 | 生产实证 |
|---|---|
| 镜像层只增不减,越用越大 | 单沙箱 snap-gen606 / gen854(600+ 层) |
| commit Job 挂宿主 containerd socket,特权 + 节点本地绑定 | commit 依赖节点本地镜像名记录,记录被 GC 后无限重试(死循环实例) |
| 依赖 OCI registry,每沙箱一 repo、数百 tag | registry 存储与 retention 成本 |
| 提交全量文件系统(含未变化的系统层),又大又慢 | pause/resume 耗时随使用线性增长 |

issue #1169 提议:pause 只把工作区同步到对象存储,resume 用固定基础镜像 + 挂回工作区。本设计将其落地为**第三种 checkpoint provider**,与现有 rootfs / qemu 并存。

**存储后端是 S3 协议兼容的对象存储,不绑定具体厂商**:阿里云 OSS、MinIO、AWS S3、Ceph RGW 均可;通过 endpoint + region + path-style 寻址,支持部署级前缀隔离(多套环境/多租户共用一个 bucket)。

## 2. 目标 / 非目标

**目标**
- pause = 增量同步指定路径(默认 `/workspace`)到 S3 兼容对象存储 → 删除 Pod 释放算力;不产生任何 OCI 镜像。
- resume = 固定基础镜像 + `workspace-restore` init 容器从对象存储拉回工作区。
- 后端中立:同一实现跑通 OSS / MinIO / AWS S3(e2e 矩阵覆盖)。
- 前缀隔离:可配置基础前缀,**所有对象一律写在该前缀之下**(结构性保证,不依赖使用方自觉)。
- 同步数据面完全在沙箱容器内完成(execd),无 hostPath、无 containerd socket、兼容 PSS restricted。
- 增量同步:二次起 pause 只传 delta,目标 <5s(百 MB 级 delta)。
- 失败语义有熔断:同步失败沙箱保持 Running,有限次重试后终止,绝不无限循环。
- 与 rootfs provider 完全向后兼容(无注解 = rootfs,`contract.go:62` 现状保持)。

**非目标(v1)**
- 保留进程内存态(Hibernate 范畴,见 OSEP-0015)。
- 对象存储直接挂载(s3fs/ossfs/CSI)作为 resume 路径——随机小 IO 性能风险,列为 P2 替代模式。
- 同步系统层路径(`/usr`、`/etc` 的修改会丢失,与 issue #1169 权衡一致;通过可配置 paths 缓解)。
- idle 自动暂停对 BatchSandbox 后端的泛化(idle_pause 目前仅 fsb,见 §11)。

## 3. 总体架构

```text
pause:
Client ──POST /pause──▶ Server ──patch spec.pause=true──▶ BatchSandbox
Controller(BatchSandbox):
  stopTasksBeforePause(现有) ─▶ ensureInternalPauseSnapshot(现有)
SandboxSnapshot Controller:
  ContractFromPod: provider=s3-workspace ─▶ Format=s3-workspace-v1
  buildSyncJob(新,非特权) ──HTTP──▶ 源 Pod 内 execd /internal/snapshot
       execd: 增量 manifest diff ─▶ 上传变化文件 ─▶ 原子换 LATEST 指针
  Job 校验 LATEST 指针 ─▶ termination message ─▶ phase=Succeed
Controller: 删源 Pod ─▶ BatchSandbox phase=Paused

resume:
Client ──POST /resume──▶ spec.pause=false
Controller: 读 SandboxSnapshot(Format=s3-workspace-v1)
  不替换镜像(保留原基础镜像)
  injectWorkspaceRestore(新):
    + 共享卷(emptyDir/generic-ephemeral)挂到同步路径
    + init 容器 workspace-restore: 按 manifest 拉回文件并校验 digest
  重建 Pod ─▶ Running
```

**对象布局(全部落在前缀之下)**:

```text
s3://{bucket}/{prefix}/sandboxes/{namespace}/{sandboxId}/
  ├─ LATEST                      # JSON: {generation, manifestDigest, completedAt}
  ├─ gen/{N}/manifest.json       # 文件清单: path→{sha256,size,mode,uid,gid,mtime,type}
  └─ blobs/{sha256前2位}/{sha256} # 共享内容寻址池(跨代去重,见附录 A)
```

- `prefix` 解析顺序:请求级 `prefix` → 部署级 `base_prefix` → 空。**所有 key 一律由 `prefix + 固定子路径` 拼接生成,客户端不存在写出前缀之外的代码路径**——这是结构性保证,使多套部署/多租户可安全共用一个 bucket,存储侧权限策略也可按 `bucket/prefix/*` 最小化授权。
- 内容寻址:文件按 sha256 存 blob,跨代去重(增量天然成立)。
- `LATEST` 只在 gen N 的 manifest 与全部 blob 上传完成后才覆写——部分上传对外不可见。
- 保留最近 N 代(默认 3),旧代由 retention 清理;沙箱删除时 best-effort 删前缀 + bucket lifecycle 兜底。

## 4. 关键设计决策

**D1 载体:增量 manifest 同步 + S3 协议对象存储,而非整盘 commit / 直挂**
issue #1169 原案即如此;相比直挂免掉 K8s 侧 FUSE/CSI 依赖与随机 IO 风险,相比 rootfs commit 免掉 registry 与特权。协议上只依赖 S3 API(Get/Put/Head/Delete + multipart),以 aws-sdk-go-v2 + `{endpoint, region, forcePathStyle}` 寻址,OSS/MinIO/AWS S3/Ceph 同一套实现。

**D2 数据面执行者:execd;触发者:非特权 sync Job**
execd 是唯一保证运行在沙箱容器内、具备文件访问能力、且由 OpenSandbox 自控的组件(Go,`components/execd`)。同步逻辑放 execd(进程内直接读文件系统),复用现有 SandboxSnapshot→Job 骨架做触发、超时与重试(Job 只做"调 execd + 校验指针 + 回填结果",非特权、可调度任意节点——摆脱同节点绑定)。
备选(否决):nsenter 特权 Job(保留 hostPath 问题);controller 直接调 execd(数据面调用进 reconcile 循环,失去 deadline/backoff 语义)。

**D3 provider 选择复用 Pod 注解契约**
`kubernetes/internal/snapshot/contract.go` 已有 `checkpoint-provider` 注解(rootfs|qemu)。新增 `s3-workspace` 及其契约注解(paths、target、credsSecretRef、exclude、execd port),server 在创建时把 `pausePolicy` 翻译成 pod template 注解——CRD 的 provider 选择面零改动,与 0015 的"claim/class 由 server 物化"方向一致。

**D4 resume 注入:镜像不变 + workspace-restore init 容器**
镜像 `injectQEMURestore` 的现成模式(`batchsandbox_pause_resume.go:634,700-706`):同步路径注入共享卷 + init 容器拉回。与 rootfs 路径的本质区别:**不替换主容器镜像**(始终用原基础镜像,镜像尺寸恒定)。
创建期一致性:server 对配置了本 pausePolicy 的沙箱,在 **create 时就注入相同的卷布局**(同步路径从第一刻起就是卷挂载),避免"首次 pause 前数据在可写层、之后在卷上"的迁移期错配。

**D5 失败熔断(生产事故教训;实现形态经集群实测修订)**
- 同步失败 → 沙箱保持 Running(与 OSEP-0008 建议一致),`SandboxSnapshot.phase=Failed`。
- 熔断实现在 **server 重试桥**侧(无限重试的真实来源,见 81887bd6 案例):workload 注解 `opensandbox.io/pause-failure-count` 计连续失败,超过 `[pause.s3] max_attempts`(默认 3,**0=无限**)时 pause 请求以 409 拒绝;resume 清零。CRD status 字段与 Event 通道留作后续增强(需要向公共 API 暴露失败历史时再加)。
- Job `activeDeadlineSeconds` 默认 600s、`backoffLimit` 3(沿用现有 commit Job 参数);控制器自身失败后不自动重试(回 Running + PauseFailed condition)。

## 5. API 与配置变更

**CreateSandboxRequest**(server `api/schema.py`,新增可选字段):

```json
{
  "pausePolicy": {
    "provider": "S3Workspace",
    "paths": ["/workspace", "/root/.lingma"],
    "bucket": "lingma-sandboxes",
    "prefix": "tenant-a",
    "credentialsSecretName": "s3-sandbox-creds",
    "excludePatterns": ["**/node_modules/.cache/**"],
    "retentionGenerations": 3
  }
}
```
- 未设置 → 行为与现在完全一致(rootfs)。
- `bucket`/`prefix` 为可选覆盖,必须命中 server 配置的 allowlist(`allowed_buckets`/`allowed_prefixes`),防越权写别的桶/前缀;缺省用部署级默认。
- server 侧翻译为 pod template 注解:
  - `sandbox.opensandbox.io/checkpoint-provider: s3-workspace`
  - `sandbox.opensandbox.io/s3-pause-paths` / `-target`(完整 `s3://bucket/prefix/sandboxes/{ns}/{id}`)/ `-creds-secret` / `-exclude`(JSON)
  - `sandbox.opensandbox.io/execd-port`(sync Job 寻址用)

**SandboxSnapshot CRD**(`sandboxsnapshot_types.go`):
- `Format` 新增枚举值 `s3-workspace-v1`。
- `Status` 新增 `workspace *WorkspaceSnapshot`(Target/Generation/ManifestDigest/Roots/Endpoint/Region/ForcePathStyle/CredentialsSecretName/FilesTotal/BytesUploaded/SyncSeconds);rootfs/qemu 路径不填。
- `sync_job_image` 以 controller 启动 flag(`--workspace-sync-image`)提供(镜像供给是控制器关切,不走 server 配置)。

**snapshot.Request**(`internal/snapshot/types.go`):新增 `S3 *S3Request{TargetURL, Paths, ExcludePatterns, CredsSecretRef, Generation}`,sync Job 以 base64 JSON 传入(沿用 qemu 模式,`types.go:43-53`)。

**server 配置**(`config.py`):

```toml
[pause.s3]
endpoint = "http://minio.opensandbox.svc:9000"  # S3 兼容端点;纯 AWS S3 可省略走默认
region = "us-east-1"
bucket = "opensandbox-snapshots"                 # 默认桶
base_prefix = "lingma/prod"                      # 基础前缀:所有对象写在其下(可空)
force_path_style = true                          # MinIO/on-prem 必需;AWS/OSS 虚拟主机风格可 false
credentials_secret = "s3-sandbox-creds"
allowed_buckets = []                             # 请求级 bucket 覆盖的白名单
allowed_prefixes = []                            # 请求级 prefix 覆盖的白名单
default_paths = ["/workspace"]
sync_job_image = ""
max_attempts = 3
```

**寻址与兼容性规则**:
- endpoint 缺省 → AWS S3 官方分区寻址;显式 endpoint → 任意 S3 兼容服务(OSS 的 S3 兼容端点、MinIO、Ceph RGW、云上自建)。
- `force_path_style=true` 时用 `https://{endpoint}/{bucket}/{key}` 路径风格(MinIO 必需);false 时虚拟主机风格。
- 目标 URL 规范形式:`s3://{bucket}/{prefix}/sandboxes/{namespace}/{sandboxId}`,组件(endpoint/region/path-style/凭证)不进 URL、由注解与配置携带。
- 凭证:Secret 内为 `{accessKey, secretKey, sessionToken?}`;不同后端同一格式。

## 6. 组件改动清单

**server(Python)**
- `api/schema.py`:PausePolicy 模型 + 校验(恰好一个 provider;S3Workspace 必填 bucket/secret;bucket/prefix 覆盖须过 allowlist;VM/Windows 平台组合直接 400)。
- `services/validators.py`:paths 白名单校验(绝对路径、去重、不含 `/` 自身与卷挂载冲突路径);target URL 规范化。
- `services/k8s/batchsandbox_provider.py`:create 时注解注入 + 同步路径卷布局注入(emptyDir;generic-ephemeral 留作 P1 的 sizeLimit 增强);VM/Windows 平台互斥校验;pause 熔断(见 D5)。
- 删除语义:**BatchSandbox finalizer 驱动**(`workspace-cleanup`)——resume 后内部快照会被平台回收,挂快照删除会漏清理;finalizer 保证任意删除时机(paused、resume 后、直接删)都会派发 purge Job(独立无属主,TTL 回收;bucket lifecycle 兜底)。

**k8s controller(Go)**
- `internal/snapshot/contract.go`:`ProviderS3Workspace = "s3-workspace"` + 契约解析(paths/target/creds/exclude/execd-port 注解)。
- `internal/controller/sandboxsnapshot_lifecycle.go`:`handlePending` 分支 → `buildSyncJob()`:非特权、无挂载、`nodeName` 不固定;通过 Pod IP + execd-port 调 `POST /internal/snapshot`;校验 `LATEST.generation == N` 后写 termination message(新 `S3Result`)。
- `internal/controller/batchsandbox_pause_resume.go`:
  - pause 熔断:失败计数、超限停摆(§4 D5)。
  - `injectWorkspaceRestore()`:镜像不变;为每个同步路径注入共享卷 + `workspace-restore` init 容器(镜像=sync job image 的 restore 模式,凭证来自 secret env,读注解里的 target/gen)。
- `internal/snapshot/types.go`:`S3Request` / `S3Result`。

**execd(Go,`components/execd`)**
- 新增内部 API:`POST /internal/snapshot/sync`(仅监听 pod 网络,幂等,带 generation 栅栏)。
- 同步引擎(`pkg/objsync/`):算法见附录 A;S3 客户端为 aws-sdk-go-v2 + 自定义 Resolver(endpoint/region/forcePathStyle),multipart 上传。
- 新增 `restore` 子命令(init 容器入口):拉 manifest → 并发下载 blob → 校验 sha256 → 恢复 mode/owner/symlink → 完成。

**镜像**
- `s3-workspace-syncer`:内含 execd(sync 触发面)+ restore 入口。无特权要求,`runAsNonRoot` 可行。

## 7. 一致性与正确性

- **暂停期写入静止**:依赖现有 `stopTasksBeforePause`(Task CR 先停);execd 侧 `syncgate`(RWMutex):变更类请求全程持读锁,sync 取写锁并有界等待(30s)排空在跑命令,同步期间新变更请求 503;freeze 原语为 P1 增强(覆盖非 execd 启动的后台进程)。
- **原子可见性**:resume 只认 `LATEST`;manifest 带 `completedAt`,未完成的 gen 目录永不进 `LATEST`。单对象 put 原子 + 单写者(控制器单 pause 语义);后端支持 If-Match 时加条件写兜底。
- **完整性校验**:restore 逐文件 sha256 校验,失败 → `ResumeFailed`(fail closed)。
- **跨节点**:sync Job 与 restore 均不绑节点;base image 各节点已有缓存,首次 resume 增量为 delta 下载。
- **多卷路径冲突**:paths 与用户显式 volumes 挂载点重叠时,create 校验直接拒绝(400)。

## 8. 安全

- v1:AK/SK(可选 sessionToken)走 K8s Secret(`credentialsSecretName`),以 env 投影给 execd/sync Job/restore init 容器;secret 不出现在公共 API 响应。
- 授权最小化:存储侧策略按 `bucket/{prefix}/*` 收敛——MinIO policy、OSS RAM、AWS IAM 均支持前缀级 ARN/condition;配合 allowlist,请求级覆盖也出不了授权前缀。
- Roadmap:workload identity / credential vault(OSEP-0012)/ 每 sandbox STS 临时凭证——OSEP-0015 已明确"凭证不过 API、不落 Secret"的方向,接口按 `credentialsSecretName` 抽象,后续替换实现不动契约。

## 9. 可观测性

- execd:sync 时长/文件数/字节数/失败计数(Prometheus),按 backend(endpoint host)打标签。
- SandboxSnapshot status 落 SyncDuration/TotalBytes(FileCount);controller 打点 phase 分布与熔断事件。
- Event:`WorkspaceSyncFailed`、`PauseCircuitBreakerOpen`、`WorkspaceRestoreFailed`。
- 对比 rootfs 的核心收益指标:pause P99 时长、每 pause 净增存储字节(delta)。

## 10. 失败模式

| 故障 | 行为 |
|---|---|
| execd 不可达 / 同步中崩溃 | sync Job 重试(≤3)→ Failed → 沙箱保持 Running;计数器 +1 |
| 对象存储限流/瞬断 | 同上;multipart 分片重试内置 |
| 上传中断 | 孤儿 blob,`LATEST` 未动,resume 不受影响;retention 回收 |
| 同步成功但删 Pod 前控制器崩溃 | `LATEST` 已指向新代;重协调后幂等完成删除(现有 reconcile 恢复) |
| restore 下载/digest 失败 | init 容器退出 → Pod 起不来 → `ResumeFailed`,沙箱回 Paused(LATEST 完好可重试) |
| 熔断打开后用户再 pause | 409 + 提示;resume→pause 或清除计数后恢复 |
| 对象前缀被误删 | resume fail closed;沙箱需重建(与 registry 镜像丢失同级风险,retention/版本化兜底) |

## 11. 与现有机制的关系

- **rootfs / qemu provider**:并存,注解缺省仍为 rootfs;qemu 沙箱(VM 类)不适用本 provider——server create 侧按平台(Windows/VM)拒绝,k8s 契约解析侧拒绝与 qemu 契约注解共存。
- **idle_pause 集成**:当前仅 fsb 后端。本设计把单次 pause 成本从"全量 commit"降为"delta 同步",使 BatchSandbox + 短空闲阈值成为可能——建议作为后续 OSEP 把 sweeper/executor 泛化到 BatchSandbox,这是短空闲 serverless 诉求的长期正解。
- **OSEP-0015 对齐**:本设计的 `pausePolicy.provider` 与 0015 的 `operatingMode + SandboxSnapshotClass` 不冲突——class 的 `S3/BlobStore` 后端类型即本设计的数据面;0015 落地时 `pausePolicy` 平移为 class/claim 选择即可,数据面(execd objsync、restore init)完全复用。
- **volumes(PVC/NAS)**:可平行增加 `pvc-workspace` provider(免同步、挂载即持久),复用同一套契约/注入机制,P1 评估。

## 12. 迁移与灰度

1. 默认不变(rootfs);部署级配置默认 provider;单沙箱级 opt-in。
2. 新建沙箱生效;存量沙箱沿用 rootfs 自然退役(registry retention 清理)。
3. 试点:新 workspace 以 `pausePolicy=S3Workspace, paths=[/workspace, /root/.lingma]` 创建;灰度观察 pause P99 / 增量字节 / resume P99。
4. 回滚:不设 pausePolicy 即回 rootfs;已 paused 的沙箱 resume 一次后,下一次 pause 可切回(注解为 create 期属性,重建时改)。

## 13. 测试计划

- 单元:manifest diff 正确性(增/删/改/symlink/mode)、契约解析、注入函数(卷/init 容器)、熔断计数、target URL 规范化与 allowlist。
- 集成(kind + MinIO):pause→sync→删 Pod→resume→文件逐字节比对;失败注入(存储断连→沙箱保持 Running;杀 execd→重试;部分上传→LATEST 不变);熔断后 409;前缀隔离(两套 base_prefix 互不可见)。
- **后端矩阵 e2e**:MinIO(必测,path-style)、阿里云 OSS S3 兼容端点、AWS S3(虚拟主机风格)。
- 性能:首同步 vs 增量(delta<100MB 时 pause 数据面 <5s);resume P99。
- 混沌:同步中删 Pod、控制器重启、重复 pause/resume 摆动。

## 14. 分阶段交付

- **P0(可用)**:契约注解、sync Job、execd 同步引擎、restore init、熔断、MinIO e2e。
- **P1**:pvc-workspace provider、冻结原语集成、每文件 zstd、指标面板、idle_pause 泛化设计、OSS/AWS 后端矩阵补齐。
- **P2**:workload identity/STS 凭证、直挂(s3fs/ossfs/CSI)resume 模式、按 delta 自适应 deadline、retention 自动化。

## 附录 A:delta 同步引擎算法详设

**模型**:manifest 指纹 diff + 内容寻址 blob 仓库 + 单写者原子指针。引擎在 execd 内、无本地状态(resume 后容器重建,全部状态在对象存储)。

**manifest(每代全量清单)**:

```json
{"formatVersion":"s3-workspace-v1","generation":N,
 "files":[{"path":"app/main.go","type":"reg","size":2048,"mode":420,
           "uid":0,"gid":0,"mtimeNano":…,"sha256":"ab12…"}],
 "stats":{"filesTotal":…,"filesChanged":…,"bytesUploaded":…}}
```

**同步流程**:读 `LATEST` 得上一代 manifest → walk(并行,statx 指纹 = type/size/mode/uid/gid/mtime_ns):
- 指纹一致 → 继承旧 sha256,不读文件(rsync quick-check;免全量哈希是延迟达标关键)
- 变化/新增 → 流式哈希,哈希后复查 mtime(TOCTOU 防护),上传共享池 `blobs/{h[:2]}/{h}`(存在则跳过,跨代去重)
- symlink 哈希 target 字符串;删除 = 新 manifest 缺席
最后上传 manifest → 覆写 `LATEST`(唯一提交点)。

**正确性**:
- 部分上传不可见:resume 只认 LATEST;单对象 put 原子 + 单写者(控制器单 pause 语义)+ 可选 If-Match 条件写。
- 幂等:同代重跑 diff 相同、blob 覆盖无害,无需事务日志。
- rename 便宜:新 path 重新哈希命中已有 blob,上传自动跳过(内容寻址白送)。
- 指纹碰撞:ns-mtime 实际难碰;每 K 代强制全量 verify 兜底。
- blob GC:retention 扫描保留代引用集,清无引用且超安全期的 blob。

**写静止**:stopTasksBeforePause(现有)+ execd 会话门控(拒新会话、有界等待在跑命令);非 execd 启动的后台进程竞态 v1 接受、P1 freeze。

**restore**:读 LATEST → manifest digest 校验 → 并发(4)下载 `path.tmp-{hash}` → 流式 sha256 校验 → rename + 恢复 mode/owner/mtime/symlink;失败 fail closed 回 Paused。

**参数**:`<8MB` 单 put,`≥8MB` 8MB 分片;worker 4-8;全程流式内存有界。
**局限**:大文件小改动整文件重传(exclude 规避,P2 CDC 分块);首次同步全量(一次性)。
