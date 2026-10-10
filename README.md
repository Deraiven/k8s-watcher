# Namespace Watcher

Namespace Watcher 是一个运行在 Kubernetes 集群中的异步控制服务，负责监听 Zadig 创建和删除 FAT 子环境时产生的 Kubernetes Namespace 事件，并同步管理子环境依赖的基础设施资源。

它不是业务服务，也不负责部署业务镜像。Zadig 负责创建环境和部署服务，Namespace Watcher 负责在环境生命周期内补齐、清理和修复外围资源，例如证书、DNS、AWS 消息资源、Kong 路由、Apollo 配置和 Istio 网络范围。

## 解决的问题

创建一个 Zadig 子环境通常不只有 Namespace，还需要一组与环境名称绑定的外部资源：

- *.testN.shub.us 的 TLS 证书和 DNS 记录
- 以 testN 命名的 SQS Queue 和 SNS Topic/Subscription
- Kong 中对应环境的路由、服务、插件和证书
- Apollo 中对应环境的 Cluster、Namespace、Item 和 Release
- Zadig workflow 中的可选环境参数
- Istio Sidecar 对跨 Namespace 配置访问范围的限制

当环境被删除时，这些资源也需要按环境清理，否则会产生残留资源、错误路由、Apollo 配置污染和 AWS 成本。Namespace Watcher 将这些操作集中在一个生命周期控制器中执行。

## 核心职责

### 1. Namespace 生命周期监听

服务使用 Kubernetes Namespace watch stream 监听 ADDED 和 DELETED 事件。只有同时满足以下条件的 Namespace 才会处理：

- 名称以 test 开头
- 名称符合 test 加数字的格式，例如 test1、test22
- Namespace label 为 createdBy=koderover
- 不在 EXCLUDED_NAMESPACES 白名单中

因此，普通系统 Namespace、非 Zadig 创建的 Namespace 和白名单环境不会被自动处理。

### 2. 子环境创建时的资源初始化

当 Zadig 创建一个符合条件的 Namespace 后，服务会启动以下流程：

~~~text
Kubernetes Namespace ADDED
        |
        +-- 加入子环境 Deployment 监控
        |
        +-- 创建或等待 cert-manager Certificate
        |       |
        |       +-- Certificate Secret 就绪
        |       +-- 上传证书到 Kong
        |
        +-- 并行创建 AWS SQS/SNS 资源
        +-- 并行复制 Kong 环境路由
        +-- 并行创建 Istio Sidecar scope
        +-- 并行更新 Zadig workflow 参数
        |
        +-- Certificate 就绪后创建 Cloudflare DNS CNAME
~~~

证书和 DNS 存在依赖关系，因此 DNS 会等待证书流程完成；AWS、Kong、Istio 和 Zadig workflow 更新可以并行执行。

### 3. Deployment 级别的子环境监控

Apollo 和部分 Kong 配置不是在 Namespace 创建时批量生成，而是由 Deployment 监控按实际服务创建。

这样可以保证：

- 子环境只创建实际部署服务对应的 Apollo Cluster
- 参考环境中存在、但当前子环境没有部署的服务不会产生 Apollo 配置
- 子环境后续新增服务时可以自动补齐配置
- watcher 重启或监控刷新后，可以回放已有 Deployment，修复遗漏配置

Deployment 监控会定期从 Zadig 获取子环境列表，并通过 Kubernetes Deployment watch stream 监听所有 Namespace 的 Deployment 事件，但只处理当前被 Zadig 识别为子环境的 Namespace。

Deployment 创建时会执行：

- 确保 Kong 中存在对应服务的环境路由
- 按服务复制 Apollo 配置

特殊服务映射：

| Deployment | Apollo App 配置 |
| --- | --- |
| backoffice-v1-web-app | backoffice-v1-web、backoffice-v2-webapp |
| beep-v1-web | beep-v1-web、beep-v1-webapp |
| 其他 Deployment | 按 Deployment 名称匹配对应 App |

bo-v1-assets 和 inventory-cronjob 当前被配置为跳过 Apollo 同步。

Deployment 删除时会删除该服务对应的 Kong 路由，不立即删除 Apollo 配置。Namespace 删除时，通过 Kong HMAC 认证调用 Apollo Admin Service 删除对应 Cluster，不回退到数据库直写。

### 4. 子环境删除时的资源清理

当 Namespace 收到 DELETED 事件时，服务会执行环境级清理：

- 删除 Cloudflare wildcard DNS CNAME
- 删除 cert-manager Certificate 和 TLS Secret
- 从 Kong 删除环境路由、服务、插件和证书关联
- 删除该环境的 SQS Queue
- 删除该环境的 SNS Subscription 和 Topic
- 通过 Apollo Admin Service 删除该环境在各 App 下的 Cluster
- 从 Zadig workflow 的环境参数中移除该环境
- 删除 Istio Sidecar scope
- 从子环境 Deployment 监控集合中移除该 Namespace
- 更新 Redis 中的 Namespace 状态

删除操作尽可能保持幂等。资源不存在时会记录并跳过，以支持重复事件、watch stream 重启和部分失败后的重试。

## Apollo 配置策略

参考环境默认为 test33，由 REFERENCE_ENV 配置。

Apollo 配置复制遵循以下策略：

1. Namespace 创建时不再复制参考环境的全部 Apollo Cluster。
2. Deployment 被发现时，按实际 App 创建目标环境的 Cluster。
3. 通过 OpenAPI 创建 Cluster，由 Apollo 自动实例化 App Namespace；可能出现空 secret Namespace，但 watcher 不读取或复制其中的配置。
4. 只读取并复制 web.<app> Namespace 下的配置 Item，已有 key 不覆盖。保留原有环境名替换规则，SQS URL 和 SNS ARN 不替换。
5. 没有 Release 时通过发布接口创建首次 Release，包含目标 Namespace 当前配置；已有 Release 不自动重发，避免发布人工草稿。补齐已有 Release 的缺失 Item 后，需要在 Portal 审核发布。
6. 新建或已有 Cluster 缺少 web.<app> Namespace 时，通过 HMAC 认证的 Admin Service 关联已有 Namespace，再通过 OpenAPI 写入参考 Cluster 的覆盖项并首次发布。不新建全局 AppNamespace，不复制公共 Namespace 的默认配置。缺失 Item 或首次 Release 可在后续同步时补齐。

只为实际 Deployment 创建 Apollo Cluster。OpenAPI 操作不是数据库事务，失败后可能留下部分状态；重试会跳过已有 Cluster 和 Item。不要将一次同步日志视为全部系统均成功。

### Apollo OpenAPI 配置

不再连接 Apollo MySQL，也不需要 MYSQL_* 环境变量或数据库权限。

| 环境变量 | 用途 | 默认值 |
| --- | --- | --- |
| APOLLO_URL | Portal 地址（不是 Config Service） | https://apollo.shub.us |
| APOLLO_ENV | Apollo 环境，不是 testN Cluster 名 | FAT |
| APOLLO_API_TOKEN | OpenAPI Token，通过 Secret 注入 | 必填 |
| APOLLO_OPERATOR | Apollo 中已存在的操作用户 | namespace-watcher |
| APOLLO_TIMEOUT_SECONDS | 单次 HTTP 请求超时秒数 | 30 |
| APOLLO_ADMIN_URL | 删除操作使用的 Admin Service 地址，必须与 APOLLO_ENV 属于同一环境 | https://apollo-admin-fat.shub.us |
| KONG_HMAC_USERNAME | Admin Service 网关 HMAC 用户名，通过 Secret 注入 | 关联 Namespace / 删除时必填 |
| KONG_HMAC_SECRET | Admin Service 网关 HMAC 密钥，通过 Secret 注入 | 关联 Namespace / 删除时必填 |

在现有 `namespace-watcher-secrets` Secret 中添加 `APOLLO_API_TOKEN`；Deployment 已通过 `envFrom.secretRef` 注入，不要把真实 Token 提交到仓库。更新 Secret 后重启 watcher Pod，环境变量才会生效。缺少 Token 且启用 Apollo 时启动直接失败。

Token 必须为单行文本。使用 YAML 多行标量时选择 `|-` 而不是 `|`，通过命令生成时使用 `printf '%s'` 而不是会追加换行的 `echo`。客户端会清除 Token 首尾空白；中间的换行和控制字符会导致启动校验失败，不会输出 Token 内容。出现 `Newline or carriage return detected in headers` 时应检查 Secret 注入的 Token 格式。

在 Apollo Portal 的开放平台为 Token 授予对应 App 的创建 Cluster 权限，以及 `web.<app>` Namespace 在 FAT 的修改和发布权限；别名对应的两个 App 都需要授权。`APOLLO_OPERATOR` 必须是 Apollo 已存在的用户，不会自动创建。Token 通过原始 `Authorization` Header 发送，不加 `Bearer` 前缀。401/403 不自动重试；只对读取操作的限流、服务端错误和网络错误做有限重试。不会记录 Token 或 API 响应配置内容。

创建和发布仍走 Portal OpenAPI。删除走 Admin Service 的 `DELETE /apps/{appId}/clusters/{clusterName}?operator=...`：以 RFC 1123 GMT Date 和包含完整 query string 的 request-line 做 HMAC-SHA256 签名。单独使用 HMAC Header，不向 Admin Service 发送 Portal Token；禁止跳转，不记录密钥或响应内容。

在现有 Secret 中额外注入 `KONG_HMAC_USERNAME` 和 `KONG_HMAC_SECRET`。网关应仅授权所需的 App/Cluster 读取和 Cluster 删除路径，不应开放 App 删除接口。分页读取 `/apps` 直到空页后，精确查找并删除目标 Cluster，不依赖 Portal Token 的 App 可见范围；拒绝参考环境、排除列表、default 和不符合 `test[0-9]+` 的名称。404 视为已不存在，其他错误会中止并上报。未配置 HMAC 凭据时，缺失 Namespace 的关联补建和环境删除都会失败；目标 Namespace 已存在时仍可通过 OpenAPI 同步。

关联接口为 `POST /apps/{appId}/clusters/{cluster}/namespaces`，只提交 `web.{appId}` 的关联信息；需要网关额外授权此 POST 路径和 Namespace GET 路径。HMAC 凭据缺失或认证失败时会终止本次同步，不写入配置或发布。已有目标 key 保留；已有 Release 不自动重发，仍需审核发布新补齐的 Item。

删除不是跨 App 事务，失败时可能部分完成；当前 Namespace 删除处理器仅记录错误并继续更新删除状态，不会自动重试 Apollo 清理，需人工核查并重试。

## AWS 资源和身份认证

AWS Manager 负责复制和清理参考环境 REFERENCE_ENV 对应的 SQS/SNS 资源：

- SQS：分页扫描 Queue、读取 Queue attributes、创建 Queue、删除 Queue
- SNS：创建 Topic、读取参考 Topic subscriptions、复制 SQS subscription、读取 subscription attributes、删除 subscription 和 Topic
- STS：调用 GetCallerIdentity 获取账号 ID，用于生成 Topic ARN

AWS SDK 使用 aioboto3 默认 credential provider chain，不在代码或配置中保存 AWS access key/secret。运行在 EKS 时应通过 IRSA 提供身份，Pod 的 AWS 身份应显示为：

~~~text
arn:aws:sts::<account-id>:assumed-role/<role-name>/<session>
~~~

而不应该是 IAM 用户身份。IRSA Role 需要具备代码实际使用的 SQS、SNS 和 sts:GetCallerIdentity 权限。

## 定时环境清理

Zadig Manager 注册了每天北京时间凌晨 3 点执行的清理任务。调度时区由 SCHEDULER_TZ 控制，当前 Kubernetes 配置使用 CST-8。

清理规则：

- 只处理名称符合 test\d+ 的环境
- 跳过白名单环境：test17、test33、test5、test50
- 跳过生产环境
- Namespace 创建超过 7 天：调用 Zadig sleep API 进入睡眠状态
- 已睡眠超过 7 天：调用 Zadig API 删除环境

同时还有孤儿 Namespace 清理任务：

- 获取 Zadig FAT 项目中的环境列表
- 扫描集群 Namespace
- 只检查 test 加数字的 Namespace
- 只处理 createdBy=koderover 的 Namespace
- 集群中存在、但 Zadig 项目中不存在的 Namespace 会被删除

## 状态恢复和 Reconcile

Redis 用于保存已处理 Namespace 的状态和资源记录。服务启动时会：

- 加载 Redis 中的 active Namespace
- 扫描集群现有 Namespace
- 找出不在 Redis 中的 Namespace 并执行 reconcile
- 找出超过 24 小时未 reconcile 的 Namespace 并重新检查
- 检查 Redis 中已标记删除但集群中已经不存在的 Namespace
- 重新补齐现有 Namespace 的 Istio Sidecar scope

Deployment 监控也会在新 Namespace 加入监控时扫描已有 Deployment，避免 Deployment 早于 watcher 监控建立而漏掉 Apollo 或 Kong 同步。

## 组件结构

~~~text
src/main.py
  NamespaceWatcher
    Kubernetes Namespace watch
    创建/删除生命周期编排
    Redis 状态管理

src/managers/
  aws_manager.py              SQS/SNS 和 STS
  apollo_manager.py           Apollo Cluster/Namespace/Item/Release
  cert_manager.py             cert-manager 和 Kong 证书
  dns_manager.py              Cloudflare wildcard CNAME
  kong_manager.py             Kong 路由、服务、插件和证书
  istio_sidecar_manager.py    Istio Sidecar scope
  zadig_manager.py            Zadig API、workflow、睡眠和清理
  subenv_monitor.py           子环境列表和 Deployment 监控
  redis_state_manager.py      Namespace 状态和资源记录

src/config/settings.py        环境变量配置
src/utils/retry.py            异步重试
src/utils/schedule.py         定时任务
~~~

## 配置

完整配置见 .env.example。常用配置如下：

| 配置 | 说明 | 默认值 |
| --- | --- | --- |
| REFERENCE_ENV | 资源和配置复制的参考环境 | test33 |
| NAMESPACE_PREFIX | Namespace 前缀 | test |
| NAMESPACE_LABEL_KEY | Namespace 管理标签名 | createdBy |
| NAMESPACE_LABEL_VALUE | Namespace 管理标签值 | koderover |
| EXCLUDED_NAMESPACES | 不处理的 Namespace，逗号分隔 | test17,test33 |
| AWS_REGION | AWS 区域 | ap-southeast-1 |
| ENABLE_AWS_RESOURCES | 是否管理 SQS/SNS | true |
| ENABLE_APOLLO_CONFIG | 是否同步 Apollo | true |
| ENABLE_KONG_ROUTES | 是否管理 Kong | true |
| ENABLE_CERT_MANAGEMENT | 是否管理证书 | true |
| ENABLE_DNS_MANAGEMENT | 是否管理 Cloudflare DNS | true |
| ENABLE_ISTIO_SIDECAR_SCOPE | 是否管理 Istio Sidecar | true |
| ENABLE_SUBENV_MONITOR | 是否监听子环境 Deployment | true |
| SUBENV_REFRESH_INTERVAL_SECONDS | Zadig 环境刷新间隔 | 60 |
| WATCH_STREAM_TIMEOUT_SECONDS | Kubernetes watch stream 超时 | 600 |
| SCHEDULER_TZ | 定时任务时区 | CST-8 |

敏感配置应通过 Kubernetes Secret 注入，包括 Zadig token、Apollo OpenAPI Token、Cloudflare token 和 Redis 连接信息。AWS 不使用静态 access key/secret，而使用 IRSA。

## 本地运行

~~~bash
cp .env.example .env
pip install -r requirements.txt
python -m src.main
~~~

本地运行需要可用的 Kubernetes kubeconfig，并设置 IN_CLUSTER=false。如果启用外部资源管理，还需要提供对应的 Zadig、Apollo、Cloudflare、Kong、Redis 和 AWS 访问配置。AWS 本地运行应使用 AWS CLI profile、SSO 或其他默认 credential provider，不要把 access key 写入代码或 .env。

## Kubernetes 部署

~~~bash
docker build -t namespace-watcher:latest .
docker push <registry>/namespace-watcher:<tag>
kubectl apply -f k8s/
~~~

部署前需要确认：

- Deployment 使用正确的镜像版本
- ServiceAccount 绑定了 IRSA Role
- Secret 和 ConfigMap 已更新
- k8s/rbac.yaml 包含 Namespace、Deployment、Secret、Certificate、Sidecar 和 Event 所需权限
- IRSA Role 包含代码实际使用的 SQS/SNS/STS 权限

## 重要日志

创建 Namespace：

~~~text
Processing namespace creation: testN
Created Certificate resource: shub-us-testN-certificate
Created DNS record: *.testN.shub.us
Completed namespace creation processing: testN
~~~

Deployment 级别 Apollo 同步：

~~~text
Sub-env deployment event: ADDED ns=testN deployment=backoffice-v1-web-app
Apollo app config synced for app=backoffice-v2-webapp env=testN ...
Apollo app config synced for app=backoffice-v1-web env=testN ...
~~~

删除 Namespace：

~~~text
Processing namespace deletion: testN
Deleted DNS record: *.testN.shub.us
Deleted Certificate resource: shub-us-testN-certificate
Deleted SQS queue: ...
Deleted SNS topic: notification_testN
Completed namespace deletion processing: testN
~~~

## 故障排查

### AWS 报 AccessDenied

确认 Pod 使用的是 IRSA Role，而不是已回收的 IAM user access key：

~~~bash
kubectl -n <namespace> describe serviceaccount <service-account>
kubectl -n <namespace> exec deploy/<deployment> -- env \
  | grep -E 'AWS_(ROLE_ARN|WEB_IDENTITY_TOKEN_FILE|ACCESS_KEY|SECRET_KEY)'
~~~

重点检查：ServiceAccount annotation、Pod 是否重新创建、IRSA Role trust policy，以及 SQS/SNS IAM policy。

### Apollo 缺少 Namespace

检查以下日志：

- 是否收到对应 Deployment 的 ADDED 事件
- 是否出现 Apollo app config synced
- 是否出现 No Apollo cluster available
- Pod 是否运行包含最新 Apollo 修复的镜像

Namespace 创建本身不会复制所有 Apollo Cluster；必须由实际 Deployment 触发对应 App 的同步。

### Deployment 事件没有触发

确认：

- Zadig API 能返回该环境
- Namespace 名称符合 test\d+ 且不在排除列表
- ENABLE_SUBENV_MONITOR=true
- ServiceAccount 有 apps/deployments 的 get/list/watch 权限
- 日志中是否出现 Sub-env monitor updated
- watch stream 是否频繁出现错误或重启

### 删除后有资源残留

删除操作是异步并行执行的，应根据日志分别检查 DNS、Certificate、AWS、Kong、Apollo、Zadig workflow 和 Sidecar。外部系统 API 失败不会自动保证其他系统回滚，需要根据失败日志重试或执行对应系统的幂等清理。

## 安全边界

- Kubernetes API 使用专用 ServiceAccount 和最小 RBAC
- AWS 使用 IRSA，不在代码、镜像或示例配置中保存静态 access key/secret
- 外部系统 token 和密码通过 Kubernetes Secret 注入
- 删除逻辑仅处理符合规则且由 koderover 创建的测试 Namespace
- 资源名称匹配使用环境 token，避免把 test1 误匹配为 test10 或 test12
- 生产环境和白名单环境不会被定时清理

## 相关文档

- [执行流程](docs/execution-flow.md)
- [Namespace 创建说明](docs/namespace-creation.md)
- [并行处理说明](docs/parallel-processing.md)

## License

MIT
