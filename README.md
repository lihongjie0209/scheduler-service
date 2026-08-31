# scheduler-service

平台集中定时任务服务。它管理 Cron 任务并通过动态 gRPC 调用内部服务，不生成或编译任何下游业务 Client Stub。

成功和失败的执行历史默认保留 90 天，由服务按 500 条有界批次删除；生产环境可以通过环境变量覆盖保留期，并在删除前用数据导出或 CDC 归档到对象存储/分析库。当前执行 ID 保持全局唯一，因此暂不直接按时间分区；达到实测容量阈值后，先演进为包含时间桶的身份约束，再使用 PostgreSQL 原生分区和可选 `pg_partman` 自动维护。

## 为什么下游接口变更不要求调度服务发版

任务只保存以下调用信息：

- 配置白名单中的上游名称，例如 `audit`
- 完整 RPC 方法名，例如 `/grpc.health.v1.Health/Check`
- 符合 Protobuf JSON mapping 的请求 JSON
- Cron、时区和调用超时

运行时通过 gRPC Server Reflection 获取目标服务的描述符，使用成熟的 `grpcurl`/`protoreflect` 组件将 JSON 转成动态 Protobuf 消息并调用一元 RPC。下游新增兼容字段或新增 RPC 后，只需创建或更新任务；破坏性契约变更仍由 `platform-protos` 的 Buf 门禁阻止。

scheduler-service 自己的管理 API 正常使用版本化的 `platform.scheduler.v1` 生成代码，因为这是本服务拥有的稳定契约，而不是下游业务依赖。

## 安全边界

- 数据库中不能保存任意目标地址、JWT、PSK 或 TLS 私钥，只能引用 `outbound.grpc.services` 的命名白名单。
- 生产环境的下游内部端口应启用 mTLS 或 PSK，并用 Kubernetes NetworkPolicy 仅允许 scheduler-service 访问。
- 下游需开放 Server Reflection；反射与业务调用走同一认证连接，流式拦截器也会传递认证元数据。
- 当前仅允许 unary RPC，拒绝客户端流、服务端流和双向流接口。
- 每个任务通过 Redis Redsync 资源锁防止多个副本重复执行，锁粒度为具体 job ID。

## 本地运行

平台开发环境由总仓库启动：

```bash
cd ../..
make dev-up
```

独立运行时先准备 PostgreSQL 与 Redis：

```bash
cp config/config.yaml config/config-local.yaml
APP_ENV=local go run ./cmd/api
```

默认 HTTP 端口为 `8080`，gRPC 管理端口为 `9090`。所有业务 HTTP 接口统一使用 POST + JSON，Swagger 位于 `/swagger/index.html`。

## 任务示例

```json
{
  "name": "audit-health",
  "cron_expression": "0 */5 * * * *",
  "timezone": "Asia/Shanghai",
  "upstream": "audit",
  "full_method": "/grpc.health.v1.Health/Check",
  "request_json": "{\"service\":\"\"}",
  "timeout_milliseconds": 5000,
  "enabled": true
}
```

管理接口：

- HTTP：`/api/v1/scheduler/jobs/{create,update,delete,get,list,trigger}` 与 `/api/v1/scheduler/executions/{get,list}`
- gRPC：`platform.scheduler.v1.SchedulerService`

更新与删除必须携带 `version`，Repository 使用乐观锁原子递增版本。任务和执行记录均包含平台统一审计字段。

## 验证

```bash
make test
make test-integration
make swagger-check
make proto-check
```

集成测试使用 Testcontainers 启动 PostgreSQL、MySQL 和 Redis，并使用进程内 gRPC Reflection 服务验证动态调用；不依赖任何其他微服务运行。服务启动时会自动执行独立 schema 和独立迁移记录表中的 pending migrations。
