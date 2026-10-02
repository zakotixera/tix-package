# Tix 工具包

用于 Python 的结构化日志、进程内事件分发和协作式关闭。

[English](README.md) | 简体中文

Tix 包含三个相互独立的 Python 包，要求 Python 3.11 或更高版本，均无运行时依赖。
各包可单独使用，也可通过应用程序中的回调函数组合使用。配置仅作用于各自的实例；
资源管理、异步执行和进程信号处理由应用程序负责。

| 包 | 导入名 | 主要功能 |
| --- | --- | --- |
| [tix-log](tix-log/README.md) | `log` | 结构化字段、请求上下文、JSON 输出、标准日志处理器和可选的有界队列投递 |
| [tix-evt](tix-evt/README.md) | `evt` | 有序的同步与异步分发、订阅过滤、一次性监听器和有界异步事件流 |
| [tix-sig](tix-sig/README.md) | `sig` | 取消令牌、支持依赖关系的分阶段清理、协作式超时和可选的操作系统信号处理 |

## 目录

- [安装](#安装)
- [基本用法](#基本用法)
- [执行与资源管理](#执行与资源管理)
- [示例](#示例)
- [开发](#开发)
- [文档](#文档)
- [许可证](#许可证)

## 安装

在仓库根目录按需安装：

```sh
python -m pip install ./tix-log
python -m pip install ./tix-evt
python -m pip install ./tix-sig
```

每条命令安装一个独立的发行包，不会安装另外两个包。发行包名带有 `tix-` 前缀，
Python 导入名分别为 `log`、`evt` 和 `sig`。也可通过
`python -m pip install path/to/package.whl` 安装已构建的 wheel 文件。

以上命令使用本地源码安装。尚未确认这些包名在 PyPI 上是否可用，也未预留包名。
开发环境的配置方法见[开发](#开发)。

## 基本用法

安装对应的包后，以下示例均可独立运行。

### 结构化日志

为日志记录器绑定字段，添加请求上下文，并对敏感字段脱敏，以 JSON 格式写入标准错误流：

```python
from log import BoundLogger, JsonFormatter, LogManager, SinkHandler, StandardStreamSink, redact

handler: SinkHandler = SinkHandler(
    StandardStreamSink(),
    formatter=JsonFormatter(),
    close_sink=True,
)
manager: LogManager
with LogManager(
    "service",
    handlers=[handler],
    processors=[redact(["password", "token"])],
    close_handlers=True,
) as manager:
    logger: BoundLogger = manager.get_logger(component="api")
    with manager.context(request_id="req-1"):
        logger.info("request completed", status=200, password="secret")
```

输出的 JSON 记录包含 `component`、`request_id` 和 `status` 字段，`password` 的值已脱敏。
字段同名时，调用时传入的值优先于上下文中的值，上下文中的值优先于绑定的值。
脱敏仅作用于结构化字段，不处理消息文本或异常堆栈。退出管理器的 `with` 代码块时，
管理器会刷新并关闭其负责的处理器，标准错误流保持打开。

文件输出、按需生成日志消息、`TRACE` 级别、日志限流和后台队列投递的用法，
见[日志指南](tix-log/README.md)。

### 有序事件

订阅主题并查看回调的返回值。退出 `with` 代码块时，订阅自动取消：

```python
from evt import DispatchResult, Event, EventBus


def confirm_order(
    event: Event,
) -> str:
    """
    返回订单确认消息。

    Args:
        event: 以订单 ID 为载荷的订单事件。
    """
    return f"Order {event.payload} accepted"


bus: EventBus = EventBus()
with bus.subscribe("order.created", confirm_order, priority=10):
    result: DispatchResult = bus.emit("order.created", 42)
    assert result.outcomes[0].value == "Order 42 accepted"

assert bus.listener_count == 0
```

回调按优先级从高到低执行，优先级相同时按注册顺序执行。默认情况下，所有符合条件的回调
执行完毕后，其抛出的普通异常会汇总为 `ExceptionGroup` 抛出。使用
`EventBus(errors="collect")` 可在返回结果中查看异常。

异步回调通过 `await bus.emit_async(...)` 执行。`concurrency=N` 限制每次分发的并发任务数，
返回结果仍按订阅顺序排列。同步回调在当前线程中直接执行。通配订阅、过滤器、事件流、
等待单个事件和跨线程提交事件的用法，见[事件指南](tix-evt/README.md)。

### 协作式关闭

注册清理回调，发出取消请求，再执行关闭流程：

```python
from sig import Phase, ShutdownContext, ShutdownManager, ShutdownReport


def close_resources(
    context: ShutdownContext,
) -> None:
    """
    输出资源关闭信息。

    Args:
        context: 包含关闭原因、取消令牌和剩余可用时间的上下文。
    """
    print(f"Closing resources: {context.reason}")


manager: ShutdownManager = ShutdownManager(timeout=5)
manager.register("resources", close_resources, phase=Phase.CLOSE)
manager.request("work complete")
assert manager.token.cancelled

report: ShutdownReport = manager.shutdown()
report.raise_errors()
```

清理按 `STOP`、`DRAIN`、`CLOSE` 的阶段顺序执行。同一阶段内，先满足依赖关系，
再按优先级从高到低执行；优先级相同时按注册顺序的逆序执行。清理回调抛出的普通异常会被记录。
在总时限内，后续回调继续执行。`request()` 仅发出取消通知，不执行清理。

异步清理使用 `await manager.shutdown_async()`。如需处理操作系统信号，在主线程中使用
`with manager.signals():` 安装信号处理器，等待关闭请求后执行清理。父子取消令牌、依赖校验、
超时、等待关闭完成和信号处理器管理的用法，见[关闭指南](tix-sig/README.md)。

## 执行与资源管理

各包的执行方式和资源管理规则如下：

| 项目 | 行为说明 |
| --- | --- |
| 导入行为 | 导入时不配置根日志记录器，也不安装信号处理器。创建可选的 `QueueHandler` 时才启动日志工作线程。 |
| 日志资源 | 管理器默认不负责关闭处理器。设置 `close_handlers=True` 后，由管理器负责关闭；调用方自行管理的队列需自行关闭。管理器关闭时，先等待已接受的日志调用完成，再刷新或关闭处理器。 |
| 事件投递 | 分发使用订阅快照。载荷由调用方管理；事件元数据经浅拷贝后设为只读。不提供持久化、重放或消息代理传输。 |
| 缓冲与溢出 | 日志队列和事件流的缓冲区容量有限，可配置溢出策略。事件流限定在所属的事件循环中使用，仅支持一个消费者。 |
| 异步执行 | 事件循环由调用方管理，异步分发和清理由调用方等待完成。阻塞的同步回调可通过 `asyncio.to_thread` 等方式在其他线程中执行。 |
| 关闭超时 | 超时采用协作式机制，无法中断同步回调，异步回调需响应取消。重复请求会设置 `escalated` 标志，不会强制退出进程。 |
| 进程边界 | 管理器、令牌、事件总线和输出目标仅限在创建它们的进程中使用。子进程需新建实例；跨进程共享任务或传递取消通知需使用进程间通信（IPC）。 |

线程使用规则、超时行为、进程派生（fork）和多进程日志汇集的说明，见[并发约定](docs/concurrency.md)。

## 示例

[配置开发环境](#开发)后，可运行以下示例：

| 示例 | 运行命令 | 内容 |
| --- | --- | --- |
| [服务生命周期](examples/service.py) | `just example` | 组合使用异步事件、带上下文的 JSON 日志和分阶段关闭 |
| [多进程日志](examples/process_logging.py) | `just example-process` | 以 spawn 方式启动子进程，各自创建管理器，通过标准库队列将日志记录发送至父进程的监听器 |

两个示例均在执行完成后退出。服务示例通过应用程序中的回调组合三个包，包之间仍相互独立。

## 开发

安装 [uv](https://docs.astral.sh/uv/getting-started/installation/) 和
[just](https://just.systems/man/en/packages.html) 1.55 或更高版本，然后运行：

```sh
just setup
just check
```

`just setup` 按锁定文件安装工作区中的包和开发依赖。Windows 下的任务使用 PowerShell 7（`pwsh`）；
Linux 和 macOS 使用默认的 POSIX shell。在仓库根目录及其子目录中均可运行任务。
执行 `just` 可列出可用命令。

| 命令 | 用途 |
| --- | --- |
| `just fmt` | 执行 Ruff 的安全自动修复，并格式化 Python 文件 |
| `just check` | 运行静态检查、格式检查、严格类型检查和全部测试 |
| `just test-package evt` | 测试单个包，可选 `log`、`evt` 或 `sig` |
| `just test-python` | 在隔离环境中使用 Python 3.11 测试 |
| `just test-python 3.14` | 使用指定的解释器版本测试 |
| `just build` | 为所有包构建源码发行包和 wheel 文件 |
| `just wheels` | 构建 wheel 文件，并检查各包能否独立安装 |
| `just verify` | 运行代码检查和测试，构建发行包，并检查各包能否独立安装 |
| `just bench 20000` | 运行本地微基准测试 |
| `just bench-io 20000` | 比较文件写入的刷新策略 |

`just test`、`just lint` 和 `just types` 分别用于运行测试、静态与格式检查、类型检查。
也可在仓库根目录直接使用 uv：

```sh
uv sync --locked
uv run --locked pytest -q
uv run --locked ruff check .
uv run --locked ruff format --check .
uv run --locked mypy
uv build --all-packages
uv run --locked python tools/check_wheels.py
uv run --locked python examples/service.py
uv run --locked python examples/process_logging.py
uv run --locked python benchmarks/bench.py --iterations 20000
uv run --locked python benchmarks/io.py --iterations 20000
```

持续集成（CI）配置覆盖 Linux 和 Windows，以及 Python 3.11–3.14。
本地已执行的检查和平台限制见[验证记录](docs/validation.md)。基准数据来自本地测量，
持续集成不以特定机器上的运行耗时作为通过条件。

## 文档

以下文档目前为英文：

| 指南 | 内容 |
| --- | --- |
| [日志](tix-log/README.md) | 字段、上下文、处理函数、格式化器、输出目标、队列投递与资源管理 |
| [事件](tix-evt/README.md) | 订阅、执行顺序、错误处理策略、异步分发、事件流与跨线程调用 |
| [关闭](tix-sig/README.md) | 令牌、清理阶段与依赖、超时、报告和操作系统信号 |
| [架构](docs/architecture.md) | 设计决策、执行约定与功能范围 |
| [并发](docs/concurrency.md) | 线程与进程约定、生命周期同步，以及协作式关闭的限制 |
| [验证](docs/validation.md) | 验证结果、测试覆盖、平台注意事项与基准测试方法 |
| [性能](docs/performance.md) | 性能测量、I/O 方案比较与后续工作安排 |
| [事件性能](docs/event-performance.md) | 事件分发与订阅优化，以及优化前后的测量结果 |
| [参考调研](docs/research.md) | Python 基础机制及相关的 Python、Rust 和 Go API |

## 许可证

[Apache License 2.0](LICENSE)。
