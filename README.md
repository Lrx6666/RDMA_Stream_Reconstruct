# RDMA/SPDK 流式文件传输与边接收边恢复

本项目实现了一套基于 SPDK NVMe-oF/RDMA 的文件传输与流式恢复流程：发送端将多个文件按照索引顺序归并为一个连续 payload，通过 RDMA 传输到接收端；接收端在 payload 仍然写入时，根据实时写入进度恢复已经完整到达的文件。

项目重点验证以下能力：

- 多文件归并为单一连续 payload，减少逐文件传输带来的额外开销；
- 基于 SPDK AIO bdev 和 NVMe-oF/RDMA 的高速传输；
- 文件级流水线：payload 接收和文件恢复可以重叠执行；
- 在不修改发送端主要传输逻辑的情况下，由接收端监控写入进度并恢复文件；
- 安全处理相对路径、临时文件和原子替换，避免恢复过程中产生半成品文件。

## 系统结构

```text
发送端
  文件目录
      │
      ├─ 扫描并生成 index
      └─ dd731_with_index
              │ 通过 NVMe-oF/RDMA 写入
              ▼
接收端
  SPDK NVMe-oF target
      │
      ├─ AIO bdev：payload_stream.bin
      ├─ monitor_aio_progress.py
      │      └─ 查询 bdev_get_iostat 的 bytes_written
      │             └─ payload.progress
      └─ live_unpack_payload_stream.py
             └─ 恢复已经完整到达的文件
```

## 项目文件

建议仓库至少包含以下内容：

| 文件 | 作用 |
|---|---|
| `dd731_with_index` 或其源码 | 发送端程序，按照 index 将多个文件归并并写入目标块设备 |
| `monitor_aio_progress.py` | 监控接收端 AIO bdev 的累计写入量并发布进度 |
| `live_unpack_payload_stream.py` | 根据 index 和进度文件恢复已完整接收的文件 |
| `simulate_live_payload.py` | 可选，本地模拟渐进写入，用于测试恢复逻辑 |
| `example.index` | 可选，展示 index 文件格式 |
| `LICENSE` | 建议添加，明确代码使用许可 |
| `README.md` | 使用说明和复现实验流程 |

仅上传三个程序通常不够。特别是 `monitor_aio_progress.py` 依赖接收端的 SPDK `rpc.py`，发送程序还依赖具体的 SPDK/RDMA 环境。建议上传发送程序源码、构建方法、配置示例和测试数据生成说明，但不要上传大型 payload、AIO 后端文件、恢复结果目录以及真实实验数据。

## Index 文件格式

Index 是发送端和接收端共享的元数据。示例：

```text
# file_count=3
# total_size=35
file_a.txt|0|10
subdir/file_b.bin|10|20
empty.dat|30|0
```

每个数据行格式为：

```text
相对路径|payload中的起始偏移|文件大小
```

要求：

- 偏移必须从 `0` 开始并连续排列；
- 最后一项的结束位置应等于 `total_size`；
- 路径必须是相对路径，不能包含 `..` 或 Windows 反斜杠；
- 文件恢复按照 payload 中的顺序进行。

## 运行环境

- Linux；
- Python 3.10 或更高版本；
- SPDK NVMe-oF target；
- 已配置好的 RoCE/RDMA 网络；
- 接收端可通过 SPDK RPC 查询 AIO bdev 的写入统计；
- 发送端和接收端能够访问同一份 index 文件，或通过其他方式将 index 提前传到接收端。

本项目的 Python 程序只使用 Python 标准库，不需要额外安装第三方 Python 包。

## 接收端配置概览

下面的命令是示意命令，具体路径、NQN、网络地址和 PCI 地址需要根据实验环境修改。

### 1. 创建 AIO bdev

```bash
python3 /root/spdk/scripts/rpc.py bdev_aio_create \
  /root/spdk/payload_stream.bin Aio0 4096
```

其中 `payload_stream.bin` 是接收端承载 payload 的文件。AIO 后端容量必须不小于 index 中的 `total_size`，并且所在文件系统需要有足够的可用空间。

### 2. 将 Aio0 添加为 NVMe-oF namespace

```bash
python3 /root/spdk/scripts/rpc.py nvmf_subsystem_add_ns \
  nqn.2026-08.io.lrx:pipeline-test Aio0
```

如果 Aio0 已存在但无法打开，先检查当前 bdev、后端文件是否被旧进程占用，以及 subsystem 是否仍引用旧 bdev。

### 3. 启动进度监控

```bash
python3 monitor_aio_progress.py \
  --rpc /root/spdk/scripts/rpc.py \
  --bdev Aio0 \
  --total 10641454 \
  --progress /root/spdk/payload.progress \
  --interval 0.5
```

`--total` 应填写当前 index 的 `total_size`，不能直接照搬其他测试的数据。

### 4. 启动实时恢复

```bash
python3 live_unpack_payload_stream.py \
  /root/spdk/input_files.index \
  /root/spdk/payload_stream.bin \
  /root/spdk/restored_live \
  /root/spdk/payload.progress \
  --overwrite
```

程序会等待 `payload.progress` 出现。当某个文件的 `offset + size` 不超过当前 `written_bytes` 时，该文件会被恢复到输出目录。恢复先写入 `.live-part` 临时文件，完成并刷盘后再原子替换为正式文件。

## 进度文件格式

监控程序会原子地发布如下内容：

```text
written_bytes=6553600
done=0
```

传输完成时：

```text
written_bytes=10641454
done=1
```

`done=1` 只有在写入量达到 index 的 `total_size` 后才应发布。

## 本地模拟测试

在没有 RDMA 环境时，可以使用模拟程序验证实时恢复逻辑：

```bash
python3 simulate_live_payload.py \
  payload_source.bin \
  payload_stream.bin \
  payload.progress \
  --chunk-size 65536 \
  --interval 0.2
```

另开终端运行 `live_unpack_payload_stream.py`。模拟程序会分块复制 payload，并逐步更新进度文件。

## 重要限制

- `live_unpack_payload_stream.py` 只能恢复已经完整写入 payload 的文件；大文件排在 index 前部时，后续小文件可能要等它到达后才会恢复，这是顺序 payload 布局导致的正常现象。
- AIO 后端的容量必须覆盖完整 payload，而不是只覆盖当前已经写入的部分。
- `monitor_aio_progress.py` 使用 `bytes_written` 的初始值作为基线，因此每次测试应确认 AIO 后端和 payload 文件对应当前测试，避免复用旧进度。
- 进度文件、index 和 payload 必须属于同一次测试。
- 当前实现是文件级流水线，不是字节级解包；一个文件只有在其全部数据到达后才会恢复。

## 正确性验证

恢复完成后，至少检查文件数量、相对路径、文件大小和内容哈希：

```bash
find restored_live -type f | wc -l
sha256sum -c expected.sha256
```

建议每次测试保存以下信息：

- index 文件；
- 发送端和接收端命令；
- SPDK 版本及关键配置；
- 文件总数和 payload 总大小；
- 传输完成时间、首个文件恢复时间和全部文件恢复时间；
- 恢复结果的哈希校验结果。

## 不建议提交到 GitHub 的内容

建议在 `.gitignore` 中排除：

```gitignore
*.bin
*.img
*.qcow2
payload.progress
payload.progress.part
*.live-part
restored_*/
__pycache__/
*.pyc
```

如果 `dd731_with_index` 是编译后的二进制文件，最好同时上传源码和构建说明，而不是只上传二进制文件。若发送程序基于 SPDK 修改，建议说明对应的 SPDK commit、修改位置和编译命令。

## 研究用途

本项目适合用于比较以下两种接收策略：

1. payload 全部接收完成后再统一解包；
2. payload 接收过程中，根据实时写入进度恢复已完成文件。

推荐记录 `T_first`（首个文件可用时间）、`T_total`（全部文件恢复完成时间）、恢复吞吐和传输/恢复重叠程度，而不只记录网络吞吐。

## License

本项目暂未指定许可证。公开仓库前，请根据代码来源和 SPDK 的许可证要求补充 `LICENSE` 文件，并明确哪些代码是自行修改、哪些代码来自 SPDK。
