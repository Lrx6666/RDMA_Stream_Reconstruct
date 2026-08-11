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

建议仓库包含以下内容：

| 文件 | 作用 |
|---|---|
| `dd731_with_index` 或其源码 | 发送端程序，按照 index 将多个文件归并并写入目标块设备 |
| `monitor_aio_progress.py` | 监控接收端 AIO bdev 的累计写入量并发布进度 |
| `live_unpack_payload_stream.py` | 根据 index 和进度文件恢复已完整接收的文件 |
| `config_common.json` | 根据本地实验环境可自行修改的配置文件 |
| `LICENSE` | 明确代码使用许可 |
| `实验流程.md` | 使用说明和复现实验流程 |


## 运行环境

- Ubuntu 22.04.5 LTS (GNU/Linux 6.8.0-136-generic x86_64)；
- Python 3；
- SPDK NVMe-oF target；
- 已配置好的 RoCE/RDMA 网络；
- 接收端可通过 SPDK RPC 查询 AIO bdev 的写入统计；
- 发送端和接收端能够访问同一份 index 文件，或通过其他方式将 index 提前传到接收端。

本项目的 Python 程序只使用 Python 标准库，不需要额外安装第三方 Python 包。



## License

本项目中由作者独立编写的代码采用 [MIT License](LICENSE) 发布。

某些组件基于 SPDK 或其他第三方项目修改而来，这些组件仍受其原始许可证和版权声明约束。使用、修改或重新发布相关代码时，请同时遵守对应项目的许可证要求。

本项目不授予任何第三方项目、硬件平台或实验环境的额外使用权，也不对实验结果或特定用途作任何保证。

