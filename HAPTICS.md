# Haptic relay 与生产链路复测

本次修改基于 `tiny-xie/pico_software` 的 `98e966d845c531472c9a16aaa364e7e09e28b717`，新增 `vr_haptic_relay.py` 并接入 `vr_data_publisher.py`。需要配套安装已修复震动处理的 PrimusLoop Pico App；本仓库不包含 APK。

## 启动

沿用已修复的 VR App，在 publisher 的 Python 环境安装官方 SDK，然后启动：

```bash
python -m pip install xrobotoolkit-sdk==1.1.0 pyzmq PyYAML
python vr_data_publisher.py --config configs/vr_bridge.yaml
```

配置：

```yaml
zmq:
  ipc_addr: "ipc:///tmp/vr_data.ipc"
publish_rate: 80
haptic:
  enabled: true
  cmd_addr: "ipc:///tmp/vr_haptic.ipc"
  max_command_age_ms: 250
  # 默认自动选择唯一在线头显；可选限制为指定设备
  # device_id: "<HEADSET_DEVICE_ID>"
```

RLinf 力反馈无需配置设备 ID。publisher 的 `xr_sdk_client.py` 用标准库 `ctypes` 调用官方 wheel 中的原生库，通过同一 SDK session 的设备事件自动选择唯一在线头显，日志显示 `Haptic target selected: <ID>`。零台或多台在线时记录提示并丢弃震动命令，直到恢复唯一设备；断开后排队请求不会在重连时重放。零台、多台或尚无有效姿态帧时不发布位姿；恢复唯一设备后等待新帧，请只连接一台头显。

旧 `send_pico_haptic/stop_pico_haptic` 显式 ID 命令仍接受，但必须匹配自动选择的设备。启动日志须出现 `Haptic PULL bound ...`。无需维护 XR SDK 分支或安装定制 wheel，也不调用 Python 扩展的 `init()`。`haptic.enabled: false` 可关闭震动转发。

只有 publisher 调用原生 `PXREAInit()`。不要同时运行另一个初始化 SDK 的诊断脚本；验证转发链路时应由现有生产者通过 ZMQ 发送命令，不另起 SDK session；避免多个生产者同时发送震动命令。

## 有界处理

publisher 主线程采集并发布姿态，独立的 `pico-haptic` 线程接收 PULL 消息并调用 SDK；两者共用一个 SDK session。震动线程在自身内部创建、使用和关闭 PULL socket，无额外 Python 命令队列。两条循环均沿用 `publish_rate`（默认 80 Hz）调度，互不等待；慢调用后不补发落后的轮次。震动单轮至多读 64 条/约 2 ms，至多提交两次 SDK 命令；按手柄合并当前唯一设备的请求。`both` 拆成独立的左右手，避免 ZMQ 全局 CONFLATE 丢掉另一只手的状态。目标变化会清空旧请求，并在原有每轮两次调用预算内优先停止旧手柄。

协议保持 RLinf 格式：

```json
{"functionName":"Haptic","value":"{\"hand\":\"right\",\"amplitude\":0.3,\"duration\":100,\"frequency\":150}","timestamp_ns":1234567890000000000}
```

`timestamp_ns` 必须使用当前 `time.time_ns()`；示例数字不是有效实时命令。时间比较适用于同一 PC 的 IPC 生产者；若跨主机使用 TCP，需保证时钟同步。旧于该目标最后接受时间戳的命令被忽略，过期启动变成停止，排队时间会从震动时长扣除。退出会丢弃待发启动并停止已触及的活动手柄；突然断电或传输断开仍依赖 App 的连接处理和原生命令时长到期。

SDK 请求失败只计数，不重放过时力反馈；每 5 秒的汇总包括失败原因、慢调用次数和最大 SDK 耗时。提交成功不是物理 ACK，也不代表设备在线。

SDK 初始化和关闭由 publisher 主线程调用，控制调用仅在震动线程中执行；原生接收线程回调 `xr_sdk_client.py`，只复制最新帧并维护设备集合，Python 在发布时解析一份完整帧，因此左右手、按键和源时间戳来自同一帧。回调返回前复制 C 缓冲区；调用 C API 时不持有缓存锁，`ctypes.CDLL` 会释放 GIL。控制器数据缺失、追踪项明确无效或姿态格式错误时不发布。App 关闭头显追踪时仍可发布控制器数据，头显字段使用原有的七个零缺省值。

官方 PC Service 的 `WatchServerFeedback` 会关闭已有订阅，不能为了查询 ID 再创建一个 SDK session 或 gRPC 订阅。这里继续使用官方原生库的唯一订阅，不解析 stdout，不修改 PC Service 或 App 协议。接口依据 [官方 PXREARobotSDK.h](https://github.com/XR-Robotics/XRoboToolkit-PC-Service/blob/85bac4dbc1fd5cef42c74a160d9c30aa3491f122/RoboticsService/SDK/include/PXREARobotSDK.h)，会话限制见 [官方服务实现](https://github.com/XR-Robotics/XRoboToolkit-PC-Service/blob/85bac4dbc1fd5cef42c74a160d9c30aa3491f122/RoboticsService/PXREAGRPCServer/PXREAGRPCServer.cpp)。

启动会等待震动线程完成 socket 绑定，绑定或线程启动失败会向主线程报告并清理 SDK。运行中震动线程若意外退出，会记录错误并关闭自己的 socket，姿态发布继续，恢复反馈需重启 publisher。正常退出先通知震动线程停止，丢弃后续启动命令并停止活动手柄；等待线程结束后才关闭 SDK 和共享 ZMQ context。

数量/时间预算只约束队列处理，不能中断已进入的原生控制调用。慢控制调用不再要求姿态主循环等待，但若原生调用永久不返回，正常退出也会等待，不能在它仍运行时强行关闭 SDK。线程拆分不能解决下游停止产生新姿态的问题；查看 `max_sdk_ms` 和源时间戳。设备上下线依据 PC Service 的事件，不能当作即时物理断连检测或震动 ACK；服务报告断开或 session 关闭时清除设备与姿态缓存。


## 部署核对

1. 确认官方 PC Service 已连接新 App，publisher 日志出现 `Haptic PULL bound ...` 和 `Haptic target selected: <ID>`。
2. 由 RLinf 或兼容生产者发送上述 ZMQ 协议，确认左右手、停止命令、持续强度变化和重新连接后的行为。
3. 同时观察 publisher 的 `timestamp_ns` 是否持续变化。`publish_rate=80` 表示读取/发布频率，不代表头显每秒产生 80 个新姿态；重复发布旧时间戳不能算作追踪恢复。
4. 对照 publisher 的 `max_sdk_ms` 与 App logcat 的 `maxNativeMs`，分别定位 PC 提交耗时和头显原生调用耗时。SDK 返回成功、提交计数增加都不是物理震动 ACK。

先做无震动对照和单条长震动，再逐档增加命令频率；每档重复测试，并核对震动结束后源时间戳是否恢复。记录 App 构建、设备电量及实际发送参数，不能由单次通过推定稳定的安全频率上限。

本次修改经过 59 项本地回归，覆盖真实 ZMQ、C 编译的 SDK 替身与原生线程回调、缓冲区生命周期、GIL 释放、单 session、设备选择、过期/乱序、初始化失败清理及退出等待。慢原生控制调用期间持续注入新姿态，验证源时间戳仍能更新。本地测试与诊断脚本不随本次功能提交；这些结果不能替代真实 Pico/G2 硬件验收。
