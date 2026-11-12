# h2 SETTINGS 生命周期与流控生效点

对象与代码位置（`src/h2/`）：

| 概念 | 状态对象 | 代码位置 |
|---|---|---|
| 本地设置（我通告的） | `H2Connection.local_settings: Settings` | `connection.py:355` |
| 远端设置（对端通告的） | `H2Connection.remote_settings: Settings` | `connection.py:363` |
| 本地设置的值栈 | `Settings._settings[code]: deque[int]` | `settings.py:137` |
| 流级发送窗口 | `H2Stream.outbound_flow_control_window: int` | `stream.py:784` |
| 流级接收窗口 | `H2Stream._inbound_window_manager: WindowManager` | `stream.py:787` |
| 连接级发送窗口 | `H2Connection.outbound_flow_control_window` | `connection.py:367` |
| 连接级接收窗口 | `H2Connection._inbound_flow_control_window_manager` | `connection.py:400` |
| 我方可发帧上限 | `H2Connection.max_outbound_frame_size` | `connection.py:373` |
| 我方收帧上限 | `H2Connection.max_inbound_frame_size` | `connection.py:377` |

## 1. 三种“值”

- **当前值** = `deque[0]`。`Settings.__getitem__` 永远读它；`initial_window_size` 等属性也是读它。
- **待确认值** = `deque[1:]`。每次 `__setitem__` 用 `deque.append` 压栈，**不会**改 `deque[0]`。
- **实际作用于流的值** 需按方向区分：
  - 发送侧：`H2Stream.outbound_flow_control_window`，创建时取 `remote_settings.initial_window_size` 的**当前值**，之后由远端 SETTINGS 的 delta 直接修正。
  - 接收侧：`H2Stream._inbound_window_manager`，创建时取 `local_settings.initial_window_size` 的**当前值**；已存在流在**本地 SETTINGS 被 ACK** 时按 delta 修正。

## 2. 状态变化表

前提：默认 `INITIAL_WINDOW_SIZE = 65535`，流 1 已存在（服务器接收窗口 65535，客户端发送窗口 65535）。

### 2.1 我方连发两次 IWS（100 → 50），夹着建流与 WINDOW_UPDATE

| # | 事件 | `local_settings` 栈 | 当前本地 IWS | 对端 `remote_settings` 栈 / 当前 | 已存在流（对端接收）| 新建流（对端接收）| 连接级窗口 |
|---|---|---|---|---|---|---|---|
| 0 | 基线 | `[65535]` | 65535 | `[65535]` / 65535 | 65535 | — | 65535 |
| 1 | `update_settings(IWS=100)` | `[65535,100]` | 65535 | 不变 | 不变 | — | 不变 |
| 2 | `update_settings(IWS=50)` | `[65535,100,50]` | 65535 | 不变 | 不变 | — | 不变 |
| 3 | 对端收 SETTINGS#1，自动回 ACK | 不变 | 65535 | `[100]` / 100（收即生效）| 65535→100 | — | 不变 |
| 4 | 对端收 SETTINGS#2，自动回 ACK | 不变 | 65535 | `[50]` / 50 | 100→50 | — | 不变 |
| 5 | 我方收 ACK#1 | `[100,50]` | **100** | — | 不变（对端已按 100 调整过）| — | 不变 |
| 6 | 我方在 ACK#1 与 ACK#2 之间建流 3 | `[100,50]` | 100 | — | — | 接收 100 | 不变 |
| 7 | 我方收 ACK#2 | `[50]` | **50** | — | 不变 | — | 不变 |
| 8 | ACK 之后再建流 5 | `[50]` | 50 | — | — | 接收 50 | 不变 |
| 9 | 收到无对应变更的 ACK | `[50]` | 50 | — | 不变 | — | 不变，仍发 `SettingsAcknowledged`（`changed_settings` 为空）|

关键点：对端收到 SETTINGS 后**在同一次 `receive_data()` 内**先 `remote_settings.update()` 再 `_acknowledge_settings()`，所以对端永远看到单一当前值，栈不累积；而我方本地的两次变更各自独立 ACK，按 FIFO 逐层生效——这正对应题面“应用该依据哪个值发数据”：

- **发数据的应用**应依据“对方已确认收到的 IWS”，也就是本端 `remote_settings.initial_window_size` 当前值（值 50），并落到每条流的 `outbound_flow_control_window`。
- 我方自己通告的 IWS 只约束**对端**能发给我多少；在我方收到 ACK 前，它不改动我方任何接收窗口。中途新建的流用 ACK 时点的当前值（上表第 6 行 = 100），不是最终值。

### 2.2 对端两次收缩 IWS（发送方视角，窗口变负）

| # | 事件 | `remote_settings` 栈/当前 | 流 1 `outbound` | 可否发 1 字节 | 空 DATA |
|---|---|---|---|---|---|
| 0 | 基线 | `[65535]`/65535 | 65535 | 是 | 是 |
| 1 | 收 IWS=100 | `[100]`/100 | 100 | 是 | 是 |
| 2 | 收 IWS=50 | `[50]`/50 | 50 | 是 | 是 |
| 3 | 发完 50 字节 | 同 | 0 | 否 | 是 |
| 4 | 收 IWS=0（delta=-50）| `[0]`/0 | **-50** | 否（`FlowControlError`）| **是** |
| 5 | 收 WU(stream)+80 | 同 | 30 | 是 | 是 |

`guard_increment_window` 只挡上界 `> 2^31-1`，负值可存续；`send_data` 的检查是 `frame_size > 0 and frame_size > window`，所以空帧在负窗口下仍可发（用于 END_STREAM）。

### 2.3 接收侧负窗口是非对称的

接收方 `WindowManager.window_consumed(0)` 在窗口已为负时仍会抛 `FlowControlError`（`windows.py:47-50`），于是**即使空 DATA 也会**触发连接级 `GOAWAY(FLOW_CONTROL_ERROR)`。这与发送侧“空帧合法”不对称。

### 2.4 MAX_FRAME_SIZE

| 事件 | `max_outbound_frame_size` | `max_inbound_frame_size` | 已存在流 `max_outbound_frame_size` | FrameBuffer 阈值 |
|---|---|---|---|---|
| 默认 | 16384 | 16384 | 16384 | 16384 |
| 收远端 MFS=32768 | **32768** | 16384 | **32768（被回填）** | 16384 |
| 本地 `update_settings(MFS=32768)`，未 ACK | 32768 | 16384 | 32768 | 16384 |
| 收 ACK | 32768 | **32768** | 32768 | **32768** |

出站检查在 `H2Connection.send_data`（`connection.py:896`），入站检查在 `FrameBuffer._validate_frame_length`（`frame_buffer.py:70`），发生在分派给 `_receive_settings_frame` 之前。

### 2.5 无待确认设置时收到 ACK

`Settings.acknowledge()` 只对 `len(deque) > 1` 的项 pop；空栈返回 `{}`。`_receive_settings_frame` 无论是否真的消耗了待确认值，都会 append `SettingsAcknowledged`。因此多余 ACK 被静默接受，状态机不变，也不发 GOAWAY。

## 3. 发现的实现偏差（最小证据）

1. **多余 ACK 不报错**：RFC 9113 §6.5.3 要求无待确认设置时收到 ACK 视为 `PROTOCOL_ERROR` 连接错误。h2 接受并发出空 `SettingsAcknowledged`。
   - 证据：`connection.py:1772-1777` 无栈空判断；`repro/settings_window_repro.py` 场景 4（两次多余 ACK，状态保持 `CLIENT_OPEN`，无 GOAWAY）。
   - 影响：对本端无害（只是容忍坏对端）；对“严格互操作/合规测试”是偏差。

2. **接收侧负窗口拒绝空 DATA**：`windows.py:47-50` 在 `size=0` 且窗口为负时仍抛错，导致连接被 `GOAWAY` 拆除。
   - 证据：场景 1 尾部；`empty DATA on -100 inbound -> FlowControlError error_code: 3`，队列里是 `GoAwayFrame(FLOW_CONTROL_ERROR, last_stream_id=5)`。
   - 影响：合法实现（如对端只是先把窗口收缩到 0 再发 END_STREAM 空帧）会被误判为流控违规，连接中断。发送侧对应的 `send_data` 已特意放行空帧（`connection.py:893`），两侧语义不一致。

3. **`Settings.acknowledge()` 不校验栈深与 ACK 数量对应**：ACK 与 SETTINGS 帧没有一一绑定，连续收到多个 ACK 会线性消费栈。
   - 证据：场景 5；三次 `acknowledge()` 依次得 `(65535→100)`、`(100→50)`、`{}`。
   - 影响：若对端多发或少发 ACK，本地“当前值”会与对端认知错位（多发 → 提前生效；少发 → 一直停在旧值）。这是设计使然（无帧序号），但值得在应用层知晓。

> 说明：以上均按“不修代码”要求仅记录证据与影响范围。
