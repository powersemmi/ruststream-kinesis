# Kinesis { #kinesis }

`ruststream-kinesis` 让 RustStream 服务运行在 Amazon Kinesis Data Streams 上。Kinesis 是一个分片
的、带保留期的日志。订阅从流仍然保留的任意位置读取，运行当中也可以重新定位。确认就是分片级的检查
点，一个分片同一时刻由一个实例读取。

在 SDK 之上，这个 crate 提供日志消费方需要的东西：跨分裂和合并的分片发现、带防护的分片租约，以及
按分片的检查点。框架本身的概念（编写订阅者、路由、编解码器、中间件）见
[RustStream 文档](https://powersemmi.github.io/ruststream/)。

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-kinesis = "0.7"
serde = { version = "1", features = ["derive"] }
```

## 能力 { #capabilities }

框架的可选能力 trait，以及这个 crate 对每一项的处理：

| 能力 | 原生支持 | 说明 |
| --- | --- | --- |
| `Subscribe` | 是 | 字符串字面量就能点名流，因此 `#[subscriber("orders")]` 不需要描述符即可挂载。见[订阅](#subscriptions)。 |
| `Seekable` + `Positioned` | 是 | 处理器读出自己这条记录所在的位置，也给订阅重新定位，两者都走投递上下文。见[位置](#positions)。 |
| `Partitioned` | 是 | 投递的记录报出自己的分区键：既是挑中它那个分片的键，也是它据以保持顺序的键。见[发布](#publishing)。 |
| `BatchSubscriber` | 是 | 挂载点点名的批大小成为 `GetRecords` 的上限，因此一次读取最多取回一批的量。见[批](#batches)。 |
| `RequestReply` | 否 | Kinesis 没有回复地址，也没有关联原语；回复就得是第二条流，而那得由这个 crate 自己发明。 |
| `TransactionalPublisher` | 否 | 这项服务没有事务：一次 `PutRecords` 调用里的各个条目，各自成功或各自返回错误。 |
| `OwnedTransactions` | 否 | 同样的原因：没有事务可以拥有。 |
| `DescribeServer` | 是 | 生成的 AsyncAPI 文档在 `kinesis` 协议下写出客户端拨号的主机和端口。见[生成的文档](#the-generated-document)。 |

确认不是能力 trait，所以表里没有它；`ack` 在这里做什么，见[租约与检查点](#leases-and-checkpoints)。

`ruststream_kinesis::prelude` 用一个 glob 带来框架自己的 prelude 和这个 crate 的表面：给订阅重新
定位的 `Seeker`、读出投递记录位置的 `Positioned`，以及投递上下文 `KinesisContext` 和
`KinesisBatchContext`，还有读取它们的键 `Position` 和 `SeekHandle`。记录的分区键通过
`IncomingMessage::partition_key` 读取。

## 生命周期 { #the-lifecycle }

每一次转换都消耗掉它之前的状态，因此每个状态都是各自不同的类型：

```text
KinesisBroker::new()      只有配置，同步，不做 I/O
  .connect()   ->  ConnectedKinesisBroker    活的 SDK 客户端；订阅和发布者
  .shutdown()  ->  ()                        读取任务停止，租约到期失效
```

`new` 不做 I/O：区域和凭证要等运行时连接 Broker 时才解析。`from_config(config)` 接收在别处构建好
的 AWS 配置，而 `endpoint`、`region` 和 `test_credentials` 把 Broker 指向本地的模拟环境。

`shutdown` 消耗掉已连接的 Broker，因此在它之后订阅或发布无法通过编译。更早交出去的发布者比连接存
在得久，它会返回 `KinesisError::NotConnected`，而不是写进服务已经不再消费的流。

## 订阅 { #subscriptions }

`KinesisStream::new(name)` 是订阅描述符：一条流，用名字或 ARN 点名。它直接写在
`#[subscriber(..)]` 装饰器里：

```rust
--8<-- "crates/ruststream-kinesis/examples/kinesis_service.rs:handler"
```

把它挂到 Broker 上：

```rust
--8<-- "crates/ruststream-kinesis/examples/kinesis_service.rs:app"
```

不启用 `macros` feature 的服务，把同一个描述符交给 `subscriber(source, handler)` 构造函数。

描述符还带着消费方的开销，在 Kinesis 上这是一项决定，而不是细节：

| 设置 | 默认值 | 含义 |
| --- | --- | --- |
| `poll_interval(d)` | 1 秒 | 空闲分片上两次读取之间的停顿。这项服务对每个分片允许每秒读五次，停顿越短，这份预算花得越快。 |
| `create_if_missing(shards)` | 关闭 | 流不存在时用这么多分片创建它，然后最多等一分钟，直到它可用。供本地开发和测试使用；生产环境的流按基础设施来管理。 |

这两项在挂载点上也能用，走 prelude 带来的 `KinesisSubscriberExt` trait；[批](#batches)一节的例子
就是这样挂载订阅者的。它们变换描述符，所以需要有一个描述符可变换：`start_at(..)` 会把描述符换成框
架的位置包装器，因此它们要链在它之前。属性里已经点名起始位置的订阅者，改为在描述符上设定这两项，
方法还是同一个类型上的同样两个。

任何 I/O 之前就会拒绝无效的描述符。

订阅会列出流的分片，在分裂和合并改变这个集合时重新列出，为每个分片取一份租约，并为自己持有的每个
分片跑一个读取任务。分裂或合并产生的子分片，只有在父分片消费完之后才启动，正是这一点在重新分片时
保住了按键的顺序。

读取走流的共享吞吐；enhanced fan-out 没有实现。KPL 聚合过的记录会作为错误返回，而不是以不透明的
protobuf 形式送到处理器。

## 批 { #batches }

接收切片的处理器消费一批，批的大小由它的挂载点点名。这是框架唯一往下传给 Broker 的订阅参数。在这
里它成为每个分片读取任务请求时使用的 `GetRecords` 上限，因此一次读取最多取回一批的量，任何一批带
的记录都不会超过它点名的数量：

```rust
--8<-- "crates/ruststream-kinesis/examples/kinesis_batches.rs:batches"
```

分片里只有这么多记录时，一批就少一些；还在填充的批，会在它第一条记录之后 50 毫秒投递出去。

一批里混着这个实例持有的每个分片的记录。每条记录各自对着自己分片的水位线结算，因此逐元素返回结果
的批量函数体，是按记录写检查点的。

大小同时也是一项开销决定：批小意味着读取小，而每个分片每秒仍然只有五次读取。这份预算花在哪里，由
`poll_interval(..)` 决定。这项服务限流掉的一次读取不是投递失败：读取任务等一个间隔，再从停下的地
方读一次，因此处理器看到的是一次停顿，而不是一个错误。

批量函数体读的是 `KinesisBatchContext`，不是 `KinesisContext`，见[位置](#positions)。

## 租约与检查点 { #leases-and-checkpoints }

`ack` 把一条记录标记为已处理。检查点覆盖它之前的每一条记录，因此只有当这个分片上没有更早的记录还
悬着时，分片的水位线才前移，检查点才写进租约存储。

- `HandlerOutcome::ack()` 把记录标记为已处理。
- `HandlerOutcome::retry()` 让它保持未处理。水位线停在那里，因此下次有人取走这个分片的租约时，分
  片会从这条记录重放。分片日志能重新定位，却没法把单独一条记录放回队列。
- `HandlerOutcome::drop()` 把检查点写到这条记录之后，有毒记录就是这样退役的。

投递是至少一次的：一条没有确认的记录把水位线按在原地，从它往后的一切，在重启或租约交接之后会再投
递一次。

检查点存放在 `LeaseStore` 里。`acquire` 为某个所有者取走一个分片，并抢走已经过期的租约，`renew`
给它续期，`checkpoint` 在所有者仍然持有租约期间记录进度，`read` 返回持久化的状态，`release` 不等
到期就把分片交还回去。续期失败意味着另一个所有者已经取走了这个分片，那个读取任务立刻停止。完整消
费完的分片以 `SHARD_END` 写检查点，这就是它的子分片可以启动的信号。

默认的存储是 `MemoryLeaseStore`：在进程内，对单个服务实例是正确的，重启之后又是空的。

### 延迟后重试 { #retrying-after-a-delay }

Kinesis 没有重新投递定时器，因此返回 `HandlerOutcome::retry_after(delay)` 的处理器由框架来照应：
延迟走完之后，框架把这条记录再发布一次，并把重试次数放进一个消息头。它用的那个发布者，在 Broker
作用域上接好：

```rust
--8<-- "crates/ruststream-kinesis/examples/kinesis_retry.rs:retry"
```

处理器请求这次停顿，并读出自己已经请求过多少次：

```rust
--8<-- "crates/ruststream-kinesis/examples/kinesis_retry.rs:handler"
```

副本进入订阅所读的那条流，这条流由描述符报出。没有 `retry_via` 时，延迟退化成从这条记录起对分片
的一次立即重放。

副本落在流的尾部，而不是这条记录原来占的位置，落在哪个分片由它的分区键挑定。因此延迟的记录会丢掉
它相对于原先同处一批发布的那些记录的顺序。这个顺序要紧的地方，改用 `HandlerOutcome::retry()` 按住
分片：水位线停在这条记录上，分片从它重放。

### 在多个实例间共享分片 { #sharing-shards-between-instances }

`dynamodb-lease` feature 加入 `DynamoLeaseStore`，一个服务的多个实例于是共享这些分片。表只需要一
个名为 `lease_key` 的字符串分区键，别的都不要；按需计费就够了。每次写入都是条件写入，并递增一个防
护计数器，因此两个实例不可能同时持有同一个分片。

```rust
--8<-- "crates/ruststream-kinesis/examples/kinesis_leases.rs:leases"
```

`owner_id` 在表里标明这个实例；不写它时，取一个进程唯一的值。

## 位置 { #positions }

订阅从哪里读，全部词汇就是 `KinesisPosition`。默认情况下每个分片从自己存下的检查点续读，没有检查
点的分片在尾部打开。

| 位置 | 范围 | 含义 |
| --- | --- | --- |
| `KinesisPosition::horizon()` | 整条流 | 裁剪视界：流仍然保留的一切。 |
| `KinesisPosition::latest()` | 整条流 | 尾部：只有重新定位之后发布的记录。 |
| `KinesisPosition::timestamp(millis)` | 整条流 | 每个分片在自己从那一刻起的第一条记录上打开，单位是 Unix 纪元以来的毫秒。 |
| `KinesisPosition::sequence(shard, seq)` | 单个分片 | 恰好一条记录。 |

整条流范围的位置也作用到后来才发现的分片，其中包括分裂产生的子分片，因此流重新分片时，一次定位仍
然保持它的含义。分片范围的那种形式，是框架从投递记录上取下的固定位置（`Positioned::position`）：
定位到它会把那条记录重新投递一次，不移动任何别的分片。它需要这个实例里有该分片的活读取任务；对于
这个实例并不持有、或者已经读完的分片，它返回错误。

装饰器上的 `start_at(..)` 让订阅在你点名的位置打开，而不是在存下的检查点上。运行中的订阅从处理器
里重新定位：这个 Broker 往投递上下文里放入记录的位置和订阅的定位句柄，`Ctx<SeekHandle>` 把这个句
柄绑定为处理器的一个参数：

```rust
--8<-- "crates/ruststream-kinesis/examples/kinesis_seek.rs:seek"
```

`Ctx<Position>` 以同样的方式绑定记录自己的固定位置。两者都要的处理器，把 `KinesisContext` 写成自
己的上下文类型，用 `ctx.context(..)` 读这些键。

批量处理器拿到的是 `KinesisBatchContext`。里面有同一个 `SeekHandle`，因为一次定位移动的是整条订
阅；里面没有位置，因为一批跨越很多条记录。要对位置作出反应的函数体，读它各个元素的
`kinesis-sequence-number` 和 `kinesis-shard-id` 消息头。

一次重新定位会丢掉它移动的每个分片的水位线记账，因此定位之前投递的记录，它的确认没法把游标拉回到
刚刚取到的位置之前。从新位置往后的记录会再投递一次，这是至少一次所允许的。这项能力本身在框架文档
里是[定位](https://powersemmi.github.io/ruststream/latest/guides/subscribers/#seeking)。

## 发布 { #publishing }

`KinesisPublish` 是构造 `KinesisPublisher` 的策略，运行时在启动时于已连接的 Broker 上实例化一个。
它同时是这个 Broker 的默认策略，因此挂载时没有指定策略的回复处理器，就通过它回复。挂载点上的
`.out(Reply, Publish::default())` 明确点名它；标记换成注入槽位自己的标记而不是 `Reply` 时，同一个
调用绑定的是那个槽位的发布者。

处理器函数体只导入框架（`use ruststream::prelude::*`），用一项能力 trait 约束注入进来的发布者，因
此它永远不知道自己跑在哪个 Broker 上。挂载这些处理器的文件导入
`use ruststream_kinesis::prelude::*`，这个 Broker 的策略在那里用统一的挂载点名字 `Publish`；
`KinesisPublish` 留在 crate 根上，供同时挂载两个 Broker 的文件使用。

发布构建器是框架自己的：`message(..)` 带上声明好的类型，然后是 `to(..)` 带上流名或 ARN、
`with_headers(..)`、`with_codec(..)` 和 `publish()`。服务手上已经编码好的载荷，声明成
`#[derive(Outgoing, Serialized)]` 的 newtype：编解码器不在它上面运行，生成的文档照样写出这条消
息。见[发布指南](https://powersemmi.github.io/ruststream/latest/guides/publishing/)。

### 分区键 { #the-partition-key }

分区键挑出记录落在哪个分片上，随之也定下这条记录相对于邻居保持的顺序。它属于某一条记录而不是属于
发布者，所以它是发布上的一个步骤 `partition_key(..)`，站在框架自己那些步骤中间：

```rust
--8<-- "crates/ruststream-kinesis/examples/kinesis_seek.rs:publish"
```

这个步骤是构建器上的一个位置，不是一个包在另一个发布者外面的发布者，因此记录仍然从挂载点点名的那
个入口出去：那个入口挑的编解码器仍然为它编码，它上面的各项变换仍然运行，测试也仍然把这次发布归到
那个入口的槽位上。

回复没有调用点可以挂步骤：回复处理器返回一个值，由运行时把它发布出去。回复的键改在策略上点名，在
挂载点，走 prelude 带来的 `KinesisPublishSettings` trait：

```rust
--8<-- "crates/ruststream-kinesis/examples/kinesis_replies.rs:replies"
```

一个键就是一个分片，随之也就是那一个分片的吞吐。记录之间必须保持顺序时就点名键，其余情况不点名：
哪里都没有键时，一个进程唯一的键会把记录摊到各个分片上。

有四个地方可以点名这个键，它们按一个顺序解析：

1. 这次发布的 `partition_key(..)` 步骤。
2. 调用手写的 `partition-key` 消息头。这是可移植的写法，也是记录从本框架另一个 Broker 过来时所带
   的写法。
3. 挂载点在策略上点名的键。
4. 一个进程唯一的键，它把记录摊开：Kinesis 记录没有键就发不出去。

这个键就是通过消息头传递的：发布者把解析出的键写进记录自己的分区键字段，投递的记录又在这个消息头
里把它报回来，`IncomingMessage::partition_key` 和框架的按键分发都在那里找它。投递里还有
`kinesis-sequence-number` 和 `kinesis-shard-id`（`SEQUENCE_HEADER` 和 `SHARD_HEADER`）。

这个步骤是一项逐消息设置，而逐消息设置是处理器函数体唯一会说出 Broker 名字的东西。点名这个键的函
数体，导入这个 crate 的 prelude，并把自己的槽位约束在选项类型上：

```rust
--8<-- "crates/ruststream-kinesis/tests/harness_kinesis.rs:keyed_handler"
```

这个签名说明函数体是为 Kinesis 写的。不点名键的函数体保留框架的 prelude 和普通的
`Out<impl Publisher, _>` 约束，原封不动地在各个 Broker 之间搬动。

## 生成的文档 { #the-generated-document }

`DescribeServer` 把 Broker 放进生成的 AsyncAPI 文档的 `servers` 段，放在 `kinesis` 协议下。它报出
的是一个坐标：客户端拨号用的主机和端口。

- 有 `endpoint(..)` 时，是那个 URL 的主机和端口。协议方案以及用户名和密码都丢掉：文档生成出来就是
  要发布的，凭证绝不能跟着它走。
- 否则，`region(..)` 点过名时是 `kinesis.<region>.amazonaws.com`。
- 否则是 `kinesis.amazonaws.com`，因为从环境里解析区域的 Broker，在构建文档的时刻还没有解析出来。

## 消息头信封 { #the-header-envelope }

一条 Kinesis 记录里只有一个数据块和一个分区键，因此分区键之外的用户消息头，写进载荷外面一个小小的
信封里，而且只有存在这样的消息头时才写：

- 没有用户消息头就发布出去的记录，就是纯载荷，任何 Kinesis 消费方都读得了；别的生产者写下的记录，
  读回来就是没有消息头的。
- 否则这个数据块是：四字节的魔数 `RSK1`、大端序的 `u32` 消息头块长度、消息头块，以及载荷。

分区键留在信封外面：记录为它准备了自己的字段。

## 在 LocalStack 上运行 { #running-against-localstack }

[LocalStack](https://localstack.cloud/) 在开发机上模拟 Kinesis。`just brokers-up` 启动
`docker-compose.test.yml` 定义的容器，`just brokers-down` 把它移除。三项设置把 Broker 指向它：

```text
KinesisBroker::new()
    .endpoint("http://localhost:4566")
    .test_credentials()
    .region("us-east-1")
```

compose 文件钉住最后一个不需要令牌的镜像，因为更新的 LocalStack 镜像要求一个鉴权令牌，而来自 fork
的 pull request 读不到 secrets 里的它。文件里还设了 `KINESIS_LATENCY=0`；否则这个模拟器给每次调用
加上 500 毫秒。

## 测试 { #testing }

`testing` feature 提供 `KinesisTestBroker`：一个进程内传输，没有服务器，也没有网络。它和真实的
Broker 有同样的状态，`TestApp` 测试套件在它上面运行。这两个名字都不在 prelude 里，因此测试导入
`ruststream_kinesis::testing::KinesisTestBroker`，并在旁边导入 `ruststream::testing::TestApp`。见
[用 `TestApp` 对服务做单元测试](https://powersemmi.github.io/ruststream/latest/guides/testing/#unit-testing-a-service-with-testapp)。

这个传输给每条流保留一份日志，因为这正是处理器在没有服务器时能观察到的性质。订阅在尾部打开，没有
检查点的分片也是在那里打开；`start_at(..)` 或者处理器的 `SeekHandle` 会真的从它点名的位置重读日
志。投递带着完整的表面：一个 `KinesisPosition`、`kinesis-sequence-number` 和 `kinesis-shard-id`
消息头、分区键，以及读取这些键用的 `KinesisContext` 或 `KinesisBatchContext`。所以服务原封不动就
挂得上：

```rust
--8<-- "crates/ruststream-kinesis/tests/harness_kinesis.rs:seek_handler"
```

```rust
--8<-- "crates/ruststream-kinesis/tests/harness_kinesis.rs:seek_test"
```

两边的挂载都是服务自己的那一份。`KinesisStream` 在这里也打开订阅，带着它在生产环境里带的那些设
置。`poll_interval` 给这个传输从不发起的读取定价，`create_if_missing` 没有流可创建，因此两项都接
受下来并忽略。服务会拒绝的描述符（流名为空）在这里挂载同样失败。`Publish` 构造出这个进程内传输的
发布者，它按服务用的同样四步顺序解析记录的分区键，因此带键的发布在这里读回来的样子，和它将来在线
上读回来的一样。它同时也是已连接传输的默认策略，因此没有自己发布者的回复就从它走。这里没有只供测
试用的描述符，也没有只供测试用的策略。

逐消息设置记录在这次发布离开的那个槽位上，因此测试套件直接对它做断言：点名了键的调用用
`tb.out::<Journal>().with_options(&KinesisPublishOptions { partition_key: Some(..) })`，没有动策略
自身设置的调用用 `assert_options_default()`。

框架的契约套件在这个进程内传输上运行：路由（`conformance::harness::run_suite`）、生命周期的那串转换
（`harness::lifecycle`），以及 `Seekable` 和批这两项能力（`conformance::capabilities::seeking` 和
`batches`）。生命周期套件一直查到 `shutdown` 之前创建的发布者：之后它必须返回错误，而不是写进已经
关闭的传输。批由框架客户端一侧的适配器分组，因此批量挂载在这里和对着服务时是同一个挂载。同样这些
套件在 `KINESIS_TEST_ENDPOINT` 下对着 LocalStack 运行。

它没有的，是服务器才拥有的那些：它只路由一个分片（`testing::IN_PROCESS_SHARD`），所以这里没有租
约、没有检查点的持久性、没有保留期上限、没有重新分片，也没有重新投递的时序。这些由实况测试覆盖，
以 `KINESIS_TEST_ENDPOINT` 为开关：

```text
just test-brokers
```

这条命令启动 LocalStack，并对着它运行这个 crate 自己的检查，单线程运行，这样几轮之间不会看到彼此
的流。
