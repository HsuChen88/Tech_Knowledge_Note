# ⚡ Apache Kafka

:::spoiler 📚 目錄
[TOC]
:::

:::spoiler 🔗 資料來源
[Kafka Tutorial for Beginners | Everything you need to get started](https://youtu.be/QkdkLdMBuL0?si=wj3AR3_TcDQadPm4)
:::

## ❓ 解決什麼問題（Why）
### Problems
* **Tight Coupling** → 系統間高度依賴，難以獨立擴展或修改
* **Synchronous Execution** → 阻塞式處理，影響系統效能
* **Single Point of Failure** → 單一服務失效導致整體系統癱瘓

### Kafka 的解法：
- **De-coupling** → <span style="color:#f00">**++事件導向架構(Event-Driven Architecture)++**</span> 解耦服務間的依賴
- **Asynchronous** → 非同步傳遞機制提高效能
- **Fault Tolerance** → 多 Broker 與 Partition + Replication 提升容錯能力

## 📮 核心概念與架構（What / How）
Kafka 是分散式的 **事件串流平台（Distributed Event Streaming Platform）**，具備以下特性：

* 🚀 **高吞吐 High throughput** (因為允許 Asynchronous)
* 📈 **可擴展 Scalability** (因為有做 decoupling)
* 💾 **可持久化 Persistence** (因為有做 Replication)
* 🔁 **可容錯 Fault-tolerance** (因為有多 Broker 和有做 Partition + Replication)

> 雖然可用作 Message Queue，但功能遠超過傳統 Message Queue。

 
### Kafka's Role
```
送方 --(Broker)--> 收方
```
* 相當於 **郵局(Post Office)**
* 透過 **Kafka API** 作為傳遞接口
<span style=></span>
### 🧬 基本架構
* **Event**: A <span style="color:#f00">**++key-Value pair++**</span> and meta data
* **Topic**: A <span style="color:#f00">**++queue++**</span> for different service types (like schema of DB)
* **Producer**(送方): A microservice which <span style="color:#f00">**++sends++**</span> Event into the Topic
* **Consumer**(收方): A microservice which subscribes to the topic and <span style="color:#f00">**++receive++**</span> Events from that Topic

![image](https://hackmd.io/_uploads/BJfLBW9rxg.png)
（圖片來源： Youtube - TechWorld with Nana）

### 🔗 Chain of Events

> Event 本身是 immutable，Kafka 也保證 Event 的順序不亂掉，Kafka 還可以重播任何一段紀錄。

```
  (Service)      (Topic)    (Service)   (Topic)        (Service)
Inventory_svc → Inventory → Alert_svc → Restock → Inventory_Restock_svc
                _________               _______
                ....E.E                 ..E.E
                _________               _______
```

### ⏱️ Real-Time Streams Processing
:::info 💡
Use **Kafka Streams** API
> **Kafka Streams** 是一個 Java Library，可以將 Kafka Topic 當成資料來源與輸出，進行即時運算與轉換。

---

Use **[Apache Flink](https://flink.apache.org/)**
> 可以搭配的 Processing 工具

:::

Event Stream 會被持續紀錄，Consumer 不需要 Request 就可以直接收到 
![image](https://hackmd.io/_uploads/SkNBbGcHeg.png)
（圖片來源： Youtube - TechWorld with Nana）

可以對收到的 Event Stream 進行計算，例如用 Kafka Streams 中現成的函式就可以做 Real-Time 分析，包括兩大處理邏輯：
* 🔄 **Stateless Transformations**
    * e.g. map(), filter()
* 🧠 **Stateful Operations**
    * e.g. aggregation, joins

:::spoiler **Stateless Transformations 和 Stateful Operations**

### ✅ 1. **Transformations（轉換）**

這類操作是 **stateless** 的（不需要記住前後狀態），每個 event 是獨立處理的。常用於格式轉換、重新命名欄位、過濾等。

#### 常見 Transformation 操作：

| 操作          | 說明              | 範例                      |
| ----------- | --------------- | ----------------------- |
| `mapValues` | 對每個 value 做轉換   | 把金額從美元轉換成台幣             |
| `map`       | 同時改 key 和 value | 把訂單 ID 當作 key，內容變為 JSON |
| `filter`    | 篩選不符合條件的事件      | 僅處理總金額 > 1000 的訂單       |
| `flatMap`   | 一個事件轉換為多個事件     | 一筆複合訂單拆成多筆商品單           |
| `selectKey` | 重新設定 key        | 根據使用者 ID 重設 key，用於分群    |

#### 📌 例子：

```java
stream.filter((key, order) -> order.getTotal() > 1000)
      .mapValues(order -> convertToTWD(order.getAmount()));
```

---

### ✅ 2. **Stateful Operations（有狀態操作）**

這類操作會「記住上下文」，例如跨時間累加、統計或關聯事件，需要用 Kafka Streams 內建的 **state store** 來維持狀態。

#### 常見 Stateful 操作：

| 操作                         | 說明                        | 範例                       |
| -------------------------- | ------------------------- | ------------------------ |
| `groupByKey()` + `count()` | 根據 key 計數                 | 統計每個用戶下單次數               |
| `aggregate()`              | 聚合運算，可自定邏輯                | 統計每種商品總銷售額               |
| `reduce()`                 | 累加/合併資料（簡化版 aggregate）    | 累加某 key 的值               |
| `join()`                   | 將兩個 stream 或 table 做 join | 訂單 stream 和商品資訊 table 結合 |
| `windowedBy()`             | 在時間區間內做聚合                 | 每 5 分鐘計算每種商品的銷量          |

#### 📌 例子 1：累計統計銷售金額

```java
stream.groupByKey()
      .reduce((v1, v2) -> v1 + v2);
```

#### 📌 例子 2：時間視窗內的聚合（Windowed aggregation）

```java
stream.groupByKey()
      .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
      .count();
```

#### 📌 例子 3：Stream-Table Join

```java
KStream<String, Order> orders = ...;
KTable<String, Product> products = ...;

KStream<String, EnrichedOrder> enriched = orders.join(
    products,
    (order, product) -> new EnrichedOrder(order, product)
);
```

:::

---

> **澄清**：
> 只是在 Stream 從 input 到 output 的過程中，把需要的資訊 process 出來。
![image](https://hackmd.io/_uploads/BJBLNM9Bge.png)
（圖片來源： Youtube - TechWorld with Nana）

:::info

#### Real-Time Streams Processing 案例
* **Uber**: 實時更新司機和乘客的**位置資訊**。
* **電商**: 在++客戶購買++、++商家售出++、++商家補貨++時，後台**同時分析**++存貨情況++。

:::

---

### 👨‍👩‍👦‍👦 Partitions and Consumer Group


:::spoiler **名詞解釋**
#### 名詞解釋
* 🖥️ **Broker**: Kafka Server，用於接收、儲存與傳遞事件
* ⚙️ **Partition**: 每個 Topic 可分割為多個 Partition，允許資料並行處理
* 📍 **Offset**: 指出每個 Event 在 Partition 中的唯一位置，用來追蹤讀取進度
* 👥 **Consumer Group**: 一組 Consumer，Kafka 將 Partition 的資料分派給同一組內的 Consumer 處理
:::

> ⚡ 提供 Scalability & Performance (Parallelism)

可以把 Comsumer 視為一個群體(Consumer Group)，利用多個服務的單位(ex. k8s replicas) 去接收這些大量的 Event。

- 將一個 Topic 切分成多個 Partition，分類去處理大量的 Event。 (如下圖 EU, US, Asia)
- Consumer端的所有 Consumer replica 共同擁有一組 Group ID，Kafka 會將大量 Event 傳給這組 Consumer Group 中的所有單位，並**自動分配負載**。

:::danger

* 每個 Partition **最多只能被一個** Consumer (replica) 實例讀取
* 如果 Consumer 實例數多於 Partition 數，會有 Consumer 處於 idle 狀態

:::

![image](https://hackmd.io/_uploads/BkFdZS5Hgl.png)
（圖片來源： Youtube - TechWorld with Nana）

完整示意圖： 
![image](https://hackmd.io/_uploads/H1kKn8Ijeg.png)
（圖片來源： Youtube - ByteByteGo）

---

### 🗄️ 實體資料存在哪？
Kafka Cluster 中有**多台 Kafka Brokers** (Server)，Kafka Brokers 可以想像成多個 **郵局的分局**，Topic's Partitions 存放在多個分局(Brokers)中，存在一個主要的 Leader Broker 也在其他多個 Brokers 上備份 Replicas，達到 Fault Tolerance。

![image](https://hackmd.io/_uploads/S1q97r5rel.png)
（圖片來源： Youtube - TechWorld with Nana）

> * Kafka 中的資料預設會依據 Topic 設定的 retention policy（例如保留 7 天）儲存，即使被讀取過也不會馬上刪除
> * Partition 的 Replication 預設為 1，但可設定 >1 提升容錯性
> * 只有 Leader Partition 可被讀寫，其他 Follower 負責同步備援

---

## 📍 使用場景 (When / Where)
任何和 Real-time Data Streaming 有關的情境都可以。

1. Log Analysis
2. ML Pipeline
3. System Monitoring & Alerting
4. Capture Data Change (用 Kafka Connect 連到其他工具)
5. System Migration
 

#### 1. Log Analysis
搭配 Logstash → Elasticsearch(s) → Kibana 
[什麼是 ELK ?](https://hackmd.io/@ZhengHsuChen/HJbGDh60Jx)

![image](https://hackmd.io/_uploads/BkpS_IUixl.png)
![image](https://hackmd.io/_uploads/B1XcOI8oge.png)
（圖片來源： Youtube - ByteByteGo）

#### 2. ML Pipeline
搭配 **Apache Flink** or **Apache Spark**

:::spoiler **Apache Flink** 與 **Apache Spark**
### 🔹 Apache Flink

* **定位**：專注於 **分散式資料流處理 (Stream Processing)** 的引擎。
* **特點**：

  * 原生支援 **真正的流處理 (True Streaming)**，事件來時即時處理。
  * 也能支援批處理，但本質是「批處理被視為有限的流」。
  * **低延遲 (Low Latency)**，通常在毫秒級。
  * **事件時間 (Event Time)** 與 **狀態管理 (Stateful Processing)** 功能強大，適合處理亂序事件與需要長時間狀態保存的應用（如 fraud detection、IoT）。
  * 與 **Apache Kafka**、**Pulsar** 等消息系統結合良好。

---

### 🔹 Apache Spark

* **定位**：最初是 **分散式批次資料處理 (Batch Processing)** 框架，後來發展出 **Spark Streaming**。
* **特點**：

  * 批處理性能強大，能處理大型 ETL、數據分析、機器學習（MLlib）、SQL 查詢（Spark SQL）。
  * **Spark Streaming** 採用 **微批次 (Micro-batch)** 模式，並非真正的逐事件流處理。
  * 延遲通常在秒級（比 Flink 高）。
  * 擁有完整的大數據生態系，適合資料科學與分析型場景。
  * 可與 Hadoop、Hive、Presto 等整合。

---

### 🔸 Flink vs Spark 比較

| 面向       | Apache Flink            | Apache Spark            |
| -------- | ----------------------- | ----------------------- |
| **處理模式** | 原生流處理 (Streaming-first) | 批處理為主，流處理為輔             |
| **批處理**  | 將批次視為有限流                | 強項，性能優異                 |
| **流處理**  | 真正的逐事件流 (低延遲)           | 微批次 (延遲較高)              |
| **延遲**   | 毫秒級                     | 秒級                      |
| **生態系**  | 偏向即時應用、IoT、CEP          | 偏向大數據分析、ML、BI           |
| **狀態管理** | 支援大規模狀態 (一致性快照)         | 狀態管理較弱                  |
| **學習曲線** | 偏難，概念偏近即時系統             | 較簡單，批次任務直觀              |
| **適合場景** | IoT、金融風控、即時推薦、線上監控      | 大規模 ETL、數據倉庫、ML/AI、離線報表 |

---

### ✅ **結論**

* 如果你主要是 **批次分析 (Batch Analytics)** → **選 Spark**。
* 如果你需要 **低延遲的流處理 (Real-time Streaming)** → **選 Flink**。
* 在一些企業場景，兩者會 **搭配使用**：Spark 負責離線批處理，Flink 處理即時事件流。

:::

![image](https://hackmd.io/_uploads/rJomYU8oxl.png)
（圖片來源： Youtube - ByteByteGo）


#### 3. System Monitoring & Alerting
搭配 **Apache Flink** or **Apache Spark**
![image](https://hackmd.io/_uploads/H1FOqLLoeg.png)
![image](https://hackmd.io/_uploads/HJMPcL8sgg.png)
（圖片來源： Youtube - ByteByteGo）

#### 4. Capture Data Change
追蹤「目標 DB」的變化，將其同步到其他 DB 。
作法：將 Transaction Log 存入 Kafka Topic 中，DB 作為 Consumer 取得資料變化。
![image](https://hackmd.io/_uploads/rJhEqULoeg.png)
![image](https://hackmd.io/_uploads/Hk9hjUUoxx.png)
（圖片來源： Youtube - ByteByteGo）

使用 Kafka Connect 串到其他工具
![image](https://hackmd.io/_uploads/rJSx388oge.png)
（圖片來源： Youtube - ByteByteGo）

:::spoiler **RDBMS** 與 **Hadoop**

### 🔹 RDBMS 比較

|        面向        |                           MySQL                           |                     Oracle                     |           PostgreSQL           |
|:------------------:|:---------------------------------------------------------:|:----------------------------------------------:|:------------------------------:|
|      **授權**      |                        開源 (GPL)                         |                商業授權 (昂貴)                 |   開源 (PostgreSQL License)    |
|      **定位**      |                    中小型應用、Web系統                    |               企業級、金融、ERP                | 開源中的企業級，分析 & 擴展強  |
| **SQL 標準相容性** |                           中等                            |                      很高                      |              很高              |
|  **擴展性/彈性**   |                       水平擴展有限                        |             分區、分片、RAC (強大)             | 擴展性極佳，可自定義型別與函式 |
|   **NoSQL 功能**   |                       JSON 支援有限                       |               JSON / XML 支援佳                |  原生 JSONB、HStore，支援靈活  |
|      **適合**      | 中小型 Web 應用、LAMP 架構 (Linux + Apache + MySQL + PHP) | 適合金融、電信、ERP 等 高可靠性與高一致性 場景 |      複雜查詢、分析型任務      |
|     **可靠性**     |                    適合 Web & 一般應用                    |                  金融級高可靠                  |        高可靠、科研常用        |
|    **學習難度**    |                            低                             |                       高                       |      中等 (較工程師導向)       |

**簡單理解**：

* 想要 **快速開發、低成本** → MySQL
* 需要 **穩定、安全、企業級功能** → Oracle
* 想要 **開源 + 強大功能 + 擴展性** → PostgreSQL

------

### 🔹 Hadoop

* **性質**：分散式資料處理生態系。
* **核心組件**：

  * **HDFS**（Hadoop Distributed File System） → 大數據存儲。
  * **YARN** → 資源調度。
  * **MapReduce** → 分散式計算模型。
* **特點**：

  * 適合 **大數據 (TB\~PB 級) 批次處理**。
  * 本身不是傳統的資料庫，而是 **分散式檔案系統 + 計算框架**。
  * 常與 Hive, HBase, Spark, Flink 搭配。

---

### 🔸 三者與 Hadoop 的互動關係

Hadoop 本質是 **大數據存儲與處理平台**，而 MySQL / Oracle / PostgreSQL 是 **關聯式資料庫 (OLTP 為主)**。兩者的互動通常是 **數據交換與協同處理**：

> OLTP（聯機事務處理，Online Transaction Processing）是一種處理大量即時交易的資料庫處理類型。

1. **ETL / 數據導入導出**

   * 將 MySQL/Oracle/PostgreSQL 中的 **結構化資料** 抽取 (Extract)，轉換 (Transform)，載入 (Load) 到 Hadoop（HDFS/Hive）做大數據分析。
   * 也可能反過來，把 Hadoop 中處理完的大數據結果，寫回 MySQL/Oracle/PostgreSQL 提供查詢。

2. **數據倉庫整合**

   * Hadoop + Hive/HBase 作為 **大數據倉庫**，與 Oracle / PostgreSQL 整合，供 BI (Business Intelligence) 使用。
   * MySQL 通常扮演前端業務數據庫，將部分資料抽到 Hadoop 做 OLAP。

3. **工具支援**

   * **Sqoop**：常見的工具，用於 **關聯式資料庫 ↔ Hadoop** 之間的數據傳輸。
   * **Kafka + Flink/Spark**：可從 MySQL/Oracle/PostgreSQL 捕捉變更數據 (CDC, Change Data Capture)，再流式導入 Hadoop。

:::

#### 5. System Migration
因為 Kafka 可以將所有 Event 照順序記下來並重播，將 舊系統、新系統 都串到 Kafka 上紀錄 Event Stream，就可以在 Migrate 的過程中確認操作無誤。
![image](https://hackmd.io/_uploads/rkzygK8sex.png)

---


### Kafka 的特性
* 🕹️ **Asynchronous**: 非同步處理傳遞
* 🔗 **Chain of Events**: 一個事件的發生，引發後續的多個事件
* 📊 **Real-Time Analytics**   →  (使用 **Kafka Streams API**)


### 限制或成本（Trade-offs / Pain points）
* **運維成本高**：需要管理多台 Kafka Broker、ZooKeeper/Quorum
* **監控 Broker 硬體使用狀況**（如使用 Prometheus + Grafana）
* 須自訂**資料治理**策略：需要手動設計 Topic retention、資料清理與監控策略
* **整合 CI/CD 自動化部署 Kafka 應用程式**（如 Kafka Streams / Connect）到 Kubernetes 或 VM 


### 實際使用案例
* 🚗 **Uber** (IoT 裝置資料流)
* 🛒 **電商平台** (訂單與交易事件流)
* 📜 異常監控 / 日誌收集系統（如 ELK Stack 前置）


### 專案適合引入 Kafka 的條件
* 系統中有大量 **非同步事件**（ex. 訂單、日誌、IoT、資料管線）
* 想要實作「**解耦合(Decoupling)**」**的事件驅動架構(EDA)**
* 有**即時分析、實時監控**的需求（例如 log aggregation、fraud detection）
* **多個系統間需要共享**事件資訊或交易資料

> 因為**微服務架構天生就適合事件導向架構(EDA)**，所以 Kafka 在微服務架構中很常見，但在傳統架構中一樣可以有效提升擴展性與解耦能力。

---

## ⚔️ 類似工具比較 (vs. Message Queue)

### Kafka
> → **Netflix** (at any time, for any times, at any pace, can replay)

* **Real-time** data processing
* Consumer can read **multiple** times :star2: 
* Keep datas for long period


### RabbitMQ / Active MQ
> → **TV** (**everyone at the same time**, **no replay**)

* Event is deleted after read
* Prioritize **reliability** over ~~speed~~

---

### 🧠 Kafka 分散式事件串流平台 vs 傳統 Message Queue 差異

| 功能     | Kafka                       | 傳統 MQ（如 RabbitMQ） |
| ------ | --------------------------- | ----------------- |
| 儲存     | 可設定保留時間（長期）                 | 預設即時消費、資料不持久      |
| 消費模式   | **Pull-based** + offset control | **Push-based**        |
| 可擴展性   | 分區式架構（高吞吐）                  | 較低，單一隊列瓶頸         |
| 數據處理能力 | 支援即時處理（Streams API）         | 較單純傳遞             |
| 架構角色   | 事件平台（不只是傳訊）                 | 訊息分派系統            |


## 🔄 新舊 Kafka 的差異

- **Before Kafka 3.0**: Kafka Brokers 集中由 ==Apache ZooKeeper== 管理 (非 Kafka 內部套件

====================== **Kafka 3.0** ======================

- **After Kafka 3.0**: ==Kafka Raft (KRaft)==


:::spoiler 🗳️ **Raft Consensus Algorithm**

## 🗳️ Raft Consensus Algorithm for Leader Election

### 簡介

**Raft** 是一種分散式共識演算法，用於在多個節點之間**一致地選出一個 Leader**，進行決策與指令的同步。

---

### 核心概念

Raft 把共識問題分成幾個清楚的子問題：

1. **Leader Election**（選舉）
2. **Log Replication**（日誌複製）
3. **Safety**（保證一致性）

---

### 運作流程：Leader Election

1. 初始所有節點為 **Follower**
2. 若 Follower 一段時間未收到 Leader 的心跳訊號，會轉為 **Candidate** 並發起選舉
3. 每個 Candidate 會廣播請求投票（RequestVote RPC）
4. 其他節點會依據下列條件投票：

   * 任期較新者優先
   * 自己還沒投過票
   * 日誌最新者優先（防止舊節點成為 Leader）
5. 若 Candidate 得到多數票（>50%）則成為 **Leader**
6. Leader 開始發送心跳（AppendEntries RPC）給其他節點

---

### Kafka 與 Raft 的關聯（KRaft 模式）

自 Kafka 2.8+（實驗）、3.3+（穩定）開始，支援 **KRaft 模式**（Kafka + Raft），用來取代 ZooKeeper：

* **Kafka Controller** 不再由 Zookeeper 管理，而是透過內建的 Raft 協議進行 metadata 同步
* **Raft quorum** 通常包含多個 Controller 節點，彼此協商 Leader、同步 Topic/Partition 等資訊
* 降低系統複雜度，也改善一致性與可靠性

---

### 📌 圖解流程（簡化）

```txt
Follower (沒收到心跳) → 成為 Candidate → 發送投票請求 → 選出 Leader → 同步日誌 & 心跳
```
:::


---

## ⚙️ 常用配置或參數說明

| 參數                                                            | 參數內容                  |
|:--------------------------------------------------------------- |:------------------------- |
| <text>broker.id</text>                                          | Kafka Broker 的唯一 ID    |
| num.partitions                                                  | 預設的 Partition 數量     |
| replication.factor                                              | 每個 Partition 的複本數   |
| <text>  retention.ms</text>                                     | 訊息保留時間（毫秒）      |
| log.segment.bytes                                               | 每個 segment 檔案最大大小 |
| zookeeper.connect（舊版本）<br> kafka.metadata.quorum（新版本） | 集群管理配置              |

---

## 🛠️ 如何實際使用 Kafka？

1. 官往下載後簡易測試 ([Kafka Quick Start](https://kafka.apache.org/quickstart))
2. Docker Compose（開發用）
3. Kafka on Kubernetes（生產環境）
4. Kafka as a Service（如 Confluent Cloud）


---

以下是常見步驟：

1. 建立 Kafka Cluster（本地或雲端）
2. 使用 Producer 發送資料
3. 使用 Consumer 接收資料
4. 使用 Kafka Streams 或 ksqlDB 處理資料
5. 使用 Kafka Connect 整合系統
6. 監控與視覺化（可觀察性）

---

### 🏗️ 1. **建立 Kafka Cluster（平台層）**

#### 🧱 Kafka 組成

* **Kafka Broker**：儲存與轉發訊息
* **ZooKeeper**（舊）或 **KRaft**（新）：維護 metadata 與選舉 Leader
* **Topic**：類似資料表，為資料分類的單位
* **Partition**：每個 Topic 可分片，實現平行處理

---

### 📨 2. **使用 Producer 發送事件**

```java
KafkaProducer<String, String> producer = new KafkaProducer<>(props);
ProducerRecord<String, String> record = new ProducerRecord<>("topic-name", "key", "value");
producer.send(record);
```

* **用途**：從應用程式、log 系統、IoT 裝置、資料庫等傳送事件進入 Kafka
* **工具選擇**：

  * 自寫 Producer 程式（Java、Python、Go...）
  * Kafka Connect（例如 JDBC Source Connector）

---

### 📥 3. **使用 Consumer 讀取事件**

```java
KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
consumer.subscribe(List.of("topic-name"));
while (true) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
    for (var record : records) {
        System.out.println(record.value());
    }
}
```

* **用途**：讓下游系統即時處理事件（如驗證、計算、存資料庫）
* **應用場景**：微服務架構中的一個服務負責消費特定 topic

---

### 🔄 4. **使用 Kafka Streams 做事件串流處理**

```java
KStream<String, String> stream = builder.stream("input-topic");
stream.mapValues(value -> process(value))
      .to("output-topic");
```

* **用途**：在資料傳輸途中進行實時處理、過濾、彙總、join
* **Kafka Streams 特點**：在++應用層++內嵌處理邏輯，不需額外部署 cluster
* **替代方案**：Apache Flink、ksqlDB（提供 SQL 查詢 Kafka）

---

### 🔌 5. **使用 Kafka Connect 整合系統**

Kafka Connect 是一種 **無需寫程式即可連接 Kafka 與外部系統的框架**。

| 類型               | 範例                                                |
| ---------------- | ------------------------------------------------- |
| Source Connector | 將資料從 DB、API、MQ 等傳入 Kafka（如 JDBC、MongoDB、Debezium） |
| Sink Connector   | 將 Kafka 資料傳出到外部系統（如 Elasticsearch、MySQL、S3）       |

使用 YAML / REST API 設定 connector，即可部署在 Connect Cluster 上。

---

### 📊 6. **可視化與監控 Kafka**

| 工具                              | 用途                      |
| ------------------------------- | ----------------------- |
| Kafka UI（AKHQ, Conduktor, Kowl） | 查看 topic、offset、message |
| Prometheus + Grafana            | 監控 Broker 指標（lag、吞吐、容量） |
| Burrow、Cruise Control           | Lag 管理、Broker 負載均衡      |

---


## 🎮 Demo / PoC
github 連結

## ❓ 問問自己都學會了嗎?
1. Kafka 可以解決什麼問題?
1. Kafka 的解法(核心概念)是什麼?
1. Kafka(Server) 的角色是什麼?
1. Kafka 的特性是什麼?
1. Kafka 的限制或成本（Trade-offs / Pain points）是什麼?
1. 什麼是 Chain of Events ?
1. 什麼是 Real-Time Streams Processing ?
1. 什麼是 Partitions and Consumer Group ?
1. 實體資料存在哪？
1. 目前有哪些實際使用案例?
1. 專案適合引入 Kafka 的條件?
1. Kafka 分散式事件串流平台 vs 傳統 Message Queue 差異?
