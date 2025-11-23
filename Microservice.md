# 🛒 Microservice (微服務架構)

:::spoiler 📚 目錄
[TOC]
:::

## 1. 什麼是微服務？

微服務（Microservices）是一種軟體架構風格，它 **將應用程式拆分成多個獨立的、小型的、專注於單一職責的服務**，每個服務都可以獨立開發、部署和運行。這些服務通常透過 **API（通常是 REST 或 gRPC）** 進行通信。

> - **API**: Application Programming Interface
> 
> - **REST**: REpresentational State Transfer (表現層狀態轉移)
> 
> - **gRPC**: google Remote Procedure Call
> 

:::spoiler What is RPC ?

## 一、 RPC 的定義

「Remote Procedure Call」這個名字的本質意涵是：

- **讓「分散式系統的通訊」像「呼叫本地函式」一樣方便**，但實際上那個函式會在遠端系統執行。 (執行中的函式就是 Procedure)
- RPC **通訊協定** 會自動幫你完成序列化、傳輸、反序列化等動作。


## 二、RPC 的底層邏輯

傳統上，如果你要呼叫遠端系統的功能，要自己寫：

1. 建立 TCP 連線
2. 打包參數（序列化）
3. 傳送
4. 等待回應（反序列化）
5. 處理錯誤、重試、超時等邏輯

這些繁瑣又容易出錯。

所以 **RPC 的目標** 就是**把網路傳輸這些細節封裝起來**，讓開發者「像呼叫本地函式一樣」呼叫遠端邏輯。

例如：

```java
// 假設這個函式實際上在另一台伺服器
User user = userService.getUserById(123);
```

你看起來只是呼叫一個 Java method，但底層實際上是：

```bash=
Client → serialize request → send over network → 
Server → deserialize → execute real method → serialize response → send back → 
Client → deserialize → return result
```

---

## 三、RPC 底層技術在演進

| 層面   | 早期 RPC    | 現代 gRPC                |
| ---- | --------- | ---------------------- |
| 序列化  | 自定義二進位格式  | Protocol Buffers（二進位）  |
| 傳輸協定 | TCP / UDP | HTTP/2                 |
| 呼叫型式 | 單次請求回應    | + streaming（雙向流）       |
| 語言支援 | 通常單一語言    | 多語言自動生成 stub           |
| 安全   | 弱或無       | TLS / mTLS 支援          |
| 通訊樣式 | 同步        | 支援同步 / 非同步 / streaming |

:::

## 2. 微服務的核心概念

- **獨立部署**：每個微服務都是獨立的，可單獨部署與更新，而不影響其他服務。
- **獨立技術棧**：每個微服務可以使用不同的編程語言、框架和數據存儲技術。
- **單一職責（Single Responsibility）**：每個微服務只關注一個特定的業務功能，如用戶管理、訂單處理、支付系統等。
- **分散式架構**：微服務運行於不同的伺服器或容器內，透過網絡進行通信。
- **容錯性**：如果某個微服務失敗，不會影響整個系統，其他服務仍能正常運行。


:::info
微服務架構只建立在**分散式系統**上
:::

### → 這麼做能解決什麼問題？
#### ⚠️ 單體式架構 (Monolith) 的困難

- **難以維護**：隨著功能不斷增加，單體應用會變得龐大且難以理解。
- **部署風險高**：小改動也可能需要重新部署整個應用，增加出錯機率。
- **擴展性受限**：只能整體水平擴展，無法針對單一功能進行精準擴展。
- **技術選型受限**：整個系統被綁定在同一技術棧，不易引入新技術。
- **團隊協作困難**：多人同時開發會造成模組依賴與衝突，降低開發效率。

:::spoiler 什麼是 Monolith (單體式架構)？
### 📦 Monolith (單體式架構)

**定義**
Monolith 是一種 **將整個應用程式集中在單一程式碼庫或部署單位** 的架構風格。
應用程式的所有功能模組（UI、業務邏輯、資料存取…）都被打包成 **一個執行檔或一個應用程式**，並且一起部署、一起運作。

---

### 🏗️ 架構特徵

1. **單一程式碼庫（Single Codebase）**

   * 所有功能寫在同一個專案或 repo 裡。

2. **單一部署單位（Single Deployment Unit）**

   * 不論新增或修改哪一個功能，都需要 **重新打包、重新部署整個應用**。

3. **集中式資料庫**

   * 通常共用同一個資料庫，所有模組都會直接存取。

4. **緊密耦合（Tightly Coupled）**

   * 功能彼此相依，難以獨立修改或擴展。

---

### ✅ 優點

* **開發簡單**：新手容易上手，初期開發速度快。
* **部署簡單**：只需要部署一次，不用管理多個服務。
* **測試容易**：整個應用在一個環境裡測試即可。
* **效能好**：模組間呼叫是函式呼叫，不需跨網路。

---

### ⚠️ 缺點

* **維護困難**：程式碼龐大後，修改一處可能影響全域。
* **部署風險大**：小改動也要重新部署整個應用，出錯風險高。
* **擴展性差**：無法針對單一模組獨立擴展，只能整體水平擴展。
* **技術債累積**：不同功能模組強烈耦合，導致開發效率下降。

---

### 🖼️ 簡單範例

假設你有一個電商平台（購物車、支付、會員、商品管理）：

* 在 **Monolith 架構** 中，所有功能都寫在同一個專案裡：

  * `UserController`
  * `ProductController`
  * `CartService`
  * `PaymentService`
  * **同一個 JAR / WAR / 執行檔** 一起部署。

---

### 👉 總結：
**Monolith = 一個大盒子，所有功能都塞在裡面，部署和擴展都只能整體操作。**

:::


---

## 3. 微服務的常見技術

- **API Gateway**：Kong、Traefik、NGINX、Spring Cloud Gateway
- **服務發現（Service Discovery）**：Consul、Eureka、Zookeeper
- **微服務框架**：Spring Boot（Java）、NestJS（Node.js）、Quarkus（Java）、Go Micro（Go）
- **容器與編排**：Docker、Kubernetes（K8s）
- **分佈式追蹤**：Jaeger、Zipkin
- **Message Queue**：RabbitMQ、Kafka、ActiveMQ、SQS（AWS）


:::spoiler 什麼是 Message Queue？
## Message Queue（消息隊列）

### 1. 什麼是 Message Queue？

消息隊列（Message Queue，MQ）是一種 **非同步通信機制**，用於在分散式系統中傳遞數據（不限傳遞資料格式？）。它允許不同的服務之間使用「消息」進行通信，而不需要彼此同步運行，從而提高系統的**可擴展性**、**可靠性**和**解耦性**。

需要額外 Message Queue Middleware 協助，也會被稱作 **Message Broker** 或 **Message Bus**。

### 2. 為什麼需要Message Queue？

- **Decoupling （解耦）**：發送者（Producer）和接收者（Consumer）不需要直接通信，而是透過Message Queue進行數據傳遞。
- **Asynchronous Processing （異步處理）**：允許系統在不同步的情況下處理請求，提升性能與響應速度。
- **Traffic Smoothing （流量削峰）**：當流量突增時，Message Queue可以作為緩衝，防止系統過載。
- **Reliability & Fault Tolerance （可靠性與容錯）**：如果某個服務暫時無法處理請求，消息會被保留在Message Queue，等系統恢復後再處理。

### 3. Message Queue 的工作流程

1. **Producer（生產者）**：發送消息到Message Queue。
2. **Consumer（消費者）**：從Message Queue中獲取消息並處理。

### 4. Message Queue的類型

#### 1️⃣ 點對點（Point-to-Point）

- **特點**：一條消息只能被 **一個** Consumer消費一次。
- **應用場景**：任務隊列、訂單處理。
- **代表技術**：ActiveMQ、RabbitMQ、Amazon SQS。

#### 2️⃣ 發布/訂閱（Publish-Subscribe）

- **特點**：一條消息可以被 **多個** Consumer訂閱並處理。
- **應用場景**：事件驅動架構、日誌廣播、通知系統。
- **代表技術**：Kafka、Redis Pub/Sub、Google Pub/Sub。

### 5. 常見的消息隊列技術

| MQ 技術 | 特點 | 適用場景 |
| --- | --- | --- |
| **RabbitMQ** | 基於 AMQP 協議，低延遲，可靠性高 | 任務隊列、短時間存儲消息 |
| **Apache Kafka** | 高吞吐量，分布式流處理 | 大數據、日誌分析、事件驅動架構 |
| **ActiveMQ** | 兼容多種協議，適合企業級應用 | 分布式系統的通信 |
| **Amazon SQS** | 雲端託管，無需維護 | 雲端應用消息傳遞 |

---

### 6. Microservice 和 Message Queue 的關係

在微服務架構中，Message Queue 經常被用來**解決微服務之間的通信問題**，例如：

1. **Asynchronous Processing（非同步處理）**
- 訂單系統（Order Service）下單後，通過消息隊列通知庫存服務（Inventory Service）更新庫存，而不用等待庫存更新完成再返回結果。
2. **Event-Driven Architecture（ 事件驅動架構）**
- 當用戶完成支付（Payment Service），系統可以透過 Kafka 廣播「支付成功」的事件，通知其他微服務（如物流、通知等）。
3. **Traffic Smoothing （削峰填谷）**
- 當有大量請求湧入時，服務可以先將請求放入 MQ，慢慢處理，避免瞬間超載。
:::


---

:::danger
### 容器化 ≠ 微服務

| **容器化 Containerization** | **微服務 Microservice** |
| --- | --- |
| 一個容器只放一種應用程式 | 一種應用程式只做一件事情 |
:::

---


## 4. 微服務的優缺點

### ✅ **優點**

- **易於擴展（Scalability）**：可以根據需求對特定微服務進行獨立擴展。
- **高可用性（High Availability）**：故障影響範圍小，不會導致整個系統崩潰。
- **方便 CI/CD**：每個微服務可以單獨測試、部署，而不影響其他部分。
- **技術靈活性（Technology Flexibility）**：可以使用適合每個微服務的技術棧，而不是統一規範所有技術。

### ❌ **缺點**

- **運維複雜度提高**：需要管理多個獨立的服務，包含監控、部署、日誌管理等。
- **分布式通信開銷**：微服務之間需要透過 API 或 Message Queue 進行通信，可能會增加延遲和故障點。
- **數據一致性問題**：不同微服務使用不同的數據存儲時，確保一致性變得更具挑戰性（可使用分佈式事務或最終一致性策略）。

---

## 5. 轉成微服務架構可能會遇到的困難

### 1️⃣ 版本控制不易 (有 Mono-repo, Poly-repo 兩種方法)

1. **Mono-repo**
    - **所有微服務程式碼集中在一個倉庫**
    - 優點：方便統一管理、共享程式庫、確保版本一致性、CI/CD 流程簡單
    - 缺點：倉庫龐大，權限控管不易，不同團隊可能會互相干擾

2. **Poly-repo**
    - **每個微服務都有獨立的倉庫**
    - 優點：團隊自主性高、權限隔離清楚、符合微服務獨立的特性
    - 缺點：跨服務版本管理困難，CI/CD 複雜度提升，依賴套件難以同步

### 2️⃣ 太多 service 跟 pod 不易管理配置
(例：網路配置)


即便順利的將服務搬到 k8s 上，當 service/pod 一多，可以使用像是 **[Istio](#%E2%9B%B5-Istio-%E7%AE%A1%E7%90%86-ServicePod-%E9%96%93%E7%9A%84%E7%B6%B2%E8%B7%AF%E6%A9%9F%E5%88%B6) 的工具來統一管理各個 pod 間的網路連線**。


---

## ⛵ Istio: 管理 Service/Pod 間的網路機制
> "Istio is a service mesh."

Service Mesh 的基本概念就是在每個 pod 旁增加一個 side car，**把關於流量、網路的雜事都抽離出來**，接下來所有經過 pod 的 request 都是經由 side car 轉導，大大減輕開發者們的負擔。除了 Istio 常見的還有 Linkerd。

另外，Istio 也順帶整合了許多第三方的觀測工具(e.g. Prometheus, Kiali, Grafana, etc.)


### Istio 架構
Istio 會在各個 Pod 旁 Inject 一個 side car (叫做 istio-proxy) 來幫助我們轉發 request、蒐集資訊等；而上層則會==多一層 Control Plane 層==，會透過一個叫做 istiod 的 Pod 來幫助我們控管服務，開發者可以**透過指令/設定去統一管控 Service/Pod 間的網路機制**。

而 istiod 中間的 Pilot、Citadel、Galley 等元件有個概念就可以了，一般 Istio 操作上應該是不會遇到它們。


### 是否使用 Istio 的比較
在原生 K8S 上，各 Pod 間的溝通是透過 Node 上的 kube-proxy(它會紀錄其他節點上的 Pod iptables)並做 request 轉發。
而在 Istio 上，則不用透過原生機制，因為==每個 Side car 就是個 proxy==，所以直接幫你做 request 轉發。
![image](https://hackmd.io/_uploads/B1cO4e3yge.png)

> CNI (Container Network Interface)

---


## 總結一下

### ❓ 解決了什麼問題（Why）
- 解決單體式應用在**維護困難、部署風險、擴展受限**等痛點
- 提升 **可用性**、**彈性**、**團隊協作效率**

### 🧱 核心概念與架構（What / How）
- 每個服務 **單一職責、獨立部署、獨立技術棧**
- 透過 **API Gateway / Service Discovery / Message Queue / Service Mesh** 等基礎建設串接
- 使用 **Docker + Kubernetes** 來管理部署與擴展

### 📈 專案適合引入 Microservice 的條件
- 系統功能龐大且持續擴充
- 使用者數量龐大，需要水平方向的擴展能力
- 團隊人數多，需要分工協作與技術多樣性
- 需要快速上線與頻繁迭代（CI/CD）
- 需要高可用與容錯性

---
