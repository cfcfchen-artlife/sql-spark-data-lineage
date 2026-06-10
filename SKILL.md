---
name: sql-spark-data-lineage
description: 扫描 SQL/PySpark/Scala/Jupyter 项目提取表级依赖关系，生成带缩放工具栏的自包含 Mermaid 数据血缘图 HTML。触发词：数据血缘、血缘图、血缘分析、data lineage、表依赖、Mermaid 血缘。
---

# sql-spark-data-lineage — SQL/Spark 数据血缘自动分析

给定一个项目目录，自动扫描 SQL、PySpark、Scala、Jupyter Notebook 文件，提取所有 CREATE TABLE / spark.read.table() 语句的上下游依赖关系，生成自包含的 Mermaid 流程图 HTML。

## 触发词

`数据血缘`、`血缘图`、`血缘分析`、`data lineage`、`表依赖`、`Mermaid 血缘`、`dependency graph`

## 工作流

### 阶段 1：扫描文件

递归扫描项目目录，收集以下文件：
- `*.sql`：Hive/Spark SQL 脚本
- `*.py`：PySpark 脚本
- `*.scala`：Spark Scala 脚本
- `*.ipynb`：Jupyter Notebook — 先解析 JSON 提取每个 cell 的 source 文本，对 code cell 按 language（`python` / `sql` / `scala`）复用对应正则；特别注意 `%%sql` / `%sql` magic cell 中的 SQL 语句

自适应规则：遇到其他扩展名的文本文件，如果内容匹配到下面阶段 2 中的任一模式，也按对应逻辑提取。**以内容为准，扩展名只作为首轮过滤**。

### 阶段 2：表依赖模式匹配（统一的正则清单）

对每行内容，按顺序尝试以下模式。同一次匹配中，`{source}` 标注为上游读表，`{target}` 标注为下游写表。

#### A. SQL 端 — 写表（目标表）

| 序号 | 模式 | 正则要点 | 示例 |
|---|---|---|---|
| S1 | `CREATE TABLE ... AS SELECT ...` | `CREATE\s+(EXTERNAL\s+)?TABLE\s+{target}\s+.*AS\s+SELECT` | `CREATE TABLE db.t1 AS SELECT ...` |
| S2 | `INSERT INTO ... SELECT ...` | `INSERT\s+(INTO|OVERWRITE\s+TABLE)\s+{target}\s+.*SELECT` | `INSERT INTO db.t1 SELECT ...` |
| S3 | `INSERT OVERWRITE TABLE ... SELECT ...` | 同上 S2（`OVERWRITE` 版本） | `INSERT OVERWRITE TABLE db.t1 SELECT ...` |
| S4 | Hive 多路插入 | `FROM\s+{source}\s+INSERT\s+(INTO|OVERWRITE)\s+{target1}\s+SELECT\s+...\s+INSERT\s+(INTO|OVERWRITE)\s+{target2}\s+SELECT` | `FROM src INSERT INTO t1 SELECT a INSERT INTO t2 SELECT b` |

对于 S1，额外记录存储格式（`STORED AS orc / holodesk / textfile`）。

#### B. SQL 端 — 读表（上游源表）

| 序号 | 模式 | 正则要点 |
|---|---|---|
| S5 | `FROM db.table` | `FROM\s+{source}` — 在 SELECT / INSERT 语句中提取 |
| S6 | `JOIN db.table` | `JOIN\s+{source}\s+ON` — 含 LEFT/RIGHT/INNER/FULL/CROSS |
| S7 | `UNION ALL` 子查询中的 FROM | 每个分支独立提取其 FROM 源表 |
| S8 | WITH 子句 CTE 内的 FROM | 展开 CTE 到最终物理表：`WITH cte AS (SELECT ... FROM {source})` → 提取 `{source}` |

忽略 `VALUES` 和纯字面量子查询中的表引用。

#### C. PySpark / Scala 端 — 读表

| 序号 | 模式 | 正则要点 | 示例 |
|---|---|---|---|
| P1 | `spark.table(...)` | `spark\.table\("{source}"\)` | `spark.table("db.t1")` |
| P2 | `spark.read.table(...)` | `spark\.read\.table\("{source}"\)` | `spark.read.table("db.t1")` |
| P3 | `spark.sql("...")` 内含读表 | 提取字符串内容，用 SQL 端 S5-S8 正则再扫一遍 | `spark.sql("SELECT * FROM db.t1 JOIN db.t2")` |
| P4 | `spark.read.format(...).load(...)` | `spark\.read\.format\(.*\)\.load\("{source}"\)` | `spark.read.format("csv").load("/path")` |
| P5 | `ArgodbSource(...)` / 专有格式 | `ArgodbSource\("{source}"\)` / `.option\("dbtablename",\s*"{source}"\)` | — |
| P6 | JDBC 源 | `spark\.read\.format\("jdbc"\).*option\("dbtable",\s*"{source}"\)` | — |

#### D. PySpark / Scala 端 — 写表

| 序号 | 模式 | 正则要点 | 示例 |
|---|---|---|---|
| P7 | `.write.saveAsTable(...)` | `\.write.*\.saveAsTable\("{target}"\)` | `df.write.mode("overwrite").saveAsTable("db.t2")` |
| P8 | `.write.insertInto(...)` | `\.write.*\.insertInto\("{target}"\)` | `df.write.insertInto("db.t2")` |
| P9 | `.write.save("...")` | `\.write.*\.save\("{target}"\)` → 标注为 HDFS/外部路径，非 Hive 表 | — |
| P10 | `.write.csv(...)` / `.parquet(...)` / `.orc(...)` | 同上，标注为文件路径 | — |
| P11 | `spark.sql("INSERT INTO ...")` | 提取字符串内容，用 SQL 端 S2/S3 正则再扫一遍 | `spark.sql("INSERT INTO db.t2 SELECT ...")` |

#### E. ipynb `%%sql` magic 特殊处理

Jupyter Notebook 中 `%%sql` / `%sql` cell 的内容是整个 SQL 语句。直接对 cell source 文本跑 SQL 端全部正则（S1-S8），提取读写表。

#### F. 通用兜底

如果 `spark.sql("...")` 或 `%%sql` 的字符串中包含 `${variable}` / `{param}` / `f"...{var}"` 等动态拼接，正则可能失败。此时至少提取参数名作为疑似表名，标记 `⚠️ 动态SQL` 提醒人工复核。

### 阶段 3：按数据层分类

根据表名字段和注释将每张表归入一个层：

| 层 | 表名规则 | 颜色 |
|---|---|---|
| 外部源表 | `ods_*` 后缀为 `_holo` 或被 ODS 层引用的外部表 | 🟡 #FFF3CD |
| ODS 层 | `ods_ep_*` | 🔵 #D1ECF1 |
| DWD 层 | `dwd_ep_*` | 🟢 #C3E6CB |
| 维表 | `dim_*` | 🟣 #F3E5F5 |
| 中间处理 | `tmp_*` / `viu_*` / `transc_*` / `need_*` / `step*` / `oppp2_*` | 🔴 #FDE2E2 |
| 最终输出 | `fin_*` / `dws_*` / `agg_*` / `res_*` | 🟠 #FFEAA7 |
| PySpark/Scala | `.py` / `.scala` 脚本 | ⚪ #E8DAEF |

### 阶段 4：生成 Mermaid HTML

输出输出一个自包含的 `data_lineage.html` 文件。必须使用以下经过验证的模板结构：

## Mermaid HTML 模板

```html
<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<title>数据血缘图 — {项目名}</title>
<script src="https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js"></script>
<style>
  body { margin: 20px; font-family: "Microsoft YaHei", sans-serif; background: #fafafa; }
  h1 { text-align: center; color: #333; }
  .tabs { display: flex; gap: 10px; margin: 20px 0; justify-content: center; }
  .tabs button { padding: 10px 24px; border: 2px solid #2c5e3f; border-radius: 6px; cursor: pointer; font-size: 16px; background: #fff; color: #2c5e3f; }
  .tabs button.active { background: #2c5e3f; color: #fff; }
  .mermaid { text-align: center; margin: 20px auto; width: 100%; overflow-x: auto; }
  .mermaid svg { width: 100% !important; height: auto !important; max-width: none !important; }
  .legend { display: flex; gap: 16px; flex-wrap: wrap; justify-content: center; margin: 10px 0; font-size: 13px; }
  .legend span { padding: 2px 10px; border-radius: 3px; }
  .zoom-bar { display: flex; gap: 6px; justify-content: center; align-items: center; margin: 8px 0; font-size: 14px; }
  .zoom-bar button { padding: 4px 14px; border: 1px solid #999; border-radius: 4px; cursor: pointer; background: #fff; font-size: 16px; min-width: 36px; }
  .zoom-bar button:hover { background: #eee; }
  .zoom-bar .zoom-pct { display: inline-block; width: 50px; text-align: center; font-weight: bold; }
  .zoom-wrapper { width: 100%; overflow: auto; transform-origin: top left; transition: transform 0.15s; }
  .mermaid.hidden-tab { visibility: hidden; position: absolute; pointer-events: none; }
</style>
</head>
<body>
<h1>🚗 数据血缘图 — {项目名/主题}</h1>
<p style="text-align:center;color:#666;font-size:14px">{简要说明}</p>

<div class="zoom-bar">
  <button onclick="zoomOut()" title="缩小">🔍−</button>
  <span class="zoom-pct" id="zoomLabel">100%</span>
  <button onclick="zoomIn()" title="放大">🔍+</button>
  <button onclick="zoomReset()" title="重置">↺</button>
</div>

<div class="legend">
  {各层色块}
</div>

<div class="zoom-wrapper" id="zoomWrapper">

<div class="tabs">
  <button class="active" onclick="showTab('full', this)">📊 全量血缘图</button>
  <button onclick="showTab('simple', this)">🎯 核心链路（PPT用）</button>
</div>

<div id="full" class="mermaid">
flowchart LR
  {全量 Mermaid 内容 — 按层 subgraph 分组}
</div>

<div id="simple" class="mermaid">
flowchart LR
  {简化版 Mermaid 内容 — 只保留核心主链路 ~20-40 节点}
</div>

</div><!-- end zoom-wrapper -->

<script>
var zoomLevel = 1.0;
var wrapper = document.getElementById('zoomWrapper');
var zoomLabel = document.getElementById('zoomLabel');

function applyZoom() {
  wrapper.style.transform = 'scale(' + zoomLevel + ')';
  zoomLabel.textContent = Math.round(zoomLevel * 100) + '%';
}
function zoomIn()  { zoomLevel = Math.min(5.0, zoomLevel + 0.15); applyZoom(); }
function zoomOut() { zoomLevel = Math.max(0.4, zoomLevel - 0.15); applyZoom(); }
function zoomReset() { zoomLevel = 1.0; applyZoom(); }

mermaid.initialize({ startOnLoad: true, securityLevel: 'loose', flowchart: { useMaxWidth: true, htmlLabels: true } });

var svgFixed = 0;
var obs = new MutationObserver(function(mutations) {
  mutations.forEach(function(m) {
    m.addedNodes.forEach(function(node) {
      if (node.tagName === 'svg' || (node.querySelectorAll && node.querySelectorAll('svg').length)) {
        var svgs = (node.tagName === 'svg') ? [node] : node.querySelectorAll('svg');
        svgs.forEach(function(svg) {
          svg.setAttribute('width', '100%');
          svg.setAttribute('height', 'auto');
          svg.style.maxWidth = 'none';
        });
        svgFixed++;
        if (svgFixed >= 2) {
          document.getElementById('simple').classList.add('hidden-tab');
          obs.disconnect();
        }
      }
    });
  });
});
obs.observe(document.body, { childList: true, subtree: true });

function showTab(tabId, btn) {
  wrapper.style.transform = 'scale(1.0)';
  zoomLevel = 1.0;
  zoomLabel.textContent = '100%';
  document.querySelectorAll('.mermaid').forEach(function(el) {
    el.style.display = 'none';
    el.style.visibility = '';
    el.style.position = '';
    el.style.pointerEvents = '';
  });
  var target = document.getElementById(tabId);
  target.style.display = 'block';
  target.classList.remove('hidden-tab');
  document.querySelectorAll('.tabs button').forEach(function(b) { b.classList.remove('active'); });
  btn.classList.add('active');
}
</script>
</body>
</html>
```

## Mermaid 构图规则

1. **用 `flowchart LR`**（左→右流向，适合表依赖关系）
2. **每个数据层一个 `subgraph`**，subgraph 标题包含中文层名和 emoji 图标
3. **节点 ID 用英文小写 + 下划线**，节点标签用中文（带 `<br/>` 换行）
4. **维表用 🌐 标记 Holodesk 格式**
5. **每个表节点显示简化表名 + 一句中文说明**，如：`dwd_entry["dwd_ep_entry<br/>（入口）"]`
6. **连线用 `-->`**
7. **mermaid init 必须加 `useMaxWidth: true, htmlLabels: true`**

## 简化版（PPT 用）生成规则

1. 精简到 6-8 个 subgraph，每个 subgraph 保留 2-5 个关键节点
2. 将旁链路合并为一个节点（如 `viu_plate_stp1~9` → 一个 `plate_chain["牌识关联链"]` 节点）
3. 去掉中间临时表的细碎步骤，保留核心数据处理阶段
4. 最终节点总数控制在 30-50 个

## 注意

- 不要依赖本地安装的工具（graphviz、mmdc 等），全部通过 CDN 加载 mermaid.js
- 生成的 HTML 必须是自包含的，浏览器打开即渲染
- 表格名和依赖关系必须从实际代码中提取，不要编造
- 如果项目同时有 SQL 和 PySpark/Scala，必须标注脚本的 I/O 关系
- `visibility: hidden` + `position: absolute` 隐藏策略必须严格遵守，这是解决双 Tab 渲染空白的关键
- zoom 上限 500%（`Math.min(5.0, ...)`）
- `spark.sql("...")` / `%%sql` 内嵌 SQL 尽量提取，但字符串拼接或 f-string 的动态 SQL 可能只抓到参数名，无法溯源到物理表名
