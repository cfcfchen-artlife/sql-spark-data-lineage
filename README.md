# sql-spark-data-lineage

> 扫描 SQL / PySpark / Scala / Jupyter Notebook 项目，自动提取数据血缘关系，生成带缩放工具栏的自包含 Mermaid 流程图 HTML。

只需告诉 AI 一句话，就能把 的上百行 ETL 脚本变成一张可在浏览器中任意缩放、可截图贴入 PPT 的血缘关系图。

[📥 查看 Demo 输出](examples/demo-lineage.html)（一个 19KB 的自包含 HTML，浏览器打开即渲染）

---

## 适配平台

| 平台 | 安装路径 | 状态 |
|---|---|---|
| **Reasonix** | `~/.reasonix/skills/sql-spark-data-lineage/SKILL.md` | ✅ 已验证 |
| **Claude Code** | `~/.claude/skills/sql-spark-data-lineage/SKILL.md` | 🔄 待社区验证 |
| **OpenClaw（龙虾）** | 需确认：`SKILL.md` 还是 `claude.md`？路径是 `~/.claude/skills/` 还是其他？ | 🔄 待社区验证 |
| **其他 Claw 衍生版** | 取决于分支的 skill 加载逻辑 | 🔄 待社区验证 |

> 如果你在某个平台上验证过并确认可用，欢迎告知我/提 PR 更新此表。

---

## 安装

### 一键安装（推荐）

告诉你的 AI agent：

```
帮我安装 https://github.com/xxx/sql-spark-data-lineage
```

大多数 Claw / Claude Code / Reasonix 的衍生版本会自动 clone 到正确的 skills 目录。

### 手动安装

如果一键安装不工作，在终端执行：

```bash
# Reasonix
git clone https://github.com/xxx/sql-spark-data-lineage.git ~/.reasonix/skills/sql-spark-data-lineage

# Claude Code / 大部分 Claw 衍生版
git clone https://github.com/xxx/sql-spark-data-lineage.git ~/.claude/skills/sql-spark-data-lineage
```

然后重启 agent 即可识别。

---
## 使用示例

在你的 AI agent 中说：

```
分析这个项目的 SQL 数据血缘
```

```
帮我把拟合26may17.sql 里所有 CREATE TABLE 的依赖关系画成血缘图
```

```
这个 PySpark + Hive 项目的表依赖关系是什么？出个 Mermaid 图
```

---

## 效果预览

输出一个自包含 HTML 文件，包含：

- 📊 **全量血缘图** — 所有表按 ODS / DWD / DIM / TMP / FIN 分层着色，左→右流向
- 🎯 **核心链路图** — 精简到 ~40 个节点，适合一页 PPT
- 🔍 **缩放工具栏** — 40%~500% 自由缩放，不依赖浏览器 zoom
- 双 Tab 切换，全量/简化一键切换

---

## 适用场景

| 项目类型 | 识别对象 |
|---|---|
| Hive SQL（`*.sql`） | `CREATE TABLE ... AS SELECT ... FROM ... JOIN ... UNION ALL ...` |
| PySpark（`*.py`） | `spark.read.table()` / `ArgodbSource()` → `.write.csv()` /other regex methods, 19 in total|
| Spark Scala（`*.scala`） | 复用 PySpark 正则：`spark.read.table()` / `.write.save()` /same as pyspark|
| Jupyter Notebook（`*.ipynb`） | 解析 JSON cell，按语言复用 SQL / PySpark / Scala 正则 |
| 星环 Transwarp ArgoDB | Holodesk 表也一并纳入血缘（自动标注 🌐） |

---

## 局限性

- 只追踪表级依赖，不处理列级血缘
- CTE（WITH 子句）按简单模式展开，复杂嵌套可能丢失中间名
- INSERT / MERGE 等写操作暂不纳入
- 基于正则提取，极端复杂的动态 SQL 拼接可能漏表
- `spark.sql("...")` / `%%sql` 内嵌 SQL 尽量提取，但 f-string 拼接或变量插值的动态 SQL 可能只抓到参数名

---


## License

MIT — 详见 [LICENSE](LICENSE)
