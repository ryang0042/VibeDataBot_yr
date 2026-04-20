# PDF Extractor Atomic Capability

本目录提供一个可独立集成的 PDF 版面分块原子能力，目标是：

- 使用 `docling` 对论文类 PDF 做快速版面识别
- 基于 `patch_engine.py` 对 Docling 输出做增量补丁
- 输出结构化 JSON、可视化打框 PDF，以及面向后续集成的混合内容结果

## 1. 输入契约

当前入口脚本已经支持两种输入形式：

- 单个 PDF 文件
- 一个包含多个 PDF 文件的目录

默认输入路径是 `./test_data`，因此你可以继续保持现在的使用方式；如果传入的是单个 PDF，程序也会在输出根目录下为该 PDF 创建一个同名子目录，并按照和目录模式完全一致的结构产出结果。

示例：

```bash
python test_0.py
python test_1.py

python test_0.py ./test_data/2307.04327.pdf
python test_1.py ./test_data/2307.04327.pdf

python test_0.py ./some_pdf_dir --output-base ./output_data
python test_1.py ./some_pdf_dir --output-base ./output_data
```

## 2. 输出契约

每个 PDF 都会在输出根目录下生成一个同名文件夹：

```text
output_data/
  2307.04327/
    1_original.pdf
    2_docling_raw.json
    3_docling_visual.pdf
    4_patched_final.json
    5_patched_visual.pdf
    6_mixed_content.md
    7_backend_payload.json
    8_block_assets/
      page_001/
      page_002/
      ...
```

各文件含义如下：

- `1_original.pdf`
  原始 PDF 备份，方便对照查看。
- `2_docling_raw.json`
  `docling_parser.py` 的原始版面识别结果，字段中包含 `label/page/bbox/text/...` 等结构化信息。
- `3_docling_visual.pdf`
  基于 `2_docling_raw.json` 渲染出的 Docling 原始打框可视化结果。
- `4_patched_final.json`
  `patch_engine.py` 做完增量补丁之后的最终 JSON。
- `5_patched_visual.pdf`
  基于 `4_patched_final.json` 渲染出的最终打框可视化结果。
- `6_mixed_content.md`
  按 patched 后阅读顺序组织的“正文文本 + 资产块包装”混合输出。正文类块直接提取文本；图表、公式、复杂块则以块属性、相对路径和预览图的形式呈现。
- `7_backend_payload.json`
  面向后续系统集成的结构化输出，包含 `markdown_content`、`segments`、`extracted_assets` 与统计信息。
- `8_block_assets/`
  从最终 patched 结果中裁剪出的块级资产目录。当前会为 `TABLE`、`PICTURE`、`COMPLEX_BLOCK`、`FORMULA` 导出无损 `pdf` 裁片，并额外生成 `png` 预览图，方便在 Markdown 中直接图文交织展示。

如果只运行 `test_0.py`，则只会生成：

- `1_original.pdf`
- `2_docling_raw.json`
- `3_docling_visual.pdf`

如果运行 `test_1.py`，则会额外生成：

- `4_patched_final.json`
- `5_patched_visual.pdf`
- `6_mixed_content.md`
- `7_backend_payload.json`
- `8_block_assets/`

## 3. 脚本职责

### `docling_parser.py`

负责：

- 调用 `docling` 对 PDF 做版面识别
- 将 Docling 原始对象统一转换成后续算法可消费的 JSON 契约
- 做 Docling 坐标系到 PyMuPDF 坐标系的统一映射

输出的 JSON 是整个补丁算法的基础输入。

### `visualizer.py`

负责：

- 读取 JSON
- 按 `bbox + label + id` 在 PDF 上渲染可视化框

它不参与算法判定，只负责结果展示。

### `patch_engine.py`

负责：

- 基于 `2_docling_raw.json` 对 Docling 的输出做增量修补
- 尽量保留 Docling 已经做对的块
- 主要处理幽灵文本、公式 complex 化、漏框补齐、页边噪声过滤、重叠收敛等问题

它是本原子能力的核心。

### `test_0.py`

负责：

- 跑“Docling 原始解析 + 原始可视化”
- 适合验证 `docling_parser.py` 和 `visualizer.py`

### `test_1.py`

负责：

- 跑“Docling 原始解析 + 原始可视化 + patch 修补 + patched 可视化”
- 适合作为完整原子能力入口

### `pipeline_io.py`

负责：

- 统一解析输入路径
- 判断输入是单 PDF 还是 PDF 目录
- 为每个 PDF 创建标准输出目录
- 复制 `1_original.pdf`

### `backend_output.py`

负责：

- 读取 `4_patched_final.json`
- 基于最终阅读顺序只对正文类文本块重新从原 PDF 中抽取文本
- 将 `TABLE`、`PICTURE`、`COMPLEX_BLOCK`、`FORMULA` 裁剪成独立资产
- 为资产同时导出 `pdf` 和 `png` 预览图
- 将图注优先挂接到相邻的 figure/table 资产上
- 对 `FOOTNOTE` 做轻量启发式筛选，仅保留具备明显正文信息量的脚注
- 生成 `6_mixed_content.md` 和 `7_backend_payload.json`

## 4. 执行顺序

完整流水线由 `test_1.py` 按如下顺序执行：

1. 解析输入路径，收集待处理 PDF
2. 为每个 PDF 创建输出目录
3. 复制原始 PDF 到 `1_original.pdf`
4. `docling_parser.py` 生成 `2_docling_raw.json`
5. `visualizer.py` 生成 `3_docling_visual.pdf`
6. `patch_engine.py` 基于原始 JSON 生成 `4_patched_final.json`
7. `visualizer.py` 基于 patched JSON 生成 `5_patched_visual.pdf`
8. `backend_output.py` 生成 `6_mixed_content.md`
9. `backend_output.py` 导出 `8_block_assets/` 并写出 `7_backend_payload.json`

## 5. 补丁算法的大体逻辑

`patch_engine.py` 的设计原则是：

- 信任 Docling
- 只做增量修改
- 尽量避免破坏 Docling 原本正确的块

目前补丁流程整体上可理解为以下几类阶段：

1. 清理噪声
   删除表格/目录内部的幽灵文本，过滤页眉页脚/边缘噪声。
2. 公式相关修补
   对公式编号锚点、公式 drawing、公式栈、重叠公式区域进行 complex 化与闭包合并。
3. 漏框补齐
   对正文、公式邻域、部分覆盖 block、微小文本等进行定向补漏。
4. 冲突收敛
   解决 `TEXT / FORMULA / COMPLEX_BLOCK` 之间的重叠冲突，得到最终无重叠输出。
5. 安全排序
   尽量保留原始 Docling 块的阅读顺序，并为新增块插入合理位置。

如果你后续要在大项目里集成，推荐直接把 `test_1.py` 视作这个原子能力的完整入口。

## 6. JSON 结果说明

当前 JSON 中核心字段包括：

- `id`
  当前输出中的顺序编号
- `label`
  块类型，如 `TEXT`、`FORMULA`、`TABLE`、`PICTURE`、`COMPLEX_BLOCK`
- `page`
  页码，从 1 开始
- `bbox`
  坐标框，格式为 `[x0, y0, x1, y1]`，坐标系与 PyMuPDF 一致
- `text`
  该块的文本内容；部分公式 complex 或图片类块可能为空或仅有标记文本
- `text_preview`
  文本预览

`2_docling_raw.json` 是原始 Docling 输出契约；`4_patched_final.json` 是补丁后的最终契约。

## 7. 运行依赖

核心三方库：

- `docling`
- `PyMuPDF`（导入名：`fitz`）
- `shapely`

标准库依赖包括：

- `json`
- `re`
- `uuid`
- `argparse`
- `pathlib`
- `collections`
- `statistics`
- `shutil`

## 8. 运行环境上的补充说明

`docling_parser.py` 内部默认设置了以下环境变量：

- `HF_ENDPOINT=https://hf-mirror.com`
- 如需强制离线运行，可显式设置 `VIBEDATABOT_HF_OFFLINE=1`
- `OMP_NUM_THREADS=6`
- `OPENBLAS_NUM_THREADS=6`
- `MKL_NUM_THREADS=6`

这些设置主要用于：

- 降低依赖远端网络的不确定性
- 控制本地并行线程数

## 9. 集成建议

如果后续要集成到更大的项目里，推荐：

- 把 `test_1.py` 当成完整入口
- 把 `4_patched_final.json` 当成主消费产物
- 把 `5_patched_visual.pdf` 当成调试与人工验收产物
- 把 `7_backend_payload.json` 当成后续 VibeDataBot 集成时最直接的接口候选
- 在上层系统中，把“输入路径 -> 输出根目录”的映射交给当前脚本处理，不需要额外再写一套单文件/目录判定逻辑
