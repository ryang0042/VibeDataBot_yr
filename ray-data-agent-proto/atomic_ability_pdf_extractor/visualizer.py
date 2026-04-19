import fitz  # PyMuPDF
import json
from pathlib import Path

# 颜色映射字典 (R, G, B)
COLOR_MAP = {
    "TITLE": (0, 0, 1),
    "SECTION_HEADER": (0, 0.8, 1),
    "TEXT": (0.5, 0.5, 0.5),
    "LIST_ITEM": (0.6, 0.6, 0.6),
    "PICTURE": (1, 0, 0),    # Docling v2 合并了 Figure 和 Picture
    "TABLE": (0, 0.8, 0),
    "FORMULA": (1, 0.5, 0),
    "CAPTION": (0.8, 0, 0.8),
    "PAGE_HEADER": (0.8, 0.8, 0.8), # 页眉页脚用浅灰色，方便辨认
    "PAGE_FOOTER": (0.8, 0.8, 0.8)
}
DEFAULT_COLOR = (0, 0, 0)

def draw_bboxes(pdf_path, json_path, output_pdf_path):
    """根据 JSON 文件在 PDF 上绘制 BBox 和标签"""
    print(f"  [Visualizer] 正在渲染可视化文件: {Path(output_pdf_path).name} ...")
    
    with open(json_path, 'r', encoding='utf-8') as f:
        items = json.load(f)
        
    fitz_doc = fitz.open(pdf_path)
    
    for item in items:
        page_no = item["page"]
        fitz_page_idx = page_no - 1
        
        if fitz_page_idx < 0 or fitz_page_idx >= len(fitz_doc):
            continue
            
        fitz_page = fitz_doc[fitz_page_idx]
        x0, y0, x1, y1 = item["bbox"]
        rect = fitz.Rect(x0, y0, x1, y1)
        
        label = item["label"]
        color = COLOR_MAP.get(label, DEFAULT_COLOR)
        
        # 1. 画透明度较低的框线
        fitz_page.draw_rect(rect, color=color, width=1.0)
        
        # 2. 画标签背景和文字
        label_text = f"[{item['id']}] {label}"
        text_bg_rect = fitz.Rect(rect.x0, rect.y0 - 10, rect.x0 + len(label_text) * 5.5, rect.y0)
        fitz_page.draw_rect(text_bg_rect, color=color, fill=color)
        fitz_page.insert_text((rect.x0 + 2, rect.y0 - 2), label_text, fontsize=8, color=(1, 1, 1))
        
    fitz_doc.save(output_pdf_path)
    fitz_doc.close()
    print(f"  [Visualizer] 渲染完成！")