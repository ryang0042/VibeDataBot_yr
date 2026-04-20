import os
import json
import fitz  # PyMuPDF
from pathlib import Path

# --- 网络与性能环境（必须放在最前面） ---
# 不要默认强制离线，否则新设备首次运行时无法拉取 Docling 依赖。
os.environ.setdefault("HF_ENDPOINT", "https://hf-mirror.com")
if os.environ.get("VIBEDATABOT_HF_OFFLINE") == "1":
    os.environ.setdefault("HF_HUB_OFFLINE", "1")
os.environ.setdefault("OMP_NUM_THREADS", "6")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "6")
os.environ.setdefault("MKL_NUM_THREADS", "6")

from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.datamodel.base_models import InputFormat


def _normalize_ref(ref):
    if ref is None:
        return None
    if hasattr(ref, "cref"):
        return ref.cref
    return str(ref)


def _normalize_label(item):
    label = getattr(item, "label", None)
    if label is None:
        return type(item).__name__.replace("Item", "").upper()
    return getattr(label, "name", str(label)).upper()


def _iter_ref_nodes(doc):
    for attr in ("texts", "groups", "tables", "pictures", "furniture"):
        for node in getattr(doc, attr, []) or []:
            yield node


def _build_ref_index(doc):
    ref_index = {}
    for node in _iter_ref_nodes(doc):
        self_ref = getattr(node, "self_ref", None)
        if not self_ref:
            continue
        ref_index[self_ref] = {
            "self_ref": self_ref,
            "parent_ref": _normalize_ref(getattr(node, "parent", None)),
            "label": _normalize_label(node),
            "item_type": type(node).__name__,
        }
    return ref_index


def _resolve_ancestor_chain(ref_index, parent_ref, max_depth=16):
    ancestor_refs = []
    ancestor_labels = []
    current_ref = parent_ref
    seen = set()

    while current_ref and current_ref not in seen and len(ancestor_refs) < max_depth:
        seen.add(current_ref)
        ancestor_refs.append(current_ref)

        meta = ref_index.get(current_ref)
        if not meta:
            ancestor_labels.append(current_ref.split("/")[-2].upper() if "/" in current_ref else current_ref.upper())
            break

        ancestor_labels.append(meta["label"])
        current_ref = meta["parent_ref"]

    return ancestor_refs, ancestor_labels

def convert_bbox_to_fitz_rect(docling_bbox, docling_page_size, fitz_page):
    """【核心坐标映射】将 Docling BBox 转换为 PyMuPDF 标准 [x0, y0, x1, y1]"""
    dl_w = docling_page_size.width
    dl_h = docling_page_size.height
    fz_w = fitz_page.rect.width
    fz_h = fitz_page.rect.height
   
    scale_x = fz_w / dl_w if dl_w > 0 else 1.0
    scale_y = fz_h / dl_h if dl_h > 0 else 1.0
   
    if hasattr(docling_bbox, "coord_origin") and "BOTTOMLEFT" in str(docling_bbox.coord_origin):
        top = dl_h - docling_bbox.t
        bottom = dl_h - docling_bbox.b
        top, bottom = min(top, bottom), max(top, bottom)
    else:
        top = docling_bbox.t
        bottom = docling_bbox.b
       
    left = docling_bbox.l
    right = docling_bbox.r
   
    return [
        round(left * scale_x, 2),
        round(top * scale_y, 2),
        round(right * scale_x, 2),
        round(bottom * scale_y, 2)
    ]

def parse_pdf_to_json(pdf_path, output_json_path):
    """解析 PDF 并将结果保存为标准 JSON 契约文件"""
    print(f"  [Parser] 正在解析: {Path(pdf_path).name} ...")
   
    # 极速探测配置
    pipeline_options = PdfPipelineOptions()
    pipeline_options.do_ocr = False
    pipeline_options.generate_picture_images = False
    pipeline_options.do_table_structure = False
   
    converter = DocumentConverter(
        format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)}
    )
   
    conversion_result = converter.convert(pdf_path, raises_on_error=False)
    doc = conversion_result.document
    fitz_doc = fitz.open(pdf_path)
    ref_index = _build_ref_index(doc)
   
    parsed_items = []
    reading_id = 1
   
    for item, level in doc.iterate_items():
        if not item.prov:
            continue
           
        prov = item.prov[0]
        page_no = prov.page_no # Docling 从 1 开始
        fitz_page_idx = page_no - 1
       
        if fitz_page_idx < 0 or fitz_page_idx >= len(fitz_doc):
            continue
           
        fitz_page = fitz_doc[fitz_page_idx]
        docling_page_size = doc.pages[page_no].size
       
        # 统一坐标
        bbox_coords = convert_bbox_to_fitz_rect(prov.bbox, docling_page_size, fitz_page)
       
        raw_text = ""
        if hasattr(item, "text") and item.text:
            raw_text = item.text.strip().replace("\n", " ")

        text_preview = raw_text[:80]
        self_ref = getattr(item, "self_ref", None)
        parent_ref = _normalize_ref(getattr(item, "parent", None))
        ancestor_refs, ancestor_labels = _resolve_ancestor_chain(ref_index, parent_ref)
        ancestor_container_label = next(
            (label for label in ancestor_labels if label in {"TABLE", "DOCUMENT_INDEX", "TOC", "PICTURE"}),
            None,
        )
           
        # 组装标准 JSON 对象
        item_data = {
            "id": reading_id,
            "label": item.label.name, # 转为纯字符串，如 "TEXT", "FORMULA"
            "page": page_no,
            "bbox": bbox_coords,
            "text": raw_text,
            "text_preview": text_preview,
            "item_type": type(item).__name__,
            "self_ref": self_ref,
            "parent_ref": parent_ref,
            "ancestor_refs": ancestor_refs,
            "ancestor_labels": ancestor_labels,
            "ancestor_container_label": ancestor_container_label,
            "level": level,
            "charspan": list(prov.charspan) if getattr(prov, "charspan", None) else None,
            "content_layer": str(getattr(item, "content_layer", "")),
        }
        parsed_items.append(item_data)
        reading_id += 1
       
    fitz_doc.close()
   
    # 写入 JSON
    with open(output_json_path, 'w', encoding='utf-8') as f:
        json.dump(parsed_items, f, indent=2, ensure_ascii=False)
       
    print(f"  [Parser] 解析完成，已写入 JSON。共提取 {len(parsed_items)} 个元素。")
