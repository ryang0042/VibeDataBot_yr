import json
import re
from pathlib import Path

import fitz


BODY_TEXT_LABELS = {
    "TEXT",
    "LIST_ITEM",
    "SECTION_HEADER",
    "TITLE",
}
ASSET_LABELS = {"TABLE", "PICTURE", "COMPLEX_BLOCK", "FORMULA"}


def _normalize_spaces(text):
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    return text.strip()


def _clean_text(text):
    if not text:
        return ""
    text = text.replace("\xa0", " ")
    text = text.replace("-\n", "")
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    return text.strip()


def _flatten_block_text(text):
    text = _clean_text(text)
    if not text:
        return ""
    text = re.sub(r"\s*\n\s*", " ", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    return text.strip()


def _has_substantial_text(text):
    compact = re.sub(r"\s+", "", text)
    if len(compact) < 8:
        return False
    return bool(re.search(r"[A-Za-z\u4e00-\u9fff]", compact))


def _should_keep_footnote(text):
    cleaned = _clean_text(text)
    if not cleaned:
        return False
    if re.fullmatch(r"[\d\W_]+", cleaned):
        return False
    url_stripped = re.sub(r"https?://\S+|www\.\S+", "", cleaned)
    if not _has_substantial_text(url_stripped):
        return False
    return _has_substantial_text(cleaned)


def _asset_kind(label):
    return {
        "PICTURE": "figure",
        "TABLE": "table",
        "FORMULA": "formula",
        "COMPLEX_BLOCK": "complex",
    }.get(label, label.lower())


def _extract_text_from_bbox(page, bbox):
    rect = fitz.Rect(*bbox)
    rect = rect & page.rect
    if rect.is_empty or rect.width <= 0 or rect.height <= 0:
        return ""
    return _clean_text(page.get_textbox(rect))


def _expand_rect(rect, page_rect, padding=4.0):
    clip = fitz.Rect(rect.x0 - padding, rect.y0 - padding, rect.x1 + padding, rect.y1 + padding)
    clip = clip & page_rect
    if clip.is_empty or clip.width <= 0 or clip.height <= 0:
        return None
    return clip


def _export_crop_pdf(doc, page_index, bbox, output_path):
    page = doc[page_index]
    clip = _expand_rect(fitz.Rect(*bbox), page.rect)
    if clip is None:
        return False

    out_doc = fitz.open()
    out_page = out_doc.new_page(width=clip.width, height=clip.height)
    out_page.show_pdf_page(out_page.rect, doc, page_index, clip=clip)
    out_doc.save(output_path)
    out_doc.close()
    return True


def _export_crop_png(doc, page_index, bbox, output_path, zoom=2.0):
    page = doc[page_index]
    clip = _expand_rect(fitz.Rect(*bbox), page.rect)
    if clip is None:
        return False

    matrix = fitz.Matrix(zoom, zoom)
    pix = page.get_pixmap(matrix=matrix, clip=clip, alpha=False)
    pix.save(output_path)
    return True


def _format_text_segment(label, text):
    if not text:
        return ""
    if label in {"TITLE", "SECTION_HEADER"}:
        return text
    if label == "LIST_ITEM":
        return f"- {text}"
    return text


def _format_asset_segment(segment):
    lines = [
        f"```asset",
        f"type: {segment['asset_kind']}",
        f"label: {segment['label']}",
        f"id: {segment['id']}",
        f"page: {segment['page']}",
        f"pdf_path: {segment['relative_pdf_path']}",
    ]
    if segment.get("relative_preview_path"):
        lines.append(f"preview_path: {segment['relative_preview_path']}")
    if segment.get("caption"):
        lines.append(f"caption: {segment['caption']}")
    lines.append("```")

    if segment.get("relative_preview_path"):
        lines.append(
            f"![{segment['asset_kind']} page {segment['page']}]({segment['relative_preview_path']})"
        )

    return "\n".join(lines)


def _build_markdown(segments):
    pieces = []
    for segment in segments:
        if segment["kind"] == "text":
            piece = _format_text_segment(segment["label"], segment["text"])
        else:
            piece = _format_asset_segment(segment)
        if piece:
            pieces.append(piece)
    return "\n\n".join(pieces).strip()


def _item_center_y(item):
    return (item["bbox"][1] + item["bbox"][3]) / 2.0


def _item_center_x(item):
    return (item["bbox"][0] + item["bbox"][2]) / 2.0


def _find_caption_attachments(items):
    attachments = {}
    asset_indices = [
        idx for idx, item in enumerate(items) if item["label"] in {"PICTURE", "TABLE", "COMPLEX_BLOCK"}
    ]

    for idx, item in enumerate(items):
        if item["label"] != "CAPTION":
            continue

        caption_hint = _normalize_spaces(item.get("text", "")).lower()
        preferred_labels = None
        if caption_hint.startswith("fig"):
            preferred_labels = {"PICTURE", "COMPLEX_BLOCK"}
        elif caption_hint.startswith("table"):
            preferred_labels = {"TABLE"}

        best_asset_idx = None
        best_score = None
        for asset_idx in asset_indices:
            asset = items[asset_idx]
            if asset["page"] != item["page"]:
                continue
            if abs(asset_idx - idx) > 3:
                continue
            if preferred_labels and asset["label"] not in preferred_labels:
                continue

            vertical_gap = abs(_item_center_y(asset) - _item_center_y(item))
            horizontal_gap = abs(_item_center_x(asset) - _item_center_x(item))
            score = (abs(asset_idx - idx) * 1000.0) + vertical_gap + horizontal_gap * 0.1

            if best_score is None or score < best_score:
                best_score = score
                best_asset_idx = asset_idx

        if best_asset_idx is None and preferred_labels:
            for asset_idx in asset_indices:
                asset = items[asset_idx]
                if asset["page"] != item["page"]:
                    continue
                if abs(asset_idx - idx) > 3:
                    continue

                vertical_gap = abs(_item_center_y(asset) - _item_center_y(item))
                horizontal_gap = abs(_item_center_x(asset) - _item_center_x(item))
                score = (abs(asset_idx - idx) * 1000.0) + vertical_gap + horizontal_gap * 0.1

                if best_score is None or score < best_score:
                    best_score = score
                    best_asset_idx = asset_idx

        if best_asset_idx is not None:
            attachments.setdefault(best_asset_idx, []).append(idx)

    return attachments


def build_backend_output(pdf_path, patched_json_path, output_dir):
    pdf_path = Path(pdf_path).expanduser().resolve()
    patched_json_path = Path(patched_json_path).expanduser().resolve()
    output_dir = Path(output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    markdown_path = output_dir / "6_mixed_content.md"
    payload_path = output_dir / "7_backend_payload.json"
    asset_root = output_dir / "8_block_assets"
    asset_root.mkdir(parents=True, exist_ok=True)

    with open(patched_json_path, "r", encoding="utf-8") as f:
        items = json.load(f)

    doc = fitz.open(pdf_path)
    segments = []
    exported_assets = []
    caption_attachments = _find_caption_attachments(items)
    attached_caption_indices = {
        caption_idx for caption_indices in caption_attachments.values() for caption_idx in caption_indices
    }

    try:
        for idx, item in enumerate(items):
            page_index = item["page"] - 1
            if page_index < 0 or page_index >= len(doc):
                continue

            label = item["label"]
            bbox = item["bbox"]
            page = doc[page_index]
            fallback_text = _clean_text(item.get("text", ""))

            if label in ASSET_LABELS:
                page_dir = asset_root / f"page_{item['page']:03d}"
                page_dir.mkdir(parents=True, exist_ok=True)
                asset_kind = _asset_kind(label)
                pdf_filename = (
                    f"page_{item['page']:03d}_block_{item['id']:04d}_{asset_kind}.pdf"
                )
                png_filename = (
                    f"page_{item['page']:03d}_block_{item['id']:04d}_{asset_kind}.png"
                )
                pdf_asset_path = page_dir / pdf_filename
                png_asset_path = page_dir / png_filename
                exported_pdf = _export_crop_pdf(doc, page_index, bbox, pdf_asset_path)
                if not exported_pdf:
                    continue
                exported_png = _export_crop_png(doc, page_index, bbox, png_asset_path)

                attached_captions = []
                for caption_idx in caption_attachments.get(idx, []):
                    caption_item = items[caption_idx]
                    caption_text = _flatten_block_text(
                        _extract_text_from_bbox(doc[caption_item["page"] - 1], caption_item["bbox"])
                        or caption_item.get("text", "")
                    )
                    if caption_text:
                        attached_captions.append(caption_text)

                relative_pdf_path = pdf_asset_path.relative_to(output_dir).as_posix()
                relative_preview_path = (
                    png_asset_path.relative_to(output_dir).as_posix() if exported_png else None
                )
                segments.append(
                    {
                        "kind": "asset",
                        "id": item["id"],
                        "page": item["page"],
                        "label": label,
                        "asset_kind": asset_kind,
                        "bbox": bbox,
                        "caption": "\n".join(attached_captions).strip(),
                        "relative_pdf_path": relative_pdf_path,
                        "relative_preview_path": relative_preview_path,
                    }
                )
                exported_assets.append(
                    {
                        "id": item["id"],
                        "page": item["page"],
                        "label": label,
                        "asset_kind": asset_kind,
                        "pdf_path": relative_pdf_path,
                        "preview_path": relative_preview_path,
                    }
                )
                continue

            if label == "CAPTION" and idx in attached_caption_indices:
                continue

            if label == "FOOTNOTE":
                extracted_text = _extract_text_from_bbox(page, bbox)
                final_text = extracted_text or fallback_text
                if not _should_keep_footnote(final_text):
                    continue
            elif label in BODY_TEXT_LABELS:
                extracted_text = _extract_text_from_bbox(page, bbox)
                final_text = extracted_text or fallback_text
            else:
                continue

            final_text = _flatten_block_text(final_text)
            if not final_text:
                continue

            segments.append(
                {
                    "kind": "text",
                    "id": item["id"],
                    "page": item["page"],
                    "label": label,
                    "bbox": bbox,
                    "text": final_text,
                }
            )

        markdown_content = _build_markdown(segments)
        markdown_path.write_text(markdown_content, encoding="utf-8")

        payload = {
            "doc_id": pdf_path.name,
            "source_pdf": "1_original.pdf",
            "patched_json": patched_json_path.name,
            "markdown_path": markdown_path.name,
            "markdown_content": markdown_content,
            "segments": segments,
            "extracted_assets": exported_assets,
            "metadata": {
                "page_count": len(doc),
                "segment_count": len(segments),
                "text_segment_count": sum(1 for segment in segments if segment["kind"] == "text"),
                "asset_segment_count": sum(1 for segment in segments if segment["kind"] == "asset"),
                "asset_root": asset_root.name,
            },
        }

        with open(payload_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
    finally:
        doc.close()

    return {
        "markdown_path": markdown_path,
        "payload_path": payload_path,
        "asset_root": asset_root,
        "asset_count": len(exported_assets),
        "segment_count": len(segments),
    }
