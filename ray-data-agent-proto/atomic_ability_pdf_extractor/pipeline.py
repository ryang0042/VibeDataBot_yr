from __future__ import annotations

import hashlib
import json
import os
import shutil
import time
from pathlib import Path
from typing import Any, Dict

from .backend_output import build_backend_output
from .docling_parser import parse_pdf_to_json
from .patch_engine import run_patch_engine
from .visualizer import COLOR_MAP, draw_bboxes

COLOR_MAP["COMPLEX_BLOCK"] = (1, 0, 1)


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _default_output_root() -> Path:
    return _project_root() / "runtime" / "pdf_extractor_outputs"


def _job_dir_name(pdf_path: Path) -> str:
    signature = hashlib.sha1(str(pdf_path).encode("utf-8")).hexdigest()[:10]
    return f"{pdf_path.stem}_{signature}"


def _copy_original(pdf_path: Path, output_dir: Path) -> Path:
    target = output_dir / "1_original.pdf"
    shutil.copy2(pdf_path, target)
    return target


def _cleanup_intermediates(*paths: Path) -> None:
    for path in paths:
        if path.exists():
            path.unlink()


def _build_plain_text(payload: Dict[str, Any]) -> str:
    text_segments = [
        segment["text"]
        for segment in payload.get("segments", [])
        if segment.get("kind") == "text" and isinstance(segment.get("text"), str)
    ]
    return "\n\n".join(segment.strip() for segment in text_segments if segment.strip()).strip()


def build_pdf_pipeline(
    file_path: str,
    *,
    keep_intermediates: bool = False,
    output_root: str | None = None,
) -> Dict[str, Any]:
    start_time = time.time()

    pdf_path = Path(file_path).expanduser().resolve()
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF 文件未找到: {pdf_path}")
    if pdf_path.suffix.lower() != ".pdf":
        raise ValueError(f"输入文件不是 PDF: {pdf_path}")

    output_base = Path(output_root).expanduser().resolve() if output_root else _default_output_root()
    output_base.mkdir(parents=True, exist_ok=True)

    job_dir = output_base / _job_dir_name(pdf_path)
    if job_dir.exists():
        shutil.rmtree(job_dir)
    job_dir.mkdir(parents=True, exist_ok=True)

    original_copy = _copy_original(pdf_path, job_dir)
    raw_json = job_dir / "2_docling_raw.json"
    raw_visual = job_dir / "3_docling_visual.pdf"
    final_json = job_dir / "4_patched_final.json"
    final_visual = job_dir / "5_patched_visual.pdf"

    parse_pdf_to_json(str(pdf_path), str(raw_json))
    draw_bboxes(str(pdf_path), str(raw_json), str(raw_visual))
    run_patch_engine(str(pdf_path), str(raw_json), str(final_json))
    draw_bboxes(str(pdf_path), str(final_json), str(final_visual))

    backend_outputs = build_backend_output(str(pdf_path), str(final_json), str(job_dir))
    payload_path = Path(backend_outputs["payload_path"])
    with open(payload_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    if not keep_intermediates:
        _cleanup_intermediates(raw_json, raw_visual)

    plain_text_content = _build_plain_text(payload)
    end_time = time.time()

    return {
        "doc_id": pdf_path.name,
        "source_url": f"local://{pdf_path}",
        "preview_url": f"local://{final_visual}",
        "markdown_content": payload.get("markdown_content", ""),
        "plain_text_content": plain_text_content,
        "metadata": {
            "page_count": payload.get("metadata", {}).get("page_count", 0),
            "pipeline_name": "Atomic Layout",
            "used_extract_kit": True,
            "fast_track_enabled": False,
            "output_dir": str(job_dir),
            "patched_json_path": str(final_json),
            "backend_payload_path": str(payload_path),
            "final_visual_path": str(final_visual),
            "original_copy_path": str(original_copy),
            "asset_count": payload.get("metadata", {}).get("asset_segment_count", 0),
            "text_segment_count": payload.get("metadata", {}).get("text_segment_count", 0),
            "extracted_images": [
                asset.get("preview_path")
                for asset in payload.get("extracted_assets", [])
                if asset.get("preview_path")
            ],
            "keep_intermediates": keep_intermediates,
        },
        "extracted_images": [
            asset.get("preview_path")
            for asset in payload.get("extracted_assets", [])
            if asset.get("preview_path")
        ],
        "segments": payload.get("segments", []),
        "_is_scanned_pdf": False,
        "_processing_time_ms": round((end_time - start_time) * 1000, 2),
    }
