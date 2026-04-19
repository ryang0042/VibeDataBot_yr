import shutil
from pathlib import Path


def resolve_pdf_inputs(input_path):
    candidate = Path(input_path).expanduser().resolve()
    if not candidate.exists():
        raise FileNotFoundError(f"输入路径不存在: {candidate}")

    if candidate.is_file():
        if candidate.suffix.lower() != ".pdf":
            raise ValueError(f"输入文件不是 PDF: {candidate}")
        return [candidate]

    pdf_files = sorted(
        path for path in candidate.iterdir() if path.is_file() and path.suffix.lower() == ".pdf"
    )
    if not pdf_files:
        raise ValueError(f"输入目录中没有 PDF 文件: {candidate}")
    return pdf_files


def prepare_pdf_output_dir(output_base_dir, pdf_path):
    output_base = Path(output_base_dir).expanduser().resolve()
    output_base.mkdir(parents=True, exist_ok=True)

    pdf_out_dir = output_base / pdf_path.stem
    pdf_out_dir.mkdir(parents=True, exist_ok=True)

    original_copy_path = pdf_out_dir / "1_original.pdf"
    shutil.copy2(pdf_path, original_copy_path)
    return pdf_out_dir
