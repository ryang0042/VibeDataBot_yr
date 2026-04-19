"""CLI entrypoint for the embedded atomic PDF extractor."""

from __future__ import annotations

import argparse
import contextlib
import json
import sys

from .pipeline import build_pdf_pipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Atomic PDF extractor for VibeDataBot")
    parser.add_argument("--file-path", type=str, required=True, help="Absolute or relative path to the PDF file")
    parser.add_argument(
        "--keep-intermediates",
        action="store_true",
        help="Keep Docling raw JSON and raw visualization outputs for debugging.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    try:
        with contextlib.redirect_stdout(sys.stderr):
            result = build_pdf_pipeline(
                args.file_path,
                keep_intermediates=args.keep_intermediates,
            )
        print(json.dumps(result, ensure_ascii=False))
        return 0
    except Exception as exc:
        print(json.dumps({"error": True, "message": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
