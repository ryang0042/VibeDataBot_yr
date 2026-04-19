export interface PdfExtractionMetadata {
    page_count: number;
    fast_track_enabled: boolean;
    pipeline_name?: string;
    output_dir?: string;
    patched_json_path?: string;
    backend_payload_path?: string;
    final_visual_path?: string;
    original_copy_path?: string;
    asset_count?: number;
    text_segment_count?: number;
    extracted_images?: string[];
    [key: string]: unknown; // Allow future dynamic metadata like confidence scores
}

export interface PdfExtractionResult {
    doc_id: string;               // e.g. "report-2024.pdf"
    source_url: string;           // Where the PDF was loaded from (S3, local)
    preview_url?: string;         // The patched visual PDF rendered in the left pane
    markdown_content: string;     // The fully assembled reading-order markdown string
    plain_text_content?: string;  // Text-only content for cleaning, deduplication, and quality checks
    metadata: PdfExtractionMetadata;
    extracted_images: string[];   // Array of S3/CDN URLs for the cropped image blocks
    segments?: Array<Record<string, unknown>>;
    _is_scanned_pdf: boolean;     // Indicates if DEEP_TRACK (OCR) was forced
    _processing_time_ms: number;  // The time taken by the Ray backend
}
