"use client";

import React from "react";
import { cn } from "@/lib/utils";
import { FileText, Eye, Clock, Hash, ImageIcon } from "lucide-react";

interface MarkdownViewerProps {
    data: {
        markdown_content: string;
        metadata?: Record<string, unknown>;
        source_url: string;
        preview_url?: string;
        _is_pdf_result?: boolean;
    };
    title?: string;
    className?: string;
}

type ParsedBlock =
    | { type: "paragraph"; text: string }
    | {
          type: "asset";
          assetType?: string;
          label?: string;
          id?: string;
          page?: string;
          pdfPath?: string;
          previewPath?: string;
          caption?: string;
      };

function normalizeLocalPath(filePath: string) {
    if (filePath.startsWith("local:")) {
        return filePath.replace(/^local:/, "");
    }
    if (filePath.startsWith("file://")) {
        return filePath.replace(/^file:\/\//, "");
    }
    return filePath;
}

function resolveArtifactPath(rawPath: string | undefined, outputDir: string | undefined) {
    if (!rawPath) {
        return "";
    }

    if (rawPath.startsWith("/") || rawPath.startsWith("local:") || rawPath.startsWith("file://")) {
        return normalizeLocalPath(rawPath);
    }

    if (!outputDir) {
        return rawPath;
    }

    return `${outputDir.replace(/\/$/, "")}/${rawPath.replace(/^\.\//, "")}`;
}

function buildProxyUrl(filePath: string) {
    if (!filePath) {
        return "";
    }
    return `/api/file?path=${encodeURIComponent(normalizeLocalPath(filePath))}`;
}

function parseAssetFence(lines: string[], startIndex: number) {
    const meta: Record<string, string> = {};
    let index = startIndex + 1;

    while (index < lines.length && lines[index].trim() !== "```") {
        const line = lines[index];
        const separator = line.indexOf(":");
        if (separator >= 0) {
            const key = line.slice(0, separator).trim();
            const value = line.slice(separator + 1).trim();
            if (key) {
                meta[key] = value;
            }
        }
        index += 1;
    }

    if (index < lines.length && lines[index].trim() === "```") {
        index += 1;
    }

    while (index < lines.length && !lines[index].trim()) {
        index += 1;
    }

    if (index < lines.length && lines[index].trim().startsWith("![")) {
        const imageMatch = lines[index].trim().match(/!\[[^\]]*\]\(([^)]+)\)/);
        if (imageMatch) {
            meta.preview_path = imageMatch[1];
        }
        index += 1;
    }

    return {
        block: {
            type: "asset" as const,
            assetType: meta.type,
            label: meta.label,
            id: meta.id,
            page: meta.page,
            pdfPath: meta.pdf_path,
            previewPath: meta.preview_path,
            caption: meta.caption,
        },
        nextIndex: index,
    };
}

function parseMarkdownContent(content: string): ParsedBlock[] {
    const lines = content.split(/\r?\n/);
    const blocks: ParsedBlock[] = [];
    const paragraphBuffer: string[] = [];

    const flushParagraph = () => {
        const text = paragraphBuffer.join(" ").replace(/\s+/g, " ").trim();
        if (text) {
            blocks.push({ type: "paragraph", text });
        }
        paragraphBuffer.length = 0;
    };

    let index = 0;
    while (index < lines.length) {
        const line = lines[index];
        const trimmed = line.trim();

        if (trimmed === "```asset") {
            flushParagraph();
            const parsed = parseAssetFence(lines, index);
            blocks.push(parsed.block);
            index = parsed.nextIndex;
            continue;
        }

        if (!trimmed) {
            flushParagraph();
            index += 1;
            continue;
        }

        paragraphBuffer.push(trimmed);
        index += 1;
    }

    flushParagraph();
    return blocks;
}

export function MarkdownViewer({ data, title, className }: MarkdownViewerProps) {
    const outputDir = typeof data.metadata?.output_dir === "string" ? data.metadata.output_dir : undefined;
    const previewSource = data.preview_url || data.source_url;
    const previewUrl = previewSource ? buildProxyUrl(previewSource) : "";
    const parsedBlocks = parseMarkdownContent(data.markdown_content || "");
    const usedExtractKit = data.metadata?.used_extract_kit === true;
    const processingTime =
        typeof data.metadata?._processing_time_ms === "number" ? data.metadata._processing_time_ms : 0;
    const pageCount = typeof data.metadata?.page_count === "number" ? data.metadata.page_count : 1;
    const pipelineLabel =
        typeof data.metadata?.pipeline_name === "string"
            ? data.metadata.pipeline_name
            : data.metadata?.fast_track_enabled
              ? "Fast (Native)"
              : "Deep (VDU)";

    return (
        <div className={cn("rounded-lg border border-border overflow-hidden bg-card shadow-sm flex flex-col h-[700px]", className)}>
            <div className="bg-muted/30 px-4 py-3 border-b border-border flex flex-col gap-2 shadow-sm z-10">
                <div className="flex items-center justify-between">
                    <div className="flex items-center gap-2 text-sm font-semibold text-foreground">
                        <FileText size={16} className="text-primary" />
                        <span>{title || "PDF Extraction Result"}</span>
                        {usedExtractKit && (
                            <span className="ml-2 text-[10px] bg-indigo-500/20 text-indigo-400 px-2 py-0.5 rounded-full border border-indigo-500/30 uppercase tracking-wider">
                                Atomic PDF Extractor
                            </span>
                        )}
                    </div>
                    <div className="flex gap-2">
                        <span className="text-[10px] bg-muted/60 text-muted-foreground px-2 py-1 rounded flex items-center gap-1 border border-border/50">
                            <Clock size={10} /> {processingTime} ms
                        </span>
                        <span className="text-[10px] bg-primary/10 text-primary px-2 py-1 rounded flex items-center gap-1">
                            <Hash size={10} /> {pageCount} PGs
                        </span>
                        <span className="text-[10px] bg-muted-foreground/10 text-muted-foreground px-2 py-1 rounded flex items-center gap-1">
                            <Eye size={10} /> {pipelineLabel}
                        </span>
                    </div>
                </div>

                {data.source_url && (
                    <div className="text-[10px] font-mono text-muted-foreground truncate w-full flex items-center gap-1">
                        FILE: {data.source_url}
                    </div>
                )}
            </div>

            <div className="flex flex-1 overflow-hidden h-full">
                <div className="w-1/2 border-r border-border bg-black/5 flex flex-col relative group">
                    <div className="absolute top-2 left-2 bg-black/60 text-white text-[10px] px-2 py-1 rounded backdrop-blur-md opacity-50 group-hover:opacity-100 transition-opacity z-10 pointer-events-none">
                        Patched Layout Preview
                    </div>
                    {previewUrl ? (
                        <iframe
                            src={previewUrl + "#toolbar=0&navpanes=0"}
                            className="w-full h-full border-0 bg-white"
                            style={{ colorScheme: "light" }}
                            title="Patched PDF Preview"
                        />
                    ) : (
                        <div className="m-auto text-sm text-muted-foreground italic flex flex-col items-center gap-2">
                            <FileText className="opacity-20" size={48} />
                            No preview file available.
                        </div>
                    )}
                </div>

                <div className="w-1/2 bg-background p-6 overflow-y-auto font-sans leading-relaxed text-[13px] text-foreground relative group">
                    <div className="absolute top-2 right-2 bg-primary/10 text-primary text-[10px] px-2 py-1 rounded backdrop-blur-md opacity-50 group-hover:opacity-100 transition-opacity z-10">
                        Extracted Markdown
                    </div>

                    <div className="max-w-none flex flex-col gap-4 pr-2">
                        {parsedBlocks.length > 0 ? (
                            parsedBlocks.map((block, index) => {
                                if (block.type === "paragraph") {
                                    return (
                                        <p key={`paragraph-${index}`} className="whitespace-pre-wrap text-[13px] leading-7 text-foreground/90">
                                            {block.text}
                                        </p>
                                    );
                                }

                                const previewPath = resolveArtifactPath(block.previewPath, outputDir);
                                const pdfPath = resolveArtifactPath(block.pdfPath, outputDir);
                                const previewImageUrl = previewPath ? buildProxyUrl(previewPath) : "";
                                const pdfFileUrl = pdfPath ? buildProxyUrl(pdfPath) : "";

                                return (
                                    <div key={`asset-${index}`} className="rounded-xl border border-border/60 bg-muted/20 overflow-hidden">
                                        <div className="px-4 py-3 border-b border-border/50 flex items-center justify-between gap-3">
                                            <div className="flex items-center gap-2 text-sm font-medium text-foreground">
                                                <ImageIcon size={14} className="text-primary" />
                                                <span>{block.assetType || "asset"}</span>
                                            </div>
                                            <div className="text-[11px] text-muted-foreground font-mono">
                                                {block.label || "BLOCK"} #{block.id || "?"} · page {block.page || "?"}
                                            </div>
                                        </div>

                                        {block.caption ? (
                                            <div className="px-4 pt-3 text-[12px] leading-6 text-muted-foreground">
                                                {block.caption}
                                            </div>
                                        ) : null}

                                        {previewImageUrl ? (
                                            <div className="px-4 py-4">
                                                {/* eslint-disable-next-line @next/next/no-img-element */}
                                                <img
                                                    src={previewImageUrl}
                                                    alt={`${block.assetType || "asset"} preview`}
                                                    className="w-full rounded-lg border border-border/50 bg-white"
                                                />
                                            </div>
                                        ) : null}

                                        <div className="px-4 pb-4 text-[11px] text-muted-foreground flex flex-col gap-2">
                                            {block.pdfPath ? (
                                                <div className="break-all">
                                                    PDF: {block.pdfPath}
                                                </div>
                                            ) : null}
                                            {pdfFileUrl ? (
                                                <a
                                                    href={pdfFileUrl}
                                                    target="_blank"
                                                    rel="noreferrer"
                                                    className="text-primary hover:underline"
                                                >
                                                    Open cropped PDF
                                                </a>
                                            ) : null}
                                        </div>
                                    </div>
                                );
                            })
                        ) : (
                            <div className="text-sm text-muted-foreground italic">Empty content returned</div>
                        )}
                    </div>
                </div>
            </div>
        </div>
    );
}
