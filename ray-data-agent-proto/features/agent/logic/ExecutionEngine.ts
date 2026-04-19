import { ExecutionPlan, JobStep } from "../types/AgentTypes";
import { MockDataService } from "../../data-view/logic/MockDataService";

type PdfArtifact = {
    _is_pdf_result: true;
    markdown_content: string;
    plain_text_content?: string;
    metadata?: Record<string, unknown>;
    source_url?: string;
    preview_url?: string;
    doc_id?: string;
};

type QualityArtifact = {
    _is_quality_result: true;
    evaluation: {
        final_decision?: string;
        final_score?: number | null;
        score_detail?: {
            rule_hits?: Array<{ code: string; severity?: string }>;
        };
    };
};

type ExecutionArtifact = unknown;

interface ExecutionCallbacks {
    onStepUpdate: (stepId: string, status: "active" | "completed" | "failed") => void;
    onLog: (log: string) => void;
    onArtifact: (stepId: string, data: ExecutionArtifact[]) => void;
    onProgress: (progress: {
        stepId: string | null;
        stepLabel: string;
        message: string;
        percent: number;
        indeterminate?: boolean;
    } | null) => void;
}

export class ExecutionEngine {
    static async executePlan(plan: ExecutionPlan, callbacks: ExecutionCallbacks) {
        callbacks.onLog(`🚀 Starting execution of plan: ${plan.id}`);
        callbacks.onProgress({
            stepId: null,
            stepLabel: "Preparing",
            message: "Queued execution plan and checking prerequisites...",
            percent: 0,
        });

        // 用于在 Pipeline 级联步骤之间互相传递依赖数据
        const artifactPayloads: Record<string, ExecutionArtifact[]> = {};
        const totalSteps = Math.max(plan.steps.length, 1);

        for (const [stepIndex, step] of plan.steps.entries()) {
            const basePercent = Math.round((stepIndex / totalSteps) * 100);

            // 1. Mark Running
            callbacks.onStepUpdate(step.id, "active");
            callbacks.onProgress({
                stepId: step.id,
                stepLabel: step.label,
                message: `Starting ${step.label}...`,
                percent: basePercent,
                indeterminate: true,
            });
            callbacks.onLog(`\n--- [Step: ${step.label}] ---`);
            callbacks.onLog(`Executing: ${step.description}`);
            if (step.codeSnippet) {
                callbacks.onLog(`Code:\n${step.codeSnippet}`);
            }

            // 2. Simulate Work
            try {
                const stepArtifact = await this.simulateExecution(step, callbacks, artifactPayloads);
                if (stepArtifact) {
                    artifactPayloads[step.id] = stepArtifact;
                    callbacks.onArtifact(step.id, stepArtifact);
                }

                // 3. Mark Done
                callbacks.onStepUpdate(step.id, "completed");
                callbacks.onProgress({
                    stepId: step.id,
                    stepLabel: step.label,
                    message: `${step.label} completed.`,
                    percent: Math.round(((stepIndex + 1) / totalSteps) * 100),
                    indeterminate: false,
                });
            } catch (error) {
                callbacks.onLog(`❌ Error in step ${step.id}: ${error}`);
                callbacks.onStepUpdate(step.id, "failed");
                callbacks.onProgress({
                    stepId: step.id,
                    stepLabel: step.label,
                    message: `${step.label} failed.`,
                    percent: basePercent,
                    indeterminate: false,
                });
                throw error;
            }
        }

        callbacks.onLog(`\n✅ Plan execution finished successfully.`);
        callbacks.onProgress({
            stepId: null,
            stepLabel: "Completed",
            message: "All plan steps finished successfully.",
            percent: 100,
            indeterminate: false,
        });
    }

    private static async simulateExecution(
        step: JobStep,
        callbacks: ExecutionCallbacks,
        artifactPayloads: Record<string, ExecutionArtifact[]>
    ): Promise<ExecutionArtifact[] | void> {
        const baseDelay = 2000;

        switch (step.type) {
            case "LOAD_DATA":
                callbacks.onLog("Connecting to S3 bucket s3://customer-logs/...");
                await this.delay(1000);
                const sourceData = MockDataService.generateSourceData(50);
                callbacks.onLog(`Found ${sourceData.length} parquet files (24.5 GB).`);
                
                await this.delay(1000);
                callbacks.onLog("Reading schema... Done.");
                return sourceData;

            case "SCAN_PII":
                callbacks.onLog("Initializing Presidio Analyzer...");
                await this.delay(800);
                const currentData = MockDataService.generateSourceData(50); // In real app, would get from previous step
                const scannedData = MockDataService.scanForPII(currentData);
                const piiCount = scannedData.filter(r => r._pii_detected).length;

                callbacks.onLog("Distributing tasks to 4 Ray Actors...");
                await this.delay(1500);
                callbacks.onLog(`[Worker 1] Scanning... Found ${Math.floor(piiCount / 2)} issues.`);
                callbacks.onLog(`[Worker 2] Scanning... Found ${Math.ceil(piiCount / 2)} issues.`);

                const redactedData = MockDataService.redactPII(scannedData);
                await this.delay(1000);
                callbacks.onLog(`Aggregation results: Found ${piiCount} PII instances.`);
                return redactedData;

            case "TRANSFORM":
                callbacks.onLog("Compiling Ray DAG...");
                await this.delay(500);
                const dirtyData = MockDataService.generateSourceData(50);
                const cleanData = MockDataService.cleanData(dirtyData);
                callbacks.onLog(`Applying filter: x != null. Removed ${dirtyData.length - cleanData.length} rows.`);
                
                callbacks.onLog("Transforming columns...");
                await this.delay(1000);
                return cleanData;

            case "EXTRACT_PDF":
                callbacks.onProgress({
                    stepId: step.id,
                    stepLabel: step.label,
                    message: "Preparing atomic PDF extraction pipeline...",
                    percent: 8,
                    indeterminate: true,
                });
                callbacks.onLog("Initializing PyMuPDF / VDU Pipeline Engine...");
                const targetFilePath = step.metadata?.filePath;
                if (!targetFilePath) {
                    throw new Error("No PDF file path provided. Please specify a file path.");
                }

                const isAdvancedLayout = step.metadata?.isAdvancedLayout;

                if (isAdvancedLayout) {
                    callbacks.onLog("🚀 Booting PDF-Extract-Kit...");
                    await this.delay(800);
                    callbacks.onLog("[LayoutYOLO] Loaded model weights from cluster.");
                    callbacks.onLog("[TableMaster] Initialized formula and tabular extractors.");
                }

                callbacks.onProgress({
                    stepId: step.id,
                    stepLabel: step.label,
                    message: "Reading source PDF and dispatching extraction job...",
                    percent: 18,
                    indeterminate: true,
                });
                callbacks.onLog(`Dispatching Ray Tasks for local file: ${targetFilePath}`);
                await this.delay(600);
                
                // Attempt to call Next.js Local API
                callbacks.onProgress({
                    stepId: step.id,
                    stepLabel: step.label,
                    message: "Running layout parsing, patching, and asset export. This can take a little while...",
                    percent: 42,
                    indeterminate: true,
                });
                const resp = await fetch("/api/extract-pdf", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ filePath: targetFilePath })
                });

                if (!resp.ok) {
                    const errorData = await resp.json().catch(() => ({}));
                    throw new Error(errorData.message || `API request failed with status: ${resp.status}`);
                }

                const pdfData = await resp.json();
                callbacks.onProgress({
                    stepId: step.id,
                    stepLabel: step.label,
                    message: "Rendering patched preview and assembling markdown result...",
                    percent: 78,
                    indeterminate: true,
                });
                
                callbacks.onLog(`✅ Extraction Layout Algorithm Completed! Time: ${pdfData._processing_time_ms}ms.`);

                const outMarkdown = pdfData.markdown_content as string;
                const plainTextContent =
                    (pdfData.plain_text_content as string | undefined) ?? (pdfData.markdown_content as string);
                const addScannedWarn = pdfData._is_scanned_pdf;

                if(addScannedWarn) {
                   callbacks.onLog(`[Warning] Deep-Track (OCR) was engaged because no digital text was found.`);
                }

                callbacks.onProgress({
                    stepId: step.id,
                    stepLabel: step.label,
                    message: "PDF extraction artifacts are ready.",
                    percent: 92,
                    indeterminate: false,
                });
                
                return [{ 
                    _is_pdf_result: true, 
                    markdown_content: outMarkdown, 
                    plain_text_content: plainTextContent,
                    metadata: {
                        ...pdfData.metadata,
                        _is_scanned_pdf: pdfData._is_scanned_pdf,
                        used_extract_kit: pdfData.metadata?.used_extract_kit ?? isAdvancedLayout ?? false,
                        _processing_time_ms: pdfData._processing_time_ms,
                    },
                    source_url: pdfData.source_url || targetFilePath,
                    preview_url: pdfData.preview_url || pdfData.source_url || targetFilePath,
                    doc_id: pdfData.doc_id,
                }];

            case "CLEAN_TEXT":
                callbacks.onLog("Initializing Text Cleaning RegEx ruleset & Hyphenation Fixer...");
                await this.delay(800);
                
                // 找到上游产生的 PDF 文档数据
                const lastPdfEntry = this.getLatestArtifact(artifactPayloads, this.isPdfArtifact);
                if (!lastPdfEntry) {
                    throw new Error("No upstream Markdown content found. Please extract PDF first.");
                }

                const originalText = this.getArtifactText(lastPdfEntry);
                callbacks.onLog(`Input text size: ${originalText.length} characters.`);
                
                await this.delay(1500);
                // 简单正则清洗：替换连续3个以上的换行为2个换行，替换多余空格
                let cleanedText = originalText.replace(/\\n{3,}/g, '\\n\\n'); 
                cleanedText = cleanedText.replace(/[ \\t]{2,}/g, ' ');
                // 跨行连字修复 (Hyphenation Fix): "infor-\nmation" -> "information"
                const beforeHyphenFix = cleanedText.length;
                cleanedText = cleanedText.replace(/([a-zA-Z]+)-\\n([a-zA-Z]+)/g, "$1$2");
                const fixedHyphens = (beforeHyphenFix - cleanedText.length);

                callbacks.onLog(`Cleanup complete. Fixed ${fixedHyphens} hyphens. Reduced size by ${originalText.length - cleanedText.length} chars.`);
                
                return [{
                    ...lastPdfEntry,
                    markdown_content: cleanedText,
                    plain_text_content: cleanedText,
                    metadata: {
                        ...lastPdfEntry.metadata,
                        cleaned: true,
                        hyphens_fixed: fixedHyphens,
                        removed_chars: originalText.length - cleanedText.length
                    }
                }];

            case "DEDUPLICATE":
                callbacks.onLog("Hashing paragraphs to find duplicate boilerplates...");
                await this.delay(800);
                
                // 找到上游产生的清洗数据
                const upstreamNode = this.getLatestArtifact(artifactPayloads, this.isPdfArtifact);
                if (!upstreamNode) {
                    throw new Error("No upstream Markdown content found to deduplicate.");
                }

                const sourceText = this.getArtifactText(upstreamNode);
                const paragraphs = this.splitIntoBlocks(sourceText);
                callbacks.onLog(`Total blocks analyzed: ${paragraphs.length}`);
                
                await this.delay(1500);
                
                const uniqueParagraphs = new Set<string>();
                const dedupedBlocks = [];
                let removedLines = 0;

                for (const p of paragraphs) {
                    const cleanP = p.trim();
                    if (!cleanP) continue;
                    // 跳过多短的句子，不进行去重（比如页码）
                    if (cleanP.length < 10) {
                        dedupedBlocks.push(p);
                        continue;
                    }
                    if (uniqueParagraphs.has(cleanP)) {
                        removedLines++;
                        continue; // skip duplicate
                    }
                    uniqueParagraphs.add(cleanP);
                    dedupedBlocks.push(p);
                }

                callbacks.onLog(`Deduplication finished. Suppressed ${removedLines} duplicate blocks.`);
                
                return [{
                    ...upstreamNode,
                    markdown_content: dedupedBlocks.join('\\n\\n'),
                    plain_text_content: dedupedBlocks.join('\\n\\n'),
                    metadata: {
                        ...upstreamNode.metadata,
                        deduplicated: true,
                        removed_blocks: removedLines
                    }
                }];

            case "QUALITY_CHECK":
                callbacks.onLog("Running atomic 15-dimension quality evaluation...");
                await this.delay(600);
                const upstreamForQuality = this.getLatestArtifact(artifactPayloads, this.isPdfArtifact);
                if (!upstreamForQuality) throw new Error("No upstream document found for quality check.");

                const sourceType = upstreamForQuality.metadata?._is_scanned_pdf ? "pdf_ocr" : "pdf_text";
                const extractMode = upstreamForQuality.metadata?._is_scanned_pdf ? "ocr" : "direct";
                callbacks.onLog(`Submitting document to quality evaluator as ${sourceType}...`);

                const qualityResp = await fetch("/api/evaluate-quality", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({
                        text: this.getArtifactText(upstreamForQuality),
                        sourceType,
                        extractMode,
                        outputProfile: "standard",
                        includeInputMeta: true,
                        includeNormalizedTextLength: true,
                        inputMeta: {
                            page_count: upstreamForQuality.metadata?.page_count,
                            source_path: upstreamForQuality.source_url,
                            used_extract_kit: upstreamForQuality.metadata?.used_extract_kit ?? false,
                            cleaned: upstreamForQuality.metadata?.cleaned ?? false,
                            deduplicated: upstreamForQuality.metadata?.deduplicated ?? false,
                        },
                    }),
                });

                if (!qualityResp.ok) {
                    const errorData = await qualityResp.json().catch(() => ({}));
                    throw new Error(errorData.message || `Quality evaluation failed with status: ${qualityResp.status}`);
                }

                const qualityData = await qualityResp.json();
                const decision = qualityData.final_decision ?? "manual_review";
                const finalScore = Number(qualityData.final_score ?? 0);
                const ruleHits: Array<{ code: string; severity?: string }> = qualityData.score_detail?.rule_hits ?? [];
                callbacks.onLog(
                    `Quality Validation: Score ${finalScore.toFixed(2)} / 100, Decision = ${decision.toUpperCase()}`
                );
                if (ruleHits.length > 0) {
                    callbacks.onLog(
                        `Quality Flags: ${ruleHits.map((hit) => hit.code).join(", ")}`
                    );
                }

                return [{
                    _is_quality_result: true,
                    evaluation: qualityData,
                    source_url: upstreamForQuality.source_url,
                    document_preview: this.truncateText(this.getArtifactText(upstreamForQuality), 1200),
                    metadata: {
                        page_count: upstreamForQuality.metadata?.page_count,
                        source_type: sourceType,
                        extract_mode: extractMode,
                        used_extract_kit: upstreamForQuality.metadata?.used_extract_kit ?? false,
                    }
                }];

            case "GENERATE_CORPUS":
                callbacks.onLog("Initiating Text Chunking (Context Window = ~1024 tokens)");
                await this.delay(1000);
                
                const upstreamForCorpus = this.getLatestArtifact(artifactPayloads, this.isPdfArtifact);
                if (!upstreamForCorpus) throw new Error("No upstream document found for chunking.");
                const latestQualityResult = this.getLatestArtifact(artifactPayloads, this.isQualityArtifact);
                const qualityDecision = latestQualityResult?.evaluation?.final_decision ?? "manual_review";
                if (latestQualityResult && qualityDecision !== "pass") {
                    callbacks.onLog(
                        `Quality gate blocked corpus generation: ${qualityDecision.toUpperCase()}`
                    );
                    throw new Error(
                        `Corpus generation halted because quality decision is ${qualityDecision}.`
                    );
                }

                const fullText = this.getArtifactText(upstreamForCorpus);
                callbacks.onLog("Splitting by logical paragraphs to preserve semantic borders...");
                await this.delay(1500);

                const finalParagraphs = this.splitIntoBlocks(fullText);
                const chunks = [];
                let currentChunk = "";
                // 模拟一个极其简单的 1000 字符切片（在真实中会走 tokenizer计算 token数）
                const MAX_CHARS = 1000; 

                for (const p of finalParagraphs) {
                    if (currentChunk.length + p.length > MAX_CHARS) {
                        if (currentChunk) chunks.push(currentChunk);
                        currentChunk = p;
                    } else {
                        currentChunk += (currentChunk ? "\\n\\n" : "") + p;
                    }
                }
                if (currentChunk) chunks.push(currentChunk);

                callbacks.onLog(`Generated ${chunks.length} Chunks. Serializing to JSONL-like Array...`);
                await this.delay(1000);

                // 组装最终结果数据集格式
                const corpusDataset = chunks.map((chunkText, idx) => ({
                    chunk_id: `${upstreamForCorpus.doc_id || "doc"}_${idx}`,
                    text: chunkText,
                    meta_domain: "pdf_extraction",
                    meta_source: upstreamForCorpus.source_url || "local_upload",
                    meta_quality: latestQualityResult?.evaluation?.final_score ?? 100,
                    char_length: chunkText.length
                }));

                // 此处我们利用 DataFrame 来展示多行的 JSON 结构
                return corpusDataset;

            default:
                await this.delay(baseDelay);
                break;
        }
    }

    private static delay(ms: number) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    private static getLatestArtifact<T extends ExecutionArtifact>(
        artifactPayloads: Record<string, ExecutionArtifact[]>,
        predicate: (artifact: ExecutionArtifact) => artifact is T
    ): T | null {
        const flattened = Object.values(artifactPayloads).flat();
        for (let index = flattened.length - 1; index >= 0; index -= 1) {
            const candidate = flattened[index];
            if (predicate(candidate)) {
                return candidate;
            }
        }
        return null;
    }

    private static truncateText(text: string, maxLength: number) {
        if (text.length <= maxLength) {
            return text;
        }
        return `${text.slice(0, maxLength).trimEnd()}...`;
    }

    private static getArtifactText(artifact: PdfArtifact) {
        const plain = typeof artifact.plain_text_content === "string" ? artifact.plain_text_content : "";
        if (plain.trim()) {
            return plain;
        }
        return artifact.markdown_content;
    }

    private static splitIntoBlocks(text: string) {
        return text
            .split(/\n\s*\n/g)
            .map((block) => block.trim())
            .filter((block) => block.length > 0);
    }

    private static isPdfArtifact(artifact: ExecutionArtifact): artifact is PdfArtifact {
        if (typeof artifact !== "object" || artifact === null) {
            return false;
        }
        return Boolean(
            "_is_pdf_result" in artifact &&
            artifact._is_pdf_result === true &&
            "markdown_content" in artifact &&
            typeof artifact.markdown_content === "string"
        );
    }

    private static isQualityArtifact(artifact: ExecutionArtifact): artifact is QualityArtifact {
        if (typeof artifact !== "object" || artifact === null) {
            return false;
        }
        return Boolean(
            "_is_quality_result" in artifact &&
            artifact._is_quality_result === true &&
            "evaluation" in artifact &&
            typeof artifact.evaluation === "object" &&
            artifact.evaluation !== null
        );
    }
}
