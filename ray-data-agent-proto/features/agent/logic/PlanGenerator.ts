import { ExecutionPlan, JobStep, StepType } from "../types/AgentTypes";

export class MockPlanGenerator {
    static async generate(userInput: string): Promise<ExecutionPlan> {
        // Simulate network delay for "Thinking"
        await new Promise(resolve => setTimeout(resolve, 1500));

        const steps: JobStep[] = [];
        const lowerInput = userInput.toLowerCase();

        // Simple Keyword-based Intent Parser (Mocking an LLM)

        if (lowerInput.includes("pdf") || lowerInput.includes("extract") || lowerInput.includes("提取")) {
            // Check if it's requesting Advanced VDU (like LayoutYOLO or PDF-Extract-Kit)
            const isAdvancedLayout = lowerInput.includes("版面") || lowerInput.includes("pdf-extract-kit") || lowerInput.includes("table") || lowerInput.includes("公式");

            // 优先匹配从目录树点击发送的标准格式 "路径是：/xxx.pdf"，其次匹配带有斜杠的绝对路径，最后退化为单词匹配
            let filePath = "";
            const pathIndicatorMatch = userInput.match(/路径.*?(?:[:：]|是\s*[:：]?)\s*(.*?\.pdf)/i);
            if (pathIndicatorMatch) {
                filePath = pathIndicatorMatch[1].trim();
            } else {
                const pathMatch = userInput.match(/(?:[a-zA-Z]:[/\\]|\/|~\/)[^\n\r"'<>|]*?\.pdf/i);
                if (pathMatch) {
                    filePath = pathMatch[0].trim();
                } else {
                    const simpleMatch = userInput.match(/[a-zA-Z0-9_\\-\\.\\/\\\\]+\\.pdf/i);
                    if (simpleMatch) {
                        filePath = simpleMatch[0].trim();
                    }
                }
            }

            steps.push({
                id: `step-pdf-${Date.now()}`,
                type: "EXTRACT_PDF",
                label: isAdvancedLayout ? "VDU Layout Analysis Pipeline" : "PDF Extraction Pipeline",
                description: filePath ? `Running ${isAdvancedLayout ? 'PDF-Extract-Kit (LayoutYOLO)' : 'PyMuPDF'} on ${filePath}` : "Pending PDF file path for execution...",
                status: "pending",
                codeSnippet: isAdvancedLayout ? `from pdf_extract_kit import LayoutYOLO, TableMaster\nmodel = LayoutYOLO.load()\nds = ray.data.read_binary_files("${filePath || '<wait_for_user_path>'}")\nds = ds.map_batches(OcrPipelineActor, concurrency=8)` : `ds = ray.data.read_binary_files("${filePath || '<wait_for_user_path>'}")\nds = ds.map_batches(OcrPipelineActor)`,
                metadata: { filePath, isAdvancedLayout }
            });

            if (lowerInput.includes("清洗") || lowerInput.includes("清理") || lowerInput.includes("clean")) {
                steps.push({
                    id: `step-clean-${Date.now() + 1}`,
                    type: "CLEAN_TEXT",
                    label: "Clean Layout Noise",
                    description: "Removing consecutive newlines, whitespace anomalies, and corrupted unicode sequences.",
                    status: "pending",
                    codeSnippet: `def _clean_text(row):\n    return {'text': re.sub(r'\\s+', ' ', row['text'])}\n\nds = ds.map(_clean_text)`
                });
            }

            if (lowerInput.includes("去重") || lowerInput.includes("重复") || lowerInput.includes("dedup")) {
                steps.push({
                    id: `step-dedup-${Date.now() + 2}`,
                    type: "DEDUPLICATE",
                    label: "Paragraph Deduplication",
                    description: "Applying Set-based or MinHash deduplication to remove duplicate boilerplate text blocks.",
                    status: "pending",
                    codeSnippet: `ds = ds.groupby("text_hash").first()`
                });
            }

            if (lowerInput.includes("质检") || lowerInput.includes("品质") || lowerInput.includes("过滤") || lowerInput.includes("check")) {
                steps.push({
                    id: `step-quality-${Date.now() + 3}`,
                    type: "QUALITY_CHECK",
                    label: "LangIDs & Heuristics Check",
                    description: "Filtering out badly formatted text chunks, checking for extremely short bodies and perplexity scoring.",
                    status: "pending",
                    codeSnippet: `def _quality_filter(row):\n    if len(row['text']) < 50: return False\n    return True\n\nds = ds.filter(_quality_filter)`
                });
            }

            if (lowerInput.includes("语料") || lowerInput.includes("分段") || lowerInput.includes("截断") || lowerInput.includes("jsonl") || lowerInput.includes("chunk")) {
                steps.push({
                    id: `step-corpus-${Date.now() + 4}`,
                    type: "GENERATE_CORPUS",
                    label: "LLM Corpus Chunking",
                    description: "Slicing full documents into standard 1024-token chunks and formatting into JSONL for HuggingFace pre-training datasets.",
                    status: "pending",
                    codeSnippet: `def _chunker(row):\n    return split_by_tokens(row['text'], max_tokens=1024)\n\nds = ds.flat_map(_chunker)\nds.write_json("s3://corpus/batch1/")`
                });
            }
        }
        
        if (lowerInput.includes("load") || lowerInput.includes("s3") || lowerInput.includes("read")) {
            steps.push({
                id: "step-1",
                type: "LOAD_DATA",
                label: "Load S3 Data",
                description: "Reading parquet files from s3://customer-logs/2024/*",
                status: "pending",
                codeSnippet: `ds = ray.data.read_parquet("s3://customer-logs/2024/")`
            });
        }
        
        if (lowerInput.includes("pii") || lowerInput.includes("scan") || lowerInput.includes("sensitive")) {
            steps.push({
                id: "step-2",
                type: "SCAN_PII",
                label: "Scan for PII",
                description: "Running Presidio Analyzer on 'email' and 'message' columns",
                status: "pending",
                codeSnippet: `ds = ds.map_batches(PIIScanner, compute=ray.data.ActorPoolStrategy())`
            });
        }

        if (lowerInput.includes("clean") || lowerInput.includes("transform") || lowerInput.includes("filter")) {
            steps.push({
                id: "step-3",
                type: "TRANSFORM",
                label: "Clean Data",
                description: "Filtering null values and standardizing specific columns",
                status: "pending",
                codeSnippet: `ds = ds.filter(lambda x: x['id'] is not None)`
            });
        }

        // Default Step if no keywords matched
        if (steps.length === 0) {
            steps.push({
                id: "step-default",
                type: "INSPECT_SCHEMA",
                label: "Inspect Data",
                description: "Loading sample data to infer schema",
                status: "pending",
                codeSnippet: `ds.show(limit=5)`
            });
        }

        return {
            id: `plan-${Date.now()}`,
            goal: userInput,
            steps: steps,
            createdAt: Date.now()
        };
    }
}
