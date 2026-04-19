import { NextRequest, NextResponse } from "next/server";
import { execFile } from "child_process";
import { promises as fs } from "fs";
import os from "os";
import path from "path";
import { promisify } from "util";

const execFileAsync = promisify(execFile);

type OutputProfile = "compact" | "standard" | "full";

function getPythonCommand() {
    if (process.env.PYTHON_BIN) {
        return process.env.PYTHON_BIN;
    }

    if (process.env.VIRTUAL_ENV) {
        return process.platform === "win32"
            ? path.join(process.env.VIRTUAL_ENV, "Scripts", "python.exe")
            : path.join(process.env.VIRTUAL_ENV, "bin", "python");
    }

    return "python";
}

export async function POST(req: NextRequest) {
    let tempDir: string | null = null;

    try {
        const body = await req.json();
        const {
            filePath,
            text,
            sourceType = "pdf_text",
            extractMode = "direct",
            ocrConfidence,
            outputProfile = "standard",
            includeInputMeta = true,
            includeDependencyReport = false,
            includeNormalizedTextLength = true,
            includeScoreDetail = outputProfile !== "compact",
            includeChunkMetrics = false,
            includeChunkMeta = false,
            inputMeta = {},
            strictDeps = false,
        } = body ?? {};

        if (!filePath && typeof text !== "string") {
            return NextResponse.json(
                { error: true, message: "Either filePath or text must be provided for quality evaluation." },
                { status: 400 }
            );
        }

        const rootDir = process.cwd();
        const pythonCommand = getPythonCommand();
        const args: string[] = [
            "-m",
            "atomic_ability_evaluate.cli",
            "--source-type",
            sourceType,
            "--extract-mode",
            extractMode,
            "--output-profile",
            normalizeOutputProfile(outputProfile),
        ];

        if (ocrConfidence !== undefined && ocrConfidence !== null) {
            args.push("--ocr-confidence", String(ocrConfidence));
        }
        if (includeInputMeta) {
            args.push("--include-input-meta");
        }
        if (includeDependencyReport) {
            args.push("--include-dependency-report");
        }
        if (includeNormalizedTextLength) {
            args.push("--include-normalized-text-length");
        }
        if (includeScoreDetail) {
            args.push("--include-score-detail");
        }
        if (includeChunkMetrics) {
            args.push("--include-chunk-metrics");
        }
        if (includeChunkMeta) {
            args.push("--include-chunk-meta");
        }
        if (strictDeps) {
            args.push("--strict-deps");
        }

        if (filePath) {
            args.push("--input-path", filePath);
        } else {
            tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "vibedatabot-quality-"));
            const payloadPath = path.join(tempDir, "quality-input.json");
            await fs.writeFile(
                payloadPath,
                JSON.stringify(
                    {
                        text,
                        meta: {
                            ...inputMeta,
                            source_type: sourceType,
                            extract_mode: extractMode,
                            ocr_confidence: ocrConfidence ?? null,
                        },
                    },
                    null,
                    2
                ),
                "utf-8"
            );
            args.push("--input-path", payloadPath);
        }

        const { stdout, stderr } = await execFileAsync(pythonCommand, args, {
            cwd: rootDir,
            env: { ...process.env, PYTHONPATH: rootDir },
            maxBuffer: 1024 * 1024 * 10,
        });

        if (stderr?.trim()) {
            console.warn("[API] quality-evaluate stderr:", stderr);
        }

        const payload = JSON.parse(stdout.trim());
        const normalized = Array.isArray(payload) ? payload[0] : payload;
        return NextResponse.json(normalized, { status: 200 });
    } catch (error: unknown) {
        console.error("Failed to run quality evaluation API:", error);
        return NextResponse.json(
            { error: true, message: getErrorMessage(error) || "Unknown quality evaluation error" },
            { status: 500 }
        );
    } finally {
        if (tempDir) {
            await fs.rm(tempDir, { recursive: true, force: true }).catch(() => undefined);
        }
    }
}

function normalizeOutputProfile(profile: string): OutputProfile {
    if (profile === "compact" || profile === "full" || profile === "standard") {
        return profile;
    }
    return "standard";
}

function getErrorMessage(error: unknown) {
    if (error instanceof Error) {
        return error.message;
    }
    return String(error);
}
