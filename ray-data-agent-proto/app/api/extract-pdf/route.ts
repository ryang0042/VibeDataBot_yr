import { NextRequest, NextResponse } from "next/server";
import { execFile } from "child_process";
import path from "path";
import { promisify } from "util";

const execFileAsync = promisify(execFile);

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
    try {
        const body = await req.json();
        const { filePath, keepIntermediates = false } = body ?? {};

        if (!filePath) {
            return NextResponse.json(
                { error: true, message: "No filePath provided for extraction." },
                { status: 400 }
            );
        }

        const rootDir = process.cwd();
        const pythonCommand = getPythonCommand();

        const args = ["-m", "atomic_ability_pdf_extractor.cli", "--file-path", filePath];
        if (keepIntermediates) {
            args.push("--keep-intermediates");
        }

        const { stdout, stderr } = await execFileAsync(pythonCommand, args, {
            cwd: rootDir,
            env: { ...process.env, PYTHONPATH: rootDir },
            maxBuffer: 1024 * 1024 * 20,
        });

        if (stderr?.trim()) {
            console.warn("[API] extract-pdf stderr:", stderr);
        }

        const result = JSON.parse(stdout.trim());
        if (result.error) {
            return NextResponse.json(result, { status: 500 });
        }

        return NextResponse.json(result, { status: 200 });
    } catch (error: unknown) {
        console.error("Failed to run PDF extraction API:", error);
        return NextResponse.json(
            { error: true, message: getErrorMessage(error) || "Unknown execution error" },
            { status: 500 }
        );
    }
}

function getErrorMessage(error: unknown) {
    if (error instanceof Error) {
        return error.message;
    }
    return String(error);
}
