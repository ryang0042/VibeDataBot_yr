import { NextRequest, NextResponse } from "next/server";
import fs from "fs";
import path from "path";

function normalizeIncomingPath(filePath: string) {
    let cleanPath = filePath.trim();

    if (cleanPath.startsWith("file://")) {
        try {
            const url = new URL(cleanPath);
            let pathname = decodeURIComponent(url.pathname);

            if (process.platform === "win32") {
                if (pathname.startsWith("/") && /^[A-Za-z]:/.test(pathname.slice(1))) {
                    pathname = pathname.slice(1);
                }
                pathname = pathname.replace(/\//g, "\\");
            }

            return pathname;
        } catch {
            cleanPath = cleanPath.replace(/^file:\/\//, "");
        }
    }

    if (cleanPath.startsWith("local://")) {
        cleanPath = cleanPath.replace(/^local:\/\//, "");
    } else if (cleanPath.startsWith("local:")) {
        cleanPath = cleanPath.replace(/^local:/, "");
    }

    if (process.platform === "win32" && cleanPath.startsWith("/") && /^[A-Za-z]:/.test(cleanPath.slice(1))) {
        cleanPath = cleanPath.slice(1);
    }

    return cleanPath;
}

export async function GET(request: NextRequest) {
    const searchParams = request.nextUrl.searchParams;
    const filePath = searchParams.get("path");

    if (!filePath) {
        return NextResponse.json({ error: "Missing 'path' parameter" }, { status: 400 });
    }

    try {
        const cleanPath = normalizeIncomingPath(filePath);
        const resolvedPath = path.resolve(cleanPath);

        // Security check: ensure the file exists and is a file
        const stat = await fs.promises.stat(resolvedPath);
        if (!stat.isFile()) {
            return NextResponse.json({ error: "Provided path is not a valid file" }, { status: 400 });
        }

        // Extremely basic content type mapping (Expandable for images later if needed)
        let contentType = "application/octet-stream";
        const ext = path.extname(resolvedPath).toLowerCase();
        if (ext === ".pdf") contentType = "application/pdf";
        else if (ext === ".jpg" || ext === ".jpeg") contentType = "image/jpeg";
        else if (ext === ".png") contentType = "image/png";

        // Read the file as a buffer to send as response body
        // Note: For very large files in Next.js App Router, using Streams is preferred
        // but for PDF previewing (usually < 20MB) a direct buffer is often reliable enough
        const fileBuffer = await fs.promises.readFile(resolvedPath);

        return new NextResponse(fileBuffer, {
            status: 200,
            headers: {
                "Content-Type": contentType,
                "Content-Length": stat.size.toString(),
                // Tell browser to display inline (don't force a download dialog)
                "Content-Disposition": `inline; filename="${path.basename(resolvedPath)}"`,
            },
        });
    } catch (error: unknown) {
        console.error("Local File Proxy Error:", error);
        const details = error instanceof Error ? error.message : String(error);
        return NextResponse.json(
            { error: "Failed to read file", details },
            { status: 500 }
        );
    }
}
