"use client";

import React, { useState, useEffect } from "react";
import { ChevronRight, Folder, File, FileText, Loader2, ArrowUp, House } from "lucide-react";
import { cn } from "@/lib/utils";
import { useAgent } from "@/features/agent/context/AgentContext";

interface FileNode {
    name: string;
    path: string;
    isDirectory: boolean;
}

interface FileTreeProps {
    initialDir?: string;
}

const DEFAULT_WORKSPACE_LABEL = "VibeDataBot-main";

function getBaseName(targetPath: string) {
    const trimmed = targetPath.replace(/[\\/]+$/, "");
    if (!trimmed) {
        return DEFAULT_WORKSPACE_LABEL;
    }
    const parts = trimmed.split(/[\\/]/);
    return parts[parts.length - 1] || DEFAULT_WORKSPACE_LABEL;
}

function getParentDir(targetPath: string) {
    const trimmed = targetPath.replace(/[\\/]+$/, "");
    if (!trimmed) {
        return "";
    }

    const isWindowsDriveRoot = /^[A-Za-z]:$/.test(trimmed);
    if (isWindowsDriveRoot || trimmed === "/" || trimmed === "\\") {
        return trimmed;
    }

    const parts = trimmed.split(/[\\/]/);
    if (parts.length <= 1) {
        return trimmed;
    }

    const parentParts = parts.slice(0, -1);
    if (parentParts.length === 1 && parentParts[0] === "") {
        return "/";
    }
    return parentParts.join(trimmed.includes("\\") ? "\\" : "/");
}

export function FileTree({ initialDir = "" }: FileTreeProps) {
    const [rootPath, setRootPath] = useState(initialDir);
    const [resolvedRootPath, setResolvedRootPath] = useState<string>("");
    const [workspaceRoot, setWorkspaceRoot] = useState<string>("");

    const effectiveRootPath = rootPath || initialDir;
    const canGoUp = Boolean(resolvedRootPath) && getParentDir(resolvedRootPath) !== resolvedRootPath;
    const showReturnToWorkspace = Boolean(workspaceRoot) && resolvedRootPath !== workspaceRoot;

    return (
        <div className="w-full text-sm text-foreground/80 p-2 max-h-[400px] overflow-hidden flex flex-col gap-2">
            <div className="rounded-md border border-border/50 bg-background/50 px-2 py-2 flex items-center gap-1.5">
                <button
                    type="button"
                    onClick={() => {
                        if (resolvedRootPath) {
                            setRootPath(getParentDir(resolvedRootPath));
                        }
                    }}
                    disabled={!canGoUp}
                    className="h-7 w-7 rounded-md flex items-center justify-center text-muted-foreground hover:bg-muted/70 hover:text-foreground disabled:opacity-40 disabled:cursor-not-allowed"
                    title="Go up one directory"
                >
                    <ArrowUp size={14} />
                </button>
                <button
                    type="button"
                    onClick={() => {
                        if (workspaceRoot) {
                            setRootPath(workspaceRoot);
                        } else {
                            setRootPath(initialDir);
                        }
                    }}
                    disabled={!showReturnToWorkspace}
                    className="h-7 w-7 rounded-md flex items-center justify-center text-muted-foreground hover:bg-muted/70 hover:text-foreground disabled:opacity-40 disabled:cursor-not-allowed"
                    title="Return to workspace root"
                >
                    <House size={14} />
                </button>
                <div className="min-w-0 flex-1">
                    <div className="text-[10px] uppercase tracking-wider text-muted-foreground/60">Current Root</div>
                    <div className="truncate text-xs font-medium">{resolvedRootPath || DEFAULT_WORKSPACE_LABEL}</div>
                </div>
            </div>

            <div className="w-full flex-1 overflow-y-auto scrollbar-thin">
                <TreeNode
                    key={effectiveRootPath || "__workspace_root__"}
                    path={effectiveRootPath}
                    name=""
                    isDirectory={true}
                    isRoot={true}
                    defaultExpanded={true}
                    onResolvedPath={(resolvedPath) => {
                        setResolvedRootPath(resolvedPath);
                        if (!workspaceRoot) {
                            setWorkspaceRoot(resolvedPath);
                        }
                    }}
                />
            </div>
        </div>
    );
}

function TreeNode({
    path,
    name,
    isDirectory,
    isRoot,
    defaultExpanded = false,
    onResolvedPath,
}: {
    path: string;
    name: string;
    isDirectory: boolean;
    isRoot?: boolean;
    defaultExpanded?: boolean;
    onResolvedPath?: (resolvedPath: string) => void;
}) {
    const [expanded, setExpanded] = useState(defaultExpanded);
    const [children, setChildren] = useState<FileNode[]>([]);
    const [loading, setLoading] = useState(false);
    const [hasFetched, setHasFetched] = useState(false);
    const [resolvedPath, setResolvedPath] = useState(path);
    
    // Auto-inject context
    const { setChatInput } = useAgent();

    const fetchFolder = async (dirPath: string) => {
        setLoading(true);
        try {
            const res = await fetch(`/api/fs?dir=${encodeURIComponent(dirPath)}`);
            if (res.ok) {
                const data = await res.json();
                setChildren(data.items || []);
                if (typeof data.path === "string") {
                    setResolvedPath(data.path);
                    onResolvedPath?.(data.path);
                }
            }
        } catch (e) {
            console.error("Failed to load fs", e);
        } finally {
            setLoading(false);
            setHasFetched(true);
        }
    };

    useEffect(() => {
        if (expanded && !hasFetched && isDirectory) {
            fetchFolder(path);
        }
    }, [expanded, path, isDirectory, hasFetched]);

    const handleToggle = () => {
        if (isDirectory) {
            setExpanded(!expanded);
        } else {
            // It's a file!
            if (name.toLowerCase().endsWith(".pdf")) {
                setChatInput(`提取该 PDF 文件内容，路径是：${path}`);
            } else {
                setChatInput(`载入本地文件：${path}`);
            }
        }
    };

    const getIcon = () => {
        if (isDirectory) {
            return expanded ? <Folder size={14} className="text-primary/70 fill-primary/20" /> : <Folder size={14} className="text-muted-foreground" />;
        }
        if (name.toLowerCase().endsWith(".pdf")) {
            return <FileText size={14} className="text-red-400" />;
        }
        return <File size={14} className="text-muted-foreground/70" />;
    };

    const displayName = isRoot ? getBaseName(resolvedPath) : name;

    return (
        <div className="flex flex-col">
            <div
                className={cn(
                    "flex items-center gap-1.5 py-1 px-1 hover:bg-muted/50 rounded cursor-pointer select-none transition-colors",
                    isRoot && "font-semibold mb-1"
                )}
                onClick={handleToggle}
            >
                {/* Expand Indicator */}
                <div className="w-4 flex justify-center shrink-0">
                    {isDirectory && (
                        <div className={cn("text-muted-foreground transition-transform", expanded && "rotate-90")}>
                            {loading ? <Loader2 size={12} className="animate-spin" /> : <ChevronRight size={14} />}
                        </div>
                    )}
                </div>

                {/* File/Folder Icon */}
                {getIcon()}

                {/* Label */}
                <span className="truncate flex-1">{displayName}</span>
            </div>

            {/* Children List */}
            {expanded && isDirectory && (
                <div className="ml-3 pl-2 border-l border-border/30 flex flex-col gap-0.5 mt-0.5">
                    {children.map((child, i) => (
                        <TreeNode
                            key={`${child.path}-${i}`}
                            path={child.path}
                            name={child.name}
                            isDirectory={child.isDirectory}
                            onResolvedPath={undefined}
                        />
                    ))}
                    {hasFetched && children.length === 0 && (
                        <div className="text-xs text-muted-foreground/50 italic px-4 py-1">Empty directory</div>
                    )}
                </div>
            )}
        </div>
    );
}
