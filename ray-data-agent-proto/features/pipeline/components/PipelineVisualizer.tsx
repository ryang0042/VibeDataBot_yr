"use client";

import React from "react";
import { cn } from "@/lib/utils";
import { Database, FileJson, ShieldAlert, CheckCircle, ArrowRight } from "lucide-react";

export function PipelineVisualizer({ onNodeClick }: { onNodeClick?: (nodeId: string) => void }) {
    return (
        <div className="w-full h-64 bg-card/40 border border-border/50 rounded-xl relative overflow-hidden flex items-center justify-center p-8">
            {/* Connecting Line */}
            <div className="absolute top-1/2 left-10 right-10 h-0.5 bg-border -translate-y-1/2 z-0" />

            <div className="flex justify-between w-full max-w-2xl relative z-10">
                <PipelineNode
                    icon={<Database size={20} />}
                    label="S3: logs/2024"
                    status="done"
                    onClick={() => onNodeClick?.("source")}
                />
                <PipelineArrow />
                <PipelineNode
                    icon={<FileJson size={20} />}
                    label="Parquet Read"
                    status="done"
                />
                <PipelineArrow />
                <PipelineNode
                    icon={<ShieldAlert size={20} />}
                    label="PII Scan"
                    status="active"
                    onClick={() => onNodeClick?.("pii")}
                />
                <PipelineArrow />
                <PipelineNode
                    icon={<CheckCircle size={20} />}
                    label="S3: clean/"
                    status="pending"
                />
            </div>
        </div>
    );
}

interface PipelineNodeProps {
    icon: React.ReactNode;
    label: string;
    status: "done" | "active" | "pending";
    onClick?: () => void;
}

function PipelineNode({ icon, label, status, onClick }: PipelineNodeProps) {
    return (
        <button
            onClick={onClick}
            disabled={!onClick}
            className={cn(
                "flex flex-col items-center gap-2 group focus:outline-none",
                onClick && "cursor-pointer hover:scale-110 transition-transform"
            )}
        >
            <div className={cn(
                "w-12 h-12 rounded-full border-2 flex items-center justify-center bg-background transition-all duration-500 relative",
                status === "done" && "border-green-500 text-green-500 shadow-[0_0_10px_rgba(34,197,94,0.3)]",
                status === "active" && "border-primary text-primary shadow-[0_0_15px_rgba(59,130,246,0.5)] animate-pulse",
                status === "pending" && "border-muted text-muted-foreground"
            )}>
                {icon}
                {onClick && (
                    <span className="absolute -bottom-8 text-[10px] bg-card border border-border px-1.5 py-0.5 rounded opacity-0 group-hover:opacity-100 transition-opacity whitespace-nowrap text-muted-foreground z-20">
                        View Data
                    </span>
                )}
            </div>
            <span className={cn(
                "text-xs font-medium",
                status === "done" && "text-green-500",
                status === "active" && "text-primary",
                status === "pending" && "text-muted-foreground"
            )}>{label}</span>
        </button>
    );
}

function PipelineArrow() {
    return (
        <div className="flex items-center justify-center text-muted-foreground/30">
            <ArrowRight size={20} />
        </div>
    )
}
