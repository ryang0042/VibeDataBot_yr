"use client";

import React from "react";
import { cn } from "@/lib/utils";

interface DataFrameProps {
    data: any[];
    columns: string[];
    highlightColumns?: string[];
    title?: string;
    className?: string;
}

export function DataFrame({ data, columns, highlightColumns = [], title, className }: DataFrameProps) {
    return (
        <div className={cn("rounded-lg border border-border overflow-hidden bg-card shadow-sm", className)}>
            {title && (
                <div className="bg-muted/30 px-4 py-2 border-b border-border text-xs font-semibold uppercase tracking-wider text-muted-foreground flex items-center justify-between">
                    <span>{title}</span>
                    <span className="text-[10px] bg-primary/10 text-primary px-1.5 py-0.5 rounded">
                        {data.length} rows
                    </span>
                </div>
            )}
            <div className="overflow-x-auto">
                <table className="w-full text-sm text-left">
                    <thead className="text-xs text-muted-foreground uppercase bg-muted/50">
                        <tr>
                            {columns.map((col) => (
                                <th key={col} className={cn(
                                    "px-4 py-3 font-medium whitespace-nowrap",
                                    highlightColumns.includes(col) && "text-primary bg-primary/5"
                                )}>
                                    {col}
                                </th>
                            ))}
                        </tr>
                    </thead>
                    <tbody className="divide-y divide-border/50">
                        {data.map((row, i) => (
                            <tr key={i} className="hover:bg-muted/30 transition-colors bg-card">
                                {columns.map((col) => (
                                    <td key={`${i}-${col}`} className={cn(
                                        "px-4 py-3 font-mono text-xs whitespace-nowrap max-w-[200px] overflow-hidden text-ellipsis",
                                        highlightColumns.includes(col) && "bg-primary/5 text-foreground font-medium"
                                    )}>
                                        {renderCell(row[col])}
                                    </td>
                                ))}
                            </tr>
                        ))}
                    </tbody>
                </table>
            </div>
        </div>
    );
}

function renderCell(value: any) {
    if (value === null || value === undefined) return <span className="text-muted-foreground/50 italic">null</span>;
    if (typeof value === "boolean") return value ? "true" : "false";
    if (typeof value === "object") return JSON.stringify(value);
    return String(value);
}
