"use client";

import React from "react";
import { Resource } from "@/features/agent/context/AgentContext";
import { Activity, Server, Database, Cloud, ShieldCheck, Clock } from "lucide-react";

export function ResourceDetailView({ resource }: { resource: Resource }) {
    if (resource.type === "cluster") {
        return (
            <div className="flex-1 w-full bg-background relative overflow-hidden p-8">
                <div className="max-w-6xl mx-auto flex flex-col gap-8">
                    <div className="flex items-center justify-between">
                        <div>
                            <h2 className="text-2xl font-bold flex items-center gap-3">
                                <Server className="text-primary" />
                                {resource.label}
                            </h2>
                            <p className="text-muted-foreground">Ray 2.9.0 • US-East-1 • GPU Accelerated</p>
                        </div>
                        <div className="px-3 py-1 rounded-full bg-green-500/20 text-green-500 text-sm font-medium flex items-center gap-2">
                            <div className="w-2 h-2 rounded-full bg-green-500 animate-pulse" />
                            {resource.status?.toUpperCase()}
                        </div>
                    </div>

                    <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                        <StatCard icon={<Activity />} label="CPU Usage" value="45%" sub="32/64 Cores Active" />
                        <StatCard icon={<Activity />} label="GPU Usage" value="12%" sub="2/8 A100s Active" />
                        <StatCard icon={<Server />} label="Active Nodes" value="4" sub="1 Head, 3 Workers" />
                    </div>

                    <div className="rounded-xl border border-border/50 bg-card/30 overflow-hidden">
                        <div className="bg-muted/30 px-4 py-2 border-b border-border/50 font-medium text-sm">Active Jobs</div>
                        <div className="divide-y divide-border/50">
                            {[1, 2, 3].map(i => (
                                <div key={i} className="p-4 flex items-center justify-between hover:bg-muted/10 transition-colors">
                                    <div className="flex flex-col">
                                        <span className="font-mono text-xs text-primary">job_id_{i}02934</span>
                                        <span className="text-sm">PII_Scan_Batch_{i}</span>
                                    </div>
                                    <span className="text-xs text-muted-foreground">Running (2m 14s)</span>
                                </div>
                            ))}
                        </div>
                    </div>
                </div>
            </div>
        )
    }

    // Datasource View
    return (
        <div className="flex-1 w-full bg-background relative overflow-hidden p-8">
            <div className="max-w-6xl mx-auto flex flex-col gap-8">
                <div className="flex items-center justify-between">
                    <div>
                        <h2 className="text-2xl font-bold flex items-center gap-3">
                            {resource.id.includes("hf") ? <Cloud className="text-blue-400" /> : <Database className="text-primary" />}
                            {resource.label}
                        </h2>
                        <p className="text-muted-foreground">{resource.subType} • Connected via Ray Data</p>
                    </div>
                    <div className="px-3 py-1 rounded-full bg-green-500/20 text-green-500 text-sm font-medium flex items-center gap-2">
                        <ShieldCheck size={14} />
                        Verified
                    </div>
                </div>

                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                    <StatCard icon={<Database />} label="Total Size" value="2.4 TB" sub="1.2B Records" />
                    <StatCard icon={<Clock />} label="Last Sync" value="10m ago" sub="Auto-sync enabled" />
                </div>

                <div className="rounded-xl border border-border/50 bg-card/30 overflow-hidden">
                    <div className="bg-muted/30 px-4 py-2 border-b border-border/50 font-medium text-sm flex justify-between items-center">
                        <span>Schema Preview (First 5 Columns)</span>
                        <button className="text-xs text-primary hover:underline">View Full Schema</button>
                    </div>
                    <div className="p-0">
                        <table className="w-full text-sm text-left">
                            <thead className="text-xs text-muted-foreground uppercase bg-muted/50">
                                <tr>
                                    <th className="px-4 py-3">Column Name</th>
                                    <th className="px-4 py-3">Type</th>
                                    <th className="px-4 py-3">Nullable</th>
                                </tr>
                            </thead>
                            <tbody className="divide-y divide-border/50">
                                <tr><td className="px-4 py-3 font-mono text-xs">user_id</td><td className="px-4 py-3 text-muted-foreground">VARCHAR(36)</td><td className="px-4 py-3 text-muted-foreground">NO</td></tr>
                                <tr><td className="px-4 py-3 font-mono text-xs">created_at</td><td className="px-4 py-3 text-muted-foreground">TIMESTAMP</td><td className="px-4 py-3 text-muted-foreground">NO</td></tr>
                                <tr><td className="px-4 py-3 font-mono text-xs">metadata</td><td className="px-4 py-3 text-muted-foreground">JSONB</td><td className="px-4 py-3 text-muted-foreground">YES</td></tr>
                                <tr><td className="px-4 py-3 font-mono text-xs">email</td><td className="px-4 py-3 text-muted-foreground">VARCHAR(255)</td><td className="px-4 py-3 text-muted-foreground">YES</td></tr>
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        </div>
    )
}

function StatCard({ icon, label, value, sub }: { icon: React.ReactNode; label: string; value: string; sub: string }) {
    return (
        <div className="p-4 rounded-xl border border-border/50 bg-card/50 flex flex-col gap-1">
            <div className="flex items-center gap-2 text-muted-foreground text-xs font-medium uppercase tracking-wider">
                {icon} {label}
            </div>
            <div className="text-2xl font-bold">{value}</div>
            <div className="text-xs text-muted-foreground">{sub}</div>
        </div>
    )
}
