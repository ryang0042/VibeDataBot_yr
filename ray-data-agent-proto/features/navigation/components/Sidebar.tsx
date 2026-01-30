"use client";

import React, { useState } from "react";
import { cn } from "@/lib/utils";
import { useAgent, Resource } from "@/features/agent/context/AgentContext";
import {
    FolderGit2,
    Database,
    Server,
    Settings,
    ChevronLeft,
    ChevronRight,
    Hexagon,
    ChevronDown,
    Cloud,
    HardDrive,
    Activity,
    Cpu
} from "lucide-react";

interface SidebarProps {
    isOpen: boolean;
    toggle: () => void;
}

export function Sidebar({ isOpen, toggle }: SidebarProps) {
    const { setSelectedResource, selectedResource } = useAgent();

    const handleResourceClick = (res: Resource) => {
        setSelectedResource(res);
    };

    return (
        <div
            className={cn(
                "h-full border-r border-border bg-card/50 backdrop-blur-sm transition-all duration-300 flex flex-col z-40",
                isOpen ? "w-64" : "w-16"
            )}
        >
            {/* Header */}
            <div className="flex h-14 items-center px-4 border-b border-border/50">
                <div className="flex items-center gap-2 text-primary font-bold overflow-hidden whitespace-nowrap cursor-pointer" onClick={() => setSelectedResource(null)}>
                    <Hexagon size={24} className="shrink-0 animate-pulse" />
                    <span className={cn("transition-opacity duration-200", isOpen ? "opacity-100" : "opacity-0 hidden")}>
                        VibeDataBot
                    </span>
                </div>
            </div>

            {/* Nav Items */}
            <div className="flex-1 py-4 flex flex-col gap-1 px-2 overflow-y-auto scrollbar-hide">
                <NavItem icon={<FolderGit2 size={20} />} label="Projects" isOpen={isOpen} active={!selectedResource} onClick={() => setSelectedResource(null)} />

                <NavSection
                    icon={<Database size={20} />}
                    label="Data Sources"
                    isOpen={isOpen}
                >
                    <div className="flex flex-col gap-1 mt-1 ml-2 border-l border-border/50 pl-2">
                        <div className="text-[10px] transform uppercase tracking-wider text-muted-foreground/50 font-semibold px-2 py-1">Internal</div>
                        <SubNavItem
                            icon={<HardDrive size={14} />}
                            label="Postgres (Prod)"
                            onClick={() => handleResourceClick({ type: "datasource", id: "pg-prod", label: "Postgres (Prod)", subType: "Internal DB", status: "active" })}
                            active={selectedResource?.id === "pg-prod"}
                        />
                        <SubNavItem
                            icon={<Database size={14} />}
                            label="Snowflake (Analytics)"
                            onClick={() => handleResourceClick({ type: "datasource", id: "sf-analytics", label: "Snowflake (Analytics)", subType: "Data Warehouse", status: "active" })}
                            active={selectedResource?.id === "sf-analytics"}
                        />

                        <div className="text-[10px] transform uppercase tracking-wider text-muted-foreground/50 font-semibold px-2 py-1 mt-2">External</div>
                        <SubNavItem
                            icon={<Cloud size={14} />}
                            label="HuggingFace Hub"
                            onClick={() => handleResourceClick({ type: "datasource", id: "hf-hub", label: "HuggingFace Hub", subType: "Public Datasets", status: "active" })}
                            active={selectedResource?.id === "hf-hub"}
                        />
                        <SubNavItem
                            icon={<Cloud size={14} />}
                            label="S3 Public Buckets"
                            onClick={() => handleResourceClick({ type: "datasource", id: "s3-public", label: "S3 Public Buckets", subType: "Object Storage", status: "active" })}
                            active={selectedResource?.id === "s3-public"}
                        />
                    </div>
                </NavSection>

                <NavSection
                    icon={<Server size={20} />}
                    label="Clusters"
                    isOpen={isOpen}
                >
                    <div className="flex flex-col gap-1 mt-1 ml-2 border-l border-border/50 pl-2">
                        <SubNavItem
                            icon={<Activity size={14} />}
                            label="Ray-Cluster-Prod"
                            statusColor="bg-green-500"
                            onClick={() => handleResourceClick({ type: "cluster", id: "ray-prod", label: "Ray-Cluster-Prod", status: "active" })}
                            active={selectedResource?.id === "ray-prod"}
                        />
                        <SubNavItem
                            icon={<Cpu size={14} />}
                            label="Ray-Cluster-Dev"
                            statusColor="bg-yellow-500"
                            onClick={() => handleResourceClick({ type: "cluster", id: "ray-dev", label: "Ray-Cluster-Dev", status: "idle" })}
                            active={selectedResource?.id === "ray-dev"}
                        />
                    </div>
                </NavSection>
            </div>

            {/* Footer Actions */}
            <div className="p-2 border-t border-border/50 flex flex-col gap-1">
                <NavItem icon={<Settings size={20} />} label="Settings" isOpen={isOpen} />
                <button
                    onClick={toggle}
                    className="flex items-center justify-center h-10 w-full hover:bg-muted/50 rounded-md transition-colors text-muted-foreground mt-2"
                >
                    {isOpen ? <ChevronLeft size={20} /> : <ChevronRight size={20} />}
                </button>
            </div>
        </div>
    );
}

function NavItem({ icon, label, isOpen, active, onClick }: { icon: React.ReactNode; label: string; isOpen: boolean; active?: boolean; onClick?: () => void }) {
    return (
        <button
            onClick={onClick}
            className={cn(
                "flex items-center gap-3 px-3 py-2 rounded-md transition-all duration-200 group overflow-hidden whitespace-nowrap text-sm w-full font-medium",
                active
                    ? "bg-primary/10 text-primary hover:bg-primary/20"
                    : "text-muted-foreground hover:bg-muted hover:text-foreground"
            )}
        >
            <div className="shrink-0">{icon}</div>
            <span className={cn("transition-all duration-200", isOpen ? "opacity-100 translate-x-0" : "opacity-0 -translate-x-4")}>
                {label}
            </span>
        </button>
    );
}

function NavSection({ icon, label, isOpen, children }: { icon: React.ReactNode; label: string; isOpen: boolean; children: React.ReactNode }) {
    const [expanded, setExpanded] = useState(true);

    if (!isOpen) {
        return (
            <div className="group relative">
                <button className="flex items-center gap-3 px-3 py-2 rounded-md text-muted-foreground hover:bg-muted hover:text-foreground transition-all duration-200 w-full justify-center">
                    <div className="shrink-0">{icon}</div>
                </button>
            </div>
        )
    }

    return (
        <div className="flex flex-col gap-1">
            <button
                onClick={() => setExpanded(!expanded)}
                className="flex items-center justify-between px-3 py-2 rounded-md text-muted-foreground hover:bg-muted hover:text-foreground transition-all duration-200 w-full text-sm font-medium group"
            >
                <div className="flex items-center gap-3">
                    <div className="shrink-0">{icon}</div>
                    <span>{label}</span>
                </div>
                <ChevronDown size={14} className={cn("transition-transform duration-200", expanded ? "" : "-rotate-90")} />
            </button>
            <div className={cn("grid transition-all duration-300 ease-in-out overflow-hidden", expanded ? "grid-rows-[1fr] opacity-100" : "grid-rows-[0fr] opacity-0")}>
                <div className="min-h-0">
                    {children}
                </div>
            </div>
        </div>
    );
}

function SubNavItem({ icon, label, active, onClick, statusColor }: { icon: React.ReactNode; label: string; active?: boolean; onClick?: () => void; statusColor?: string }) {
    return (
        <button
            onClick={onClick}
            className={cn(
                "flex items-center gap-2 px-3 py-1.5 rounded-md transition-all duration-200 group overflow-hidden whitespace-nowrap text-xs w-full text-left",
                active
                    ? "bg-primary/10 text-primary"
                    : "text-muted-foreground hover:text-foreground hover:bg-muted/50"
            )}
        >
            <div className="shrink-0 flex items-center justify-center w-4">
                {icon}
            </div>
            <span className="truncate flex-1">{label}</span>
            {statusColor && <div className={cn("w-1.5 h-1.5 rounded-full shrink-0", statusColor)} />}
        </button>
    )
}
