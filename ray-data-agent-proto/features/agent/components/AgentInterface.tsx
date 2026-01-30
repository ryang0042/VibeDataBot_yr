"use client";

import React, { useState } from "react";

import { Sidebar } from "@/features/navigation/components/Sidebar";
import { MainCanvas } from "@/features/agent/components/MainCanvas";
import { ChatInterface } from "@/features/agent/components/ChatInterface";
import { AgentProvider } from "@/features/agent/context/AgentContext";

export function AgentInterface() {
    const [sidebarOpen, setSidebarOpen] = useState(true);

    return (
        <AgentProvider>
            <div className="flex h-full w-full bg-background text-foreground">
                {/* Sidebar */}
                <Sidebar isOpen={sidebarOpen} toggle={() => setSidebarOpen(!sidebarOpen)} />

                {/* Main Content Area */}
                <div className="flex flex-1 flex-col overflow-hidden relative transition-all duration-300">
                    <MainCanvas />

                    {/* Floating Chat Interface */}
                    <div className="absolute bottom-6 left-1/2 -translate-x-1/2 w-full max-w-3xl px-4 z-50">
                        <ChatInterface />
                    </div>
                </div>
            </div>
        </AgentProvider>
    );
}
