import { useState } from 'react';
import {
    BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer,
    AreaChart, Area, ScatterChart, Scatter, ZAxis, Legend, LineChart, Line
} from 'recharts';
import {
    TrendingUp, Users, FileText, CheckCircle, AlertCircle,
    Clock, Server, Database, Activity, Info, ExternalLink, Calendar,
    ChevronUp, ArrowRight
} from 'lucide-react';
import data from './analytics_data.json';

// --- COMPONENTS ---

const Card = ({ children, className = "" }) => (
    <div className={`bg-white rounded-xl shadow-sm border border-slate-200 p-6 ${className}`}>
        {children}
    </div>
);

const StatCard = ({ title, value, subtext, icon: Icon, color = "blue", trend }) => (
    <Card className="relative overflow-hidden group hover:shadow-md transition-shadow">
        <div className={`absolute top-0 right-0 p-4 opacity-5 group-hover:opacity-10 transition-opacity text-${color}-600`}>
            <Icon size={48} />
        </div>
        <div className="flex items-start justify-between relative z-10">
            <div>
                <p className="text-sm font-medium text-slate-500 mb-1">{title}</p>
                <h3 className="text-3xl font-bold text-slate-900 tracking-tight">{value}</h3>
                {subtext && <p className="text-xs text-slate-400 mt-2">{subtext}</p>}

                {trend && (
                    <div className="flex items-center mt-2 text-xs font-medium text-green-600">
                        <TrendingUp size={12} className="mr-1" />
                        {trend}
                    </div>
                )}
            </div>
            <div className={`p-3 rounded-xl bg-${color}-50 text-${color}-600 ring-1 ring-${color}-100`}>
                <Icon size={22} />
            </div>
        </div>
    </Card>
);

const SectionHeader = ({ title, icon: Icon, color = "slate" }) => (
    <div className="flex items-center space-x-2 mb-4 px-1">
        <Icon className={`text-${color}-600`} size={24} />
        <h2 className="text-2xl font-bold text-slate-800 tracking-tight">{title}</h2>
    </div>
);

const ProgressBar = ({ value, max, color = "emerald" }) => {
    const percentage = Math.min((value / max) * 100, 100);
    return (
        <div className="w-full bg-slate-100 rounded-full h-1.5 mt-2 overflow-hidden">
            <div
                className={`bg-${color}-500 h-1.5 rounded-full`}
                style={{ width: `${percentage}%` }}
            />
        </div>
    );
};

export default function Dashboard() {
    const { overview, daily, analytics, pipeline } = data;

    // --- TRANSFORM DATA ---
    // Prepare history data for simple sparkline
    const historyData = daily.history ? daily.history.map(d => ({
        date: d.date.slice(5), // MD only
        value: d.value
    })) : [];

    return (
        <div className="min-h-screen bg-slate-50/50 p-6 md:p-12 font-sans text-slate-900">
            <div className="max-w-7xl mx-auto space-y-12">

                {/* HEADER */}
                <header className="flex flex-col md:flex-row md:items-end justify-between border-b border-slate-200 pb-6">
                    <div>
                        <h1 className="text-4xl font-extrabold text-slate-900 tracking-tight">Petition Analytics</h1>
                        <p className="text-slate-500 mt-2 text-lg">Ukrainian E-Petitions Dashboard (President & Cabinet)</p>
                    </div>
                    <div className="mt-4 md:mt-0 text-right">
                        <div className="inline-flex items-center space-x-2 bg-white px-3 py-1 rounded-full border border-slate-200 shadow-sm text-sm text-slate-600">
                            <Clock size={14} className="text-slate-400" />
                            <span>Last updated: {pipeline.last_updated}</span>
                        </div>
                    </div>
                </header>

                {/* BLOCK 1: OVERVIEW */}
                <section>
                    <SectionHeader title="High-Level Overview" icon={Activity} color="indigo" />

                    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
                        <StatCard
                            title="Total Petitions"
                            value={overview.total.toLocaleString()}
                            subtext={`Pres: ${overview.president_count.toLocaleString()} • Cab: ${overview.cabinet_count.toLocaleString()}`}
                            icon={FileText}
                            color="indigo"
                        />
                        <StatCard
                            title="Success Rate"
                            value={`${overview.success_rate}%`}
                            subtext="Reached 25,000 votes"
                            icon={CheckCircle}
                            color="emerald"
                        />
                        <StatCard
                            title="Median Votes"
                            value={overview.median_votes}
                            subtext="50% of petitions get less"
                            icon={Users}
                            color="amber"
                        />
                        <StatCard
                            title="Response Rate"
                            value={`${overview.response_rate}%`}
                            subtext="Received official answer"
                            icon={Info}
                            color="cyan"
                        />
                    </div>
                </section>

                {/* BLOCK 2: DAILY DYNAMICS */}
                <section>
                    <SectionHeader title="Daily Dynamics" icon={TrendingUp} color="emerald" />

                    <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
                        {/* 2.1 Today's Pulse */}
                        <Card className="bg-gradient-to-br from-emerald-50 to-white border-emerald-100 relative overflow-hidden">
                            <h3 className="text-lg font-bold text-emerald-900 mb-6 flex items-center">
                                <Calendar size={18} className="mr-2 opacity-75" /> Last 24 Hours
                            </h3>

                            <div className="grid grid-cols-2 gap-4 mb-8 relative z-10">
                                <div>
                                    <p className="text-sm text-emerald-600 font-medium">New Petitions</p>
                                    <p className="text-4xl font-extrabold text-emerald-700">+{daily.new_petitions}</p>
                                </div>
                                <div>
                                    <p className="text-sm text-emerald-600 font-medium">Votes Added</p>
                                    <p className="text-4xl font-extrabold text-emerald-700">+{daily.votes_added.toLocaleString()}</p>
                                </div>
                            </div>

                            {/* Sparkline Area */}
                            <div className="h-32 -mx-6 -mb-6 mt-4">
                                {historyData.length > 0 ? (
                                    <ResponsiveContainer width="100%" height="100%">
                                        <AreaChart data={historyData}>
                                            <defs>
                                                <linearGradient id="colorVote" x1="0" y1="0" x2="0" y2="1">
                                                    <stop offset="5%" stopColor="#10b981" stopOpacity={0.2} />
                                                    <stop offset="95%" stopColor="#10b981" stopOpacity={0} />
                                                </linearGradient>
                                            </defs>
                                            <Tooltip
                                                contentStyle={{ borderRadius: '8px', border: 'none', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)' }}
                                                labelStyle={{ color: '#6b7280' }}
                                                itemStyle={{ color: '#059669', fontWeight: 'bold' }}
                                            />
                                            <Area type="monotone" dataKey="value" stroke="#10b981" strokeWidth={3} fillOpacity={1} fill="url(#colorVote)" />
                                        </AreaChart>
                                    </ResponsiveContainer>
                                ) : (
                                    <div className="h-full flex items-center justify-center text-emerald-300 text-sm">
                                        Collecting trend data...
                                    </div>
                                )}
                            </div>
                        </Card>

                        {/* 2.2 Biggest Movers Table */}
                        <Card className="lg:col-span-2">
                            <div className="flex items-center justify-between mb-6">
                                <h3 className="text-lg font-bold text-slate-800">Growth Leaders (Top 5)</h3>
                                <span className="px-2 py-1 bg-green-100 text-green-700 text-xs font-bold rounded uppercase tracking-wide">Trending</span>
                            </div>

                            <div className="overflow-x-auto">
                                <table className="w-full text-sm">
                                    <thead className="text-xs text-slate-400 uppercase font-medium bg-slate-50/50">
                                        <tr>
                                            <th className="px-4 py-3 text-left">Petition</th>
                                            <th className="px-4 py-3 text-right">Growth (24h)</th>
                                            <th className="px-4 py-3 text-right">Total Votes</th>
                                            <th className="px-4 py-3 w-32">Progress</th>
                                        </tr>
                                    </thead>
                                    <tbody className="divide-y divide-slate-100">
                                        {daily.biggest_movers.map((m, i) => (
                                            <tr key={i} className="hover:bg-slate-50/80 transition-colors">
                                                <td className="px-4 py-3">
                                                    <a href={m.url} target="_blank" rel="noopener noreferrer" className="font-medium text-slate-900 hover:text-blue-600 line-clamp-1 block" title={m.title}>
                                                        {m.title}
                                                    </a>
                                                    <div className="text-xs text-slate-400 mt-0.5">ID: {m.url.split('/').pop()}</div>
                                                </td>
                                                <td className="px-4 py-3 text-right font-bold text-emerald-600">
                                                    +{m.delta}
                                                </td>
                                                <td className="px-4 py-3 text-right text-slate-600">
                                                    {m.total.toLocaleString()}
                                                </td>
                                                <td className="px-4 py-3">
                                                    <ProgressBar value={m.total} max={25000} />
                                                    <div className="text-[10px] text-slate-400 text-right mt-1">
                                                        {Math.round((m.total / 25000) * 100)}%
                                                    </div>
                                                </td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        </Card>
                    </div>
                </section>

                {/* BLOCK 3: DEEP ANALYTICS */}
                <section>
                    <SectionHeader title="Deep Dive Analytics" icon={Database} color="blue" />

                    <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
                        <Card>
                            <h3 className="text-lg font-bold text-slate-800 mb-2">Vote Distribution</h3>
                            <p className="text-sm text-slate-500 mb-6">How hard is it to get votes?</p>
                            <div className="h-64">
                                <ResponsiveContainer width="100%" height="100%">
                                    <BarChart data={analytics.histogram}>
                                        <XAxis dataKey="bin" tick={{ fontSize: 11 }} axisLine={false} tickLine={false} />
                                        <YAxis axisLine={false} tickLine={false} fontSize={11} />
                                        <Tooltip cursor={{ fill: '#f1f5f9' }} contentStyle={{ borderRadius: '8px', border: 'none', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)' }} />
                                        <Bar dataKey="count" fill="#6366f1" radius={[4, 4, 0, 0]} barSize={40} />
                                    </BarChart>
                                </ResponsiveContainer>
                            </div>
                        </Card>

                        <Card>
                            <h3 className="text-lg font-bold text-slate-800 mb-2">Petition Timeline</h3>
                            <p className="text-sm text-slate-500 mb-6">Creation volume over time</p>
                            <div className="h-64">
                                <ResponsiveContainer width="100%" height="100%">
                                    <AreaChart data={analytics.timeline}>
                                        <defs>
                                            <linearGradient id="colorPres" x1="0" y1="0" x2="0" y2="1">
                                                <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.8} />
                                                <stop offset="95%" stopColor="#3b82f6" stopOpacity={0} />
                                            </linearGradient>
                                            <linearGradient id="colorCab" x1="0" y1="0" x2="0" y2="1">
                                                <stop offset="5%" stopColor="#0ea5e9" stopOpacity={0.8} />
                                                <stop offset="95%" stopColor="#0ea5e9" stopOpacity={0} />
                                            </linearGradient>
                                        </defs>
                                        <XAxis dataKey="month" tick={{ fontSize: 10 }} minTickGap={30} axisLine={false} tickLine={false} />
                                        <YAxis axisLine={false} tickLine={false} fontSize={10} />
                                        <Tooltip contentStyle={{ borderRadius: '8px', border: 'none', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)' }} />
                                        <Legend iconType="circle" />
                                        <Area type="monotone" dataKey="president" stackId="1" stroke="#3b82f6" fill="url(#colorPres)" />
                                        <Area type="monotone" dataKey="cabinet" stackId="1" stroke="#0ea5e9" fill="url(#colorCab)" />
                                    </AreaChart>
                                </ResponsiveContainer>
                            </div>
                        </Card>
                    </div>
                </section>

                {/* BLOCK 4: INFO */}
                <footer className="mt-8 border-t border-slate-200 pt-8 text-slate-500 text-sm">
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
                        <div>
                            <h4 className="font-bold text-slate-800 mb-4 flex items-center">
                                <Server size={16} className="mr-2" /> Tech Stack
                            </h4>
                            <ul className="space-y-2">
                                <li>Python ETL (DuckDB)</li>
                                <li>React + Tailwind + Recharts</li>
                                <li>Github Actions (Planned)</li>
                            </ul>
                        </div>
                        <div>
                            <h4 className="font-bold text-slate-800 mb-4 flex items-center">
                                <Database size={16} className="mr-2" /> Data Health
                            </h4>
                            <ul className="space-y-2">
                                <li>Total Records: {pipeline.total_records.toLocaleString()}</li>
                                <li>Active Sources: {pipeline.sources.length}</li>
                            </ul>
                        </div>
                        <div>
                            <h4 className="font-bold text-slate-800 mb-4 flex items-center">
                                <Activity size={16} className="mr-2" /> Roadmap
                            </h4>
                            <p className="text-slate-500 mb-2">Automated daily sync running via cron.</p>
                            <p className="text-indigo-600 font-medium cursor-pointer hover:underline">View Implementation Plan &rarr;</p>
                        </div>
                    </div>
                    <div className="mt-8 pt-8 border-t border-slate-100 text-center text-xs text-slate-400">
                        &copy; 2025 Petition Analytics Project. Open Source.
                    </div>
                </footer>
            </div>
        </div>
    );
}
