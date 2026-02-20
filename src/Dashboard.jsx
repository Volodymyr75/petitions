import { useState, useEffect } from 'react';
import {
    BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer,
    AreaChart, Area, ScatterChart, Scatter, ZAxis, Legend,
    Cell, ReferenceLine
} from 'recharts';
import {
    TrendingUp, Users, FileText, CheckCircle,
    Clock, Server, Database, Activity, Info, ExternalLink, Calendar,
    ArrowRight, Sparkles, BarChart3,
    Zap, Layers, Sun, Moon
} from 'lucide-react';
import data from './analytics_data.json';

// --- COLOR PALETTE ---
const COLORS = {
    primary: '#6366f1',
    primaryLight: '#818cf8',
    secondary: '#0ea5e9',
    emerald: '#10b981',
    amber: '#f59e0b',
    rose: '#f43f5e',
    violet: '#8b5cf6',
    slate: '#64748b',
    cyan: '#06b6d4',
};

const CATEGORY_COLORS = ['#6366f1', '#0ea5e9', '#10b981', '#f59e0b', '#f43f5e', '#94a3b8'];

// --- DARK-AWARE TOOLTIP ---
const tooltipStyle = (isDark) => ({
    borderRadius: '12px',
    border: 'none',
    boxShadow: isDark
        ? '0 10px 25px rgb(0 0 0 / 0.4)'
        : '0 10px 25px rgb(0 0 0 / 0.08)',
    padding: '12px 16px',
    backgroundColor: isDark ? '#1e293b' : '#ffffff',
    color: isDark ? '#f1f5f9' : '#0f172a',
});

// --- COMPONENTS ---

const GlassCard = ({ children, className = "", hover = true }) => (
    <div className={`${hover ? 'glass-card' : 'glass-card-static'} p-6 ${className}`}>
        {children}
    </div>
);

const StatCard = ({ title, value, subtext, icon: Icon, gradient, trend }) => (
    <div className={`glass-card p-6 relative overflow-hidden group`}>
        <div className={`absolute -top-6 -right-6 w-28 h-28 rounded-full opacity-[0.08] ${gradient}`} />
        <div className="flex items-start justify-between relative z-10">
            <div>
                <p className="text-sm font-medium text-[var(--text-secondary)] mb-1">{title}</p>
                <h3 className="text-3xl font-bold text-[var(--text-primary)] tracking-tight font-mono">{value}</h3>
                {subtext && <p className="text-xs text-[var(--text-muted)] mt-2">{subtext}</p>}
                {trend && (
                    <div className="flex items-center mt-2 text-xs font-medium text-emerald-500">
                        <TrendingUp size={12} className="mr-1" />
                        {trend}
                    </div>
                )}
            </div>
            <div className={`p-3 rounded-xl ${gradient} text-white shadow-lg`}>
                <Icon size={22} />
            </div>
        </div>
    </div>
);

const SectionHeader = ({ title, subtitle, icon: Icon }) => (
    <div className="mb-6 px-1">
        <div className="flex items-center space-x-3 mb-1">
            <div className="p-2 rounded-lg bg-gradient-to-br from-indigo-500/10 to-violet-500/10 dark:from-indigo-500/20 dark:to-violet-500/20">
                <Icon className="text-indigo-600 dark:text-indigo-400" size={20} />
            </div>
            <h2 className="text-2xl font-bold text-[var(--text-primary)] tracking-tight">{title}</h2>
        </div>
        {subtitle && <p className="text-sm text-[var(--text-muted)] ml-12">{subtitle}</p>}
    </div>
);

const ProgressBar = ({ value, max, color = "#10b981" }) => {
    const percentage = Math.min((value / max) * 100, 100);
    return (
        <div className="w-full bg-slate-200/50 dark:bg-slate-700/50 rounded-full h-1.5 mt-2 overflow-hidden">
            <div
                className="h-1.5 rounded-full transition-all duration-700 ease-out"
                style={{ width: `${percentage}%`, backgroundColor: color }}
            />
        </div>
    );
};

const InsightPill = ({ emoji, text }) => (
    <div className="glass-card-static flex items-start gap-3 px-4 py-3 !rounded-xl">
        <span className="text-lg shrink-0 mt-0.5">{emoji}</span>
        <p className="text-sm text-[var(--text-secondary)] leading-relaxed">{text}</p>
    </div>
);

const SourceBadge = ({ source }) => (
    <span className={`px-2 py-0.5 rounded-full text-[10px] font-bold uppercase tracking-wide ${source === 'president'
            ? 'bg-blue-500/10 text-blue-600 dark:text-blue-400'
            : 'bg-teal-500/10 text-teal-600 dark:text-teal-400'
        }`}>
        {source === 'president' ? 'Pres' : 'Cab'}
    </span>
);

const ThemeToggle = ({ isDark, onToggle }) => (
    <button
        onClick={onToggle}
        className="glass-pill p-2 hover:scale-105 transition-all text-[var(--text-secondary)] hover:text-[var(--text-primary)]"
        title={isDark ? 'Switch to Light Mode' : 'Switch to Dark Mode'}
    >
        {isDark ? <Sun size={16} /> : <Moon size={16} />}
    </button>
);

// --- TIMELINE ANNOTATIONS ---
const TIMELINE_EVENTS = [
    { month: '2022-02', label: 'Full-scale invasion' },
];

// --- MAIN DASHBOARD ---

export default function Dashboard() {
    const { overview, daily, analytics, insights, pipeline } = data;
    const [activeSource, setActiveSource] = useState('all');

    // Dark mode with localStorage persistence
    const [isDark, setIsDark] = useState(() => {
        if (typeof window !== 'undefined') {
            const saved = localStorage.getItem('theme');
            return saved === 'dark';
        }
        return false;
    });

    useEffect(() => {
        const root = document.documentElement;
        if (isDark) {
            root.classList.remove('light');
            root.classList.add('dark');
        } else {
            root.classList.remove('dark');
            root.classList.add('light');
        }
        localStorage.setItem('theme', isDark ? 'dark' : 'light');
    }, [isDark]);

    // --- TRANSFORM DATA ---
    const historyData = daily.history ? daily.history.map(d => ({
        date: d.date.slice(5),
        value: d.value
    })) : [];

    const isSourceMatch = (source) => activeSource === 'all' || source === activeSource;

    const platforms = overview.platform_comparison || [];
    const presPlatform = platforms.find(p => p.source === 'president') || {};
    const cabPlatform = platforms.find(p => p.source === 'cabinet') || {};

    const filteredOverview = activeSource === 'all' ? overview : (() => {
        const p = platforms.find(pl => pl.source === activeSource) || {};
        return {
            ...overview,
            total: p.total || 0,
            president_count: activeSource === 'president' ? p.total : 0,
            cabinet_count: activeSource === 'cabinet' ? p.total : 0,
            success_rate: p.success_rate || 0,
            median_votes: p.median_votes || 0,
            response_rate: p.response_rate || 0,
        };
    })();

    const statusAggregated = {};
    (analytics.status_distribution || []).filter(s => isSourceMatch(s.source)).forEach(s => {
        if (!statusAggregated[s.status]) {
            statusAggregated[s.status] = { status: s.status, president: 0, cabinet: 0, total: 0 };
        }
        statusAggregated[s.status][s.source] = s.count;
        statusAggregated[s.status].total += s.count;
    });
    const statusData = Object.values(statusAggregated)
        .sort((a, b) => b.total - a.total)
        .slice(0, 6);

    const totalPetitions = filteredOverview.total || 1;
    const histogramWithPct = (analytics.histogram || []).map(h => ({
        ...h,
        percentage: ((h.count / totalPetitions) * 100).toFixed(1)
    }));

    const axisTickColor = isDark ? '#64748b' : '#94a3b8';

    return (
        <div className={`min-h-screen p-4 md:p-8 lg:p-12 font-sans transition-colors duration-300 ${isDark
                ? 'bg-gradient-to-br from-slate-950 via-slate-900 to-slate-950'
                : 'bg-gradient-to-br from-slate-50 via-white to-slate-100'
            }`}>
            <div className="max-w-7xl mx-auto space-y-12">

                {/* ═══════════════ HEADER ═══════════════ */}
                <header className="flex flex-col md:flex-row md:items-end justify-between border-b border-[var(--border-color)] pb-6 animate-in">
                    <div>
                        <h1 className="text-4xl font-extrabold tracking-tight">
                            <span className="gradient-text">Petition Analytics</span>
                        </h1>
                        <p className="text-[var(--text-muted)] mt-2 text-lg">
                            Ukrainian E-Petitions — {pipeline.data_span || '2015-2026'} • {overview.total?.toLocaleString()} petitions
                        </p>
                    </div>
                    <div className="mt-4 md:mt-0 flex items-center gap-3">
                        {/* Source Toggle */}
                        <div className="glass-pill flex p-0.5">
                            {['all', 'president', 'cabinet'].map(s => (
                                <button
                                    key={s}
                                    onClick={() => setActiveSource(s)}
                                    className={`px-3 py-1.5 text-xs font-semibold rounded-full transition-all ${activeSource === s
                                            ? 'bg-indigo-600 text-white shadow-lg shadow-indigo-500/25'
                                            : 'text-[var(--text-muted)] hover:text-[var(--text-primary)]'
                                        }`}
                                >
                                    {s === 'all' ? 'All' : s === 'president' ? 'President' : 'Cabinet'}
                                </button>
                            ))}
                        </div>
                        <div className="glass-pill flex items-center space-x-2 px-3 py-1.5 text-sm text-[var(--text-secondary)]">
                            <Clock size={14} className="text-[var(--text-muted)]" />
                            <span>{pipeline.last_updated}</span>
                        </div>
                        <ThemeToggle isDark={isDark} onToggle={() => setIsDark(!isDark)} />
                    </div>
                </header>

                {/* ═══════════════ BLOCK 1: OVERVIEW ═══════════════ */}
                <section className="animate-in animate-in-delay-1">
                    <SectionHeader title="Overview" subtitle="High-level metrics across all petitions" icon={Activity} />

                    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-5">
                        <StatCard
                            title="Total Petitions"
                            value={filteredOverview.total?.toLocaleString()}
                            subtext={activeSource === 'all' ? `President: ${overview.president_count?.toLocaleString()} • Cabinet: ${overview.cabinet_count?.toLocaleString()}` : `Filtered: ${activeSource}`}
                            icon={FileText}
                            gradient="bg-gradient-to-br from-indigo-500 to-violet-600"
                        />
                        <StatCard
                            title="Success Rate"
                            value={`${filteredOverview.success_rate}%`}
                            subtext="Reached 25,000 votes threshold"
                            icon={CheckCircle}
                            gradient="bg-gradient-to-br from-emerald-500 to-teal-600"
                        />
                        <StatCard
                            title="Median Votes"
                            value={filteredOverview.median_votes?.toLocaleString()}
                            subtext="50% of petitions receive fewer"
                            icon={Users}
                            gradient="bg-gradient-to-br from-amber-500 to-orange-600"
                        />
                        <StatCard
                            title="Response Rate"
                            value={`${filteredOverview.response_rate}%`}
                            subtext="Received an official answer"
                            icon={Info}
                            gradient="bg-gradient-to-br from-cyan-500 to-blue-600"
                        />
                    </div>

                    {/* Insights Banner */}
                    {insights && insights.length > 0 && (
                        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3 mt-6">
                            {insights.slice(0, 5).map((ins, i) => (
                                <InsightPill key={i} emoji={ins.emoji} text={ins.text} />
                            ))}
                        </div>
                    )}

                    {/* Platform Comparison */}
                    {platforms.length >= 2 && (
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-5 mt-6">
                            {[
                                { p: presPlatform, label: 'Presidential Portal', icon: '🏛️', accent: 'indigo' },
                                { p: cabPlatform, label: 'Cabinet of Ministers', icon: '🏢', accent: 'cyan' }
                            ].map(({ p, label, icon, accent }) => (
                                <GlassCard key={label}>
                                    <div className="flex items-center gap-2 mb-4">
                                        <span className="text-xl">{icon}</span>
                                        <h4 className="font-bold text-[var(--text-primary)]">{label}</h4>
                                    </div>
                                    <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
                                        <div>
                                            <p className="text-xs text-[var(--text-muted)]">Total</p>
                                            <p className="text-lg font-bold font-mono text-[var(--text-primary)]">{p.total?.toLocaleString()}</p>
                                        </div>
                                        <div>
                                            <p className="text-xs text-[var(--text-muted)]">Avg Votes</p>
                                            <p className="text-lg font-bold font-mono text-[var(--text-primary)]">{p.avg_votes?.toLocaleString()}</p>
                                        </div>
                                        <div>
                                            <p className="text-xs text-[var(--text-muted)]">Success Rate</p>
                                            <p className="text-lg font-bold font-mono text-emerald-500">{p.success_rate}%</p>
                                        </div>
                                        <div>
                                            <p className="text-xs text-[var(--text-muted)]">Response Rate</p>
                                            <p className="text-lg font-bold font-mono text-cyan-500">{p.response_rate}%</p>
                                        </div>
                                    </div>
                                </GlassCard>
                            ))}
                        </div>
                    )}
                </section>

                {/* ═══════════════ BLOCK 2: DAILY DYNAMICS ═══════════════ */}
                <section className="animate-in animate-in-delay-2">
                    <SectionHeader title="Daily Dynamics" subtitle="Since the last automated sync" icon={TrendingUp} />

                    <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                        {/* Today's Pulse */}
                        <GlassCard className={`relative overflow-hidden ${isDark ? '!bg-emerald-950/30' : '!bg-gradient-to-br !from-emerald-50 !to-white'}`}>
                            <h3 className="text-lg font-bold text-emerald-600 dark:text-emerald-400 mb-6 flex items-center">
                                <Calendar size={18} className="mr-2 opacity-75" /> Last 24 Hours
                            </h3>

                            <div className="grid grid-cols-2 gap-4 mb-8 relative z-10">
                                <div>
                                    <p className="text-sm text-emerald-600/70 dark:text-emerald-400/70 font-medium">New Petitions</p>
                                    <p className="text-4xl font-extrabold text-emerald-600 dark:text-emerald-400 font-mono">+{daily.new_petitions}</p>
                                </div>
                                <div>
                                    <p className="text-sm text-emerald-600/70 dark:text-emerald-400/70 font-medium">Votes Added</p>
                                    <p className="text-4xl font-extrabold text-emerald-600 dark:text-emerald-400 font-mono">+{daily.votes_added?.toLocaleString()}</p>
                                </div>
                            </div>

                            <div className="h-32 -mx-6 -mb-6 mt-4">
                                {historyData.length > 0 ? (
                                    <ResponsiveContainer width="100%" height="100%">
                                        <AreaChart data={historyData}>
                                            <defs>
                                                <linearGradient id="colorVote" x1="0" y1="0" x2="0" y2="1">
                                                    <stop offset="5%" stopColor="#10b981" stopOpacity={0.3} />
                                                    <stop offset="95%" stopColor="#10b981" stopOpacity={0} />
                                                </linearGradient>
                                            </defs>
                                            <Tooltip contentStyle={tooltipStyle(isDark)} />
                                            <Area type="monotone" dataKey="value" stroke="#10b981" strokeWidth={3} fillOpacity={1} fill="url(#colorVote)" />
                                        </AreaChart>
                                    </ResponsiveContainer>
                                ) : (
                                    <div className="h-full flex items-center justify-center text-emerald-500/40 text-sm">
                                        Collecting trend data...
                                    </div>
                                )}
                            </div>
                        </GlassCard>

                        {/* Biggest Movers Table */}
                        <GlassCard className="lg:col-span-2">
                            <div className="flex items-center justify-between mb-6">
                                <h3 className="text-lg font-bold text-[var(--text-primary)]">Growth Leaders (Top 5)</h3>
                                <span className="px-2.5 py-1 bg-emerald-500/10 text-emerald-600 dark:text-emerald-400 text-xs font-bold rounded-full uppercase tracking-wide">
                                    Trending
                                </span>
                            </div>

                            <div className="overflow-x-auto">
                                <table className="w-full text-sm">
                                    <thead className="text-xs text-[var(--text-muted)] uppercase font-medium">
                                        <tr className="border-b border-[var(--border-color)]">
                                            <th className="px-4 py-3 text-left">Petition</th>
                                            <th className="px-4 py-3 text-right">Growth (24h)</th>
                                            <th className="px-4 py-3 text-right">Total Votes</th>
                                            <th className="px-4 py-3 w-32">Progress to 25k</th>
                                        </tr>
                                    </thead>
                                    <tbody className="divide-y divide-[var(--border-color)]">
                                        {daily.biggest_movers?.map((m, i) => (
                                            <tr key={i} className="hover:bg-[var(--accent-glow)] transition-colors">
                                                <td className="px-4 py-3">
                                                    <a href={m.url} target="_blank" rel="noopener noreferrer"
                                                        className="font-medium text-[var(--text-primary)] hover:text-indigo-500 line-clamp-1 block transition-colors"
                                                        title={m.title}>
                                                        {m.title}
                                                    </a>
                                                    <div className="flex items-center gap-2 mt-1">
                                                        <span className="text-[10px] text-[var(--text-muted)]">ID: {m.url?.split('/').pop()}</span>
                                                        <SourceBadge source={m.url?.includes('kmu') ? 'cabinet' : 'president'} />
                                                    </div>
                                                </td>
                                                <td className="px-4 py-3 text-right">
                                                    <span className="font-bold text-emerald-500 font-mono">+{m.delta?.toLocaleString()}</span>
                                                </td>
                                                <td className="px-4 py-3 text-right font-mono text-[var(--text-secondary)]">
                                                    {m.total?.toLocaleString()}
                                                </td>
                                                <td className="px-4 py-3">
                                                    <ProgressBar value={m.total} max={25000} />
                                                    <div className="text-[10px] text-[var(--text-muted)] text-right mt-1 font-mono">
                                                        {Math.round((m.total / 25000) * 100)}%
                                                    </div>
                                                </td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        </GlassCard>
                    </div>
                </section>

                {/* ═══════════════ BLOCK 3: ENGAGEMENT & CONTENT ═══════════════ */}
                <section className="animate-in animate-in-delay-3">
                    <SectionHeader title="Engagement & Content" subtitle="How petitions gather support and what drives votes" icon={BarChart3} />

                    <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                        {/* Vote Distribution */}
                        <GlassCard>
                            <h3 className="text-lg font-bold text-[var(--text-primary)] mb-1">Vote Distribution</h3>
                            <p className="text-sm text-[var(--text-muted)] mb-6">How hard is it to get votes?</p>
                            <div className="h-64">
                                <ResponsiveContainer width="100%" height="100%">
                                    <BarChart data={histogramWithPct}>
                                        <XAxis dataKey="bin" tick={{ fontSize: 11, fill: axisTickColor }} axisLine={false} tickLine={false} />
                                        <YAxis axisLine={false} tickLine={false} tick={{ fontSize: 11, fill: axisTickColor }} />
                                        <Tooltip cursor={{ fill: isDark ? 'rgba(99,102,241,0.08)' : '#f1f5f9' }} contentStyle={tooltipStyle(isDark)}
                                            formatter={(value) => [`${value.toLocaleString()} petitions`, 'Count']} />
                                        <Bar dataKey="count" radius={[6, 6, 0, 0]} barSize={40}>
                                            {histogramWithPct.map((_, index) => (
                                                <Cell key={`cell-${index}`} fill={index === 0 ? '#94a3b8' : index < 3 ? COLORS.primary : COLORS.emerald} />
                                            ))}
                                        </Bar>
                                    </BarChart>
                                </ResponsiveContainer>
                            </div>
                            <div className="flex flex-wrap gap-2 mt-3">
                                {histogramWithPct.map((h, i) => (
                                    <span key={i} className="text-xs text-[var(--text-muted)]">
                                        {h.bin}: <span className="font-semibold font-mono text-[var(--text-secondary)]">{h.percentage}%</span>
                                    </span>
                                ))}
                            </div>
                        </GlassCard>

                        {/* Status Distribution */}
                        <GlassCard>
                            <h3 className="text-lg font-bold text-[var(--text-primary)] mb-1">Status Distribution</h3>
                            <p className="text-sm text-[var(--text-muted)] mb-6">Petition lifecycle stages by source</p>
                            <div className="h-64">
                                <ResponsiveContainer width="100%" height="100%">
                                    <BarChart data={statusData} layout="vertical">
                                        <XAxis type="number" axisLine={false} tickLine={false} tick={{ fontSize: 11, fill: axisTickColor }} />
                                        <YAxis type="category" dataKey="status" width={130} tick={{ fontSize: 10, fill: axisTickColor }} axisLine={false} tickLine={false} />
                                        <Tooltip contentStyle={tooltipStyle(isDark)} />
                                        <Legend iconType="circle" />
                                        <Bar dataKey="president" stackId="a" fill="#3b82f6" radius={[0, 0, 0, 0]} name="President" />
                                        <Bar dataKey="cabinet" stackId="a" fill="#0ea5e9" radius={[0, 4, 4, 0]} name="Cabinet" />
                                    </BarChart>
                                </ResponsiveContainer>
                            </div>
                        </GlassCard>

                        {/* Scatter Plot */}
                        <GlassCard>
                            <h3 className="text-lg font-bold text-[var(--text-primary)] mb-1">Text Length vs Votes</h3>
                            <p className="text-sm text-[var(--text-muted)] mb-6">Do longer petitions get more support?</p>
                            <div className="h-64">
                                <ResponsiveContainer width="100%" height="100%">
                                    <ScatterChart>
                                        <XAxis type="number" dataKey="x" name="Text Length" tick={{ fontSize: 10, fill: axisTickColor }} axisLine={false} tickLine={false}
                                            label={{ value: 'Text Length (chars)', position: 'bottom', fontSize: 11, fill: axisTickColor }} />
                                        <YAxis type="number" dataKey="y" name="Votes" tick={{ fontSize: 10, fill: axisTickColor }} axisLine={false} tickLine={false}
                                            label={{ value: 'Votes', angle: -90, position: 'insideLeft', fontSize: 11, fill: axisTickColor }} />
                                        <ZAxis range={[20, 80]} />
                                        <Tooltip contentStyle={tooltipStyle(isDark)} formatter={(value, name) => [value?.toLocaleString(), name]} />
                                        {(activeSource === 'all' || activeSource === 'president') && (
                                            <Scatter data={(analytics.scatter || []).filter(s => s.source === 'president')} fill="#3b82f6" fillOpacity={0.5} name="President" />
                                        )}
                                        {(activeSource === 'all' || activeSource === 'cabinet') && (
                                            <Scatter data={(analytics.scatter || []).filter(s => s.source === 'cabinet')} fill="#0ea5e9" fillOpacity={0.7} name="Cabinet" />
                                        )}
                                        <Legend iconType="circle" />
                                    </ScatterChart>
                                </ResponsiveContainer>
                            </div>
                        </GlassCard>

                        {/* Top Authors */}
                        <GlassCard>
                            <h3 className="text-lg font-bold text-[var(--text-primary)] mb-1">Top Authors</h3>
                            <p className="text-sm text-[var(--text-muted)] mb-6">Most impactful petition creators by total votes</p>
                            <div className="h-64">
                                <ResponsiveContainer width="100%" height="100%">
                                    <BarChart data={(analytics.top_authors || []).slice(0, 7)} layout="vertical">
                                        <XAxis type="number" axisLine={false} tickLine={false} tick={{ fontSize: 10, fill: axisTickColor }} />
                                        <YAxis type="category" dataKey="author" width={120} tick={{ fontSize: 9, fill: axisTickColor }} axisLine={false} tickLine={false}
                                            tickFormatter={(v) => v.length > 18 ? v.substring(0, 18) + '…' : v} />
                                        <Tooltip contentStyle={tooltipStyle(isDark)}
                                            formatter={(value, _, props) => [`${value.toLocaleString()} votes (${props.payload.petitions} petitions)`, 'Total Votes']} />
                                        <Bar dataKey="total_votes" fill={COLORS.violet} radius={[0, 6, 6, 0]} barSize={20} />
                                    </BarChart>
                                </ResponsiveContainer>
                            </div>
                        </GlassCard>
                    </div>
                </section>

                {/* ═══════════════ BLOCK 4: DEEP ANALYSIS ═══════════════ */}
                <section>
                    <SectionHeader title="Deep Analysis" subtitle="Categorical breakdown, keyword trends, and historical patterns" icon={Layers} />

                    <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                        {/* Category Breakdown */}
                        <GlassCard>
                            <h3 className="text-lg font-bold text-[var(--text-primary)] mb-1">Category Breakdown</h3>
                            <p className="text-sm text-[var(--text-muted)] mb-6">Topic classification via keyword analysis</p>
                            <div className="space-y-3">
                                {(analytics.categories || []).map((cat, i) => (
                                    <div key={cat.category}>
                                        <div className="flex items-center justify-between mb-1">
                                            <span className="text-sm font-medium text-[var(--text-secondary)]">{cat.category}</span>
                                            <span className="text-sm font-mono text-[var(--text-muted)]">
                                                {cat.count.toLocaleString()} <span className="text-xs">({cat.percentage}%)</span>
                                            </span>
                                        </div>
                                        <div className="w-full bg-slate-200/50 dark:bg-slate-700/50 rounded-full h-2.5 overflow-hidden">
                                            <div
                                                className="h-2.5 rounded-full transition-all duration-700 ease-out"
                                                style={{ width: `${cat.percentage}%`, backgroundColor: CATEGORY_COLORS[i] || '#94a3b8' }}
                                            />
                                        </div>
                                    </div>
                                ))}
                            </div>
                        </GlassCard>

                        {/* Keywords Top-10 */}
                        <GlassCard>
                            <h3 className="text-lg font-bold text-[var(--text-primary)] mb-1">Keywords Top-10</h3>
                            <p className="text-sm text-[var(--text-muted)] mb-6">Most frequent words in petition titles</p>
                            <div className="h-64">
                                <ResponsiveContainer width="100%" height="100%">
                                    <BarChart data={analytics.keywords_top10 || []} layout="vertical">
                                        <XAxis type="number" axisLine={false} tickLine={false} tick={{ fontSize: 10, fill: axisTickColor }} />
                                        <YAxis type="category" dataKey="word" width={100} tick={{ fontSize: 11, fill: axisTickColor }} axisLine={false} tickLine={false} />
                                        <Tooltip contentStyle={tooltipStyle(isDark)} formatter={(v) => [`${v.toLocaleString()} occurrences`, 'Frequency']} />
                                        <Bar dataKey="count" radius={[0, 6, 6, 0]} barSize={18}>
                                            {(analytics.keywords_top10 || []).map((_, index) => (
                                                <Cell key={`cell-${index}`} fill={index < 3 ? COLORS.primary : index < 6 ? COLORS.secondary : '#94a3b8'} />
                                            ))}
                                        </Bar>
                                    </BarChart>
                                </ResponsiveContainer>
                            </div>
                        </GlassCard>

                        {/* Monthly Timeline with annotation */}
                        <GlassCard className="md:col-span-2">
                            <h3 className="text-lg font-bold text-[var(--text-primary)] mb-1">Petition Creation Timeline</h3>
                            <p className="text-sm text-[var(--text-muted)] mb-6">Volume by month since 2015 — stacked by source</p>
                            <div className="h-72">
                                <ResponsiveContainer width="100%" height="100%">
                                    <AreaChart data={analytics.timeline || []}>
                                        <defs>
                                            <linearGradient id="colorPres" x1="0" y1="0" x2="0" y2="1">
                                                <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.7} />
                                                <stop offset="95%" stopColor="#3b82f6" stopOpacity={0.05} />
                                            </linearGradient>
                                            <linearGradient id="colorCab" x1="0" y1="0" x2="0" y2="1">
                                                <stop offset="5%" stopColor="#0ea5e9" stopOpacity={0.7} />
                                                <stop offset="95%" stopColor="#0ea5e9" stopOpacity={0.05} />
                                            </linearGradient>
                                        </defs>
                                        <XAxis dataKey="month" tick={{ fontSize: 10, fill: axisTickColor }} minTickGap={40} axisLine={false} tickLine={false} />
                                        <YAxis axisLine={false} tickLine={false} tick={{ fontSize: 10, fill: axisTickColor }} />
                                        <Tooltip contentStyle={tooltipStyle(isDark)} />
                                        <Legend iconType="circle" />
                                        {/* Timeline annotation: Feb 2022 */}
                                        {TIMELINE_EVENTS.map(evt => (
                                            <ReferenceLine key={evt.month} x={evt.month} stroke="#ef4444" strokeDasharray="4 4" strokeWidth={2}
                                                label={{ value: evt.label, position: 'top', fill: '#ef4444', fontSize: 11, fontWeight: 600 }} />
                                        ))}
                                        <Area type="monotone" dataKey="president" stackId="1" stroke="#3b82f6" fill="url(#colorPres)" strokeWidth={2} name="President" />
                                        <Area type="monotone" dataKey="cabinet" stackId="1" stroke="#0ea5e9" fill="url(#colorCab)" strokeWidth={2} name="Cabinet" />
                                    </AreaChart>
                                </ResponsiveContainer>
                            </div>
                        </GlassCard>
                    </div>
                </section>

                {/* ═══════════════ BLOCK 5: VOTE VELOCITY ═══════════════ */}
                {analytics.vote_velocity && analytics.vote_velocity.length > 0 && (
                    <section>
                        <SectionHeader title="Vote Velocity" subtitle="Fastest growing active petitions (last 7 days)" icon={Zap} />
                        <GlassCard>
                            <div className="overflow-x-auto">
                                <table className="w-full text-sm">
                                    <thead className="text-xs text-[var(--text-muted)] uppercase font-medium">
                                        <tr className="border-b border-[var(--border-color)]">
                                            <th className="px-4 py-3 text-left">Petition</th>
                                            <th className="px-4 py-3 text-right">7-Day Growth</th>
                                            <th className="px-4 py-3 text-right">Daily Rate</th>
                                            <th className="px-4 py-3 text-right">Current Votes</th>
                                        </tr>
                                    </thead>
                                    <tbody className="divide-y divide-[var(--border-color)]">
                                        {analytics.vote_velocity.slice(0, 5).map((v, i) => (
                                            <tr key={i} className="hover:bg-[var(--accent-glow)] transition-colors">
                                                <td className="px-4 py-3">
                                                    <a href={v.url} target="_blank" rel="noopener noreferrer"
                                                        className="font-medium text-[var(--text-primary)] hover:text-indigo-500 line-clamp-1 transition-colors"
                                                        title={v.title}>
                                                        {v.title}
                                                    </a>
                                                </td>
                                                <td className="px-4 py-3 text-right font-bold text-amber-500 font-mono">+{v.growth_7d?.toLocaleString()}</td>
                                                <td className="px-4 py-3 text-right font-mono text-[var(--text-secondary)]">~{v.daily_rate?.toLocaleString()}/day</td>
                                                <td className="px-4 py-3 text-right font-mono text-[var(--text-secondary)]">{v.votes_current?.toLocaleString()}</td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        </GlassCard>
                    </section>
                )}

                {/* ═══════════════ FOOTER ═══════════════ */}
                <footer className="mt-8 border-t border-[var(--border-color)] pt-8 text-[var(--text-muted)] text-sm">
                    <div className="max-w-5xl mx-auto space-y-10">
                        {/* Data Freshness + Coverage */}
                        <div className="flex flex-wrap items-center justify-center gap-3">
                            <span className="glass-pill inline-flex items-center gap-1.5 px-3 py-1.5 text-emerald-600 dark:text-emerald-400 text-xs font-semibold">
                                <span className="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse" />
                                Auto-ETL every 24h
                            </span>
                            <span className="glass-pill inline-flex items-center gap-1.5 px-3 py-1.5 text-blue-600 dark:text-blue-400 text-xs font-semibold">
                                <Database size={12} />
                                {pipeline.data_span} • {pipeline.total_records?.toLocaleString()} records
                            </span>
                            <span className="glass-pill inline-flex items-center gap-1.5 px-3 py-1.5 text-violet-600 dark:text-violet-400 text-xs font-semibold">
                                <Layers size={12} />
                                {pipeline.coverage}
                            </span>
                        </div>

                        {/* Tech Stack + Roadmap */}
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-10 text-center">
                            <div className="flex flex-col items-center">
                                <h4 className="font-bold text-[var(--text-primary)] mb-3 flex items-center justify-center">
                                    <Server size={16} className="mr-2 text-indigo-500" /> Tech Stack
                                </h4>
                                <ul className="space-y-1.5 text-sm">
                                    <li>Python ETL + DuckDB + MotherDuck</li>
                                    <li>React + Tailwind + Recharts</li>
                                    <li>GitHub Actions (automated daily sync)</li>
                                </ul>
                            </div>
                            <div className="flex flex-col items-center">
                                <h4 className="font-bold text-[var(--text-primary)] mb-3 flex items-center justify-center">
                                    <Activity size={16} className="mr-2 text-emerald-500" /> Roadmap
                                </h4>
                                <ul className="space-y-1.5 text-sm">
                                    <li className="text-indigo-500 font-medium flex items-center justify-center">
                                        <Sparkles size={14} className="mr-1 text-amber-500" />
                                        AI-powered insights assistant
                                    </li>
                                    <li className="pt-1">
                                        <a href="https://github.com/Volodymyr75/petitions/blob/main/PROJECT_STATE.md"
                                            target="_blank" rel="noopener noreferrer"
                                            className="text-[var(--text-secondary)] font-medium hover:text-indigo-500 flex items-center justify-center transition-colors">
                                            View Implementation Plan <ArrowRight size={14} className="ml-1" />
                                        </a>
                                    </li>
                                </ul>
                            </div>
                        </div>
                    </div>

                    <div className="mt-8 pt-6 border-t border-[var(--border-color)] flex flex-col md:flex-row items-center justify-between gap-4">
                        <div className="flex items-center space-x-6">
                            <a href="https://github.com/Volodymyr75/petitions" target="_blank" rel="noopener noreferrer"
                                className="flex items-center text-[var(--text-muted)] hover:text-indigo-500 transition-colors">
                                <Database size={16} className="mr-2" />
                                GitHub Repository
                            </a>
                            <a href="mailto:strembov@gmail.com"
                                className="flex items-center text-[var(--text-muted)] hover:text-indigo-500 transition-colors">
                                <Info size={16} className="mr-2" />
                                strembov@gmail.com
                            </a>
                        </div>
                        <div className="text-xs text-[var(--text-muted)]">
                            &copy; 2025-2026 Petition Analytics Project. Open Source.
                        </div>
                    </div>
                </footer>
            </div>
        </div>
    );
}
