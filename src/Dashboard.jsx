import { useState } from 'react';
import {
    BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer,
    AreaChart, Area, ScatterChart, Scatter, ZAxis, Legend, LineChart, Line,
    Cell, PieChart, Pie
} from 'recharts';
import {
    TrendingUp, Users, FileText, CheckCircle,
    Clock, Server, Database, Activity, Info, ExternalLink, Calendar,
    ChevronUp, ArrowRight, Sparkles, BarChart3, PieChart as PieChartIcon,
    Zap, Award, Tag, GitBranch, Layers, Filter
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

const STATUS_COLORS = {
    'Триває збір підписів': '#3b82f6',
    'На розгляді': '#f59e0b',
    'З відповіддю': '#10b981',
    'Архів': '#94a3b8',
    'Не підтримано': '#ef4444',
    'Unsupported': '#ef4444',
    'Approved': '#10b981',
    'Answered': '#10b981',
    'Supported': '#3b82f6',
};

const CATEGORY_COLORS = ['#6366f1', '#0ea5e9', '#10b981', '#f59e0b', '#f43f5e', '#94a3b8'];

// --- COMPONENTS ---

const Card = ({ children, className = "" }) => (
    <div className={`bg-white rounded-2xl shadow-sm border border-slate-200/60 p-6 hover:shadow-md transition-all duration-300 ${className}`}>
        {children}
    </div>
);

const StatCard = ({ title, value, subtext, icon: Icon, color = "blue", trend }) => (
    <Card className="relative overflow-hidden group hover:scale-[1.01] transition-transform">
        <div className={`absolute -top-4 -right-4 w-24 h-24 rounded-full opacity-[0.07] bg-${color}-500`} />
        <div className="flex items-start justify-between relative z-10">
            <div>
                <p className="text-sm font-medium text-slate-500 mb-1">{title}</p>
                <h3 className="text-3xl font-bold text-slate-900 tracking-tight font-mono">{value}</h3>
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

const SectionHeader = ({ title, subtitle, icon: Icon, color = "slate" }) => (
    <div className="mb-6 px-1">
        <div className="flex items-center space-x-2 mb-1">
            <Icon className={`text-${color}-600`} size={24} />
            <h2 className="text-2xl font-bold text-slate-800 tracking-tight">{title}</h2>
        </div>
        {subtitle && <p className="text-sm text-slate-500 ml-8">{subtitle}</p>}
    </div>
);

const ProgressBar = ({ value, max, color = "emerald" }) => {
    const percentage = Math.min((value / max) * 100, 100);
    return (
        <div className="w-full bg-slate-100 rounded-full h-1.5 mt-2 overflow-hidden">
            <div
                className={`bg-${color}-500 h-1.5 rounded-full transition-all duration-500`}
                style={{ width: `${percentage}%` }}
            />
        </div>
    );
};

const InsightPill = ({ emoji, text }) => (
    <div className="flex items-start gap-3 px-4 py-3 bg-gradient-to-r from-indigo-50 to-violet-50 rounded-xl border border-indigo-100/50">
        <span className="text-lg shrink-0 mt-0.5">{emoji}</span>
        <p className="text-sm text-slate-700 leading-relaxed">{text}</p>
    </div>
);

const SourceBadge = ({ source }) => (
    <span className={`px-2 py-0.5 rounded-full text-[10px] font-bold uppercase tracking-wide ${source === 'president'
            ? 'bg-blue-100 text-blue-700'
            : 'bg-teal-100 text-teal-700'
        }`}>
        {source === 'president' ? 'Pres' : 'Cab'}
    </span>
);

const CustomTooltipStyle = {
    borderRadius: '12px',
    border: 'none',
    boxShadow: '0 10px 25px -5px rgb(0 0 0 / 0.08), 0 4px 6px -2px rgb(0 0 0 / 0.05)',
    padding: '12px 16px'
};

// --- MAIN DASHBOARD ---

export default function Dashboard() {
    const { overview, daily, analytics, insights, pipeline } = data;
    const [activeSource, setActiveSource] = useState('all');

    // --- TRANSFORM DATA ---
    const historyData = daily.history ? daily.history.map(d => ({
        date: d.date.slice(5),
        value: d.value
    })) : [];

    // Aggregate status distribution for stacked bars
    const statusAggregated = {};
    (analytics.status_distribution || []).forEach(s => {
        if (!statusAggregated[s.status]) {
            statusAggregated[s.status] = { status: s.status, president: 0, cabinet: 0, total: 0 };
        }
        statusAggregated[s.status][s.source] = s.count;
        statusAggregated[s.status].total += s.count;
    });
    const statusData = Object.values(statusAggregated)
        .sort((a, b) => b.total - a.total)
        .slice(0, 6);

    // Platform comparison
    const platforms = overview.platform_comparison || [];
    const presPlatform = platforms.find(p => p.source === 'president') || {};
    const cabPlatform = platforms.find(p => p.source === 'cabinet') || {};

    // Histogram with percentage
    const totalPetitions = overview.total || 1;
    const histogramWithPct = (analytics.histogram || []).map(h => ({
        ...h,
        percentage: ((h.count / totalPetitions) * 100).toFixed(1)
    }));

    return (
        <div className="min-h-screen bg-gradient-to-br from-slate-50 via-white to-slate-50 p-4 md:p-8 lg:p-12 font-sans text-slate-900">
            <div className="max-w-7xl mx-auto space-y-12">

                {/* ═══════════════ HEADER ═══════════════ */}
                <header className="flex flex-col md:flex-row md:items-end justify-between border-b border-slate-200 pb-6">
                    <div>
                        <h1 className="text-4xl font-extrabold text-slate-900 tracking-tight">
                            Petition Analytics
                        </h1>
                        <p className="text-slate-500 mt-2 text-lg">
                            Ukrainian E-Petitions Dashboard — {pipeline.data_span || '2015-2026'} • {overview.total?.toLocaleString()} petitions
                        </p>
                    </div>
                    <div className="mt-4 md:mt-0 flex items-center gap-3">
                        {/* Source Toggle */}
                        <div className="flex bg-white rounded-lg border border-slate-200 shadow-sm p-0.5">
                            {['all', 'president', 'cabinet'].map(s => (
                                <button
                                    key={s}
                                    onClick={() => setActiveSource(s)}
                                    className={`px-3 py-1.5 text-xs font-semibold rounded-md transition-all ${activeSource === s
                                            ? 'bg-indigo-600 text-white shadow-sm'
                                            : 'text-slate-500 hover:text-slate-700'
                                        }`}
                                >
                                    {s === 'all' ? 'All' : s === 'president' ? 'President' : 'Cabinet'}
                                </button>
                            ))}
                        </div>
                        <div className="inline-flex items-center space-x-2 bg-white px-3 py-1.5 rounded-lg border border-slate-200 shadow-sm text-sm text-slate-600">
                            <Clock size={14} className="text-slate-400" />
                            <span>{pipeline.last_updated}</span>
                        </div>
                    </div>
                </header>

                {/* ═══════════════ BLOCK 1: OVERVIEW ═══════════════ */}
                <section>
                    <SectionHeader title="Overview" subtitle="High-level metrics across all petitions" icon={Activity} color="indigo" />

                    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-5">
                        <StatCard
                            title="Total Petitions"
                            value={overview.total?.toLocaleString()}
                            subtext={`President: ${overview.president_count?.toLocaleString()} • Cabinet: ${overview.cabinet_count?.toLocaleString()}`}
                            icon={FileText}
                            color="indigo"
                        />
                        <StatCard
                            title="Success Rate"
                            value={`${overview.success_rate}%`}
                            subtext="Reached 25,000 votes threshold"
                            icon={CheckCircle}
                            color="emerald"
                        />
                        <StatCard
                            title="Median Votes"
                            value={overview.median_votes?.toLocaleString()}
                            subtext="50% of petitions receive fewer"
                            icon={Users}
                            color="amber"
                        />
                        <StatCard
                            title="Response Rate"
                            value={`${overview.response_rate}%`}
                            subtext="Received an official answer"
                            icon={Info}
                            color="cyan"
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
                                { p: presPlatform, label: 'Presidential Portal', icon: '🏛️', accent: 'blue' },
                                { p: cabPlatform, label: 'Cabinet of Ministers', icon: '🏢', accent: 'teal' }
                            ].map(({ p, label, icon, accent }) => (
                                <Card key={label} className={`border-${accent}-100`}>
                                    <div className="flex items-center gap-2 mb-4">
                                        <span className="text-xl">{icon}</span>
                                        <h4 className="font-bold text-slate-800">{label}</h4>
                                    </div>
                                    <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
                                        <div>
                                            <p className="text-xs text-slate-500">Total</p>
                                            <p className="text-lg font-bold font-mono text-slate-900">{p.total?.toLocaleString()}</p>
                                        </div>
                                        <div>
                                            <p className="text-xs text-slate-500">Avg Votes</p>
                                            <p className="text-lg font-bold font-mono text-slate-900">{p.avg_votes?.toLocaleString()}</p>
                                        </div>
                                        <div>
                                            <p className="text-xs text-slate-500">Success Rate</p>
                                            <p className="text-lg font-bold font-mono text-emerald-600">{p.success_rate}%</p>
                                        </div>
                                        <div>
                                            <p className="text-xs text-slate-500">Response Rate</p>
                                            <p className="text-lg font-bold font-mono text-cyan-600">{p.response_rate}%</p>
                                        </div>
                                    </div>
                                </Card>
                            ))}
                        </div>
                    )}
                </section>

                {/* ═══════════════ BLOCK 2: DAILY DYNAMICS ═══════════════ */}
                <section>
                    <SectionHeader title="Daily Dynamics" subtitle="Since the last automated sync" icon={TrendingUp} color="emerald" />

                    <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                        {/* Today's Pulse */}
                        <Card className="bg-gradient-to-br from-emerald-50 to-white border-emerald-100 relative overflow-hidden">
                            <h3 className="text-lg font-bold text-emerald-900 mb-6 flex items-center">
                                <Calendar size={18} className="mr-2 opacity-75" /> Last 24 Hours
                            </h3>

                            <div className="grid grid-cols-2 gap-4 mb-8 relative z-10">
                                <div>
                                    <p className="text-sm text-emerald-600 font-medium">New Petitions</p>
                                    <p className="text-4xl font-extrabold text-emerald-700 font-mono">+{daily.new_petitions}</p>
                                </div>
                                <div>
                                    <p className="text-sm text-emerald-600 font-medium">Votes Added</p>
                                    <p className="text-4xl font-extrabold text-emerald-700 font-mono">+{daily.votes_added?.toLocaleString()}</p>
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
                                            <Tooltip contentStyle={CustomTooltipStyle} />
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

                        {/* Biggest Movers Table */}
                        <Card className="lg:col-span-2">
                            <div className="flex items-center justify-between mb-6">
                                <h3 className="text-lg font-bold text-slate-800">Growth Leaders (Top 5)</h3>
                                <span className="px-2 py-1 bg-green-100 text-green-700 text-xs font-bold rounded-lg uppercase tracking-wide">Trending</span>
                            </div>

                            <div className="overflow-x-auto">
                                <table className="w-full text-sm">
                                    <thead className="text-xs text-slate-400 uppercase font-medium">
                                        <tr className="border-b border-slate-100">
                                            <th className="px-4 py-3 text-left">Petition</th>
                                            <th className="px-4 py-3 text-right">Growth (24h)</th>
                                            <th className="px-4 py-3 text-right">Total Votes</th>
                                            <th className="px-4 py-3 w-32">Progress to 25k</th>
                                        </tr>
                                    </thead>
                                    <tbody className="divide-y divide-slate-50">
                                        {daily.biggest_movers?.map((m, i) => (
                                            <tr key={i} className="hover:bg-slate-50/80 transition-colors">
                                                <td className="px-4 py-3">
                                                    <a href={m.url} target="_blank" rel="noopener noreferrer"
                                                        className="font-medium text-slate-900 hover:text-indigo-600 line-clamp-1 block transition-colors"
                                                        title={m.title}>
                                                        {m.title}
                                                    </a>
                                                    <div className="flex items-center gap-2 mt-1">
                                                        <span className="text-[10px] text-slate-400">ID: {m.url?.split('/').pop()}</span>
                                                        <SourceBadge source={m.url?.includes('kmu') ? 'cabinet' : 'president'} />
                                                    </div>
                                                </td>
                                                <td className="px-4 py-3 text-right">
                                                    <span className="font-bold text-emerald-600 font-mono">+{m.delta?.toLocaleString()}</span>
                                                </td>
                                                <td className="px-4 py-3 text-right font-mono text-slate-600">
                                                    {m.total?.toLocaleString()}
                                                </td>
                                                <td className="px-4 py-3">
                                                    <ProgressBar value={m.total} max={25000} />
                                                    <div className="text-[10px] text-slate-400 text-right mt-1 font-mono">
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

                {/* ═══════════════ BLOCK 3: ENGAGEMENT & CONTENT ═══════════════ */}
                <section>
                    <SectionHeader title="Engagement & Content" subtitle="How petitions gather support and what drives votes" icon={BarChart3} color="violet" />

                    <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                        {/* Vote Distribution */}
                        <Card>
                            <h3 className="text-lg font-bold text-slate-800 mb-1">Vote Distribution</h3>
                            <p className="text-sm text-slate-500 mb-6">How hard is it to get votes?</p>
                            <div className="h-64">
                                <ResponsiveContainer width="100%" height="100%">
                                    <BarChart data={histogramWithPct}>
                                        <XAxis dataKey="bin" tick={{ fontSize: 11 }} axisLine={false} tickLine={false} />
                                        <YAxis axisLine={false} tickLine={false} fontSize={11} />
                                        <Tooltip
                                            cursor={{ fill: '#f1f5f9' }}
                                            contentStyle={CustomTooltipStyle}
                                            formatter={(value, name) => [
                                                `${value.toLocaleString()} petitions`,
                                                'Count'
                                            ]}
                                        />
                                        <Bar dataKey="count" fill={COLORS.primary} radius={[6, 6, 0, 0]} barSize={40}>
                                            {histogramWithPct.map((entry, index) => (
                                                <Cell key={`cell-${index}`} fill={index === 0 ? '#94a3b8' : index < 3 ? COLORS.primary : COLORS.emerald} />
                                            ))}
                                        </Bar>
                                    </BarChart>
                                </ResponsiveContainer>
                            </div>
                            <div className="flex flex-wrap gap-2 mt-3">
                                {histogramWithPct.map((h, i) => (
                                    <span key={i} className="text-xs text-slate-500">
                                        {h.bin}: <span className="font-semibold font-mono">{h.percentage}%</span>
                                    </span>
                                ))}
                            </div>
                        </Card>

                        {/* Status Distribution */}
                        <Card>
                            <h3 className="text-lg font-bold text-slate-800 mb-1">Status Distribution</h3>
                            <p className="text-sm text-slate-500 mb-6">Petition lifecycle stages by source</p>
                            <div className="h-64">
                                <ResponsiveContainer width="100%" height="100%">
                                    <BarChart data={statusData} layout="vertical">
                                        <XAxis type="number" axisLine={false} tickLine={false} fontSize={11} />
                                        <YAxis type="category" dataKey="status" width={130} tick={{ fontSize: 10 }} axisLine={false} tickLine={false} />
                                        <Tooltip contentStyle={CustomTooltipStyle} />
                                        <Legend iconType="circle" />
                                        <Bar dataKey="president" stackId="a" fill="#3b82f6" radius={[0, 0, 0, 0]} name="President" />
                                        <Bar dataKey="cabinet" stackId="a" fill="#0ea5e9" radius={[0, 4, 4, 0]} name="Cabinet" />
                                    </BarChart>
                                </ResponsiveContainer>
                            </div>
                        </Card>

                        {/* Scatter Plot: Text Length vs Votes */}
                        <Card>
                            <h3 className="text-lg font-bold text-slate-800 mb-1">Text Length vs Votes</h3>
                            <p className="text-sm text-slate-500 mb-6">Do longer petitions get more support?</p>
                            <div className="h-64">
                                <ResponsiveContainer width="100%" height="100%">
                                    <ScatterChart>
                                        <XAxis type="number" dataKey="x" name="Text Length" tick={{ fontSize: 10 }} axisLine={false} tickLine={false} label={{ value: 'Text Length (chars)', position: 'bottom', fontSize: 11, fill: '#94a3b8' }} />
                                        <YAxis type="number" dataKey="y" name="Votes" tick={{ fontSize: 10 }} axisLine={false} tickLine={false} label={{ value: 'Votes', angle: -90, position: 'insideLeft', fontSize: 11, fill: '#94a3b8' }} />
                                        <ZAxis range={[20, 80]} />
                                        <Tooltip contentStyle={CustomTooltipStyle} formatter={(value, name) => [value?.toLocaleString(), name]} />
                                        <Scatter
                                            data={(analytics.scatter || []).filter(s => s.source === 'president')}
                                            fill="#3b82f6"
                                            fillOpacity={0.5}
                                            name="President"
                                        />
                                        <Scatter
                                            data={(analytics.scatter || []).filter(s => s.source === 'cabinet')}
                                            fill="#0ea5e9"
                                            fillOpacity={0.7}
                                            name="Cabinet"
                                        />
                                        <Legend iconType="circle" />
                                    </ScatterChart>
                                </ResponsiveContainer>
                            </div>
                        </Card>

                        {/* Top Authors */}
                        <Card>
                            <h3 className="text-lg font-bold text-slate-800 mb-1">Top Authors</h3>
                            <p className="text-sm text-slate-500 mb-6">Most impactful petition creators by total votes</p>
                            <div className="h-64">
                                <ResponsiveContainer width="100%" height="100%">
                                    <BarChart data={(analytics.top_authors || []).slice(0, 7)} layout="vertical">
                                        <XAxis type="number" axisLine={false} tickLine={false} fontSize={10} />
                                        <YAxis type="category" dataKey="author" width={120} tick={{ fontSize: 9 }} axisLine={false} tickLine={false}
                                            tickFormatter={(v) => v.length > 18 ? v.substring(0, 18) + '…' : v}
                                        />
                                        <Tooltip
                                            contentStyle={CustomTooltipStyle}
                                            formatter={(value, name, props) => [
                                                `${value.toLocaleString()} votes (${props.payload.petitions} petitions)`,
                                                'Total Votes'
                                            ]}
                                        />
                                        <Bar dataKey="total_votes" fill={COLORS.violet} radius={[0, 6, 6, 0]} barSize={20} />
                                    </BarChart>
                                </ResponsiveContainer>
                            </div>
                        </Card>
                    </div>
                </section>

                {/* ═══════════════ BLOCK 4: DEEP ANALYSIS ═══════════════ */}
                <section>
                    <SectionHeader title="Deep Analysis" subtitle="Categorical breakdown, keyword trends, and historical patterns" icon={Layers} color="blue" />

                    <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                        {/* Category Breakdown */}
                        <Card>
                            <h3 className="text-lg font-bold text-slate-800 mb-1">Category Breakdown</h3>
                            <p className="text-sm text-slate-500 mb-6">Topic classification via keyword analysis</p>
                            <div className="space-y-3">
                                {(analytics.categories || []).map((cat, i) => (
                                    <div key={cat.category}>
                                        <div className="flex items-center justify-between mb-1">
                                            <span className="text-sm font-medium text-slate-700">{cat.category}</span>
                                            <span className="text-sm font-mono text-slate-500">
                                                {cat.count.toLocaleString()} <span className="text-xs text-slate-400">({cat.percentage}%)</span>
                                            </span>
                                        </div>
                                        <div className="w-full bg-slate-100 rounded-full h-2.5 overflow-hidden">
                                            <div
                                                className="h-2.5 rounded-full transition-all duration-700"
                                                style={{
                                                    width: `${cat.percentage}%`,
                                                    backgroundColor: CATEGORY_COLORS[i] || '#94a3b8'
                                                }}
                                            />
                                        </div>
                                    </div>
                                ))}
                            </div>
                        </Card>

                        {/* Keywords Top-10 */}
                        <Card>
                            <h3 className="text-lg font-bold text-slate-800 mb-1">Keywords Top-10</h3>
                            <p className="text-sm text-slate-500 mb-6">Most frequent words in petition titles</p>
                            <div className="h-64">
                                <ResponsiveContainer width="100%" height="100%">
                                    <BarChart data={analytics.keywords_top10 || []} layout="vertical">
                                        <XAxis type="number" axisLine={false} tickLine={false} fontSize={10} />
                                        <YAxis type="category" dataKey="word" width={100} tick={{ fontSize: 11 }} axisLine={false} tickLine={false} />
                                        <Tooltip contentStyle={CustomTooltipStyle} formatter={(v) => [`${v.toLocaleString()} occurrences`, 'Frequency']} />
                                        <Bar dataKey="count" fill={COLORS.secondary} radius={[0, 6, 6, 0]} barSize={18}>
                                            {(analytics.keywords_top10 || []).map((_, index) => (
                                                <Cell key={`cell-${index}`} fill={index < 3 ? COLORS.primary : index < 6 ? COLORS.secondary : '#94a3b8'} />
                                            ))}
                                        </Bar>
                                    </BarChart>
                                </ResponsiveContainer>
                            </div>
                        </Card>

                        {/* Monthly Timeline */}
                        <Card className="md:col-span-2">
                            <h3 className="text-lg font-bold text-slate-800 mb-1">Petition Creation Timeline</h3>
                            <p className="text-sm text-slate-500 mb-6">Volume by month since 2015 — stacked by source</p>
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
                                        <XAxis dataKey="month" tick={{ fontSize: 10 }} minTickGap={40} axisLine={false} tickLine={false} />
                                        <YAxis axisLine={false} tickLine={false} fontSize={10} />
                                        <Tooltip contentStyle={CustomTooltipStyle} />
                                        <Legend iconType="circle" />
                                        <Area type="monotone" dataKey="president" stackId="1" stroke="#3b82f6" fill="url(#colorPres)" strokeWidth={2} name="President" />
                                        <Area type="monotone" dataKey="cabinet" stackId="1" stroke="#0ea5e9" fill="url(#colorCab)" strokeWidth={2} name="Cabinet" />
                                    </AreaChart>
                                </ResponsiveContainer>
                            </div>
                        </Card>
                    </div>
                </section>

                {/* ═══════════════ BLOCK 5: VOTE VELOCITY ═══════════════ */}
                {analytics.vote_velocity && analytics.vote_velocity.length > 0 && (
                    <section>
                        <SectionHeader title="Vote Velocity" subtitle="Fastest growing active petitions (last 7 days)" icon={Zap} color="amber" />
                        <Card>
                            <div className="overflow-x-auto">
                                <table className="w-full text-sm">
                                    <thead className="text-xs text-slate-400 uppercase font-medium">
                                        <tr className="border-b border-slate-100">
                                            <th className="px-4 py-3 text-left">Petition</th>
                                            <th className="px-4 py-3 text-right">7-Day Growth</th>
                                            <th className="px-4 py-3 text-right">Daily Rate</th>
                                            <th className="px-4 py-3 text-right">Current Votes</th>
                                        </tr>
                                    </thead>
                                    <tbody className="divide-y divide-slate-50">
                                        {analytics.vote_velocity.slice(0, 5).map((v, i) => (
                                            <tr key={i} className="hover:bg-slate-50/80 transition-colors">
                                                <td className="px-4 py-3">
                                                    <a href={v.url} target="_blank" rel="noopener noreferrer"
                                                        className="font-medium text-slate-900 hover:text-indigo-600 line-clamp-1 transition-colors"
                                                        title={v.title}>
                                                        {v.title}
                                                    </a>
                                                </td>
                                                <td className="px-4 py-3 text-right font-bold text-amber-600 font-mono">
                                                    +{v.growth_7d?.toLocaleString()}
                                                </td>
                                                <td className="px-4 py-3 text-right font-mono text-slate-600">
                                                    ~{v.daily_rate?.toLocaleString()}/day
                                                </td>
                                                <td className="px-4 py-3 text-right font-mono text-slate-600">
                                                    {v.votes_current?.toLocaleString()}
                                                </td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        </Card>
                    </section>
                )}

                {/* ═══════════════ FOOTER ═══════════════ */}
                <footer className="mt-8 border-t border-slate-200 pt-8 text-slate-500 text-sm">
                    <div className="max-w-5xl mx-auto space-y-10">
                        {/* Data Freshness + Coverage */}
                        <div className="flex flex-wrap items-center justify-center gap-3">
                            <span className="inline-flex items-center gap-1.5 px-3 py-1.5 bg-emerald-50 text-emerald-700 rounded-full text-xs font-semibold">
                                <span className="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse" />
                                Auto-ETL every 24h
                            </span>
                            <span className="inline-flex items-center gap-1.5 px-3 py-1.5 bg-blue-50 text-blue-700 rounded-full text-xs font-semibold">
                                <Database size={12} />
                                {pipeline.data_span} • {pipeline.total_records?.toLocaleString()} records
                            </span>
                            <span className="inline-flex items-center gap-1.5 px-3 py-1.5 bg-violet-50 text-violet-700 rounded-full text-xs font-semibold">
                                <Layers size={12} />
                                {pipeline.coverage}
                            </span>
                        </div>

                        {/* Tech Stack + Roadmap */}
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-10 text-center">
                            <div className="flex flex-col items-center">
                                <h4 className="font-bold text-slate-800 mb-3 flex items-center justify-center">
                                    <Server size={16} className="mr-2 text-indigo-500" /> Tech Stack
                                </h4>
                                <ul className="space-y-1.5 text-sm">
                                    <li>Python ETL + DuckDB + MotherDuck</li>
                                    <li>React + Tailwind + Recharts</li>
                                    <li>GitHub Actions (automated daily sync)</li>
                                </ul>
                            </div>
                            <div className="flex flex-col items-center">
                                <h4 className="font-bold text-slate-800 mb-3 flex items-center justify-center">
                                    <Activity size={16} className="mr-2 text-emerald-500" /> Roadmap
                                </h4>
                                <ul className="space-y-1.5 text-sm">
                                    <li className="text-slate-500">Dark Mode + Premium Design (next)</li>
                                    <li className="text-indigo-600 font-medium flex items-center justify-center">
                                        <Sparkles size={14} className="mr-1 text-amber-500" />
                                        AI-powered insights assistant
                                    </li>
                                    <li className="pt-1">
                                        <a href="https://github.com/Volodymyr75/petitions/blob/main/PROJECT_STATE.md"
                                            target="_blank" rel="noopener noreferrer"
                                            className="text-slate-600 font-medium hover:text-indigo-600 flex items-center justify-center transition-colors">
                                            View Implementation Plan <ArrowRight size={14} className="ml-1" />
                                        </a>
                                    </li>
                                </ul>
                            </div>
                        </div>
                    </div>

                    <div className="mt-8 pt-6 border-t border-slate-200 flex flex-col md:flex-row items-center justify-between gap-4">
                        <div className="flex items-center space-x-6">
                            <a href="https://github.com/Volodymyr75/petitions" target="_blank" rel="noopener noreferrer"
                                className="flex items-center text-slate-500 hover:text-slate-900 transition-colors">
                                <Database size={16} className="mr-2" />
                                GitHub Repository
                            </a>
                            <a href="mailto:strembov@gmail.com"
                                className="flex items-center text-slate-500 hover:text-slate-900 transition-colors">
                                <Info size={16} className="mr-2" />
                                strembov@gmail.com
                            </a>
                        </div>
                        <div className="text-xs text-slate-400">
                            &copy; 2025-2026 Petition Analytics Project. Open Source.
                        </div>
                    </div>
                </footer>
            </div>
        </div>
    );
}
