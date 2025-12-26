import { useState } from 'react';
import {
    BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer,
    AreaChart, Area, ScatterChart, Scatter, ZAxis, Legend
} from 'recharts';
import {
    TrendingUp, Users, FileText, CheckCircle, AlertCircle,
    Clock, Server, Database, Activity, Info
} from 'lucide-react';
import data from './analytics_data.json';

const Card = ({ children, className = "" }) => (
    <div className={`bg-white rounded-xl shadow-sm border border-slate-200 p-6 ${className}`}>
        {children}
    </div>
);

const StatCard = ({ title, value, subtext, icon: Icon, color = "blue" }) => (
    <Card>
        <div className="flex items-start justify-between">
            <div>
                <p className="text-sm font-medium text-slate-500 mb-1">{title}</p>
                <h3 className="text-2xl font-bold text-slate-900">{value}</h3>
                {subtext && <p className="text-xs text-slate-400 mt-2">{subtext}</p>}
            </div>
            <div className={`p-3 rounded-lg bg-${color}-50 text-${color}-600`}>
                <Icon size={20} />
            </div>
        </div>
    </Card>
);

export default function Dashboard() {
    const { overview, daily, analytics, pipeline } = data;

    return (
        <div className="min-h-screen bg-slate-50 p-8 font-sans text-slate-900">
            <div className="max-w-7xl mx-auto space-y-8">

                {/* HEADER */}
                <header className="mb-8">
                    <h1 className="text-3xl font-bold text-slate-900 tracking-tight">Petition Analytics</h1>
                    <p className="text-slate-500 mt-2">Comprehensive analysis of Ukrainian electronic petitions (President & Cabinet)</p>
                </header>

                {/* BLOCK 1: OVERVIEW */}
                <section className="space-y-4">
                    <div className="flex items-center space-x-2 mb-2">
                        <Activity className="text-blue-600" size={20} />
                        <h2 className="text-xl font-semibold text-slate-800">Overview</h2>
                    </div>

                    <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                        <StatCard
                            title="Total Petitions"
                            value={overview.total.toLocaleString()}
                            subtext={`${overview.president_count.toLocaleString()} President • ${overview.cabinet_count.toLocaleString()} Cabinet`}
                            icon={FileText}
                            color="indigo"
                        />
                        <StatCard
                            title="Success Rate"
                            value={`${overview.success_rate}%`}
                            subtext="reached 25,000 votes"
                            icon={CheckCircle}
                            color="green"
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
                            subtext="received official answer"
                            icon={Info}
                            color="cyan"
                        />
                    </div>

                    <div className="bg-blue-50 border border-blue-100 rounded-lg p-4 flex items-start space-x-3">
                        <Info className="text-blue-600 min-w-5 mt-0.5" size={20} />
                        <p className="text-sm text-blue-800">
                            <strong>Data Insight:</strong> {overview.insight}
                        </p>
                    </div>
                </section>

                {/* BLOCK 2: DAILY DYNAMICS */}
                <section className="space-y-4">
                    <div className="flex items-center space-x-2 mb-2">
                        <TrendingUp className="text-emerald-600" size={20} />
                        <h2 className="text-xl font-semibold text-slate-800">Daily Dynamics</h2>
                    </div>

                    <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                        <Card className="lg:col-span-1 bg-gradient-to-br from-emerald-50 to-white border-emerald-100">
                            <h3 className="text-lg font-semibold text-emerald-900 mb-4">Last 24 Hours</h3>
                            <div className="space-y-4">
                                <div className="flex justify-between items-center border-b border-emerald-100 pb-2">
                                    <span className="text-emerald-700">New Petitions</span>
                                    <span className="font-bold text-2xl text-emerald-600">+{daily.new_petitions}</span>
                                </div>
                                <div className="flex justify-between items-center border-b border-emerald-100 pb-2">
                                    <span className="text-emerald-700">Votes Added</span>
                                    <span className="font-bold text-2xl text-emerald-600">+{daily.votes_added}</span>
                                </div>
                                <div className="text-xs text-emerald-500 mt-2">
                                    * Updates occur daily at 23:00 UTC
                                </div>
                            </div>
                        </Card>

                        <Card className="lg:col-span-2">
                            <h3 className="text-lg font-semibold text-slate-800 mb-4">Biggest Movers (Top Growth)</h3>
                            {daily.biggest_movers.length > 0 ? (
                                <div className="overflow-x-auto">
                                    <table className="w-full text-sm text-left">
                                        <thead className="text-xs text-slate-500 uppercase bg-slate-50">
                                            <tr>
                                                <th className="px-4 py-2">Change</th>
                                                <th className="px-4 py-2">Total</th>
                                                <th className="px-4 py-2">Petition</th>
                                            </tr>
                                        </thead>
                                        <tbody>
                                            {daily.biggest_movers.map((m, i) => (
                                                <tr key={i} className="border-b border-slate-100 last:border-0 hover:bg-slate-50">
                                                    <td className="px-4 py-3 font-bold text-green-600">+{m.delta}</td>
                                                    <td className="px-4 py-3 text-slate-600">{m.total.toLocaleString()}</td>
                                                    <td className="px-4 py-3 truncate max-w-xs">
                                                        <a href={m.url} target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:underline">
                                                            {m.title}
                                                        </a>
                                                    </td>
                                                </tr>
                                            ))}
                                        </tbody>
                                    </table>
                                </div>
                            ) : (
                                <div className="h-32 flex items-center justify-center text-slate-400">
                                    Waiting for next daily update cycle...
                                </div>
                            )}
                        </Card>
                    </div>
                </section>

                {/* BLOCK 3: DEEP ANALYTICS */}
                <section className="space-y-4">
                    <div className="flex items-center space-x-2 mb-2">
                        <Database className="text-indigo-600" size={20} />
                        <h2 className="text-xl font-semibold text-slate-800">Deep Analytics</h2>
                    </div>

                    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                        <Card>
                            <h3 className="text-lg font-semibold text-slate-800 mb-4">Vote Distribution</h3>
                            <p className="text-xs text-slate-500 mb-4">Logarithmic breakdown of vote counts</p>
                            <div className="h-64">
                                <ResponsiveContainer width="100%" height="100%">
                                    <BarChart data={analytics.histogram}>
                                        <XAxis dataKey="bin" tick={{ fontSize: 12 }} />
                                        <YAxis />
                                        <Tooltip
                                            contentStyle={{ borderRadius: '8px', border: 'none', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)' }}
                                        />
                                        <Bar dataKey="count" fill="#4f46e5" radius={[4, 4, 0, 0]} />
                                    </BarChart>
                                </ResponsiveContainer>
                            </div>
                        </Card>

                        <Card>
                            <h3 className="text-lg font-semibold text-slate-800 mb-4">Historical Activity</h3>
                            <p className="text-xs text-slate-500 mb-4">Number of petitions created over time</p>
                            <div className="h-64">
                                <ResponsiveContainer width="100%" height="100%">
                                    <AreaChart data={analytics.timeline}>
                                        <XAxis dataKey="month" tick={{ fontSize: 10 }} minTickGap={30} />
                                        <YAxis />
                                        <Tooltip />
                                        <Legend />
                                        <Area type="monotone" dataKey="president" stackId="1" stroke="#3b82f6" fill="#3b82f6" />
                                        <Area type="monotone" dataKey="cabinet" stackId="1" stroke="#0ea5e9" fill="#0ea5e9" />
                                    </AreaChart>
                                </ResponsiveContainer>
                            </div>
                        </Card>

                        <Card className="lg:col-span-2">
                            <h3 className="text-lg font-semibold text-slate-800 mb-4">Text Length vs. Votes</h3>
                            <p className="text-xs text-slate-500 mb-4">Sample of 300 petitions</p>
                            <div className="h-64">
                                <ResponsiveContainer width="100%" height="100%">
                                    <ScatterChart margin={{ top: 20, right: 20, bottom: 20, left: 20 }}>
                                        <XAxis type="number" dataKey="x" name="Chars" unit=" chars" />
                                        <YAxis type="number" dataKey="y" name="Votes" />
                                        <ZAxis type="category" dataKey="has_answer" name="Answered" />
                                        <Tooltip cursor={{ strokeDasharray: '3 3' }} />
                                        <Scatter name="Petitions" data={analytics.scatter} fill="#8884d8" shape="circle" />
                                    </ScatterChart>
                                </ResponsiveContainer>
                            </div>
                        </Card>
                    </div>
                </section>

                {/* BLOCK 4: PIPELINE INFO */}
                <footer className="mt-12 bg-white border-t border-slate-200 p-8 text-center text-slate-500 text-sm">
                    <div className="flex flex-col md:flex-row justify-center items-center space-y-4 md:space-y-0 md:space-x-8 mb-6">
                        <div className="flex items-center space-x-2">
                            <Server size={16} />
                            <span>Pipeline: <strong>Python + DuckDB</strong></span>
                        </div>
                        <div className="flex items-center space-x-2">
                            <Clock size={16} />
                            <span>Last Updated: <strong>{pipeline.last_updated}</strong></span>
                        </div>
                        <div className="flex items-center space-x-2">
                            <Database size={16} />
                            <span>DB Size: <strong>{pipeline.db_size_mb} MB</strong></span>
                        </div>
                    </div>

                    <div className="max-w-xl mx-auto border-t border-slate-100 pt-6">
                        <div className="flex items-center justify-center space-x-2 text-indigo-600 mb-2">
                            <Activity size={16} />
                            <span className="font-semibold">Coming Soon</span>
                        </div>
                        <p>AI Agent integration to answer questions like "Which petitions about taxes are trending?"</p>
                    </div>
                </footer>

            </div>
        </div>
    );
}
