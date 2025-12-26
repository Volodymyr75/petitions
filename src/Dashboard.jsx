```javascript
import { useState } from 'react';
import { 
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, 
  AreaChart, Area, ScatterChart, Scatter, ZAxis, Legend 
} from 'recharts';
    const { totals, top_president, top_cabinet, generated_at } = analyticsData;

    // KPIs
    const totalCount = (totals.president || 0) + (totals.cabinet || 0);
    const presCount = totals.president || 0;
    const cabCount = totals.cabinet || 0;

    // Data for Pie Chart
    const pieData = [
        { name: 'President', value: presCount, color: '#0088FE' },
        { name: 'Cabinet', value: cabCount, color: '#00C49F' },
    ];

    // Helper for Top List
    const TopList = ({ data, title, icon: Icon }) => (
        <div className="bg-white p-4 rounded-lg shadow-sm border border-slate-200">
            <div className="flex items-center gap-2 mb-4 border-b pb-2">
                <Icon className="w-5 h-5 text-blue-600" />
                <h3 className="font-semibold text-slate-700">{title}</h3>
            </div>
            <div className="space-y-3">
                {data.slice(0, 5).map((item, idx) => (
                    <a
                        key={idx}
                        href={item.url}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="block group"
                    >
                        <div className="flex justify-between items-start">
                            <span className="text-sm font-medium text-slate-500 w-6">#{idx + 1}</span>
                            <div className="flex-1">
                                <p className="text-sm text-slate-800 line-clamp-2 group-hover:text-blue-600 transition-colors">
                                    {item.title}
                                </p>
                                <div className="flex items-center gap-2 mt-1 text-xs text-slate-400">
                                    <span>{item.date}</span>
                                    <span className={`px - 1.5 py - 0.5 rounded - full ${
    item.status.includes('відповіддю') ? 'bg-green-100 text-green-700' : 'bg-gray-100 text-gray-600'
} `}>
                                        {item.status}
                                    </span>
                                </div>
                            </div>
                            <div className="text-right pl-2">
                                <span className="block text-sm font-bold text-slate-700">
                                    {item.votes.toLocaleString()}
                                </span>
                                <span className="text-[10px] text-slate-400">votes</span>
                            </div>
                        </div>
                    </a>
                ))}
            </div>
        </div>
    );

    return (
        <div className="max-w-6xl mx-auto p-6 bg-slate-50 min-h-screen">
            <header className="mb-8">
                <h1 className="text-3xl font-bold text-slate-800 mb-2">📊 Analytics Dashboard</h1>
                <p className="text-sm text-slate-500">
                    Last updated: {generated_at} | Source: Active & Archived Petitions
                </p>
            </header>

            {/* KPI Cards */}
            <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-8">
                <div className="bg-white p-4 rounded-lg shadow-sm border border-slate-200 flex items-center gap-4">
                    <div className="p-3 bg-blue-100 rounded-full text-blue-600">
                        <Activity size={24} />
                    </div>
                    <div>
                        <p className="text-xs text-slate-500 uppercase font-semibold">Total Petitions</p>
                        <p className="text-2xl font-bold text-slate-800">{totalCount.toLocaleString()}</p>
                    </div>
                </div>

                <div className="bg-white p-4 rounded-lg shadow-sm border border-slate-200 flex items-center gap-4">
                    <div className="p-3 bg-indigo-100 rounded-full text-indigo-600">
                        <FileText size={24} />
                    </div>
                    <div>
                        <p className="text-xs text-slate-500 uppercase font-semibold">President's Office</p>
                        <p className="text-2xl font-bold text-slate-800">{presCount.toLocaleString()}</p>
                    </div>
                </div>

                <div className="bg-white p-4 rounded-lg shadow-sm border border-slate-200 flex items-center gap-4">
                    <div className="p-3 bg-teal-100 rounded-full text-teal-600">
                        <FileText size={24} />
                    </div>
                    <div>
                        <p className="text-xs text-slate-500 uppercase font-semibold">Cabinet of Ministers</p>
                        <p className="text-2xl font-bold text-slate-800">{cabCount.toLocaleString()}</p>
                    </div>
                </div>

                <div className="bg-white p-4 rounded-lg shadow-sm border border-slate-200 flex items-center gap-4">
                    <div className="p-3 bg-amber-100 rounded-full text-amber-600">
                        <ThumbsUp size={24} />
                    </div>
                    <div>
                        <p className="text-xs text-slate-500 uppercase font-semibold">Top Vote</p>
                        <p className="text-2xl font-bold text-slate-800">
                            {Math.max(
                                ...top_president.map(p => p.votes),
                                ...top_cabinet.map(p => p.votes)
                            ).toLocaleString()}
                        </p>
                    </div>
                </div>
            </div>

            {/* TRENDING SECTION (New) */}
            {analyticsData.trending && analyticsData.trending.length > 0 && (
                <div className="mb-8">
                    <div className="flex items-center gap-2 mb-4">
                        <div className="p-2 bg-pink-100 rounded-lg text-pink-600">
                            <Activity size={20} />
                        </div>
                        <h3 className="text-xl font-bold text-slate-800">🚀 Trending Today (Live Updates)</h3>
                    </div>
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                        {analyticsData.trending.slice(0, 3).map((item, idx) => (
                            <div key={idx} className="bg-gradient-to-br from-white to-pink-50 p-4 rounded-xl border border-pink-100 shadow-sm">
                                <div className="flex justify-between items-start mb-2">
                                    <span className="bg-pink-100 text-pink-700 text-xs font-bold px-2 py-1 rounded-full">
                                        +{item.delta} votes
                                    </span>
                                    <span className="text-slate-400 text-xs">Total: {item.total.toLocaleString()}</span>
                                </div>
                                <a href={item.url} target="_blank" rel="noreferrer" className="font-semibold text-slate-800 hover:text-blue-600 line-clamp-2">
                                    {item.title}
                                </a>
                            </div>
                        ))}
                    </div>
                </div>
            )}

            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-8">
                {/* Source Distribution */}
                <div className="bg-white p-4 rounded-lg shadow-sm border border-slate-200 lg:col-span-1">
                    <h3 className="font-semibold text-slate-700 mb-4">Platform Distribution</h3>
                    <div className="h-64">
                        <ResponsiveContainer width="100%" height="100%">
                            <PieChart>
                                <Pie
                                    data={pieData}
                                    cx="50%"
                                    cy="50%"
                                    innerRadius={60}
                                    outerRadius={80}
                                    paddingAngle={5}
                                    dataKey="value"
                                >
                                    {pieData.map((entry, index) => (
                                        <Cell key={`cell - ${ index } `} fill={entry.color} />
                                    ))}
                                </Pie>
                                <Tooltip />
                                <Legend verticalAlign="bottom" height={36} />
                            </PieChart>
                        </ResponsiveContainer>
                    </div>
                </div>

                {/* Voting Power Comparison */}
                <div className="bg-white p-4 rounded-lg shadow-sm border border-slate-200 lg:col-span-2">
                    <h3 className="font-semibold text-slate-700 mb-4">Top 5 Petitions (Votes Comparison)</h3>
                    <div className="h-64">
                        <ResponsiveContainer width="100%" height="100%">
                            <BarChart
                                data={[
                                    ...top_cabinet.slice(0, 3).map(i => ({ name: 'Cab: ' + i.title.substring(0, 10) + '...', votes: i.votes, full: i })),
                                    ...top_president.slice(0, 3).map(i => ({ name: 'Pres: ' + i.title.substring(0, 10) + '...', votes: i.votes, full: i }))
                                ].sort((a, b) => b.votes - a.votes)}
                                layout="vertical"
                                margin={{ top: 5, right: 30, left: 40, bottom: 5 }}
                            >
                                <XAxis type="number" hide />
                                <YAxis type="category" dataKey="name" width={150} tick={{ fontSize: 12 }} />
                                <Tooltip
                                    content={({ payload }) => {
                                        if (payload && payload.length) {
                                            const data = payload[0].payload;
                                            return (
                                                <div className="bg-white p-2 border shadow-lg rounded max-w-xs">
                                                    <p className="font-bold text-sm">{data.full.title}</p>
                                                    <p className="text-blue-600">{data.votes.toLocaleString()} votes</p>
                                                </div>
                                            );
                                        }
                                        return null;
                                    }}
                                />
                                <Bar dataKey="votes" fill="#8884d8" radius={[0, 4, 4, 0]}>
                                    {
                                        [...top_cabinet.slice(0, 3), ...top_president.slice(0, 3)]
                                            .sort((a, b) => b.votes - a.votes)
                                            .map((entry, index) => (
                                                <Cell key={`cell - ${ index } `} fill={entry.source === 'president' ? '#0088FE' : '#00C49F'} />
                                            ))
                                    }
                                </Bar>
                            </BarChart>
                        </ResponsiveContainer>
                    </div>
                </div>
            </div>

            {/* Lists */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                <TopList data={top_president} title="🏛️ Top President Petitions" icon={CheckCircle} />
                <TopList data={top_cabinet} title="🏢 Top Cabinet Petitions" icon={CheckCircle} />
            </div>
        </div>
    );
};

export default Dashboard;
