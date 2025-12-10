import { useState } from 'react'
import './App.css'

function App() {
  const [presData, setPresData] = useState(null)
  const [presLoading, setPresLoading] = useState(false)
  const [presError, setPresError] = useState(null)

  const [cabData, setCabData] = useState(null)
  const [cabLoading, setCabLoading] = useState(false)
  const [cabError, setCabError] = useState(null)

  const fetchPresident = async () => {
    setPresLoading(true); setPresError(null); setPresData(null);
    try {
      const response = await fetch('/.netlify/functions/get_president') // Python function
      if (!response.ok) {
        const err = await response.json().catch(() => ({}));
        throw new Error(err.error || `Error ${response.status}`);
      }
      const result = await response.json()
      setPresData(result)
    } catch (err) {
      setPresError(err.message)
    } finally {
      setPresLoading(false)
    }
  }

  const fetchCabinet = async () => {
    setCabLoading(true); setCabError(null); setCabData(null);
    try {
      const response = await fetch('/.netlify/functions/get_cabinet') // Python function
      if (!response.ok) {
        const err = await response.json().catch(() => ({}));
        throw new Error(err.error || `Error ${response.status}`);
      }
      const result = await response.json()
      setCabData(result)
    } catch (err) {
      setCabError(err.message)
    } finally {
      setCabLoading(false)
    }
  }

  return (
    <div className="container">
      <h1>🇺🇦 Govt. Petition Monitor</h1>

      <div className="grid">
        {/* President Section */}
        <div className="column">
          <h2>🏛️ President</h2>
          <button onClick={fetchPresident} disabled={presLoading}>
            {presLoading ? 'Scraping...' : 'Load President Petitions'}
          </button>

          {presError && <div className="error">{presError}</div>}

          {presData && (
            <div className="results">
              <p><strong>Source:</strong> {presData.source}</p>
              <div className="list">
                {presData.data.map((p) => (
                  <div key={p.id} className="item">
                    <a href={p.url} target="_blank" rel="noreferrer"><strong>{p.number}</strong></a>
                    <p>{p.title}</p>
                    <div className="meta">
                      <span>📅 {p.date}</span>
                      <span>✍️ {p.votes}</span>
                    </div>
                    <small>{p.status}</small>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>

        {/* Cabinet Section */}
        <div className="column">
          <h2>🏢 Cabinet (KMU)</h2>
          <button onClick={fetchCabinet} disabled={cabLoading}>
            {cabLoading ? 'Fetching API...' : 'Load Cabinet Petitions'}
          </button>

          {cabError && <div className="error">{cabError}</div>}

          {cabData && (
            <div className="results">
              <p><strong>Source:</strong> {cabData.source}</p>
              <div className="list">
                {cabData.data.map((p) => (
                  <div key={p.id} className="item">
                    <a href={p.url} target="_blank" rel="noreferrer"><strong>{p.number}</strong></a>
                    <p>{p.title}</p>
                    <div className="meta">
                      <span>📅 {new Date(p.date).toLocaleDateString('uk-UA')}</span>
                      <span>✍️ {p.votes}</span>
                    </div>
                    <small>{p.status}</small>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

export default App
