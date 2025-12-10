import { useState } from 'react'
import './App.css'

function App() {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  const fetchData = async () => {
    setLoading(true)
    setError(null)
    try {
      // Call our own Netlify function
      const response = await fetch('/.netlify/functions/get-petitions')

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}))
        throw new Error(errorData.error || `Server responded with ${response.status}`)
      }

      const result = await response.json()
      setData(result)
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="container">
      <h1>🇺🇦 Ukrainian E-Petitions Monitor</h1>
      <div className="card">
        <p>
          Click below to test connection to <code>data.gov.ua</code> via Netlify Functions.
        </p>
        <button onClick={fetchData} disabled={loading}>
          {loading ? 'Loading...' : 'Load Latest Petitions Data'}
        </button>
      </div>

      {error && <div className="error">Error: {error}</div>}

      {data && (
        <div className="results">
          <h3>Source: {data.source_package}</h3>
          <p><small>Resource: {data.resource_url}</small></p>

          <pre>{JSON.stringify(data.data, null, 2)}</pre>
        </div>
      )}
    </div>
  )
}

export default App
