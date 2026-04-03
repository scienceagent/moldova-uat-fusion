import { useEffect, useState, useCallback, useRef, useMemo } from "react";
import {
  MapContainer, TileLayer, GeoJSON, useMap, useMapEvents
} from "react-leaflet";
import L from "leaflet";
import "leaflet/dist/leaflet.css";
import axios from "axios";

const API = "http://127.0.0.1:8000";

/* ─── Helpers ─── */
const fmt = (n) => {
  if (n == null) return "—";
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + "M";
  if (n >= 1_000) return (n / 1_000).toFixed(1) + "K";
  return n.toLocaleString("ro-MD");
};

const fmtMDL = (n) => {
  if (n == null) return "—";
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(2) + " mln MDL";
  if (n >= 1_000) return (n / 1_000).toFixed(0) + " mii MDL";
  return n + " MDL";
};

const getColor = (p) => {
  return p > 100000 ? '#00441b' :
         p > 50000  ? '#006d2c' :
         p > 20000  ? '#238b45' :
         p > 10000  ? '#41ae76' :
         p > 5000   ? '#66c2a4' :
         p > 3000   ? '#99d8c9' :
         p > 1000   ? '#ccece6' :
                      '#f7fcfd';
};

/* ─── Components ─── */
function BudgetBar({ label, amount, maxAmount, className }) {
  const pct = maxAmount > 0 ? Math.min((amount / maxAmount) * 100, 100) : 0;
  return (
    <div className="budget-bar">
      <div className="bar-header">
        <span>{label}</span>
        <span className="amount">{fmtMDL(amount)}</span>
      </div>
      <div className="bar-track">
        <div className={`bar-fill ${className}`} style={{ width: `${pct}%` }} />
      </div>
    </div>
  );
}

function UATDetail({ uat, onClose }) {
  if (!uat) return null;
  const b = uat.budget || uat;
  const maxVal = Math.max(
    b.total_revenues || 0, b.total_expenditures || 0, 1
  );
  const ownPct = b.total_revenues > 0
    ? ((b.own_revenues / b.total_revenues) * 100).toFixed(0)
    : 0;
  const canPayStaff = (b.own_revenues || 0) >= (b.personnel_costs || 0);

  return (
    <div className="uat-detail">
      <div style={{ display: "flex", justifyContent: "space-between" }}>
        <div className="uat-name">{uat.name}</div>
        <button className="btn btn-outline" onClick={onClose}
          style={{ padding: "2px 8px", fontSize: "11px" }}>✕</button>
      </div>
      <div className="uat-meta">
        <span className="tag">{uat.type}</span>
        <span>{uat.raion}</span>
        <span>👥 {fmt(uat.population)}</span>
        {uat.merged_count > 1 && (
          <span className="tag" style={{ background: "var(--green-glow)", color: "var(--green)" }}>
            +{uat.merged_count - 1} unități
          </span>
        )}
      </div>

      <div className="budget-section">
        <BudgetBar label="Venituri totale" amount={b.total_revenues}
          maxAmount={maxVal} className="revenue" />
        <BudgetBar label={`Venituri proprii (${ownPct}%)`}
          amount={b.own_revenues} maxAmount={b.total_revenues || 1}
          className="own-revenue" />
        <BudgetBar label="Cheltuieli totale" amount={b.total_expenditures}
          maxAmount={maxVal} className="expenditure" />
        <BudgetBar label="Cheltuieli Personal" amount={b.personnel_costs}
          maxAmount={b.total_expenditures || 1} className="personnel" />
      </div>

      <div style={{
        marginTop: 10, fontSize: 11, padding: "8px 12px",
        borderRadius: 6,
        background: canPayStaff ? "var(--green-glow)" : "var(--red-glow)",
        color: canPayStaff ? "var(--green)" : "var(--red)",
        fontWeight: 600, border: `1px solid ${canPayStaff ? 'rgba(72,187,120,0.3)' : 'rgba(252,129,129,0.3)'}`
      }}>
        {canPayStaff
          ? "✓ SUSTENABIL: Veniturile proprii acoperă salariile"
          : "✗ INSUSTENABIL: Necesită subvenții pentru salarii"}
      </div>
    </div>
  );
}

function MapController({ selected, initialBounds }) {
  const map = useMap();

  // Initial fit
  useEffect(() => {
    if (initialBounds) {
      map.fitBounds(initialBounds, { animate: false });
    }
  }, [initialBounds, map]);

  // Zoom to selection
  useEffect(() => {
    if (selected && selected.id) {
      // Find the layer for this UAT to get its bounds
      // We search through the map's layers
      map.eachLayer((layer) => {
        if (layer.feature && layer.feature.properties.id === selected.id) {
          const bounds = layer.getBounds();
          if (bounds.isValid()) {
            map.fitBounds(bounds, { padding: [50, 50], maxZoom: 13 });
          }
        }
      });
    }
  }, [selected, map]);

  return null;
}

function MapEvents({ onMapClick }) {
  useMapEvents({
    click: (e) => {
      // Only deselect if we clicked the map background, not a polygon
      // Leaflet polygons handle their own click and usually propagate
      // But we use stopPropagation in the polygon handler.
      onMapClick();
    },
  });
  return null;
}

/* ─── Main App ─── */
export default function App() {
  const [stats, setStats] = useState(null);
  const [boundaries, setBoundaries] = useState(null);
  const [scenario, setScenario] = useState("current");
  const [selected, setSelected] = useState(null);
  const [loading, setLoading] = useState(true);
  const [selectedRaion, setSelectedRaion] = useState("");
  const geoJsonRef = useRef(null);
  const mapRef = useRef(null);

  // Load Initial Stats
  useEffect(() => {
    axios.get(`${API}/uat/stats`).then(r => setStats(r.data)).catch(e => console.error(e));
  }, []);

  const loadScenario = useCallback(async (type) => {
    setLoading(true);
    try {
      let endpoint = type === "current" 
        ? "/uat/boundaries/polygons" 
        : `/uat/boundaries/amalgamated?threshold=${type}`;
      
      const res = await axios.get(`${API}${endpoint}`);
      setBoundaries(res.data);
      setSelected(null);
      setSelectedRaion("");
    } catch (e) {
      console.error(e);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadScenario(scenario);
  }, [scenario, loadScenario]);

  // Derive Raions and UATs for dropdowns
  const raions = useMemo(() => {
    if (!boundaries) return [];
    const set = new Set();
    boundaries.features.forEach(f => {
      if (f.properties.raion) set.add(f.properties.raion);
    });
    return Array.from(set).sort();
  }, [boundaries]);

  const filteredUats = useMemo(() => {
    if (!boundaries || !selectedRaion) return [];
    return boundaries.features
      .filter(f => f.properties.raion === selectedRaion)
      .map(f => f.properties)
      .sort((a, b) => a.name.localeCompare(b.name));
  }, [boundaries, selectedRaion]);

  const selectUatById = useCallback((uatId) => {
    if (!boundaries) return;
    const feat = boundaries.features.find(f => f.properties.id === uatId);
    if (feat) {
      setSelected(feat.properties);
    }
  }, [boundaries]);

  const onEachFeature = (feature, layer) => {
    const p = feature.properties;
    layer.on({
      click: (e) => {
        L.DomEvent.stopPropagation(e);
        setSelected(p);
        layer.bringToFront();
      },
      mouseover: (e) => {
        layer.setStyle({ fillOpacity: 0.9, weight: 2, color: '#fff' });
      },
      mouseout: (e) => {
        layer.setStyle(getStyle(feature));
      }
    });

    layer.bindTooltip(`<strong>${p.name}</strong><br/>${fmt(p.population)} loc.`, {
      sticky: true,
      className: 'uat-tooltip'
    });
  };

  const getStyle = useCallback((feature) => {
    const p = feature.properties;
    const isSelected = selected?.id === p.id;
    return {
      fillColor: isSelected ? "#ffff00" : getColor(p.population),
      weight: isSelected ? 4 : 1,
      opacity: 1,
      color: isSelected ? "#ffffff" : "rgba(255,255,255,0.2)",
      fillOpacity: isSelected ? 1.0 : 0.7,
    };
  }, [selected]);

  // Effect to update styles when 'selected' changes without re-rendering whole GeoJSON
  useEffect(() => {
    if (geoJsonRef.current) {
      geoJsonRef.current.setStyle(getStyle);
    }
  }, [selected, getStyle]);

  const featureCount = boundaries?.features?.length || 0;

  return (
    <>
      <div className="sidebar">
        <div className="sidebar-header">
          <h1><span className="flag">🇲🇩</span> Moldova UAT Fusion</h1>
          <div className="subtitle">Vizualizare Poligoane Comune & Orașe</div>
        </div>

        <div className="scenario-toggle">
          {["current", "3000", "5000"].map(s => (
            <button key={s} 
              className={`scenario-btn ${scenario === s ? "active" : ""}`}
              onClick={() => setScenario(s)}
            >
              {s === "current" ? "Actual" : `Prag ${s}`}
            </button>
          ))}
        </div>

        {/* ── Selection Lists ── */}
        <div className="selector-group">
          <div className="select-wrapper">
             <label>Selectează Raion</label>
             <select 
               value={selectedRaion} 
               onChange={(e) => setSelectedRaion(e.target.value)}
               className="custom-select"
             >
               <option value="">— Alege Raion —</option>
               {raions.map(r => <option key={r} value={r}>{r}</option>)}
             </select>
          </div>

          {selectedRaion && (
            <div className="select-wrapper">
              <label>Selectează Comună/Oraș</label>
              <select 
                value={selected?.id || ""} 
                onChange={(e) => selectUatById(e.target.value)}
                className="custom-select"
              >
                <option value="">— Alege Localitate —</option>
                {filteredUats.map(u => (
                  <option key={u.id} value={u.id}>{u.name}</option>
                ))}
              </select>
            </div>
          )}
        </div>

        <div className="sidebar-content">
          {stats && (
            <div className="stats-grid">
              <div className="stat-card">
                <div className="label">UAT-uri active</div>
                <div className="value accent">{featureCount}</div>
              </div>
              <div className="stat-card">
                <div className="label">Populație Totală</div>
                <div className="value">{fmt(stats.total_population)}</div>
              </div>
            </div>
          )}

          {selected ? (
            <UATDetail uat={selected} onClose={() => setSelected(null)} />
          ) : (
            <div className="placeholder">
              <div className="icon">🗺️</div>
              <div>Selectează o unitate de pe hartă sau din listă</div>
              <div style={{marginTop: 8, fontSize: 11}}>
                {scenario === "current" ? "Toate cele 895 UAT-uri sunt active." : `Simulare: Toate comunele sub ${scenario} locuitori sunt comasate.`}
              </div>
            </div>
          )}
        </div>
      </div>

      <div className="map-container">
        {loading && <div className="loading-overlay">Se încarcă datele geografice...</div>}
        <MapContainer
          center={[47.0, 28.5]} zoom={8}
          ref={mapRef}
          style={{ height: "100%", width: "100%" }}
        >
          <TileLayer url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png" />
          <MapController 
            selected={selected} 
            initialBounds={[[45.5, 26.5], [48.5, 30.2]]} 
          />
          <MapEvents onMapClick={() => setSelected(null)} />

          {boundaries && (
            <GeoJSON 
              key={scenario}
              ref={geoJsonRef}
              data={boundaries} 
              style={getStyle}
              onEachFeature={onEachFeature}
            />
          )}
        </MapContainer>

        <div className="map-legend">
          <h4>Populație</h4>
          <div className="legend-item"><div className="legend-dot" style={{background:'#00441b'}}/> &gt; 100k</div>
          <div className="legend-item"><div className="legend-dot" style={{background:'#238b45'}}/> 20k - 50k</div>
          <div className="legend-item"><div className="legend-dot" style={{background:'#41ae76'}}/> 10k - 20k</div>
          <div className="legend-item"><div className="legend-dot" style={{background:'#66c2a4'}}/> 5k - 10k</div>
          <div className="legend-item"><div className="legend-dot" style={{background:'#99d8c9'}}/> 3k - 5k</div>
          <div className="legend-item"><div className="legend-dot" style={{background:'#ccece6'}}/> &lt; 3k</div>
        </div>
      </div>
    </>
  );
}