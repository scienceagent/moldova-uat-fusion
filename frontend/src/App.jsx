import { useEffect, useState } from "react";
import { MapContainer, TileLayer, GeoJSON } from "react-leaflet";
import "leaflet/dist/leaflet.css";
import axios from "axios";

const API = "http://127.0.0.1:8000";

export default function App() {
  const [master, setMaster] = useState(null);
  const [geo, setGeo] = useState(null);
  const [selected, setSelected] = useState(null);

  useEffect(() => {
    axios.get(`${API}/uat/master`).then((r) => setMaster(r.data)).catch(console.error);
    axios.get(`${API}/uat/boundaries`).then((r) => setGeo(r.data)).catch(console.error);
  }, []);

  const onEachFeature = (feature, layer) => {
    const name =
      feature?.properties?.name ||
      feature?.properties?.NAME_2 ||
      feature?.properties?.NAME_1 ||
      "UAT necunoscut";

    layer.bindTooltip(name);

    layer.on({
      click: () => {
        setSelected({
          name,
          ...feature.properties,
        });
      },
    });
  };

  const style = () => ({
    color: "#4c6fff",
    weight: 1.2,
    fillColor: "#6f8cff",
    fillOpacity: 0.2,
  });

  return (
    <div style={{ display: "grid", gridTemplateColumns: "340px 1fr", height: "100vh" }}>
      <aside style={{ padding: 16, borderRight: "1px solid #2a2a2a", overflow: "auto" }}>
        <h2 style={{ marginTop: 0 }}>Moldova UAT Fusion</h2>
        <p><b>Updated:</b> {master?.updated_at || "-"}</p>
        <p><b>Admin pages:</b> {master?.admin_pages_count ?? "-"}</p>
        <p><b>Boundaries:</b> {geo?.features?.length ?? 0}</p>

        <hr />

        <h3>UAT selectat</h3>
        {!selected ? (
          <p>Click pe hartă pe un poligon UAT.</p>
        ) : (
          <pre style={{ whiteSpace: "pre-wrap", fontSize: 12 }}>
            {JSON.stringify(selected, null, 2)}
          </pre>
        )}

        <hr />

        <h3>Pagini administrative (primele 10)</h3>
        <ul>
          {(master?.admin_pages || []).slice(0, 10).map((p, i) => (
            <li key={i}>
              <a href={p.url} target="_blank" rel="noreferrer">
                {p.title || p.url}
              </a>
            </li>
          ))}
        </ul>
      </aside>

      <MapContainer center={[47.2, 28.4]} zoom={7} style={{ height: "100%", width: "100%" }}>
        <TileLayer
          attribution="&copy; OpenStreetMap contributors"
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        />
        {geo && <GeoJSON data={geo} style={style} onEachFeature={onEachFeature} />}
      </MapContainer>
    </div>
  );
}