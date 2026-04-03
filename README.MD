## Live Demo

Try the application here:  
🔗 **https://scienceagent.github.io/moldova-uat-fusion/**

## Local Development

### Backend
```bash
python -m venv venv
# Windows
venv\Scripts\Activate.ps1
# Linux/macOS
# source venv/bin/activate

python -m pip install -r requirements.txt
python scripts/run_pipeline.py
python -m uvicorn src.api.main:app --reload --port 8000
```

### Frontend
```bash
cd frontend
npm install
npm run dev
```

Open:
- Frontend: http://localhost:5173
- API: http://127.0.0.1:8000/uat/master