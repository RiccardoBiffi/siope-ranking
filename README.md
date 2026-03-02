# siope-ranking

Pipeline e dashboard per analizzare i dati SIOPE delle università italiane (62 atenei) e produrre ranking interattivi.

## Cosa fa il progetto

1. **Raccolta dati SIOPE**: script per scaricare transazioni annuali per ateneo.
2. **Pre-processing professionale**:
   - normalizzazione su numero studenti e docenti;
   - KPI economici comparabili;
   - metadati geografici (regione, città, coordinate).
3. **Dashboard interattiva**:
   - ranking dinamico con scelta metrica e anno;
   - grafico dell'andamento anno-per-anno per singolo ateneo.

## Struttura

- `config/universities.csv`: anagrafica dei 62 atenei con metadati.
- `src/siope_ranking/data_pipeline.py`: download dati SIOPE + costruzione KPI.
- `src/siope_ranking/demo_data.py`: genera dataset demo realistico quando l'endpoint SIOPE non è disponibile.
- `src/siope_ranking/dashboard.py`: web app Streamlit con ranking e trend.

## Installazione

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Generare dataset

### Opzione A - dati reali da SIOPE

```bash
PYTHONPATH=src python -m siope_ranking.data_pipeline --start-year 2019 --end-year 2024
```

> Se l'endpoint non risponde o cambia formato, usa la modalità demo.

### Opzione B - dataset demo

```bash
PYTHONPATH=src python -m siope_ranking.demo_data
```

Output: `data/processed/university_kpis.csv`

## Avviare la dashboard

```bash
PYTHONPATH=src streamlit run src/siope_ranking/dashboard.py
```

Poi apri `http://localhost:8501`.
