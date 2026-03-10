# siope-ranking

Pipeline e dashboard per analizzare i bilanci annuali delle universita italiane insieme a iscritti, docenti e metadati geografici.

## Cosa fa il progetto

1. **Pre-processing workbook**: legge il file Excel nazionale con bilanci, iscritti e docenti.
2. **Pre-processing professionale**:
   - normalizzazione su studenti e docenti;
   - KPI finanziari comparabili per ateneo e anno;
   - breakdown per categoria generale, macrocategoria e categoria;
   - metadati geografici (regione, citta, coordinate).
3. **Dashboard interattiva**:
   - ranking dinamico con scelta metrica e anno;
   - trend storico per singolo ateneo;
   - composizione del bilancio per categoria.

## Struttura

- `data/processed/Bilanci, iscritti e docenti - nazionali.xlsx`: sorgente principale.
- `src/siope_ranking/data_pipeline.py`: preprocessing workbook + fallback opzionale SIOPE.
- `src/siope_ranking/dashboard.py`: web app Streamlit con ranking, trend e breakdown.
- `src/siope_ranking/tabular_import.py`: utilita per convertire Excel/CSV in CSV pulito.

## Installazione

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Generare dataset

### Workflow principale: workbook nazionale

```bash
PYTHONPATH=src python -m siope_ranking.data_pipeline --source workbook
```

Output:

- `data/processed/university_kpis.csv`
- `data/processed/university_category_breakdown.csv`

### Fallback opzionale: download online SIOPE

```bash
PYTHONPATH=src python -m siope_ranking.data_pipeline --source siope --start-year 2019 --end-year 2024
```

Oppure, in modalita automatica, basta passare l'intervallo anni:

```bash
PYTHONPATH=src python -m siope_ranking.data_pipeline --start-year 2019 --end-year 2024
```

La pipeline online usa i file pubblici annuali `SIOPE_ENTRATE.<anno>.zip`, `SIOPE_USCITE.<anno>.zip`
e `SIOPE_ANAGRAFICHE.zip` pubblicati sul sito ufficiale SIOPE.

## Importare un Excel e convertirlo in CSV

### Conversione da terminale

```bash
PYTHONPATH=src python -m siope_ranking.tabular_import path/al/file.xlsx --sheet Sheet1
```

Output di default: `data/imported/<nome_file>.csv`

### Conversione direttamente nella dashboard

La sidebar della dashboard permette di:

- caricare file `.xlsx`, `.xls`, `.xlsm` o `.csv`;
- scegliere il foglio Excel da importare;
- scaricare il CSV convertito oppure salvarlo in `data/imported/`;
- visualizzare subito la tabella caricata.

Se il file contiene gia le colonne KPI del progetto, ranking e trend vengono aggiornati usando i dati importati.

## Avviare la dashboard

```bash
PYTHONPATH=src streamlit run src/siope_ranking/dashboard.py
```

Poi apri `http://localhost:8501`.
