# TODO

## Plan
- [x] Analizzare repository e definire architettura minima per pipeline scraping + preprocessing + dashboard interattiva.
- [x] Creare dataset anagrafico atenei (62 università) con metadati geografici e variabili di normalizzazione.
- [x] Implementare pipeline dati: raccolta (client SIOPE), normalizzazione e ranking.
- [x] Implementare web app interattiva con ranking dinamici e trend per singolo ateneo.
- [x] Aggiungere documentazione operativa e comandi di esecuzione.
- [x] Eseguire validazione locale (lint/test run) e screenshot UI.
- [ ] Commit finale + PR message.

## Review
- Implementata pipeline dati con client SIOPE parametrico e gestione errori di download.
- Aggiunta normalizzazione su studenti/docenti + score composito per ranking multi-criterio.
- Dashboard Streamlit pronta per ranking interattivo e trend storico per singolo ateneo.
- Limitazione ambiente: installazione dipendenze bloccata dal proxy, quindi screenshot UI non eseguibile.
