# DatabricksMedallionArchitecture

# Data Pipeline - Brewery Analytics (Medallion Architecture)

[![Databricks](https://img.shields.io/badge/Databricks-UC%20Pipeline-blue)](https://www.databricks.com/)
[![Python](https://img.shields.io/badge/Python-3.10%2B-green)](https://www.python.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.0-orange)](https://delta.io/)

## 🎯 Overview

Progetto di data engineering che implementa l'architettura **Medallion** su Databricks con:
- **Pipeline DLT** per Bronze → Silver → Gold layers
- **Generatore di dati randomici** per simulare SCD Type 2
- **Apply Changes** per gestione automatica dello storico

## 🏗️ Architettura


| Layer | Scopo | Tecnologie |
|-------|-------|------------|
| **Bronze** | Dati raw API | DLT Streaming |
| **Silver Staging** | Ingestion + UNION CDC | DLT Pipeline |
| **Silver Clean** | Data quality + SK | DLT Pipeline |
| **Gold** | SCD Type 2 storico | Apply Changes |

## 🚀 Quick Start

### 1. Clona il repository
```bash
git clone https://github.com/raffmarro97-lab/DatabricksMedallionArchitecture
cd brewery-pipeline
```

### 2. Deploy su Databricks
```bash
# Importa notebook in workspace/pipeline_breweries/
# Crea pipeline DLT da "01_Bronze_to_Gold"
# Aggiungi notebook_breweries.cdc_breweries_events come external table
```

### 3. Esegui il job


## 📁 Struttura del progetto

## 🔧 Key Features

- **SCD Type 2 automatico** con `apply_changes(sequence_by=ingestion_ts)`
- **Generatore CDC** modifica 3-10 record random per run
- **Data quality** in Silver (null handling, regex cleaning)
- **Unity Catalog** governance completa
- **left_anti** per evitare conflitti API vs CDC

## ⚙️ Configurazione

| Parametro | Valore | Descrizione |
|-----------|--------|-------------|
| `NUM_BREWERIES_UPDATE` | 3-10 | Record modificati per run |
| `CDC_TABLE` | `notebook_breweries.cdc_breweries_events` | Tabella esterna |
| `sequence_by` | `ingestion_ts` | Ordinamento SCD2 |

## 🧪 Test & Validazione

```sql
-- Verifica SCD2 (dovresti vedere valid_to popolati)
SELECT id, street, phone, __START_AT, __END_AT
FROM pipeline_breweries.gold_breweries 
WHERE id IN (SELECT id FROM notebook_breweries.cdc_breweries_events)
ORDER BY id, __START_AT;
```

## 📈 Risultati

Dopo 5 run del job:
- **~15-50 versioni** uniche in gold
- **100% record CDC** con `__END_AT` valorizzato
- **~200 record totali** con storico completo

## 🔍 Troubleshooting

| Problema | Soluzione |
|----------|-----------|
| `multiple source rows` | Deduplica silver per `ingestion_ts DESC` |
| No `__END_AT` | Full refresh pipeline DLT |
| CDC sovrascritto | Aggiungi `left_anti` in silver staging |

## 📄 Licenza

MIT License - vedi [LICENSE](LICENSE)

## 👨‍💻 Autore

**Raffaele Marro**  
Data Engineer | Databricks | SCD2 Specialist  
[LinkedIn](https://linkedin.com/in/raffaelemarro)
