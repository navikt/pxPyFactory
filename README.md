# pxPyFactory

Bygger PX-filer og Saved Query-filer fra metadata og datagrunnlag.

## Hva prosjektet gjør

Prosjektet leser metadata og tabellfiler, og produserer:

- PX-filer (`.px`)
- Saved Query-filer (`.sqa` og `.sqs`)
- Produksjonslogg (`log/production_log.jsonl`)

Kjøringen er inkrementell: hvis input ikke har endret seg siden forrige kjøring, hopper den over unødvendig bygging.

## Kjøreflyt

1. `run.py` kaller `pxpyfactory.__main__.go()`.
2. `PXMain.mainprep()` leser argumenter, gjør eventuell `clean`, sjekker endringer og forbereder metadata.
3. For hvert dataprodukt bygges PX-innhold og filer skrives.
4. Saved Query-filer genereres.
5. Produksjonslogg skrives, og deploy kan trigges.

## Lagring (lokal vs GCS)

`pxpyfactory/file_io.py` delegerer all IO til valgt backend.

### Lokal modus

I lokal modus brukes mapper i repo:

- `input_bucket/` for input
- `output_bucket/` for output

Stier som starter med `px/` eller `sq/` skrives i `output_bucket/`. Andre stier leses fra `input_bucket/`.

### GCS-modus

I GCS-modus brukes bucket-navn fra config:

- `pxpyfactory.config.gcs.BUCKET_INPUT`
- `pxpyfactory.config.gcs.BUCKET_OUTPUT`

Samme sti-logikk gjelder her: `px/` og `sq/` rutes til output, resten til input.

### Bytte backend

Bytt import i `pxpyfactory/file_io.py`:

Lokal:

```python
# import pxpyfactory.file_io_gcs as _backend
import pxpyfactory.file_io_local as _backend
```

GCS:

```python
import pxpyfactory.file_io_gcs as _backend
# import pxpyfactory.file_io_local as _backend
```

## Viktige inputfiler

- Lokal modus:
	- `input_bucket/common_meta.xlsx`
	- `input_bucket/stats/*.csv` (tabellfiler)
	- `input_bucket/stats/*_meta.csv` (tabellspesifikk metadata)
- GCS-modus:
	- `<input-bucket>/common_meta.xlsx`
	- `<input-bucket>/stats/*.csv`
	- `<input-bucket>/stats/*_meta.csv`

## Kolonnevalg fra _meta (CS)

Valg av `DATA`, `STUB`, `HEADING` og `TIMEVAL` hentes nå primært fra tabellspesifikk metadata i `*_meta.csv` (ikke fra kolonnefeltene i Excel-arket for dataprodukter).

I `*_meta.csv` brukes rader med `TYPE=CS` og kolonnene:

- `KEYWORD` (`DATA`, `STUB`, `HEADING`, `TIMEVAL`)
- `VALUE` (kommaseparert liste)

Eksempel:

```csv
TYPE,KEYWORD,VALUE
CS,DATA,"ANTALL#0#personer,PROSENT#1#%"
CS,STUB,KJONN
CS,HEADING,"ALDER,TID"
CS,TIMEVAL,TID
```

Format for `DATA`-verdi:

- Del 1: kolonnenavn
- Del 2 (valgfri): precision
- Del 3 (valgfri): unit

Delimiter er `#`, altså `KOLONNE#PRECISION#UNIT`.

Eksempel:

- `ANTALL#0#personer` gir:
- `data_list`: `ANTALL`
- `data_precision_list`: `0`
- `data_units_list`: `personer`

Fallback-regler:

- Hvis `DATA` mangler i `CS`, velges datakolonne(r) automatisk basert på antall unike verdier.
- Hvis `STUB` mangler i `CS`, velges første tilgjengelige ikke-data-kolonne.
- Hvis `HEADING` mangler i `CS`, brukes resterende kolonner som `HEADING`.
- Hvis antall precision/enheter ikke matcher antall data-kolonner, fylles resterende med `None`.

## Kjøring fra terminal

Eksempler under er for PowerShell på Windows.

### 1) Oppsett

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Hvis du kjører i GCS-modus må du i tillegg autentisere mot Google Cloud (ADC), for eksempel:

```powershell
gcloud auth login --update-adc
```

### 2) Standard kjøring

```powershell
python run.py
```

### 3) Kjøring med argumenter

Argumenter leses som `key=value` eller flagg uten verdi.

```powershell
python run.py build=all print=1
```

```powershell
python run.py build=agg_bt_px_mottaker_fylke no_deploy print=2
```

```powershell
python run.py clean build=all
```

```powershell
python run.py test build=all print=3
```

```powershell
python run.py deploy environment=dev branch=main
```

## Støttede argumenter

- `build=all`: bygg alle tabeller
- `build=<TABLEID eller TABLEID_RAW>`: bygg kun én tabell
- `clean`: slett innhold i output/sq før bygg
- `test`: skriv resultat til konsoll i stedet for å skrive filer
- `test_full`: som `test`, men skriver hele filinnhold
- `print=<nivå>`: loggnivå (høyere tall gir mer logging)
- `no_deploy`: hopp over deploy selv om filer er bygget
- `deploy`: tving deploy
- `environment=<navn>`: deploy-miljø
- `branch=<navn>`: branch for deploy

## Deploy-forutsetning

Deploy bruker GitHub Actions dispatch og krever miljøvariabel:

- `GITHUB_TOKEN_PX`

Denne kan settes i miljøet eller i `.env`.

## Nyttige tips

- Bruk `print=2` eller `print=3` ved feilsøking.
- Bruk `build=<tableid>` for rask iterasjon på én tabell.
- Bruk `test` eller `test_full` når du vil validere output uten å skrive filer.
- Start i lokal modus når du utvikler, og bytt til GCS-modus når du vil kjøre mot bucket-data.