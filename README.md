Innholdet nedenfor er AI generert.

## pxPyFactory: Kodeforklaring

Denne README-en beskriver hvordan pxPyFactory bygger PX-filer og Saved Query-filer fra metadata og datagrunnlag i Google Cloud Storage.

## Hva løsningen gjør

pxPyFactory leser:
- felles metadata fra common_meta.xlsx
- tabellfiler per dataprodukt (csv, kan inkludere parquet senere)
- valgfri tabellspesifikk metadatafil med suffix _meta.csv

Deretter produseres:
- en PX-fil per tabell
- et sett med Saved Query-filer (.sqa og .sqs) per tabell
- oppdatert produksjonslogg (jsonl)

## Kjøreflyt fra start til slutt

1. Startpunkt er run.py som kaller pxpyfactory.__main__.go().
2. go() oppretter PXMain og kjører run().
3. PXMain.mainprep() leser argumenter, sjekker endringer via logg, forbereder metadata og mappestruktur.
4. For hver rad i dataproduktlisten kjøres PXMain.process_data_product().
5. PXDataProduct.create_px_content() bygger alle PX-linjer (metadata + DATA-seksjon).
6. Fil skrives til output-bucket.
7. SavedQueryGenerator lager .sqa og .sqs.
8. PXMain.log_and_deploy() skriver logg og kan trigge deploy.

## Viktigste moduler og ansvar

### pxpyfactory/main.py

Orkestrering av hele byggeløpet.

Viktige metoder:
- mainprep():
	- parser CLI-argumenter (helpers.set_input_args)
	- håndterer clean
	- sjekker om input/common metadata er endret (PXLog)
	- bygger data_products_df og keywords_base
	- oppdaterer alias-filer og mappestruktur ved behov
- process_data_product():
	- avgjør om tabellen må bygges
	- bygger PX-innhold
	- skriver PX-fil
	- genererer saved query
- log_and_deploy():
	- skriver oppsummeringslogg
	- trigger deploy hvis betingelser er oppfylt

### pxpyfactory/data_product.py

Representerer ett dataprodukt (én tabell) og inneholder logikken for å bygge den konkrete PX-filen.

Hovedsteg i create_px_content():
- les tabellfil
- normaliser kolonner og identifiser STUB/HEADING/DATA
- bygg values_dict med unike verdier for dimensjoner
- opprett keywords-basert modell for tabellen
- les og splitt tabellspesifikk metadata i:
	- PX (vanlige keyword-verdier)
	- SQ (saved query-innstillinger)
	- CR (column rename/oversettelser)
- oppdater keyword-objekter
- generer ferdige PX-linjer
- generer DATA-seksjon med komplett kombinasjon av dimensjonsverdier

Viktig detalj:
- contvariable settes til STAT_VAR internt, og brukes som innholdsvariabel i HEADING/CONTVARIABLE/VALUES.

### pxpyfactory/keyword.py og pxpyfactory/multilingual_value.py

Dette er kjerne for håndtering av PX-keywords.

Keyword-klassen støtter:
- språkavhengige verdier
- scopes (for eksempel VALUES("fylke"), PRECISION("STAT_VAR", "antall"))
- fallback til default-verdi
- validering/coercion av typer
- serialisering til korrekt PX-linjeformat

MultilingualValue og MultilingualValueScope gir:
- lagring av verdi per språk
- fallback-regler når språk mangler
- oppdatering av verdier ved rename/oversettelse (CR)

### pxpyfactory/main_praparation.py

Forberedelse av felles metadata:
- prepare_data_products(): leser dataprodukter-ark, korter tableid, filtrerer på BUILD og build-argument
- prepare_keywords_base(): bygger base med Keyword-objekter fra metadata-default
- prepare_alias(): leser alias for mapper
- update_folder_structure(): skriver alias_no.txt og alias_en.txt i outputstruktur

### pxpyfactory/saved_query.py

Genererer saved query-filer.

generate_sqa() bygger en Selection-struktur med:
- VariableCode
- ValueCodes
- Placement (Heading/Stub)

generate_sqs() lager enkel statistikkblokk med Created/LastUsed/UsageCount.

Merk:
- SQA må bruke variablekoder som matcher språk/metadata riktig. Ulik kode mellom PX og SQA kan føre til at PXWeb ikke godtar query.

### pxpyfactory/file_io.py

All IO mot Google Cloud Storage.

Modulen:
- velger input- eller output-bucket basert på sti
- leser xlsx/csv/jsonl til pandas
- skriver filer
- henter filstørrelse og endringstid
- sletter innhold ved clean

### pxpyfactory/log.py

Endringsdeteksjon og produksjonslogg.

PXLog sammenligner nåsituasjon med siste loggede kjøring for å avgjøre:
- om input har endret seg
- om common metadata har endret seg
- om et konkret dataprodukt har endret seg

Dette er grunnlaget for inkrementelle bygg.

### pxpyfactory/config.py

Sentral konfigurasjon:
- bucket-navn
- standard paths
- navnekonstanter
- terskler som MAX_SQ_CELLS

## Datakontrakt i metadata

common_meta.xlsx inneholder blant annet:
- dataprodukter: hvilke tabeller som skal bygges
- metadata-default: basisdefinisjon av keywords
- folder-alias: navn på mappealias per språk

Tabellspesifikk metadata i denne pipelinen leses fra <TABLEID_RAW>_meta.csv (ikke fra ark i common_meta.xlsx).

Tabellspesifikk _meta.csv forventer feltene TYPE, KEYWORD, VALUE, og valgfritt LANGUAGE.

TYPE brukes slik:
- PX: setter vanlig PX-keyword-verdi
- SQ: styrer saved query utvalg
- CR: rename/oversettelse av kolonner og relaterte keyword-scopes

## Hvordan DATA-seksjonen bygges

For korrekt PX-format må datasettet dekke alle kombinasjoner av STUB + HEADING.

Logikken i _get_lines_of_data_from_table():
- bygger kartesisk produkt av alle dimensjonsverdier
- left join mot faktiske data
- fyller manglende celler med DATASYMBOL2
- pivoterer til PX-rekkefølge
- skriver hver rad som space-separert streng

Dette sikrer stabil og komplett DATA-blokk selv ved manglende observasjoner.

## Kommandoargumenter

Argumenter leses i helpers.set_input_args(). Eksempler:
- build=all: bygg alle
- build=<tableid>: bygg én tabell
- clean: tøm output/sq før bygg
- test: skriv ut i stedet for å lagre filer
- test_full: skriv ut full filtekst
- no_deploy: hopp over deploy
- deploy: tving deploy
- print=<nivå>: loggnivå

## Typiske feilkilder

1. Ulik navngiving av variabler på tvers av keyword-linjer
Hvis for eksempel HEADING/CONTVARIABLE/VALUES/PRECISION ikke peker på samme variabelkode, kan PXWeb avvise filen.

2. Språkoversettelse brukt inkonsistent
Hvis språkspesifikke navn brukes i noen felter, men rå-navn i andre, kan det gi mismatch i både PX og SQA.

3. Ufullstendig metadata
Manglende KEYWORD eller feil TYPE i _meta.csv kan gi stille fallback eller manglende linjer.

4. For mange celler i standard query
SQA begrenses av MAX_SQ_CELLS. Ved overskridelse kuttes antall valgte verdier per variabel.

## Praktisk debug-oppskrift

1. Kjør med print=3 eller print=4 for å se hvilke keywords/scopes som bygges.
2. Verifiser at samme variabelkode brukes konsekvent i:
	 - HEADING
	 - CONTVARIABLE
	 - VALUES(scope)
	 - PRECISION(scope)
3. Kontroller at eventuelle CR-renames også slår gjennom i Saved Query-filen.
4. Sammenlign fungerende og ikke-fungerende PX med fokus på keyword-sammenheng, ikke bare tekstlikhet.

## Kort oppsummering

Arkitekturen er delt i:
- orkestrering (PXMain)
- per-tabell byggelogikk (PXDataProduct)
- keyword/språk/scope-modell (Keyword + MultilingualValue)
- IO og endringsdeteksjon (file_io + PXLog)
- query-generering (SavedQueryGenerator)

Denne oppdelingen gjør at pipeline kan bygges inkrementelt, samtidig som PX-formatet holdes konsistent på tvers av språk, scopes og tabeller.