# Wavite-UI Projekt Gedächtnis

## Server & Dienste
- **Weaviate**: http://62.171.136.239:8080
- **Ollama**: http://62.171.136.239:11434
- **Processor (Flask)**: http://62.171.136.239:5000
- **WebUI**: http://62.171.136.239:7000
- **n8n**: http://vmd188735.contaboserver.net:5678
- **Upload-Ordner**: /home/mechtech/uploads

## Weaviate Authentifizierung
- **Benutzername**: Lena
- **Passwort**: Lena.8473xxx
- **HTTP URL**: http://62.171.136.239:8080
- **gRPC Host**: 62.171.136.239
- **gRPC Port**: 50051

## n8n Weaviate Credentials
- **Credential Name**: Weaviate Credentials account (MUSS genau so heißen!)
- **Credential ID**: VDLgCmqkGcdyHynZ
- In n8n: Settings → Credentials → Weaviate Credentials account
- Connection Type: Custom Connection
- HTTP Host: 62.171.136.239
- HTTP Port: 8080
- HTTP Secure: Aus
- gRPC Host: 62.171.136.239
- gRPC Port: 50051
- gRPC Secure: Aus
- Weaviate Api Key: (leer)

## Weaviate Klassen
- Lexware_rag_knowledge
- Lexware_import_buffer

## Dateitypen-Kategorisierung für Lexware Buchhalter

### ✅ Vektorisieren (Text-Inhalte extrahieren)
- Dokumente: pdf, txt, csv, xml, json, rtf, docx, xlsx
- Web/Log: html, log
- Lexware DB (wichtig für Buchhalter!): db, dat, idx, f5, dbf, btr, cdx, fpt

### ❌ Nicht vektorisieren (nur Metadata)
- Binary: lxd, lxa, lxv, bak, zip, 7z, lbu, lbk, lex
- Lexware intern: lhd, lpd, lpe, lre, lva, lza, lxs, lfo, lfa, lwe, lwa, lwi, lwo, llg, lmd, lna, lso, elfo, elst, pfx
- SQL Server: mdf, ldf, ndf, trn
- Banking: mt940, camt, dta, sepa

## n8n Weaviate Vector Store Node Einstellungen
- **URL**: http://62.171.136.239:8080 (oder http://vmd188735.contaboserver.net:8080)
- **Auth**: None (kein API Key)
- **Class**: Lexware_rag_knowledge
- **GraphQL**: http://62.171.136.239:8080/v1/graphql

## n8n Workflow Updates
- Workflow ID: BnSgeDwufUviP7EU (RAG_WEAVITE)
- Collection in Nodes aktualisiert auf: lexware_rag_knowledge
- Credentials existieren: Weaviate Credentials account (ID: VDLgCmqkGcdyHynZ)

## Fehlerbehebung
- Fehler "50051" = falsche URL in Credentials
- Lösung: In n8n UI die Weaviate Credentials auf http://62.171.136.239:8080 setzen

## Fehlerbehebung
- Fehler "Weaviate failed to startup" = Connection refused auf Port 50051
- Lösung: Container im gleichen Docker-Netzwerk oder korrekte URL verwenden
- n8n kann Weaviate nicht erreichen -> Host-IP statt localhost verwenden

## Letzte Änderungen
- Processor unterstützt jetzt .db, .dat, .idx, .f5, .dbf, .btr Dateien (extrahiert für Buchhalter-Analyse)
- FTP-Upload-Ordner: /home/mechtech/uploads
- Fortschrittsbalken mit Prozent-Anzeige in WebUI