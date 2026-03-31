#!/usr/bin/env python3
"""
Lexware Folder/ZIP to CSV Converter
Konvertiert Lexware-Ordner oder ZIP-Dateien in CSV für einfachen Import.

Verwendung:
    python lexware_folder_to_csv.py --input /path/to/folder
    python lexware_folder_to_csv.py --input /path/to/file.zip
    python lexware_folder_to_csv.py --input /path/to/folder --output mechtetech_knowledge.csv
"""

import os
import sys
import csv
import json
import zipfile
import argparse
import tempfile
from pathlib import Path

# Unterstützte Lexware-Dateitypen
LEXWARE_TEXT_TYPES = {'.txt', '.csv', '.xml', '.json', '.log', '.html', '.rtf', '.msg'}
LEXWARE_DB_TYPES = {'.dat', '.db', '.idx', '.f5', '.dbf', '.btr', '.cdx', '.fpt'}
LEXWARE_BIN_TYPES = {'.pdf', '.docx', '.xlsx'}

def extract_text_from_file(filepath):
    """Extrahiert Text aus einer Datei"""
    ext = os.path.splitext(filepath)[1].lower()

    try:
        if ext == '.txt':
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                return f.read()

        elif ext == '.csv':
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                # Ersetze Semikolons durch Kommas falls nötig
                return content

        elif ext == '.xml':
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                # Entferne XML-Tags für einfacheren Text
                import re
                text = re.sub(r'<[^>]+>', ' ', content)
                return re.sub(r'\s+', ' ', text).strip()

        elif ext == '.json':
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                try:
                    data = json.load(f)
                    return json.dumps(data, indent=2, ensure_ascii=False)
                except:
                    return f.read()

        elif ext == '.log':
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                return f.read()

        elif ext == '.msg':
            # Outlook MSG - versuche OLE
            try:
                import olefile
                ole = olefile.OleFileIO(filepath)
                for stream in ole.listdir():
                    stream_name = '_'.join(str(s) for s in stream)
                    if '0037001F' in stream or '0030001F' in stream:
                        data = ole.openstream(stream).read()
                        try:
                            return data.decode('utf-8', errors='ignore')
                        except:
                            pass
                ole.close()
            except:
                pass
            return f"[MSG file: {os.path.basename(filepath)}]"

        elif ext in LEXWARE_DB_TYPES:
            # Lexware DB - versuche verschiedene Encodings
            with open(filepath, 'rb') as f:
                raw = f.read()

            for encoding in ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']:
                try:
                    text = raw.decode(encoding, errors='ignore')
                    printable = sum(1 for c in text if c.isprintable() or c in '\n\r\t')
                    if printable > len(text) * 0.3:
                        return f"[Lexware {ext.upper()} Export]\n{text[:50000]}"
                except:
                    pass

            # Hex dump als Fallback
            hex_sample = raw[:1000].hex()
            return f"[Lexware {ext.upper()} Binary - Hex: {hex_sample[:200]}...]"

        elif ext in LEXWARE_BIN_TYPES:
            return f"[Binary file: {os.path.basename(filepath)} - Type: {ext}]"

        else:
            # Unbekannter Typ - als Text versuchen
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                return f.read()[:10000]

    except Exception as e:
        return f"[Error reading {filepath}: {e}]"

def process_folder(folder_path, extensions=None):
    """Verarbeitet einen Ordner rekursiv"""
    results = []

    for root, dirs, files in os.walk(folder_path):
        # Optional: Unterordner ausschließen
        dirs[:] = [d for d in dirs if not d.startswith('.')]

        for filename in files:
            filepath = os.path.join(root, filename)
            rel_path = os.path.relpath(filepath, folder_path)

            ext = os.path.splitext(filename)[1].lower()

            # Filter nach Extension wenn angegeben
            if extensions and ext not in extensions:
                continue

            size = os.path.getsize(filepath)

            # Text extrahieren
            text = extract_text_from_file(filepath)

            results.append({
                'quelle': rel_path,
                'dateiname': filename,
                'extension': ext,
                'groesse_bytes': size,
                'ordner': os.path.dirname(rel_path),
                'text': text[:50000] if text else ''  # Limitiere Text
            })

    return results

def process_zip(zip_path, extensions=None):
    """Verarbeitet eine ZIP-Datei"""
    results = []

    with zipfile.ZipFile(zip_path, 'r') as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue

            filename = os.path.basename(info.filename)
            ext = os.path.splitext(filename)[1].lower()

            if extensions and ext not in extensions:
                continue

            # Extrahiere in Temp-Datei
            with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
                tmp.write(zf.read(info.filename))
                tmp_path = tmp.name

            try:
                text = extract_text_from_file(tmp_path)
                size = info.file_size

                results.append({
                    'quelle': info.filename,
                    'dateiname': filename,
                    'extension': ext,
                    'groesse_bytes': size,
                    'ordner': os.path.dirname(info.filename),
                    'text': text[:50000] if text else ''
                })
            finally:
                os.unlink(tmp_path)

    return results

def write_csv(results, output_path):
    """Schreibt Ergebnisse als CSV"""
    if not results:
        print("Keine Daten zum Schreiben!")
        return

    # CSV Felder
    fieldnames = ['quelle', 'dateiname', 'extension', 'groesse_bytes', 'ordner', 'text']

    with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in results:
            # Stelle sicher dass text nicht zu lang ist für CSV
            if len(row['text']) > 50000:
                row['text'] = row['text'][:50000]
            writer.writerow(row)

    return len(results)

def main():
    parser = argparse.ArgumentParser(description='Lexware Folder/ZIP to CSV Converter')
    parser.add_argument('--input', '-i', required=True, help='Folder oder ZIP-Datei')
    parser.add_argument('--output', '-o', help='Output CSV (default: auto-generiert)')
    parser.add_argument('--extensions', '-e', help='Comma-sep. Extension-Filter (z.B. .txt,.csv,.dat)')
    parser.add_argument('--list-only', '-l', action='store_true', help='Nur Dateien auflisten')

    args = parser.parse_args()

    input_path = args.input

    if not os.path.exists(input_path):
        print(f"FEHLER: Pfad existiert nicht: {input_path}")
        return 1

    # Extension-Filter
    extensions = None
    if args.extensions:
        extensions = set(args.extensions.split(','))
        print(f"Filter: Nur {extensions} werden verarbeitet")

    # Ausgabe-Name generieren
    if args.output:
        output_path = args.output
    else:
        base = os.path.splitext(os.path.basename(input_path))[0]
        output_path = f"{base}_export.csv"

    # Verarbeiten
    print(f"Verarbeite: {input_path}")

    if os.path.isfile(input_path) and zipfile.is_zipfile(input_path):
        print("→ ZIP-Datei erkannt...")
        results = process_zip(input_path, extensions)
    else:
        print("→ Folder erkannt...")
        results = process_folder(input_path, extensions)

    print(f"→ {len(results)} Dateien gefunden")

    if args.list_only:
        for r in results[:50]:
            print(f"  {r['quelle']} ({r['groesse_bytes']} bytes)")
        if len(results) > 50:
            print(f"  ... und {len(results) - 50} weitere")
        return 0

    if not results:
        print("Keine Dateien gefunden!")
        return 1

    # CSV schreiben
    count = write_csv(results, output_path)
    print(f"\n✅ CSV erstellt: {output_path}")
    print(f"   {count} Einträge")

    # Zeige Info zu den Daten
    extensions_found = {}
    for r in results:
        ext = r['extension']
        extensions_found[ext] = extensions_found.get(ext, 0) + 1

    print("\nDateitypen:")
    for ext, count in sorted(extensions_found.items()):
        print(f"  {ext}: {count}")

    total_size = sum(r['groesse_bytes'] for r in results)
    print(f"\nGesamtgröße: {total_size / (1024*1024):.2f} MB")

    return 0

if __name__ == '__main__':
    sys.exit(main())
