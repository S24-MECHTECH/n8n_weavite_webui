#!/usr/bin/env python3
"""
Weaviate Backup Script
Exportiert alle Daten aus Weaviate in JSON-Dateien zur Sicherung.

Verwendung:
    python backup_weaviate.py                 # Backup aller Klassen
    python backup_weaviate.py --restore        # Wiederherstellung
    python backup_weaviate.py --class NAME     # Nur bestimmte Klasse sichern
    python backup_weaviate.py --list           # Vorhandene Backups anzeigen
"""

import os
import json
import argparse
import zipfile
from datetime import datetime
import requests

# ============ KONFIGURATION ============
WEAVIATE_URL = os.environ.get('WEAVIATE_URL', 'http://62.171.136.239:8080')
BACKUP_DIR = '/home/mechtech/Lexware_Buchhaltung_Weavite_Backups'

# Klassen die gesichert werden sollen (in Reihenfolge!)
CLASSES_TO_BACKUP = [
    'Lexware_rag_knowledge',
    'Lexware_import_buffer'
]

# ============ HILFSFUNKTIONEN ============

def get_headers():
    """Gibt HTTP-Headers für Weaviate zurück"""
    return {'Content-Type': 'application/json'}

def ensure_backup_dir():
    """Erstellt das Backup-Verzeichnis falls nicht vorhanden"""
    if not os.path.exists(BACKUP_DIR):
        os.makedirs(BACKUP_DIR)
        print(f"✓ Backup-Verzeichnis erstellt: {BACKUP_DIR}")

def get_all_objects(class_name, limit=1000):
    """Holt alle Objekte einer Klasse"""
    all_objects = []
    offset = 0
    batch_size = 100

    print(f"  → Lese {class_name}...")

    while True:
        query = f"""
        {{
          Get {{
            {class_name}(limit: {batch_size}, offset: {offset}) {{
              _additional {{
                id
                creationTimeUnix
                lastUpdateTimeUnix
                vector
              }}
              text
              quelle
              kategorie
              titel
              status
              datum
              file_type
            }}
          }}
        }}
        """

        try:
            response = requests.post(
                f'{WEAVIATE_URL}/v1/graphql',
                headers=get_headers(),
                json={'query': query},
                timeout=30
            )

            if response.status_code != 200:
                print(f"  ✗ Fehler bei {class_name} (offset {offset}): {response.status_code}")
                break

            data = response.json()
            objects = data.get('data', {}).get('Get', {}).get(class_name, [])

            if not objects:
                break

            all_objects.extend(objects)
            offset += batch_size

            if len(objects) < batch_size:
                break

            if offset % 500 == 0:
                print(f"  → {offset} Objekte gelesen...")

        except Exception as e:
            print(f"  ✗ Fehler: {e}")
            break

    return all_objects

def export_class_to_json(class_name, filepath):
    """Exportiert eine Klasse in eine JSON-Datei"""
    print(f"\n📦 Exportiere {class_name}...")

    objects = get_all_objects(class_name)

    if not objects:
        print(f"  ⚠ Keine Objekte in {class_name}")
        return False

    # Metadaten hinzufügen
    backup_data = {
        'metadata': {
            'class_name': class_name,
            'export_date': datetime.now().isoformat(),
            'object_count': len(objects),
            'weaviate_url': WEAVIATE_URL
        },
        'objects': objects
    }

    # JSON speichern
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(backup_data, f, indent=2, ensure_ascii=False)

    print(f"  ✓ {len(objects)} Objekte gespeichert in {filepath}")
    return True

def backup_all_classes():
    """Sichert alle Klassen als ZIP"""
    print("=" * 60)
    print("🚀 WEAVIATE BACKUP")
    print("=" * 60)
    print(f"📁 Backup-Verzeichnis: {BACKUP_DIR}")
    print(f"🔗 Weaviate URL: {WEAVIATE_URL}")
    print()

    ensure_backup_dir()

    # Zeitstempel für Dateinamen
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    zip_filename = f'weaviate_backup_{timestamp}.zip'
    zip_filepath = os.path.join(BACKUP_DIR, zip_filename)

    # Erstelle ZIP-Datei
    with zipfile.ZipFile(zip_filepath, 'w', zipfile.ZIP_DEFLATED) as zipf:
        total_objects = 0

        for class_name in CLASSES_TO_BACKUP:
            objects = get_all_objects(class_name)

            if objects:
                # JSON-Datei für diese Klasse
                class_data = {
                    'metadata': {
                        'class_name': class_name,
                        'export_date': datetime.now().isoformat(),
                        'object_count': len(objects),
                        'weaviate_url': WEAVIATE_URL
                    },
                    'objects': objects
                }

                json_filename = f'{class_name}.json'
                json_content = json.dumps(class_data, indent=2, ensure_ascii=False)

                # In ZIP schreiben
                zipf.writestr(json_filename, json_content)

                print(f"  ✓ {class_name}: {len(objects)} Objekte")
                total_objects += len(objects)

        # Zusammenfassung in ZIP
        summary_content = f"""Weaviate Backup Zusammenfassung
{'=' * 40}
Datum: {datetime.now().isoformat()}
Gesamt Objekte: {total_objects}

Klassen:
"""
        for class_name in CLASSES_TO_BACKUP:
            summary_content += f"  - {class_name}\n"

        zipf.writestr('README.txt', summary_content)

    zip_size = os.path.getsize(zip_filepath) / (1024*1024)

    print()
    print("=" * 60)
    print(f"✅ BACKUP ABGESCHLOSSEN")
    print(f"   Gesamt Objekte: {total_objects}")
    print(f"   📦 ZIP-Datei: {zip_filepath}")
    print(f"   Größe: {zip_size:.2f} MB")
    print("=" * 60)

    return zip_filepath

def restore_from_backup(backup_file):
    """Stellt Daten aus einer Backup-Datei wieder her"""
    print("=" * 60)
    print("♻️  WEAVIATE RESTORE")
    print("=" * 60)

    if not os.path.exists(backup_file):
        print(f"✗ Backup-Datei nicht gefunden: {backup_file}")
        return False

    with open(backup_file, 'r', encoding='utf-8') as f:
        backup_data = json.load(f)

    metadata = backup_data.get('metadata', {})
    objects = backup_data.get('objects', [])
    class_name = metadata.get('class_name', 'Unknown')

    print(f"\n📦 Wiederherstellung: {class_name}")
    print(f"   Objekte: {len(objects)}")
    print(f"   Original-Datum: {metadata.get('export_date')}")

    # Warnung
    response = input(f"\n⚠️  WARNUNG: Dies fügt {len(objects)} Objekte zu Weaviate hinzu!\n   Fortfahren? (j/n): ")
    if response.lower() != 'j':
        print("Abgebrochen.")
        return False

    success_count = 0
    error_count = 0

    for i, obj in enumerate(objects):
        obj_id = obj.get('_additional', {}).get('id')
        properties = {k: v for k, v in obj.items() if k != '_additional'}

        body = {
            'class': class_name,
            'properties': properties
        }

        # Vektor hinzufügen falls vorhanden
        if obj.get('_additional', {}).get('vector'):
            body['vector'] = obj['_additional']['vector']

        try:
            response = requests.post(
                f'{WEAVIATE_URL}/v1/objects',
                headers=get_headers(),
                json=body,
                timeout=10
            )

            if response.status_code in [200, 201]:
                success_count += 1
            else:
                error_count += 1
                print(f"  ✗ Fehler bei Objekt {i}: {response.status_code}")

        except Exception as e:
            error_count += 1
            print(f"  ✗ Exception: {e}")

        if (i + 1) % 50 == 0:
            print(f"  → {i + 1} / {len(objects)} Objekte...")

    print()
    print("=" * 60)
    print(f"✅ RESTORE ABGESCHLOSSEN")
    print(f"   Erfolgreich: {success_count}")
    print(f"   Fehler: {error_count}")
    print("=" * 60)

    return True

def list_backups():
    """Listet alle vorhandenen Backups auf"""
    print("=" * 60)
    print("📋 VORHANDENE BACKUPS")
    print("=" * 60)

    if not os.path.exists(BACKUP_DIR):
        print(f"✗ Backup-Verzeichnis nicht gefunden: {BACKUP_DIR}")
        return

    files = sorted(os.listdir(BACKUP_DIR), reverse=True)

    if not files:
        print("Keine Backups vorhanden.")
        return

    # Nur ZIP-Dateien anzeigen
    zip_files = [f for f in files if f.endswith('.zip')]

    if not zip_files:
        print("Keine ZIP-Backups vorhanden.")
        return

    for f in zip_files[:20]:  # Nur letzte 20 anzeigen
        filepath = os.path.join(BACKUP_DIR, f)
        size = os.path.getsize(filepath)
        mtime = datetime.fromtimestamp(os.path.getmtime(filepath))

        # Versuche Objekt-Anzahl aus ZIP zu lesen
        try:
            with zipfile.ZipFile(filepath, 'r') as zf:
                names = zf.namelist()
                json_files = [n for n in names if n.endswith('.json')]
                total_count = 0
                for jf in json_files:
                    try:
                        data = json.loads(zf.read(jf))
                        total_count += data.get('metadata', {}).get('object_count', 0)
                    except:
                        pass
                print(f"  📦 {f} ({size/(1024*1024):.2f} MB) - {total_count} Objekte - {mtime.strftime('%Y-%m-%d %H:%M')}")
        except Exception as e:
            print(f"  📦 {f} ({size/(1024*1024):.2f} MB) - {mtime.strftime('%Y-%m-%d %H:%M')}")

    if len(zip_files) > 20:
        print(f"\n  ... und {len(zip_files) - 20} weitere Backups")

# ============ MAIN ============

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Weaviate Backup Script')
    parser.add_argument('--restore', metavar='DATEI', help='Wiederherstellung aus Backup')
    parser.add_argument('--class', dest='class_name', help='Nur bestimmte Klasse sichern')
    parser.add_argument('--list', action='store_true', help='Vorhandene Backups anzeigen')
    parser.add_argument('--dir', dest='backup_dir', default=BACKUP_DIR, help='Backup-Verzeichnis')

    args = parser.parse_args()

    # Backup-Verzeichnis setzen
    if args.backup_dir:
        BACKUP_DIR = args.backup_dir

    if args.list:
        list_backups()
    elif args.restore:
        restore_from_backup(args.restore)
    else:
        backup_all_classes()
