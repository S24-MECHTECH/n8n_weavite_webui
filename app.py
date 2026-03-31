import os
import json
import uuid
import zipfile
import tempfile
import shutil
import time
from flask import Flask, request, jsonify
from flask_cors import CORS
import requests

app = Flask(__name__)
CORS(app)

# Config
WEAVIATE_URL = os.environ.get('WEAVIATE_URL', 'http://62.171.136.239:8080')
OLLAMA_URL = os.environ.get('OLLAMA_URL', 'http://62.171.136.239:11434')
CLASS_NAME = os.environ.get('CLASS_NAME', 'Lexware_rag_knowledge')
OLLAMA_MODEL = os.environ.get('OLLAMA_MODEL', 'nomic-embed-text')

# Slow Mode für bessere Stabilität
SLOW_MODE = os.environ.get('SLOW_MODE', 'true').lower() == 'true'
SLOW_DELAY_FILE = float(os.environ.get('SLOW_DELAY_FILE', '0.5'))  # Pause nach jeder Datei
SLOW_DELAY_WEAVIATE = float(os.environ.get('SLOW_DELAY_WEAVIATE', '1.0'))  # Pause nach jedem Weaviate-Request
SLOW_DELAY_OLLAMA = float(os.environ.get('SLOW_DELAY_OLLAMA', '2.0'))  # Pause nach jedem Ollama-Request

TEMP_DIR = '/tmp/uploads'

if not os.path.exists(TEMP_DIR):
    os.makedirs(TEMP_DIR)

# File type classification for Lexware Buchhalter
# Kategorie 1: Text-Dateien die vektorisiert werden
VECTORIZE_TYPES = {'pdf', 'txt', 'csv', 'xml', 'json', 'log', 'html', 'rtf', 'docx', 'xlsx'}

# Kategorie 2: Lexware DB-Dateien die extrahiert & vektorisiert werden (Buchhaltung!)
LEXWARE_DB_TYPES = {'db', 'dat', 'idx', 'f5', 'dbf', 'btr', 'cdx', 'fpt'}

# Kategorie 3: Binary-Dateien die NICHT vektorisiert werden
NON_VECTORIZE_TYPES = {'lxd', 'lxa', 'lxv', 'bak', 'zip', '7z', 'lbu', 'lbk', 'lex', 'lhd', 'lpd', 'lpe', 'lre', 'lva', 'lza', 'lxs', 'lfo', 'lfa', 'lwe', 'lwa', 'lwi', 'lwo', 'llg', 'lmd', 'lna', 'lso', 'elfo', 'elst', 'pfx', 'mdf', 'ldf', 'ndf', 'trn', 'mt940', 'camt', 'dta', 'sepa'}

# Mapping für Dateiformate zu Kategorien
FILE_CATEGORIES = {
    # Dokumente (vektorisieren)
    'pdf': ('Dokument', True), 'txt': ('Dokument', True), 'csv': ('Buchung', True),
    'xml': ('XML', True), 'json': ('JSON', True), 'log': ('Log', True),
    'html': ('HTML', True), 'rtf': ('Dokument', True), 'docx': ('Dokument', True),
    'xlsx': ('Excel', True),
    # Lexware DB (vektorisieren für Buchhalter-Analyse)
    'db': ('Lexware DB', True), 'dat': ('Lexware Datensatz', True),
    'idx': ('Lexware Index', True), 'f5': ('F5 Datenbank', True),
    'dbf': ('dBase', True), 'btr': ('Btrieve', True), 'cdx': ('Index', True),
    'fpt': ('FoxPro', True),
    # Binary (nicht vektorisieren)
    'lxd': ('Lexware Archiv', False), 'lxa': ('Lexware Archiv', False),
    'lxv': ('Lexware Verz.', False), 'bak': ('Backup', False),
    'zip': ('Archive', False), '7z': ('Archive', False),
    'lbu': ('LXBackup', False), 'lbk': ('LXBackup', False),
    'lex': ('Lexware', False)
}

def classify_file(ext):
    """Classify file type for Lexware Buchhalter"""
    ext = ext.lower()
    if ext in VECTORIZE_TYPES:
        return 'text', True, FILE_CATEGORIES.get(ext, ('Dokument', True))[0]
    elif ext in LEXWARE_DB_TYPES:
        return 'lexware_db', True, FILE_CATEGORIES.get(ext, ('Datenbank', True))[0]
    elif ext in NON_VECTORIZE_TYPES:
        return 'binary', False, FILE_CATEGORIES.get(ext, ('Binary', False))[0]
    else:
        return 'unknown', False, 'Unbekannt'

def get_embedding(text):
    """Get embedding from Ollama"""
    try:
        response = requests.post(
            f'{OLLAMA_URL}/api/embeddings',
            json={'model': OLLAMA_MODEL, 'prompt': text[:8000]},
            timeout=120  # Längeres Timeout für Ollama
        )
        if response.ok:
            # Slow Mode: Pause nach Ollama-Request
            if SLOW_MODE:
                time.sleep(SLOW_DELAY_OLLAMA)
            return response.json().get('embedding')
    except Exception as e:
        print(f"Embedding error: {e}")
    return None

def extract_lexware_db(filepath, ext):
    """Extract text content from Lexware DB/DAT/IDX/F5 files"""
    try:
        # Try reading as text (works for many .dat and .db files)
        with open(filepath, 'rb') as f:
            raw = f.read()

        # Try UTF-8
        try:
            text = raw.decode('utf-8')
            if len(text.strip()) > 10:
                return f"[Lexware DB Export]\n{text[:50000]}"
        except:
            pass

        # Try Latin-1 (common for old German software)
        try:
            text = raw.decode('latin-1')
            if len(text.strip()) > 10:
                return f"[Lexware DB Export]\n{text[:50000]}"
        except:
            pass

        # Try Windows-1252
        try:
            text = raw.decode('cp1252')
            if len(text.strip()) > 10:
                return f"[Lexware DB Export]\n{text[:50000]}"
        except:
            pass

        # For .idx files - often plain text with structure
        if ext == 'idx':
            lines = []
            for i in range(0, min(len(raw), 10000), 16):
                chunk = raw[i:i+16]
                try:
                    lines.append(chunk.decode('utf-8', errors='ignore'))
                except:
                    pass
            if lines:
                return f"[Lexware Index]\n" + " ".join(lines)

        return None
    except Exception as e:
        print(f"DB extraction error: {e}")
        return None

def extract_text_from_file(filepath, filename):
    """Extract text based on file extension - returns text only"""
    ext = filename.lower().split('.')[-1]

    # For binary types, return metadata only
    if ext in NON_VECTORIZE_TYPES:
        return f"[Binary Lexware file: {ext}]"

    # For Lexware DB types - try to extract readable content
    if ext in LEXWARE_DB_TYPES:
        text = extract_lexware_db(filepath, ext)
        return text if text else f"[Lexware DB: {filename}]"

    # Text extraction for vectorizable types
    if ext == 'txt':
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            return f.read()

    elif ext == 'pdf':
        try:
            import PyPDF2
            with open(filepath, 'rb') as f:
                reader = PyPDF2.PdfReader(f)
                text = ''
                for page in reader.pages:
                    text += page.extract_text() or ''
                if text.strip():
                    return text
        except Exception as e:
            print(f"PDF error: {e}")
        # Try OCR
        try:
            import pytesseract
            from pdf2image import convert_from_path
            images = convert_from_path(filepath)
            text = ''
            for img in images:
                text += pytesseract.image_to_string(img, lang='de') or ''
            return text
        except Exception as e2:
            print(f"OCR error: {e2}")
        return ''

    elif ext == 'csv':
        import csv
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f)
            return ' '.join([' '.join(row) for row in reader])

    elif ext == 'xml':
        # XML mit besserer Lesbarkeit
        try:
            import xml.etree.ElementTree as ET
            tree = ET.parse(filepath)
            root = tree.get_root()
            # Extrahiere Text aus allen Elementen
            texts = []
            for elem in root.iter():
                if elem.text and elem.text.strip():
                    texts.append(elem.text.strip())
            if texts:
                return '\n'.join(texts)
        except:
            pass
        # Fallback
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            # Entferne XML-Tags für bessere Lesbarkeit
            import re
            text = re.sub(r'<[^>]+>', '', content)
            return text

    elif ext == 'msg':
        # Outlook .msg Dateien - versuche Text zu extrahieren
        try:
            # Versuche mit python-docmsg oder OLE File
            import olefile
            ole = olefile.OleFileIO(filepath)
            # Suche nach RTF oder Plain Text
            for stream in ole.listdir():
                if '0037001F' in stream or '0030001F' in stream:  # PR_BODY / PR_RTF
                    data = ole.openstream(stream).read()
                    try:
                        return data.decode('utf-8', errors='ignore')
                    except:
                        pass
            ole.close()
        except:
            pass
        # Fallback: binäre Datei als Text versuchen
        try:
            with open(filepath, 'rb') as f:
                data = f.read()
                # Versuche verschiedene Encodings
                for enc in ['utf-8', 'latin-1', 'cp1252']:
                    try:
                        return data.decode(enc, errors='ignore')
                    except:
                        continue
        except:
            pass
        return f"[Outlook MSG: {filename}]"

    elif ext == 'json':
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            try:
                data = json.load(f)
                return json.dumps(data, indent=2)
            except:
                return f.read()

    elif ext in ['log', 'err', 'trace']:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            return f.read()

    elif ext == 'html':
        from bs4 import BeautifulSoup
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            soup = BeautifulSoup(f.read(), 'html.parser')
            return soup.get_text()

    elif ext == 'rtf':
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            import re
            text = f.read()
            text = re.sub(r'\\[a-z]+\d*\s?', '', text)
            text = re.sub(r'[{}]', '', text)
            return text

    elif ext == 'docx':
        try:
            from docx import Document
            doc = Document(filepath)
            return '\n'.join([p.text for p in doc.paragraphs])
        except Exception as e:
            print(f"docx error: {e}")
            return ''

    elif ext == 'xlsx':
        try:
            import openpyxl
            wb = openpyxl.load_workbook(filepath, data_only=True)
            text = ''
            for sheet in wb.sheetnames:
                ws = wb[sheet]
                for row in ws.iter_rows():
                    text += ' '.join([str(cell.value) if cell.value else '' for cell in row]) + '\n'
            return text
        except Exception as e:
            print(f"xlsx error: {e}")
            return ''

    elif ext in ['mt940', 'camt', 'dta', 'sepa']:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            return f.read()

    else:
        try:
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                return f.read()
        except:
            return f"[Unsupported format: {ext}]"

def get_category(filename):
    """Get category for a file"""
    ext = filename.lower().split('.')[-1]
    _, _, category = classify_file(ext)
    return category

def process_file(filepath, filename):
    """Process single file and return document object"""
    ext = filename.lower().split('.')[-1]
    file_type, should_vectorize, category = classify_file(ext)

    text = extract_text_from_file(filepath, filename)

    if not text or len(text.strip()) < 10:
        text = f"[File: {filename}] [Type: {ext}] [Classified: {category}]"

    doc = {
        'text': text[:50000],
        'quelle': filename,
        'kategorie': category,
        'titel': filename,
        'file_type': ext,
        'vectorize': should_vectorize
    }

    # Get embedding only if should vectorize
    if should_vectorize:
        embedding = get_embedding(text[:8000])
        if embedding:
            doc['_vector'] = embedding

    return doc

def add_to_weaviate(doc, target_class=None):
    """Add document to Weaviate"""
    if target_class is None:
        target_class = CLASS_NAME

    obj = {
        'class': target_class,
        'properties': {
            'text': doc.get('text', ''),
            'quelle': doc.get('quelle', ''),
            'kategorie': doc.get('kategorie', ''),
            'titel': doc.get('titel', '')
        }
    }

    if '_vector' in doc:
        obj['vector'] = doc['_vector']

    try:
        response = requests.post(
            f'{WEAVIATE_URL}/v1/objects',
            headers={'Content-Type': 'application/json'},
            json=obj,
            timeout=60
        )
        # Slow Mode: Pause nach Weaviate-Request
        if SLOW_MODE:
            time.sleep(SLOW_DELAY_WEAVIATE)

        if response.ok:
            return response.json()
        else:
            print(f"Weaviate error: {response.text}")
            return None
    except Exception as e:
        print(f"Weaviate connection error: {e}")
        return None

@app.route('/upload', methods=['POST'])
def upload():
    """Handle file upload"""
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'Empty filename'}), 400

    # Get target class from form data
    target_class = request.form.get('class')
    if not target_class:
        target_class = request.args.get('class', CLASS_NAME)
    if not target_class:
        target_class = CLASS_NAME
    print(f"Upload target class: {target_class}")

    filename = file.filename
    results = []

    # Handle ZIP files
    if filename.lower().endswith('.zip'):
        temp_path = os.path.join(TEMP_DIR, f"{uuid.uuid4()}_{filename}")
        file.save(temp_path)

        extract_dir = tempfile.mkdtemp()
        try:
            try:
                with zipfile.ZipFile(temp_path, 'r') as zip_ref:
                    zip_ref.extractall(extract_dir)
            except NotImplementedError as zip_err:
                # Try with different methods
                try:
                    import subprocess
                    subprocess.run(['unzip', '-o', temp_path, '-d', extract_dir], check=True)
                except:
                    results.append({'file': filename, 'status': 'error', 'error': f'ZIP format not supported: {zip_err}'})
                    return jsonify({'status': 'completed', 'processed': 0, 'results': results})

            # Process all files - SLOW MODE für Stabilität
            all_files = []
            for root, dirs, files in os.walk(extract_dir):
                for fname in files:
                    all_files.append(os.path.join(root, fname))

            total_files = len(all_files)
            print(f"SLOW_MODE: Verarbeite {total_files} Dateien...")

            for i, fpath in enumerate(all_files):
                fname = os.path.basename(fpath)
                print(f"  [{i+1}/{total_files}] Verarbeite: {fname}")

                # Slow Mode: Pause zwischen Dateien
                if SLOW_MODE and i > 0:
                    time.sleep(SLOW_DELAY_FILE)

                try:
                    doc = process_file(fpath, fname)
                    if doc:
                        result = add_to_weaviate(doc, target_class)
                        status = 'success' if result else 'failed'
                        if result:
                            print(f"    ✓ Erfolgreich (ID: {result.get('id', '')[:8]}...)")
                        else:
                            print(f"    ✗ Fehlgeschlagen")
                        results.append({
                            'file': fname,
                            'status': status,
                            'id': result.get('id') if result else None,
                            'vectorized': doc.get('vectorize', False)
                        })
                    else:
                        print(f"    ⚠ Kein Text extrahiert")
                        results.append({'file': fname, 'status': 'skipped', 'error': 'No text extracted'})
                except Exception as e:
                    print(f"    ✗ Fehler: {e}")
                    results.append({'file': fname, 'status': 'error', 'error': str(e)})
        finally:
            try:
                shutil.rmtree(extract_dir)
            except: pass
            try:
                os.remove(temp_path)
            except: pass

    else:
        # Single file
        temp_path = os.path.join(TEMP_DIR, f"{uuid.uuid4()}_{filename}")
        file.save(temp_path)

        try:
            doc = process_file(temp_path, filename)
            if doc:
                result = add_to_weaviate(doc, target_class)
                results.append({
                    'file': filename,
                    'status': 'success' if result else 'failed',
                    'id': result.get('id') if result else None,
                    'vectorized': doc.get('vectorize', False)
                })
            else:
                results.append({'file': filename, 'status': 'error', 'error': 'No text extracted'})
        finally:
            os.remove(temp_path)

    return jsonify({
        'status': 'completed',
        'processed': len(results),
        'results': results
    })

@app.route('/process_folder', methods=['POST'])
def process_folder():
    """Process all files in a folder"""
    data = request.json or {}
    folder_path = data.get('path', '/tmp/uploads')
    target_class = data.get('class', CLASS_NAME)

    if not os.path.exists(folder_path):
        return jsonify({'error': 'Folder not found'}), 400

    results = []
    files = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
    total = len(files)

    for i, fname in enumerate(files):
        fpath = os.path.join(folder_path, fname)
        print(f"  [{i+1}/{total}] {fname}")

        # Slow Mode
        if SLOW_MODE and i > 0:
            time.sleep(SLOW_DELAY_FILE)

        try:
            doc = process_file(fpath, fname)
            if doc:
                result = add_to_weaviate(doc, target_class)
                results.append({
                    'file': fname,
                    'status': 'success' if result else 'failed',
                    'id': result.get('id') if result else None
                })
        except Exception as e:
            results.append({'file': fname, 'status': 'error', 'error': str(e)})

    return jsonify({
        'status': 'completed',
        'processed': len(results),
        'results': results
    })

@app.route('/health', methods=['GET'])
def health():
    """Health check"""
    return jsonify({
        'status': 'ok',
        'weaviate': WEAVIATE_URL,
        'ollama': OLLAMA_URL,
        'slow_mode': SLOW_MODE,
        'slow_delay_file': SLOW_DELAY_FILE,
        'slow_delay_weaviate': SLOW_DELAY_WEAVIATE,
        'slow_delay_ollama': SLOW_DELAY_OLLAMA,
        'vectorize_types': list(VECTORIZE_TYPES),
        'lexware_db_types': list(LEXWARE_DB_TYPES),
        'non_vectorize_types': list(NON_VECTORIZE_TYPES),
        'temp_folder': TEMP_DIR
    })

@app.route('/config', methods=['GET', 'POST'])
def config():
    """Get/Set configuration"""
    if request.method == 'POST':
        data = request.json
        global WEAVIATE_URL, OLLAMA_URL, CLASS_NAME, OLLAMA_MODEL
        if 'weaviate_url' in data:
            WEAVIATE_URL = data['weaviate_url']
        if 'ollama_url' in data:
            OLLAMA_URL = data['ollama_url']
        if 'class_name' in data:
            CLASS_NAME = data['class_name']
        if 'ollama_model' in data:
            OLLAMA_MODEL = data['ollama_model']

    return jsonify({
        'weaviate_url': WEAVIATE_URL,
        'ollama_url': OLLAMA_URL,
        'class_name': CLASS_NAME,
        'ollama_model': OLLAMA_MODEL
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
