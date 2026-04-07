from __future__ import annotations

import shlex
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

from flask import Flask, jsonify, render_template, request
from werkzeug.utils import secure_filename

# 想定配置:
# docs/
#   app.py
#   csv_to_czml_hae.py
#   templates/
#     index.html
#   uploads/
#   outputs/
BASE_DIR = Path(__file__).resolve().parent
UPLOAD_DIR = BASE_DIR / 'uploads'
OUTPUT_DIR = BASE_DIR / 'outputs'
SCRIPT_PATH = BASE_DIR / 'csv_to_czml_hae.py'

UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

app = Flask(__name__, template_folder=str(BASE_DIR / 'templates'))
app.config['MAX_CONTENT_LENGTH'] = 200 * 1024 * 1024


def build_command(input_csv: Path, output_czml: Path, show_path: bool, trail: float) -> list[str]:
    cmd = [
        sys.executable,
        str(SCRIPT_PATH),
        str(input_csv),
        str(output_czml),
    ]
    if show_path:
        cmd.extend(['--show-path', '--trail', str(trail)])
    return cmd


@app.get('/')
def index():
    return render_template('index.html')


@app.get('/health')
def health():
    return jsonify({
        'ok': True,
        'base_dir': str(BASE_DIR),
        'script_found': SCRIPT_PATH.exists(),
        'script_path': str(SCRIPT_PATH),
        'template_dir': str(BASE_DIR / 'templates'),
        'template_found': (BASE_DIR / 'templates' / 'index.html').exists(),
        'python': sys.executable,
    })


@app.post('/preview-command')
def preview_command():
    filename = (request.form.get('filename') or 'input.csv').strip() or 'input.csv'
    output_name = (request.form.get('output_name') or 'output.czml').strip() or 'output.czml'
    show_path = request.form.get('show_path') == 'true'
    trail_raw = (request.form.get('trail') or '30').strip()

    try:
        trail = float(trail_raw)
    except ValueError:
        return jsonify({'ok': False, 'error': 'trail は数値で入力してください。'}), 400

    fake_input = Path(filename)
    fake_output = Path(output_name)
    cmd = build_command(fake_input, fake_output, show_path, trail)
    return jsonify({
        'ok': True,
        'command': shlex.join(cmd),
        'script_found': SCRIPT_PATH.exists(),
        'script_path': str(SCRIPT_PATH),
    })


@app.post('/run')
def run_converter():
    if not SCRIPT_PATH.exists():
        return jsonify({
            'ok': False,
            'error': '変換スクリプトが見つかりません。docs/app.py と同じ階層に csv_to_czml_hae.py を置いてください。',
            'script_path': str(SCRIPT_PATH),
        }), 500

    upload = request.files.get('csv_file')
    if upload is None or not upload.filename:
        return jsonify({'ok': False, 'error': 'CSVファイルを選択してください。'}), 400

    output_name = secure_filename((request.form.get('output_name') or '').strip()) or 'output.czml'
    if not output_name.lower().endswith('.czml'):
        output_name += '.czml'

    show_path = request.form.get('show_path') == 'true'
    trail_raw = (request.form.get('trail') or '30').strip()
    try:
        trail = float(trail_raw)
    except ValueError:
        return jsonify({'ok': False, 'error': 'trail は数値で入力してください。'}), 400

    unique_prefix = uuid4().hex[:8]
    saved_name = f'{unique_prefix}_{secure_filename(upload.filename)}'
    input_path = UPLOAD_DIR / saved_name
    output_path = OUTPUT_DIR / f'{unique_prefix}_{output_name}'
    upload.save(input_path)

    cmd = build_command(input_path, output_path, show_path, trail)
    result = subprocess.run(cmd, capture_output=True, text=True)

    return jsonify({
        'ok': result.returncode == 0,
        'returncode': result.returncode,
        'command': shlex.join(cmd),
        'stdout': result.stdout,
        'stderr': result.stderr,
        'output_path': str(output_path),
        'script_path': str(SCRIPT_PATH),
        'python': sys.executable,
    })


if __name__ == '__main__':
    app.run(debug=True)
