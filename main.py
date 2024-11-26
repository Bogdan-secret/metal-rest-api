import os
import json
import subprocess
import unittest

from flask import Flask, jsonify, request
from flask_cors import CORS
from dotenv import load_dotenv

load_dotenv()

RAW_DATA_PATH = './path/to/my_dir/raw'
REQUEST_JSON_FILE = "request.json"

def create_app():
    """Factory function to create the Flask app."""
    app = Flask(__name__)
    CORS(app)
    register_routes(app)
    return app

def register_routes(app):
    """Register routes for the Flask app."""

    @app.route('/')
    def main_page():
        return 'Main page'

    @app.route('/firebase')
    def firebase_page():
        return 'FirebasePage'

    @app.route('/firebase/result', methods=['GET'])
    def get_result():
        try:
            request_data = load_json_file(REQUEST_JSON_FILE)
            if not request_data:
                return jsonify({'error': 'Invalid request data', 'status': 'failed'}), 400

            date = request_data.get('date')
            feature = request_data.get('feature')

            if not date or not feature:
                return jsonify({'error': 'Missing required fields: date or feature', 'status': 'failed'}), 400

            file_path = os.path.join(RAW_DATA_PATH, feature, f'metal_data_{date}.json')

            if os.path.exists(file_path):
                data = load_json_file(file_path)
                return jsonify(data)

            result = run_subprocess(['python3', 'job.py', date, feature])
            if result['success']:
                data = load_json_file(file_path)
                return jsonify(data)
            else:
                return jsonify(result['error']), 500

        except Exception as e:
            return jsonify({'error': f'Error processing request: {str(e)}', 'status': 'failed'}), 500

def load_json_file(file_path):
    """Load JSON data from a file."""
    try:
        with open(file_path, 'r') as file:
            return json.load(file)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        return None

def run_subprocess(command):
    """Run a subprocess command and return the result."""
    try:
        result = subprocess.run(command, capture_output=True, text=True)
        if result.returncode == 0:
            return {'success': True}
        else:
            return {'success': False, 'error': {'error': 'Failed to generate data', 'status': 'failed', 'details': result.stderr}}
    except Exception as e:
        return {'success': False, 'error': {'error': str(e), 'status': 'failed'}}

def run_flask(app):
    """Run the Flask application."""
    app.run(host='0.0.0.0', port=8081)

if __name__ == "__main__":
    app = create_app()
    run_flask(app)
