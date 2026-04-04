#!/usr/bin/env python3
# krysolvm.py - KrysolVM Panel - Full Web-Based LXC Container VPS Management System
# Version: 1.0
# Author: KrysolDev

import os
import sys
import json
import time
import shlex
import shutil
import asyncio
import sqlite3
import random
import threading
import logging
import subprocess
import ipaddress
import socket
import requests
import urllib3
import secrets
import hashlib

# Disable SSL warnings for nodes with verify_ssl=False
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import hmac
import base64
import uuid
import re
from datetime import datetime, timedelta
from datetime import timezone
from typing import Optional, List, Dict, Any, Tuple, Union
from functools import wraps
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import urlencode, parse_qs

# Web framework imports
import flask
from flask import Flask, render_template, request, jsonify, redirect, url_for, session, flash, send_file, send_from_directory, make_response, current_app, abort
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.middleware.proxy_fix import ProxyFix
from werkzeug.utils import secure_filename
from werkzeug.datastructures import FileStorage

# Socket.IO for real-time updates
try:
    from flask_socketio import SocketIO, emit, join_room, leave_room
    SOCKETIO_AVAILABLE = True
except ImportError:
    SOCKETIO_AVAILABLE = False
    print("Warning: flask-socketio not installed, real-time features disabled")

# SSH client for web SSH
try:
    import paramiko
    SSH_AVAILABLE = True
except ImportError:
    SSH_AVAILABLE = False
    print("Warning: paramiko not installed, SSH console disabled")

# Async support for Flask
try:
    from hypercorn.asyncio import serve
    from hypercorn.config import Config as HyperConfig
    HYPERCORN_AVAILABLE = True
except ImportError:
    HYPERCORN_AVAILABLE = False
    print("Warning: hypercorn not installed, using Flask development server")

# ASGI support
try:
    from asgiref.wsgi import WsgiToAsgi
    ASGIREF_AVAILABLE = True
except ImportError:
    ASGIREF_AVAILABLE = False
    print("Warning: asgiref not installed, ASGI support disabled")

# For file uploads
try:
    from PIL import Image
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False
    Image = None
    logging.warning("PIL not installed - image optimization disabled")

# Environment variables
PANEL_NAME = os.getenv('PANEL_NAME', 'KrysolVM')
PANEL_VERSION = os.getenv('PANEL_VERSION', '1.0')
PANEL_DEVELOPER = os.getenv('PANEL_DEVELOPER', 'KrysolDev')
SECRET_KEY = os.getenv('SECRET_KEY', secrets.token_urlsafe(32))
DATABASE_PATH = os.getenv('DATABASE_PATH', 'krysolvm.db')
HOST = os.getenv('HOST', '0.0.0.0')
PORT = int(os.getenv('PORT', 5000))
MAIN_ADMIN_USERNAME = os.getenv('MAIN_ADMIN_USERNAME', 'admin')
MAIN_ADMIN_PASSWORD = os.getenv('MAIN_ADMIN_PASSWORD', 'admin')
MAIN_ADMIN_EMAIL = os.getenv('MAIN_ADMIN_EMAIL', 'admin@localhost')
YOUR_SERVER_IP = os.getenv('YOUR_SERVER_IP', socket.gethostbyname(socket.gethostname()))
DEFAULT_STORAGE_POOL = os.getenv('DEFAULT_STORAGE_POOL', 'default')
DEBUG_MODE = os.getenv('DEBUG_MODE', 'False').lower() == 'true'
AUTO_BACKUP_INTERVAL = int(os.getenv('AUTO_BACKUP_INTERVAL', 3600))
STATS_UPDATE_INTERVAL = int(os.getenv('STATS_UPDATE_INTERVAL', 5))

# OS Options for VPS Creation and Reinstall
OS_OPTIONS = [
    {"label": "Ubuntu 20.04 LTS", "value": "ubuntu:20.04", "version": "20.04", "icon": "ubuntu"},
    {"label": "Ubuntu 22.04 LTS", "value": "ubuntu:22.04", "version": "22.04", "icon": "ubuntu"},
    {"label": "Ubuntu 24.04 LTS", "value": "ubuntu:24.04", "version": "24.04", "icon": "ubuntu"},
    {"label": "Debian 10 (Buster)", "value": "images:debian/10", "version": "10", "icon": "debian"},
    {"label": "Debian 11 (Bullseye)", "value": "images:debian/11", "version": "11", "icon": "debian"},
    {"label": "Debian 12 (Bookworm)", "value": "images:debian/12", "version": "12", "icon": "debian"},
    {"label": "Debian 13 (Trixie)", "value": "images:debian/13", "version": "13", "icon": "debian"},
]

# OS Icons mapping
OS_ICONS = {
    "ubuntu": "fab fa-ubuntu",
    "debian": "fab fa-debian",
    "centos": "fab fa-centos",
    "alpine": "fas fa-mountain",
    "fedora": "fab fa-fedora",
    "rocky": "fas fa-mountain",
    "default": "fab fa-linux"
}

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('krysolvm.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('krysolvm_panel')

# Initialize Flask app
app = Flask(__name__)
app.config['SECRET_KEY'] = SECRET_KEY
app.config['SESSION_TYPE'] = 'filesystem'
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=1)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024
app.config['UPLOAD_FOLDER'] = 'static/uploads'
app.config['ALLOWED_EXTENSIONS'] = {'png', 'jpg', 'jpeg', 'gif', 'webp', 'ico', 'svg'}
app.config['MAX_IMAGE_SIZE'] = 5 * 1024 * 1024
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1)

# Initialize SocketIO
if SOCKETIO_AVAILABLE:
    socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading', ping_timeout=60, ping_interval=25)
else:
    socketio = None

# Login manager
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'
login_manager.login_message = 'Please log in to access this page.'
login_manager.login_message_category = 'warning'

# Active console sessions tracking
active_consoles = {}
active_consoles_lock = threading.Lock()

# Database setup
@contextmanager
def get_db():
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_PATH, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.row_factory = sqlite3.Row
        yield conn
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Database error: {e}")
        raise
    finally:
        if conn:
            conn.close()

def init_db():
    """Initialize database with all tables"""
    with get_db() as conn:
        cur = conn.cursor()
        
        # Users table
        cur.execute('''CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            is_admin INTEGER DEFAULT 0,
            is_main_admin INTEGER DEFAULT 0,
            created_at TEXT NOT NULL,
            last_login TEXT,
            last_active TEXT,
            api_key TEXT UNIQUE,
            profile_picture TEXT,
            preferences TEXT DEFAULT '{}',
            two_factor_secret TEXT,
            two_factor_enabled INTEGER DEFAULT 0,
            theme TEXT DEFAULT 'default',
            language TEXT DEFAULT 'en'
        )''')
        
        cur.execute('CREATE INDEX IF NOT EXISTS idx_users_api_key ON users(api_key)')
        
        # Notifications table
        cur.execute('''CREATE TABLE IF NOT EXISTS notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            type TEXT NOT NULL,
            title TEXT NOT NULL,
            message TEXT NOT NULL,
            read INTEGER DEFAULT 0,
            created_at TEXT NOT NULL,
            expires_at TEXT,
            data TEXT,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        )''')
        
        cur.execute('CREATE INDEX IF NOT EXISTS idx_notifications_user_id ON notifications(user_id)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_notifications_read ON notifications(read)')
        
        # Nodes table
        cur.execute('''CREATE TABLE IF NOT EXISTS nodes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            location TEXT,
            total_vps INTEGER DEFAULT 50,
            used_vps INTEGER DEFAULT 0,
            tags TEXT DEFAULT '[]',
            api_key TEXT UNIQUE,
            api_key_last_used TEXT,
            url TEXT,
            is_local INTEGER DEFAULT 0,
            verify_ssl INTEGER DEFAULT 1,
            ip_addresses TEXT DEFAULT '[]',
            ip_aliases TEXT DEFAULT '[]',
            status TEXT DEFAULT 'unknown',
            last_seen TEXT,
            cpu_cores INTEGER DEFAULT 0,
            ram_total INTEGER DEFAULT 0,
            disk_total INTEGER DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )''')
        
        # VPS table
        cur.execute('''CREATE TABLE IF NOT EXISTS vps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            node_id INTEGER NOT NULL DEFAULT 1,
            container_name TEXT UNIQUE NOT NULL,
            hostname TEXT,
            ram TEXT NOT NULL,
            cpu TEXT NOT NULL,
            storage TEXT NOT NULL,
            config TEXT NOT NULL,
            os_version TEXT DEFAULT 'ubuntu:22.04',
            status TEXT DEFAULT 'stopped',
            suspended INTEGER DEFAULT 0,
            suspended_reason TEXT,
            whitelisted INTEGER DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            last_started TEXT,
            last_stopped TEXT,
            backup_schedule TEXT,
            backup_count INTEGER DEFAULT 0,
            ip_address TEXT,
            ip_alias TEXT,
            shared_with TEXT DEFAULT '[]',
            suspension_history TEXT DEFAULT '[]',
            notes TEXT,
            metadata TEXT DEFAULT '{}',
            expires_at TEXT,
            expiration_days INTEGER DEFAULT 0,
            auto_suspend_enabled INTEGER DEFAULT 0,
            last_renewed_at TEXT,
            renewal_count INTEGER DEFAULT 0,
            root_password TEXT DEFAULT 'root',
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY(node_id) REFERENCES nodes(id) ON DELETE RESTRICT
        )''')
        
        # Add root_password column if it doesn't exist (for existing databases)
        try:
            cur.execute("SELECT root_password FROM vps LIMIT 1")
        except sqlite3.OperationalError:
            cur.execute("ALTER TABLE vps ADD COLUMN root_password TEXT DEFAULT 'root'")
            logger.info("Added root_password column to vps table")
        
        # Add network bandwidth limit columns if they don't exist
        try:
            cur.execute("SELECT network_limit_ingress FROM vps LIMIT 1")
        except sqlite3.OperationalError:
            cur.execute("ALTER TABLE vps ADD COLUMN network_limit_ingress INTEGER DEFAULT 0")
            logger.info("Added network_limit_ingress column to vps table")
        
        try:
            cur.execute("SELECT network_limit_egress FROM vps LIMIT 1")
        except sqlite3.OperationalError:
            cur.execute("ALTER TABLE vps ADD COLUMN network_limit_egress INTEGER DEFAULT 0")
            logger.info("Added network_limit_egress column to vps table")
        
        try:
            cur.execute("SELECT network_priority FROM vps LIMIT 1")
        except sqlite3.OperationalError:
            cur.execute("ALTER TABLE vps ADD COLUMN network_priority INTEGER DEFAULT 5")
            logger.info("Added network_priority column to vps table")
        
        # Check and add bandwidth quota columns
        try:
            cur.execute("SELECT bandwidth_quota_gb FROM vps LIMIT 1")
        except sqlite3.OperationalError:
            cur.execute("ALTER TABLE vps ADD COLUMN bandwidth_quota_gb INTEGER DEFAULT 0")
            logger.info("Added bandwidth_quota_gb column to vps table")
        
        try:
            cur.execute("SELECT bandwidth_used_gb FROM vps LIMIT 1")
        except sqlite3.OperationalError:
            cur.execute("ALTER TABLE vps ADD COLUMN bandwidth_used_gb REAL DEFAULT 0.0")
            logger.info("Added bandwidth_used_gb column to vps table")
        
        try:
            cur.execute("SELECT bandwidth_reset_date FROM vps LIMIT 1")
        except sqlite3.OperationalError:
            cur.execute("ALTER TABLE vps ADD COLUMN bandwidth_reset_date TEXT")
            logger.info("Added bandwidth_reset_date column to vps table")
        
        # Initialize bandwidth_reset_date for existing VPS that don't have it
        try:
            cur.execute("UPDATE vps SET bandwidth_reset_date = created_at WHERE bandwidth_reset_date IS NULL")
            conn.commit()
            logger.info("Initialized bandwidth_reset_date for existing VPS")
        except Exception as e:
            logger.warning(f"Failed to initialize bandwidth_reset_date: {e}")
        
        cur.execute('CREATE INDEX IF NOT EXISTS idx_vps_user_id ON vps(user_id)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_vps_node_id ON vps(node_id)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_vps_status ON vps(status)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_vps_suspended ON vps(suspended)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_vps_expires_at ON vps(expires_at)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_vps_auto_suspend ON vps(auto_suspend_enabled)')
        
        # Settings table
        cur.execute('''CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            description TEXT,
            updated_at TEXT NOT NULL
        )''')
        
        # Initialize settings
        settings_init = [
            ('cpu_threshold', '90', 'CPU usage threshold for auto-suspension (%)'),
            ('ram_threshold', '90', 'RAM usage threshold for auto-suspension (%)'),
            ('site_name', 'KrysolVM', 'Site name'),
            ('site_description', 'High-Performance VPS Management Panel', 'Site description'),
            ('header_icon', '/static/img/logo.png', 'Header icon path'),
            ('favicon', '/static/img/favicon.ico', 'Favicon path'),
            ('footer_text', 'Powered by KrysolVM', 'Footer text'),
            ('maintenance_mode', '0', 'Maintenance mode (1=enabled, 0=disabled)'),
            ('maintenance_message', 'Site is under maintenance. Please check back later.', 'Maintenance message'),
            ('registration_enabled', '1', 'Registration enabled (1=enabled, 0=disabled)'),
            ('default_port_quota', '5', 'Default port quota for new users'),
            ('max_vps_per_user', '10', 'Maximum VPS per user'),
            ('session_timeout', '86400', 'Session timeout in seconds'),
            ('backup_enabled', '1', 'Auto backup enabled'),
            ('backup_retention', '7', 'Number of backups to retain'),
            ('smtp_host', '', 'SMTP host'),
            ('smtp_port', '587', 'SMTP port'),
            ('smtp_user', '', 'SMTP username'),
            ('smtp_pass', '', 'SMTP password'),
            ('smtp_from', '', 'SMTP from email'),
            ('theme', 'default', 'Default theme'),
            ('language', 'en', 'Default language'),
            ('timezone', 'UTC', 'Default timezone'),
        ]
        
        for key, value, description in settings_init:
            cur.execute('INSERT OR IGNORE INTO settings (key, value, description, updated_at) VALUES (?, ?, ?, ?)',
                       (key, value, description, datetime.now().isoformat()))
        
        # Port allocations table
        cur.execute('''CREATE TABLE IF NOT EXISTS port_allocations (
            user_id INTEGER PRIMARY KEY,
            allocated_ports INTEGER DEFAULT 0,
            used_ports INTEGER DEFAULT 0,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        )''')
        
        # Port forwards table
        cur.execute('''CREATE TABLE IF NOT EXISTS port_forwards (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            vps_container TEXT NOT NULL,
            vps_port INTEGER NOT NULL,
            host_port INTEGER NOT NULL,
            protocol TEXT DEFAULT 'tcp,udp',
            description TEXT,
            created_at TEXT NOT NULL,
            last_used TEXT,
            hits INTEGER DEFAULT 0,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY(vps_container) REFERENCES vps(container_name) ON DELETE CASCADE
        )''')
        
        cur.execute('CREATE INDEX IF NOT EXISTS idx_port_forwards_user_id ON port_forwards(user_id)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_port_forwards_vps_container ON port_forwards(vps_container)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_port_forwards_host_port ON port_forwards(host_port)')
        
        # Sessions table
        cur.execute('''CREATE TABLE IF NOT EXISTS sessions (
            id TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            expires TEXT NOT NULL,
            data TEXT,
            ip_address TEXT,
            user_agent TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        )''')
        
        # Activity logs table
        cur.execute('''CREATE TABLE IF NOT EXISTS activity_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            action TEXT NOT NULL,
            resource_type TEXT,
            resource_id TEXT,
            details TEXT,
            ip_address TEXT,
            user_agent TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE SET NULL
        )''')
        
        cur.execute('CREATE INDEX IF NOT EXISTS idx_activity_logs_user_id ON activity_logs(user_id)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_activity_logs_created_at ON activity_logs(created_at)')
        
        # Backups table
        cur.execute('''CREATE TABLE IF NOT EXISTS backups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vps_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            size INTEGER DEFAULT 0,
            path TEXT NOT NULL,
            created_at TEXT NOT NULL,
            expires_at TEXT,
            status TEXT DEFAULT 'completed',
            FOREIGN KEY(vps_id) REFERENCES vps(id) ON DELETE CASCADE
        )''')
        
        # OS Icons table
        cur.execute('''CREATE TABLE IF NOT EXISTS os_icons (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            os_name TEXT UNIQUE NOT NULL,
            icon_path TEXT NOT NULL,
            uploaded_at TEXT NOT NULL,
            uploaded_by INTEGER,
            FOREIGN KEY(uploaded_by) REFERENCES users(id) ON DELETE SET NULL
        )''')
        
        # API Keys table
        cur.execute('''CREATE TABLE IF NOT EXISTS api_keys (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            key TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            description TEXT,
            is_active INTEGER DEFAULT 1,
            created_at TEXT NOT NULL,
            last_used_at TEXT,
            expires_at TEXT,
            permissions TEXT DEFAULT '[]',
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        )''')
        
        cur.execute('CREATE INDEX IF NOT EXISTS idx_api_keys_user_id ON api_keys(user_id)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_api_keys_key ON api_keys(key)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_api_keys_active ON api_keys(is_active)')
        
        # Password Reset Tokens table
        cur.execute('''CREATE TABLE IF NOT EXISTS password_reset_tokens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            token TEXT UNIQUE NOT NULL,
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            used INTEGER DEFAULT 0,
            used_at TEXT,
            ip_address TEXT,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        )''')
        
        cur.execute('CREATE INDEX IF NOT EXISTS idx_password_reset_tokens_token ON password_reset_tokens(token)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_password_reset_tokens_user_id ON password_reset_tokens(user_id)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_password_reset_tokens_expires_at ON password_reset_tokens(expires_at)')
        
        # Create main admin if not exists
        cur.execute('SELECT COUNT(*) FROM users WHERE is_main_admin = 1')
        if cur.fetchone()[0] == 0:
            password_hash = generate_password_hash(MAIN_ADMIN_PASSWORD)
            api_key = generate_api_key(64)
            now = datetime.now().isoformat()
            cur.execute('''INSERT INTO users 
                (username, email, password_hash, is_admin, is_main_admin, created_at, last_login, api_key, preferences)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (MAIN_ADMIN_USERNAME, MAIN_ADMIN_EMAIL, password_hash, 1, 1, now, now, api_key, '{}'))
            
            cur.execute('INSERT INTO port_allocations (user_id, allocated_ports, updated_at) VALUES (?, ?, ?)',
                       (cur.lastrowid, 100, now))
        
        # Add local node if not exists AND not intentionally deleted
        cur.execute('SELECT COUNT(*) FROM nodes WHERE is_local = 1')
        local_node_exists = cur.fetchone()[0] > 0
        
        # Check if local node was intentionally deleted (stored in settings)
        cur.execute('SELECT value FROM settings WHERE key = ?', ('local_node_deleted',))
        local_node_deleted_setting = cur.fetchone()
        local_node_deleted = local_node_deleted_setting and local_node_deleted_setting[0] == '1'
        
        # Only create local node if it doesn't exist AND wasn't intentionally deleted
        if not local_node_exists and not local_node_deleted:
            now = datetime.now().isoformat()
            cur.execute('''INSERT INTO nodes 
                (name, location, total_vps, tags, api_key, url, is_local, ip_addresses, created_at, updated_at) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                ('Local Node', 'Local', 100, '[]', None, None, 1, 
                 json.dumps([YOUR_SERVER_IP]), now, now))
            logger.info("Local node created automatically during initialization")
        
        conn.commit()

def migrate_discord_auth():
    """Add Discord authentication fields to users table"""
    with get_db() as conn:
        cur = conn.cursor()
        
        # Check if discord_id column exists
        cur.execute("PRAGMA table_info(users)")
        columns = [column[1] for column in cur.fetchall()]
        
        if 'discord_id' not in columns:
            try:
                cur.execute('ALTER TABLE users ADD COLUMN discord_id TEXT')
                cur.execute('ALTER TABLE users ADD COLUMN discord_username TEXT')
                cur.execute('ALTER TABLE users ADD COLUMN discord_avatar TEXT')
                cur.execute('ALTER TABLE users ADD COLUMN discord_email TEXT')
                cur.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_users_discord_id ON users(discord_id) WHERE discord_id IS NOT NULL')
                conn.commit()
                logger.info("Discord authentication fields added to users table")
            except Exception as e:
                logger.error(f"Error adding Discord fields: {e}")
        
        # Add Discord settings if they don't exist
        discord_settings = {
            'discord_auth_enabled': '0',
            'discord_client_id': '',
            'discord_client_secret': '',
            'discord_redirect_uri': 'http://localhost:5000/auth/discord/callback',
            'discord_auto_register': '1',
            'discord_button_text': 'Continue with Discord'
        }
        
        for key, default_value in discord_settings.items():
            cur.execute('SELECT value FROM settings WHERE key = ?', (key,))
            if not cur.fetchone():
                cur.execute('INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?)',
                          (key, default_value, datetime.now().isoformat()))
        
        conn.commit()

def generate_api_key(length=64):
    return secrets.token_urlsafe(length)

def generate_strong_vps_password(length=24):
    """
    Generate a strong, secure password for VPS root access
    - Minimum 24 characters
    - Mix of uppercase, lowercase, numbers, and special characters
    - Cryptographically secure random generation
    """
    import string
    
    # Define character sets
    uppercase = string.ascii_uppercase
    lowercase = string.ascii_lowercase
    digits = string.digits
    # Use safe special characters that work well in shell commands
    special = '!@#$%^&*()-_=+[]{}|;:,.<>?'
    
    # Ensure at least one character from each set
    password = [
        secrets.choice(uppercase),
        secrets.choice(uppercase),
        secrets.choice(lowercase),
        secrets.choice(lowercase),
        secrets.choice(digits),
        secrets.choice(digits),
        secrets.choice(special),
        secrets.choice(special),
    ]
    
    # Fill the rest with random characters from all sets
    all_chars = uppercase + lowercase + digits + special
    password.extend(secrets.choice(all_chars) for _ in range(length - len(password)))
    
    # Shuffle to avoid predictable patterns
    secrets.SystemRandom().shuffle(password)
    
    return ''.join(password)

def store_vps_password(vps_id: int, password: str):
    """Store VPS root password securely in database metadata"""
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('''UPDATE vps SET 
                          metadata = json_set(COALESCE(metadata, '{}'), '$.root_password', ?)
                          WHERE id = ?''', (password, vps_id))
            conn.commit()
        logger.info(f"VPS {vps_id} root password stored securely")
    except Exception as e:
        logger.error(f"Failed to store password for VPS {vps_id}: {e}")

def get_vps_password(vps_id: int) -> str:
    """Retrieve VPS root password from database metadata"""
    try:
        vps = get_vps_by_id(vps_id)
        if vps:
            metadata = vps.get('metadata', {})
            if isinstance(metadata, str):
                import json
                try:
                    metadata = json.loads(metadata)
                except:
                    metadata = {}
            return metadata.get('root_password', 'root')
        return 'root'
    except Exception as e:
        logger.error(f"Failed to retrieve password for VPS {vps_id}: {e}")
        return 'root'


def get_setting(key: str, default: Any = None):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('SELECT value FROM settings WHERE key = ?', (key,))
        row = cur.fetchone()
        return row[0] if row else default

def set_setting(key: str, value: str):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('INSERT OR REPLACE INTO settings (key, value, updated_at) VALUES (?, ?, ?)',
                   (key, value, datetime.now().isoformat()))
        conn.commit()

def log_activity(user_id: Optional[int], action: str, resource_type: Optional[str] = None,
                 resource_id: Optional[str] = None, details: Optional[Dict] = None):
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('''INSERT INTO activity_logs 
                (user_id, action, resource_type, resource_id, details, ip_address, user_agent, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                (user_id, action, resource_type, resource_id, 
                 json.dumps(details) if details else None,
                 request.remote_addr if request else None,
                 request.user_agent.string if request and hasattr(request, 'user_agent') else None,
                 datetime.now().isoformat()))
            conn.commit()
    except Exception as e:
        logger.error(f"Failed to log activity: {e}")

def create_notification(user_id: int, type: str, title: str, message: str, data: Optional[Dict] = None, expires_in: Optional[int] = None):
    try:
        expires_at = None
        if expires_in:
            expires_at = (datetime.now() + timedelta(seconds=expires_in)).isoformat()
        
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('''INSERT INTO notifications 
                (user_id, type, title, message, created_at, expires_at, data)
                VALUES (?, ?, ?, ?, ?, ?, ?)''',
                (user_id, type, title, message, datetime.now().isoformat(), expires_at, json.dumps(data) if data else None))
            conn.commit()
            
            if socketio:
                socketio.emit('new_notification', {
                    'id': cur.lastrowid,
                    'type': type,
                    'title': title,
                    'message': message,
                    'created_at': datetime.now().isoformat()
                }, room=f'user_{user_id}')
    except Exception as e:
        logger.error(f"Failed to create notification: {e}")

def get_user_notifications(user_id: int, unread_only: bool = False, limit: int = 50):
    with get_db() as conn:
        cur = conn.cursor()
        query = '''SELECT * FROM notifications WHERE user_id = ?'''
        params = [user_id]
        
        if unread_only:
            query += ' AND read = 0'
        
        query += ' AND (expires_at IS NULL OR expires_at > ?)'
        params.append(datetime.now().isoformat())
        
        query += ' ORDER BY created_at DESC LIMIT ?'
        params.append(limit)
        
        cur.execute(query, params)
        notifications = [dict(row) for row in cur.fetchall()]
        
        for notif in notifications:
            if notif['data']:
                try:
                    notif['data'] = json.loads(notif['data'])
                except:
                    notif['data'] = {}
        
        return notifications

def mark_notification_read(notification_id: int, user_id: int):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('UPDATE notifications SET read = 1 WHERE id = ? AND user_id = ?', (notification_id, user_id))
        conn.commit()
        return cur.rowcount > 0

def mark_all_notifications_read(user_id: int):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('UPDATE notifications SET read = 1 WHERE user_id = ? AND read = 0', (user_id,))
        conn.commit()
        return cur.rowcount

def get_unread_notifications_count(user_id: int):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('''SELECT COUNT(*) FROM notifications 
                      WHERE user_id = ? AND read = 0 
                      AND (expires_at IS NULL OR expires_at > ?)''',
                   (user_id, datetime.now().isoformat()))
        return cur.fetchone()[0]

# Maintenance mode middleware
@app.before_request
def check_maintenance_mode():
    # Skip maintenance check for these endpoints
    if request.endpoint in ['static', 'login', 'logout', 'register', 'health', 'favicon', 'serve_static']:
        return None
    
    # Skip maintenance check for API endpoints
    if request.path.startswith('/api/'):
        return None
    
    maintenance_mode = get_setting('maintenance_mode', '0') == '1'
    
    if maintenance_mode:
        # Allow authenticated admin users
        if current_user.is_authenticated and current_user.is_admin:
            return None
        
        return render_template('maintenance.html',
                             message=get_setting('maintenance_message', 'Site is under maintenance. Please check back later.'),
                             panel_name=get_setting('site_name', 'KrysolVM')), 503

@app.after_request
def after_request(response):
    """Ensure proper headers are set for all responses"""
    try:
        # Ensure Content-Type is set
        if not response.content_type:
            response.content_type = 'text/html; charset=utf-8'
        
        # Add security headers
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['X-Frame-Options'] = 'SAMEORIGIN'
        response.headers['X-XSS-Protection'] = '1; mode=block'
        
        return response
    except Exception as e:
        logger.error(f"Error in after_request: {e}")
        return response

# User class for Flask-Login
class User(UserMixin):
    def __init__(self, user_data):
        self.id = user_data['id']
        self.username = user_data['username']
        self.email = user_data['email']
        self.password_hash = user_data['password_hash']
        self.is_admin = bool(user_data['is_admin'])
        self.is_main_admin = bool(user_data['is_main_admin'])
        self.created_at = user_data['created_at']
        self.last_login = user_data.get('last_login')
        self.last_active = user_data.get('last_active')
        self.api_key = user_data.get('api_key')
        self.profile_picture = user_data.get('profile_picture')
        try:
            self.preferences = json.loads(user_data.get('preferences', '{}'))
        except:
            self.preferences = {}
        self.two_factor_enabled = bool(user_data.get('two_factor_enabled', 0))
        self.theme = user_data.get('theme', 'default')
        self.language = user_data.get('language', 'en')
        # Discord fields
        self.discord_id = user_data.get('discord_id')
        self.discord_username = user_data.get('discord_username')
        self.discord_avatar = user_data.get('discord_avatar')
        self.discord_email = user_data.get('discord_email')

    @staticmethod
    def get(user_id):
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('SELECT * FROM users WHERE id = ?', (user_id,))
            user_data = cur.fetchone()
            if user_data:
                return User(dict(user_data))
        return None

    @staticmethod
    def get_by_username(username):
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('SELECT * FROM users WHERE username = ?', (username,))
            user_data = cur.fetchone()
            if user_data:
                return User(dict(user_data))
        return None

    @staticmethod
    def get_by_email(email):
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('SELECT * FROM users WHERE email = ?', (email,))
            user_data = cur.fetchone()
            if user_data:
                return User(dict(user_data))
        return None

    @staticmethod
    def get_by_api_key(api_key):
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('SELECT * FROM users WHERE api_key = ?', (api_key,))
            user_data = cur.fetchone()
            if user_data:
                cur.execute('UPDATE users SET last_active = ? WHERE id = ?',
                           (datetime.now().isoformat(), user_data['id']))
                conn.commit()
                return User(dict(user_data))
        return None

@login_manager.user_loader
def load_user(user_id):
    return User.get(int(user_id))

# Decorators for permissions
def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated or not current_user.is_admin:
            if request.is_json or request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                return jsonify({'success': False, 'error': 'Admin access required'}), 403
            flash('Admin access required.', 'danger')
            return redirect(url_for('dashboard'))
        return f(*args, **kwargs)
    return decorated_function

def main_admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated or not current_user.is_main_admin:
            if request.is_json or request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                return jsonify({'success': False, 'error': 'Main admin access required'}), 403
            flash('Main admin access required.', 'danger')
            return redirect(url_for('dashboard'))
        return f(*args, **kwargs)
    return decorated_function

def api_key_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        api_key = request.headers.get('X-API-Key') or request.args.get('api_key')
        if not api_key:
            return jsonify({'error': 'API key required'}), 401
        
        user = User.get_by_api_key(api_key)
        if not user:
            return jsonify({'error': 'Invalid API key'}), 401
        
        request.api_user = user
        return f(*args, **kwargs)
    return decorated_function

def vps_owner_or_admin_required(f):
    @wraps(f)
    def decorated_function(vps_id, *args, **kwargs):
        vps = get_vps_by_id(vps_id)
        if not vps:
            flash('VPS not found', 'danger')
            return redirect(url_for('vps_list'))
        
        if vps['user_id'] != current_user.id and not current_user.is_admin:
            shared_with = vps.get('shared_with', [])
            if str(current_user.id) not in [str(uid) for uid in shared_with]:
                flash('Access denied', 'danger')
                return redirect(url_for('vps_list'))
        
        return f(vps_id, *args, **kwargs)
    return decorated_function

app.jinja_env.globals.update(get_setting=get_setting)
app.jinja_env.globals.update(now=datetime.now)
app.jinja_env.globals.update(get_unread_notifications_count=get_unread_notifications_count)

# ============================================================================
# Socket.IO Events
# ============================================================================
if socketio:
    @socketio.on('connect')
    def handle_connect():
        if current_user.is_authenticated:
            join_room(f'user_{current_user.id}')
            emit('connected', {'status': 'connected', 'user_id': current_user.id})
            
            count = get_unread_notifications_count(current_user.id)
            emit('unread_count', {'count': count})
    
    @socketio.on('disconnect')
    def handle_disconnect():
        if current_user.is_authenticated:
            leave_room(f'user_{current_user.id}')
        
        # Clean up any console sessions for this socket
        with active_consoles_lock:
            to_remove = []
            for vps_id, info in active_consoles.items():
                if info.get('sid') == request.sid:
                    to_remove.append(vps_id)
                    try:
                        if 'proc' in info and info['proc']:
                            info['proc'].terminate()
                    except:
                        pass
            
            for vps_id in to_remove:
                active_consoles.pop(vps_id, None)
    
    @socketio.on('join_vps_room')
    def handle_join_vps_room(data):
        vps_id = data.get('vps_id')
        if current_user.is_authenticated and vps_id:
            room = f'vps_{vps_id}'
            join_room(room)
            emit('joined_vps_room', {'vps_id': vps_id, 'room': room})
    
    @socketio.on('leave_vps_room')
    def handle_leave_vps_room(data):
        vps_id = data.get('vps_id')
        if vps_id:
            leave_room(f'vps_{vps_id}')
    
    @socketio.on('request_vps_stats')
    def handle_request_vps_stats(data):
        vps_id = data.get('vps_id')
        if not current_user.is_authenticated or not vps_id:
            return
        
        vps = get_vps_by_id(vps_id)
        if not vps or (vps['user_id'] != current_user.id and not current_user.is_admin):
            return
        
        try:
            stats = run_sync(get_container_stats(vps['container_name'], vps['node_id']))
            emit('vps_stats', {'vps_id': vps_id, 'stats': stats})
        except Exception as e:
            logger.error(f"Error getting VPS stats for socket: {e}")
    
    @socketio.on('console_input')
    def handle_console_input(data):
        vps_id = data.get('vps_id')
        input_data = data.get('input', '')
        
        if not current_user.is_authenticated or not vps_id:
            return
        
        with active_consoles_lock:
            info = active_consoles.get(vps_id)
            if not info or info.get('sid') != request.sid:
                emit('console_output', b'Console not active or not owned by you\r\n')
                return
            
            proc = info.get('proc')
            if not proc:
                return
            
            try:
                if isinstance(input_data, str):
                    input_data = input_data.encode('utf-8', errors='replace')
                proc.stdin.write(input_data)
                proc.stdin.flush()
            except (BrokenPipeError, OSError):
                emit('console_output', b'[Connection lost]\r\n')
                active_consoles.pop(vps_id, None)
            except Exception as e:
                emit('console_output', f"[Error: {e}]\r\n".encode('utf-8'))
    
    @socketio.on('console_resize')
    def handle_console_resize(data):
        vps_id = data.get('vps_id')
        cols = data.get('cols', 80)
        rows = data.get('rows', 24)
        
        with active_consoles_lock:
            info = active_consoles.get(vps_id)
            if info and info.get('sid') == request.sid and 'proc' in info:
                try:
                    import fcntl
                    import termios
                    import struct
                    
                    fcntl.ioctl(info['proc'].stdout.fileno(), termios.TIOCSWINSZ,
                               struct.pack("HHHH", rows, cols, 0, 0))
                except:
                    pass

# ============================================================================
# LXC Command Execution
# ============================================================================
async def execute_lxc(container_name: str, command: str, timeout=120, node_id: Optional[int] = None, operation_type: str = "general"):
    if node_id is None and container_name:
        node_id = find_node_id_for_container(container_name)
    
    # Check circuit breaker for remote nodes
    if node_id and is_node_circuit_open(node_id):
        logger.info(f"Circuit breaker open for node {node_id}, skipping LXC command: {command}")
        raise Exception(f"Circuit breaker open for node {node_id}")
    
    node = get_node(node_id)
    
    if not node:
        raise Exception(f"Node {node_id} not found")
    
    full_command = f"lxc {command}"
    
    # Adjust timeout based on operation type for remote nodes
    if not node['is_local']:
        if operation_type == "create":
            timeout = min(timeout, 180)
        elif operation_type == "start":
            timeout = min(timeout, 120)
        elif operation_type == "config":
            timeout = min(timeout, 60)
        elif operation_type == "stats":
            timeout = min(timeout, 30)
        else:
            timeout = min(timeout, 30)
    
    if node['is_local']:
        try:
            logger.debug(f"Executing local command: {full_command}")
            cmd = shlex.split(full_command)
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            try:
                stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
                raise asyncio.TimeoutError(f"Command timed out after {timeout} seconds")
            
            stdout_str = stdout.decode().strip() if stdout else ""
            stderr_str = stderr.decode().strip() if stderr else ""
            
            if proc.returncode != 0:
                error = stderr_str if stderr_str else f"Command failed with return code {proc.returncode}"
                raise Exception(f"Local LXC command failed: {error}\nCommand: {full_command}")
            
            return stdout_str if stdout_str else True
        except asyncio.TimeoutError as te:
            logger.error(f"LXC command timed out: {full_command} - {str(te)}")
            raise
        except Exception as e:
            logger.error(f"LXC Error: {full_command} - {str(e)}")
            raise
    else:
        try:
            import requests
            url = f"{node['url']}/api/execute"
            data = {"command": full_command, "timeout": timeout}
            headers = {"X-API-Key": node["api_key"]}
            verify_ssl = bool(node.get('verify_ssl', 1))
            
            with get_db() as conn:
                cur = conn.cursor()
                cur.execute('UPDATE nodes SET api_key_last_used = ? WHERE id = ?',
                           (datetime.now().isoformat(), node_id))
                conn.commit()
            
            logger.debug(f"Executing remote command on {node['name']} (timeout={timeout}s): {full_command}")
            
            response = requests.post(url, json=data, headers=headers, timeout=timeout + 10, verify=verify_ssl)
            response.raise_for_status()
            
            res = response.json()
            if not res.get("success", False):
                stderr = res.get("stderr", "Command failed")
                error_msg = f"Remote LXC command failed on {node['name']}: {stderr}\nCommand: {full_command}"
                logger.warning(error_msg)
                record_node_failure(node_id)
                raise Exception(error_msg)
            
            record_node_success(node_id)
            return res.get("stdout", True)
        
        except requests.exceptions.RequestException as e:
            if hasattr(e, 'response') and hasattr(e.response, 'status_code'):
                status_code = e.response.status_code
                if status_code >= 500:
                    logger.error(f"Remote LXC error on node {node['name']} ({url}): {str(e)}")
                    record_node_failure(node_id, is_http_500=True)
                    
                    with get_db() as conn:
                        cur = conn.cursor()
                        cur.execute('UPDATE nodes SET last_seen = ? WHERE id = ?',
                                   (datetime.now().isoformat(), node_id))
                        conn.commit()
                    
                    raise Exception(f"Remote execution failed on {node['name']}: HTTP {status_code} - {str(e)}")
                elif status_code >= 400:
                    logger.warning(f"Remote LXC client error on node {node['name']} ({url}): {str(e)}")
                    raise Exception(f"Remote execution failed on {node['name']}: HTTP {status_code} - {str(e)}")
                else:
                    logger.warning(f"Remote LXC error on node {node['name']} ({url}): {str(e)}")
                    record_node_failure(node_id)
                    raise Exception(f"Remote execution failed on {node['name']}: HTTP {status_code} - {str(e)}")
            else:
                logger.warning(f"Remote LXC network error on node {node['name']} ({url}): {str(e)}")
                record_node_failure(node_id)
                
                with get_db() as conn:
                    cur = conn.cursor()
                    cur.execute('UPDATE nodes SET status = ?, last_seen = ? WHERE id = ?',
                               ('offline', datetime.now().isoformat(), node_id))
                    conn.commit()
                
                raise Exception(f"Remote execution failed on {node['name']}: {str(e)}")
        except Exception as e:
            logger.warning(f"Unexpected error executing LXC command on node {node['name']}: {str(e)}")
            record_node_failure(node_id)
            raise

def run_sync(coro):
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    
    if loop and loop.is_running():
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(asyncio.run, coro)
            return future.result()
    else:
        return asyncio.run(coro)

async def container_action_remote(node: Dict, container_name: str, action: str, timeout: int = 60) -> bool:
    try:
        import requests
        url = f"{node['url']}/api/container/{action}"
        data = {"container": container_name, "timeout": timeout}
        headers = {"X-API-Key": node["api_key"]}
        verify_ssl = bool(node.get('verify_ssl', 1))
        
        response = requests.post(url, json=data, headers=headers, timeout=timeout + 5, verify=verify_ssl)
        response.raise_for_status()
        
        result = response.json()
        if result.get("success", False):
            logger.info(f"Container {action} successful on remote node {node['name']}: {container_name}")
            return True
        else:
            error = result.get("error", "Unknown error")
            logger.error(f"Container {action} failed on remote node {node['name']}: {error}")
            return False
            
    except Exception as e:
        logger.error(f"Failed to {action} container {container_name} on remote node {node['name']}: {e}")
        return False

async def apply_lxc_config(container_name: str, node_id: int):
    try:
        await execute_lxc(container_name, f"config set {container_name} security.nesting true", node_id=node_id, operation_type="config")
        await execute_lxc(container_name, f"config set {container_name} security.privileged true", node_id=node_id, operation_type="config")
        await execute_lxc(container_name, f"config set {container_name} security.syscalls.intercept.mknod true", node_id=node_id, operation_type="config")
        await execute_lxc(container_name, f"config set {container_name} security.syscalls.intercept.setxattr true", node_id=node_id, operation_type="config")
        await execute_lxc(container_name, f"config set {container_name} linux.kernel_modules overlay,loop,nf_nat,ip_tables,ip6_tables,netlink_diag,br_netfilter", node_id=node_id, operation_type="config")
        
        try:
            await execute_lxc(container_name, f"config device add {container_name} fuse unix-char path=/dev/fuse", node_id=node_id, operation_type="config")
        except Exception as e:
            logger.warning(f"Could not add fuse device for {container_name}: {e}")
        
        raw_lxc_config = (
            "lxc.apparmor.profile = unconfined\n"
            "lxc.apparmor.allow_nesting = 1\n"
            "lxc.apparmor.allow_incomplete = 1\n"
            "\n"
            "lxc.cap.drop =\n"
            "lxc.cgroup.devices.allow = a\n"
            "lxc.cgroup2.devices.allow = a\n"
            "\n"
            "lxc.mount.auto = proc:rw sys:rw cgroup:rw shmounts:rw\n"
            "\n"
            "lxc.mount.entry = /dev/fuse dev/fuse none bind,create=file 0 0\n"
        )
        await execute_lxc(container_name, f"config set {container_name} raw.lxc '{raw_lxc_config}'", node_id=node_id, operation_type="config")
        
        logger.info(f"LXC permissions applied to {container_name} on node {node_id}")
    except Exception as e:
        logger.error(f"Failed to apply LXC config to {container_name}: {e}")
        raise

async def configure_routed_ip(container_name: str, ip_address: str, node_id: int):
    """Configure routed IP address for LXC container using routed NIC"""
    try:
        logger.info(f"Configuring routed IP {ip_address} for {container_name}")
        
        await execute_lxc(container_name, f"exec -- ip addr add {ip_address}/32 dev eth0", node_id=node_id, operation_type="config")
        logger.info(f"Added IP {ip_address}/32 to host eth0")
        
        await execute_lxc(container_name, f"config device add {container_name} pubip nic nictype=routed parent=eth0 ipv4.address={ip_address} name=eth1", node_id=node_id, operation_type="config")
        logger.info(f"Added routed NIC device for {ip_address} to {container_name}")
        
        await execute_lxc(container_name, f"restart {container_name}", node_id=node_id, operation_type="start")
        logger.info(f"Restarted {container_name} to apply routed IP configuration")
        
        await asyncio.sleep(5)
        await execute_lxc(container_name, f"exec -- ip addr del {ip_address}/32 dev eth0", node_id=node_id, operation_type="config")
        logger.info(f"Removed IP {ip_address}/32 from host eth0 (now routed to container)")
        
        logger.info(f"Successfully configured routed IP {ip_address} for {container_name}")
        
    except Exception as e:
        logger.error(f"Failed to configure routed IP {ip_address} for {container_name}: {e}")
        try:
            await execute_lxc(container_name, f"exec -- ip addr del {ip_address}/32 dev eth0", node_id=node_id, operation_type="config")
        except:
            pass
        try:
            await execute_lxc(container_name, f"config device remove {container_name} pubip", node_id=node_id, operation_type="config")
        except:
            pass
        raise

async def remove_routed_ip(container_name: str, ip_address: str, node_id: int):
    """Remove routed IP address from LXC container"""
    try:
        logger.info(f"Removing routed IP {ip_address} from {container_name}")
        
        await execute_lxc(container_name, f"config device remove {container_name} pubip", node_id=node_id, operation_type="config")
        logger.info(f"Removed routed NIC device from {container_name}")
        
        try:
            await execute_lxc(container_name, f"exec -- ip addr del {ip_address}/32 dev eth0", node_id=node_id, operation_type="config")
            logger.info(f"Removed IP {ip_address}/32 from host eth0")
        except:
            pass
        
        logger.info(f"Successfully removed routed IP {ip_address} from {container_name}")
        
    except Exception as e:
        logger.error(f"Failed to remove routed IP {ip_address} from {container_name}: {e}")
        raise

async def update_routed_ip(container_name: str, old_ip: str, new_ip: str, node_id: int):
    """Update routed IP address for LXC container"""
    try:
        logger.info(f"Updating routed IP for {container_name} from {old_ip} to {new_ip}")
        
        if old_ip:
            await remove_routed_ip(container_name, old_ip, node_id)
            await asyncio.sleep(2)
        
        if new_ip:
            await configure_routed_ip(container_name, new_ip, node_id)
        
        logger.info(f"Successfully updated routed IP for {container_name}")
        
    except Exception as e:
        logger.error(f"Failed to update routed IP for {container_name}: {e}")
        raise

async def configure_network_limits(container_name: str, ingress_mbps: int, egress_mbps: int, priority: int, node_id: int):
    """Configure network bandwidth limits for LXC container"""
    try:
        logger.info(f"Configuring network limits for {container_name}: ingress={ingress_mbps}Mbps, egress={egress_mbps}Mbps, priority={priority}")
        
        if ingress_mbps > 0:
            ingress_bps = ingress_mbps * 1000000
            await execute_lxc(container_name, f"config set {container_name} limits.network.ingress {ingress_bps}bit", node_id=node_id, operation_type="config")
            logger.info(f"Set ingress limit to {ingress_mbps}Mbps for {container_name}")
        else:
            await execute_lxc(container_name, f"config unset {container_name} limits.network.ingress", node_id=node_id, operation_type="config")
            logger.info(f"Removed ingress limit for {container_name} (unlimited)")
        
        if egress_mbps > 0:
            egress_bps = egress_mbps * 1000000
            await execute_lxc(container_name, f"config set {container_name} limits.network.egress {egress_bps}bit", node_id=node_id, operation_type="config")
            logger.info(f"Set egress limit to {egress_mbps}Mbps for {container_name}")
        else:
            await execute_lxc(container_name, f"config unset {container_name} limits.network.egress", node_id=node_id, operation_type="config")
            logger.info(f"Removed egress limit for {container_name} (unlimited)")
        
        if priority >= 1 and priority <= 10:
            await execute_lxc(container_name, f"config set {container_name} limits.network.priority {priority}", node_id=node_id, operation_type="config")
            logger.info(f"Set network priority to {priority} for {container_name}")
        
        logger.info(f"Successfully configured network limits for {container_name}")
        
    except Exception as e:
        logger.error(f"Failed to configure network limits for {container_name}: {e}")
        raise

async def configure_bandwidth_quota(container_name: str, quota_gb: int, node_id: int):
    """Configure bandwidth quota monitoring for LXC container"""
    try:
        logger.info(f"Setting up bandwidth quota monitoring for {container_name}: {quota_gb}GB")
        
        if quota_gb > 0:
            monitoring_script = f'''#!/bin/bash
# Bandwidth quota monitoring script for {container_name}
QUOTA_BYTES=$((({quota_gb} * 1024 * 1024 * 1024)))
INTERFACE="eth0"
STATS_FILE="/tmp/bandwidth_usage"

# Get current network statistics
RX_BYTES=$(cat /sys/class/net/$INTERFACE/statistics/rx_bytes 2>/dev/null || echo 0)
TX_BYTES=$(cat /sys/class/net/$INTERFACE/statistics/tx_bytes 2>/dev/null || echo 0)
TOTAL_BYTES=$((RX_BYTES + TX_BYTES))

# Store current usage
echo "$TOTAL_BYTES" > $STATS_FILE
echo "Bandwidth usage: $((TOTAL_BYTES / 1024 / 1024))MB / {quota_gb}GB"

# Check if quota exceeded
if [ $TOTAL_BYTES -gt $QUOTA_BYTES ]; then
    echo "QUOTA_EXCEEDED" > /tmp/bandwidth_status
    echo "Bandwidth quota exceeded: $((TOTAL_BYTES / 1024 / 1024 / 1024))GB / {quota_gb}GB"
else
    echo "QUOTA_OK" > /tmp/bandwidth_status
fi
'''
            
            script_cmd = f"exec {container_name} -- bash -c 'cat > /usr/local/bin/check_bandwidth.sh << \"EOF\"\n{monitoring_script}\nEOF'"
            await execute_lxc(container_name, script_cmd, node_id=node_id, operation_type="config")
            
            await execute_lxc(container_name, f"exec {container_name} -- chmod +x /usr/local/bin/check_bandwidth.sh", node_id=node_id, operation_type="config")
            
            cron_cmd = f"exec {container_name} -- bash -c 'echo \"*/5 * * * * /usr/local/bin/check_bandwidth.sh\" | crontab -'"
            await execute_lxc(container_name, cron_cmd, node_id=node_id, operation_type="config")
            
            logger.info(f"Bandwidth quota monitoring set up for {container_name} with {quota_gb}GB limit")
        else:
            await execute_lxc(container_name, f"exec {container_name} -- rm -f /usr/local/bin/check_bandwidth.sh", node_id=node_id, operation_type="config")
            await execute_lxc(container_name, f"exec {container_name} -- crontab -r", node_id=node_id, operation_type="config")
            logger.info(f"Removed bandwidth quota monitoring for {container_name}")
        
    except Exception as e:
        logger.error(f"Failed to configure bandwidth quota for {container_name}: {e}")
        raise

async def get_bandwidth_usage(container_name: str, node_id: int) -> dict:
    """Get current bandwidth usage for a container"""
    try:
        rx_cmd = f"exec {container_name} -- cat /sys/class/net/eth0/statistics/rx_bytes"
        tx_cmd = f"exec {container_name} -- cat /sys/class/net/eth0/statistics/tx_bytes"
        
        rx_bytes = int(await execute_lxc(container_name, rx_cmd, node_id=node_id, operation_type="general"))
        tx_bytes = int(await execute_lxc(container_name, tx_cmd, node_id=node_id, operation_type="general"))
        
        total_bytes = rx_bytes + tx_bytes
        total_gb = total_bytes / (1024 * 1024 * 1024)
        
        try:
            status_cmd = f"exec {container_name} -- cat /tmp/bandwidth_status"
            status = await execute_lxc(container_name, status_cmd, node_id=node_id, operation_type="general")
            quota_exceeded = "QUOTA_EXCEEDED" in status
        except:
            quota_exceeded = False
        
        return {
            'rx_bytes': rx_bytes,
            'tx_bytes': tx_bytes,
            'total_bytes': total_bytes,
            'total_gb': round(total_gb, 3),
            'quota_exceeded': quota_exceeded
        }
        
    except Exception as e:
        logger.error(f"Failed to get bandwidth usage for {container_name}: {e}")
        return {
            'rx_bytes': 0,
            'tx_bytes': 0,
            'total_bytes': 0,
            'total_gb': 0.0,
            'quota_exceeded': False
        }

async def reset_bandwidth_usage(container_name: str, node_id: int):
    """Reset bandwidth usage counter for a container"""
    try:
        await execute_lxc(container_name, f"restart {container_name}", node_id=node_id, operation_type="start")
        logger.info(f"Reset bandwidth usage for {container_name}")
    except Exception as e:
        logger.error(f"Failed to reset bandwidth usage for {container_name}: {e}")
        raise

def format_bandwidth_quota(gb: int) -> str:
    """Format bandwidth quota for display"""
    if gb == 0:
        return "Unlimited"
    elif gb >= 1000:
        return f"{gb / 1000:.1f} TB"
    else:
        return f"{gb} GB"

def get_priority_label(priority: int) -> str:
    """Get priority label for display"""
    priority_labels = {
        1: "Very Low",
        2: "Low", 
        3: "Below Normal",
        4: "Below Normal",
        5: "Normal",
        6: "Above Normal",
        7: "Above Normal", 
        8: "High",
        9: "High",
        10: "Very High"
    }
    return priority_labels.get(priority, "Normal")

async def configure_container_ip(container_name: str, ip_address: str, node_id: int):
    """Legacy function - now uses routed IP configuration"""
    await configure_routed_ip(container_name, ip_address, node_id)

async def apply_internal_permissions(container_name: str, node_id: int):
    try:
        await asyncio.sleep(5)
        
        try:
            os_check = await execute_lxc(container_name, f"exec {container_name} -- cat /etc/os-release", node_id=node_id)
            if 'alpine' in os_check.lower():
                is_alpine = True
            else:
                is_alpine = False
        except:
            is_alpine = False
        
        if is_alpine:
            commands = [
                "mkdir -p /etc/sysctl.d/",
                "echo 'net.ipv4.ip_unprivileged_port_start=0' > /etc/sysctl.d/99-custom.conf",
                "echo 'net.ipv4.ping_group_range=0 2147483647' >> /etc/sysctl.d/99-custom.conf",
                "echo 'fs.inotify.max_user_watches=524288' >> /etc/sysctl.d/99-custom.conf",
                "echo 'kernel.unprivileged_userns_clone=1' >> /etc/sysctl.d/99-custom.conf",
                "sysctl -p /etc/sysctl.d/99-custom.conf || true",
                "apk update",
                "apk add curl wget net-tools htop"
            ]
        else:
            commands = [
                "mkdir -p /etc/sysctl.d/",
                "echo 'net.ipv4.ip_unprivileged_port_start=0' > /etc/sysctl.d/99-custom.conf",
                "echo 'net.ipv4.ping_group_range=0 2147483647' >> /etc/sysctl.d/99-custom.conf",
                "echo 'fs.inotify.max_user_watches=524288' >> /etc/sysctl.d/99-custom.conf",
                "echo 'kernel.unprivileged_userns_clone=1' >> /etc/sysctl.d/99-custom.conf",
                "sysctl -p /etc/sysctl.d/99-custom.conf || true",
                "apt-get update -y || true",
                "apt-get install -y curl wget net-tools htop || true"
            ]
        
        for cmd in commands:
            try:
                await execute_lxc(container_name, f"exec {container_name} -- sh -c \"{cmd}\"", node_id=node_id)
            except Exception as cmd_error:
                logger.warning(f"Command failed in {container_name}: {cmd} - {cmd_error}")
        
        logger.info(f"Internal permissions applied to {container_name}")
    except Exception as e:
        logger.error(f"Failed to apply internal permissions to {container_name}: {e}")

async def configure_ssh_and_root_password(container_name: str, node_id: int, password: str = None):
    """Configure SSH settings and set root password to a strong generated password"""
    try:
        if not password:
            password = generate_strong_vps_password()
        
        await asyncio.sleep(2)
        
        try:
            os_check = await execute_lxc(container_name, f"exec {container_name} -- cat /etc/os-release", node_id=node_id, operation_type="config")
            if 'alpine' in os_check.lower():
                await execute_lxc(container_name, f"exec {container_name} -- apk add openssh", node_id=node_id, operation_type="config")
            else:
                await execute_lxc(container_name, f"exec {container_name} -- apt-get install -y openssh-server", node_id=node_id, operation_type="config")
        except Exception as e:
            logger.warning(f"Could not install SSH server in {container_name}: {e}")
        
        ssh_config = """# SSH LOGIN SETTINGS
PasswordAuthentication yes
PermitRootLogin yes
PubkeyAuthentication no
ChallengeResponseAuthentication no
UsePAM yes

# SFTP SETTINGS
Subsystem sftp /usr/lib/openssh/sftp-server"""
        
        ssh_config_cmd = f"cat <<'EOF' > /etc/ssh/sshd_config\n{ssh_config}\nEOF"
        await execute_lxc(container_name, f"exec {container_name} -- sh -c \"{ssh_config_cmd}\"", node_id=node_id, operation_type="config")
        logger.info(f"SSH config written for {container_name}")
        
        try:
            await execute_lxc(container_name, f"exec {container_name} -- systemctl restart ssh", node_id=node_id, operation_type="config")
        except:
            try:
                await execute_lxc(container_name, f"exec {container_name} -- service ssh restart", node_id=node_id, operation_type="config")
            except:
                try:
                    await execute_lxc(container_name, f"exec {container_name} -- /etc/init.d/sshd restart", node_id=node_id, operation_type="config")
                except Exception as e:
                    logger.warning(f"Could not restart SSH in {container_name}: {e}")
        
        logger.info(f"SSH service restarted for {container_name}")
        
        try:
            escaped_password = password.replace("'", "'\\''")
            await execute_lxc(container_name, f"exec {container_name} -- sh -c \"echo 'root:{escaped_password}' | chpasswd\"", node_id=node_id, operation_type="config")
            logger.info(f"Root password set for {container_name} (length: {len(password)} chars)")
        except Exception as e:
            logger.error(f"Failed to set root password for {container_name}: {e}")
            raise
        
        logger.info(f"SSH and root password configured for {container_name}")
        return password
    except Exception as e:
        logger.error(f"Failed to configure SSH for {container_name}: {e}")
        raise

async def live_migrate_vps(vps_id: int, source_node_id: int, target_node_id: int, container_name: str):
    """Perform live migration of VPS from source node to target node"""
    try:
        logger.info(f"Starting live migration for VPS {vps_id} ({container_name}) from node {source_node_id} to node {target_node_id}")
        
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('UPDATE vps SET status = ? WHERE id = ?', ('transferring', vps_id))
            conn.commit()
        
        update_migration_progress(vps_id, 5, "Preparing for migration...")
        
        source_node = get_node(source_node_id)
        target_node = get_node(target_node_id)
        
        if not source_node or not target_node:
            raise Exception("Source or target node not found")
        
        vps = get_vps_by_id(vps_id)
        if not vps:
            raise Exception("VPS not found")
        
        update_migration_progress(vps_id, 10, "Checking container status...")
        try:
            status = await get_container_status(container_name, source_node_id)
            was_running = status.lower() == 'running'
        except:
            was_running = False
        
        logger.info(f"Container {container_name} status on source: was_running={was_running}")
        
        if was_running:
            update_migration_progress(vps_id, 15, "Stopping container on source node...")
            try:
                await execute_lxc(container_name, f"stop {container_name}", node_id=source_node_id, operation_type="general", timeout=60)
                logger.info(f"Container {container_name} stopped successfully")
            except Exception as e:
                logger.warning(f"Graceful stop failed, forcing stop: {e}")
                try:
                    await execute_lxc(container_name, f"stop {container_name} --force", node_id=source_node_id, operation_type="general")
                except:
                    logger.warning(f"Force stop also failed, continuing anyway")
        
        update_migration_progress(vps_id, 25, "Creating fresh container on target node...")
        
        ram_gb = int(vps['ram'].replace('GB', '').strip())
        cpu = int(vps['cpu'])
        storage_gb = int(vps['storage'].replace('GB', '').strip())
        ram_mb = ram_gb * 1024
        os_version = vps['os_version']
        
        try:
            await execute_lxc(container_name, f"init {os_version} {container_name} -s {DEFAULT_STORAGE_POOL}", 
                             node_id=target_node_id, operation_type="create", timeout=120)
            logger.info(f"Container {container_name} initialized on target node")
        except Exception as e:
            logger.error(f"Failed to create container on target: {e}")
            raise Exception(f"Failed to create container on target node: {str(e)}")
        
        update_migration_progress(vps_id, 35, "Configuring resources...")
        
        try:
            await execute_lxc(container_name, f"config set {container_name} limits.memory {ram_mb}MB", 
                             node_id=target_node_id, operation_type="config")
            await execute_lxc(container_name, f"config set {container_name} limits.cpu {cpu}", 
                             node_id=target_node_id, operation_type="config")
            await execute_lxc(container_name, f"config device set {container_name} root size={storage_gb}GB", 
                             node_id=target_node_id, operation_type="config")
            logger.info(f"Resource limits applied on target node")
        except Exception as e:
            logger.warning(f"Some resource limits may not have applied: {e}")
        
        update_migration_progress(vps_id, 45, "Applying security configuration...")
        
        try:
            await apply_lxc_config(container_name, target_node_id)
            logger.info(f"LXC config applied on target node")
        except Exception as e:
            logger.warning(f"LXC config application had issues: {e}")
        
        update_migration_progress(vps_id, 50, "Starting container on target node...")
        try:
            await execute_lxc(container_name, f"start {container_name}", node_id=target_node_id, operation_type="start", timeout=60)
            logger.info(f"Container {container_name} started on target node")
            await asyncio.sleep(3)
        except Exception as e:
            logger.error(f"Failed to start container on target: {e}")
            raise Exception(f"Failed to start container on target node: {str(e)}")
        
        update_migration_progress(vps_id, 60, "Configuring network...")
        if vps.get('ip_address'):
            try:
                await configure_routed_ip(container_name, vps['ip_address'], target_node_id)
                logger.info(f"IP address {vps['ip_address']} configured on target node")
            except Exception as e:
                logger.warning(f"IP configuration had issues: {e}")
        
        update_migration_progress(vps_id, 70, "Applying system permissions...")
        try:
            await apply_internal_permissions(container_name, target_node_id)
            logger.info(f"Internal permissions applied on target node")
        except Exception as e:
            logger.warning(f"Permission application had issues: {e}")
        
        update_migration_progress(vps_id, 75, "Configuring SSH access...")
        try:
            password = get_vps_password(vps_id)
            if not password:
                password = generate_strong_vps_password()
                store_vps_password(vps_id, password)
            await configure_ssh_and_root_password(container_name, target_node_id, password)
            logger.info(f"SSH configured on target node")
        except Exception as e:
            logger.warning(f"SSH configuration had issues: {e}")
        
        update_migration_progress(vps_id, 80, "Setting up bandwidth monitoring...")
        if vps.get('bandwidth_quota_gb', 0) > 0:
            try:
                await configure_bandwidth_quota(container_name, vps['bandwidth_quota_gb'], target_node_id)
                logger.info(f"Bandwidth quota configured on target node")
            except Exception as e:
                logger.warning(f"Bandwidth configuration had issues: {e}")
        
        update_migration_progress(vps_id, 85, "Updating database records...")
        
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('''UPDATE vps SET node_id = ?, status = ?, updated_at = ? WHERE id = ?''',
                       (target_node_id, 'running' if was_running else 'stopped', datetime.now().isoformat(), vps_id))
            conn.commit()
        
        logger.info(f"Database updated: VPS {vps_id} now on node {target_node_id}")
        
        update_migration_progress(vps_id, 88, "Recreating port forwards...")
        try:
            readded = await recreate_port_forwards(container_name)
            logger.info(f"Port forwards recreated for {container_name}: {readded} forwards")
        except Exception as e:
            logger.warning(f"Port forward recreation had issues: {e}")
        
        update_migration_progress(vps_id, 92, "Cleaning up source node...")
        try:
            await execute_lxc(container_name, f"delete {container_name} --force", node_id=source_node_id, operation_type="general", timeout=60)
            logger.info(f"Container {container_name} deleted from source node {source_node_id}")
        except Exception as e:
            logger.warning(f"Failed to delete container from source node (may not exist): {e}")
        
        update_migration_progress(vps_id, 95, "Finalizing migration...")
        
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('UPDATE nodes SET used_vps = (SELECT COUNT(*) FROM vps WHERE node_id = ?) WHERE id = ?',
                       (source_node_id, source_node_id))
            cur.execute('UPDATE nodes SET used_vps = (SELECT COUNT(*) FROM vps WHERE node_id = ?) WHERE id = ?',
                       (target_node_id, target_node_id))
            conn.commit()
        
        update_migration_progress(vps_id, 100, "Migration completed successfully!")
        
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('''UPDATE vps SET 
                          metadata = json_set(COALESCE(metadata, '{}'), '$.last_migration', ?),
                          metadata = json_set(metadata, '$.migration_completed', ?),
                          metadata = json_set(metadata, '$.migrated_from_node', ?),
                          metadata = json_set(metadata, '$.migrated_to_node', ?)
                          WHERE id = ?''', 
                       (datetime.now().isoformat(), datetime.now().isoformat(), 
                        source_node_id, target_node_id, vps_id))
            conn.commit()
        
        vps = get_vps_by_id(vps_id)
        if vps:
            create_notification(vps['user_id'], 'success', 'VPS Migrated', 
                              f'Your VPS {container_name} has been successfully migrated to {target_node["name"]}.')
        
        logger.info(f"Live migration completed successfully for VPS {vps_id} ({container_name})")
        
    except Exception as e:
        logger.error(f"Live migration failed for VPS {vps_id} ({container_name}): {e}", exc_info=True)
        
        try:
            await execute_lxc(container_name, f"delete {container_name} --force", node_id=target_node_id, operation_type="general", timeout=30)
            logger.info(f"Cleaned up failed container on target node")
        except:
            pass
        
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('''UPDATE vps SET 
                          status = 'stopped',
                          metadata = json_set(COALESCE(metadata, '{}'), '$.migration_error', ?),
                          metadata = json_set(metadata, '$.migration_failed_at', ?)
                          WHERE id = ?''', (str(e), datetime.now().isoformat(), vps_id))
            conn.commit()
        
        vps = get_vps_by_id(vps_id)
        if vps:
            create_notification(vps['user_id'], 'danger', 'VPS Migration Failed', 
                              f'Failed to migrate VPS {container_name}. Error: {str(e)[:100]}')
        
        raise

def update_migration_progress(vps_id: int, progress: int, message: str):
    """Update VPS migration progress in database"""
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('''UPDATE vps SET 
                          metadata = json_set(
                              json_set(COALESCE(metadata, '{}'), '$.migration_progress', ?),
                              '$.migration_message', ?
                          )
                          WHERE id = ?''', (progress, message, vps_id))
            conn.commit()
        logger.info(f"VPS {vps_id} migration progress: {progress}% - {message}")
    except Exception as e:
        logger.error(f"Failed to update migration progress for VPS {vps_id}: {e}")

async def install_vps_async(vps_id: int, container_name: str, node_id: int, ram_mb: int, 
                           cpu: int, disk: int, os_version: str, ip_address: str, bandwidth_quota_gb: int):
    """Asynchronously install VPS with progress tracking"""
    try:
        logger.info(f"Starting VPS installation for {container_name} (VPS ID: {vps_id})")
        
        vps_password = generate_strong_vps_password()
        logger.info(f"Generated strong password for VPS {vps_id} (length: {len(vps_password)} chars)")
        
        update_vps_installation_progress(vps_id, 10, "Initializing container...")
        await execute_lxc(container_name, f"init {os_version} {container_name} -s {DEFAULT_STORAGE_POOL}", 
                         node_id=node_id, operation_type="create")
        
        update_vps_installation_progress(vps_id, 25, "Configuring CPU and RAM...")
        await execute_lxc(container_name, f"config set {container_name} limits.memory {ram_mb}MB", 
                         node_id=node_id, operation_type="config")
        await execute_lxc(container_name, f"config set {container_name} limits.cpu {cpu}", 
                         node_id=node_id, operation_type="config")
        
        update_vps_installation_progress(vps_id, 40, "Configuring disk storage...")
        await execute_lxc(container_name, f"config device set {container_name} root size={disk}GB", 
                         node_id=node_id, operation_type="config")
        
        update_vps_installation_progress(vps_id, 55, "Applying security settings...")
        await apply_lxc_config(container_name, node_id)
        
        update_vps_installation_progress(vps_id, 70, "Starting container...")
        await execute_lxc(container_name, f"start {container_name}", node_id=node_id, operation_type="start")
        
        if ip_address:
            update_vps_installation_progress(vps_id, 80, "Configuring IP address...")
            await configure_routed_ip(container_name, ip_address, node_id)
        
        update_vps_installation_progress(vps_id, 85, "Applying permissions...")
        await apply_internal_permissions(container_name, node_id)
        
        update_vps_installation_progress(vps_id, 90, "Configuring SSH and setting secure password...")
        password_set = await configure_ssh_and_root_password(container_name, node_id, vps_password)
        
        store_vps_password(vps_id, vps_password)
        logger.info(f"VPS {vps_id} password stored securely in database")
        
        if bandwidth_quota_gb > 0:
            update_vps_installation_progress(vps_id, 95, "Setting up bandwidth monitoring...")
            await configure_bandwidth_quota(container_name, bandwidth_quota_gb, node_id)
        
        update_vps_installation_progress(vps_id, 100, "Installation complete!")
        
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('''UPDATE vps SET status = 'running', 
                          metadata = json_set(COALESCE(metadata, '{}'), '$.installation_completed', ?)
                          WHERE id = ?''', (datetime.now().isoformat(), vps_id))
            conn.commit()
        
        vps = get_vps_by_id(vps_id)
        if vps:
            create_notification(vps['user_id'], 'success', 'VPS Ready', 
                              f'Your VPS {container_name} is now ready! Root password has been set. Check VPS details to view credentials.')
        
        logger.info(f"VPS installation completed successfully for {container_name} (VPS ID: {vps_id})")
        
    except Exception as e:
        logger.error(f"VPS installation failed for {container_name} (VPS ID: {vps_id}): {e}", exc_info=True)
        
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('''UPDATE vps SET status = 'failed', 
                          metadata = json_set(COALESCE(metadata, '{}'), '$.installation_error', ?)
                          WHERE id = ?''', (str(e), vps_id))
            conn.commit()
        
        vps = get_vps_by_id(vps_id)
        if vps:
            create_notification(vps['user_id'], 'danger', 'VPS Installation Failed', 
                              f'Failed to install VPS {container_name}. Please contact support.')
        
        try:
            await execute_lxc(container_name, f"delete {container_name} --force", 
                            node_id=node_id, operation_type="general")
        except:
            pass

def update_vps_installation_progress(vps_id: int, progress: int, message: str):
    """Update VPS installation progress in database"""
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('''UPDATE vps SET 
                          metadata = json_set(
                              json_set(COALESCE(metadata, '{}'), '$.installation_progress', ?),
                              '$.installation_message', ?
                          )
                          WHERE id = ?''', (progress, message, vps_id))
            conn.commit()
        logger.info(f"VPS {vps_id} installation progress: {progress}% - {message}")
    except Exception as e:
        logger.error(f"Failed to update installation progress for VPS {vps_id}: {e}")

# ============================================================================
# Database helper functions
# ============================================================================
def get_nodes() -> List[Dict]:
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('SELECT * FROM nodes ORDER BY name')
        rows = cur.fetchall()
        nodes = []
        for row in rows:
            node = dict(row)
            try:
                node['tags'] = json.loads(node['tags']) if node['tags'] else []
            except:
                node['tags'] = []
            try:
                node['ip_addresses'] = json.loads(node['ip_addresses']) if node['ip_addresses'] else []
            except:
                node['ip_addresses'] = []
            try:
                node['ip_aliases'] = json.loads(node['ip_aliases']) if node['ip_aliases'] else []
            except:
                node['ip_aliases'] = []
            nodes.append(node)
        return nodes

def get_node(node_id: Optional[int]) -> Optional[Dict]:
    if node_id is None:
        return None
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('SELECT * FROM nodes WHERE id = ?', (node_id,))
        row = cur.fetchone()
        if row:
            node = dict(row)
            try:
                node['tags'] = json.loads(node['tags']) if node['tags'] else []
            except:
                node['tags'] = []
            try:
                node['ip_addresses'] = json.loads(node['ip_addresses']) if node['ip_addresses'] else []
            except:
                node['ip_addresses'] = []
            try:
                node['ip_aliases'] = json.loads(node['ip_aliases']) if node['ip_aliases'] else []
            except:
                node['ip_aliases'] = []
            return node
    return None

def update_node(node_id: int, **kwargs):
    with get_db() as conn:
        cur = conn.cursor()
        fields = []
        values = []
        for key, value in kwargs.items():
            if key in ['tags', 'ip_addresses', 'ip_aliases'] and isinstance(value, (list, dict)):
                value = json.dumps(value)
            fields.append(f"{key} = ?")
            values.append(value)
        values.append(node_id)
        values.append(datetime.now().isoformat())
        cur.execute(f'UPDATE nodes SET {", ".join(fields)}, updated_at = ? WHERE id = ?', values)
        conn.commit()

def get_current_vps_count(node_id: int) -> int:
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('SELECT COUNT(*) FROM vps WHERE node_id = ?', (node_id,))
        count = cur.fetchone()[0]
        return count

def get_vps_for_user(user_id: int) -> List[Dict]:
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('SELECT * FROM vps WHERE user_id = ? ORDER BY created_at DESC', (user_id,))
        rows = cur.fetchall()
        vps_list = []
        for row in rows:
            vps = dict(row)
            try:
                vps['shared_with'] = json.loads(vps['shared_with']) if vps['shared_with'] else []
            except:
                vps['shared_with'] = []
            try:
                vps['suspension_history'] = json.loads(vps['suspension_history']) if vps['suspension_history'] else []
            except:
                vps['suspension_history'] = []
            try:
                vps['metadata'] = json.loads(vps['metadata']) if vps['metadata'] else {}
            except:
                vps['metadata'] = {}
            vps['suspended'] = bool(vps['suspended'])
            vps['whitelisted'] = bool(vps['whitelisted'])
            
            if 'bandwidth_quota_gb' not in vps or vps['bandwidth_quota_gb'] is None:
                vps['bandwidth_quota_gb'] = 0
            if 'bandwidth_used_gb' not in vps or vps['bandwidth_used_gb'] is None:
                vps['bandwidth_used_gb'] = 0.0
            if 'bandwidth_reset_date' not in vps or vps['bandwidth_reset_date'] is None:
                vps['bandwidth_reset_date'] = vps.get('created_at')
            
            vps_list.append(vps)
        return vps_list

def get_all_vps() -> List[Dict]:
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('SELECT * FROM vps ORDER BY created_at DESC')
        rows = cur.fetchall()
        vps_list = []
        for row in rows:
            vps = dict(row)
            try:
                vps['shared_with'] = json.loads(vps['shared_with']) if vps['shared_with'] else []
            except:
                vps['shared_with'] = []
            try:
                vps['suspension_history'] = json.loads(vps['suspension_history']) if vps['suspension_history'] else []
            except:
                vps['suspension_history'] = []
            try:
                vps['metadata'] = json.loads(vps['metadata']) if vps['metadata'] else {}
            except:
                vps['metadata'] = {}
            vps['suspended'] = bool(vps['suspended'])
            vps['whitelisted'] = bool(vps['whitelisted'])
            vps_list.append(vps)
        return vps_list

def get_vps_by_id(vps_id: int) -> Optional[Dict]:
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('SELECT * FROM vps WHERE id = ?', (vps_id,))
        row = cur.fetchone()
        if row:
            vps = dict(row)
            try:
                vps['shared_with'] = json.loads(vps['shared_with']) if vps['shared_with'] else []
            except:
                vps['shared_with'] = []
            try:
                vps['suspension_history'] = json.loads(vps['suspension_history']) if vps['suspension_history'] else []
            except:
                vps['suspension_history'] = []
            try:
                vps['metadata'] = json.loads(vps['metadata']) if vps['metadata'] else {}
            except:
                vps['metadata'] = {}
            vps['suspended'] = bool(vps['suspended'])
            vps['whitelisted'] = bool(vps['whitelisted'])
            
            if 'bandwidth_quota_gb' not in vps or vps['bandwidth_quota_gb'] is None:
                vps['bandwidth_quota_gb'] = 0
            if 'bandwidth_used_gb' not in vps or vps['bandwidth_used_gb'] is None:
                vps['bandwidth_used_gb'] = 0.0
            if 'bandwidth_reset_date' not in vps or vps['bandwidth_reset_date'] is None:
                vps['bandwidth_reset_date'] = vps.get('created_at')
            
            return vps
    return None

def get_vps_by_container(container_name: str) -> Optional[Dict]:
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('SELECT * FROM vps WHERE container_name = ?', (container_name,))
        row = cur.fetchone()
        if row:
            vps = dict(row)
            try:
                vps['shared_with'] = json.loads(vps['shared_with']) if vps['shared_with'] else []
            except:
                vps['shared_with'] = []
            try:
                vps['suspension_history'] = json.loads(vps['suspension_history']) if vps['suspension_history'] else []
            except:
                vps['suspension_history'] = []
            try:
                vps['metadata'] = json.loads(vps['metadata']) if vps['metadata'] else {}
            except:
                vps['metadata'] = {}
            vps['suspended'] = bool(vps['suspended'])
            vps['whitelisted'] = bool(vps['whitelisted'])
            
            if 'bandwidth_quota_gb' not in vps or vps['bandwidth_quota_gb'] is None:
                vps['bandwidth_quota_gb'] = 0
            if 'bandwidth_used_gb' not in vps or vps['bandwidth_used_gb'] is None:
                vps['bandwidth_used_gb'] = 0.0
            if 'bandwidth_reset_date' not in vps or vps['bandwidth_reset_date'] is None:
                vps['bandwidth_reset_date'] = vps.get('created_at')
            
            return vps
    return None

def create_vps(user_id: int, node_id: int, container_name: str, ram: str, cpu: str, storage: str,
               config: str, os_version: str, hostname: Optional[str] = None,
               ip_address: Optional[str] = None, ip_alias: Optional[str] = None,
               expiration_days: int = 0, auto_suspend_enabled: bool = False,
               bandwidth_quota_gb: int = 0, status: str = 'stopped') -> int:
    now = datetime.now().isoformat()
    expires_at = None
    
    if auto_suspend_enabled and expiration_days > 0:
        expires_at = (datetime.now() + timedelta(days=expiration_days)).isoformat()
    
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('''INSERT INTO vps
            (user_id, node_id, container_name, hostname, ram, cpu, storage, config, os_version,
             status, created_at, updated_at, ip_address, ip_alias, shared_with, suspension_history, metadata,
             expires_at, expiration_days, auto_suspend_enabled, renewal_count, bandwidth_quota_gb, 
             bandwidth_used_gb, bandwidth_reset_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (user_id, node_id, container_name, hostname or container_name, ram, cpu, storage, config, os_version,
             status, now, now, ip_address, ip_alias, '[]', '[]', '{}',
             expires_at, expiration_days, 1 if auto_suspend_enabled else 0, 0, bandwidth_quota_gb, 
             0.0, now))
        conn.commit()
        vps_id = cur.lastrowid
        
        cur.execute('UPDATE nodes SET used_vps = (SELECT COUNT(*) FROM vps WHERE node_id = ?) WHERE id = ?',
                   (node_id, node_id))
        conn.commit()
        
        log_activity(user_id, 'create_vps', 'vps', str(vps_id), {'container': container_name})
        
        if status != 'installing':
            if auto_suspend_enabled and expiration_days > 0:
                create_notification(user_id, 'success', 'VPS Created', 
                                  f'Your VPS {container_name} has been created successfully. It will auto-suspend in {expiration_days} days.')
            else:
                create_notification(user_id, 'success', 'VPS Created', f'Your VPS {container_name} has been created successfully.')
        
        return vps_id

def update_vps(vps_id: int, **kwargs):
    logger.debug(f"update_vps called for VPS {vps_id} with parameters: {kwargs}")
    
    with get_db() as conn:
        cur = conn.cursor()
        fields = []
        values = []
        for key, value in kwargs.items():
            if key in ['shared_with', 'suspension_history', 'metadata']:
                json_value = json.dumps(value)
                logger.debug(f"Converting {key} to JSON: {value} -> {json_value}")
                value = json_value
            fields.append(f"{key} = ?")
            values.append(value)
        
        fields.append("updated_at = ?")
        values.append(datetime.now().isoformat())
        
        values.append(vps_id)
        
        sql = f'UPDATE vps SET {", ".join(fields)} WHERE id = ?'
        logger.debug(f"Executing SQL: {sql} with values: {values}")
        
        cur.execute(sql, values)
        conn.commit()
        
        if cur.rowcount > 0:
            important_fields = ['status', 'suspended', 'hostname', 'os_version']
            if any(field in kwargs for field in important_fields):
                logger.info(f"VPS {vps_id} updated: {', '.join(f'{k}={v}' for k, v in kwargs.items() if k in important_fields)}")
            else:
                logger.debug(f"VPS {vps_id} updated successfully. Rows affected: {cur.rowcount}")
        else:
            logger.warning(f"VPS {vps_id} update failed - no rows affected")
        
        if 'shared_with' in kwargs:
            cur.execute('SELECT shared_with FROM vps WHERE id = ?', (vps_id,))
            row = cur.fetchone()
            if row:
                logger.debug(f"Verified shared_with in DB: {row[0]}")
            else:
                logger.error(f"VPS {vps_id} not found after update!")

def delete_vps(vps_id: int):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('SELECT container_name, user_id, node_id FROM vps WHERE id = ?', (vps_id,))
        row = cur.fetchone()
        if row:
            container_name, user_id, node_id = row
            
            logger.info(f"Deleting VPS {vps_id} ({container_name}) and all related records")
            
            try:
                cur.execute('DELETE FROM port_forwards WHERE vps_container = ?', (container_name,))
                logger.info(f"Deleted port forwards for container {container_name}")
            except Exception as e:
                logger.warning(f"Error deleting port forwards for {container_name}: {e}")
            
            try:
                cur.execute('DELETE FROM backups WHERE vps_id = ?', (vps_id,))
                logger.info(f"Deleted backups for VPS {vps_id}")
            except Exception as e:
                logger.warning(f"Error deleting backups for VPS {vps_id}: {e}")
            
            try:
                cur.execute('DELETE FROM vps_metrics WHERE vps_id = ?', (vps_id,))
                logger.info(f"Deleted metrics data for VPS {vps_id}")
            except sqlite3.OperationalError as e:
                if "no such table" in str(e).lower():
                    logger.info(f"vps_metrics table doesn't exist, skipping metrics deletion")
                else:
                    logger.warning(f"Error deleting metrics for VPS {vps_id}: {e}")
            except Exception as e:
                logger.warning(f"Error deleting metrics for VPS {vps_id}: {e}")
            
            try:
                cur.execute('DELETE FROM activity_logs WHERE resource_type = ? AND resource_id = ?', ('vps', str(vps_id)))
                logger.info(f"Deleted activity logs for VPS {vps_id}")
            except Exception as e:
                logger.warning(f"Error deleting activity logs for VPS {vps_id}: {e}")
            
            try:
                cur.execute('DELETE FROM notifications WHERE message LIKE ?', (f'%{container_name}%',))
                logger.info(f"Deleted notifications for container {container_name}")
            except Exception as e:
                logger.warning(f"Error deleting notifications for {container_name}: {e}")
            
            try:
                cur.execute('DELETE FROM vps WHERE id = ?', (vps_id,))
                logger.info(f"Deleted VPS record {vps_id}")
            except Exception as e:
                logger.error(f"Error deleting VPS record {vps_id}: {e}")
                raise
            
            conn.commit()
            
            if node_id:
                try:
                    cur.execute('UPDATE nodes SET used_vps = (SELECT COUNT(*) FROM vps WHERE node_id = ?) WHERE id = ?',
                               (node_id, node_id))
                    conn.commit()
                    logger.info(f"Updated VPS count for node {node_id}")
                except Exception as e:
                    logger.warning(f"Error updating VPS count for node {node_id}: {e}")
            
            try:
                log_activity(user_id, 'delete_vps', 'vps', str(vps_id), {'container': container_name})
                create_notification(user_id, 'info', 'VPS Deleted', f'Your VPS {container_name} has been deleted successfully.')
            except Exception as e:
                logger.warning(f"Error logging deletion activity for VPS {vps_id}: {e}")
            
            logger.info(f"Successfully deleted VPS {vps_id} ({container_name})")
        else:
            logger.warning(f"VPS {vps_id} not found for deletion")

def find_node_id_for_container(container_name: str) -> int:
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('SELECT node_id FROM vps WHERE container_name = ?', (container_name,))
        row = cur.fetchone()
        return row[0] if row else 1

@app.context_processor
def utility_processor():
    return dict(
        now=datetime.now,
        get_current_vps_count=get_current_vps_count,
        get_setting=get_setting,
        get_unread_notifications_count=get_unread_notifications_count,
        OS_ICONS=OS_ICONS,
        get_os_label=get_os_label,
        get_os_icon_name=get_os_icon_name,
        is_vps_suspended=is_vps_suspended,
        is_vps_whitelisted=is_vps_whitelisted,
        PANEL_VERSION=PANEL_VERSION,
        PANEL_NAME=PANEL_NAME,
        PANEL_DEVELOPER=PANEL_DEVELOPER
    )

def is_vps_suspended(vps):
    suspended = vps.get("suspended", 0)
    if isinstance(suspended, bool):
        return suspended
    return int(suspended) == 1

def is_vps_whitelisted(vps):
    whitelisted = vps.get("whitelisted", 0)
    if isinstance(whitelisted, bool):
        return whitelisted
    return int(whitelisted) == 1

def get_os_label(os_value):
    for os_option in OS_OPTIONS:
        if os_option["value"] == os_value:
            return os_option["label"]
    return os_value

def get_os_icon_name(os_value):
    for os_option in OS_OPTIONS:
        if os_option["value"] == os_value:
            return os_option.get("icon", "default")
    return "default"

def refresh_vps_status(vps_id):
    vps = get_vps_by_id(vps_id)
    if not vps:
        return None
    
    if is_vps_suspended(vps):
        return 'suspended'
    
    try:
        status = run_sync(get_container_status(vps['container_name'], vps['node_id']))
        if status != vps['status']:
            update_vps(vps_id, status=status)
        return status
    except Exception as e:
        logger.error(f"Error refreshing VPS {vps_id} status: {e}")
        return vps.get('status', 'unknown')

# ============================================================================
# IP Address and Alias Functions
# ============================================================================
def get_node_display_ip(node_id: int, use_alias: bool = True) -> Optional[str]:
    node = get_node(node_id)
    if not node:
        return None
    
    if use_alias and node['ip_aliases'] and len(node['ip_aliases']) > 0:
        return node['ip_aliases'][0]
    elif node['ip_addresses'] and len(node['ip_addresses']) > 0:
        return node['ip_addresses'][0]
    return None

def get_node_all_ips(node_id: int) -> List[Dict[str, str]]:
    node = get_node(node_id)
    if not node:
        return []
    
    ips = []
    for alias in node.get('ip_aliases', []):
        ips.append({'type': 'alias', 'value': alias})
    
    for ip in node.get('ip_addresses', []):
        ips.append({'type': 'ip', 'value': ip})
    
    return ips

def get_vps_display_ip(vps: Dict) -> Optional[str]:
    if vps.get('ip_alias'):
        return vps['ip_alias']
    return vps.get('ip_address')

def format_ip_for_display(ip: str, port: Optional[int] = None) -> str:
    if port:
        return f"{ip}:{port}"
    return ip

# ============================================================================
# Port forwarding functions
# ============================================================================
def get_user_allocation(user_id: int) -> int:
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('SELECT allocated_ports FROM port_allocations WHERE user_id = ?', (user_id,))
        row = cur.fetchone()
        return row[0] if row else 0

def get_user_used_ports(user_id: int) -> int:
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('SELECT COUNT(*) FROM port_forwards WHERE user_id = ?', (user_id,))
        row = cur.fetchone()
        return row[0] if row else 0

def allocate_ports(user_id: int, amount: int):
    now = datetime.now().isoformat()
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('''INSERT OR REPLACE INTO port_allocations (user_id, allocated_ports, used_ports, updated_at)
                       VALUES (?, COALESCE((SELECT allocated_ports FROM port_allocations WHERE user_id = ?), 0) + ?, 
                               COALESCE((SELECT used_ports FROM port_allocations WHERE user_id = ?), 0), ?)''',
                    (user_id, user_id, amount, user_id, now))
        conn.commit()

def deallocate_ports(user_id: int, amount: int):
    now = datetime.now().isoformat()
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('''UPDATE port_allocations 
                       SET allocated_ports = MAX(0, allocated_ports - ?),
                           updated_at = ?
                       WHERE user_id = ?''',
                    (amount, now, user_id))
        conn.commit()

def get_available_host_port(node_id: int) -> Optional[int]:
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('SELECT host_port FROM port_forwards WHERE vps_container IN (SELECT container_name FROM vps WHERE node_id = ?)',
                    (node_id,))
        used_ports = set(row[0] for row in cur.fetchall())
    
    for _ in range(100):
        port = random.randint(20000, 50000)
        if port not in used_ports:
            return port
    return None

async def create_port_forward(user_id: int, container: str, vps_port: int, node_id: int,
                              protocol: str = 'tcp,udp', description: str = '') -> Optional[int]:
    host_port = get_available_host_port(node_id)
    if not host_port:
        return None
    
    try:
        if 'tcp' in protocol:
            await execute_lxc(container, f"config device add {container} tcp_proxy_{host_port} proxy listen=tcp:0.0.0.0:{host_port} connect=tcp:127.0.0.1:{vps_port}", node_id=node_id)
        
        if 'udp' in protocol:
            await execute_lxc(container, f"config device add {container} udp_proxy_{host_port} proxy listen=udp:0.0.0.0:{host_port} connect=udp:127.0.0.1:{vps_port}", node_id=node_id)
        
        now = datetime.now().isoformat()
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('''INSERT INTO port_forwards 
                (user_id, vps_container, vps_port, host_port, protocol, description, created_at, last_used, hits)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (user_id, container, vps_port, host_port, protocol, description, now, now, 0))
            conn.commit()
            
            cur.execute('UPDATE port_allocations SET used_ports = used_ports + 1, updated_at = ? WHERE user_id = ?',
                       (now, user_id))
            conn.commit()
        
        log_activity(user_id, 'create_port_forward', 'port', str(host_port),
                    {'container': container, 'vps_port': vps_port, 'host_port': host_port, 'protocol': protocol})
        create_notification(user_id, 'success', 'Port Forward Created', 
                          f'Port {vps_port} forwarded to port {host_port} on {container}')
        
        if socketio:
            socketio.emit('port_forward_created', {
                'host_port': host_port,
                'vps_port': vps_port,
                'container': container
            }, room=f'user_{user_id}')
        
        return host_port
    except Exception as e:
        logger.error(f"Failed to create port forward: {e}")
        return None

async def remove_port_forward(forward_id: int) -> Tuple[bool, Optional[int]]:
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('SELECT user_id, vps_container, host_port, protocol FROM port_forwards WHERE id = ?', (forward_id,))
        row = cur.fetchone()
        if not row:
            return False, None
        user_id, container, host_port, protocol = row
    
    node_id = find_node_id_for_container(container)
    try:
        if 'tcp' in protocol:
            try:
                await execute_lxc(container, f"config device remove {container} tcp_proxy_{host_port}", node_id=node_id)
            except:
                pass
        
        if 'udp' in protocol:
            try:
                await execute_lxc(container, f"config device remove {container} udp_proxy_{host_port}", node_id=node_id)
            except:
                pass
        
        now = datetime.now().isoformat()
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('DELETE FROM port_forwards WHERE id = ?', (forward_id,))
            conn.commit()
            
            cur.execute('UPDATE port_allocations SET used_ports = used_ports - 1, updated_at = ? WHERE user_id = ?',
                       (now, user_id))
            conn.commit()
        
        log_activity(user_id, 'remove_port_forward', 'port', str(host_port))
        create_notification(user_id, 'info', 'Port Forward Removed', f'Port forward {host_port} has been removed.')
        
        if socketio:
            socketio.emit('port_forward_removed', {
                'host_port': host_port,
                'container': container
            }, room=f'user_{user_id}')
        
        return True, user_id
    except Exception as e:
        logger.error(f"Failed to remove port forward {forward_id}: {e}")
        return False, None

def get_user_forwards(user_id: int) -> List[Dict]:
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('SELECT * FROM port_forwards WHERE user_id = ? ORDER BY created_at DESC', (user_id,))
        rows = cur.fetchall()
        return [dict(row) for row in rows]

async def recreate_port_forwards(container_name: str) -> int:
    node_id = find_node_id_for_container(container_name)
    readded_count = 0
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('SELECT vps_port, host_port, protocol FROM port_forwards WHERE vps_container = ?', (container_name,))
        rows = cur.fetchall()
    
    for row in rows:
        vps_port, host_port, protocol = row['vps_port'], row['host_port'], row['protocol']
        try:
            if 'tcp' in protocol:
                await execute_lxc(container_name, f"config device add {container_name} tcp_proxy_{host_port} proxy listen=tcp:0.0.0.0:{host_port} connect=tcp:127.0.0.1:{vps_port}", node_id=node_id)
            if 'udp' in protocol:
                await execute_lxc(container_name, f"config device add {container_name} udp_proxy_{host_port} proxy listen=udp:0.0.0.0:{host_port} connect=udp:127.0.0.1:{vps_port}", node_id=node_id)
            logger.info(f"Re-added port forward {host_port}->{vps_port} for {container_name}")
            readded_count += 1
        except Exception as e:
            logger.error(f"Failed to re-add port forward {host_port}->{vps_port} for {container_name}: {e}")
    
    return readded_count

async def update_port_forward_hit(host_port: int):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('UPDATE port_forwards SET hits = hits + 1, last_used = ? WHERE host_port = ?',
                   (datetime.now().isoformat(), host_port))
        conn.commit()

def relativeTime(dt):
    if not dt:
        return "Never"

    if isinstance(dt, str):
        try:
            dt = datetime.fromisoformat(dt)
        except:
            return "Invalid date"

    now = datetime.now()
    diff = now - dt

    seconds = int(diff.total_seconds())

    if seconds < 60:
        return "just now"
    elif seconds < 3600:
        return f"{seconds // 60} minutes ago"
    elif seconds < 86400:
        return f"{seconds // 3600} hours ago"
    elif seconds < 604800:
        return f"{seconds // 86400} days ago"
    else:
        return dt.strftime("%Y-%m-%d")

# ============================================================================
# Host resource functions
# ============================================================================
def get_host_cpu_usage():
    """Get host CPU usage with multiple fallback methods"""
    try:
        try:
            import psutil
            cpu_percent = psutil.cpu_percent(interval=0.5)
            if cpu_percent is not None:
                return float(cpu_percent)
        except ImportError:
            pass
        except Exception as e:
            logger.debug(f"psutil CPU method failed: {e}")
        
        if shutil.which("mpstat"):
            try:
                result = subprocess.run(['mpstat', '1', '1'], capture_output=True, text=True, timeout=3)
                if result.returncode == 0:
                    output = result.stdout
                    for line in output.split('\n'):
                        if 'all' in line.lower() and '%' in line:
                            parts = line.split()
                            for i, part in enumerate(parts):
                                if 'idle' in part.lower():
                                    try:
                                        idle = float(parts[i+1] if i+1 < len(parts) else parts[-1])
                                        return max(0.0, min(100.0, 100.0 - idle))
                                    except:
                                        pass
                            try:
                                idle = float(parts[-1])
                                return max(0.0, min(100.0, 100.0 - idle))
                            except:
                                pass
            except Exception as e:
                logger.debug(f"mpstat method failed: {e}")
        
        try:
            def get_cpu_times():
                with open('/proc/stat', 'r') as f:
                    line = f.readline()
                    values = [float(x) for x in line.split()[1:8]]
                    return values
            
            times1 = get_cpu_times()
            time.sleep(0.5)
            times2 = get_cpu_times()
            
            deltas = [times2[i] - times1[i] for i in range(len(times1))]
            total_delta = sum(deltas)
            
            if total_delta > 0:
                idle_delta = deltas[3]
                cpu_usage = 100.0 * (total_delta - idle_delta) / total_delta
                return max(0.0, min(100.0, cpu_usage))
        except Exception as e:
            logger.debug(f"/proc/stat method failed: {e}")
        
        try:
            result = subprocess.run(['top', '-bn1'], capture_output=True, text=True, timeout=3)
            if result.returncode == 0:
                for line in result.stdout.split('\n'):
                    if 'Cpu' in line or 'CPU' in line:
                        import re
                        idle_match = re.search(r'(\d+\.?\d*)\s*%?\s*id', line)
                        if idle_match:
                            idle = float(idle_match.group(1))
                            return max(0.0, min(100.0, 100.0 - idle))
        except Exception as e:
            logger.debug(f"top method failed: {e}")
        
        logger.warning("All CPU usage methods failed, returning 0.0")
        return 0.0
        
    except Exception as e:
        logger.error(f"Error getting CPU usage: {e}")
        return 0.0

def get_host_ram_usage():
    """Get host RAM usage with multiple fallback methods"""
    try:
        try:
            import psutil
            mem = psutil.virtual_memory()
            return {
                'total': mem.total // (1024**2),
                'used': mem.used // (1024**2),
                'free': mem.available // (1024**2),
                'percent': float(mem.percent)
            }
        except ImportError:
            pass
        except Exception as e:
            logger.debug(f"psutil RAM method failed: {e}")
        
        try:
            result = subprocess.run(['free', '-m'], capture_output=True, text=True, timeout=3)
            if result.returncode == 0:
                lines = result.stdout.splitlines()
                if len(lines) > 1:
                    mem = lines[1].split()
                    total = int(mem[1])
                    used = int(mem[2])
                    if total > 0:
                        return {
                            'total': total,
                            'used': used,
                            'free': total - used,
                            'percent': float((used / total * 100))
                        }
        except Exception as e:
            logger.debug(f"free command method failed: {e}")
        
        try:
            with open('/proc/meminfo', 'r') as f:
                meminfo = {}
                for line in f:
                    parts = line.split(':')
                    if len(parts) == 2:
                        key = parts[0].strip()
                        value = parts[1].strip().split()[0]
                        meminfo[key] = int(value)
                
                total = meminfo.get('MemTotal', 0) // 1024
                free = meminfo.get('MemFree', 0) // 1024
                buffers = meminfo.get('Buffers', 0) // 1024
                cached = meminfo.get('Cached', 0) // 1024
                
                if total > 0:
                    available = free + buffers + cached
                    used = total - available
                    return {
                        'total': total,
                        'used': used,
                        'free': available,
                        'percent': float((used / total * 100))
                    }
        except Exception as e:
            logger.debug(f"/proc/meminfo method failed: {e}")
        
        logger.warning("All RAM usage methods failed, returning zeros")
        return {'total': 0, 'used': 0, 'free': 0, 'percent': 0.0}
        
    except Exception as e:
        logger.error(f"Error getting RAM usage: {e}")
        return {'total': 0, 'used': 0, 'free': 0, 'percent': 0.0}

def get_host_disk_usage():
    """Get host disk usage with multiple fallback methods"""
    try:
        try:
            import psutil
            usage = psutil.disk_usage('/')
            return {
                'total': f"{usage.total / (1024**3):.1f}G",
                'used': f"{usage.used / (1024**3):.1f}G",
                'free': f"{usage.free / (1024**3):.1f}G",
                'percent': f"{usage.percent:.0f}%"
            }
        except ImportError:
            pass
        except Exception as e:
            logger.debug(f"psutil disk method failed: {e}")
        
        try:
            import shutil
            usage = shutil.disk_usage('/')
            total_gb = usage.total / (1024**3)
            used_gb = usage.used / (1024**3)
            free_gb = usage.free / (1024**3)
            percent = (usage.used / usage.total * 100) if usage.total > 0 else 0
            return {
                'total': f"{total_gb:.1f}G",
                'used': f"{used_gb:.1f}G",
                'free': f"{free_gb:.1f}G",
                'percent': f"{percent:.0f}%"
            }
        except Exception as e:
            logger.debug(f"shutil disk method failed: {e}")
        
        try:
            result = subprocess.run(['df', '-h', '/'], capture_output=True, text=True, timeout=3)
            if result.returncode == 0:
                lines = result.stdout.splitlines()
                if len(lines) > 1:
                    parts = lines[1].split()
                    if len(parts) >= 5:
                        return {
                            'total': parts[1],
                            'used': parts[2],
                            'free': parts[3],
                            'percent': parts[4]
                        }
        except Exception as e:
            logger.debug(f"df command method failed: {e}")
        
        try:
            import os
            stat = os.statvfs('/')
            total = stat.f_blocks * stat.f_frsize
            free = stat.f_bavail * stat.f_frsize
            used = total - free
            
            total_gb = total / (1024**3)
            used_gb = used / (1024**3)
            free_gb = free / (1024**3)
            percent = (used / total * 100) if total > 0 else 0
            
            return {
                'total': f"{total_gb:.1f}G",
                'used': f"{used_gb:.1f}G",
                'free': f"{free_gb:.1f}G",
                'percent': f"{percent:.0f}%"
            }
        except Exception as e:
            logger.debug(f"statvfs method failed: {e}")
        
        logger.warning("All disk usage methods failed, returning Unknown")
        return {'total': 'Unknown', 'used': 'Unknown', 'free': 'Unknown', 'percent': 'Unknown'}
        
    except Exception as e:
        logger.error(f"Error getting disk usage: {e}")
        return {'total': 'Unknown', 'used': 'Unknown', 'free': 'Unknown', 'percent': 'Unknown'}

def get_host_uptime():
    try:
        import platform
        system = platform.system().lower()
        
        if system == 'linux':
            with open('/proc/uptime', 'r') as f:
                uptime_seconds = float(f.readline().split()[0])
        elif system == 'windows':
            try:
                import subprocess
                result = subprocess.run(['wmic', 'os', 'get', 'LastBootUpTime', '/value'], 
                                      capture_output=True, text=True, timeout=10)
                if result.returncode == 0:
                    for line in result.stdout.split('\n'):
                        if 'LastBootUpTime=' in line:
                            boot_time_str = line.split('=')[1].strip()
                            if boot_time_str:
                                boot_time_str = boot_time_str.split('.')[0]
                                from datetime import datetime
                                boot_time = datetime.strptime(boot_time_str, '%Y%m%d%H%M%S')
                                uptime_seconds = (datetime.now() - boot_time).total_seconds()
                                break
                    else:
                        raise Exception("Could not parse boot time")
                else:
                    raise Exception("WMIC command failed")
            except Exception as wmic_error:
                try:
                    import psutil
                    uptime_seconds = time.time() - psutil.boot_time()
                except ImportError:
                    import os
                    try:
                        import psutil
                        current_process = psutil.Process(os.getpid())
                        uptime_seconds = time.time() - current_process.create_time()
                    except:
                        return "Unknown"
        else:
            try:
                import subprocess
                result = subprocess.run(['uptime'], capture_output=True, text=True, timeout=5)
                if result.returncode == 0:
                    uptime_output = result.stdout.strip()
                    if 'up' in uptime_output:
                        return uptime_output.split('up')[1].split(',')[0].strip()
                    else:
                        return "Unknown"
                else:
                    return "Unknown"
            except:
                return "Unknown"
        
        days = int(uptime_seconds // 86400)
        hours = int((uptime_seconds % 86400) // 3600)
        minutes = int((uptime_seconds % 3600) // 60)
        
        if days > 0:
            return f"{days}d {hours}h {minutes}m"
        elif hours > 0:
            return f"{hours}h {minutes}m"
        else:
            return f"{minutes}m"
            
    except Exception as e:
        logger.debug(f"Error getting uptime: {e}")
        return "Unknown"

async def get_host_stats(node_id: int) -> Dict:
    node = get_node(node_id)
    if not node:
        logger.warning(f"Node {node_id} not found")
        return {
            "cpu": 0.0, 
            "ram": {'total': 0, 'used': 0, 'free': 0, 'percent': 0.0}, 
            "disk": {'total': 'Unknown', 'used': 'Unknown', 'free': 'Unknown', 'percent': 'Unknown'}, 
            "uptime": "Unknown"
        }
    
    if node['is_local']:
        try:
            stats = {
                "cpu": get_host_cpu_usage(),
                "ram": get_host_ram_usage(),
                "disk": get_host_disk_usage(),
                "uptime": get_host_uptime()
            }
            logger.debug(f"Local node {node_id} stats: CPU={stats['cpu']:.1f}%, RAM={stats['ram']['percent']:.1f}%, Uptime={stats['uptime']}")
            return stats
        except Exception as e:
            logger.error(f"Error getting local node stats: {e}", exc_info=True)
            return {
                "cpu": 0.0, 
                "ram": {'total': 0, 'used': 0, 'free': 0, 'percent': 0.0}, 
                "disk": {'total': 'Error', 'used': 'Error', 'free': 'Error', 'percent': '0%'}, 
                "uptime": "Error"
            }
    else:
        try:
            import requests
            url = f"{node['url']}/api/host/stats"
            headers = {"X-API-Key": node["api_key"]}
            verify_ssl = bool(node.get('verify_ssl', 1))
            
            logger.debug(f"Fetching stats from remote node {node['name']}: {url} (verify_ssl={verify_ssl})")
            response = requests.get(url, headers=headers, timeout=10, verify=verify_ssl)
            response.raise_for_status()
            stats = response.json()
            
            logger.debug(f"Remote node {node['name']} stats received: {stats}")
            
            try:
                with get_db() as conn:
                    cur = conn.cursor()
                    cur.execute('''UPDATE nodes SET 
                                  status = ?, 
                                  last_seen = ?, 
                                  cpu_cores = ?, 
                                  ram_total = ?, 
                                  disk_total = ? 
                                  WHERE id = ?''',
                               ('online', 
                                datetime.now().isoformat(), 
                                stats.get('cpu_cores', 0), 
                                stats.get('ram', {}).get('total', 0),
                                stats.get('disk', {}).get('total_gb', 0), 
                                node_id))
                    conn.commit()
            except Exception as db_err:
                logger.error(f"Failed to update node {node_id} in database: {db_err}")
            
            return stats
            
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout connecting to node {node['name']}")
            mark_node_offline(node_id)
            return {
                "cpu": 0.0, 
                "ram": {'total': 0, 'used': 0, 'free': 0, 'percent': 0.0}, 
                "disk": {'total': 'Unknown', 'used': 'Unknown', 'free': 'Unknown', 'percent': 'Unknown'}, 
                "uptime": "Unknown"
            }
        except requests.exceptions.ConnectionError:
            logger.warning(f"Connection error to node {node['name']}")
            mark_node_offline(node_id)
            return {
                "cpu": 0.0, 
                "ram": {'total': 0, 'used': 0, 'free': 0, 'percent': 0.0}, 
                "disk": {'total': 'Unknown', 'used': 'Unknown', 'free': 'Unknown', 'percent': 'Unknown'}, 
                "uptime": "Unknown"
            }
        except Exception as e:
            logger.error(f"Failed to get host stats from node {node['name']}: {e}", exc_info=True)
            mark_node_offline(node_id)
            return {
                "cpu": 0.0, 
                "ram": {'total': 0, 'used': 0, 'free': 0, 'percent': 0.0}, 
                "disk": {'total': 'Unknown', 'used': 'Unknown', 'free': 'Unknown', 'percent': 'Unknown'}, 
                "uptime": "Unknown"
            }

def mark_node_offline(node_id: int):
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('UPDATE nodes SET status = ?, last_seen = ? WHERE id = ?',
                       ('offline', datetime.now().isoformat(), node_id))
            conn.commit()
    except Exception as e:
        logger.error(f"Failed to mark node {node_id} offline: {e}")

async def get_node_status(node_id: int) -> Dict:
    node = get_node(node_id)
    if not node:
        logger.warning(f"Node {node_id} not found for status check")
        return {"status": "❓ Unknown", "online": False}
    
    if node['is_local']:
        try:
            stats = await get_host_stats(node_id)
            return {
                "status": "🟢 Online (Local)",
                "online": True,
                "local": True,
                "last_seen": datetime.now().isoformat(),
                "stats": stats
            }
        except Exception as e:
            logger.error(f"Error getting local node status: {e}")
            return {
                "status": "⚠️ Error",
                "online": False,
                "local": True,
                "last_seen": datetime.now().isoformat()
            }
    
    try:
        import requests
        headers = {"X-API-Key": node['api_key']}
        verify_ssl = bool(node.get('verify_ssl', 1))
        
        logger.debug(f"Pinging remote node {node['name']}: {node['url']}/api/ping (verify_ssl={verify_ssl})")
        response = requests.get(f"{node['url']}/api/ping", headers=headers, timeout=5, verify=verify_ssl)
        
        if response.status_code == 200:
            data = response.json()
            now = datetime.now().isoformat()
            
            try:
                with get_db() as conn:
                    cur = conn.cursor()
                    cur.execute('UPDATE nodes SET status = ?, last_seen = ? WHERE id = ?',
                               ('online', now, node_id))
                    conn.commit()
            except Exception as db_err:
                logger.error(f"Failed to update node {node_id} status in database: {db_err}")
            
            stats = await get_host_stats(node_id)
            return {
                "status": "🟢 Online",
                "online": True,
                "local": False,
                "last_seen": now,
                "stats": stats,
                "ping_time": data.get('time')
            }
        else:
            logger.warning(f"Node {node['name']} returned status {response.status_code}")
            mark_node_offline(node_id)
            return {
                "status": "🔴 Offline",
                "online": False,
                "local": False,
                "last_seen": node.get('last_seen')
            }
            
    except requests.exceptions.Timeout:
        logger.warning(f"Timeout pinging node {node['name']}")
        mark_node_offline(node_id)
        return {
            "status": "⏱️ Timeout",
            "online": False,
            "local": False,
            "last_seen": node.get('last_seen')
        }
    except requests.exceptions.ConnectionError:
        logger.warning(f"Connection error to node {node['name']}")
        mark_node_offline(node_id)
        return {
            "status": "🔴 Offline",
            "online": False,
            "local": False,
            "last_seen": node.get('last_seen')
        }
    except Exception as e:
        logger.error(f"Failed to ping node {node['name']}: {e}", exc_info=True)
        mark_node_offline(node_id)
        return {
            "status": "❌ Error",
            "online": False,
            "local": False,
            "last_seen": node.get('last_seen'),
            "error": str(e)
        }

# ============================================================================
# Container stats functions
# ============================================================================
async def get_container_status(container_name: str, node_id: Optional[int] = None) -> str:
    if node_id is None:
        node_id = find_node_id_for_container(container_name)
    
    try:
        result = await execute_lxc(container_name, f"info {container_name}", node_id=node_id, timeout=15, operation_type="stats")
        logger.debug(f"LXC info result for {container_name}: {result}")
        
        for line in result.split('\n'):
            if line.startswith("Status: "):
                status = line.split(": ", 1)[1].strip().lower()
                logger.debug(f"Container {container_name} status: {status}")
                return status
        logger.warning(f"Status line not found for container {container_name}. Full output: {result}")
        return "unknown"
    except Exception as e:
        logger.error(f"Error getting status for {container_name}: {e}")
        return "unknown"

def get_node_health_status(node_id):
    node = get_node(node_id)
    if not node:
        return {'status': 'not_found', 'message': 'Node not found'}
    
    health_status = {
        'node_id': node_id,
        'node_name': node['name'],
        'is_local': node['is_local'],
        'circuit_breaker_open': is_node_circuit_open(node_id),
        'failure_count': 0,
        'http_500_failures': 0,
        'last_failure': None,
        'last_500_failure': None,
        'status': 'healthy'
    }
    
    if node_id in node_circuit_breakers:
        breaker = node_circuit_breakers[node_id]
        health_status['failure_count'] = breaker['failures']
        health_status['http_500_failures'] = breaker.get('http_500_failures', 0)
        health_status['last_failure'] = breaker['last_failure']
        health_status['last_500_failure'] = breaker.get('last_500_failure', None)
        
        if breaker.get('http_500_failures', 0) >= HTTP_500_THRESHOLD:
            health_status['status'] = 'http_500_circuit_open'
            time_since_failure = time.time() - breaker.get('last_500_failure', 0)
            time_remaining = CIRCUIT_BREAKER_TIMEOUT - time_since_failure
            health_status['retry_in_seconds'] = max(0, int(time_remaining))
            health_status['message'] = f'HTTP 500 circuit breaker open ({breaker.get("http_500_failures", 0)} server errors)'
        elif breaker['failures'] >= CIRCUIT_BREAKER_THRESHOLD:
            health_status['status'] = 'circuit_open'
            time_since_failure = time.time() - breaker['last_failure']
            time_remaining = CIRCUIT_BREAKER_TIMEOUT - time_since_failure
            health_status['retry_in_seconds'] = max(0, int(time_remaining))
            health_status['message'] = f'Circuit breaker open ({breaker["failures"]} failures)'
        elif breaker['failures'] >= 2 or breaker.get('http_500_failures', 0) > 0:
            health_status['status'] = 'degraded'
            health_status['message'] = f'Node experiencing issues ({breaker["failures"]} failures, {breaker.get("http_500_failures", 0)} server errors)'
        elif breaker['failures'] == 1:
            current_time = time.time()
            time_since_failure = current_time - breaker.get('last_failure', 0)
            if time_since_failure < 60:
                health_status['status'] = 'degraded'
                health_status['message'] = f'Node experiencing recent issues (1 recent failure)'
    
    return health_status

def log_node_health_summary():
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('SELECT id, name FROM nodes')
            nodes = cur.fetchall()
        
        unhealthy_nodes = []
        for node in nodes:
            node_id, node_name = node
            health = get_node_health_status(node_id)
            if health['status'] != 'healthy':
                unhealthy_nodes.append(f"{node_name} (ID: {node_id}): {health['status']}")
        
        if unhealthy_nodes:
            logger.info(f"Unhealthy nodes detected: {', '.join(unhealthy_nodes)}")
        else:
            logger.debug("All nodes are healthy")
            
    except Exception as e:
        logger.error(f"Error checking node health: {e}")

node_circuit_breakers = {}
CIRCUIT_BREAKER_THRESHOLD = 8
CIRCUIT_BREAKER_TIMEOUT = 180
HTTP_500_THRESHOLD = 4

vps_stats_cache = {}
VPS_STATS_CACHE_TIMEOUT = 180

stats_request_timestamps = {}
STATS_REQUEST_COOLDOWN = 5

def is_node_circuit_open(node_id):
    if node_id not in node_circuit_breakers:
        return False
    
    breaker = node_circuit_breakers[node_id]
    current_time = time.time()
    
    if breaker.get('http_500_failures', 0) >= HTTP_500_THRESHOLD:
        if current_time - breaker.get('last_500_failure', 0) > CIRCUIT_BREAKER_TIMEOUT:
            breaker['http_500_failures'] = 0
            logger.info(f"HTTP 500 circuit breaker reset for node {node_id}")
        else:
            return True
    
    if breaker['failures'] >= CIRCUIT_BREAKER_THRESHOLD:
        if current_time - breaker['last_failure'] > CIRCUIT_BREAKER_TIMEOUT:
            breaker['failures'] = 0
            breaker['http_500_failures'] = 0
            logger.info(f"Circuit breaker reset for node {node_id}")
            return False
        return True
    return False

def record_node_failure(node_id, is_http_500=False):
    if node_id not in node_circuit_breakers:
        node_circuit_breakers[node_id] = {
            'failures': 0, 
            'last_failure': 0,
            'http_500_failures': 0,
            'last_500_failure': 0
        }
    
    current_time = time.time()
    node_circuit_breakers[node_id]['failures'] += 1
    node_circuit_breakers[node_id]['last_failure'] = current_time
    
    if is_http_500:
        node_circuit_breakers[node_id]['http_500_failures'] += 1
        node_circuit_breakers[node_id]['last_500_failure'] = current_time
        
        if node_circuit_breakers[node_id]['http_500_failures'] >= HTTP_500_THRESHOLD:
            logger.warning(f"HTTP 500 circuit breaker opened for node {node_id} after {HTTP_500_THRESHOLD} server errors")
    
    if node_circuit_breakers[node_id]['failures'] >= CIRCUIT_BREAKER_THRESHOLD:
        logger.warning(f"Circuit breaker opened for node {node_id} after {CIRCUIT_BREAKER_THRESHOLD} failures")

def record_node_success(node_id):
    if node_id in node_circuit_breakers:
        node_circuit_breakers[node_id]['failures'] = 0
        node_circuit_breakers[node_id]['http_500_failures'] = 0
        logger.debug(f"Node {node_id} success recorded - failure counts reset")

def cleanup_old_node_failures():
    current_time = time.time()
    cleanup_threshold = 300
    
    for node_id, breaker in list(node_circuit_breakers.items()):
        if (breaker.get('last_failure', 0) > 0 and 
            current_time - breaker['last_failure'] > cleanup_threshold and
            breaker['failures'] < CIRCUIT_BREAKER_THRESHOLD):
            
            logger.debug(f"Cleaning up old failures for node {node_id} (last failure was {int((current_time - breaker['last_failure'])/60)} minutes ago)")
            breaker['failures'] = 0
            
        if (breaker.get('last_500_failure', 0) > 0 and 
            current_time - breaker.get('last_500_failure', 0) > cleanup_threshold and
            breaker.get('http_500_failures', 0) < HTTP_500_THRESHOLD):
            
            logger.debug(f"Cleaning up old HTTP 500 failures for node {node_id}")
            breaker['http_500_failures'] = 0

def reset_node_circuit_breaker(node_id):
    if node_id in node_circuit_breakers:
        logger.info(f"Manually resetting circuit breaker for node {node_id}")
        node_circuit_breakers[node_id]['failures'] = 0
        node_circuit_breakers[node_id]['http_500_failures'] = 0
        node_circuit_breakers[node_id]['last_failure'] = 0
        node_circuit_breakers[node_id]['last_500_failure'] = 0
        return True
    return False

def get_healthy_nodes():
    nodes = get_nodes()
    healthy_nodes = []
    
    for node in nodes:
        if node['is_local'] or not is_node_circuit_open(node['id']):
            health_status = get_node_health_status(node['id'])
            node['health_status'] = health_status['status']
            node['health_message'] = health_status.get('message', 'Healthy')
            healthy_nodes.append(node)
    
    return healthy_nodes

def get_node_availability_info(node_id):
    node = get_node(node_id)
    if not node:
        return None
    
    health_status = get_node_health_status(node_id)
    
    return {
        'node': node,
        'is_available': not is_node_circuit_open(node_id),
        'health_status': health_status['status'],
        'message': health_status.get('message', 'Healthy'),
        'retry_in_seconds': health_status.get('retry_in_seconds', 0),
        'failure_count': health_status.get('failure_count', 0),
        'http_500_failures': health_status.get('http_500_failures', 0)
    }

def should_skip_stats_request(container_name: str) -> bool:
    current_time = time.time()
    last_request = stats_request_timestamps.get(container_name, 0)
    
    if current_time - last_request < 3:
        logger.debug(f"Skipping stats request for {container_name} due to rate limiting")
        return True
    
    stats_request_timestamps[container_name] = current_time
    return False

def get_cached_vps_stats(container_name: str) -> Optional[Dict]:
    if container_name not in vps_stats_cache:
        return None
    
    cache_entry = vps_stats_cache[container_name]
    current_time = time.time()
    
    if current_time - cache_entry['timestamp'] > VPS_STATS_CACHE_TIMEOUT:
        del vps_stats_cache[container_name]
        return None
    
    return cache_entry['stats']

def cache_vps_stats(container_name: str, stats: Dict):
    if stats.get('status') not in ['timeout', 'error', 'unknown', 'server_error', 'circuit_open', 'connection_error']:
        vps_stats_cache[container_name] = {
            'stats': stats.copy(),
            'timestamp': time.time()
        }

def cleanup_expired_cache():
    current_time = time.time()
    expired_keys = []
    
    for container_name, cache_entry in vps_stats_cache.items():
        if current_time - cache_entry['timestamp'] > VPS_STATS_CACHE_TIMEOUT:
            expired_keys.append(container_name)
    
    for key in expired_keys:
        del vps_stats_cache[key]
    
    rate_limit_expired = []
    for container_name, timestamp in stats_request_timestamps.items():
        if current_time - timestamp > 3600:
            rate_limit_expired.append(container_name)
    
    for key in rate_limit_expired:
        del stats_request_timestamps[key]
    
    if expired_keys or rate_limit_expired:
        logger.debug(f"Cleaned up {len(expired_keys)} expired cache entries and {len(rate_limit_expired)} old rate limit entries")

def get_all_circuit_breaker_status():
    status = {}
    for node_id, breaker in node_circuit_breakers.items():
        node = get_node(node_id)
        node_name = node['name'] if node else f"Node {node_id}"
        
        is_open = is_node_circuit_open(node_id)
        status[node_id] = {
            'node_name': node_name,
            'is_open': is_open,
            'failures': breaker['failures'],
            'http_500_failures': breaker.get('http_500_failures', 0),
            'last_failure': breaker.get('last_failure', 0),
            'last_500_failure': breaker.get('last_500_failure', 0)
        }
    return status

async def get_container_stats(container_name: str, node_id: Optional[int] = None) -> Dict:
    if node_id is None:
        node_id = find_node_id_for_container(container_name)
    
    node = get_node(node_id)
    if node and not node['is_local'] and should_skip_stats_request(container_name):
        cached_stats = get_cached_vps_stats(container_name)
        if cached_stats:
            logger.debug(f"Returning cached stats for {container_name} due to rate limiting")
            return cached_stats
    
    if node_id and is_node_circuit_open(node_id):
        logger.info(f"Circuit breaker open for node {node_id}, checking for cached stats")
        
        cached_stats = get_cached_vps_stats(container_name)
        if cached_stats:
            logger.info(f"Returning cached stats for {container_name} due to circuit breaker")
            cached_stats = cached_stats.copy()
            cached_stats['status'] = 'circuit_open_cached'
            cached_stats['uptime'] = 'Circuit Breaker Open (Cached Data)'
            return cached_stats
        
        return {
            "status": "circuit_open", 
            "cpu": 0.0, 
            "ram": {"used": 0, "total": 0, "pct": 0.0}, 
            "disk": {"use_percent": "0%"}, 
            "uptime": "Circuit Breaker Open",
            "processes": 0,
            "network": {},
            "load_average": {"1min": 0.0, "5min": 0.0, "15min": 0.0}
        }
    
    node = get_node(node_id)
    if not node:
        logger.warning(f"Node {node_id} not found for container {container_name}")
        return {
            "status": "unknown", 
            "cpu": 0.0, 
            "ram": {"used": 0, "total": 0, "pct": 0.0}, 
            "disk": {"use_percent": "0%"}, 
            "uptime": "Unknown",
            "processes": 0,
            "network": {},
            "load_average": {"1min": 0.0, "5min": 0.0, "15min": 0.0}
        }
    
    if node['is_local']:
        try:
            status = await get_container_status(container_name, node_id)
            logger.debug(f"Container {container_name} status from get_container_status: '{status}'")
            
            if status == "running":
                logger.debug(f"Container {container_name} is running, fetching detailed stats")
                cpu = await get_container_cpu_pct_local(container_name, node_id)
                ram = await get_container_ram_local(container_name, node_id)
                disk = await get_container_disk_local(container_name, node_id)
                uptime = await get_container_uptime_local(container_name, node_id)
                processes = await get_container_processes_local(container_name, node_id)
                network = await get_container_network_local(container_name, node_id)
                
                try:
                    private_ip = await get_container_private_ip(container_name, node_id)
                except Exception as e:
                    logger.warning(f"Error getting private IP for {container_name}: {e}")
                    private_ip = "N/A"
            else:
                logger.debug(f"Container {container_name} is not running (status: {status}), using default stats")
                cpu = 0.0
                ram = {"used": 0, "total": 0, "pct": 0.0}
                disk = {"use_percent": "0%"}
                uptime = "Stopped"
                processes = 0
                network = {}
                private_ip = "N/A"
            
            result = {
                "status": status,
                "cpu": cpu,
                "ram": ram,
                "disk": disk,
                "uptime": uptime,
                "processes": processes,
                "network": network,
                "load_average": {"1min": 0.0, "5min": 0.0, "15min": 0.0}
            }
            
            if private_ip != "N/A":
                result['private_ip'] = private_ip
            cache_vps_stats(container_name, result)
            
            logger.debug(f"Returning stats for {container_name}: status={result['status']}, cpu={result['cpu']}")
            return result
        except Exception as e:
            logger.error(f"Error getting local container stats for {container_name}: {e}", exc_info=True)
            return {
                "status": "unknown", 
                "cpu": 0.0, 
                "ram": {"used": 0, "total": 0, "pct": 0.0}, 
                "disk": {"use_percent": "0%"}, 
                "uptime": "Unknown",
                "processes": 0,
                "network": {},
                "load_average": {"1min": 0.0, "5min": 0.0, "15min": 0.0}
            }
    else:
        try:
            import requests
            url = f"{node['url']}/api/container/stats"
            data = {"container": container_name}
            headers = {"X-API-Key": node["api_key"]}
            verify_ssl = bool(node.get('verify_ssl', 1))
            
            logger.debug(f"Fetching container stats from {node['name']}: {url} (verify_ssl={verify_ssl})")
            
            response = requests.post(url, json=data, headers=headers, timeout=8, verify=verify_ssl)
            response.raise_for_status()
            stats = response.json()
            
            record_node_success(node_id)
            
            ram_data = stats.get("ram", {"used": 0, "total": 0, "percent": 0.0})
            if isinstance(ram_data, dict):
                if "percent" in ram_data and "pct" not in ram_data:
                    ram_data["pct"] = ram_data["percent"]
                if "pct" not in ram_data:
                    ram_data["pct"] = 0.0
            else:
                ram_data = {"used": 0, "total": 0, "pct": 0.0}
            
            disk_data = stats.get("disk", "Unknown")
            if isinstance(disk_data, str):
                if disk_data in ["Unknown", "Stopped", "N/A"]:
                    disk_data = {
                        "size": "Unknown",
                        "used": "Unknown",
                        "available": "Unknown",
                        "use_percent": "0%"
                    }
                else:
                    import re
                    match = re.match(r'(.+?)/(.+?)\s*\((.+?)\)', disk_data)
                    if match:
                        used, total, percent = match.groups()
                        disk_data = {
                            "size": total.strip(),
                            "used": used.strip(),
                            "available": "Unknown",
                            "use_percent": percent.strip()
                        }
                    else:
                        disk_data = {
                            "size": "Unknown",
                            "used": "Unknown",
                            "available": "Unknown",
                            "use_percent": "0%"
                        }
            elif not isinstance(disk_data, dict):
                disk_data = {"use_percent": "0%"}
            
            if "use_percent" not in disk_data:
                disk_data["use_percent"] = "0%"
            
            result = {
                "status": stats.get("status", "unknown"),
                "cpu": float(stats.get("cpu", 0.0)),
                "ram": ram_data,
                "disk": disk_data,
                "uptime": stats.get("uptime", "Unknown"),
                "processes": stats.get("processes", 0),
                "network": stats.get("network", {}),
                "load_average": stats.get("load_average", {"1min": 0.0, "5min": 0.0, "15min": 0.0})
            }
            
            cache_vps_stats(container_name, result)
            
            return result
            
        except requests.exceptions.Timeout:
            last_timeout_log = getattr(record_node_failure, f'_last_timeout_log_{node_id}', 0)
            current_time = time.time()
            
            if current_time - last_timeout_log > 60:
                logger.warning(f"Timeout getting container stats from node {node['name']}")
                setattr(record_node_failure, f'_last_timeout_log_{node_id}', current_time)
            
            cached_stats = get_cached_vps_stats(container_name)
            if cached_stats:
                logger.debug(f"Returning cached stats for {container_name} due to timeout")
                cached_stats = cached_stats.copy()
                cached_stats['status'] = 'timeout_cached'
                cached_stats['uptime'] = 'Connection Timeout (Cached Data)'
                return cached_stats
            
            record_node_failure(node_id)
            
            return {
                "status": "timeout", 
                "cpu": 0.0, 
                "ram": {"used": 0, "total": 0, "pct": 0.0}, 
                "disk": {"use_percent": "0%"}, 
                "uptime": "Connection Timeout",
                "processes": 0,
                "network": {},
                "load_average": {"1min": 0.0, "5min": 0.0, "15min": 0.0}
            }
        except requests.exceptions.ConnectionError:
            last_connection_log = getattr(record_node_failure, f'_last_connection_log_{node_id}', 0)
            current_time = time.time()
            
            if current_time - last_connection_log > 60:
                logger.warning(f"Connection error getting container stats from node {node['name']}")
                setattr(record_node_failure, f'_last_connection_log_{node_id}', current_time)
            
            cached_stats = get_cached_vps_stats(container_name)
            if cached_stats:
                logger.debug(f"Returning cached stats for {container_name} due to connection error")
                cached_stats = cached_stats.copy()
                cached_stats['status'] = 'connection_error_cached'
                cached_stats['uptime'] = 'Connection Error (Cached Data)'
                return cached_stats
            
            record_node_failure(node_id)
            
            return {
                "status": "connection_error", 
                "cpu": 0.0, 
                "ram": {"used": 0, "total": 0, "pct": 0.0}, 
                "disk": {"use_percent": "0%"}, 
                "uptime": "Connection Error",
                "processes": 0,
                "network": {},
                "load_average": {"1min": 0.0, "5min": 0.0, "15min": 0.0}
            }
        except requests.exceptions.HTTPError as e:
            if hasattr(e, 'response') and hasattr(e.response, 'status_code'):
                status_code = e.response.status_code
                if status_code >= 500:
                    logger.error(f"HTTP {status_code} error getting container stats from node {node['name']}")
                    record_node_failure(node_id, is_http_500=True)
                    return {
                        "status": "server_error", 
                        "cpu": 0.0, 
                        "ram": {"used": 0, "total": 0, "pct": 0.0}, 
                        "disk": {"use_percent": "0%"}, 
                        "uptime": "Unknown",
                        "processes": 0,
                        "network": {},
                        "load_average": {"1min": 0.0, "5min": 0.0, "15min": 0.0}
                    }
                else:
                    logger.warning(f"HTTP {status_code} error getting container stats from node {node['name']}")
                    record_node_failure(node_id)
                    return {
                        "status": "unknown", 
                        "cpu": 0.0, 
                        "ram": {"used": 0, "total": 0, "pct": 0.0}, 
                        "disk": {"use_percent": "0%"}, 
                        "uptime": "Unknown",
                        "processes": 0,
                        "network": {},
                        "load_average": {"1min": 0.0, "5min": 0.0, "15min": 0.0}
                    }
            else:
                logger.warning(f"HTTP error getting container stats from node {node['name']}: {e}")
                record_node_failure(node_id)
                return {
                    "status": "unknown", 
                    "cpu": 0.0, 
                    "ram": {"used": 0, "total": 0, "pct": 0.0}, 
                    "disk": {"use_percent": "0%"}, 
                    "uptime": "Unknown",
                    "processes": 0,
                    "network": {},
                    "load_average": {"1min": 0.0, "5min": 0.0, "15min": 0.0}
                }
        except Exception as e:
            logger.error(f"Failed to get container stats from node {node['name']}: {e}", exc_info=True)
            record_node_failure(node_id)
            return {
                "status": "unknown", 
                "cpu": 0.0, 
                "ram": {"used": 0, "total": 0, "pct": 0.0}, 
                "disk": {"use_percent": "0%"}, 
                "uptime": "Unknown",
                "processes": 0,
                "network": {},
                "load_average": {"1min": 0.0, "5min": 0.0, "15min": 0.0}
            }

async def get_container_cpu_pct_local(container_name: str, node_id: int) -> float:
    try:
        simple_script = r"""sh -c '
cat /proc/stat | grep "^cpu " > /tmp/cpu1
sleep 1
cat /proc/stat | grep "^cpu " > /tmp/cpu2
awk "{
    getline < \"/tmp/cpu1\"
    u1=\$2; n1=\$3; s1=\$4; i1=\$5
    getline < \"/tmp/cpu2\"
    u2=\$2; n2=\$3; s2=\$4; i2=\$5
    total=(u2-u1)+(n2-n1)+(s2-s1)+(i2-i1)
    used=(u2-u1)+(n2-n1)+(s2-s1)
    if(total>0) print (used*100)/total; else print 0
}" /tmp/cpu2
rm -f /tmp/cpu1 /tmp/cpu2
'"""
        try:
            result = await execute_lxc(container_name, f"exec {container_name} -- {simple_script}", node_id=node_id)
            cpu_pct = float(result.strip())
            if 0 <= cpu_pct <= 100:
                logger.debug(f"CPU for {container_name}: {cpu_pct}%")
                return round(cpu_pct, 2)
        except Exception as e:
            logger.debug(f"Simple sh script failed for {container_name}: {e}")
        
        try:
            result = await execute_lxc(container_name, f"exec {container_name} -- sh -c 'top -bn1 | grep \"Cpu(s)\"'", node_id=node_id)
            if result:
                import re
                idle_match = re.search(r'(\d+\.?\d*)\s*id', result)
                if idle_match:
                    idle = float(idle_match.group(1))
                    cpu_pct = 100.0 - idle
                    logger.debug(f"CPU for {container_name} (top): {cpu_pct}%")
                    return round(cpu_pct, 2)
        except Exception as e:
            logger.debug(f"Top method failed for {container_name}: {e}")
        
        try:
            result = await execute_lxc(container_name, f"exec {container_name} -- sh -c 'vmstat 1 2 | tail -1'", node_id=node_id)
            if result:
                parts = result.split()
                if len(parts) >= 15:
                    idle = float(parts[14])
                    cpu_pct = 100.0 - idle
                    logger.debug(f"CPU for {container_name} (vmstat): {cpu_pct}%")
                    return round(cpu_pct, 2)
        except Exception as e:
            logger.debug(f"vmstat method failed for {container_name}: {e}")
        
        try:
            result = await execute_lxc(container_name, 
                f"exec {container_name} -- sh -c 'grep \"^cpu \" /proc/stat && sleep 1 && grep \"^cpu \" /proc/stat'", 
                node_id=node_id)
            
            lines = [line for line in result.split('\n') if line.startswith('cpu ')]
            if len(lines) >= 2:
                fields1 = [int(x) for x in lines[0].split()[1:8]]
                total1 = sum(fields1)
                idle1 = fields1[3]
                
                fields2 = [int(x) for x in lines[1].split()[1:8]]
                total2 = sum(fields2)
                idle2 = fields2[3]
                
                total_delta = total2 - total1
                idle_delta = idle2 - idle1
                
                if total_delta > 0:
                    cpu_pct = 100.0 * (total_delta - idle_delta) / total_delta
                    logger.debug(f"CPU for {container_name} (/proc/stat): {cpu_pct}%")
                    return round(cpu_pct, 2)
        except Exception as e:
            logger.debug(f"/proc/stat method failed for {container_name}: {e}")
        
        logger.warning(f"All CPU detection methods failed for {container_name}, returning 0")
        return 0.0
        
    except Exception as e:
        logger.error(f"Error getting CPU for {container_name}: {e}")
        return 0.0

async def get_container_ram_local(container_name: str, node_id: int) -> Dict:
    try:
        result = await execute_lxc(container_name, f"exec {container_name} -- free -m", node_id=node_id)
        lines = result.split('\n')
        if len(lines) > 1:
            parts = lines[1].split()
            total = int(parts[1])
            used = int(parts[2])
            free = int(parts[3])
            pct = (used / total * 100) if total > 0 else 0.0
            return {'used': used, 'total': total, 'free': free, 'pct': pct}
        return {'used': 0, 'total': 0, 'free': 0, 'pct': 0.0}
    except Exception as e:
        logger.error(f"Error getting RAM for {container_name}: {e}")
        return {'used': 0, 'total': 0, 'free': 0, 'pct': 0.0}

async def get_container_disk_local(container_name: str, node_id: int) -> Dict:
    try:
        result = await execute_lxc(
            container_name,
            f"exec {container_name} -- df -h /",
            node_id=node_id
        )

        lines = result.strip().split("\n")
        
        if len(lines) >= 2:
            for line in lines[1:]:
                parts = line.split()
                
                if len(parts) >= 6:
                    filesystem, size, used, avail, usep, mount = parts[:6]
                    
                    return {
                        "filesystem": filesystem,
                        "size": size,
                        "used": used,
                        "available": avail,
                        "use_percent": usep,
                        "mounted": mount,
                    }
                elif len(parts) >= 5:
                    size, used, avail, usep = parts[0], parts[1], parts[2], parts[3]
                    
                    return {
                        "filesystem": "rootfs",
                        "size": size,
                        "used": used,
                        "available": avail,
                        "use_percent": usep,
                        "mounted": "/",
                    }

        try:
            result = await execute_lxc(
                container_name,
                f"exec {container_name} -- df -hP /",
                node_id=node_id
            )
            
            lines = result.strip().split("\n")
            if len(lines) >= 2:
                parts = lines[1].split()
                if len(parts) >= 5:
                    return {
                        "filesystem": parts[0],
                        "size": parts[1],
                        "used": parts[2],
                        "available": parts[3],
                        "use_percent": parts[4],
                        "mounted": parts[5] if len(parts) > 5 else "/",
                    }
        except Exception as e:
            logger.debug(f"df -hP fallback failed for {container_name}: {e}")

        return {
            "size": "Unknown",
            "used": "Unknown",
            "available": "Unknown",
            "use_percent": "0%"
        }

    except Exception as e:
        logger.error(f"Error getting disk for {container_name}: {e}")
        return {
            "size": "Unknown",
            "used": "Unknown",
            "available": "Unknown",
            "use_percent": "0%"
        }

async def get_container_uptime_local(container_name: str, node_id: int) -> str:
    try:
        result = await execute_lxc(container_name, f"exec {container_name} -- uptime -p", node_id=node_id, timeout=3)
        if result and result.strip():
            return result.strip()
    except Exception as e:
        logger.debug(f"uptime -p failed for {container_name}: {e}")
    
    try:
        result = await execute_lxc(container_name, f"exec {container_name} -- uptime", node_id=node_id, timeout=3)
        if result and result.strip():
            return result.strip()
    except Exception as e:
        logger.debug(f"uptime failed for {container_name}: {e}")
    
    try:
        result = await execute_lxc(container_name, f"exec {container_name} -- cat /proc/uptime", node_id=node_id, timeout=3)
        if result:
            uptime_seconds = float(result.split()[0])
            days = int(uptime_seconds // 86400)
            hours = int((uptime_seconds % 86400) // 3600)
            minutes = int((uptime_seconds % 3600) // 60)
            
            parts = []
            if days > 0:
                parts.append(f"{days}d")
            if hours > 0:
                parts.append(f"{hours}h")
            if minutes > 0 or not parts:
                parts.append(f"{minutes}m")
            
            return " ".join(parts)
    except Exception as e:
        logger.debug(f"/proc/uptime failed for {container_name}: {e}")
    
    logger.warning(f"All uptime methods failed for {container_name}")
    return "Unknown"

async def get_container_processes_local(container_name: str, node_id: int) -> int:
    try:
        result = await execute_lxc(container_name, f"exec {container_name} -- sh -c 'ps aux | wc -l'", node_id=node_id, timeout=3)
        if result and result.strip().isdigit():
            count = int(result.strip()) - 1
            return max(0, count)
    except Exception as e:
        logger.debug(f"ps aux method failed for {container_name}: {e}")
    
    try:
        result = await execute_lxc(container_name, f"exec {container_name} -- sh -c 'ps -e | wc -l'", node_id=node_id, timeout=3)
        if result and result.strip().isdigit():
            count = int(result.strip()) - 1
            return max(0, count)
    except Exception as e:
        logger.debug(f"ps -e method failed for {container_name}: {e}")
    
    try:
        result = await execute_lxc(container_name, f"exec {container_name} -- sh -c 'ls -d /proc/[0-9]* | wc -l'", node_id=node_id, timeout=3)
        if result and result.strip().isdigit():
            return int(result.strip())
    except Exception as e:
        logger.debug(f"/proc count method failed for {container_name}: {e}")
    
    logger.warning(f"All process count methods failed for {container_name}")
    return 0

async def get_container_network_local(container_name: str, node_id: int) -> Dict:
    ips = []
    
    try:
        result = await execute_lxc(container_name, f"exec {container_name} -- ip addr show", node_id=node_id, timeout=3)
        if result:
            for line in result.split('\n'):
                if 'inet ' in line and '127.0.0.1' not in line:
                    parts = line.strip().split()
                    if len(parts) >= 2:
                        ip = parts[1].split('/')[0]
                        if ip not in ips:
                            ips.append(ip)
    except Exception as e:
        logger.debug(f"ip addr method failed for {container_name}: {e}")
    
    if not ips:
        try:
            result = await execute_lxc(container_name, f"exec {container_name} -- hostname -I", node_id=node_id, timeout=3)
            if result:
                for ip in result.strip().split():
                    if ip and ip != '127.0.0.1' and ip not in ips:
                        ips.append(ip)
        except Exception as e:
            logger.debug(f"hostname -I method failed for {container_name}: {e}")
    
    return {'ips': ips}

async def get_container_private_ip(container_name: str, node_id: int) -> str:
    try:
        result = await execute_lxc(container_name, f"exec {container_name} -- sh -c 'hostname -I'", node_id=node_id)
        if result:
            ips = result.strip().split()
            if ips:
                return ips[0]
        
        result = await execute_lxc(container_name, f"exec {container_name} -- ip addr show", node_id=node_id)
        if result:
            for line in result.split('\n'):
                if 'inet ' in line and '127.0.0.1' not in line:
                    parts = line.strip().split()
                    if len(parts) >= 2:
                        ip = parts[1].split('/')[0]
                        if not ip.startswith('127.'):
                            return ip
        
        return "N/A"
    except Exception as e:
        logger.error(f"Error getting private IP for {container_name}: {e}")
        return "N/A"

# ============================================================================
# Register API Blueprint
# ============================================================================
from api import api_bp
app.register_blueprint(api_bp)
logger.info("API blueprint registered at /api/v1")

# ============================================================================
# Web Routes - Authentication
# ============================================================================
@app.route('/')
def index():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))
    return render_template('index.html', panel_name=get_setting('site_name', 'KrysolVM'))

# ============================================================================
# AUTHENTICATION ROUTES
# ============================================================================

@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))
    
    if request.method == 'POST':
        username_or_email = request.form.get('username')
        password = request.form.get('password')
        remember = request.form.get('remember') == 'on'
        
        user = User.get_by_username(username_or_email)
        if not user:
            user = User.get_by_email(username_or_email)
        
        if user and check_password_hash(user.password_hash, password):
            if user.two_factor_enabled:
                session['2fa_user_id'] = user.id
                return redirect(url_for('two_factor'))
            
            login_user(user, remember=remember)
            now = datetime.now().isoformat()
            with get_db() as conn:
                cur = conn.cursor()
                cur.execute('UPDATE users SET last_login = ?, last_active = ? WHERE id = ?',
                           (now, now, user.id))
                conn.commit()
            
            log_activity(user.id, 'login', 'auth', None, {'ip': request.remote_addr})
            create_notification(user.id, 'info', 'New Login', f'New login from {request.remote_addr}', expires_in=86400)
            flash(f'Welcome back, {user.username}!', 'success')
            
            next_page = request.args.get('next')
            return redirect(next_page) if next_page else redirect(url_for('dashboard'))
        else:
            flash('Invalid username/email or password', 'danger')
            log_activity(None, 'login_failed', 'auth', None, {'username_or_email': username_or_email, 'ip': request.remote_addr})
    
    return render_template('login.html', panel_name=get_setting('site_name', 'KrysolVM'))

@app.route('/2fa', methods=['GET', 'POST'])
def two_factor():
    if '2fa_user_id' not in session:
        return redirect(url_for('login'))
    
    if request.method == 'POST':
        code = request.form.get('code')
        user_id = session.pop('2fa_user_id', None)
        user = User.get(user_id)
        if user:
            login_user(user)
            now = datetime.now().isoformat()
            with get_db() as conn:
                cur = conn.cursor()
                cur.execute('UPDATE users SET last_login = ?, last_active = ? WHERE id = ?',
                           (now, now, user.id))
                conn.commit()
            flash(f'Welcome back, {user.username}!', 'success')
            return redirect(url_for('dashboard'))
        else:
            flash('Invalid 2FA code', 'danger')
    
    return render_template('2fa.html', panel_name=get_setting('site_name', 'KrysolVM'))

@app.route('/register', methods=['GET', 'POST'])
def register():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))
    
    registration_enabled = get_setting('registration_enabled', '1')
    if registration_enabled != '1':
        flash('Registration is currently disabled', 'warning')
        return redirect(url_for('login'))
    
    if request.method == 'POST':
        username = request.form.get('username')
        email = request.form.get('email')
        password = request.form.get('password')
        confirm_password = request.form.get('confirm_password')
        terms = request.form.get('terms') == 'on'
        
        if not terms:
            flash('You must accept the terms of service', 'danger')
            return render_template('register.html', panel_name=get_setting('site_name', 'KrysolVM'))
        
        if password != confirm_password:
            flash('Passwords do not match', 'danger')
            return render_template('register.html', panel_name=get_setting('site_name', 'KrysolVM'))
        
        if len(password) < 8:
            flash('Password must be at least 8 characters', 'danger')
            return render_template('register.html', panel_name=get_setting('site_name', 'KrysolVM'))
        
        if User.get_by_username(username):
            flash('Username already taken', 'danger')
            return render_template('register.html', panel_name=get_setting('site_name', 'KrysolVM'))
        
        if User.get_by_email(email):
            flash('Email already registered', 'danger')
            return render_template('register.html', panel_name=get_setting('site_name', 'KrysolVM'))
        
        password_hash = generate_password_hash(password)
        api_key = generate_api_key()
        now = datetime.now().isoformat()
        
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('''INSERT INTO users 
                (username, email, password_hash, is_admin, is_main_admin, created_at, last_login, api_key, preferences)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (username, email, password_hash, 0, 0, now, now, api_key, '{}'))
            user_id = cur.lastrowid
            
            default_quota = int(get_setting('default_port_quota', '5'))
            cur.execute('INSERT INTO port_allocations (user_id, allocated_ports, used_ports, updated_at) VALUES (?, ?, ?, ?)',
                       (user_id, default_quota, 0, now))
            conn.commit()
        
        log_activity(user_id, 'register', 'auth', None, {'username': username, 'email': email})
        create_notification(user_id, 'success', 'Welcome!', f'Welcome to {get_setting("site_name", "KrysolVM")}! Your account has been created.')
        flash('Registration successful! Please log in.', 'success')
        return redirect(url_for('login'))
    
    return render_template('register.html', panel_name=get_setting('site_name', 'KrysolVM'))

# ============================================================================
# Discord OAuth Authentication
# ============================================================================

@app.route('/auth/discord/login')
def discord_login():
    if not get_setting('discord_auth_enabled', '0') == '1':
        flash('Discord authentication is not enabled', 'danger')
        return redirect(url_for('login'))
    
    client_id = get_setting('discord_client_id', '')
    redirect_uri = get_setting('discord_redirect_uri', '')
    
    if not client_id or not redirect_uri:
        flash('Discord authentication is not configured', 'danger')
        return redirect(url_for('login'))
    
    state = secrets.token_urlsafe(32)
    session['discord_oauth_state'] = state
    session['discord_oauth_action'] = 'login'
    
    params = {
        'client_id': client_id,
        'redirect_uri': redirect_uri,
        'response_type': 'code',
        'scope': 'identify email',
        'state': state
    }
    
    oauth_url = f"https://discord.com/api/oauth2/authorize?{urlencode(params)}"
    return redirect(oauth_url)

@app.route('/auth/discord/register')
def discord_register():
    if not get_setting('discord_auth_enabled', '0') == '1':
        flash('Discord authentication is not enabled', 'danger')
        return redirect(url_for('register'))
    
    if not get_setting('registration_enabled', '1') == '1':
        flash('Registration is currently disabled', 'danger')
        return redirect(url_for('login'))
    
    client_id = get_setting('discord_client_id', '')
    redirect_uri = get_setting('discord_redirect_uri', '')
    
    if not client_id or not redirect_uri:
        flash('Discord authentication is not configured', 'danger')
        return redirect(url_for('register'))
    
    state = secrets.token_urlsafe(32)
    session['discord_oauth_state'] = state
    session['discord_oauth_action'] = 'register'
    
    params = {
        'client_id': client_id,
        'redirect_uri': redirect_uri,
        'response_type': 'code',
        'scope': 'identify email',
        'state': state
    }
    
    oauth_url = f"https://discord.com/api/oauth2/authorize?{urlencode(params)}"
    return redirect(oauth_url)

@app.route('/auth/discord/callback')
def discord_callback():
    state = request.args.get('state')
    if not state or state != session.get('discord_oauth_state'):
        flash('Invalid OAuth state', 'danger')
        return redirect(url_for('login'))
    
    code = request.args.get('code')
    error = request.args.get('error')
    
    if error:
        flash(f'Discord authorization failed: {error}', 'danger')
        return redirect(url_for('login'))
    
    if not code:
        flash('No authorization code received', 'danger')
        return redirect(url_for('login'))
    
    client_id = get_setting('discord_client_id', '')
    client_secret = get_setting('discord_client_secret', '')
    redirect_uri = get_setting('discord_redirect_uri', '')
    
    token_data = {
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': redirect_uri
    }
    
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    
    try:
        token_response = requests.post(
            'https://discord.com/api/oauth2/token',
            data=token_data,
            headers=headers,
            timeout=10
        )
        token_response.raise_for_status()
        token_json = token_response.json()
        access_token = token_json['access_token']
    except Exception as e:
        logger.error(f"Discord token exchange error: {e}")
        flash('Failed to authenticate with Discord', 'danger')
        return redirect(url_for('login'))
    
    try:
        user_response = requests.get(
            'https://discord.com/api/users/@me',
            headers={'Authorization': f'Bearer {access_token}'},
            timeout=10
        )
        user_response.raise_for_status()
        discord_user = user_response.json()
    except Exception as e:
        logger.error(f"Discord user fetch error: {e}")
        flash('Failed to fetch user information from Discord', 'danger')
        return redirect(url_for('login'))
    
    discord_id = discord_user['id']
    discord_username = f"{discord_user['username']}#{discord_user['discriminator']}" if discord_user.get('discriminator') and discord_user.get('discriminator') != '0' else discord_user['username']
    discord_email = discord_user.get('email')
    discord_avatar = discord_user.get('avatar')
    
    action = session.get('discord_oauth_action', 'login')
    
    session.pop('discord_oauth_state', None)
    session.pop('discord_oauth_action', None)
    
    with get_db() as conn:
        cur = conn.cursor()
        
        cur.execute('SELECT * FROM users WHERE discord_id = ?', (discord_id,))
        existing_user = cur.fetchone()
        
        if action == 'login':
            if existing_user:
                user = User(dict(existing_user))
                login_user(user, remember=True)
                
                profile_picture = None
                if discord_avatar:
                    profile_picture = f"https://cdn.discordapp.com/avatars/{discord_id}/{discord_avatar}.png"
                
                cur.execute('UPDATE users SET last_login = ?, last_active = ?, discord_username = ?, discord_avatar = ?, discord_email = ?, profile_picture = ? WHERE id = ?',
                          (datetime.now().isoformat(), datetime.now().isoformat(), discord_username, discord_avatar, discord_email, profile_picture, user.id))
                conn.commit()
                
                log_activity(user.id, 'login_discord', 'auth', str(user.id))
                flash(f'Welcome back, {user.username}!', 'success')
                return redirect(url_for('dashboard'))
            else:
                if get_setting('discord_auto_register', '0') == '1':
                    return discord_auto_register(discord_id, discord_username, discord_email, discord_avatar)
                else:
                    flash('No account found with this Discord account. Please register first.', 'warning')
                    return redirect(url_for('register'))
        
        elif action == 'register':
            if existing_user:
                flash('This Discord account is already registered. Please log in instead.', 'warning')
                return redirect(url_for('login'))
            else:
                return discord_auto_register(discord_id, discord_username, discord_email, discord_avatar)
        
        elif action == 'link':
            if not current_user.is_authenticated:
                flash('You must be logged in to link Discord', 'danger')
                return redirect(url_for('login'))
            
            if existing_user and existing_user['id'] != current_user.id:
                flash(f'This Discord account is already linked to another user', 'danger')
                return redirect(url_for('profile'))
            
            profile_picture = None
            if discord_avatar:
                profile_picture = f"https://cdn.discordapp.com/avatars/{discord_id}/{discord_avatar}.png"
            
            cur.execute('''UPDATE users 
                SET discord_id = ?, discord_username = ?, discord_avatar = ?, discord_email = ?, profile_picture = ?
                WHERE id = ?''',
                (discord_id, discord_username, discord_avatar, discord_email, profile_picture, current_user.id))
            conn.commit()
            
            flash('Discord account linked successfully!', 'success')
            log_activity(current_user.id, 'link_discord', 'auth', str(current_user.id))
            return redirect(url_for('profile'))
    
    flash('An error occurred during authentication', 'danger')
    return redirect(url_for('login'))

def discord_auto_register(discord_id, discord_username, discord_email, discord_avatar):
    try:
        with get_db() as conn:
            cur = conn.cursor()
            
            base_username = discord_username.split('#')[0].lower()
            base_username = re.sub(r'[^a-z0-9_]', '', base_username)
            
            username = base_username
            counter = 1
            while True:
                cur.execute('SELECT id FROM users WHERE username = ?', (username,))
                if not cur.fetchone():
                    break
                username = f"{base_username}{counter}"
                counter += 1
            
            email = discord_email if discord_email else f"{username}@discord.local"
            
            counter = 1
            original_email = email
            while True:
                cur.execute('SELECT id FROM users WHERE email = ?', (email,))
                if not cur.fetchone():
                    break
                email = f"{username}{counter}@discord.local"
                counter += 1
            
            profile_picture = None
            if discord_avatar:
                profile_picture = f"https://cdn.discordapp.com/avatars/{discord_id}/{discord_avatar}.png"
            
            random_password = secrets.token_urlsafe(32)
            password_hash = generate_password_hash(random_password)
            api_key = generate_api_key()
            
            now = datetime.now().isoformat()
            
            cur.execute('''INSERT INTO users 
                (username, email, password_hash, is_admin, is_main_admin, created_at, last_login, last_active, api_key, preferences,
                 discord_id, discord_username, discord_avatar, discord_email, profile_picture)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (username, email, password_hash, 0, 0, now, now, now, api_key, '{}',
                 discord_id, discord_username, discord_avatar, discord_email, profile_picture))
            
            user_id = cur.lastrowid
            
            default_quota = int(get_setting('default_port_quota', '5'))
            cur.execute('INSERT INTO port_allocations (user_id, allocated_ports, used_ports, updated_at) VALUES (?, ?, ?, ?)',
                       (user_id, default_quota, 0, now))
            
            conn.commit()
            
            cur.execute('SELECT * FROM users WHERE id = ?', (user_id,))
            user_row = cur.fetchone()
            user = User(dict(user_row))
            login_user(user, remember=True)
            
            log_activity(user.id, 'register_discord', 'auth', str(user.id))
            create_notification(user.id, 'success', 'Welcome!', f'Welcome to {get_setting("site_name", "KrysolVM")}, {username}! Your account has been created.')
            flash(f'Welcome to {get_setting("site_name", "KrysolVM")}, {username}! Your account has been created.', 'success')
            return redirect(url_for('dashboard'))
            
    except Exception as e:
        logger.error(f"Discord auto-register error: {e}")
        flash('Failed to create account. Please try again.', 'danger')
        return redirect(url_for('register'))

@app.route('/auth/discord/link')
@login_required
def discord_link():
    if not get_setting('discord_auth_enabled', '0') == '1':
        flash('Discord authentication is not enabled', 'danger')
        return redirect(url_for('profile'))
    
    client_id = get_setting('discord_client_id', '')
    redirect_uri = get_setting('discord_redirect_uri', '')
    
    if not client_id or not redirect_uri:
        flash('Discord authentication is not configured', 'danger')
        return redirect(url_for('profile'))
    
    state = secrets.token_urlsafe(32)
    session['discord_oauth_state'] = state
    session['discord_oauth_action'] = 'link'
    
    params = {
        'client_id': client_id,
        'redirect_uri': redirect_uri,
        'response_type': 'code',
        'scope': 'identify email',
        'state': state
    }
    
    oauth_url = f"https://discord.com/api/oauth2/authorize?{urlencode(params)}"
    return redirect(oauth_url)

@app.route('/auth/discord/link-callback')
@login_required
def discord_link_callback():
    state = request.args.get('state')
    if not state or state != session.get('discord_oauth_state'):
        flash('Invalid OAuth state', 'danger')
        return redirect(url_for('profile'))
    
    code = request.args.get('code')
    if not code:
        flash('No authorization code received', 'danger')
        return redirect(url_for('profile'))
    
    client_id = get_setting('discord_client_id', '')
    client_secret = get_setting('discord_client_secret', '')
    redirect_uri = get_setting('discord_redirect_uri', '').replace('/callback', '/link-callback')
    
    token_data = {
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': redirect_uri
    }
    
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    
    try:
        token_response = requests.post(
            'https://discord.com/api/oauth2/token',
            data=token_data,
            headers=headers,
            timeout=10
        )
        token_response.raise_for_status()
        token_json = token_response.json()
        access_token = token_json['access_token']
        
        user_response = requests.get(
            'https://discord.com/api/users/@me',
            headers={'Authorization': f'Bearer {access_token}'},
            timeout=10
        )
        user_response.raise_for_status()
        discord_user = user_response.json()
        
        discord_id = discord_user['id']
        discord_username = f"{discord_user['username']}#{discord_user['discriminator']}" if discord_user.get('discriminator') and discord_user.get('discriminator') != '0' else discord_user['username']
        discord_email = discord_user.get('email')
        discord_avatar = discord_user.get('avatar')
        
        session.pop('discord_oauth_state', None)
        session.pop('discord_oauth_action', None)
        
        with get_db() as conn:
            cur = conn.cursor()
            
            cur.execute('SELECT id, username FROM users WHERE discord_id = ? AND id != ?', (discord_id, current_user.id))
            other_user = cur.fetchone()
            if other_user:
                flash(f'This Discord account is already linked to another user ({other_user[1]})', 'danger')
                return redirect(url_for('profile'))
            
            profile_picture = None
            if discord_avatar:
                profile_picture = f"https://cdn.discordapp.com/avatars/{discord_id}/{discord_avatar}.png"
            
            cur.execute('''UPDATE users 
                SET discord_id = ?, discord_username = ?, discord_avatar = ?, discord_email = ?, profile_picture = ?
                WHERE id = ?''',
                (discord_id, discord_username, discord_avatar, discord_email, profile_picture, current_user.id))
            conn.commit()
        
        flash('Discord account linked successfully!', 'success')
        log_activity(current_user.id, 'link_discord', 'auth', str(current_user.id))
        
    except Exception as e:
        logger.error(f"Discord link error: {e}")
        flash('Failed to link Discord account', 'danger')
    
    return redirect(url_for('profile'))

@app.route('/auth/discord/unlink', methods=['POST'])
@login_required
def discord_unlink():
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('''UPDATE users 
                SET discord_id = NULL, discord_username = NULL, discord_avatar = NULL, discord_email = NULL
                WHERE id = ?''',
                (current_user.id,))
            conn.commit()
        
        flash('Discord account unlinked successfully', 'success')
        log_activity(current_user.id, 'unlink_discord', 'auth', str(current_user.id))
    except Exception as e:
        logger.error(f"Discord unlink error: {e}")
        flash('Failed to unlink Discord account', 'danger')
    
    return redirect(url_for('profile'))

@app.route('/logout')
@login_required
def logout():
    log_activity(current_user.id, 'logout', 'auth')
    logout_user()
    flash('You have been logged out', 'info')
    return redirect(url_for('index'))

def send_email(to_email, subject, body, html_body=None):
    try:
        import smtplib
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart
        
        smtp_host = get_setting('smtp_host', '')
        smtp_port = int(get_setting('smtp_port', '587'))
        smtp_username = get_setting('smtp_username', '')
        smtp_password = get_setting('smtp_password', '')
        smtp_use_tls = get_setting('smtp_use_tls', '1') == '1'
        smtp_use_ssl = get_setting('smtp_use_ssl', '0') == '1'
        smtp_from_email = get_setting('smtp_from_email', smtp_username)
        smtp_from_name = get_setting('smtp_from_name', get_setting('site_name', 'KrysolVM'))
        
        if not smtp_host or not smtp_username or not smtp_password:
            logger.error("SMTP configuration incomplete")
            return False, "SMTP not configured"
        
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = f"{smtp_from_name} <{smtp_from_email}>"
        msg['To'] = to_email
        
        text_part = MIMEText(body, 'plain')
        msg.attach(text_part)
        
        if html_body:
            html_part = MIMEText(html_body, 'html')
            msg.attach(html_part)
        
        if smtp_use_ssl:
            server = smtplib.SMTP_SSL(smtp_host, smtp_port)
        else:
            server = smtplib.SMTP(smtp_host, smtp_port)
            if smtp_use_tls:
                server.starttls()
        
        server.login(smtp_username, smtp_password)
        server.send_message(msg)
        server.quit()
        
        logger.info(f"Email sent successfully to {to_email}")
        return True, "Email sent successfully"
        
    except Exception as e:
        logger.error(f"Failed to send email to {to_email}: {e}")
        return False, str(e)

def generate_password_reset_token(user_id):
    import secrets
    import hashlib
    
    token = secrets.token_urlsafe(32)
    
    expiry = (datetime.now() + timedelta(hours=1)).isoformat()
    
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('DELETE FROM password_reset_tokens WHERE user_id = ?', (user_id,))
        
        cur.execute('''INSERT INTO password_reset_tokens 
                      (user_id, token, expires_at, created_at) 
                      VALUES (?, ?, ?, ?)''',
                   (user_id, token, expiry, datetime.now().isoformat()))
        conn.commit()
    
    return token

def verify_password_reset_token(token):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('''SELECT user_id, expires_at FROM password_reset_tokens 
                      WHERE token = ?''', (token,))
        row = cur.fetchone()
        
        if not row:
            return None
        
        user_id, expires_at = row
        
        if datetime.fromisoformat(expires_at) < datetime.now():
            cur.execute('DELETE FROM password_reset_tokens WHERE token = ?', (token,))
            conn.commit()
            return None
        
        return user_id

def cleanup_expired_reset_tokens():
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('DELETE FROM password_reset_tokens WHERE expires_at < ?',
                   (datetime.now().isoformat(),))
        deleted = cur.rowcount
        conn.commit()
        if deleted > 0:
            logger.info(f"Cleaned up {deleted} expired password reset tokens")

@app.route('/forgot-password', methods=['GET', 'POST'])
def forgot_password():
    if request.method == 'POST':
        email = request.form.get('email', '').strip().lower()
        
        if not email:
            flash('Please enter your email address', 'danger')
            return render_template('forgot_password.html', 
                                 panel_name=get_setting('site_name', 'KrysolVM'))
        
        success_message = 'If the email address is registered, you will receive a password reset link shortly.'
        
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('SELECT id, username FROM users WHERE email = ?', (email,))
            user = cur.fetchone()
        
        if user:
            user_id, username = user
            
            token = generate_password_reset_token(user_id)
            
            reset_url = url_for('reset_password', token=token, _external=True)
            
            site_name = get_setting('site_name', 'KrysolVM')
            subject = f"Password Reset - {site_name}"
            
            text_body = f"""Hello {username},

You have requested a password reset for your {site_name} account.

Click the link below to reset your password:
{reset_url}

This link will expire in 1 hour for security reasons.

If you did not request this password reset, please ignore this email.

Best regards,
{site_name} Team"""

            html_body = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Password Reset</title>
    <style>
        body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
        .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
        .header {{ background: #3b82f6; color: white; padding: 20px; text-align: center; border-radius: 8px 8px 0 0; }}
        .content {{ background: #f8f9fa; padding: 30px; border-radius: 0 0 8px 8px; }}
        .button {{ display: inline-block; background: #3b82f6; color: white; padding: 12px 24px; text-decoration: none; border-radius: 6px; margin: 20px 0; }}
        .footer {{ margin-top: 20px; padding-top: 20px; border-top: 1px solid #ddd; font-size: 14px; color: #666; }}
        .warning {{ background: #fff3cd; border: 1px solid #ffeaa7; padding: 15px; border-radius: 6px; margin: 20px 0; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>{site_name}</h1>
            <p>Password Reset Request</p>
        </div>
        <div class="content">
            <h2>Hello {username},</h2>
            <p>You have requested a password reset for your {site_name} account.</p>
            <p>Click the button below to reset your password:</p>
            <p style="text-align: center;">
                <a href="{reset_url}" class="button">Reset Password</a>
            </p>
            <div class="warning">
                <strong>⚠️ Security Notice:</strong>
                <ul>
                    <li>This link will expire in <strong>1 hour</strong></li>
                    <li>If you did not request this reset, please ignore this email</li>
                    <li>Never share this link with anyone</li>
                </ul>
            </div>
            <p>If the button doesn't work, copy and paste this link into your browser:</p>
            <p style="word-break: break-all; background: #e9ecef; padding: 10px; border-radius: 4px; font-family: monospace;">
                {reset_url}
            </p>
        </div>
        <div class="footer">
            <p>Best regards,<br>{site_name} Team</p>
            <p><small>This is an automated message. Please do not reply to this email.</small></p>
        </div>
    </div>
</body>
</html>"""
            
            success, error_msg = send_email(email, subject, text_body, html_body)
            
            if success:
                logger.info(f"Password reset email sent to {email} for user {username}")
                log_activity(user_id, 'request_password_reset', 'user', str(user_id), 
                           {'email': email})
            else:
                logger.error(f"Failed to send password reset email to {email}: {error_msg}")
        
        flash(success_message, 'info')
        return redirect(url_for('login'))
    
    return render_template('forgot_password.html', 
                         panel_name=get_setting('site_name', 'KrysolVM'))

@app.route('/reset-password/<token>', methods=['GET', 'POST'])
def reset_password(token):
    user_id = verify_password_reset_token(token)
    
    if not user_id:
        flash('Invalid or expired reset link. Please request a new password reset.', 'danger')
        return redirect(url_for('forgot_password'))
    
    if request.method == 'POST':
        password = request.form.get('password', '')
        confirm_password = request.form.get('confirm_password', '')
        
        if not password or len(password) < 8:
            flash('Password must be at least 8 characters long', 'danger')
            return render_template('reset_password.html', token=token, 
                                 panel_name=get_setting('site_name', 'KrysolVM'))
        
        if password != confirm_password:
            flash('Passwords do not match', 'danger')
            return render_template('reset_password.html', token=token,
                                 panel_name=get_setting('site_name', 'KrysolVM'))
        
        password_hash = generate_password_hash(password)
        
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('UPDATE users SET password_hash = ? WHERE id = ?', 
                       (password_hash, user_id))
            
            cur.execute('DELETE FROM password_reset_tokens WHERE token = ?', (token,))
            
            cur.execute('SELECT username, email FROM users WHERE id = ?', (user_id,))
            user_info = cur.fetchone()
            
            conn.commit()
        
        if user_info:
            username, email = user_info
            logger.info(f"Password reset completed for user {username} ({email})")
            log_activity(user_id, 'complete_password_reset', 'user', str(user_id))
            
            site_name = get_setting('site_name', 'KrysolVM')
            subject = f"Password Changed - {site_name}"
            
            text_body = f"""Hello {username},

Your password has been successfully changed for your {site_name} account.

If you did not make this change, please contact support immediately.

Best regards,
{site_name} Team"""

            html_body = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Password Changed</title>
    <style>
        body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
        .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
        .header {{ background: #10b981; color: white; padding: 20px; text-align: center; border-radius: 8px 8px 0 0; }}
        .content {{ background: #f8f9fa; padding: 30px; border-radius: 0 0 8px 8px; }}
        .success {{ background: #d1fae5; border: 1px solid #10b981; padding: 15px; border-radius: 6px; margin: 20px 0; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>{site_name}</h1>
            <p>Password Changed Successfully</p>
        </div>
        <div class="content">
            <h2>Hello {username},</h2>
            <div class="success">
                <strong>✅ Success!</strong> Your password has been successfully changed.
            </div>
            <p>Your {site_name} account password was updated on {datetime.now().strftime('%B %d, %Y at %I:%M %p')}.</p>
            <p>If you did not make this change, please contact our support team immediately.</p>
            <p>Best regards,<br>{site_name} Team</p>
        </div>
    </div>
</body>
</html>"""
            
            send_email(email, subject, text_body, html_body)
        
        flash('Password reset successful! You can now log in with your new password.', 'success')
        return redirect(url_for('login'))
    
    return render_template('reset_password.html', token=token,
                         panel_name=get_setting('site_name', 'KrysolVM'))

# ============================================================================
# Notifications Routes
# ============================================================================
@app.route('/notifications')
@login_required
def notifications():
    page = int(request.args.get('page', 1))
    per_page = 20
    offset = (page - 1) * per_page
    
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('''SELECT * FROM notifications 
                      WHERE user_id = ? AND (expires_at IS NULL OR expires_at > ?)
                      ORDER BY created_at DESC LIMIT ? OFFSET ?''',
                   (current_user.id, datetime.now().isoformat(), per_page, offset))
        notifications = [dict(row) for row in cur.fetchall()]
        
        for notif in notifications:
            if notif['data']:
                try:
                    notif['data'] = json.loads(notif['data'])
                except:
                    notif['data'] = {}
        
        cur.execute('''SELECT COUNT(*) FROM notifications 
                      WHERE user_id = ? AND (expires_at IS NULL OR expires_at > ?)''',
                   (current_user.id, datetime.now().isoformat()))
        total = cur.fetchone()[0]
    
    return render_template('notifications.html',
                          panel_name=get_setting('site_name', 'KrysolVM'),
                          notifications=notifications,
                          page=page,
                          total_pages=(total + per_page - 1) // per_page)

@app.route('/notifications/unread')
@login_required
def unread_notifications():
    notifications = get_user_notifications(current_user.id, unread_only=True, limit=10)
    count = get_unread_notifications_count(current_user.id)
    
    return jsonify({
        'success': True,
        'count': count,
        'notifications': notifications
    })

@app.route('/notifications/mark-read/<int:notification_id>', methods=['POST'])
@login_required
def mark_notification_read_route(notification_id):
    success = mark_notification_read(notification_id, current_user.id)
    return jsonify({'success': success})

@app.route('/notifications/mark-all-read', methods=['POST'])
@login_required
def mark_all_notifications_read_route():
    count = mark_all_notifications_read(current_user.id)
    return jsonify({'success': True, 'count': count})

@app.route('/notifications/clear-all', methods=['POST'])
@login_required
def clear_all_notifications():
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('DELETE FROM notifications WHERE user_id = ?', (current_user.id,))
        conn.commit()
    return jsonify({'success': True})

@app.route('/notifications/delete/<int:notification_id>', methods=['POST'])
@login_required
def delete_notification_route(notification_id):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('DELETE FROM notifications WHERE id = ? AND user_id = ?', 
                   (notification_id, current_user.id))
        conn.commit()
    return jsonify({'success': True})

# ============================================================================
# OS Icons Routes
# ============================================================================
@app.route('/os-icons')
@login_required
@admin_required
def os_icons():
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('SELECT * FROM os_icons ORDER BY os_name')
        icons = [dict(row) for row in cur.fetchall()]
    
    return render_template('admin/os_icons.html',
                          panel_name=get_setting('site_name', 'KrysolVM'),
                          icons=icons,
                          os_options=OS_OPTIONS)

@app.route('/os-icons/upload', methods=['POST'])
@login_required
@admin_required
def upload_os_icon():
    os_name = request.form.get('os_name')
    
    if 'icon' not in request.files:
        return jsonify({'success': False, 'error': 'No file uploaded'}), 400
    
    file = request.files['icon']
    if file.filename == '':
        return jsonify({'success': False, 'error': 'No file selected'}), 400
    
    if not os_name:
        return jsonify({'success': False, 'error': 'OS name required'}), 400
    
    if not allowed_file(file.filename):
        return jsonify({'success': False, 'error': 'Invalid file type'}), 400
    
    file.seek(0, os.SEEK_END)
    size = file.tell()
    file.seek(0)
    
    if size > app.config['MAX_IMAGE_SIZE']:
        return jsonify({'success': False, 'error': 'File too large (max 5MB)'}), 400
    
    filename = secure_filename(f"os_{os_name}_{int(time.time())}.{file.filename.rsplit('.', 1)[1].lower()}")
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], 'os_icons', filename)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    file.save(filepath)
    
    if PIL_AVAILABLE and Image:
        try:
            img = Image.open(filepath)
            img.thumbnail((64, 64), Image.Resampling.LANCZOS)
            img.save(filepath, optimize=True, quality=85)
        except Exception as e:
            logger.error(f"Failed to optimize image: {e}")
    
    icon_path = f'/static/uploads/os_icons/{filename}'
    
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('''INSERT OR REPLACE INTO os_icons (os_name, icon_path, uploaded_at, uploaded_by)
                      VALUES (?, ?, ?, ?)''',
                   (os_name, icon_path, datetime.now().isoformat(), current_user.id))
        conn.commit()
    
    log_activity(current_user.id, 'upload_os_icon', 'os_icon', None, {'os_name': os_name})
    return jsonify({'success': True, 'icon_path': icon_path})

@app.route('/os-icons/<path:os_name>/delete', methods=['POST'])
@login_required
@admin_required
def delete_os_icon(os_name):
    try:
        with get_db() as conn:
            cur = conn.cursor()
            
            cur.execute('SELECT icon_path FROM os_icons WHERE os_name = ?', (os_name,))
            row = cur.fetchone()
            
            if row:
                icon_path = row[0]
                
                cur.execute('DELETE FROM os_icons WHERE os_name = ?', (os_name,))
                conn.commit()
                
                try:
                    if icon_path and icon_path.startswith('/static/uploads/'):
                        file_path = icon_path.replace('/static/', 'static/')
                        if os.path.exists(file_path):
                            os.remove(file_path)
                            logger.info(f"Deleted icon file: {file_path}")
                except Exception as e:
                    logger.warning(f"Failed to delete icon file: {e}")
                
                log_activity(current_user.id, 'delete_os_icon', 'os_icon', None, {'os_name': os_name})
                return jsonify({'success': True, 'message': 'Icon removed successfully'})
            else:
                return jsonify({'success': False, 'error': 'Icon not found'}), 404
                
    except Exception as e:
        logger.error(f"Error deleting OS icon: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/os-icons/<os_name>')
def get_os_icon(os_name):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('SELECT icon_path FROM os_icons WHERE os_name = ?', (os_name,))
        row = cur.fetchone()
        if row:
            return jsonify({'success': True, 'icon_path': row[0]})
    
    return jsonify({'success': True, 'icon_path': '/static/img/os/default.png'})

# ============================================================================
# User Profile Routes
# ============================================================================
@app.route('/profile', methods=['GET', 'POST'])
@login_required
def profile():
    if request.method == 'POST':
        username = request.form.get('username')
        email = request.form.get('email')
        current_password = request.form.get('current_password')
        new_password = request.form.get('new_password')
        confirm_password = request.form.get('confirm_password')
        theme = request.form.get('theme')
        language = request.form.get('language')
        
        updates = {}
        
        if any([username != current_user.username, email != current_user.email, new_password]):
            if not current_password or not check_password_hash(current_user.password_hash, current_password):
                flash('Current password is incorrect', 'danger')
                return redirect(url_for('profile'))
        
        if username and username != current_user.username:
            if User.get_by_username(username):
                flash('Username already taken', 'danger')
            else:
                updates['username'] = username
        
        if email and email != current_user.email:
            if User.get_by_email(email):
                flash('Email already taken', 'danger')
            else:
                updates['email'] = email
        
        if new_password:
            if new_password != confirm_password:
                flash('New passwords do not match', 'danger')
            elif len(new_password) < 8:
                flash('Password must be at least 8 characters', 'danger')
            else:
                updates['password_hash'] = generate_password_hash(new_password)
        
        if theme:
            updates['theme'] = theme
        if language:
            updates['language'] = language
        
        if updates:
            with get_db() as conn:
                cur = conn.cursor()
                fields = ', '.join(f"{k} = ?" for k in updates)
                values = list(updates.values()) + [current_user.id]
                cur.execute(f'UPDATE users SET {fields} WHERE id = ?', values)
                conn.commit()
            
            log_activity(current_user.id, 'update_profile', 'user', str(current_user.id), 
                        {'fields': list(updates.keys())})
            create_notification(current_user.id, 'success', 'Profile Updated', 'Your profile has been updated successfully.')
            flash('Profile updated successfully', 'success')
        else:
            flash('No changes made', 'info')
        
        return redirect(url_for('profile'))
    
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('''SELECT * FROM activity_logs WHERE user_id = ? 
                       ORDER BY created_at DESC LIMIT 50''', (current_user.id,))
        activities = [dict(row) for row in cur.fetchall()]
    
    notifications = get_user_notifications(current_user.id, limit=10)
    
    return render_template('profile.html',
                          panel_name=get_setting('site_name', 'KrysolVM'),
                          activities=activities,
                          notifications=notifications)

@app.route('/profile/picture', methods=['POST'])
@login_required
def upload_profile_picture():
    if 'picture' not in request.files:
        return jsonify({'success': False, 'error': 'No file uploaded'}), 400
    
    file = request.files['picture']
    if file.filename == '':
        return jsonify({'success': False, 'error': 'No file selected'}), 400
    
    if not allowed_file(file.filename):
        return jsonify({'success': False, 'error': 'Invalid file type'}), 400
    
    file.seek(0, os.SEEK_END)
    size = file.tell()
    file.seek(0)
    
    if size > app.config['MAX_IMAGE_SIZE']:
        return jsonify({'success': False, 'error': 'File too large (max 5MB)'}), 400
    
    filename = secure_filename(f"user_{current_user.id}_{int(time.time())}.{file.filename.rsplit('.', 1)[1].lower()}")
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], 'profiles', filename)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    file.save(filepath)
    
    if PIL_AVAILABLE and Image:
        try:
            img = Image.open(filepath)
            img.thumbnail((256, 256), Image.Resampling.LANCZOS)
            img.save(filepath, optimize=True, quality=85)
        except Exception as e:
            logger.error(f"Failed to optimize image: {e}")
    
    if current_user.profile_picture:
        old_path = current_user.profile_picture.lstrip('/')
        if os.path.exists(old_path):
            try:
                os.remove(old_path)
            except:
                pass
    
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('UPDATE users SET profile_picture = ? WHERE id = ?',
                   (f'/static/uploads/profiles/{filename}', current_user.id))
        conn.commit()
    
    return jsonify({'success': True, 'path': f'/static/uploads/profiles/{filename}'})

@app.route('/profile/picture/delete', methods=['POST'])
@login_required
def delete_profile_picture():
    if current_user.profile_picture:
        old_path = current_user.profile_picture.lstrip('/')
        if os.path.exists(old_path):
            try:
                os.remove(old_path)
            except:
                pass
    
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('UPDATE users SET profile_picture = NULL WHERE id = ?', (current_user.id,))
        conn.commit()
    
    return jsonify({'success': True})

@app.route('/profile/api-key/regenerate', methods=['POST'])
@login_required
def regenerate_api_key():
    new_key = generate_api_key()
    
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('UPDATE users SET api_key = ? WHERE id = ?', (new_key, current_user.id))
        conn.commit()
    
    log_activity(current_user.id, 'regenerate_api_key', 'user', str(current_user.id))
    create_notification(current_user.id, 'warning', 'API Key Regenerated', 'Your API key has been regenerated.')
    return jsonify({'success': True, 'api_key': new_key})

@app.route('/profile/preferences', methods=['POST'])
@login_required
def update_preferences():
    data = request.get_json()
    
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('UPDATE users SET preferences = ? WHERE id = ?',
                   (json.dumps(data.get('preferences', {})), current_user.id))
        conn.commit()
    
    return jsonify({'success': True})

@app.route('/profile_picture')
@login_required
def profile_picture():
    if current_user.profile_picture and os.path.exists(current_user.profile_picture.lstrip('/')):
        return send_from_directory('static', current_user.profile_picture.replace('/static/', ''))
    else:
        return send_from_directory('static/img', 'default_avatar.png')

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']

# ============================================================================
# Web Routes - Main Dashboard
# ============================================================================
@app.route('/dashboard')
@login_required
def dashboard():
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('UPDATE users SET last_active = ? WHERE id = ?',
                   (datetime.now().isoformat(), current_user.id))
        conn.commit()
    
    vps_list = get_vps_for_user(current_user.id)
    
    for vps in vps_list:
        if is_vps_suspended(vps):
            vps['live_status'] = 'suspended'
        else:
            vps['live_status'] = vps.get('status', 'unknown').lower()
        
        vps['live_cpu'] = 0
        vps['live_ram'] = {'pct': 0}
    
    total_cpu = sum(int(vps['cpu']) for vps in vps_list)
    total_ram = sum(int(str(vps['ram']).replace('GB', '').replace('MB', '')) for vps in vps_list)
    total_disk = sum(int(str(vps['storage']).replace('GB', '')) for vps in vps_list)
    
    running_count = sum(1 for vps in vps_list if vps.get('live_status') == 'running' and not is_vps_suspended(vps))
    suspended_count = sum(1 for vps in vps_list if is_vps_suspended(vps))
    stopped_count = len(vps_list) - running_count - suspended_count
    
    notifications = get_user_notifications(current_user.id, unread_only=True, limit=5)
    
    for vps in vps_list:
        if vps.get('live_status') == 'running' and vps.get('live_ram', {}).get('pct', 0) > 90:
            create_notification(
                current_user.id, 
                'warning', 
                'High RAM Usage', 
                f'VPS {vps["container_name"]} is using high RAM ({vps["live_ram"]["pct"]:.1f}%)'
            )
    
    nodes = get_nodes()
    node_status = []
    for node in nodes[:3]:
        status = run_sync(get_node_status(node['id']))
        node_status.append({
            'id': node['id'],
            'name': node['name'],
            'status': status['status'],
            'online': status.get('online', False),
            'vps_count': get_current_vps_count(node['id']),
            'total_vps': node['total_vps']
        })
    
    return render_template('dashboard.html',
                          panel_name=get_setting('site_name', 'KrysolVM'),
                          site_description=get_setting('site_description', ''),
                          header_icon=get_setting('header_icon', '/static/img/logo.png'),
                          vps_list=vps_list,
                          vps_count=len(vps_list),
                          running_count=running_count,
                          suspended_count=suspended_count,
                          stopped_count=stopped_count,
                          total_cpu=total_cpu,
                          total_ram=total_ram,
                          total_disk=total_disk,
                          notifications=notifications,
                          node_status=node_status,
                          socketio_available=SOCKETIO_AVAILABLE)

@app.route('/vps')
@login_required
def vps_list():
    vps_list = get_vps_for_user(current_user.id)

    for vps in vps_list:
        if is_vps_suspended(vps):
            vps['live_status'] = 'suspended'
        else:
            vps['live_status'] = vps.get('status', 'unknown').lower()
        
        vps['live_cpu'] = 0.0
        vps['live_ram'] = {'used': 0, 'total': 0, 'pct': 0.0}
        vps['live_disk'] = {'use_percent': '0%', 'pct': 0.0}

    return render_template(
        'vps_list.html',
        panel_name=get_setting('site_name', 'KrysolVM'),
        vps_list=vps_list,
        socketio_available=SOCKETIO_AVAILABLE
    )

@app.route('/vps/<int:vps_id>')
@login_required
def vps_detail(vps_id):
    vps = get_vps_by_id(vps_id)
    if not vps:
        flash('VPS not found', 'danger')
        return redirect(url_for('vps_list'))

    shared_with = vps.get('shared_with', []) or []

    allowed = (
        vps['user_id'] == current_user.id
        or str(current_user.id) in [str(uid) for uid in shared_with]
        or current_user.is_admin
    )

    if not allowed:
        flash('VPS not found or access denied', 'danger')
        return redirect(url_for('vps_list'))

    if vps.get('status') == 'installing':
        return redirect(url_for('vps_installing', vps_id=vps_id))
    
    if vps.get('status') == 'transferring':
        return redirect(url_for('vps_migrating_page', vps_id=vps_id))

    if is_vps_suspended(vps):
        return redirect(url_for('vps_suspended_page', vps_id=vps_id))

    node = get_node(vps['node_id'])

    try:
        logger.debug(f"Fetching stats for container {vps['container_name']} on node {vps['node_id']}")
        stats = run_sync(
            get_container_stats(
                vps['container_name'],
                vps['node_id']
            )
        ) or {}
        
        logger.debug(f"Stats fetched for {vps['container_name']}: CPU={stats.get('cpu', 'N/A')}, Status={stats.get('status', 'N/A')}")

    except Exception as e:
        logger.error(
            f"Stats error for {vps['container_name']}: {e}", exc_info=True
        )
        stats = {
            "status": "unknown",
            "cpu": 0.0,
            "ram": {"used": 0, "total": 0, "pct": 0.0},
            "disk": {"use_percent": "0%"},
            "uptime": "Unknown",
            "processes": 0,
            "network": {},
            "load_average": {"1min": 0.0, "5min": 0.0, "15min": 0.0}
        }

    live_status = stats.get("status")
    logger.debug(f"VPS {vps_id} ({vps['container_name']}) - Live status: '{live_status}', DB status: '{vps.get('status')}'")

    connection_issue = False
    if live_status and ('_cached' in live_status or live_status in ('timeout', 'error', 'unknown', 'server_error', 'circuit_open', 'connection_error')):
        connection_issue = True
        logger.debug(f"VPS {vps_id} has connection issue: '{live_status}'")
        
        if live_status.endswith('_cached'):
            cached_stats = get_cached_vps_stats(vps['container_name'])
            if cached_stats and cached_stats.get('status') in ('running', 'stopped'):
                live_status = cached_stats['status']
                logger.debug(f"VPS {vps_id} using cached real status: '{live_status}'")
            else:
                live_status = vps.get('status', 'stopped').lower()
                logger.debug(f"VPS {vps_id} using database status: '{live_status}'")
        else:
            live_status = vps.get('status', 'stopped').lower()
            logger.debug(f"VPS {vps_id} using database status due to error: '{live_status}'")

    if is_vps_suspended(vps):
        live_status = 'suspended'
        vps['status'] = 'suspended'
        logger.debug(f"VPS {vps_id} is suspended, setting status to suspended")
    elif live_status in ("running", "stopped"):
        db_status = (vps.get("status") or "").lower()
        live_status_lower = live_status.lower()
        
        if not connection_issue and live_status_lower != db_status:
            logger.info(f"VPS {vps_id} status updated from '{db_status}' to '{live_status_lower}'")
            update_vps(vps_id, status=live_status_lower)
            vps["status"] = live_status_lower
        else:
            logger.debug(f"VPS {vps_id} status: '{live_status_lower}' (connection_issue={connection_issue})")
    else:
        logger.warning(f"VPS {vps_id} has unexpected live_status: '{live_status}'")

    private_ip = "N/A"
    if live_status == "running":
        try:
            private_ip = run_sync(
                get_container_private_ip(
                    vps['container_name'],
                    vps['node_id']
                )
            )
            logger.debug(f"VPS {vps_id} ({vps['container_name']}) private IP: {private_ip}")
        except Exception as e:
            logger.error(f"Error getting private IP for {vps['container_name']}: {e}")
            private_ip = "N/A"

    current_bandwidth_usage = None
    if live_status == "running" and vps.get('bandwidth_quota_gb', 0) > 0:
        try:
            logger.debug(f"Fetching bandwidth usage for VPS {vps_id} ({vps['container_name']})")
            current_bandwidth_usage = run_sync(get_bandwidth_usage(vps['container_name'], vps['node_id']))
            
            if current_bandwidth_usage and current_bandwidth_usage.get('total_gb', 0) > 0:
                new_usage = current_bandwidth_usage['total_gb']
                logger.debug(f"VPS {vps_id} current bandwidth usage: {new_usage} GB")
                
                update_vps(vps_id, bandwidth_used_gb=new_usage)
                vps['bandwidth_used_gb'] = new_usage
                
                logger.debug(f"Updated VPS {vps_id} bandwidth usage to {new_usage} GB")
            else:
                logger.debug(f"VPS {vps_id} bandwidth usage: 0 GB or no data")
                
        except Exception as e:
            logger.error(f"Error getting bandwidth usage for VPS {vps_id}: {e}")
            current_bandwidth_usage = None

    forwards = get_user_forwards(vps['user_id'])
    vps_forwards = [
        f for f in forwards
        if f['vps_container'] == vps['container_name']
    ]

    shared_users = []
    if shared_with:
        logger.info(f"VPS {vps_id} shared_with list: {shared_with}")
        with get_db() as conn:
            cur = conn.cursor()

            for uid in shared_with:
                try:
                    uid_int = int(uid)
                    cur.execute(
                        '''
                        SELECT id, username, email, profile_picture
                        FROM users WHERE id=?
                        ''',
                        (uid_int,)
                    )
                    row = cur.fetchone()
                    if row:
                        user_dict = dict(row)
                        shared_users.append(user_dict)
                        logger.info(f"Added shared user: {user_dict['username']} (ID: {user_dict['id']})")
                    else:
                        logger.warning(f"User ID {uid_int} not found in database")
                except Exception as e:
                    logger.error(f"Error loading shared user {uid}: {e}")
    
    logger.debug(f"VPS {vps_id} total shared_users: {len(shared_users)}")

    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            '''
            SELECT * FROM backups
            WHERE vps_id=?
            ORDER BY created_at DESC
            ''',
            (vps_id,)
        )
        backups = [dict(r) for r in cur.fetchall()]

    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            '''
            SELECT * FROM activity_logs
            WHERE resource_type='vps'
            AND resource_id=?
            ORDER BY created_at DESC
            LIMIT 20
            ''',
            (str(vps_id),)
        )
        activities = [dict(r) for r in cur.fetchall()]

    display_ip = get_vps_display_ip(vps) or YOUR_SERVER_IP

    os_icon = "default"
    for os_option in OS_OPTIONS:
        if os_option["value"] == vps["os_version"]:
            os_icon = os_option.get("icon", "default")
            break

    return render_template(
        "vps_detail.html",
        panel_name=get_setting('site_name', 'KrysolVM'),
        vps=vps,
        node=node,
        stats=stats,
        forwards=vps_forwards,
        shared_users=shared_users,
        os_options=OS_OPTIONS,
        backups=backups,
        activities=activities,
        private_ip=private_ip,
        display_ip=display_ip,
        YOUR_SERVER_IP=YOUR_SERVER_IP,
        os_icon=os_icon,
        current_bandwidth_usage=current_bandwidth_usage,
        socketio_available=SOCKETIO_AVAILABLE
    )

# ============================================================================
# VPS File Manager Routes - SFTP Based
# ============================================================================

def get_sftp_connection(vps):
    import paramiko

    if not vps:
        raise Exception("VPS data is None")
    
    if 'node_id' not in vps or vps['node_id'] is None:
        raise Exception("VPS node_id is missing or None")
    
    if 'container_name' not in vps or not vps['container_name']:
        raise Exception("VPS container_name is missing or empty")

    vps_id = vps.get('id')
    if not vps_id:
        raise Exception("VPS ID is missing")
    
    password = get_vps_password(vps_id)
    logger.info(f"Using stored password for SFTP connection to VPS {vps_id} (length: {len(password)} chars)")

    node = get_node(vps['node_id'])
    if not node:
        raise Exception(f"Node {vps['node_id']} not found")

    node_url = node.get('url')
    if not node_url:
        raise Exception(f"Node {vps['node_id']} URL not configured")
    
    from urllib.parse import urlparse
    parsed = urlparse(node_url)
    
    if parsed.hostname:
        node_host = parsed.hostname
    elif '://' in node_url:
        node_host = node_url.split('://')[1].split(':')[0]
    else:
        node_host = node_url.split(':')[0]
    
    if not node_host:
        raise Exception(f"Could not extract hostname from node URL: {node_url}")

    try:
        private_ip = run_sync(get_container_private_ip(vps['container_name'], vps['node_id']))
        if private_ip and private_ip != "N/A":
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            try:
                ssh.connect(
                    hostname=private_ip,
                    port=22,
                    username='root',
                    password=password,
                    timeout=5,
                    allow_agent=False,
                    look_for_keys=False
                )
                sftp = ssh.open_sftp()
                logger.info(f"SFTP connected directly to {private_ip}:22")
                return ssh, sftp
            except Exception as e:
                logger.debug(f"Direct IP connection failed: {e}, trying port forward")
                ssh.close()
    except Exception as e:
        logger.debug(f"Could not get private IP: {e}")

    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('''
            SELECT host_port FROM port_forwards
            WHERE vps_container = ? AND vps_port = 22
        ''', (vps['container_name'],))
        row = cur.fetchone()

        if row:
            ssh_port = row['host_port']
        else:
            logger.info(f"Auto-creating SSH port forward for {vps['container_name']}")
            try:
                host_port = run_sync(create_port_forward(
                    user_id=vps['user_id'],
                    container=vps['container_name'],
                    vps_port=22,
                    node_id=vps['node_id'],
                    protocol='tcp',
                    description='SSH (auto-created for file manager)'
                ))
                
                if not host_port:
                    raise Exception("No available ports for SSH forward")
                
                ssh_port = host_port
                logger.info(f"Created SSH forward: {node_host}:{ssh_port} -> {vps['container_name']}:22")
            except Exception as e:
                raise Exception(f"Could not create SSH port forward: {str(e)}")

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        ssh.connect(
            hostname=node_host,
            port=ssh_port,
            username='root',
            password=password,
            timeout=10,
            allow_agent=False,
            look_for_keys=False
        )

        sftp = ssh.open_sftp()
        logger.info(f"SFTP connected via port forward {node_host}:{ssh_port}")
        return ssh, sftp
    except Exception as e:
        ssh.close()
        raise Exception(f"SFTP connection failed: {str(e)}")

@app.route('/vps/<int:vps_id>/files')
@login_required
def vps_files(vps_id):
    vps = get_vps_by_id(vps_id)
    if not vps:
        flash('VPS not found', 'danger')
        return redirect(url_for('dashboard'))
    
    if vps['user_id'] != current_user.id and not current_user.is_admin:
        flash('Access denied', 'danger')
        return redirect(url_for('dashboard'))
    
    if is_vps_suspended(vps):
        return redirect(url_for('vps_suspended_page', vps_id=vps_id))
    
    status = run_sync(get_container_status(vps['container_name'], vps['node_id']))
    
    return render_template('vps_files.html',
                          panel_name=get_setting('site_name', 'KrysolVM'),
                          vps=vps,
                          status=status)

@app.route('/vps/<int:vps_id>/files/browse', methods=['POST'])
@login_required
def vps_files_browse(vps_id):
    vps = get_vps_by_id(vps_id)
    if not vps or (vps['user_id'] != current_user.id and not current_user.is_admin):
        return jsonify({'success': False, 'error': 'Access denied'}), 403
    
    if is_vps_suspended(vps) and not current_user.is_admin:
        return jsonify({'success': False, 'error': 'VPS is suspended'}), 403
    
    data = request.get_json()
    if not data:
        return jsonify({'success': False, 'error': 'No data provided'}), 400
    
    path = data.get('path', '/root')
    
    if not path or '..' in path:
        return jsonify({'success': False, 'error': 'Invalid path'}), 400
    
    ssh = None
    sftp = None
    try:
        ssh, sftp = get_sftp_connection(vps)
        
        files = []
        for item in sftp.listdir_attr(path):
            import stat
            is_dir = stat.S_ISDIR(item.st_mode)
            is_link = stat.S_ISLNK(item.st_mode)
            
            if is_dir:
                size = '-'
            else:
                size_bytes = item.st_size
                if size_bytes < 1024:
                    size = f"{size_bytes}B"
                elif size_bytes < 1024 * 1024:
                    size = f"{size_bytes / 1024:.1f}K"
                elif size_bytes < 1024 * 1024 * 1024:
                    size = f"{size_bytes / (1024 * 1024):.1f}M"
                else:
                    size = f"{size_bytes / (1024 * 1024 * 1024):.1f}G"
            
            perms = stat.filemode(item.st_mode)
            
            from datetime import datetime
            modified = datetime.fromtimestamp(item.st_mtime).strftime('%Y-%m-%d %H:%M')
            
            files.append({
                'name': item.filename,
                'size': size,
                'modified': modified,
                'permissions': perms,
                'owner': f"{item.st_uid}:{item.st_gid}",
                'is_dir': is_dir,
                'is_link': is_link
            })
        
        files.sort(key=lambda x: (not x['is_dir'], x['name'].lower()))
        
        return jsonify({
            'success': True,
            'path': path,
            'files': files
        })
        
    except Exception as e:
        logger.error(f"Error browsing files: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        if sftp:
            sftp.close()
        if ssh:
            ssh.close()

@app.route('/vps/<int:vps_id>/files/upload', methods=['POST'])
@login_required
def vps_files_upload(vps_id):
    vps = get_vps_by_id(vps_id)
    if not vps or (vps['user_id'] != current_user.id and not current_user.is_admin):
        return jsonify({'success': False, 'error': 'Access denied'}), 403
    
    if is_vps_suspended(vps) and not current_user.is_admin:
        return jsonify({'success': False, 'error': 'VPS is suspended'}), 403
    
    if 'file' not in request.files:
        return jsonify({'success': False, 'error': 'No file provided'}), 400
    
    file = request.files['file']
    path = request.form.get('path', '/root')
    
    if file.filename == '':
        return jsonify({'success': False, 'error': 'No file selected'}), 400
    
    if '..' in path or '..' in file.filename:
        return jsonify({'success': False, 'error': 'Invalid path'}), 400
    
    ssh = None
    sftp = None
    tmp_path = None
    try:
        import tempfile
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            file.save(tmp.name)
            tmp_path = tmp.name
        
        ssh, sftp = get_sftp_connection(vps)
        
        target_path = f"{path.rstrip('/')}/{file.filename}"
        sftp.put(tmp_path, target_path)
        
        log_activity(current_user.id, 'upload_file', 'vps', str(vps_id),
                    {'file': file.filename, 'path': path})
        
        return jsonify({
            'success': True,
            'message': f'File {file.filename} uploaded successfully'
        })
        
    except Exception as e:
        logger.error(f"Error uploading file: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)
        if sftp:
            sftp.close()
        if ssh:
            ssh.close()

@app.route('/vps/<int:vps_id>/files/download', methods=['POST'])
@login_required  
def vps_files_download(vps_id):
    vps = get_vps_by_id(vps_id)
    if not vps or (vps['user_id'] != current_user.id and not current_user.is_admin):
        return jsonify({'success': False, 'error': 'Access denied'}), 403
    
    if is_vps_suspended(vps) and not current_user.is_admin:
        return jsonify({'success': False, 'error': 'VPS is suspended'}), 403
    
    data = request.get_json()
    file_path = data.get('path')
    
    if not file_path or '..' in file_path:
        return jsonify({'success': False, 'error': 'Invalid path'}), 400
    
    ssh = None
    sftp = None
    tmp_path = None
    try:
        import tempfile
        
        ssh, sftp = get_sftp_connection(vps)
        
        tmp = tempfile.NamedTemporaryFile(delete=False)
        tmp.close()
        tmp_path = tmp.name
        
        sftp.get(file_path, tmp_path)
        
        filename = os.path.basename(file_path)
        
        log_activity(current_user.id, 'download_file', 'vps', str(vps_id),
                    {'file': file_path})
        
        return send_file(tmp_path, as_attachment=True, download_name=filename)
        
    except Exception as e:
        logger.error(f"Error downloading file: {e}")
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        if sftp:
            sftp.close()
        if ssh:
            ssh.close()

@app.route('/vps/<int:vps_id>/files/delete', methods=['POST'])
@login_required
def vps_files_delete(vps_id):
    vps = get_vps_by_id(vps_id)
    if not vps or (vps['user_id'] != current_user.id and not current_user.is_admin):
        return jsonify({'success': False, 'error': 'Access denied'}), 403
    
    if is_vps_suspended(vps) and not current_user.is_admin:
        return jsonify({'success': False, 'error': 'VPS is suspended'}), 403
    
    data = request.get_json()
    path = data.get('path')
    is_dir = data.get('is_dir', False)
    
    if not path or '..' in path:
        return jsonify({'success': False, 'error': 'Invalid path'}), 400
    
    critical_paths = ['/', '/bin', '/boot', '/dev', '/etc', '/lib', '/proc', '/root', '/sbin', '/sys', '/usr', '/var']
    if path in critical_paths:
        return jsonify({'success': False, 'error': 'Cannot delete system directory'}), 400
    
    ssh = None
    sftp = None
    try:
        ssh, sftp = get_sftp_connection(vps)
        
        if is_dir:
            def rmdir_recursive(sftp, path):
                for item in sftp.listdir_attr(path):
                    item_path = f"{path}/{item.filename}"
                    import stat
                    if stat.S_ISDIR(item.st_mode):
                        rmdir_recursive(sftp, item_path)
                    else:
                        sftp.remove(item_path)
                sftp.rmdir(path)
            
            rmdir_recursive(sftp, path)
        else:
            sftp.remove(path)
        
        log_activity(current_user.id, 'delete_file', 'vps', str(vps_id),
                    {'path': path, 'is_dir': is_dir})
        
        return jsonify({
            'success': True,
            'message': f'{"Directory" if is_dir else "File"} deleted successfully'
        })
        
    except Exception as e:
        logger.error(f"Error deleting file: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        if sftp:
            sftp.close()
        if ssh:
            ssh.close()

@app.route('/vps/<int:vps_id>/files/rename', methods=['POST'])
@login_required
def vps_files_rename(vps_id):
    vps = get_vps_by_id(vps_id)
    if not vps or (vps['user_id'] != current_user.id and not current_user.is_admin):
        return jsonify({'success': False, 'error': 'Access denied'}), 403
    
    if is_vps_suspended(vps) and not current_user.is_admin:
        return jsonify({'success': False, 'error': 'VPS is suspended'}), 403
    
    data = request.get_json()
    old_path = data.get('old_path')
    new_name = data.get('new_name')
    
    if not old_path or not new_name or '..' in old_path or '..' in new_name or '/' in new_name:
        return jsonify({'success': False, 'error': 'Invalid path or name'}), 400
    
    ssh = None
    sftp = None
    try:
        ssh, sftp = get_sftp_connection(vps)
        
        directory = os.path.dirname(old_path)
        new_path = f"{directory}/{new_name}" if directory != '/' else f"/{new_name}"
        
        sftp.rename(old_path, new_path)
        
        log_activity(current_user.id, 'rename_file', 'vps', str(vps_id),
                    {'old': old_path, 'new': new_path})
        
        return jsonify({
            'success': True,
            'message': 'Renamed successfully'
        })
        
    except Exception as e:
        logger.error(f"Error renaming file: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        if sftp:
            sftp.close()
        if ssh:
            ssh.close()

@app.route('/vps/<int:vps_id>/files/mkdir', methods=['POST'])
@login_required
def vps_files_mkdir(vps_id):
    vps = get_vps_by_id(vps_id)
    if not vps or (vps['user_id'] != current_user.id and not current_user.is_admin):
        return jsonify({'success': False, 'error': 'Access denied'}), 403
    
    if is_vps_suspended(vps) and not current_user.is_admin:
        return jsonify({'success': False, 'error': 'VPS is suspended'}), 403
    
    data = request.get_json()
    path = data.get('path')
    name = data.get('name')
    
    if not path or not name or '..' in path or '..' in name or '/' in name:
        return jsonify({'success': False, 'error': 'Invalid path or name'}), 400
    
    ssh = None
    sftp = None
    try:
        ssh, sftp = get_sftp_connection(vps)
        
        new_dir = f"{path.rstrip('/')}/{name}"
        sftp.mkdir(new_dir)
        
        log_activity(current_user.id, 'create_directory', 'vps', str(vps_id),
                    {'path': new_dir})
        
        return jsonify({
            'success': True,
            'message': f'Directory {name} created successfully'
        })
        
    except Exception as e:
        logger.error(f"Error creating directory: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        if sftp:
            sftp.close()
        if ssh:
            ssh.close()

@app.route('/vps/<int:vps_id>/files/read', methods=['POST'])
@login_required
def vps_files_read(vps_id):
    vps = get_vps_by_id(vps_id)
    if not vps or (vps['user_id'] != current_user.id and not current_user.is_admin):
        return jsonify({'success': False, 'error': 'Access denied'}), 403
    
    if is_vps_suspended(vps) and not current_user.is_admin:
        return jsonify({'success': False, 'error': 'VPS is suspended'}), 403
    
    data = request.get_json()
    file_path = data.get('path')
    
    if not file_path or '..' in file_path:
        return jsonify({'success': False, 'error': 'Invalid path'}), 400
    
    ssh = None
    sftp = None
    try:
        ssh, sftp = get_sftp_connection(vps)
        
        file_stat = sftp.stat(file_path)
        if file_stat.st_size > 5 * 1024 * 1024:
            return jsonify({'success': False, 'error': 'File too large to edit (max 5MB)'}), 400
        
        with sftp.open(file_path, 'r') as f:
            content = f.read().decode('utf-8', errors='replace')
        
        return jsonify({
            'success': True,
            'content': content,
            'size': file_stat.st_size
        })
        
    except Exception as e:
        logger.error(f"Error reading file: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        if sftp:
            sftp.close()
        if ssh:
            ssh.close()

@app.route('/vps/<int:vps_id>/files/write', methods=['POST'])
@login_required
def vps_files_write(vps_id):
    vps = get_vps_by_id(vps_id)
    if not vps or (vps['user_id'] != current_user.id and not current_user.is_admin):
        return jsonify({'success': False, 'error': 'Access denied'}), 403
    
    if is_vps_suspended(vps) and not current_user.is_admin:
        return jsonify({'success': False, 'error': 'VPS is suspended'}), 403
    
    data = request.get_json()
    file_path = data.get('path')
    content = data.get('content', '')
    
    if not file_path or '..' in file_path:
        return jsonify({'success': False, 'error': 'Invalid path'}), 400
    
    ssh = None
    sftp = None
    try:
        ssh, sftp = get_sftp_connection(vps)
        
        with sftp.open(file_path, 'w') as f:
            f.write(content.encode('utf-8'))
        
        log_activity(current_user.id, 'edit_file', 'vps', str(vps_id),
                    {'file': file_path})
        
        return jsonify({
            'success': True,
            'message': 'File saved successfully'
        })
        
    except Exception as e:
        logger.error(f"Error writing file: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        if sftp:
            sftp.close()
        if ssh:
            ssh.close()

@app.route('/vps/<int:vps_id>/files/move', methods=['POST'])
@login_required
def vps_files_move(vps_id):
    vps = get_vps_by_id(vps_id)
    if not vps or (vps['user_id'] != current_user.id and not current_user.is_admin):
        return jsonify({'success': False, 'error': 'Access denied'}), 403
    
    if is_vps_suspended(vps) and not current_user.is_admin:
        return jsonify({'success': False, 'error': 'VPS is suspended'}), 403
    
    data = request.get_json()
    source = data.get('source')
    destination = data.get('destination')
    copy = data.get('copy', False)
    
    if not source or not destination or '..' in source or '..' in destination:
        return jsonify({'success': False, 'error': 'Invalid path'}), 400
    
    ssh = None
    sftp = None
    try:
        ssh, sftp = get_sftp_connection(vps)
        
        source_name = os.path.basename(source)
        target_path = f"{destination.rstrip('/')}/{source_name}"
        
        if copy:
            stdin, stdout, stderr = ssh.exec_command(f"cp -r '{source}' '{target_path}'")
            exit_code = stdout.channel.recv_exit_status()
            if exit_code != 0:
                error = stderr.read().decode()
                raise Exception(f"Copy failed: {error}")
            action = 'copied'
        else:
            sftp.rename(source, target_path)
            action = 'moved'
        
        log_activity(current_user.id, 'move_file' if not copy else 'copy_file', 'vps', str(vps_id),
                    {'source': source, 'destination': target_path})
        
        return jsonify({
            'success': True,
            'message': f'Successfully {action}'
        })
        
    except Exception as e:
        logger.error(f"Error moving/copying file: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        if sftp:
            sftp.close()
        if ssh:
            ssh.close()

@app.route('/vps/<int:vps_id>/files/chmod', methods=['POST'])
@login_required
def vps_files_chmod(vps_id):
    vps = get_vps_by_id(vps_id)
    if not vps or (vps['user_id'] != current_user.id and not current_user.is_admin):
        return jsonify({'success': False, 'error': 'Access denied'}), 403
    
    if is_vps_suspended(vps) and not current_user.is_admin:
        return jsonify({'success': False, 'error': 'VPS is suspended'}), 403
    
    data = request.get_json()
    file_path = data.get('path')
    permissions = data.get('permissions')
    
    if not file_path or not permissions or '..' in file_path:
        return jsonify({'success': False, 'error': 'Invalid path or permissions'}), 400
    
    ssh = None
    sftp = None
    try:
        ssh, sftp = get_sftp_connection(vps)
        
        mode = int(permissions, 8)
        sftp.chmod(file_path, mode)
        
        log_activity(current_user.id, 'chmod_file', 'vps', str(vps_id),
                    {'file': file_path, 'permissions': permissions})
        
        return jsonify({
            'success': True,
            'message': 'Permissions updated successfully'
        })
        
    except Exception as e:
        logger.error(f"Error changing permissions: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        if sftp:
            sftp.close()
        if ssh:
            ssh.close()

@app.route('/vps/<int:vps_id>/files/compress', methods=['POST'])
@login_required
def vps_files_compress(vps_id):
    vps = get_vps_by_id(vps_id)
    if not vps or (vps['user_id'] != current_user.id and not current_user.is_admin):
        return jsonify({'success': False, 'error': 'Access denied'}), 403
    
    if is_vps_suspended(vps) and not current_user.is_admin:
        return jsonify({'success': False, 'error': 'VPS is suspended'}), 403
    
    data = request.get_json()
    paths = data.get('paths', [])
    archive_name = data.get('name', 'archive.tar.gz')
    current_dir = data.get('current_dir', '/root')
    
    if not paths or any('..' in p for p in paths):
        return jsonify({'success': False, 'error': 'Invalid paths'}), 400
    
    ssh = None
    try:
        ssh, _ = get_sftp_connection(vps)
        
        files_str = ' '.join([f"'{os.path.basename(p)}'" for p in paths])
        archive_path = f"{current_dir.rstrip('/')}/{archive_name}"
        
        cmd = f"cd '{current_dir}' && tar -czf '{archive_name}' {files_str}"
        stdin, stdout, stderr = ssh.exec_command(cmd)
        exit_code = stdout.channel.recv_exit_status()
        
        if exit_code != 0:
            error = stderr.read().decode()
            raise Exception(f"Compression failed: {error}")
        
        log_activity(current_user.id, 'compress_files', 'vps', str(vps_id),
                    {'archive': archive_name, 'files': len(paths)})
        
        return jsonify({
            'success': True,
            'message': f'Archive {archive_name} created successfully'
        })
        
    except Exception as e:
        logger.error(f"Error compressing files: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        if ssh:
            ssh.close()

@app.route('/vps/<int:vps_id>/files/extract', methods=['POST'])
@login_required
def vps_files_extract(vps_id):
    vps = get_vps_by_id(vps_id)
    if not vps or (vps['user_id'] != current_user.id and not current_user.is_admin):
        return jsonify({'success': False, 'error': 'Access denied'}), 403
    
    if is_vps_suspended(vps) and not current_user.is_admin:
        return jsonify({'success': False, 'error': 'VPS is suspended'}), 403
    
    data = request.get_json()
    archive_path = data.get('path')
    
    if not archive_path or '..' in archive_path:
        return jsonify({'success': False, 'error': 'Invalid path'}), 400
    
    ssh = None
    try:
        ssh, _ = get_sftp_connection(vps)
        
        if archive_path.endswith('.tar.gz') or archive_path.endswith('.tgz'):
            cmd = f"tar -xzf '{archive_path}' -C '{os.path.dirname(archive_path)}'"
        elif archive_path.endswith('.tar'):
            cmd = f"tar -xf '{archive_path}' -C '{os.path.dirname(archive_path)}'"
        elif archive_path.endswith('.zip'):
            cmd = f"unzip -o '{archive_path}' -d '{os.path.dirname(archive_path)}'"
        else:
            return jsonify({'success': False, 'error': 'Unsupported archive format'}), 400
        
        stdin, stdout, stderr = ssh.exec_command(cmd)
        exit_code = stdout.channel.recv_exit_status()
        
        if exit_code != 0:
            error = stderr.read().decode()
            raise Exception(f"Extraction failed: {error}")
        
        log_activity(current_user.id, 'extract_archive', 'vps', str(vps_id),
                    {'archive': archive_path})
        
        return jsonify({
            'success': True,
            'message': 'Archive extracted successfully'
        })
        
    except Exception as e:
        logger.error(f"Error extracting archive: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        if ssh:
            ssh.close()

@app.route('/vps/<int:vps_id>/files/search', methods=['POST'])
@login_required
def vps_files_search(vps_id):
    vps = get_vps_by_id(vps_id)
    if not vps or (vps['user_id'] != current_user.id and not current_user.is_admin):
        return jsonify({'success': False, 'error': 'Access denied'}), 403
    
    if is_vps_suspended(vps) and not current_user.is_admin:
        return jsonify({'success': False, 'error': 'VPS is suspended'}), 403
    
    data = request.get_json()
    query = data.get('query', '').strip()
    search_path = data.get('path', '/root')
    
    if not query or '..' in search_path:
        return jsonify({'success': False, 'error': 'Invalid search query'}), 400
    
    ssh = None
    try:
        ssh, _ = get_sftp_connection(vps)
        
        cmd = f"find '{search_path}' -iname '*{query}*' -type f -o -iname '*{query}*' -type d | head -100"
        stdin, stdout, stderr = ssh.exec_command(cmd, timeout=10)
        
        results = []
        for line in stdout:
            path = line.strip()
            if path:
                results.append({
                    'path': path,
                    'name': os.path.basename(path),
                    'directory': os.path.dirname(path)
                })
        
        return jsonify({
            'success': True,
            'results': results,
            'count': len(results)
        })
        
    except Exception as e:
        logger.error(f"Error searching files: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        if ssh:
            ssh.close()

# ============================================================================
# VPS Control Routes
# ============================================================================
@app.route('/vps/<int:vps_id>/control/<action>', methods=['POST'])
@login_required
def vps_control(vps_id, action):
    vps = get_vps_by_id(vps_id)
    if not vps or (vps['user_id'] != current_user.id and not current_user.is_admin):
        return jsonify({'success': False, 'error': 'Access denied'}), 403
    
    if is_vps_suspended(vps) and not current_user.is_admin:
        return jsonify({'success': False, 'error': 'VPS is suspended'}), 403
    
    actions = ['start', 'stop', 'restart', 'freeze', 'unfreeze']
    if action not in actions:
        return jsonify({'success': False, 'error': 'Invalid action'}), 400
    
    try:
        if action == 'start':
            run_sync(execute_lxc(vps['container_name'], f"start {vps['container_name']}", node_id=vps['node_id'], operation_type="start"))
            run_sync(apply_internal_permissions(vps['container_name'], vps['node_id']))
            run_sync(recreate_port_forwards(vps['container_name']))
            
            if vps.get('bandwidth_quota_gb', 0) > 0:
                try:
                    run_sync(configure_bandwidth_quota(
                        vps['container_name'], 
                        vps.get('bandwidth_quota_gb', 0), 
                        vps['node_id']
                    ))
                    logger.info(f"Applied bandwidth quota monitoring to {vps['container_name']} on start")
                except Exception as e:
                    logger.error(f"Failed to apply bandwidth quota to {vps['container_name']} on start: {e}")
            
            update_vps(vps_id, status='running', last_started=datetime.now().isoformat())
            log_activity(current_user.id, 'start_vps', 'vps', str(vps_id))
            create_notification(current_user.id, 'success', 'VPS Started', f'VPS {vps["container_name"]} has been started.')
            
            if socketio:
                socketio.emit('vps_status_change', {
                    'vps_id': vps_id,
                    'status': 'running'
                }, room=f'vps_{vps_id}')
                
        elif action == 'stop':
            run_sync(execute_lxc(vps['container_name'], f"stop {vps['container_name']}", node_id=vps['node_id'], operation_type="general"))
            update_vps(vps_id, status='stopped', last_stopped=datetime.now().isoformat())
            log_activity(current_user.id, 'stop_vps', 'vps', str(vps_id))
            create_notification(current_user.id, 'info', 'VPS Stopped', f'VPS {vps["container_name"]} has been stopped.')
            
            if socketio:
                socketio.emit('vps_status_change', {
                    'vps_id': vps_id,
                    'status': 'stopped'
                }, room=f'vps_{vps_id}')
                
        elif action == 'restart':
            run_sync(execute_lxc(vps['container_name'], f"restart {vps['container_name']}", node_id=vps['node_id'], operation_type="start"))
            run_sync(apply_internal_permissions(vps['container_name'], vps['node_id']))
            run_sync(recreate_port_forwards(vps['container_name']))
            
            if vps.get('bandwidth_quota_gb', 0) > 0:
                try:
                    run_sync(configure_bandwidth_quota(
                        vps['container_name'], 
                        vps.get('bandwidth_quota_gb', 0), 
                        vps['node_id']
                    ))
                    logger.info(f"Applied bandwidth quota monitoring to {vps['container_name']} on restart")
                except Exception as e:
                    logger.error(f"Failed to apply bandwidth quota to {vps['container_name']} on restart: {e}")
            
            update_vps(vps_id, status='running', last_started=datetime.now().isoformat())
            log_activity(current_user.id, 'restart_vps', 'vps', str(vps_id))
            create_notification(current_user.id, 'success', 'VPS Restarted', f'VPS {vps["container_name"]} has been restarted.')
            
            if socketio:
                socketio.emit('vps_status_change', {
                    'vps_id': vps_id,
                    'status': 'running'
                }, room=f'vps_{vps_id}')
                
        elif action == 'freeze':
            run_sync(execute_lxc(vps['container_name'], f"freeze {vps['container_name']}", node_id=vps['node_id']))
            update_vps(vps_id, status='frozen')
            log_activity(current_user.id, 'freeze_vps', 'vps', str(vps_id))
            create_notification(current_user.id, 'warning', 'VPS Frozen', f'VPS {vps["container_name"]} has been frozen.')
            
            if socketio:
                socketio.emit('vps_status_change', {
                    'vps_id': vps_id,
                    'status': 'frozen'
                }, room=f'vps_{vps_id}')
                
        elif action == 'unfreeze':
            run_sync(execute_lxc(vps['container_name'], f"unfreeze {vps['container_name']}", node_id=vps['node_id']))
            update_vps(vps_id, status='running')
            log_activity(current_user.id, 'unfreeze_vps', 'vps', str(vps_id))
            create_notification(current_user.id, 'success', 'VPS Unfrozen', f'VPS {vps["container_name"]} has been unfrozen.')
            
            if socketio:
                socketio.emit('vps_status_change', {
                    'vps_id': vps_id,
                    'status': 'running'
                }, room=f'vps_{vps_id}')
        
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"VPS control error: {e}")
        create_notification(current_user.id, 'error', 'Action Failed', f'Failed to {action} VPS: {str(e)}')
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================================================
# Console Routes - FIXED VERSION
# ============================================================================
@app.route('/vps/<int:vps_id>/console')
@login_required
def vps_console(vps_id):
    vps = get_vps_by_id(vps_id)
    if not vps or (vps['user_id'] != current_user.id and not current_user.is_admin):
        flash('Access denied', 'danger')
        return redirect(url_for('vps_list'))
    
    if is_vps_suspended(vps):
        return redirect(url_for('vps_suspended_page', vps_id=vps_id))

    vps_ip = vps.get('ip_address', '')
    
    node = get_node(vps['node_id'])
    
    return render_template(
        'console.html',
        panel_name=get_setting('site_name', 'KrysolVM'),
        vps=vps,
        vps_ip=vps_ip,
        node=node,
        ssh_available=SSH_AVAILABLE
    )

@app.route('/vps/<int:vps_id>/console/connect', methods=['POST'])
@login_required
def vps_console_connect(vps_id):
    vps = get_vps_by_id(vps_id)
    if not vps or (vps['user_id'] != current_user.id and not current_user.is_admin):
        return jsonify({'success': False, 'error': 'Access denied'}), 403

    if is_vps_suspended(vps) and not current_user.is_admin:
        return jsonify({'success': False, 'error': 'VPS is suspended'}), 403

    try:
        node = get_node(vps['node_id'])
        if not node:
            return jsonify({'success': False, 'error': 'Node not found'}), 404

        from urllib.parse import urlparse
        parsed = urlparse(node['url'])
        node_host = parsed.hostname or node['url'].split('://')[1].split(':')[0] if '://' in node['url'] else node['url'].split(':')[0]

        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('''
                SELECT pf.host_port, pf.protocol, pf.id
                FROM port_forwards pf
                INNER JOIN vps v ON pf.vps_container = v.container_name
                WHERE pf.vps_container = ?
                  AND pf.vps_port = 22
                  AND v.id = ?
                ORDER BY pf.created_at DESC
                LIMIT 1
            ''', (vps['container_name'], vps_id))
            row = cur.fetchone()

        ssh_port = None
        port_created = False

        if row:
            ssh_port = row['host_port']
            protocol = row['protocol']
            logger.info(f"Using existing SSH port forward (ID: {row['id']}): {node_host}:{ssh_port} -> {vps['container_name']}:22 (protocol: {protocol})")
        else:
            logger.info(f"No SSH port forward found for {vps['container_name']} (VPS ID: {vps_id}), creating new one")
            try:
                host_port = run_sync(create_port_forward(
                    user_id=vps['user_id'],
                    container=vps['container_name'],
                    vps_port=22,
                    node_id=vps['node_id'],
                    protocol='tcp,udp',
                    description='SSH (auto-created for console)'
                ))

                if not host_port:
                    return jsonify({'success': False, 'error': 'No available ports for SSH forward'}), 500

                ssh_port = host_port
                port_created = True
                logger.info(f"Created SSH forward: {node_host}:{ssh_port} -> {vps['container_name']}:22")
            except Exception as e:
                logger.error(f"Failed to create SSH port forward: {e}")
                return jsonify({'success': False, 'error': f'Could not create SSH port forward: {str(e)}'}), 500

        password = get_vps_password(vps_id)
        password_source = "database"
        logger.info(f"Retrieved password for VPS {vps_id} from database (length: {len(password)} chars)")

        private_ip = "N/A"
        status = run_sync(get_container_status(vps['container_name'], vps['node_id']))
        if status == 'running':
            try:
                private_ip = run_sync(get_container_private_ip(vps['container_name'], vps['node_id']))
            except Exception as e:
                logger.debug(f"Could not get private IP: {e}")

        log_activity(current_user.id, 'console_connect', 'vps', str(vps_id))

        return jsonify({
            'success': True,
            'connection': {
                'host': node_host,
                'port': ssh_port,
                'username': 'root',
                'password': password,
                'private_ip': private_ip,
                'container_name': vps['container_name'],
                'vps_status': status
            },
            'port_created': port_created,
            'ssh_command': f"ssh root@{node_host} -p {ssh_port}",
            'message': 'SSH connection ready' + (' (new port forward created)' if port_created else ''),
            'is_default_password': password == "root",
            'password_source': password_source
        })

    except Exception as e:
        logger.error(f"Console connect error for VPS {vps_id}: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/vps/<int:vps_id>/console/password', methods=['GET'])
@login_required
def vps_console_password(vps_id):
    vps = get_vps_by_id(vps_id)
    if not vps or (vps['user_id'] != current_user.id and not current_user.is_admin):
        return jsonify({'success': False, 'error': 'Access denied'}), 403
    
    if is_vps_suspended(vps) and not current_user.is_admin:
        return jsonify({'success': False, 'error': 'VPS is suspended'}), 403
    
    try:
        password = get_vps_password(vps_id)
        
        logger.info(f"Console password retrieved for VPS {vps_id} (length: {len(password)} chars)")
        
        return jsonify({
            'success': True,
            'password': password,
            'username': 'root',
            'is_default': password == "root"
        })
        
    except Exception as e:
        logger.error(f"Error retrieving password for VPS {vps_id}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

# SocketIO events for SSH console
if socketio and SSH_AVAILABLE:
    @socketio.on('ssh_connect')
    def handle_ssh_connect(data):
        vps_id = data.get('vps_id')
        host = data.get('host', '')
        port = data.get('port', 22)
        username = data.get('username', 'root')
        password = data.get('password', '')
        sid = request.sid

        logger.info(f"SSH connect request for VPS {vps_id} from session {sid} to {username}@{host}:{port}")

        vps = get_vps_by_id(vps_id)
        if not vps:
            logger.error(f"SSH connect failed: VPS {vps_id} not found")
            emit('ssh_error', {'error': 'VPS not found'}, room=sid)
            return

        if vps['user_id'] != current_user.id and not current_user.is_admin:
            logger.error(f"SSH connect failed: Access denied for VPS {vps_id}")
            emit('ssh_error', {'error': 'Access denied'}, room=sid)
            return

        if is_vps_suspended(vps) and not current_user.is_admin:
            logger.error(f"SSH connect failed: VPS {vps_id} is suspended")
            emit('ssh_error', {'error': 'VPS is suspended'}, room=sid)
            return

        if not host or not password:
            logger.error(f"SSH connect failed: Missing host or password")
            emit('ssh_error', {'error': 'Host and password are required'}, room=sid)
            return

        with active_consoles_lock:
            if vps_id in active_consoles:
                old_info = active_consoles[vps_id]
                try:
                    if 'ssh_client' in old_info and old_info['ssh_client']:
                        old_info['ssh_client'].close()
                    if 'channel' in old_info and old_info['channel']:
                        old_info['channel'].close()
                except:
                    pass
                active_consoles.pop(vps_id, None)
                logger.info(f"Closed existing SSH connection for VPS {vps_id}")

            try:
                ssh_client = paramiko.SSHClient()
                ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                
                try:
                    port = int(port)
                except:
                    port = 22
                
                logger.info(f"Connecting to SSH: {username}@{host}:{port}")
                ssh_client.connect(
                    hostname=host,
                    port=port,
                    username=username,
                    password=password,
                    timeout=10,
                    allow_agent=False,
                    look_for_keys=False
                )
                
                channel = ssh_client.invoke_shell(term='xterm', width=80, height=24)
                channel.settimeout(0.1)
                
                logger.info(f"SSH connected for VPS {vps_id}")

                active_consoles[vps_id] = {
                    'ssh_client': ssh_client,
                    'channel': channel,
                    'sid': sid,
                    'host': host,
                    'port': port,
                    'username': username,
                    'user_id': current_user.id
                }

                def reader():
                    try:
                        logger.info(f"SSH reader thread started for VPS {vps_id}")
                        while True:
                            if channel.closed:
                                logger.info(f"SSH channel closed for VPS {vps_id}")
                                break

                            try:
                                if channel.recv_ready():
                                    output = channel.recv(4096)
                                    if not output:
                                        break
                                    socketio.emit('ssh_output', output.decode('utf-8', errors='replace'), room=sid)
                                else:
                                    time.sleep(0.01)
                            except socket.timeout:
                                continue
                            except Exception as e:
                                logger.error(f"SSH read error for VPS {vps_id}: {e}")
                                break

                    except Exception as e:
                        logger.error(f"SSH reader error for VPS {vps_id}: {e}")
                        socketio.emit('ssh_error', {'error': str(e)}, room=sid)
                    finally:
                        logger.info(f"SSH reader thread ending for VPS {vps_id}")
                        try:
                            channel.close()
                            ssh_client.close()
                        except:
                            pass
                        with active_consoles_lock:
                            active_consoles.pop(vps_id, None)
                        socketio.emit('ssh_disconnected', {}, room=sid)

                thread = threading.Thread(target=reader, daemon=True)
                thread.start()

                emit('ssh_connected', {
                    'status': 'connected',
                    'host': host,
                    'port': port,
                    'username': username
                }, room=sid)
                logger.info(f"SSH session established for VPS {vps_id}")

            except paramiko.AuthenticationException:
                logger.error(f"SSH authentication failed for VPS {vps_id}")
                emit('ssh_error', {'error': 'Authentication failed. Invalid username or password.'}, room=sid)
            except paramiko.SSHException as e:
                logger.error(f"SSH connection error for VPS {vps_id}: {e}")
                emit('ssh_error', {'error': f'SSH connection error: {str(e)}'}, room=sid)
            except socket.timeout:
                logger.error(f"SSH connection timeout for VPS {vps_id}")
                emit('ssh_error', {'error': 'Connection timeout. Check if SSH is running on the VPS.'}, room=sid)
            except Exception as e:
                logger.error(f"Failed to start SSH for VPS {vps_id}: {e}", exc_info=True)
                emit('ssh_error', {'error': f'Failed to connect: {str(e)}'}, room=sid)

    @socketio.on('ssh_input')
    def handle_ssh_input(data):
        vps_id = data.get('vps_id')
        input_data = data.get('input', '')

        with active_consoles_lock:
            info = active_consoles.get(vps_id)
            if not info or info.get('sid') != request.sid:
                emit('ssh_error', {'error': 'SSH not connected'}, room=request.sid)
                return

            try:
                channel = info.get('channel')
                if channel and not channel.closed:
                    if isinstance(input_data, str):
                        input_data = input_data.encode('utf-8')
                    channel.send(input_data)
            except Exception as e:
                logger.error(f"SSH input error for VPS {vps_id}: {e}")
                emit('ssh_error', {'error': str(e)}, room=request.sid)

    @socketio.on('ssh_resize')
    def handle_ssh_resize(data):
        vps_id = data.get('vps_id')
        cols = data.get('cols', 80)
        rows = data.get('rows', 24)

        with active_consoles_lock:
            info = active_consoles.get(vps_id)
            if info and info.get('sid') == request.sid:
                try:
                    channel = info.get('channel')
                    if channel and not channel.closed:
                        channel.resize_pty(width=cols, height=rows)
                        logger.debug(f"SSH terminal resized for VPS {vps_id}: {cols}x{rows}")
                except Exception as e:
                    logger.error(f"SSH resize error for VPS {vps_id}: {e}")

    @socketio.on('ssh_disconnect')
    def handle_ssh_disconnect(data):
        vps_id = data.get('vps_id')
        logger.info(f"SSH disconnect request for VPS {vps_id}")
        
        with active_consoles_lock:
            info = active_consoles.pop(vps_id, None)
            if info and info.get('sid') == request.sid:
                try:
                    if 'channel' in info and info['channel']:
                        info['channel'].close()
                    if 'ssh_client' in info and info['ssh_client']:
                        info['ssh_client'].close()
                    logger.info(f"SSH disconnected for VPS {vps_id}")
                except Exception as e:
                    logger.error(f"SSH disconnect error for VPS {vps_id}: {e}")

@app.route('/vps/<int:vps_id>/ssh', methods=['POST'])
@login_required
def vps_ssh(vps_id):
    vps = get_vps_by_id(vps_id)
    if not vps or (vps['user_id'] != current_user.id and not current_user.is_admin):
        return jsonify({'success': False, 'error': 'Access denied'}), 403
    
    if is_vps_suspended(vps) and not current_user.is_admin:
        return jsonify({'success': False, 'error': 'VPS is suspended'}), 403
    
    try:
        container_name = vps['container_name']
        node_id = vps['node_id']
        session_name = f"krysolvm-session-{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        try:
            run_sync(execute_lxc(container_name, f"exec {container_name} -- which tmate", node_id=node_id, operation_type="config"))
        except:
            try:
                os_check = run_sync(execute_lxc(container_name, f"exec {container_name} -- cat /etc/os-release", node_id=node_id, operation_type="config"))
                if 'alpine' in os_check.lower():
                    run_sync(execute_lxc(container_name, f"exec {container_name} -- apk add tmate", node_id=node_id, operation_type="config"))
                else:
                    run_sync(execute_lxc(container_name, f"exec {container_name} -- apt-get update -y", node_id=node_id, operation_type="config"))
                    run_sync(execute_lxc(container_name, f"exec {container_name} -- apt-get install -y tmate", node_id=node_id, operation_type="config"))
            except Exception as e:
                return jsonify({'success': False, 'error': f'Could not install tmate: {str(e)}'}), 500
        
        run_sync(execute_lxc(container_name, f"exec {container_name} -- tmate -S /tmp/{session_name}.sock new-session -d", node_id=node_id, operation_type="config"))
        run_sync(asyncio.sleep(3))
        
        ssh_output = run_sync(execute_lxc(container_name, f"exec {container_name} -- tmate -S /tmp/{session_name}.sock display -p '#{{tmate_ssh}}'", node_id=node_id, operation_type="config"))
        ssh_url = ssh_output.strip()
        
        web_output = run_sync(execute_lxc(container_name, f"exec {container_name} -- tmate -S /tmp/{session_name}.sock display -p '#{{tmate_web}}'", node_id=node_id, operation_type="config"))
        web_url = web_output.strip()
        
        if ssh_url:
            log_activity(current_user.id, 'generate_ssh', 'vps', str(vps_id))
            create_notification(current_user.id, 'info', 'SSH Session Created', f'SSH session created for {container_name}')
            return jsonify({
                'success': True,
                'ssh_url': ssh_url,
                'web_url': web_url,
                'session': session_name
            })
        else:
            return jsonify({'success': False, 'error': 'Could not generate SSH URL'}), 500
    except Exception as e:
        logger.error(f"SSH generation error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/vps/<int:vps_id>/stats')
@login_required
def vps_stats(vps_id):
    vps = get_vps_by_id(vps_id)
    if not vps or (vps['user_id'] != current_user.id and not current_user.is_admin):
        return jsonify({'success': False, 'error': 'Access denied'}), 403
    
    try:
        stats = run_sync(get_container_stats(vps['container_name'], vps['node_id']))
        
        if not stats:
            logger.warning(f"No stats returned for VPS {vps_id}")
            return jsonify({
                'success': True, 
                'stats': {
                    'status': vps.get('status', 'stopped').lower(),
                    'cpu': 0.0,
                    'ram': {'used': 0, 'total': 0, 'pct': 0.0},
                    'disk': {'use_percent': '0%'},
                    'uptime': 'Unknown',
                    'processes': 0,
                    'network': {},
                    'load_average': {'1min': 0.0, '5min': 0.0, '15min': 0.0},
                    'private_ip': 'N/A',
                    'connection_issue': True
                }
            })
        
        raw_status = stats.get('status', 'unknown')
        connection_issue = False
        display_status = raw_status
        
        if raw_status and ('_cached' in raw_status or raw_status in ('timeout', 'error', 'unknown', 'server_error', 'circuit_open', 'connection_error')):
            connection_issue = True
            display_status = vps.get('status', 'stopped').lower()
            logger.debug(f"VPS {vps_id} has connection issue: '{raw_status}', using display status: '{display_status}'")
        
        stats['status'] = display_status
        stats['connection_issue'] = connection_issue
        stats['raw_status'] = raw_status
        
        private_ip = "N/A"
        if display_status == 'running' and not connection_issue:
            try:
                private_ip = run_sync(
                    asyncio.wait_for(
                        get_container_private_ip(vps['container_name'], vps['node_id']),
                        timeout=10.0
                    )
                )
            except asyncio.TimeoutError:
                logger.warning(f"Timeout getting private IP for VPS {vps_id}")
                private_ip = "N/A"
            except Exception as e:
                logger.warning(f"Error getting private IP for VPS {vps_id}: {e}")
                private_ip = "N/A"
        elif raw_status.endswith('_cached'):
            cached_stats = get_cached_vps_stats(vps['container_name'])
            if cached_stats and 'private_ip' in cached_stats:
                private_ip = cached_stats['private_ip']
                logger.debug(f"Using cached private IP for VPS {vps_id}: {private_ip}")
        
        stats['private_ip'] = private_ip
        
        if display_status == 'running' and not connection_issue:
            try:
                import threading
                metrics_thread = threading.Thread(
                    target=store_vps_metrics_safe, 
                    args=(vps_id, stats.copy()),
                    daemon=True
                )
                metrics_thread.start()
            except Exception as e:
                logger.warning(f"Error starting metrics storage thread for VPS {vps_id}: {e}")
        
        return jsonify({'success': True, 'stats': stats})
        
    except asyncio.TimeoutError:
        logger.warning(f"Timeout getting stats for VPS {vps_id}")
        return jsonify({
            'success': True, 
            'stats': {
                'status': vps.get('status', 'stopped').lower(),
                'cpu': 0.0,
                'ram': {'used': 0, 'total': 0, 'pct': 0.0},
                'disk': {'use_percent': '0%'},
                'uptime': 'Connection Timeout',
                'processes': 0,
                'network': {},
                'load_average': {'1min': 0.0, '5min': 0.0, '15min': 0.0},
                'private_ip': 'N/A',
                'connection_issue': True,
                'raw_status': 'timeout'
            }
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting stats for VPS {vps_id}: {e}")
        return jsonify({
            'success': True, 
            'stats': {
                'status': vps.get('status', 'stopped').lower(),
                'cpu': 0.0,
                'ram': {'used': 0, 'total': 0, 'pct': 0.0},
                'disk': {'use_percent': '0%'},
                'uptime': 'Error',
                'processes': 0,
                'network': {},
                'load_average': {'1min': 0.0, '5min': 0.0, '15min': 0.0},
                'private_ip': 'N/A',
                'connection_issue': True,
                'raw_status': 'error'
            }
        }), 200

@app.route('/dashboard/stats')
@login_required
def dashboard_stats():
    try:
        vps_list = get_vps_for_user(current_user.id)
        stats_data = {}
        
        for vps in vps_list:
            if is_vps_suspended(vps):
                stats_data[vps['id']] = {
                    'status': 'suspended',
                    'cpu': 0,
                    'ram': {'pct': 0}
                }
                continue
            
            try:
                stats = run_sync(
                    asyncio.wait_for(
                        get_container_stats(vps['container_name'], vps['node_id']),
                        timeout=5.0
                    )
                )
                
                if stats and stats.get('status'):
                    raw_status = stats['status']
                    if raw_status and ('_cached' in raw_status or raw_status in ('timeout', 'error', 'unknown', 'server_error', 'circuit_open', 'connection_error')):
                        display_status = vps.get('status', 'stopped').lower()
                    else:
                        display_status = raw_status.lower()
                    
                    stats_data[vps['id']] = {
                        'status': display_status,
                        'cpu': stats.get('cpu', 0),
                        'ram': stats.get('ram', {'pct': 0})
                    }
                else:
                    stats_data[vps['id']] = {
                        'status': vps.get('status', 'unknown').lower(),
                        'cpu': 0,
                        'ram': {'pct': 0}
                    }
                    
            except asyncio.TimeoutError:
                logger.debug(f"Timeout getting stats for VPS {vps['id']} in bulk request")
                stats_data[vps['id']] = {
                    'status': vps.get('status', 'unknown').lower(),
                    'cpu': 0,
                    'ram': {'pct': 0}
                }
            except Exception as e:
                logger.debug(f"Error getting stats for VPS {vps['id']} in bulk request: {e}")
                stats_data[vps['id']] = {
                    'status': vps.get('status', 'unknown').lower(),
                    'cpu': 0,
                    'ram': {'pct': 0}
                }
        
        return jsonify({'success': True, 'stats': stats_data})
        
    except Exception as e:
        logger.error(f"Error getting bulk dashboard stats: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/vps/<int:vps_id>/metrics/history')
@login_required
def vps_metrics_history(vps_id):
    vps = get_vps_by_id(vps_id)
    if not vps or (vps['user_id'] != current_user.id and not current_user.is_admin):
        return jsonify({'success': False, 'error': 'Access denied'}), 403
    
    try:
        time_range = request.args.get('range', '1h')
        limit = get_limit_for_range(time_range)
        
        metrics = get_vps_metrics_history(vps_id, time_range, limit)
        
        return jsonify({
            'success': True,
            'metrics': metrics,
            'range': time_range,
            'count': len(metrics)
        })
    except Exception as e:
        logger.error(f"Error getting metrics history for VPS {vps_id}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/vps/<int:vps_id>/metrics/live')
@login_required
def vps_metrics_live(vps_id):
    vps = get_vps_by_id(vps_id)
    if not vps or (vps['user_id'] != current_user.id and not current_user.is_admin):
        return jsonify({'success': False, 'error': 'Access denied'}), 403
    
    try:
        if vps['node_id'] and is_node_circuit_open(vps['node_id']):
            logger.info(f"Circuit breaker open for node {vps['node_id']}, returning cached stats for VPS {vps_id}")
            return jsonify({
                'success': True,
                'stats': {
                    'status': 'circuit_open',
                    'cpu': 0.0,
                    'ram': {'used': 0, 'total': 0, 'pct': 0.0},
                    'disk': {'use_percent': '0%'},
                    'uptime': 'Circuit Breaker Open',
                    'processes': 0,
                    'network': {'rx': '0 B', 'tx': '0 B', 'total_rx': 0, 'total_tx': 0},
                    'load_average': {'1min': 0.0, '5min': 0.0, '15min': 0.0},
                    'disk_io': {'read': '0 B', 'write': '0 B', 'read_bytes': 0, 'write_bytes': 0}
                },
                'timestamp': datetime.now().isoformat(),
                'node_health': get_node_health_status(vps['node_id'])
            })
        
        stats = run_sync(
            asyncio.wait_for(
                get_container_stats(vps['container_name'], vps['node_id']),
                timeout=8.0
            )
        )
        
        if not stats:
            stats = {
                'status': 'unknown',
                'cpu': 0.0,
                'ram': {'used': 0, 'total': 0, 'pct': 0.0},
                'disk': {'use_percent': '0%'},
                'uptime': 'Unknown',
                'processes': 0,
                'network': {'rx': '0 B', 'tx': '0 B', 'total_rx': 0, 'total_tx': 0},
                'load_average': {'1min': 0.0, '5min': 0.0, '15min': 0.0},
                'disk_io': {'read': '0 B', 'write': '0 B', 'read_bytes': 0, 'write_bytes': 0}
            }
        
        if stats.get('status') == 'running':
            try:
                network_stats = run_sync(
                    asyncio.wait_for(
                        get_enhanced_network_stats_safe(vps['container_name'], vps['node_id']),
                        timeout=3.0
                    )
                )
                if network_stats:
                    stats['network'] = network_stats
            except (asyncio.TimeoutError, Exception) as e:
                logger.debug(f"Failed to get enhanced network stats for VPS {vps_id}: {e}")
                stats['network'] = {'rx': '0 B', 'tx': '0 B', 'total_rx': 0, 'total_tx': 0}
            
            try:
                disk_io = run_sync(
                    asyncio.wait_for(
                        get_disk_io_stats_safe(vps['container_name'], vps['node_id']),
                        timeout=3.0
                    )
                )
                if disk_io:
                    stats['disk_io'] = disk_io
            except (asyncio.TimeoutError, Exception) as e:
                logger.debug(f"Failed to get disk I/O stats for VPS {vps_id}: {e}")
                stats['disk_io'] = {'read': '0 B', 'write': '0 B', 'read_bytes': 0, 'write_bytes': 0}
            
            try:
                system_info = run_sync(
                    asyncio.wait_for(
                        get_system_info_safe(vps['container_name'], vps['node_id']),
                        timeout=3.0
                    )
                )
                if system_info:
                    stats.update(system_info)
            except (asyncio.TimeoutError, Exception) as e:
                logger.debug(f"Failed to get system info for VPS {vps_id}: {e}")
                if 'processes' not in stats:
                    stats['processes'] = 0
                if 'load_average' not in stats:
                    stats['load_average'] = {'1min': 0.0, '5min': 0.0, '15min': 0.0}
            
            try:
                import threading
                metrics_thread = threading.Thread(
                    target=store_vps_metrics_safe, 
                    args=(vps_id, stats.copy()),
                    daemon=True
                )
                metrics_thread.start()
            except Exception as e:
                logger.debug(f"Error starting metrics storage thread for VPS {vps_id}: {e}")
        else:
            if 'network' not in stats:
                stats['network'] = {'rx': '0 B', 'tx': '0 B', 'total_rx': 0, 'total_tx': 0}
            if 'disk_io' not in stats:
                stats['disk_io'] = {'read': '0 B', 'write': '0 B', 'read_bytes': 0, 'write_bytes': 0}
            if 'processes' not in stats:
                stats['processes'] = 0
            if 'load_average' not in stats:
                stats['load_average'] = {'1min': 0.0, '5min': 0.0, '15min': 0.0}
        
        return jsonify({
            'success': True,
            'stats': stats,
            'timestamp': datetime.now().isoformat(),
            'node_health': get_node_health_status(vps['node_id']) if vps['node_id'] else None
        })
        
    except asyncio.TimeoutError:
        logger.warning(f"Timeout getting live metrics for VPS {vps_id}")
        return jsonify({
            'success': False,
            'error': 'Timeout getting metrics',
            'stats': {
                'status': 'timeout',
                'cpu': 0.0,
                'ram': {'used': 0, 'total': 0, 'pct': 0.0},
                'disk': {'use_percent': '0%'},
                'uptime': 'Timeout',
                'processes': 0,
                'network': {'rx': '0 B', 'tx': '0 B', 'total_rx': 0, 'total_tx': 0},
                'load_average': {'1min': 0.0, '5min': 0.0, '15min': 0.0},
                'disk_io': {'read': '0 B', 'write': '0 B', 'read_bytes': 0, 'write_bytes': 0}
            },
            'timestamp': datetime.now().isoformat(),
            'node_health': get_node_health_status(vps['node_id']) if vps['node_id'] else None
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting live metrics for VPS {vps_id}: {e}")
        return jsonify({
            'success': False,
            'error': 'Failed to get metrics',
            'stats': {
                'status': 'error',
                'cpu': 0.0,
                'ram': {'used': 0, 'total': 0, 'pct': 0.0},
                'disk': {'use_percent': '0%'},
                'uptime': 'Error',
                'processes': 0,
                'network': {'rx': '0 B', 'tx': '0 B', 'total_rx': 0, 'total_tx': 0},
                'load_average': {'1min': 0.0, '5min': 0.0, '15min': 0.0},
                'disk_io': {'read': '0 B', 'write': '0 B', 'read_bytes': 0, 'write_bytes': 0}
            },
            'timestamp': datetime.now().isoformat(),
            'node_health': get_node_health_status(vps['node_id']) if vps['node_id'] else None
        }), 200

@app.route('/vps/<int:vps_id>/ip', methods=['POST'])
@login_required
def vps_change_ip(vps_id):
    vps = get_vps_by_id(vps_id)
    if not vps or (vps['user_id'] != current_user.id and not current_user.is_admin):
        return jsonify({'success': False, 'error': 'Access denied'}), 403
    
    if is_vps_suspended(vps) and not current_user.is_admin:
        return jsonify({'success': False, 'error': 'VPS is suspended'}), 403
    
    data = request.get_json()
    new_ip = data.get('ip_address', '').strip()
    
    if new_ip:
        import ipaddress
        try:
            ipaddress.ip_address(new_ip)
        except ValueError:
            return jsonify({'success': False, 'error': 'Invalid IP address format'}), 400
    
    try:
        old_ip = vps.get('ip_address')
        container_name = vps['container_name']
        node_id = vps['node_id']
        
        status = run_sync(get_container_status(container_name, node_id))
        if status != 'running':
            return jsonify({'success': False, 'error': 'VPS must be running to change IP address'}), 400
        
        if new_ip:
            if old_ip:
                run_sync(update_routed_ip(container_name, old_ip, new_ip, node_id))
                message = f'IP address changed from {old_ip} to {new_ip}'
            else:
                run_sync(configure_routed_ip(container_name, new_ip, node_id))
                message = f'IP address {new_ip} added'
        else:
            if old_ip:
                run_sync(remove_routed_ip(container_name, old_ip, node_id))
                message = f'IP address {old_ip} removed'
            else:
                return jsonify({'success': False, 'error': 'No IP address to change'}), 400
        
        update_vps(vps_id, ip_address=new_ip if new_ip else None)
        
        log_activity(current_user.id, 'change_ip', 'vps', str(vps_id), {
            'old_ip': old_ip,
            'new_ip': new_ip,
            'container': container_name
        })
        
        create_notification(
            vps['user_id'],
            'info',
            'IP Address Changed',
            f'IP address for VPS {container_name} has been updated. {message}'
        )
        
        return jsonify({
            'success': True,
            'message': message,
            'old_ip': old_ip,
            'new_ip': new_ip
        })
        
    except Exception as e:
        logger.error(f"Error changing IP for VPS {vps_id}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/vps/<int:vps_id>/reinstall', methods=['POST'])
@login_required
def vps_reinstall(vps_id):
    try:
        vps = get_vps_by_id(vps_id)
        if not vps:
            return jsonify({'success': False, 'error': 'VPS not found'}), 404
        
        if vps['user_id'] != current_user.id and not current_user.is_admin:
            return jsonify({'success': False, 'error': 'Access denied'}), 403
        
        if is_vps_suspended(vps) and not current_user.is_admin:
            return jsonify({'success': False, 'error': 'VPS is suspended'}), 403
        
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'error': 'No data provided'}), 400
            
        os_version = data.get('os_version')
        
        if not os_version:
            return jsonify({'success': False, 'error': 'OS version is required'}), 400
        
        if os_version not in [o['value'] for o in OS_OPTIONS]:
            return jsonify({'success': False, 'error': 'Invalid OS'}), 400
        
        logger.info(f"Reinstalling VPS {vps_id} ({vps['container_name']}) with OS: {os_version}")
        
        container_name = vps['container_name']
        node_id = vps['node_id']
        
        ram_gb = int(vps['ram'].replace('GB', ''))
        cpu = int(vps['cpu'])
        storage_gb = int(vps['storage'].replace('GB', ''))
        ram_mb = ram_gb * 1024
        
        try:
            run_sync(execute_lxc(container_name, f"stop {container_name} --force", node_id=node_id))
            logger.info(f"Container {container_name} stopped")
        except Exception as e:
            logger.warning(f"Failed to stop container {container_name}: {e}")
        
        run_sync(execute_lxc(container_name, f"delete {container_name} --force", node_id=node_id))
        logger.info(f"Container {container_name} deleted")
        
        run_sync(execute_lxc(container_name, f"init {os_version} {container_name} -s {DEFAULT_STORAGE_POOL}", node_id=node_id))
        logger.info(f"Container {container_name} created with {os_version}")
        
        run_sync(execute_lxc(container_name, f"config set {container_name} limits.memory {ram_mb}MB", node_id=node_id))
        run_sync(execute_lxc(container_name, f"config set {container_name} limits.cpu {cpu}", node_id=node_id))
        run_sync(execute_lxc(container_name, f"config device set {container_name} root size={storage_gb}GB", node_id=node_id))
        logger.info(f"Resource limits applied to {container_name}")
        
        run_sync(apply_lxc_config(container_name, node_id))
        logger.info(f"LXC config applied to {container_name}")
        
        run_sync(execute_lxc(container_name, f"start {container_name}", node_id=node_id))
        logger.info(f"Container {container_name} started")
        
        if vps.get('ip_address'):
            run_sync(configure_container_ip(container_name, vps['ip_address'], node_id))
            logger.info(f"IP address {vps['ip_address']} configured for {container_name}")
        
        run_sync(apply_internal_permissions(container_name, node_id))
        logger.info(f"Internal permissions applied to {container_name}")
        
        new_password = generate_strong_vps_password()
        logger.info(f"Generated new strong password for reinstalled VPS {vps_id} (length: {len(new_password)} chars)")
        
        run_sync(configure_ssh_and_root_password(container_name, node_id, new_password))
        logger.info(f"SSH and root password configured for {container_name}")
        
        store_vps_password(vps_id, new_password)
        logger.info(f"New password stored securely for VPS {vps_id}")
        
        run_sync(recreate_port_forwards(container_name))
        logger.info(f"Port forwards recreated for {container_name}")
        
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('''UPDATE vps 
                          SET os_version = ?, 
                              status = ?,
                              last_started = ?,
                              updated_at = ?
                          WHERE id = ?''',
                       (os_version, 'running', datetime.now().isoformat(), datetime.now().isoformat(), vps_id))
            conn.commit()
            logger.info(f"Database updated for VPS {vps_id} with OS {os_version} (rows affected: {cur.rowcount})")
        
        updated_vps = get_vps_by_id(vps_id)
        logger.info(f"VPS {vps_id} OS version after update: {updated_vps.get('os_version')}")
        
        log_activity(current_user.id, 'reinstall_vps', 'vps', str(vps_id), {'os': os_version})
        
        if current_user.is_admin and current_user.id != vps['user_id']:
            log_activity(current_user.id, 'admin_reinstall_vps', 'vps', str(vps_id), 
                        {'os': os_version, 'owner_id': vps['user_id']})
        
        create_notification(current_user.id, 'success', 'VPS Reinstalled', 
                          f'VPS {container_name} has been reinstalled with {os_version}. A new secure root password has been generated.')
        
        if socketio:
            socketio.emit('vps_reinstalled', {
                'vps_id': vps_id,
                'os_version': os_version,
                'status': 'running'
            }, room=f'vps_{vps_id}')
            socketio.emit('vps_status_change', {
                'vps_id': vps_id,
                'status': 'running',
                'os_version': os_version
            }, room=f'user_{vps["user_id"]}')
            
        return jsonify({
            'success': True, 
            'message': f'VPS {container_name} has been reinstalled with {os_version}',
            'os_version': os_version,
            'status': 'running'
        })
        
    except Exception as e:
        logger.error(f"Reinstall error for VPS {vps_id}: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/vps/<int:vps_id>/rename', methods=['POST'])
@login_required
def vps_rename(vps_id):
    vps = get_vps_by_id(vps_id)
    if not vps:
        return jsonify({'success': False, 'error': 'VPS not found'}), 404
    
    if vps['user_id'] != current_user.id and not current_user.is_admin:
        return jsonify({'success': False, 'error': 'Access denied'}), 403
    
    if is_vps_suspended(vps) and not current_user.is_admin:
        return jsonify({'success': False, 'error': 'VPS is suspended'}), 403
    
    data = request.get_json()
    new_name = data.get('hostname')
    
    if not new_name or len(new_name) < 3 or len(new_name) > 63:
        return jsonify({'success': False, 'error': 'Invalid hostname'}), 400
    
    if not re.match(r'^[a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9]$', new_name):
        return jsonify({'success': False, 'error': 'Invalid hostname format'}), 400
    
    try:
        run_sync(execute_lxc(vps['container_name'], f"exec {vps['container_name']} -- hostnamectl set-hostname {new_name}", node_id=vps['node_id']))
        update_vps(vps_id, hostname=new_name)
        log_activity(current_user.id, 'rename_vps', 'vps', str(vps_id), {'new_name': new_name})
        
        if current_user.is_admin and current_user.id != vps['user_id']:
            log_activity(current_user.id, 'admin_rename_vps', 'vps', str(vps_id),
                        {'new_name': new_name, 'owner_id': vps['user_id']})
        
        create_notification(current_user.id, 'success', 'VPS Renamed', f'VPS renamed to {new_name}')
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/vps/<int:vps_id>/notes', methods=['POST'])
@login_required
def vps_notes(vps_id):
    vps = get_vps_by_id(vps_id)
    if not vps or vps['user_id'] != current_user.id:
        return jsonify({'success': False, 'error': 'Access denied'}), 403
    
    data = request.get_json()
    notes = data.get('notes', '')
    
    update_vps(vps_id, notes=notes)
    return jsonify({'success': True})

@app.route('/vps/<int:vps_id>/password', methods=['GET'])
@login_required
def vps_get_password(vps_id):
    try:
        vps = get_vps_by_id(vps_id)
        if not vps:
            return jsonify({'success': False, 'error': 'VPS not found'}), 404
        
        if vps['user_id'] != current_user.id and not current_user.is_admin:
            return jsonify({'success': False, 'error': 'Access denied'}), 403
        
        password = get_vps_password(vps_id)
        container_name = vps['container_name']
        node_id = vps['node_id']
        password_source = 'database'
        
        try:
            status = run_sync(get_container_status(container_name, node_id))
        except:
            status = 'unknown'
        
        return jsonify({
            'success': True, 
            'password': password,
            'source': password_source,
            'container_status': status,
            'password_length': len(password)
        })
        
    except Exception as e:
        logger.error(f"Error getting password for VPS {vps_id}: {e}", exc_info=True)
        return jsonify({'success': False, 'error': 'Internal server error'}), 500

@app.route('/vps/<int:vps_id>/password/generate', methods=['POST'])
@login_required
def vps_generate_password(vps_id):
    try:
        vps = get_vps_by_id(vps_id)
        if not vps:
            return jsonify({'success': False, 'error': 'VPS not found'}), 404
        
        if vps['user_id'] != current_user.id and not current_user.is_admin:
            return jsonify({'success': False, 'error': 'Access denied'}), 403
        
        password = generate_strong_vps_password()
        
        return jsonify({
            'success': True,
            'password': password,
            'length': len(password)
        })
        
    except Exception as e:
        logger.error(f"Error generating password for VPS {vps_id}: {e}", exc_info=True)
        return jsonify({'success': False, 'error': 'Internal server error'}), 500

@app.route('/vps/<int:vps_id>/password/refresh', methods=['POST'])
@login_required
def vps_refresh_password(vps_id):
    try:
        vps = get_vps_by_id(vps_id)
        if not vps:
            return jsonify({'success': False, 'error': 'VPS not found'}), 404
        
        if vps['user_id'] != current_user.id and not current_user.is_admin:
            return jsonify({'success': False, 'error': 'Access denied'}), 403
        
        password = vps.get('root_password', 'root')
        note = None
        
        import platform
        if platform.system() == 'Linux':
            import shutil
            if shutil.which('lxc'):
                try:
                    container_name = vps['container_name']
                    status_result = subprocess.run(
                        ['lxc', 'list', container_name, '--format', 'json'],
                        capture_output=True,
                        text=True,
                        timeout=5
                    )
                    
                    if status_result.returncode == 0:
                        import json
                        containers = json.loads(status_result.stdout)
                        if containers and len(containers) > 0:
                            container_status = containers[0].get('status', '').upper()
                            
                            if container_status == 'RUNNING':
                                password_files = ['/root/.krysolvm_password', '/etc/krysolvm/password', '/root/.password']
                                for pwd_file in password_files:
                                    try:
                                        result = subprocess.run(
                                            ['lxc', 'exec', container_name, '--', 'cat', pwd_file],
                                            capture_output=True,
                                            text=True,
                                            timeout=5
                                        )
                                        if result.returncode == 0 and result.stdout.strip():
                                            password = result.stdout.strip()
                                            update_vps(vps_id, root_password=password)
                                            note = 'Password synced from container'
                                            break
                                    except:
                                        continue
                            else:
                                note = f'VPS is {container_status.lower()}. Showing stored password.'
                except:
                    pass
        
        log_activity(
            user_id=current_user.id,
            action='vps_password_refresh',
            resource_type='vps',
            resource_id=vps_id,
            details=f'Refreshed password for VPS {vps["hostname"]}',
            ip_address=request.remote_addr
        )
        
        return jsonify({'success': True, 'password': password, 'note': note})
    
    except Exception as e:
        logger.error(f"Error in password refresh endpoint: {e}")
        return jsonify({'success': False, 'error': 'Internal server error'}), 500

@app.route('/vps/<int:vps_id>/password/change', methods=['POST'])
@login_required
def vps_change_password(vps_id):
    try:
        vps = get_vps_by_id(vps_id)
        if not vps:
            return jsonify({'success': False, 'error': 'VPS not found'}), 404
        
        if vps['user_id'] != current_user.id and not current_user.is_admin:
            return jsonify({'success': False, 'error': 'Access denied'}), 403
        
        if is_vps_suspended(vps) and not current_user.is_admin:
            return jsonify({'success': False, 'error': 'VPS is suspended'}), 403
        
        data = request.get_json()
        new_password = data.get('password', '').strip()
        
        if not new_password:
            return jsonify({'success': False, 'error': 'Password is required'}), 400
        
        if len(new_password) < 8:
            return jsonify({'success': False, 'error': 'Password must be at least 8 characters'}), 400
        
        if len(new_password) > 128:
            return jsonify({'success': False, 'error': 'Password must be less than 128 characters'}), 400
        
        weak_passwords = ['password', '12345678', 'qwerty123', 'admin123', 'root1234']
        if new_password.lower() in weak_passwords:
            return jsonify({'success': False, 'error': 'Password is too weak. Please choose a stronger password'}), 400
        
        container_name = vps['container_name']
        node_id = vps['node_id']
        password_changed_in_container = False
        
        logger.info(f"Changing password for VPS {vps_id} ({container_name})")
        
        try:
            status = run_sync(get_container_status(container_name, node_id))
            logger.info(f"Container {container_name} status: {status}")
            
            if status.upper() == 'RUNNING':
                change_cmd = f"exec {container_name} -- bash -c \"echo 'root:{new_password}' | chpasswd\""
                
                logger.info(f"Executing password change command for {container_name}")
                result = run_sync(execute_lxc(container_name, change_cmd, node_id=node_id, timeout=10))
                
                save_commands = [
                    f"exec {container_name} -- bash -c \"echo '{new_password}' > /root/.krysolvm_password && chmod 600 /root/.krysolvm_password\"",
                    f"exec {container_name} -- bash -c \"mkdir -p /etc/krysolvm && echo '{new_password}' > /etc/krysolvm/password && chmod 600 /etc/krysolvm/password\"",
                    f"exec {container_name} -- bash -c \"mkdir -p /root/.ssh && echo '{new_password}' > /root/.ssh/root_password && chmod 600 /root/.ssh/root_password\""
                ]
                
                for save_cmd in save_commands:
                    try:
                        run_sync(execute_lxc(container_name, save_cmd, node_id=node_id, timeout=5))
                        logger.debug(f"Password saved with command: {save_cmd}")
                    except Exception as e:
                        logger.warning(f"Failed to save password with command {save_cmd}: {e}")
                
                password_changed_in_container = True
                logger.info(f"Password successfully changed in container {container_name}")
                
            else:
                logger.info(f"Container {container_name} is not running (status: {status}). Password will be updated in database only.")
                
        except Exception as e:
            logger.error(f"Failed to change password in container {container_name}: {e}")
        
        try:
            update_vps(vps_id, root_password=new_password)
            logger.info(f"Password updated in database for VPS {vps_id}")
        except Exception as e:
            logger.error(f"Failed to update password in database for VPS {vps_id}: {e}")
            return jsonify({'success': False, 'error': 'Failed to update password in database'}), 500
        
        log_activity(
            user_id=current_user.id,
            action='vps_password_change',
            resource_type='vps',
            resource_id=str(vps_id),
            details={'container_name': container_name, 'changed_in_container': password_changed_in_container}
        )
        
        create_notification(
            user_id=vps['user_id'],
            title='VPS Password Changed',
            message=f'Password for VPS {vps["hostname"]} has been changed successfully.',
            type='success'
        )
        
        if socketio:
            socketio.emit('vps_password_changed', {
                'vps_id': vps_id,
                'container_name': container_name,
                'changed_in_container': password_changed_in_container
            }, room=f'vps_{vps_id}')
        
        response_message = 'Password changed successfully'
        if not password_changed_in_container:
            response_message += ' (VPS was not running - password will be applied when VPS starts)'
        
        return jsonify({
            'success': True, 
            'message': response_message,
            'changed_in_container': password_changed_in_container,
            'container_status': status if 'status' in locals() else 'unknown'
        })
        
    except Exception as e:
        logger.error(f"Password change error for VPS {vps_id}: {e}", exc_info=True)
        return jsonify({'success': False, 'error': 'Internal server error'}), 500

@app.route('/vps/<int:vps_id>/bandwidth-usage')
@login_required
def vps_get_bandwidth_usage(vps_id):
    vps = get_vps_by_id(vps_id)
    if not vps or (vps['user_id'] != current_user.id and not current_user.is_admin):
        return jsonify({'success': False, 'error': 'Access denied'}), 403
    
    try:
        vps_status = vps.get('status', '').lower()
        
        if vps_status == 'running' and vps.get('bandwidth_quota_gb', 0) > 0:
            try:
                logger.debug(f"Fetching bandwidth usage for VPS {vps_id} ({vps['container_name']})")
                
                usage_data = run_sync(
                    asyncio.wait_for(
                        get_bandwidth_usage(vps['container_name'], vps['node_id']),
                        timeout=10.0
                    )
                )
                
                if usage_data and usage_data.get('total_gb', 0) >= 0:
                    new_usage = usage_data['total_gb']
                    update_vps(vps_id, bandwidth_used_gb=new_usage)
                    
                    return jsonify({
                        'success': True,
                        'usage': {
                            'total_gb': usage_data['total_gb'],
                            'rx_bytes': usage_data['rx_bytes'],
                            'tx_bytes': usage_data['tx_bytes'],
                            'quota_gb': vps.get('bandwidth_quota_gb', 0),
                            'quota_exceeded': usage_data.get('quota_exceeded', False),
                            'percentage': (usage_data['total_gb'] / max(vps.get('bandwidth_quota_gb', 1), 1)) * 100 if vps.get('bandwidth_quota_gb', 0) > 0 else 0
                        }
                    })
                else:
                    logger.debug(f"VPS {vps_id} bandwidth usage: no valid data returned")
                    
            except asyncio.TimeoutError:
                logger.warning(f"Timeout getting bandwidth usage for VPS {vps_id}")
                return jsonify({
                    'success': True,
                    'usage': {
                        'total_gb': vps.get('bandwidth_used_gb', 0),
                        'rx_bytes': 0,
                        'tx_bytes': 0,
                        'quota_gb': vps.get('bandwidth_quota_gb', 0),
                        'quota_exceeded': False,
                        'percentage': (vps.get('bandwidth_used_gb', 0) / max(vps.get('bandwidth_quota_gb', 1), 1)) * 100 if vps.get('bandwidth_quota_gb', 0) > 0 else 0
                    }
                })
            except Exception as e:
                logger.error(f"Error getting live bandwidth usage for VPS {vps_id}: {e}")
        
        return jsonify({
            'success': True,
            'usage': {
                'total_gb': vps.get('bandwidth_used_gb', 0),
                'rx_bytes': 0,
                'tx_bytes': 0,
                'quota_gb': vps.get('bandwidth_quota_gb', 0),
                'quota_exceeded': False,
                'percentage': (vps.get('bandwidth_used_gb', 0) / max(vps.get('bandwidth_quota_gb', 1), 1)) * 100 if vps.get('bandwidth_quota_gb', 0) > 0 else 0
            }
        })
            
    except Exception as e:
        logger.error(f"Error getting bandwidth usage for VPS {vps_id}: {e}")
        return jsonify({
            'success': False,
            'error': 'Failed to get bandwidth usage',
            'usage': {
                'total_gb': vps.get('bandwidth_used_gb', 0),
                'rx_bytes': 0,
                'tx_bytes': 0,
                'quota_gb': vps.get('bandwidth_quota_gb', 0),
                'quota_exceeded': False,
                'percentage': 0
            }
        }), 200

@app.route('/vps/<int:vps_id>/bandwidth-quota', methods=['POST'])
@login_required
def vps_update_bandwidth_quota(vps_id):
    try:
        vps = get_vps_by_id(vps_id)
        if not vps:
            return jsonify({'success': False, 'error': 'VPS not found'}), 404
        
        if not current_user.is_admin and vps['user_id'] != current_user.id:
            return jsonify({'success': False, 'error': 'Access denied'}), 403
        
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'error': 'No data provided'}), 400
        
        quota_gb = int(data.get('quota_gb', 0))
        reset_usage = bool(data.get('reset_usage', False))
        
        if quota_gb < 0 or quota_gb > 10000:
            return jsonify({'success': False, 'error': 'Bandwidth quota must be between 0 and 10000 GB'}), 400
        
        container_name = vps['container_name']
        node_id = vps['node_id']
        
        logger.info(f"Updating bandwidth quota for VPS {vps_id} ({container_name}): {quota_gb}GB")
        
        try:
            status = run_sync(get_container_status(container_name, node_id))
            if status.upper() == 'RUNNING':
                run_sync(configure_bandwidth_quota(container_name, quota_gb, node_id))
                logger.info(f"Bandwidth quota applied to running container {container_name}")
                
                if reset_usage:
                    run_sync(reset_bandwidth_usage(container_name, node_id))
                    logger.info(f"Bandwidth usage reset for {container_name}")
            else:
                logger.info(f"Container {container_name} is not running (status: {status}). Quota will be applied when started.")
        except Exception as e:
            logger.error(f"Failed to apply bandwidth quota to container {container_name}: {e}")
            return jsonify({'success': False, 'error': f'Failed to apply bandwidth quota: {str(e)}'}), 500
        
        try:
            with get_db() as conn:
                cur = conn.cursor()
                update_fields = ['bandwidth_quota_gb = ?', 'updated_at = ?']
                update_values = [quota_gb, datetime.now().isoformat()]
                
                if reset_usage:
                    update_fields.extend(['bandwidth_used_gb = ?', 'bandwidth_reset_date = ?'])
                    update_values.extend([0.0, datetime.now().isoformat()])
                
                update_values.append(vps_id)
                
                cur.execute(f'''UPDATE vps SET {', '.join(update_fields)} WHERE id = ?''', update_values)
                conn.commit()
                logger.info(f"Bandwidth quota updated in database for VPS {vps_id}")
        except Exception as e:
            logger.error(f"Failed to update bandwidth quota in database for VPS {vps_id}: {e}")
            return jsonify({'success': False, 'error': 'Failed to update database'}), 500
        
        try:
            config_parts = []
            if vps['ram']:
                config_parts.append(vps['ram'] + ' RAM')
            if vps['cpu']:
                config_parts.append(vps['cpu'] + ' CPU')
            if vps['storage']:
                config_parts.append(vps['storage'] + ' Disk')
            
            if quota_gb > 0:
                config_parts.append(f"{format_bandwidth_quota(quota_gb)} Quota")
            
            new_config = ' / '.join(config_parts)
            
            with get_db() as conn:
                cur = conn.cursor()
                cur.execute('UPDATE vps SET config = ? WHERE id = ?', (new_config, vps_id))
                conn.commit()
        except Exception as e:
            logger.warning(f"Failed to update config string for VPS {vps_id}: {e}")
        
        log_activity(
            user_id=current_user.id,
            action='vps_bandwidth_quota_update',
            resource_type='vps',
            resource_id=str(vps_id),
            details={
                'container_name': container_name,
                'quota_gb': quota_gb,
                'reset_usage': reset_usage
            }
        )
        
        create_notification(
            user_id=vps['user_id'],
            title='Bandwidth Quota Updated',
            message=f'Bandwidth quota for VPS {vps["hostname"]} has been updated to {format_bandwidth_quota(quota_gb)}.',
            type='success'
        )
        
        return jsonify({
            'success': True,
            'message': 'Bandwidth quota updated successfully',
            'quota': {
                'quota_gb': quota_gb,
                'reset_usage': reset_usage
            }
        })
        
    except Exception as e:
        logger.error(f"Bandwidth quota update error for VPS {vps_id}: {e}", exc_info=True)
        return jsonify({'success': False, 'error': 'Internal server error'}), 500

@app.route('/vps/<int:vps_id>/suspended')
@login_required
def vps_suspended_page(vps_id):
    vps = get_vps_by_id(vps_id)

    if not vps:
        abort(404)

    shared_with = vps.get('shared_with', []) or []
    allowed = (
        vps['user_id'] == current_user.id
        or str(current_user.id) in [str(uid) for uid in shared_with]
        or current_user.is_admin
    )

    if not allowed:
        flash('VPS not found or access denied', 'danger')
        return redirect(url_for('vps_list'))

    if not is_vps_suspended(vps):
        return redirect(url_for('vps_detail', vps_id=vps_id))

    return render_template(
        "vps_suspended.html",
        vps=vps,
        panel_name=get_setting('site_name', 'KrysolVM')
    )

@app.route('/vps/<int:vps_id>/installing')
@login_required
def vps_installing(vps_id):
    vps = get_vps_by_id(vps_id)

    if not vps:
        abort(404)

    shared_with = vps.get('shared_with', []) or []
    allowed = (
        vps['user_id'] == current_user.id
        or str(current_user.id) in [str(uid) for uid in shared_with]
        or current_user.is_admin
    )

    if not allowed:
        flash('VPS not found or access denied', 'danger')
        return redirect(url_for('vps_list'))

    if vps.get('status') != 'installing':
        return redirect(url_for('vps_detail', vps_id=vps_id))

    metadata = vps.get('metadata', {})
    if isinstance(metadata, str):
        import json
        try:
            metadata = json.loads(metadata)
        except:
            metadata = {}
    
    progress = metadata.get('installation_progress', 0)
    message = metadata.get('installation_message', 'Starting installation...')
    started_at = metadata.get('installation_started')

    return render_template(
        "vps_installing.html",
        vps=vps,
        progress=progress,
        message=message,
        started_at=started_at,
        panel_name=get_setting('site_name', 'KrysolVM')
    )

@app.route('/vps/<int:vps_id>/installation-progress')
@login_required
def vps_installation_progress(vps_id):
    vps = get_vps_by_id(vps_id)
    
    if not vps:
        return jsonify({'error': 'VPS not found'}), 404
    
    shared_with = vps.get('shared_with', []) or []
    allowed = (
        vps['user_id'] == current_user.id
        or str(current_user.id) in [str(uid) for uid in shared_with]
        or current_user.is_admin
    )
    
    if not allowed:
        return jsonify({'error': 'Access denied'}), 403
    
    metadata = vps.get('metadata', {})
    if isinstance(metadata, str):
        import json
        try:
            metadata = json.loads(metadata)
        except:
            metadata = {}
    
    progress = metadata.get('installation_progress', 0)
    message = metadata.get('installation_message', 'Starting installation...')
    status = vps.get('status', 'unknown')
    
    return jsonify({
        'success': True,
        'progress': progress,
        'message': message,
        'status': status,
        'completed': progress >= 100 or status != 'installing'
    })

@app.route('/vps/<int:vps_id>/migrating')
@login_required
def vps_migrating_page(vps_id):
    vps = get_vps_by_id(vps_id)

    if not vps:
        abort(404)

    shared_with = vps.get('shared_with', []) or []
    allowed = (
        vps['user_id'] == current_user.id
        or str(current_user.id) in [str(uid) for uid in shared_with]
        or current_user.is_admin
    )

    if not allowed:
        flash('VPS not found or access denied', 'danger')
        return redirect(url_for('vps_list'))

    if vps.get('status') != 'transferring':
        return redirect(url_for('vps_detail', vps_id=vps_id))

    metadata = vps.get('metadata', {})
    if isinstance(metadata, str):
        import json
        try:
            metadata = json.loads(metadata)
        except:
            metadata = {}
    
    progress = metadata.get('migration_progress', 0)
    message = metadata.get('migration_message', 'Starting migration...')
    migrated_from = metadata.get('migrated_from_node')
    migrated_to = metadata.get('migrated_to_node')
    
    source_node_name = None
    target_node_name = None
    if migrated_from:
        source_node = get_node(migrated_from)
        if source_node:
            source_node_name = source_node['name']
    if migrated_to:
        target_node = get_node(migrated_to)
        if target_node:
            target_node_name = target_node['name']

    return render_template(
        "vps_migrating.html",
        vps=vps,
        progress=progress,
        message=message,
        source_node_name=source_node_name,
        target_node_name=target_node_name,
        panel_name=get_setting('site_name', 'KrysolVM')
    )

@app.route('/vps/<int:vps_id>/migration-progress')
@login_required
def vps_migration_progress_api(vps_id):
    vps = get_vps_by_id(vps_id)
    
    if not vps:
        return jsonify({'error': 'VPS not found'}), 404
    
    shared_with = vps.get('shared_with', []) or []
    allowed = (
        vps['user_id'] == current_user.id
        or str(current_user.id) in [str(uid) for uid in shared_with]
        or current_user.is_admin
    )
    
    if not allowed:
        return jsonify({'error': 'Access denied'}), 403
    
    metadata = vps.get('metadata', {})
    if isinstance(metadata, str):
        import json
        try:
            metadata = json.loads(metadata)
        except:
            metadata = {}
    
    progress = metadata.get('migration_progress', 0)
    message = metadata.get('migration_message', 'Starting migration...')
    status = vps.get('status', 'unknown')
    
    return jsonify({
        'progress': progress,
        'message': message,
        'status': status,
        'completed': progress >= 100 or status != 'transferring'
    })

@app.route('/vps/<int:vps_id>/expiration')
@login_required
@vps_owner_or_admin_required
def vps_expiration_info(vps_id):
    vps = get_vps_by_id(vps_id)
    if not vps:
        return jsonify({'success': False, 'error': 'VPS not found'}), 404
    
    expires_info = {
        'vps_id': vps_id,
        'container_name': vps['container_name'],
        'hostname': vps['hostname'],
        'auto_suspend_enabled': bool(vps.get('auto_suspend_enabled', 0)),
        'expiration_days': vps.get('expiration_days', 0),
        'expires_at': vps.get('expires_at'),
        'last_renewed_at': vps.get('last_renewed_at'),
        'renewal_count': vps.get('renewal_count', 0),
        'is_expired': False,
        'days_remaining': None,
        'hours_remaining': None
    }
    
    if vps.get('expires_at'):
        expires_dt = datetime.fromisoformat(vps['expires_at'])
        now = datetime.now()
        time_diff = expires_dt - now
        
        expires_info['is_expired'] = expires_dt < now
        expires_info['days_remaining'] = time_diff.days if not expires_info['is_expired'] else 0
        expires_info['hours_remaining'] = int(time_diff.total_seconds() / 3600) if not expires_info['is_expired'] else 0
        expires_info['expires_at_formatted'] = expires_dt.strftime("%Y-%m-%d %H:%M")
    
    return jsonify({'success': True, 'data': expires_info})

# ============================================================================
# Port Forwarding Routes
# ============================================================================
@app.route('/ports')
@login_required
def ports_list():
    allocated = get_user_allocation(current_user.id)
    used = get_user_used_ports(current_user.id)
    forwards = get_user_forwards(current_user.id)
    vps_list = get_vps_for_user(current_user.id)
    
    for forward in forwards:
        vps = get_vps_by_container(forward['vps_container'])
        if vps:
            forward['display_ip'] = get_vps_display_ip(vps) or YOUR_SERVER_IP
        else:
            forward['display_ip'] = YOUR_SERVER_IP
    
    return render_template('ports.html',
                          panel_name=get_setting('site_name', 'KrysolVM'),
                          allocated=allocated,
                          used=used,
                          available=allocated - used,
                          forwards=forwards,
                          vps_list=vps_list,
                          YOUR_SERVER_IP=YOUR_SERVER_IP,
                          socketio_available=SOCKETIO_AVAILABLE)

@app.route('/ports/add', methods=['POST'])
@login_required
def ports_add():
    data = request.get_json()
    vps_id = data.get('vps_id')
    vps_port = data.get('vps_port')
    protocol = data.get('protocol', 'tcp,udp')
    description = data.get('description', '')
    
    if not vps_id or not vps_port:
        return jsonify({'success': False, 'error': 'Missing parameters'}), 400
    
    try:
        vps_port = int(vps_port)
        if vps_port < 1 or vps_port > 65535:
            raise ValueError
    except ValueError:
        return jsonify({'success': False, 'error': 'Invalid port number'}), 400
    
    vps = get_vps_by_id(vps_id)
    if not vps:
        return jsonify({'success': False, 'error': 'VPS not found'}), 404
    
    if vps['user_id'] != current_user.id and not current_user.is_admin:
        return jsonify({'success': False, 'error': 'Access denied'}), 403
    
    if is_vps_suspended(vps) and not current_user.is_admin:
        return jsonify({'success': False, 'error': 'VPS is suspended'}), 403
    
    owner_id = vps['user_id']
    allocated = get_user_allocation(owner_id)
    used = get_user_used_ports(owner_id)
    
    if used >= allocated:
        return jsonify({'success': False, 'error': f'Port quota exceeded for VPS owner (used {used}/{allocated})'}), 400
    
    host_port = run_sync(create_port_forward(owner_id, vps['container_name'], vps_port, vps['node_id'], protocol, description))
    
    if host_port:
        display_ip = get_vps_display_ip(vps) or YOUR_SERVER_IP
        
        if current_user.is_admin and current_user.id != owner_id:
            log_activity(current_user.id, 'admin_create_port_forward', 'port', str(host_port),
                        {'vps_id': vps_id, 'owner_id': owner_id, 'vps_port': vps_port, 'host_port': host_port})
        
        return jsonify({
            'success': True,
            'host_port': host_port,
            'message': f'Port {vps_port} forwarded to {display_ip}:{host_port}',
            'display_ip': display_ip
        })
    else:
        return jsonify({'success': False, 'error': 'Could not assign host port'}), 500

@app.route('/ports/remove/<int:forward_id>', methods=['POST'])
@login_required
def ports_remove(forward_id):
    success, user_id = run_sync(remove_port_forward(forward_id))
    
    if success and (user_id == current_user.id or current_user.is_admin):
        if current_user.is_admin and user_id != current_user.id:
            log_activity(current_user.id, 'admin_remove_port_forward', 'port', str(forward_id),
                        {'owner_id': user_id})
        return jsonify({'success': True})
    else:
        return jsonify({'success': False, 'error': 'Forward not found or access denied'}), 404

@app.route('/ports/hit/<int:host_port>', methods=['POST'])
def port_hit(host_port):
    run_sync(update_port_forward_hit(host_port))
    return jsonify({'success': True})

# ============================================================================
# Admin Routes
# ============================================================================
@app.route('/admin')
@login_required
@admin_required
def admin_dashboard():
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('SELECT COUNT(*) FROM users')
        total_users = cur.fetchone()[0]
        
        cur.execute('SELECT COUNT(*) FROM vps')
        total_vps = cur.fetchone()[0]
        
        cur.execute('SELECT COUNT(*) FROM vps WHERE status = "running"')
        running_vps = cur.fetchone()[0]
        
        cur.execute('SELECT COUNT(*) FROM vps WHERE suspended = 1')
        suspended_vps = cur.fetchone()[0]
        
        cur.execute('SELECT COUNT(*) FROM nodes')
        total_nodes = cur.fetchone()[0]
        
        cur.execute('SELECT SUM(allocated_ports) FROM port_allocations')
        total_ports = cur.fetchone()[0] or 0
        
        cur.execute('SELECT COUNT(*) FROM port_forwards')
        used_ports = cur.fetchone()[0] or 0
        
        cur.execute('''SELECT a.*, u.username FROM activity_logs a
                       LEFT JOIN users u ON a.user_id = u.id
                       ORDER BY a.created_at DESC LIMIT 20''')
        recent_activity = [dict(row) for row in cur.fetchall()]
    
    nodes = get_nodes()
    node_status = []
    online_nodes = 0
    
    for node in nodes:
        status = run_sync(get_node_status(node['id']))
        vps_count = get_current_vps_count(node['id'])
        
        if status.get('online', False):
            online_nodes += 1
        
        node_status.append({
            'id': node['id'],
            'name': node['name'],
            'location': node['location'],
            'status': status['status'],
            'online': status.get('online', False),
            'vps_count': vps_count,
            'total_vps': node['total_vps'],
            'is_local': node['is_local'],
            'stats': status.get('stats', {})
        })
    
    host_stats = run_sync(get_host_stats(1))
    
    return render_template('admin/dashboard.html',
                          panel_name=get_setting('site_name', 'KrysolVM'),
                          total_users=total_users,
                          total_vps=total_vps,
                          running_vps=running_vps,
                          suspended_vps=suspended_vps,
                          total_nodes=total_nodes,
                          online_nodes=online_nodes,
                          total_ports=total_ports,
                          used_ports=used_ports,
                          node_status=node_status,
                          recent_activity=recent_activity,
                          host_stats=host_stats,
                          socketio_available=SOCKETIO_AVAILABLE)

@app.route('/admin/users')
@login_required
@admin_required
def admin_users():
    search_query = request.args.get('search', '')
    page = int(request.args.get('page', 1))
    per_page = 20
    offset = (page - 1) * per_page
    
    with get_db() as conn:
        cur = conn.cursor()
        if search_query:
            cur.execute('''SELECT u.*,
                           (SELECT COUNT(*) FROM vps WHERE user_id = u.id) as vps_count,
                           (SELECT allocated_ports FROM port_allocations WHERE user_id = u.id) as port_quota,
                           (SELECT used_ports FROM port_allocations WHERE user_id = u.id) as used_ports
                           FROM users u
                           WHERE u.username LIKE ? OR u.email LIKE ?
                           ORDER BY u.id
                           LIMIT ? OFFSET ?''',
                       (f'%{search_query}%', f'%{search_query}%', per_page, offset))
        else:
            cur.execute('''SELECT u.*,
                           (SELECT COUNT(*) FROM vps WHERE user_id = u.id) as vps_count,
                           (SELECT allocated_ports FROM port_allocations WHERE user_id = u.id) as port_quota,
                           (SELECT used_ports FROM port_allocations WHERE user_id = u.id) as used_ports
                           FROM users u
                           ORDER BY u.id
                           LIMIT ? OFFSET ?''',
                       (per_page, offset))
        users = [dict(row) for row in cur.fetchall()]
        
        if search_query:
            cur.execute('SELECT COUNT(*) FROM users WHERE username LIKE ? OR email LIKE ?',
                       (f'%{search_query}%', f'%{search_query}%'))
        else:
            cur.execute('SELECT COUNT(*) FROM users')
        total_users = cur.fetchone()[0]
    
    return render_template('admin/users.html',
                          panel_name=get_setting('site_name', 'KrysolVM'),
                          users=users,
                          search_query=search_query,
                          page=page,
                          total_pages=(total_users + per_page - 1) // per_page)

@app.route('/admin/users/<int:user_id>')
@login_required
@admin_required
def admin_user_detail(user_id):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('SELECT * FROM users WHERE id = ?', (user_id,))
        user = dict(cur.fetchone())
        
        cur.execute('SELECT * FROM vps WHERE user_id = ? ORDER BY created_at DESC', (user_id,))
        vps_list = []
        for row in cur.fetchall():
            vps = dict(row)
            try:
                vps['shared_with'] = json.loads(vps['shared_with']) if vps['shared_with'] else []
            except:
                vps['shared_with'] = []
            vps_list.append(vps)
        
        cur.execute('SELECT * FROM port_allocations WHERE user_id = ?', (user_id,))
        port_alloc = cur.fetchone()
        allocated_ports = port_alloc[1] if port_alloc else 0
        used_ports = port_alloc[2] if port_alloc else 0
        
        cur.execute('SELECT * FROM port_forwards WHERE user_id = ? ORDER BY created_at DESC', (user_id,))
        forwards = [dict(row) for row in cur.fetchall()]
        
        cur.execute('SELECT * FROM activity_logs WHERE user_id = ? ORDER BY created_at DESC LIMIT 50', (user_id,))
        activities = [dict(row) for row in cur.fetchall()]
    
    return render_template('admin/user_detail.html',
                          panel_name=get_setting('site_name', 'KrysolVM'),
                          user=user,
                          vps_list=vps_list,
                          allocated_ports=allocated_ports,
                          used_ports=used_ports,
                          forwards=forwards,
                          activities=activities)

@app.route('/admin/users/create', methods=['GET', 'POST'])
@login_required
@admin_required
def admin_users_create():
    if request.method == 'GET':
        return render_template('admin/users_create.html',
                              panel_name=get_setting('site_name', 'KrysolVM'))
    
    data = request.get_json() or request.form.to_dict()

    username = (data.get("username") or "").strip()
    email = (data.get("email") or "").strip()
    password = data.get("password") or ""
    is_admin = bool(data.get("is_admin", False))
    port_quota = int(data.get("port_quota", 10))

    if not username or not email or not password:
        return jsonify({"success": False, "error": "username, email and password are required"}), 400

    if len(password) < 8:
        return jsonify({"success": False, "error": "Password must be at least 8 characters"}), 400

    if len(username) < 3 or len(username) > 32:
        return jsonify({"success": False, "error": "Username must be 3–32 characters"}), 400

    if "@" not in email or "." not in email.split("@")[-1]:
        return jsonify({"success": False, "error": "Invalid email format"}), 400

    if port_quota < 0:
        return jsonify({"success": False, "error": "Port quota cannot be negative"}), 400

    now = datetime.now().isoformat()

    with get_db() as conn:
        cur = conn.cursor()

        cur.execute(
            "SELECT id FROM users WHERE username = ? OR email = ?",
            (username, email)
        )
        if cur.fetchone():
            return jsonify({"success": False, "error": "Username or email already exists"}), 409

        password_hash = generate_password_hash(password)

        try:
            cur.execute("""
                INSERT INTO users (
                    username, email, password_hash, is_admin, created_at, last_active
                ) VALUES (?, ?, ?, ?, ?, ?)
            """, (
                username,
                email,
                password_hash,
                1 if is_admin else 0,
                now,
                now
            ))

            user_id = cur.lastrowid

            if port_quota > 0:
                cur.execute("""
                    INSERT OR REPLACE INTO port_allocations
                    (user_id, allocated_ports, used_ports, updated_at)
                    VALUES (?, ?, 0, ?)
                """, (user_id, port_quota, now))

            conn.commit()

        except Exception as e:
            conn.rollback()
            return jsonify({
                "success": False,
                "error": "Database error while creating user"
            }), 500

    try:
        log_activity(
            current_user.id,
            'create_user',
            'user',
            str(user_id),
            {"username": username, "is_admin": is_admin}
        )

        create_notification(
            user_id=user_id,
            type='info',
            title='Welcome!',
            message='Your account has been created by an administrator. Welcome aboard!'
        )
    except Exception:
        pass

    return jsonify({
        "success": True,
        "message": "User created successfully",
        "user_id": user_id
    }), 201

@app.route('/admin/users/<int:user_id>/edit', methods=['GET', 'POST'])
@login_required
@admin_required
def admin_user_edit(user_id):
    logger.info(f"admin_user_edit called: user_id={user_id}, method={request.method}, is_json={request.is_json}")
    
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('SELECT * FROM users WHERE id = ?', (user_id,))
            user_row = cur.fetchone()
            
            if not user_row:
                logger.warning(f"User {user_id} not found")
                if request.method == 'GET':
                    flash('User not found', 'danger')
                    return redirect(url_for('admin_users'))
                return jsonify({'success': False, 'error': 'User not found'}), 404
            
            user = dict(user_row)
            
            cur.execute('SELECT * FROM port_allocations WHERE user_id = ?', (user_id,))
            port_row = cur.fetchone()
            port_allocation = dict(port_row) if port_row else {'allocated_ports': 0, 'used_ports': 0}
            
            cur.execute('SELECT COUNT(*) FROM vps WHERE user_id = ?', (user_id,))
            vps_count = cur.fetchone()[0]
        
        if request.method == 'GET':
            logger.info(f"Rendering edit form for user {user_id}")
            return render_template('admin/users_edit.html',
                                  panel_name=get_setting('site_name', 'KrysolVM'),
                                  user=user,
                                  port_allocation=port_allocation,
                                  vps_count=vps_count)
        
        logger.info(f"Processing POST request for user {user_id}")
        data = request.get_json() or request.form.to_dict()
        logger.info(f"Received data: {data}")
        
        with get_db() as conn:
            cur = conn.cursor()
            
            if 'is_admin' in data:
                cur.execute('UPDATE users SET is_admin = ? WHERE id = ?', (1 if data['is_admin'] else 0, user_id))
                logger.info(f"Updated is_admin for user {user_id}")
            
            if 'port_quota' in data:
                quota = int(data['port_quota'])
                now = datetime.now().isoformat()
                cur.execute('''INSERT OR REPLACE INTO port_allocations (user_id, allocated_ports, used_ports, updated_at)
                               VALUES (?, ?, COALESCE((SELECT used_ports FROM port_allocations WHERE user_id = ?), 0), ?)''',
                            (user_id, quota, user_id, now))
                logger.info(f"Updated port quota for user {user_id}: {quota}")
            
            conn.commit()
        
        log_activity(current_user.id, 'edit_user', 'user', str(user_id), data)
        create_notification(user_id, 'info', 'Account Updated', 'Your account has been updated by an administrator.')
        
        logger.info(f"Successfully updated user {user_id}")
        return jsonify({'success': True})
    
    except Exception as e:
        logger.error(f"Error in admin_user_edit for user {user_id}: {e}", exc_info=True)
        if request.method == 'GET':
            flash(f'Error: {str(e)}', 'danger')
            return redirect(url_for('admin_users'))
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/admin/users/<int:user_id>/delete', methods=['POST'])
@login_required
@admin_required
def admin_user_delete(user_id):
    if user_id == current_user.id:
        return jsonify({'success': False, 'error': 'Cannot delete yourself'}), 400
    
    vps_list = get_vps_for_user(user_id)
    for vps in vps_list:
        try:
            run_sync(execute_lxc(vps['container_name'], f"delete {vps['container_name']} --force", node_id=vps['node_id']))
        except:
            pass
    
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('DELETE FROM port_allocations WHERE user_id = ?', (user_id,))
        cur.execute('DELETE FROM port_forwards WHERE user_id = ?', (user_id,))
        cur.execute('DELETE FROM vps WHERE user_id = ?', (user_id,))
        cur.execute('DELETE FROM users WHERE id = ?', (user_id,))
        conn.commit()
    
    log_activity(current_user.id, 'delete_user', 'user', str(user_id))
    return jsonify({'success': True})

def get_all_users():
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('SELECT id, username, email FROM users ORDER BY username')
        return [dict(row) for row in cur.fetchall()]

@app.route('/admin/vps')
@login_required
@admin_required
def admin_vps():
    search_query = request.args.get('search', '')
    node_id = request.args.get('node_id', type=int)
    status_filter = request.args.get('status', '')
    page = int(request.args.get('page', 1))
    per_page = 20
    offset = (page - 1) * per_page

    vps_list = []

    query = '''SELECT v.*, u.username, n.name as node_name
               FROM vps v
               JOIN users u ON v.user_id = u.id
               JOIN nodes n ON v.node_id = n.id'''

    params = []
    conditions = []

    if search_query:
        conditions.append('(v.container_name LIKE ? OR u.username LIKE ?)')
        params.extend([f'%{search_query}%', f'%{search_query}%'])

    if node_id:
        conditions.append('v.node_id = ?')
        params.append(node_id)

    if status_filter:
        conditions.append('v.status = ?')
        params.append(status_filter)

    if conditions:
        query += ' WHERE ' + ' AND '.join(conditions)

    query += ' ORDER BY v.created_at DESC LIMIT ? OFFSET ?'
    params.extend([per_page, offset])

    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(query, params)
        rows = cur.fetchall()

        for row in rows:
            vps = dict(row)

            try:
                vps['shared_with'] = json.loads(vps['shared_with']) if vps['shared_with'] else []
            except Exception:
                vps['shared_with'] = []
            
            vps['suspended'] = bool(vps.get('suspended', 0))
            vps['whitelisted'] = bool(vps.get('whitelisted', 0))

            if is_vps_suspended(vps):
                vps['live_status'] = 'suspended'
            else:
                vps['live_status'] = vps.get('status', 'unknown').lower()
            
            vps['live_cpu'] = 0.0
            vps['live_ram'] = {'used': 0, 'total': 0, 'pct': 0.0}
            vps['live_disk'] = {'use_percent': '0%', 'pct': 0.0}

            vps_list.append(vps)

        count_query = '''
            SELECT COUNT(*)
            FROM vps v
            JOIN users u ON v.user_id = u.id
        '''

        count_params = params[:-2]

        if conditions:
            count_query += ' WHERE ' + ' AND '.join(conditions)

        cur.execute(count_query, count_params)
        total_vps = cur.fetchone()[0]

    nodes = get_nodes()

    return render_template(
        'admin/vps.html',
        panel_name=get_setting('site_name', 'KrysolVM'),
        vps_list=vps_list,
        search_query=search_query,
        node_id=node_id,
        status_filter=status_filter,
        nodes=nodes,
        page=page,
        total_pages=(total_vps + per_page - 1) // per_page,
        socketio_available=SOCKETIO_AVAILABLE
    )

@app.route('/admin/vps/expiring')
@login_required
@admin_required
def admin_vps_expiring():
    days_ahead = int(request.args.get('days', 7))
    
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        with get_db() as conn:
            cur = conn.cursor()
            now = datetime.now().isoformat()
            future_date = (datetime.now() + timedelta(days=days_ahead)).isoformat()
            
            cur.execute('''SELECT v.*, u.username, u.email, n.name as node_name
                          FROM vps v
                          JOIN users u ON v.user_id = u.id
                          JOIN nodes n ON v.node_id = n.id
                          WHERE v.auto_suspend_enabled = 1 
                          AND v.expires_at IS NOT NULL 
                          AND v.expires_at <= ?
                          ORDER BY v.expires_at ASC''', (future_date,))
            
            vps_list = []
            for row in cur.fetchall():
                vps = dict(row)
                expires_dt = datetime.fromisoformat(vps['expires_at'])
                time_diff = expires_dt - datetime.now()
                
                vps['is_expired'] = expires_dt < datetime.now()
                vps['days_remaining'] = time_diff.days if not vps['is_expired'] else 0
                vps['hours_remaining'] = int(time_diff.total_seconds() / 3600) if not vps['is_expired'] else 0
                vps['expires_at_formatted'] = expires_dt.strftime("%Y-%m-%d %H:%M")
                vps['suspended'] = bool(vps.get('suspended', 0))
                
                vps_list.append(vps)
        
        return jsonify({
            'success': True,
            'data': vps_list,
            'count': len(vps_list),
            'days_ahead': days_ahead
        })
    
    with get_db() as conn:
        cur = conn.cursor()
        
        cur.execute('''SELECT COUNT(*) FROM vps 
                      WHERE auto_suspend_enabled = 1 
                      AND expires_at IS NOT NULL 
                      AND expires_at < ?''', (datetime.now().isoformat(),))
        expired_count = cur.fetchone()[0]
        
        cur.execute('''SELECT COUNT(*) FROM vps 
                      WHERE auto_suspend_enabled = 1 
                      AND expires_at IS NOT NULL 
                      AND expires_at >= ? 
                      AND expires_at <= ?''', 
                   (datetime.now().isoformat(), 
                    (datetime.now() + timedelta(days=7)).isoformat()))
        expiring_soon_count = cur.fetchone()[0]
        
        cur.execute('''SELECT COUNT(*) FROM vps 
                      WHERE auto_suspend_enabled = 1 
                      AND expires_at IS NOT NULL''')
        total_with_expiration = cur.fetchone()[0]
    
    return render_template('admin/vps_expiring.html',
                         panel_name=get_setting('site_name', 'KrysolVM'),
                         expired_count=expired_count,
                         expiring_soon_count=expiring_soon_count,
                         total_with_expiration=total_with_expiration,
                         days_ahead=days_ahead)

@app.route('/admin/vps/<int:vps_id>/delete', methods=['POST'])
@login_required
@admin_required
def admin_vps_delete(vps_id):
    vps = get_vps_by_id(vps_id)
    if not vps:
        return jsonify({'success': False, 'error': 'VPS not found'}), 404
    
    try:
        run_sync(execute_lxc(vps['container_name'], f"delete {vps['container_name']} --force", node_id=vps['node_id']))
    except:
        pass
    
    delete_vps(vps_id)
    log_activity(current_user.id, 'admin_delete_vps', 'vps', str(vps_id))
    return jsonify({'success': True})

@app.route('/admin/vps/<int:vps_id>/suspend', methods=['POST'])
@login_required
@admin_required
def admin_vps_suspend(vps_id):
    try:
        vps = get_vps_by_id(vps_id)
        if not vps:
            logger.error(f"Suspend failed: VPS {vps_id} not found")
            return jsonify({'success': False, 'error': 'VPS not found'}), 404

        if is_vps_whitelisted(vps):
            logger.warning(f"Suspend failed: VPS {vps_id} is whitelisted")
            return jsonify({
                'success': False,
                'error': 'Whitelisted VPS cannot be suspended'
            }), 403

        data = request.get_json(silent=True) or {}
        reason = data.get('reason', 'Admin action')

        container = vps['container_name']
        
        logger.info(f"Suspending VPS {vps_id} ({container}): {reason}")

        try:
            run_sync(
                execute_lxc(
                    container,
                    f"stop {container} --force",
                    node_id=vps['node_id']
                )
            )
            logger.info(f"Container {container} stopped successfully")
        except Exception as e:
            logger.warning(f"Failed to stop container {container}: {e}")

        history = vps.get('suspension_history', [])
        if not isinstance(history, list):
            history = []
        
        history.append({
            'time': datetime.now().isoformat(),
            'reason': reason,
            'by': current_user.username
        })

        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('''UPDATE vps 
                          SET suspended = 1, 
                              suspended_reason = ?, 
                              suspension_history = ?,
                              updated_at = ?
                          WHERE id = ?''',
                       (reason, json.dumps(history), datetime.now().isoformat(), vps_id))
            conn.commit()
            logger.info(f"VPS {vps_id} suspended flag set in database (rows affected: {cur.rowcount})")
        
        updated_vps = get_vps_by_id(vps_id)
        logger.info(f"VPS {vps_id} suspended status after update: {updated_vps.get('suspended')} (type: {type(updated_vps.get('suspended'))})")

        log_activity(current_user.id, 'suspend_vps', 'vps', str(vps_id), {'reason': reason})

        create_notification(
            vps['user_id'],
            'warning',
            'VPS Suspended',
            f'{container} suspended: {reason}'
        )
        
        if socketio:
            socketio.emit('vps_suspended', {
                'vps_id': vps_id,
                'reason': reason
            }, room=f'vps_{vps_id}')
            socketio.emit('vps_status_change', {
                'vps_id': vps_id,
                'status': 'suspended'
            }, room=f'user_{vps["user_id"]}')

        return jsonify({'success': True, 'message': f'VPS {container} has been suspended'})
    
    except Exception as e:
        logger.error(f"Error suspending VPS {vps_id}: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/admin/vps/<int:vps_id>/unsuspend', methods=['POST'])
@login_required
@admin_required
def admin_vps_unsuspend(vps_id):
    try:
        vps = get_vps_by_id(vps_id)
        if not vps:
            logger.error(f"Unsuspend failed: VPS {vps_id} not found")
            return jsonify({'success': False, 'error': 'VPS not found'}), 404

        logger.info(f"Unsuspending VPS {vps_id} ({vps['container_name']})")

        history = vps.get('suspension_history', [])
        if not isinstance(history, list):
            history = []
        
        history.append({
            'time': datetime.now().isoformat(),
            'reason': 'Unsuspended by admin',
            'by': current_user.username
        })

        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('''UPDATE vps 
                          SET suspended = 0, 
                              status = 'stopped',
                              suspended_reason = NULL,
                              suspension_history = ?,
                              updated_at = ?
                          WHERE id = ?''',
                       (json.dumps(history), datetime.now().isoformat(), vps_id))
            conn.commit()
            logger.info(f"VPS {vps_id} unsuspended in database (rows affected: {cur.rowcount})")
        
        updated_vps = get_vps_by_id(vps_id)
        logger.info(f"VPS {vps_id} suspended status after unsuspend: {updated_vps.get('suspended')} (type: {type(updated_vps.get('suspended'))})")

        log_activity(current_user.id, 'unsuspend_vps', 'vps', str(vps_id))

        create_notification(
            vps['user_id'],
            'success',
            'VPS Unsuspended',
            f'{vps["container_name"]} is now active.'
        )
        
        if socketio:
            socketio.emit('vps_unsuspended', {
                'vps_id': vps_id
            }, room=f'vps_{vps_id}')
            socketio.emit('vps_status_change', {
                'vps_id': vps_id,
                'status': 'stopped'
            }, room=f'user_{vps["user_id"]}')

        return jsonify({'success': True, 'message': f'VPS {vps["container_name"]} has been unsuspended'})
    
    except Exception as e:
        logger.error(f"Error unsuspending VPS {vps_id}: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/admin/vps/<int:vps_id>/renew', methods=['POST'])
@login_required
@admin_required
def admin_vps_renew(vps_id):
    try:
        vps = get_vps_by_id(vps_id)
        if not vps:
            return jsonify({'success': False, 'error': 'VPS not found'}), 404
        
        data = request.get_json() or {}
        additional_days = int(data.get('days', vps.get('expiration_days', 30)))
        
        if additional_days <= 0:
            return jsonify({'success': False, 'error': 'Days must be greater than 0'}), 400
        
        now = datetime.now()
        
        if vps.get('expires_at'):
            current_expires = datetime.fromisoformat(vps['expires_at'])
            if current_expires < now:
                new_expires = now + timedelta(days=additional_days)
            else:
                new_expires = current_expires + timedelta(days=additional_days)
        else:
            new_expires = now + timedelta(days=additional_days)
        
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('''UPDATE vps 
                          SET expires_at = ?,
                              last_renewed_at = ?,
                              renewal_count = renewal_count + 1,
                              auto_suspend_enabled = 1,
                              expiration_days = ?,
                              updated_at = ?
                          WHERE id = ?''',
                       (new_expires.isoformat(), now.isoformat(), additional_days, now.isoformat(), vps_id))
            conn.commit()
        
        log_activity(current_user.id, 'renew_vps', 'vps', str(vps_id), 
                    {'days': additional_days, 'new_expires': new_expires.isoformat()})
        
        create_notification(vps['user_id'], 'success', 'VPS Renewed', 
                          f'Your VPS {vps["hostname"]} has been renewed for {additional_days} days. New expiration: {new_expires.strftime("%Y-%m-%d %H:%M")}')
        
        if socketio:
            socketio.emit('vps_renewed', {
                'vps_id': vps_id,
                'expires_at': new_expires.isoformat(),
                'days': additional_days
            }, room=f'user_{vps["user_id"]}')
        
        return jsonify({
            'success': True, 
            'message': f'VPS renewed for {additional_days} days',
            'expires_at': new_expires.isoformat(),
            'expires_at_formatted': new_expires.strftime("%Y-%m-%d %H:%M")
        })
    
    except Exception as e:
        logger.error(f"Error renewing VPS {vps_id}: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/admin/vps/<int:vps_id>/expiration', methods=['GET', 'POST'])
@login_required
@admin_required
def admin_vps_expiration(vps_id):
    vps = get_vps_by_id(vps_id)
    if not vps:
        if request.method == 'GET' and not request.is_json:
            flash('VPS not found', 'danger')
            return redirect(url_for('admin_vps'))
        return jsonify({'success': False, 'error': 'VPS not found'}), 404
    
    if request.method == 'GET':
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('SELECT username, email FROM users WHERE id = ?', (vps['user_id'],))
            user_row = cur.fetchone()
            user = dict(user_row) if user_row else {'username': 'Unknown', 'email': ''}
        
        expires_info = {
            'vps_id': vps_id,
            'auto_suspend_enabled': bool(vps.get('auto_suspend_enabled', 0)),
            'expiration_days': vps.get('expiration_days', 0),
            'expires_at': vps.get('expires_at'),
            'last_renewed_at': vps.get('last_renewed_at'),
            'renewal_count': vps.get('renewal_count', 0),
            'is_expired': False,
            'days_remaining': None,
            'hours_remaining': None
        }
        
        if vps.get('expires_at'):
            expires_dt = datetime.fromisoformat(vps['expires_at'])
            now = datetime.now()
            time_diff = expires_dt - now
            expires_info['is_expired'] = expires_dt < now
            expires_info['days_remaining'] = time_diff.days if not expires_info['is_expired'] else 0
            expires_info['hours_remaining'] = int(time_diff.total_seconds() / 3600) if not expires_info['is_expired'] else 0
            expires_info['expires_at_formatted'] = expires_dt.strftime("%Y-%m-%d %H:%M")
        
        if request.is_json or request.args.get('format') == 'json':
            return jsonify({'success': True, 'data': expires_info})
        
        return render_template('admin/vps_expiration.html',
                              panel_name=get_setting('site_name', 'KrysolVM'),
                              vps=vps,
                              user=user,
                              expires_info=expires_info)
    
    try:
        data = request.get_json() or {}
        auto_suspend_enabled = bool(data.get('auto_suspend_enabled', False))
        expiration_days = int(data.get('expiration_days', 0))
        
        if expiration_days < 0:
            return jsonify({'success': False, 'error': 'Expiration days cannot be negative'}), 400
        
        now = datetime.now()
        expires_at = None
        
        if auto_suspend_enabled and expiration_days > 0:
            expires_at = (now + timedelta(days=expiration_days)).isoformat()
        
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('''UPDATE vps 
                          SET auto_suspend_enabled = ?,
                              expiration_days = ?,
                              expires_at = ?,
                              updated_at = ?
                          WHERE id = ?''',
                       (1 if auto_suspend_enabled else 0, expiration_days, expires_at, now.isoformat(), vps_id))
            conn.commit()
        
        log_activity(current_user.id, 'update_vps_expiration', 'vps', str(vps_id), 
                    {'auto_suspend_enabled': auto_suspend_enabled, 'expiration_days': expiration_days})
        
        return jsonify({
            'success': True, 
            'message': 'Expiration settings updated',
            'expires_at': expires_at
        })
    
    except Exception as e:
        logger.error(f"Error updating VPS expiration {vps_id}: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/admin/vps/<int:vps_id>/whitelist', methods=['POST'])
@login_required
@admin_required
def admin_vps_whitelist(vps_id):
    try:
        vps = get_vps_by_id(vps_id)
        if not vps:
            logger.error(f"Whitelist failed: VPS {vps_id} not found")
            return jsonify({'success': False, 'error': 'VPS not found'}), 404

        data = request.get_json(silent=True) or {}
        whitelist = bool(data.get('whitelist', False))
        
        logger.info(f"Setting whitelist for VPS {vps_id} to {whitelist}")

        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('''UPDATE vps 
                          SET whitelisted = ?, 
                              updated_at = ?
                          WHERE id = ?''',
                       (1 if whitelist else 0, datetime.now().isoformat(), vps_id))
            conn.commit()
            logger.info(f"VPS {vps_id} whitelisted flag set in database (rows affected: {cur.rowcount})")
        
        updated_vps = get_vps_by_id(vps_id)
        logger.info(f"VPS {vps_id} whitelisted status after update: {updated_vps.get('whitelisted')} (type: {type(updated_vps.get('whitelisted'))})")

        log_activity(
            current_user.id,
            'whitelist_vps',
            'vps',
            str(vps_id),
            {'whitelisted': whitelist}
        )
        
        action = "whitelisted" if whitelist else "removed from whitelist"
        create_notification(
            vps['user_id'],
            'info',
            'VPS Whitelist Updated',
            f'Your VPS {vps["container_name"]} has been {action}.'
        )

        return jsonify({'success': True, 'whitelisted': whitelist, 'message': f'VPS {action} successfully'})
    
    except Exception as e:
        logger.error(f"Error whitelisting VPS {vps_id}: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/admin/vps/<int:vps_id>/edit', methods=['GET', 'POST'])
@login_required
@admin_required
def admin_vps_edit(vps_id):
    vps = get_vps_by_id(vps_id)
    if not vps:
        return "VPS not found", 404

    if request.method == 'GET':
        users = get_all_users()
        nodes = get_nodes()
        
        forwards = get_user_forwards(vps['user_id'])
        vps_forwards = [f for f in forwards if f['vps_container'] == vps['container_name']]
        
        current_user_allocation = get_user_allocation(vps['user_id'])
        current_user_used = get_user_used_ports(vps['user_id'])
        
        return render_template(
            'admin/vps_edit.html',
            vps=vps,
            users=users,
            nodes=nodes,
            os_options=OS_OPTIONS,
            panel_name=get_setting('site_name', 'KrysolVM'),
            vps_forwards=vps_forwards,
            current_user_allocation=current_user_allocation,
            current_user_used=current_user_used
        )

    data = request.form

    new_user_id = data.get('user_id')
    node_id = data.get('node_id')
    ram = data.get('ram')
    cpu = data.get('cpu')
    disk = data.get('disk')
    hostname = data.get('hostname')
    ip_address = data.get('ip_address')
    ip_alias = data.get('ip_alias')
    os_version = data.get('os_version')
    additional_ports = data.get('additional_ports', '0')
    bandwidth_quota_gb = int(data.get('bandwidth_quota_gb', 0))
    
    if bandwidth_quota_gb < 0 or bandwidth_quota_gb > 10000:
        flash('Bandwidth quota must be between 0 and 10000 GB', 'danger')
        return redirect(request.url)

    if ram and (int(ram) < 1 or int(ram) > 128):
        flash('RAM must be between 1 and 128 GB', 'danger')
        return redirect(request.url)
    
    if cpu and (int(cpu) < 1 or int(cpu) > 64):
        flash('CPU must be between 1 and 64 cores', 'danger')
        return redirect(request.url)
    
    if disk and (int(disk) < 5 or int(disk) > 2000):
        flash('Disk must be between 5 and 2000 GB', 'danger')
        return redirect(request.url)

    was_running = vps['status'] == 'running' and not is_vps_suspended(vps)

    if was_running and (ram or cpu or disk or ip_address or os_version or (node_id and node_id != str(vps['node_id']))):
        try:
            run_sync(execute_lxc(
                vps['container_name'],
                f"stop {vps['container_name']}",
                node_id=vps['node_id']
            ))
            update_vps(vps_id, status='stopped')
        except Exception as e:
            flash(f"Failed to stop VPS: {e}", "danger")
            return redirect(request.url)

    try:
        if ram:
            ram_mb = int(ram) * 1024
            run_sync(execute_lxc(
                vps['container_name'],
                f"config set {vps['container_name']} limits.memory {ram_mb}MB",
                node_id=vps['node_id']
            ))

        if cpu:
            run_sync(execute_lxc(
                vps['container_name'],
                f"config set {vps['container_name']} limits.cpu {int(cpu)}",
                node_id=vps['node_id']
            ))

        if disk:
            run_sync(execute_lxc(
                vps['container_name'],
                f"config device set {vps['container_name']} root size={int(disk)}GB",
                node_id=vps['node_id']
            ))

        if ip_address:
            old_ip = vps.get('ip_address')
            if old_ip != ip_address:
                run_sync(update_routed_ip(vps['container_name'], old_ip, ip_address, vps['node_id']))

        updates = {}
        old_user_id = vps['user_id']
        owner_changed = False

        if new_user_id and new_user_id != str(vps['user_id']):
            updates['user_id'] = int(new_user_id)
            owner_changed = True
            logger.info(f"Changing VPS {vps_id} owner from {vps['user_id']} to {new_user_id}")
        
        if node_id and node_id != str(vps['node_id']):
            updates['node_id'] = int(node_id)
            logger.info(f"Moving VPS {vps_id} from node {vps['node_id']} to {node_id}")

        if ram:
            updates['ram'] = f"{ram}GB"
        if cpu:
            updates['cpu'] = str(cpu)
        if disk:
            updates['storage'] = f"{disk}GB"
        if hostname:
            updates['hostname'] = hostname
            try:
                run_sync(execute_lxc(
                    vps['container_name'],
                    f"exec {vps['container_name']} -- hostnamectl set-hostname {hostname}",
                    node_id=vps['node_id']
                ))
            except:
                pass

        if ip_address:
            updates['ip_address'] = ip_address
        if ip_alias:
            updates['ip_alias'] = ip_alias
        if os_version:
            updates['os_version'] = os_version
        
        if bandwidth_quota_gb != vps.get('bandwidth_quota_gb', 0):
            updates['bandwidth_quota_gb'] = bandwidth_quota_gb

        if updates:
            config_str = f"{updates.get('ram', vps['ram'])} RAM / {updates.get('cpu', vps['cpu'])} CPU / {updates.get('storage', vps['storage'])} Disk"
            
            quota_gb = updates.get('bandwidth_quota_gb', vps.get('bandwidth_quota_gb', 0))
            if quota_gb > 0:
                config_str += f" / {format_bandwidth_quota(quota_gb)} Quota"
            
            updates['config'] = config_str
            updates['updated_at'] = datetime.now().isoformat()
            
            with get_db() as conn:
                cur = conn.cursor()
                set_clauses = []
                values = []
                for key, value in updates.items():
                    set_clauses.append(f"{key} = ?")
                    values.append(value)
                values.append(vps_id)
                
                sql = f"UPDATE vps SET {', '.join(set_clauses)} WHERE id = ?"
                cur.execute(sql, values)
                conn.commit()
                logger.info(f"VPS {vps_id} updated in database (rows affected: {cur.rowcount})")
                logger.info(f"Updates applied: {updates}")

        if owner_changed:
            new_owner_id = int(new_user_id)
            
            with get_db() as conn:
                cur = conn.cursor()
                
                cur.execute('''
                    SELECT id, vps_port FROM port_forwards
                    WHERE vps_container = ?
                ''', (vps['container_name'],))
                port_forwards = cur.fetchall()
                
                if port_forwards:
                    ports_to_transfer = len(port_forwards)
                    
                    deallocate_ports(old_user_id, ports_to_transfer)
                    
                    allocate_ports(new_owner_id, ports_to_transfer)
                    
                    cur.execute('''
                        UPDATE port_forwards
                        SET user_id = ?
                        WHERE vps_container = ?
                    ''', (new_owner_id, vps['container_name']))
                    conn.commit()
                    
                    logger.info(f"Transferred {ports_to_transfer} port forwards from user {old_user_id} to {new_owner_id}")
                    flash(f"Transferred {ports_to_transfer} port forwards to new owner", "info")
            
            if additional_ports and int(additional_ports) > 0:
                allocate_ports(new_owner_id, int(additional_ports))
                logger.info(f"Allocated {additional_ports} additional ports to user {new_owner_id}")
                flash(f"Allocated {additional_ports} additional ports to new owner", "success")
            
            create_notification(
                old_user_id,
                'warning',
                'VPS Ownership Transferred',
                f'VPS {vps["container_name"]} has been transferred to another user by an administrator.'
            )
            
            create_notification(
                new_owner_id,
                'success',
                'VPS Transferred to You',
                f'VPS {vps["container_name"]} has been transferred to your account. All port forwards have been transferred as well.'
            )

        if was_running:
            run_sync(execute_lxc(
                vps['container_name'],
                f"start {vps['container_name']}",
                node_id=updates.get('node_id', vps['node_id'])
            ))
            run_sync(apply_internal_permissions(vps['container_name'], updates.get('node_id', vps['node_id'])))
            run_sync(recreate_port_forwards(vps['container_name']))
            
            if 'bandwidth_quota_gb' in updates:
                try:
                    final_quota_gb = updates.get('bandwidth_quota_gb', vps.get('bandwidth_quota_gb', 0))
                    
                    run_sync(configure_bandwidth_quota(
                        vps['container_name'], 
                        final_quota_gb, 
                        updates.get('node_id', vps['node_id'])
                    ))
                    logger.info(f"Applied updated bandwidth quota to {vps['container_name']}")
                except Exception as e:
                    logger.error(f"Failed to apply bandwidth quota to {vps['container_name']} after edit: {e}")
            
            with get_db() as conn:
                cur = conn.cursor()
                cur.execute('UPDATE vps SET status = ? WHERE id = ?', ('running', vps_id))
                conn.commit()

        log_activity(current_user.id, 'edit_vps', 'vps', str(vps_id), updates)

        target_user_id = updates.get('user_id', vps['user_id'])
        if not owner_changed:
            create_notification(
                target_user_id,
                'info',
                'VPS Updated',
                f'VPS {vps["container_name"]} has been updated by an administrator.'
            )

        flash("VPS updated successfully!", "success")
        return redirect(url_for('admin_vps'))

    except Exception as e:
        if was_running:
            try:
                run_sync(execute_lxc(
                    vps['container_name'],
                    f"start {vps['container_name']}",
                    node_id=vps['node_id']
                ))
            except:
                pass

        flash(str(e), "danger")
        return redirect(request.url)

@app.route('/admin/vps/<int:vps_id>/migrate', methods=['POST'])
@login_required
@admin_required
def admin_vps_migrate(vps_id):
    try:
        vps = get_vps_by_id(vps_id)
        if not vps:
            return jsonify({'success': False, 'error': 'VPS not found'}), 404
        
        data = request.get_json()
        target_node_id = data.get('target_node_id')
        
        if not target_node_id:
            return jsonify({'success': False, 'error': 'Target node ID is required'}), 400
        
        target_node_id = int(target_node_id)
        source_node_id = vps['node_id']
        
        if source_node_id == target_node_id:
            return jsonify({'success': False, 'error': 'Source and target nodes are the same'}), 400
        
        target_node = get_node(target_node_id)
        if not target_node:
            return jsonify({'success': False, 'error': 'Target node not found'}), 404
        
        if target_node['used_vps'] >= target_node['total_vps']:
            return jsonify({'success': False, 'error': 'Target node is at full capacity'}), 400
        
        container_name = vps['container_name']
        
        logger.info(f"Admin {current_user.username} initiating migration of VPS {vps_id} ({container_name}) from node {source_node_id} to node {target_node_id}")
        
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('''UPDATE vps SET 
                          status = 'migrating',
                          metadata = json_set(COALESCE(metadata, '{}'), '$.migration_started', ?)
                          WHERE id = ?''', (datetime.now().isoformat(), vps_id))
            conn.commit()
        
        import threading
        def run_migration():
            import asyncio
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(
                    live_migrate_vps(vps_id, source_node_id, target_node_id, container_name)
                )
                loop.close()
            except Exception as e:
                logger.error(f"Background migration thread error: {e}", exc_info=True)
        
        migration_thread = threading.Thread(target=run_migration, daemon=True)
        migration_thread.start()
        
        log_activity(current_user.id, 'migrate_vps', 'vps', str(vps_id),
                    {'source_node': source_node_id, 'target_node': target_node_id})
        
        return jsonify({
            'success': True,
            'message': f'Migration started for VPS {container_name}',
            'vps_id': vps_id,
            'status': 'migrating'
        })
        
    except Exception as e:
        logger.error(f"Error initiating migration for VPS {vps_id}: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/admin/vps/<int:vps_id>/migration-progress', methods=['GET'])
@login_required
@admin_required
def admin_vps_migration_progress(vps_id):
    try:
        vps = get_vps_by_id(vps_id)
        if not vps:
            return jsonify({'success': False, 'error': 'VPS not found'}), 404
        
        metadata = vps.get('metadata', {})
        if isinstance(metadata, str):
            import json
            try:
                metadata = json.loads(metadata)
            except:
                metadata = {}
        
        status = vps.get('status', 'unknown')
        progress = metadata.get('migration_progress', 0)
        message = metadata.get('migration_message', 'Preparing migration...')
        
        return jsonify({
            'success': True,
            'status': status,
            'progress': progress,
            'message': message,
            'completed': status != 'migrating'
        })
        
    except Exception as e:
        logger.error(f"Error getting migration progress for VPS {vps_id}: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/admin/vps/create', methods=['GET', 'POST'])
@login_required
@admin_required
def admin_vps_create():
    if request.method == 'GET':
        users = []
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('SELECT id, username, email FROM users ORDER BY username')
            users = [dict(row) for row in cur.fetchall()]
        
        nodes = get_nodes()
        
        return render_template('admin/vps_create.html',
                              panel_name=get_setting('site_name', 'KrysolVM'),
                              users=users,
                              nodes=nodes,
                              os_options=OS_OPTIONS)
    
    data = request.get_json()
    user_id = data.get('user_id')
    node_id = data.get('node_id')
    ram = int(data.get('ram', 2))
    cpu = int(data.get('cpu', 2))
    disk = int(data.get('disk', 20))
    os_version = data.get('os_version', 'ubuntu:22.04')
    hostname = data.get('hostname')
    ip_address = data.get('ip_address')
    ip_alias = data.get('ip_alias')
    expiration_days = int(data.get('expiration_days', 0))
    auto_suspend_enabled = bool(data.get('auto_suspend_enabled', False))
    bandwidth_quota_gb = int(data.get('bandwidth_quota_gb', 0))
    
    if bandwidth_quota_gb < 0 or bandwidth_quota_gb > 10000:
        return jsonify({'success': False, 'error': 'Bandwidth quota must be between 0 and 10000 GB'}), 400
    
    if not all([user_id, node_id]):
        return jsonify({'success': False, 'error': 'Missing parameters'}), 400
    
    node = get_node(node_id)
    if not node:
        return jsonify({'success': False, 'error': 'Node not found'}), 404
    
    current_count = get_current_vps_count(node_id)
    if current_count >= node['total_vps']:
        return jsonify({'success': False, 'error': 'Node at full capacity'}), 400
    
    max_vps = int(get_setting('max_vps_per_user', '10'))
    user_vps_count = len(get_vps_for_user(user_id))
    if user_vps_count >= max_vps:
        return jsonify({'success': False, 'error': f'User has reached maximum VPS limit ({max_vps})'}), 400
    
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('SELECT COUNT(*) FROM vps WHERE user_id = ?', (user_id,))
            vps_count = cur.fetchone()[0] + 1
        
        container_name = f"krysolvm-vps-{user_id}-{vps_count}"
        if hostname:
            container_name = hostname.lower().replace(' ', '-').replace('_', '-')
            container_name = re.sub(r'[^a-z0-9\-]', '', container_name)
        
        ram_mb = ram * 1024
        
        config_str = f"{ram}GB RAM / {cpu} CPU / {disk}GB Disk"
        if bandwidth_quota_gb > 0:
            config_str += f" / {format_bandwidth_quota(bandwidth_quota_gb)} Quota"
        
        vps_id = create_vps(
            user_id=user_id,
            node_id=node_id,
            container_name=container_name,
            hostname=hostname or container_name,
            ram=f"{ram}GB",
            cpu=str(cpu),
            storage=f"{disk}GB",
            config=config_str,
            os_version=os_version,
            ip_address=ip_address,
            ip_alias=ip_alias,
            expiration_days=expiration_days,
            auto_suspend_enabled=auto_suspend_enabled,
            bandwidth_quota_gb=bandwidth_quota_gb,
            status='installing'
        )
        
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('''UPDATE vps SET metadata = json_set(COALESCE(metadata, '{}'), '$.installation_started', ?)
                          WHERE id = ?''', (datetime.now().isoformat(), vps_id))
            conn.commit()
        
        import threading
        def run_installation():
            import asyncio
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(
                    install_vps_async(vps_id, container_name, node_id, ram_mb, cpu, disk, 
                                     os_version, ip_address, bandwidth_quota_gb)
                )
                loop.close()
            except Exception as e:
                logger.error(f"Background installation thread error: {e}", exc_info=True)
        
        installation_thread = threading.Thread(target=run_installation, daemon=True)
        installation_thread.start()
        
        log_activity(current_user.id, 'admin_create_vps', 'vps', str(vps_id),
                    {'user_id': user_id, 'container': container_name})
        create_notification(user_id, 'info', 'VPS Installation Started', 
                          f'Your VPS {container_name} installation has started. This may take a few minutes.')
        
        return jsonify({'success': True, 'vps_id': vps_id, 'container_name': container_name, 'status': 'installing'})
    except Exception as e:
        logger.error(f"VPS creation error: {e}")
        
        error_message = str(e)
        if "Circuit breaker open" in error_message:
            node = get_node(node_id)
            node_name = node['name'] if node else f"Node {node_id}"
            
            health_status = get_node_health_status(node_id)
            retry_time = health_status.get('retry_in_seconds', 0)
            
            if retry_time > 0:
                error_message = f"Node '{node_name}' is temporarily unavailable due to recent errors. Please try again in {retry_time} seconds, or contact administrator to reset the node."
            else:
                error_message = f"Node '{node_name}' is temporarily unavailable. Please try a different node or contact administrator."
        
        try:
            run_sync(execute_lxc(container_name, f"delete {container_name} --force", node_id=node_id, operation_type="general"))
        except:
            pass
            
        return jsonify({'success': False, 'error': error_message}), 500

@app.route('/admin/nodes')
@login_required
@admin_required
def admin_nodes():
    try:
        nodes = get_nodes()
        
        for node in nodes:
            try:
                node['vps_count'] = get_current_vps_count(node['id'])
                
                status = run_sync(get_node_status(node['id']))
                node['online'] = status.get('online', False)
                node['status'] = status.get('status', 'Unknown')
                node['last_seen'] = status.get('last_seen')
                node['stats'] = status.get('stats')
                
                health_status = get_node_health_status(node['id'])
                node['circuit_breaker_open'] = health_status['circuit_breaker_open']
                node['health_status'] = health_status['status']
                
            except Exception as e:
                logger.error(f"Error getting status for node {node['id']}: {e}")
                node['vps_count'] = 0
                node['online'] = False
                node['status'] = 'Error'
                node['circuit_breaker_open'] = False
                node['health_status'] = 'error'
        
        return render_template('admin/nodes.html',
                              panel_name=get_setting('site_name', 'KrysolVM'),
                              nodes=nodes,
                              socketio_available=SOCKETIO_AVAILABLE)
    
    except Exception as e:
        logger.error(f"Error in admin_nodes: {e}", exc_info=True)
        flash('Error loading nodes', 'danger')
        return redirect(url_for('admin_dashboard'))

@app.route('/admin/nodes/circuit-breakers')
@login_required
@admin_required
def admin_circuit_breakers():
    circuit_status = get_all_circuit_breaker_status()
    nodes = get_nodes()
    
    return render_template('admin/circuit_breakers.html',
                          panel_name=get_setting('site_name', 'KrysolVM'),
                          circuit_status=circuit_status,
                          nodes=nodes)

@app.route('/admin/nodes/circuit-breakers/reset/<int:node_id>', methods=['POST'])
@login_required
@admin_required
def admin_reset_circuit_breaker(node_id):
    try:
        node = get_node(node_id)
        if not node:
            return jsonify({'success': False, 'error': 'Node not found'}), 404
        
        reset_node_circuit_breaker(node_id)
        
        log_activity(current_user.id, f"Reset circuit breaker for node {node['name']}", 'node')
        
        return jsonify({
            'success': True, 
            'message': f'Circuit breaker reset for node {node["name"]}'
        })
    
    except Exception as e:
        logger.error(f"Error resetting circuit breaker for node {node_id}: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/admin/nodes/failures/reset/<int:node_id>', methods=['POST'])
@login_required
@admin_required
def admin_reset_node_failures(node_id):
    try:
        node = get_node(node_id)
        if not node:
            return jsonify({'success': False, 'error': 'Node not found'}), 404
        
        if node_id in node_circuit_breakers:
            old_failures = node_circuit_breakers[node_id]['failures']
            old_500_failures = node_circuit_breakers[node_id].get('http_500_failures', 0)
            
            node_circuit_breakers[node_id]['failures'] = 0
            node_circuit_breakers[node_id]['http_500_failures'] = 0
            node_circuit_breakers[node_id]['last_failure'] = 0
            node_circuit_breakers[node_id]['last_500_failure'] = 0
            
            logger.info(f"Manually reset all failures for node {node_id}: {old_failures} failures, {old_500_failures} HTTP 500 errors")
        
        log_activity(current_user.id, f"Reset all failures for node {node['name']}", 'node')
        
        return jsonify({
            'success': True, 
            'message': f'All failures reset for node {node["name"]}'
        })
    
    except Exception as e:
        logger.error(f"Error resetting failures for node {node_id}: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/nodes/health')
@login_required
@admin_required
def api_nodes_health():
    try:
        nodes = get_nodes()
        nodes_health = []
        
        for node in nodes:
            health_info = get_node_availability_info(node['id'])
            vps_count = get_current_vps_count(node['id'])
            
            if health_info:
                nodes_health.append({
                    'id': node['id'],
                    'name': node['name'],
                    'location': node['location'],
                    'is_local': node['is_local'],
                    'is_available': health_info['is_available'],
                    'health_status': health_info['health_status'],
                    'message': health_info['message'],
                    'retry_in_seconds': health_info['retry_in_seconds'],
                    'used_vps': vps_count,
                    'total_vps': node['total_vps']
                })
        
        return jsonify({'success': True, 'nodes': nodes_health})
        
    except Exception as e:
        logger.error(f"Error getting nodes health: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/admin/nodes/test-connection', methods=['POST'])
@login_required
@admin_required
def admin_node_test_connection():
    try:
        data = request.get_json()
        url = data.get('url', '').strip()
        verify_ssl = data.get('verify_ssl', True)
        
        if not url:
            return jsonify({'success': False, 'error': 'URL is required'}), 400
        
        if not url.startswith(('http://', 'https://')):
            url = f"http://{url}"
        
        url = url.rstrip('/')
        
        import requests
        try:
            response = requests.get(f"{url}/api/health", timeout=10, verify=verify_ssl)
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    return jsonify({
                        'success': True,
                        'message': 'Connection successful',
                        'node_info': {
                            'hostname': data.get('hostname', 'Unknown'),
                            'version': data.get('version', 'Unknown'),
                            'status': data.get('status', 'online'),
                            'service': data.get('service', 'Unknown')
                        }
                    })
                except:
                    return jsonify({
                        'success': True,
                        'message': 'Connection successful (basic)',
                        'node_info': None
                    })
            elif response.status_code == 404:
                try:
                    response = requests.get(f"{url}/", timeout=5, verify=verify_ssl)
                    if response.status_code == 200:
                        return jsonify({
                            'success': True,
                            'message': 'Connection successful (server reachable)',
                            'node_info': {
                                'hostname': 'Unknown',
                                'version': 'Unknown',
                                'status': 'online',
                                'note': 'Update node.py to latest version for full health check'
                            }
                        })
                except:
                    pass
                
                return jsonify({
                    'success': False,
                    'error': 'Node is reachable but /api/health endpoint not found. Please update node.py to the latest version.'
                }), 400
            else:
                return jsonify({
                    'success': False,
                    'error': f'Node returned status code {response.status_code}'
                }), 400
        
        except requests.exceptions.Timeout:
            return jsonify({
                'success': False,
                'error': 'Connection timeout - node is not responding'
            }), 400
        
        except requests.exceptions.ConnectionError:
            return jsonify({
                'success': False,
                'error': 'Connection refused - check if node is running and URL is correct'
            }), 400
        
        except requests.exceptions.SSLError:
            return jsonify({
                'success': False,
                'error': 'SSL certificate error - check HTTPS configuration'
            }), 400
        
        except Exception as e:
            return jsonify({
                'success': False,
                'error': f'Connection error: {str(e)}'
            }), 400
    
    except Exception as e:
        logger.error(f"Test connection error: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/admin/nodes/create', methods=['GET', 'POST'])
@login_required
@admin_required
def admin_node_create():
    if request.method == 'GET':
        return render_template('admin/nodes_create.html',
                              panel_name=get_setting('site_name', 'KrysolVM'))
    
    data = request.get_json() or request.form.to_dict()
    
    name = data.get('name')
    location = data.get('location')
    total_vps = int(data.get('total_vps', 50))
    tags = data.get('tags', '').split(',') if data.get('tags') else []
    url = data.get('url', '').strip()
    verify_ssl = 1 if data.get('verify_ssl', True) else 0
    ip_addresses = data.get('ip_addresses', '').split(',') if data.get('ip_addresses') else []
    ip_aliases = data.get('ip_aliases', '').split(',') if data.get('ip_aliases') else []
    
    if not name or not location:
        return jsonify({'success': False, 'error': 'Name and location required'}), 400
    
    if url:
        url = url.rstrip('/')
        
        if not url.startswith(('http://', 'https://')):
            url = f"http://{url}"
        
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            
            if parsed.scheme not in ['http', 'https']:
                return jsonify({'success': False, 'error': 'URL must use http:// or https://'}), 400
            
            if not parsed.netloc:
                return jsonify({'success': False, 'error': 'Invalid URL format'}), 400
            
            url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
            
        except Exception as e:
            return jsonify({'success': False, 'error': f'Invalid URL: {str(e)}'}), 400
    
    tags = [t.strip() for t in tags if t.strip()]
    ip_addresses = [ip.strip() for ip in ip_addresses if ip.strip()]
    ip_aliases = [alias.strip() for alias in ip_aliases if alias.strip()]
    
    tags_json = json.dumps(tags)
    ip_addresses_json = json.dumps(ip_addresses)
    ip_aliases_json = json.dumps(ip_aliases)
    
    is_local = 1 if not url else 0
    api_key = None if is_local else generate_api_key()
    now = datetime.now().isoformat()
    
    with get_db() as conn:
        cur = conn.cursor()
        try:
            cur.execute('''INSERT INTO nodes 
                (name, location, total_vps, tags, api_key, url, is_local, verify_ssl,
                 ip_addresses, ip_aliases, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (name, location, total_vps, tags_json, api_key, url, is_local, verify_ssl,
                 ip_addresses_json, ip_aliases_json, now, now))
            conn.commit()
            node_id = cur.lastrowid
            
            if is_local:
                cur.execute('''INSERT OR REPLACE INTO settings (key, value, updated_at) VALUES (?, ?, ?)''',
                           ('local_node_deleted', '0', datetime.now().isoformat()))
                conn.commit()
                logger.info("Cleared local_node_deleted flag - local node manually created")
            
            logger.info(f"Node created: {name} (ID: {node_id}) - {'Local' if is_local else f'Remote: {url}'} - SSL Verify: {bool(verify_ssl)}")
        except sqlite3.IntegrityError:
            return jsonify({'success': False, 'error': 'Node name already exists'}), 400
    
    log_activity(current_user.id, 'create_node', 'node', str(node_id), {'name': name, 'url': url})
    return jsonify({'success': True, 'node_id': node_id, 'api_key': api_key})

@app.route('/admin/nodes/<int:node_id>/edit', methods=['GET', 'POST'])
@login_required
@admin_required
def admin_node_edit(node_id):
    node = get_node(node_id)
    if not node:
        if request.method == 'GET':
            flash('Node not found', 'danger')
            return redirect(url_for('admin_nodes'))
        return jsonify({'success': False, 'error': 'Node not found'}), 404
    
    if request.method == 'GET':
        vps_count = get_current_vps_count(node_id)
        
        try:
            status = run_sync(get_node_status(node_id))
        except Exception as e:
            logger.error(f"Error getting node status: {e}")
            status = {'status': 'unknown', 'online': False}
        
        try:
            tags = json.loads(node['tags']) if node['tags'] else []
            tags_str = ', '.join(tags)
        except:
            tags_str = ''
        
        try:
            ip_addresses = json.loads(node['ip_addresses']) if node['ip_addresses'] else []
            ip_addresses_str = ', '.join(ip_addresses)
            ip_addresses_count = len(ip_addresses)
        except:
            ip_addresses_str = ''
            ip_addresses_count = 0
        
        try:
            ip_aliases = json.loads(node['ip_aliases']) if node['ip_aliases'] else []
            ip_aliases_str = ', '.join(ip_aliases)
        except:
            ip_aliases_str = ''
        
        return render_template('admin/node_edit.html',
                              panel_name=get_setting('site_name', 'KrysolVM'),
                              node=node,
                              status=status,
                              vps_count=vps_count,
                              tags_str=tags_str,
                              ip_addresses_str=ip_addresses_str,
                              ip_addresses_count=ip_addresses_count,
                              ip_aliases_str=ip_aliases_str)
    
    data = request.get_json() or request.form.to_dict()
    
    with get_db() as conn:
        cur = conn.cursor()
        
        if 'name' in data:
            cur.execute('UPDATE nodes SET name = ? WHERE id = ?', (data['name'], node_id))
        if 'location' in data:
            cur.execute('UPDATE nodes SET location = ? WHERE id = ?', (data['location'], node_id))
        if 'total_vps' in data:
            cur.execute('UPDATE nodes SET total_vps = ? WHERE id = ?', (int(data['total_vps']), node_id))
        if 'tags' in data:
            tags = [t.strip() for t in data['tags'].split(',') if t.strip()]
            cur.execute('UPDATE nodes SET tags = ? WHERE id = ?', (json.dumps(tags), node_id))
        if 'url' in data and not node['is_local']:
            cur.execute('UPDATE nodes SET url = ? WHERE id = ?', (data['url'], node_id))
        if 'verify_ssl' in data and not node['is_local']:
            verify_ssl = 1 if data['verify_ssl'] else 0
            cur.execute('UPDATE nodes SET verify_ssl = ? WHERE id = ?', (verify_ssl, node_id))
        if 'ip_addresses' in data:
            ips = [ip.strip() for ip in data['ip_addresses'].split(',') if ip.strip()]
            cur.execute('UPDATE nodes SET ip_addresses = ? WHERE id = ?', (json.dumps(ips), node_id))
        if 'ip_aliases' in data:
            aliases = [alias.strip() for alias in data['ip_aliases'].split(',') if alias.strip()]
            cur.execute('UPDATE nodes SET ip_aliases = ? WHERE id = ?', (json.dumps(aliases), node_id))
        
        cur.execute('UPDATE nodes SET updated_at = ? WHERE id = ?', (datetime.now().isoformat(), node_id))
        conn.commit()
    
    log_activity(current_user.id, 'edit_node', 'node', str(node_id))
    
    if request.is_json:
        return jsonify({'success': True})
    else:
        flash('Node updated successfully', 'success')
        return redirect(url_for('admin_nodes'))

@app.route('/admin/nodes/<int:node_id>/regenerate-key', methods=['POST'])
@login_required
@admin_required
def admin_node_regenerate_key(node_id):
    node = get_node(node_id)
    if not node:
        return jsonify({'success': False, 'error': 'Node not found'}), 404
    
    if node['is_local']:
        return jsonify({'success': False, 'error': 'Cannot regenerate key for local node'}), 400
    
    new_key = generate_api_key()
    
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('UPDATE nodes SET api_key = ?, updated_at = ? WHERE id = ?',
                   (new_key, datetime.now().isoformat(), node_id))
        conn.commit()
    
    log_activity(current_user.id, 'regenerate_node_key', 'node', str(node_id))
    return jsonify({'success': True, 'api_key': new_key})

@app.route('/admin/nodes/<int:node_id>/delete', methods=['POST'])
@login_required
@admin_required
def admin_node_delete(node_id):
    try:
        node = get_node(node_id)
        if not node:
            return jsonify({'success': False, 'error': 'Node not found'}), 404
        
        data = request.get_json() or {}
        force = data.get('force', False)
        
        if node['is_local']:
            logger.warning(f"Admin {current_user.username} is deleting LOCAL node {node_id} ({node['name']})")
        
        vps_count = get_current_vps_count(node_id)
        if not force and vps_count > 0:
            return jsonify({
                'success': False, 
                'error': f'Node has {vps_count} VPS. Use force to delete all.',
                'vps_count': vps_count,
                'requires_force': True
            }), 400
        
        if force and vps_count > 0:
            logger.info(f"Force deleting {vps_count} VPS from node {node_id}")
            with get_db() as conn:
                cur = conn.cursor()
                cur.execute('SELECT id, container_name FROM vps WHERE node_id = ?', (node_id,))
                vps_list = cur.fetchall()
                
                for row in vps_list:
                    vps_id, container_name = row[0], row[1]
                    try:
                        run_sync(execute_lxc(container_name, f"delete {container_name} --force", node_id=node_id))
                        logger.info(f"Deleted container {container_name} from node {node_id}")
                    except Exception as e:
                        logger.warning(f"Failed to delete container {container_name}: {e}")
                
                cur.execute('DELETE FROM vps WHERE node_id = ?', (node_id,))
                
                cur.execute('''DELETE FROM port_forwards 
                              WHERE vps_container IN (SELECT container_name FROM vps WHERE node_id = ?)''', 
                           (node_id,))
                
                conn.commit()
                logger.info(f"Deleted {vps_count} VPS records from database")
        
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('DELETE FROM nodes WHERE id = ?', (node_id,))
            
            if node['is_local']:
                cur.execute('''INSERT OR REPLACE INTO settings (key, value, updated_at) VALUES (?, ?, ?)''',
                           ('local_node_deleted', '1', datetime.now().isoformat()))
                logger.info("Set local_node_deleted flag to prevent auto-recreation")
            
            conn.commit()
        
        log_activity(current_user.id, 'delete_node', 'node', str(node_id), 
                    {'name': node['name'], 'is_local': node['is_local'], 'vps_deleted': vps_count if force else 0})
        
        logger.info(f"Node {node_id} ({node['name']}) deleted successfully by {current_user.username}")
        
        return jsonify({
            'success': True,
            'message': f"Node '{node['name']}' deleted successfully" + (f" along with {vps_count} VPS" if force and vps_count > 0 else "")
        })
    except Exception as e:
        logger.error(f"Error deleting node {node_id}: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/admin/settings', methods=['GET', 'POST'])
@login_required
@admin_required
def admin_settings():
    if request.method == 'POST':
        data = request.form

        settings = [
            'site_name', 'site_description', 'header_icon', 'favicon',
            'footer_text', 'maintenance_message',
            'cpu_threshold', 'ram_threshold',
            'default_port_quota', 'max_vps_per_user', 'session_timeout',
            'backup_retention', 'theme', 'language', 'timezone',
            'discord_client_id', 'discord_client_secret', 'discord_redirect_uri', 'discord_button_text',
            'video_background_url',
            'smtp_host', 'smtp_port', 'smtp_username', 'smtp_password',
            'smtp_from_email', 'smtp_from_name'
        ]

        for key in settings:
            set_setting(key, data.get(key, ''))

        set_setting('maintenance_mode',
                    '1' if 'maintenance_mode' in data else '0')

        set_setting('registration_enabled',
                    '1' if 'registration_enabled' in data else '0')

        set_setting('backup_enabled',
                    '1' if 'backup_enabled' in data else '0')
        
        set_setting('discord_auth_enabled',
                    '1' if 'discord_auth_enabled' in data else '0')
        
        set_setting('discord_auto_register',
                    '1' if 'discord_auto_register' in data else '0')
        
        set_setting('video_background_enabled',
                    '1' if 'video_background_enabled' in data else '0')

        set_setting('smtp_use_tls',
                    '1' if 'smtp_use_tls' in data else '0')
        
        set_setting('smtp_use_ssl',
                    '1' if 'smtp_use_ssl' in data else '0')

        log_activity(current_user.id, 'update_settings', 'settings')
        create_notification(
            current_user.id,
            'success',
            'Settings Updated',
            'Panel settings have been updated.'
        )

        flash('Settings updated successfully', 'success')
        return redirect(url_for('admin_settings'))

    settings = {}
    keys = [
        'site_name', 'site_description', 'header_icon', 'favicon',
        'footer_text', 'maintenance_mode', 'maintenance_message',
        'registration_enabled', 'cpu_threshold', 'ram_threshold',
        'default_port_quota', 'max_vps_per_user', 'session_timeout',
        'backup_enabled', 'backup_retention', 'theme', 'language', 'timezone',
        'discord_auth_enabled', 'discord_client_id', 'discord_client_secret',
        'discord_redirect_uri', 'discord_auto_register', 'discord_button_text',
        'video_background_enabled', 'video_background_url',
        'smtp_host', 'smtp_port', 'smtp_username', 'smtp_password',
        'smtp_from_email', 'smtp_from_name', 'smtp_use_tls', 'smtp_use_ssl'
    ]

    for key in keys:
        settings[key] = get_setting(key, '')

    return render_template(
        'admin/settings.html',
        panel_name=get_setting('site_name', 'KrysolVM'),
        settings=settings
    )

@app.route('/admin/settings/test-smtp', methods=['POST'])
@login_required
@admin_required
def admin_test_smtp():
    try:
        data = request.get_json()
        test_email = data.get('email', current_user.email)
        
        if not test_email:
            return jsonify({'success': False, 'error': 'No email address provided'}), 400
        
        site_name = get_setting('site_name', 'KrysolVM')
        subject = f"SMTP Test - {site_name}"
        
        text_body = f"""Hello,

This is a test email to verify your SMTP configuration for {site_name}.

If you received this email, your SMTP settings are working correctly!

Test Details:
- Sent at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
- From: {site_name} Admin Panel
- To: {test_email}

Best regards,
{site_name} Team"""

        html_body = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>SMTP Test</title>
    <style>
        body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
        .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
        .header {{ background: #10b981; color: white; padding: 20px; text-align: center; border-radius: 8px 8px 0 0; }}
        .content {{ background: #f8f9fa; padding: 30px; border-radius: 0 0 8px 8px; }}
        .success {{ background: #d1fae5; border: 1px solid #10b981; padding: 15px; border-radius: 6px; margin: 20px 0; }}
        .details {{ background: #e5e7eb; padding: 15px; border-radius: 6px; margin: 20px 0; font-family: monospace; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>{site_name}</h1>
            <p>SMTP Configuration Test</p>
        </div>
        <div class="content">
            <div class="success">
                <strong>✅ Success!</strong> Your SMTP configuration is working correctly.
            </div>
            <p>This is a test email to verify your SMTP settings for <strong>{site_name}</strong>.</p>
            <p>If you received this email, your SMTP configuration is properly set up and ready to send password reset emails and notifications.</p>
            
            <div class="details">
                <strong>Test Details:</strong><br>
                Sent at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}<br>
                From: {site_name} Admin Panel<br>
                To: {test_email}
            </div>
            
            <p>Best regards,<br>{site_name} Team</p>
        </div>
    </div>
</body>
</html>"""
        
        success, error_msg = send_email(test_email, subject, text_body, html_body)
        
        if success:
            logger.info(f"SMTP test email sent successfully to {test_email} by admin {current_user.username}")
            log_activity(current_user.id, 'test_smtp', 'settings', '', {'test_email': test_email})
            return jsonify({
                'success': True, 
                'message': f'Test email sent successfully to {test_email}. Please check your inbox.'
            })
        else:
            logger.error(f"SMTP test failed for {test_email}: {error_msg}")
            return jsonify({
                'success': False, 
                'error': f'Failed to send test email: {error_msg}'
            }), 500
            
    except Exception as e:
        logger.error(f"SMTP test error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/admin/settings/upload-header-icon', methods=['POST'])
@login_required
@admin_required
def upload_header_icon():
    if 'icon' not in request.files:
        return jsonify({'success': False, 'error': 'No file uploaded'}), 400
    
    file = request.files['icon']
    if file.filename == '':
        return jsonify({'success': False, 'error': 'No file selected'}), 400
    
    if not allowed_file(file.filename):
        return jsonify({'success': False, 'error': 'Invalid file type'}), 400
    
    filename = secure_filename(f"header_icon_{int(time.time())}.{file.filename.rsplit('.', 1)[1].lower()}")
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], 'settings', filename)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    file.save(filepath)
    
    if PIL_AVAILABLE and Image:
        try:
            img = Image.open(filepath)
            img.thumbnail((64, 64), Image.Resampling.LANCZOS)
            img.save(filepath, optimize=True, quality=85)
        except:
            pass
    
    icon_path = f'/static/uploads/settings/{filename}'
    
    set_setting('header_icon', icon_path)
    
    return jsonify({'success': True, 'path': icon_path})

@app.route('/admin/settings/upload-favicon', methods=['POST'])
@login_required
@admin_required
def upload_favicon():
    if 'icon' not in request.files:
        return jsonify({'success': False, 'error': 'No file uploaded'}), 400
    
    file = request.files['icon']
    if file.filename == '':
        return jsonify({'success': False, 'error': 'No file selected'}), 400
    
    ext = file.filename.rsplit('.', 1)[1].lower() if '.' in file.filename else ''
    if ext not in ['ico', 'png']:
        return jsonify({'success': False, 'error': 'Invalid file type. Please upload .ico or .png'}), 400
    
    filename = f"favicon_{int(time.time())}.{ext}"
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], 'settings', filename)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    file.save(filepath)
    
    icon_path = f'/static/uploads/settings/{filename}'
    
    set_setting('favicon', icon_path)
    
    return jsonify({'success': True, 'path': icon_path})

@app.route('/admin/maintenance')
@login_required
@main_admin_required
def admin_maintenance():
    return render_template('admin/maintenance.html',
                          panel_name=get_setting('site_name', 'KrysolVM'))

@app.route('/admin/backup', methods=['POST'])
@login_required
@main_admin_required
def admin_backup():
    backup_name = f"krysolvm_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
    
    try:
        os.makedirs('backups', exist_ok=True)
        backup_path = os.path.join('backups', backup_name)
        
        shutil.copy(DATABASE_PATH, backup_path)
        
        if os.path.exists(f"{DATABASE_PATH}-wal"):
            shutil.copy(f"{DATABASE_PATH}-wal", f"{backup_path}-wal")
        if os.path.exists(f"{DATABASE_PATH}-shm"):
            shutil.copy(f"{DATABASE_PATH}-shm", f"{backup_path}-shm")
        
        log_activity(current_user.id, 'create_backup', 'system', None, {'name': backup_name})
        create_notification(current_user.id, 'success', 'Backup Created', f'Database backup {backup_name} created.')
        return send_file(backup_path, as_attachment=True, download_name=backup_name)
    except Exception as e:
        flash(f'Backup failed: {e}', 'danger')
        return redirect(url_for('admin_maintenance'))

@app.route('/admin/backup/list')
@login_required
@main_admin_required
def admin_backup_list():
    backups = []
    backup_dir = 'backups'
    if os.path.exists(backup_dir):
        for file in os.listdir(backup_dir):
            if file.endswith('.db'):
                path = os.path.join(backup_dir, file)
                backups.append({
                    'name': file,
                    'size': os.path.getsize(path),
                    'modified': datetime.fromtimestamp(os.path.getmtime(path)).isoformat()
                })
    
    backups.sort(key=lambda x: x['modified'], reverse=True)
    
    return render_template('admin/backup_list.html',
                          panel_name=get_setting('site_name', 'KrysolVM'),
                          backups=backups)

@app.route('/admin/backup/restore/<filename>', methods=['POST'])
@login_required
@main_admin_required
def admin_backup_restore(filename):
    backup_path = os.path.join('backups', filename)
    
    if not os.path.exists(backup_path):
        return jsonify({'success': False, 'error': 'Backup not found'}), 404
    
    try:
        current_backup = f"krysolvm_pre_restore_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        shutil.copy(DATABASE_PATH, os.path.join('backups', current_backup))
        
        shutil.copy(backup_path, DATABASE_PATH)
        
        log_activity(current_user.id, 'restore_backup', 'system', None, {'name': filename})
        create_notification(current_user.id, 'success', 'Backup Restored', f'Database restored from {filename}')
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/admin/backup/download/<filename>')
@login_required
@main_admin_required
def admin_backup_download(filename):
    backup_path = os.path.join('backups', filename)
    
    if not os.path.exists(backup_path):
        flash('Backup not found', 'danger')
        return redirect(url_for('admin_backup_list'))
    
    try:
        return send_file(backup_path, as_attachment=True, download_name=filename)
    except Exception as e:
        flash(f'Failed to download backup: {str(e)}', 'danger')
        return redirect(url_for('admin_backup_list'))

@app.route('/admin/backup/delete/<filename>', methods=['POST'])
@login_required
@main_admin_required
def admin_backup_delete(filename):
    backup_path = os.path.join('backups', filename)
    
    if not os.path.exists(backup_path):
        return jsonify({'success': False, 'error': 'Backup not found'}), 404
    
    try:
        os.remove(backup_path)
        log_activity(current_user.id, 'delete_backup', 'system', None, {'name': filename})
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/admin/resource-check', methods=['POST'])
@login_required
@admin_required
def admin_resource_check():
    cpu_threshold = int(get_setting('cpu_threshold', 90))
    ram_threshold = int(get_setting('ram_threshold', 90))
    
    suspended_count = 0
    vps_list = get_all_vps()
    
    for vps in vps_list:
        if vps['status'] == 'running' and not vps['suspended'] and not vps['whitelisted']:
            try:
                stats = run_sync(get_container_stats(vps['container_name'], vps['node_id']))
                cpu = stats['cpu']
                ram = stats['ram']['pct']
                
                if cpu > cpu_threshold or ram > ram_threshold:
                    reason = f"High resource usage: CPU {cpu:.1f}%, RAM {ram:.1f}%"
                    
                    run_sync(execute_lxc(vps['container_name'], f"stop {vps['container_name']} --force", node_id=vps['node_id']))
                    
                    history = vps['suspension_history']
                    history.append({
                        'time': datetime.now().isoformat(),
                        'reason': reason,
                        'by': 'Auto Resource Check'
                    })
                    
                    update_vps(vps['id'], suspended=1, status='stopped', suspension_history=history, suspended_reason=reason)
                    suspended_count += 1
                    log_activity(None, 'auto_suspend', 'vps', str(vps['id']), {'reason': reason})
                    create_notification(vps['user_id'], 'warning', 'VPS Auto-Suspended', 
                                      f'Your VPS {vps["container_name"]} has been suspended due to high resource usage.')
                    
                    if socketio:
                        socketio.emit('vps_suspended', {
                            'vps_id': vps['id'],
                            'reason': reason
                        }, room=f'vps_{vps["id"]}')
                        
            except Exception as e:
                logger.error(f"Resource check error for {vps['container_name']}: {e}")
    
    return jsonify({'success': True, 'suspended': suspended_count})

@app.route('/admin/system-info')
@login_required
@main_admin_required
def admin_system_info():
    import platform
    
    try:
        import psutil
        PSUTIL_AVAILABLE = True
    except ImportError:
        PSUTIL_AVAILABLE = False
        psutil = None
    
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest' or request.args.get('format') == 'json':
        return get_system_info_json()
    
    system_info = get_system_info_dict()
    
    return render_template('admin/system_info.html',
                         panel_name=get_setting('site_name', 'KrysolVM'),
                         system_info=system_info,
                         psutil_available=PSUTIL_AVAILABLE)

def get_system_info_json():
    import platform
    
    try:
        lxc_version = subprocess.run(['lxc', '--version'], capture_output=True, text=True, timeout=5).stdout.strip()
    except:
        lxc_version = None
    
    try:
        python_version = platform.python_version()
    except:
        python_version = None
    
    disk_usage = {}
    for path in ['/', '/var', '/home']:
        if os.path.exists(path):
            try:
                usage = shutil.disk_usage(path)
                disk_usage[path] = {
                    'total': usage.total // (1024**3),
                    'used': usage.used // (1024**3),
                    'free': usage.free // (1024**3)
                }
            except:
                pass
    
    return jsonify({
        'hostname': platform.node(),
        'platform': platform.platform(),
        'python': python_version,
        'uptime': get_host_uptime(),
        'cpu_cores': os.cpu_count(),
        'cpu_percent': get_host_cpu_usage(),
        'memory': get_host_ram_usage(),
        'disk': get_host_disk_usage(),
        'disk_detailed': disk_usage,
        'lxc_version': lxc_version,
        'database_size': os.path.getsize(DATABASE_PATH) // (1024 * 1024) if os.path.exists(DATABASE_PATH) else 0
    })

def get_system_info_dict():
    import platform
    
    try:
        import psutil
        PSUTIL_AVAILABLE = True
    except ImportError:
        PSUTIL_AVAILABLE = False
        psutil = None
    
    system_info = {}
    
    try:
        system_info['hostname'] = platform.node()
        system_info['platform'] = platform.platform()
        system_info['system'] = platform.system()
        system_info['release'] = platform.release()
        system_info['version'] = platform.version()
        system_info['machine'] = platform.machine()
        system_info['processor'] = platform.processor()
        system_info['python_version'] = platform.python_version()
        system_info['python_implementation'] = platform.python_implementation()
    except Exception as e:
        logger.error(f"Error getting basic system info: {e}")
    
    try:
        system_info['uptime'] = get_host_uptime()
    except:
        system_info['uptime'] = 'Unknown'
    
    try:
        system_info['cpu_count'] = os.cpu_count()
        system_info['cpu_usage'] = get_host_cpu_usage()
        
        if platform.system() == 'Linux':
            try:
                cpu_info_cmd = subprocess.run(['lscpu'], capture_output=True, text=True, timeout=5)
                if cpu_info_cmd.returncode == 0:
                    cpu_lines = cpu_info_cmd.stdout.strip().split('\n')
                    cpu_details = {}
                    for line in cpu_lines:
                        if ':' in line:
                            key, value = line.split(':', 1)
                            cpu_details[key.strip()] = value.strip()
                    system_info['cpu_details'] = cpu_details
            except:
                pass
    except Exception as e:
        logger.error(f"Error getting CPU info: {e}")
    
    try:
        ram = get_host_ram_usage()
        system_info['memory'] = ram
        
        if PSUTIL_AVAILABLE:
            swap = psutil.swap_memory()
            system_info['swap'] = {
                'total': swap.total // (1024**2),
                'used': swap.used // (1024**2),
                'free': swap.free // (1024**2),
                'percent': swap.percent
            }
    except Exception as e:
        logger.error(f"Error getting memory info: {e}")
    
    try:
        system_info['disk'] = get_host_disk_usage()
        
        disk_usage = {}
        for path in ['/', '/var', '/home', '/tmp']:
            if os.path.exists(path):
                try:
                    usage = shutil.disk_usage(path)
                    disk_usage[path] = {
                        'total': usage.total // (1024**3),
                        'used': usage.used // (1024**3),
                        'free': usage.free // (1024**3),
                        'percent': (usage.used / usage.total * 100) if usage.total > 0 else 0
                    }
                except:
                    pass
        system_info['disk_detailed'] = disk_usage
        
        if PSUTIL_AVAILABLE:
            partitions = []
            for partition in psutil.disk_partitions():
                try:
                    usage = psutil.disk_usage(partition.mountpoint)
                    partitions.append({
                        'device': partition.device,
                        'mountpoint': partition.mountpoint,
                        'fstype': partition.fstype,
                        'total': usage.total // (1024**3),
                        'used': usage.used // (1024**3),
                        'free': usage.free // (1024**3),
                        'percent': usage.percent
                    })
                except:
                    pass
            system_info['partitions'] = partitions
    except Exception as e:
        logger.error(f"Error getting disk info: {e}")
    
    try:
        if PSUTIL_AVAILABLE:
            net_if_addrs = psutil.net_if_addrs()
            network_interfaces = {}
            for interface, addrs in net_if_addrs.items():
                network_interfaces[interface] = []
                for addr in addrs:
                    network_interfaces[interface].append({
                        'family': str(addr.family),
                        'address': addr.address,
                        'netmask': addr.netmask,
                        'broadcast': addr.broadcast
                    })
            system_info['network_interfaces'] = network_interfaces
    except Exception as e:
        logger.error(f"Error getting network info: {e}")
    
    try:
        lxc_version = subprocess.run(['lxc', '--version'], capture_output=True, text=True, timeout=5)
        system_info['lxc_version'] = lxc_version.stdout.strip() if lxc_version.returncode == 0 else 'Not installed'
        
        try:
            pools_cmd = subprocess.run(['lxc', 'storage', 'list', '--format', 'json'], 
                                      capture_output=True, text=True, timeout=10)
            if pools_cmd.returncode == 0:
                system_info['lxc_pools'] = json.loads(pools_cmd.stdout)
        except:
            pass
        
        try:
            networks_cmd = subprocess.run(['lxc', 'network', 'list', '--format', 'json'], 
                                         capture_output=True, text=True, timeout=10)
            if networks_cmd.returncode == 0:
                system_info['lxc_networks'] = json.loads(networks_cmd.stdout)
        except:
            pass
    except:
        system_info['lxc_version'] = 'Not installed'
    
    try:
        if platform.system() == 'Linux' and os.path.exists('/etc/os-release'):
            with open('/etc/os-release', 'r') as f:
                os_release = {}
                for line in f:
                    if '=' in line:
                        key, value = line.strip().split('=', 1)
                        os_release[key] = value.strip('"')
                system_info['os_release'] = os_release
    except Exception as e:
        logger.error(f"Error getting OS info: {e}")
    
    try:
        db_size = os.path.getsize(DATABASE_PATH) // (1024 * 1024) if os.path.exists(DATABASE_PATH) else 0
        system_info['database'] = {
            'path': DATABASE_PATH,
            'size_mb': db_size,
            'exists': os.path.exists(DATABASE_PATH)
        }
        
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM users")
            user_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM vps")
            vps_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM nodes")
            node_count = cur.fetchone()[0]
            
            system_info['database']['stats'] = {
                'users': user_count,
                'vps': vps_count,
                'nodes': node_count
            }
    except Exception as e:
        logger.error(f"Error getting database info: {e}")
    
    try:
        system_info['app'] = {
            'version': PANEL_VERSION,
            'flask_version': flask.__version__,
            'debug_mode': app.debug,
            'testing_mode': app.testing
        }
    except Exception as e:
        logger.error(f"Error getting app info: {e}")
    
    try:
        if PSUTIL_AVAILABLE:
            process = psutil.Process()
            system_info['process'] = {
                'pid': process.pid,
                'memory_mb': process.memory_info().rss // (1024**2),
                'cpu_percent': process.cpu_percent(interval=0.1),
                'threads': process.num_threads(),
                'create_time': datetime.fromtimestamp(process.create_time()).strftime('%Y-%m-%d %H:%M:%S')
            }
    except Exception as e:
        logger.error(f"Error getting process info: {e}")
    
    try:
        env_vars = {
            'PANEL_NAME': PANEL_NAME,
            'PANEL_VERSION': PANEL_VERSION,
            'PANEL_DEVELOPER': PANEL_DEVELOPER,
            'DATABASE_PATH': DATABASE_PATH,
            'HOST': HOST,
            'PORT': PORT,
            'MAIN_ADMIN_USERNAME': os.getenv('MAIN_ADMIN_USERNAME', 'admin'),
            'MAIN_ADMIN_EMAIL': os.getenv('MAIN_ADMIN_EMAIL', 'admin@localhost'),
            'YOUR_SERVER_IP': YOUR_SERVER_IP,
            'DEFAULT_STORAGE_POOL': DEFAULT_STORAGE_POOL,
            'DEBUG_MODE': os.getenv('DEBUG_MODE', 'False'),
            'AUTO_BACKUP_INTERVAL': os.getenv('AUTO_BACKUP_INTERVAL', '3600'),
            'STATS_UPDATE_INTERVAL': os.getenv('STATS_UPDATE_INTERVAL', '5'),
            'PYTHON_VERSION': platform.python_version(),
            'PYTHONPATH': os.getenv('PYTHONPATH', 'Not set'),
            'PATH': os.getenv('PATH', 'Not set')[:200] + '...' if os.getenv('PATH') and len(os.getenv('PATH', '')) > 200 else os.getenv('PATH', 'Not set'),
            'HOME': os.getenv('HOME', 'Not set'),
            'USER': os.getenv('USER', 'Not set'),
            'SHELL': os.getenv('SHELL', 'Not set'),
            'LANG': os.getenv('LANG', 'Not set'),
            'TZ': os.getenv('TZ', 'Not set')
        }
        
        env_vars['CURRENT_DIRECTORY'] = os.getcwd()
        
        env_vars['PYTHON_EXECUTABLE'] = sys.executable
        
        system_info['environment'] = env_vars
    except Exception as e:
        logger.error(f"Error getting environment variables: {e}")
        system_info['environment'] = {}
    
    return system_info

@app.route('/admin/vacuum', methods=['POST'])
@login_required
@main_admin_required
def admin_vacuum():
    try:
        with get_db() as conn:
            conn.execute("VACUUM")
        log_activity(current_user.id, 'vacuum_db', 'system')
        create_notification(current_user.id, 'success', 'Database Vacuumed', 'Database has been optimized.')
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/admin/logs')
@login_required
@main_admin_required
def admin_logs():
    log_type = request.args.get('type', 'krysolvm')
    lines = int(request.args.get('lines', 100))
    download = request.args.get('download', 'false') == 'true'
    
    log_files = {
        'krysolvm': 'krysolvm.log',
        'lxc': '/var/log/lxc/lxc.log',
        'system': '/var/log/syslog',
        'auth': '/var/log/auth.log',
        'kern': '/var/log/kern.log',
        'panel': 'krysolvm.log'
    }
    
    log_file = log_files.get(log_type, 'krysolvm.log')
    
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest' or download:
        try:
            if os.path.exists(log_file):
                with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
                    all_lines = f.readlines()
                    last_lines = all_lines[-lines:] if lines > 0 else all_lines
                    log_content = ''.join(last_lines)
                    
                    if download:
                        from datetime import datetime
                        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                        filename = f"{log_type}_logs_{timestamp}.log"
                        response = make_response(log_content)
                        response.headers['Content-Type'] = 'text/plain'
                        response.headers['Content-Disposition'] = f'attachment; filename={filename}'
                        return response
                    
                    return jsonify({
                        'success': True, 
                        'logs': log_content,
                        'file': log_file,
                        'size': os.path.getsize(log_file),
                        'lines_total': len(all_lines),
                        'lines_shown': len(last_lines)
                    })
            else:
                return jsonify({'success': False, 'error': f'Log file not found: {log_file}'})
        except Exception as e:
            logger.error(f"Error reading log file: {e}")
            return jsonify({'success': False, 'error': str(e)})
    
    available_logs = []
    for name, path in log_files.items():
        if os.path.exists(path):
            try:
                stat = os.stat(path)
                available_logs.append({
                    'name': name,
                    'path': path,
                    'size': stat.st_size,
                    'size_mb': stat.st_size / (1024 * 1024),
                    'modified': datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S')
                })
            except:
                pass
    
    return render_template('admin/logs.html',
                         panel_name=get_setting('site_name', 'KrysolVM'),
                         available_logs=available_logs,
                         current_log=log_type)

# ============================================================================
# Admin - API Management
# ============================================================================

@app.route('/admin/api')
@login_required
@admin_required
def admin_api():
    with get_db() as conn:
        cur = conn.cursor()
        
        if current_user.is_main_admin:
            cur.execute('''SELECT ak.*, u.username, u.email 
                          FROM api_keys ak
                          JOIN users u ON ak.user_id = u.id
                          ORDER BY ak.created_at DESC''')
        else:
            cur.execute('''SELECT ak.*, u.username, u.email 
                          FROM api_keys ak
                          JOIN users u ON ak.user_id = u.id
                          WHERE ak.user_id = ?
                          ORDER BY ak.created_at DESC''', (current_user.id,))
        
        api_keys = [dict(row) for row in cur.fetchall()]
        
        users = []
        if current_user.is_admin:
            cur.execute('SELECT id, username, email FROM users ORDER BY username')
            users = [dict(row) for row in cur.fetchall()]
    
    return render_template('admin/api.html',
                         panel_name=get_setting('site_name', 'KrysolVM'),
                         api_keys=api_keys,
                         users=users)

@app.route('/admin/api/create', methods=['POST'])
@login_required
@admin_required
def admin_api_create():
    try:
        data = request.get_json() if request.is_json else request.form
        
        name = data.get('name')
        description = data.get('description', '')
        user_id = int(data.get('user_id', current_user.id))
        expires_days = data.get('expires_days')
        
        if not name:
            return jsonify({'success': False, 'error': 'API key name is required'}), 400
        
        if not current_user.is_admin and user_id != current_user.id:
            return jsonify({'success': False, 'error': 'Permission denied'}), 403
        
        api_key = f"krysolvm_{secrets.token_urlsafe(48)}"
        
        expires_at = None
        if expires_days:
            expires_at = (datetime.now() + timedelta(days=int(expires_days))).isoformat()
        
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('''INSERT INTO api_keys 
                          (user_id, key, name, description, is_active, created_at, expires_at)
                          VALUES (?, ?, ?, ?, ?, ?, ?)''',
                       (user_id, api_key, name, description, 1, datetime.now().isoformat(), expires_at))
            conn.commit()
            key_id = cur.lastrowid
        
        log_activity(current_user.id, 'create_api_key', 'api_key', str(key_id), {'name': name})
        
        return jsonify({
            'success': True,
            'message': 'API key created successfully',
            'api_key': api_key,
            'key_id': key_id
        })
    except Exception as e:
        logger.error(f"Error creating API key: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/admin/api/<int:key_id>/toggle', methods=['POST'])
@login_required
@admin_required
def admin_api_toggle(key_id):
    try:
        data = request.get_json() if request.is_json else request.form
        new_status = int(data.get('is_active', 0))
        
        with get_db() as conn:
            cur = conn.cursor()
            
            cur.execute('SELECT user_id, is_active FROM api_keys WHERE id = ?', (key_id,))
            result = cur.fetchone()
            
            if not result:
                return jsonify({'success': False, 'error': 'API key not found'}), 404
            
            if not current_user.is_admin and result['user_id'] != current_user.id:
                return jsonify({'success': False, 'error': 'Permission denied'}), 403
            
            cur.execute('UPDATE api_keys SET is_active = ? WHERE id = ?', (new_status, key_id))
            conn.commit()
        
        log_activity(current_user.id, 'toggle_api_key', 'api_key', str(key_id), 
                    {'status': 'active' if new_status else 'inactive'})
        
        return jsonify({
            'success': True,
            'message': f'API key {"activated" if new_status else "deactivated"}',
            'is_active': new_status
        })
    except Exception as e:
        logger.error(f"Error toggling API key: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/admin/api/<int:key_id>/delete', methods=['POST', 'DELETE'])
@login_required
@admin_required
def admin_api_delete(key_id):
    try:
        with get_db() as conn:
            cur = conn.cursor()
            
            cur.execute('SELECT user_id FROM api_keys WHERE id = ?', (key_id,))
            result = cur.fetchone()
            
            if not result:
                return jsonify({'success': False, 'error': 'API key not found'}), 404
            
            if not current_user.is_admin and result['user_id'] != current_user.id:
                return jsonify({'success': False, 'error': 'Permission denied'}), 403
            
            cur.execute('DELETE FROM api_keys WHERE id = ?', (key_id,))
            conn.commit()
        
        log_activity(current_user.id, 'delete_api_key', 'api_key', str(key_id))
        
        return jsonify({
            'success': True,
            'message': 'API key deleted successfully'
        })
    except Exception as e:
        logger.error(f"Error deleting API key: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/admin/emergency-stop-all', methods=['POST'])
@login_required
@main_admin_required
def admin_emergency_stop_all():
    stopped = 0
    vps_list = get_all_vps()
    
    for vps in vps_list:
        if vps['status'] == 'running' and not vps['suspended']:
            try:
                run_sync(execute_lxc(vps['container_name'], f"stop {vps['container_name']} --force", node_id=vps['node_id']))
                update_vps(vps['id'], status='stopped')
                stopped += 1
            except Exception as e:
                logger.error(f"Emergency stop failed for {vps['container_name']}: {e}")
    
    log_activity(current_user.id, 'emergency_stop_all', 'system', None, {'stopped': stopped})
    create_notification(current_user.id, 'warning', 'Emergency Stop', f'Emergency stop completed. {stopped} VPS stopped.')
    return jsonify({'success': True, 'stopped': stopped})

@app.route('/admin/emergency-reboot-all', methods=['POST'])
@login_required
@main_admin_required
def admin_emergency_reboot_all():
    rebooted = 0
    vps_list = get_all_vps()
    
    for vps in vps_list:
        if vps['status'] == 'running' and not vps['suspended']:
            try:
                run_sync(execute_lxc(vps['container_name'], f"restart {vps['container_name']}", node_id=vps['node_id']))
                rebooted += 1
            except Exception as e:
                logger.error(f"Emergency reboot failed for {vps['container_name']}: {e}")
    
    log_activity(current_user.id, 'emergency_reboot_all', 'system', None, {'rebooted': rebooted})
    create_notification(current_user.id, 'warning', 'Emergency Reboot', f'Emergency reboot completed. {rebooted} VPS rebooted.')
    return jsonify({'success': True, 'rebooted': rebooted})

@app.route('/admin/clear-suspensions', methods=['POST'])
@login_required
@main_admin_required
def admin_clear_suspensions():
    cleared = 0
    vps_list = get_all_vps()
    
    for vps in vps_list:
        if vps['suspended']:
            history = vps.get('suspension_history', [])
            history.append({
                'time': datetime.now().isoformat(),
                'reason': 'Suspension cleared by admin',
                'by': current_user.username
            })
            update_vps(vps['id'], suspended=0, status='stopped', suspended_reason=None, suspension_history=history)
            cleared += 1
            create_notification(vps['user_id'], 'success', 'VPS Unsuspended', f'Your VPS {vps["container_name"]} has been unsuspended.')
    
    log_activity(current_user.id, 'clear_suspensions', 'system', None, {'cleared': cleared})
    return jsonify({'success': True, 'cleared': cleared})

@app.route('/admin/reset-ports', methods=['POST'])
@login_required
@main_admin_required
def admin_reset_ports():
    recreated = 0
    vps_list = get_all_vps()
    
    for vps in vps_list:
        try:
            count = run_sync(recreate_port_forwards(vps['container_name']))
            recreated += count
        except Exception as e:
            logger.error(f"Port reset failed for {vps['container_name']}: {e}")
    
    log_activity(current_user.id, 'reset_ports', 'system', None, {'recreated': recreated})
    return jsonify({'success': True, 'recreated': recreated})

@app.route('/admin/node/<int:node_id>')
@login_required
@admin_required
def admin_node_get(node_id):
    node = get_node(node_id)
    if not node:
        return jsonify({'error': 'Node not found'}), 404
    
    return jsonify(node)

@app.route('/admin/nodes/<int:node_id>/check')
@login_required
@admin_required
def admin_node_check(node_id):
    try:
        node = get_node(node_id)
        if not node:
            return jsonify({'success': False, 'error': 'Node not found'}), 404
        
        logger.info(f"Checking node {node_id}: {node['name']}")
        
        try:
            status = run_sync(get_node_status(node_id))
            logger.info(f"Node {node_id} status: {status.get('status', 'unknown')}")
        except Exception as e:
            logger.error(f"Failed to get status for node {node_id}: {e}")
            status = {'status': 'Error', 'online': False}
        
        try:
            stats = run_sync(get_host_stats(node_id))
            logger.info(f"Node {node_id} stats retrieved successfully")
        except Exception as e:
            logger.error(f"Failed to get stats for node {node_id}: {e}")
            stats = {"cpu": 0.0, "ram": {'percent': 0.0}, "disk": {'percent': 'Unknown'}, "uptime": "Unknown"}
        
        try:
            vps_count = get_current_vps_count(node_id)
        except Exception as e:
            logger.error(f"Failed to get VPS count for node {node_id}: {e}")
            vps_count = 0
        
        try:
            pools = run_sync(execute_lxc("", "storage list", node_id=node_id, timeout=10))
        except Exception as e:
            logger.debug(f"Failed to get storage pools for node {node_id}: {e}")
            pools = None
        
        try:
            networks = run_sync(execute_lxc("", "network list", node_id=node_id, timeout=10))
        except Exception as e:
            logger.debug(f"Failed to get networks for node {node_id}: {e}")
            networks = None
        
        return jsonify({
            'success': True,
            'id': node['id'],
            'name': node['name'],
            'status': status,
            'online': status.get('online', False),
            'stats': stats,
            'vps_count': vps_count,
            'total_vps': node['total_vps'],
            'pools': pools,
            'networks': networks,
            'ip_addresses': node.get('ip_addresses', []),
            'ip_aliases': node.get('ip_aliases', [])
        })
    
    except Exception as e:
        logger.error(f"Error checking node {node_id}: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/admin/nodes/<int:node_id>/view')
@login_required
@admin_required
def admin_node_view(node_id):
    try:
        node = get_node(node_id)
        if not node:
            flash('Node not found', 'danger')
            return redirect(url_for('admin_nodes'))
        
        status = run_sync(get_node_status(node_id))
        
        stats = run_sync(get_host_stats(node_id))
        
        vps_count = get_current_vps_count(node_id)
        
        system_info = {}
        
        if node['is_local']:
            try:
                cpu_info_cmd = "lscpu | grep -E 'Model name|CPU\\(s\\)|Thread|Core|Socket|MHz'"
                cpu_info = subprocess.run(cpu_info_cmd, shell=True, capture_output=True, text=True, timeout=5)
                system_info['cpu_details'] = cpu_info.stdout if cpu_info.returncode == 0 else "N/A"
                
                mem_info_cmd = "free -h | grep -E 'Mem|Swap'"
                mem_info = subprocess.run(mem_info_cmd, shell=True, capture_output=True, text=True, timeout=5)
                system_info['memory_details'] = mem_info.stdout if mem_info.returncode == 0 else "N/A"
                
                os_info_cmd = "cat /etc/os-release | grep -E 'PRETTY_NAME|VERSION'"
                os_info = subprocess.run(os_info_cmd, shell=True, capture_output=True, text=True, timeout=5)
                system_info['os_details'] = os_info.stdout if os_info.returncode == 0 else "N/A"
                
                kernel_cmd = "uname -r"
                kernel_info = subprocess.run(kernel_cmd, shell=True, capture_output=True, text=True, timeout=5)
                system_info['kernel'] = kernel_info.stdout.strip() if kernel_info.returncode == 0 else "N/A"
                
                disk_cmd = "df -h | grep -E '^/dev/'"
                disk_info = subprocess.run(disk_cmd, shell=True, capture_output=True, text=True, timeout=5)
                system_info['disk_details'] = disk_info.stdout if disk_info.returncode == 0 else "N/A"
                
            except Exception as e:
                logger.error(f"Error getting local system info: {e}")
        else:
            try:
                import requests
                headers = {"X-API-Key": node["api_key"]}
                verify_ssl = bool(node.get('verify_ssl', 1))
                
                response = requests.get(f"{node['url']}/api/info", headers=headers, timeout=10, verify=verify_ssl)
                if response.status_code == 200:
                    info = response.json()
                    system_info['version'] = info.get('version', 'N/A')
                    system_info['python_version'] = info.get('python_version', 'N/A')
                
                try:
                    cpu_details = run_sync(execute_lxc("", "sh -c \"lscpu | grep -E 'Model name|CPU\\(s\\)|Thread|Core|Socket|MHz'\"", node_id=node_id, timeout=10, operation_type="stats"))
                    system_info['cpu_details'] = cpu_details
                except:
                    system_info['cpu_details'] = "N/A"
                
                try:
                    os_details = run_sync(execute_lxc("", "sh -c \"cat /etc/os-release | grep -E 'PRETTY_NAME|VERSION'\"", node_id=node_id, timeout=10, operation_type="stats"))
                    system_info['os_details'] = os_details
                except:
                    system_info['os_details'] = "N/A"
                
                try:
                    kernel = run_sync(execute_lxc("", "uname -r", node_id=node_id, timeout=10, operation_type="stats"))
                    system_info['kernel'] = kernel.strip()
                except:
                    system_info['kernel'] = "N/A"
                    
            except Exception as e:
                logger.error(f"Error getting remote system info: {e}")
        
        try:
            pools = run_sync(execute_lxc("", "storage list", node_id=node_id, timeout=10, operation_type="general"))
        except:
            pools = None
        
        try:
            networks = run_sync(execute_lxc("", "network list", node_id=node_id, timeout=10, operation_type="general"))
        except:
            networks = None
        
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('SELECT * FROM vps WHERE node_id = ? ORDER BY created_at DESC', (node_id,))
            vps_list = [dict(row) for row in cur.fetchall()]
        
        for vps in vps_list:
            vps['live_status'] = vps.get('status', 'unknown')
            vps['live_cpu'] = 0.0
            vps['live_ram'] = {'used': 0, 'total': 0, 'pct': 0.0}
            vps['live_disk'] = {'use_percent': '0%'}
            
            if is_vps_suspended(vps):
                vps['live_status'] = 'suspended'
            else:
                try:
                    stats = run_sync(
                        get_container_stats(
                            vps['container_name'],
                            vps['node_id']
                        )
                    )
                    
                    raw_status = stats.get('status', 'unknown')
                    if raw_status and ('_cached' in raw_status or raw_status in ('timeout', 'error', 'unknown', 'server_error', 'circuit_open', 'connection_error')):
                        vps['live_status'] = vps.get('status', 'stopped').lower()
                    else:
                        vps['live_status'] = raw_status
                    
                    vps['live_cpu'] = float(stats.get('cpu', 0.0))
                    
                    ram_data = stats.get('ram', {'used': 0, 'total': 0, 'pct': 0.0})
                    if isinstance(ram_data, dict):
                        vps['live_ram'] = ram_data
                    else:
                        vps['live_ram'] = {'used': 0, 'total': 0, 'pct': 0.0}
                    
                    disk_data = stats.get('disk', {'use_percent': '0%'})
                    if isinstance(disk_data, dict):
                        vps['live_disk'] = disk_data
                    else:
                        vps['live_disk'] = {'use_percent': '0%'}
                    
                    logger.debug(f"VPS {vps['id']} stats: status={vps['live_status']}, cpu={vps['live_cpu']}, ram={vps['live_ram']}")
                
                except Exception as e:
                    logger.warning(f"Stats error for {vps.get('container_name')}: {e}")
                    vps['live_status'] = vps.get('status', 'unknown').lower()
        
        return render_template('admin/node_view.html',
                             panel_name=get_setting('site_name', 'KrysolVM'),
                             node=node,
                             status=status,
                             stats=stats,
                             vps_count=vps_count,
                             vps_list=vps_list,
                             system_info=system_info,
                             pools=pools,
                             networks=networks)
    
    except Exception as e:
        logger.error(f"Error viewing node {node_id}: {e}", exc_info=True)
        flash(f'Error loading node details: {str(e)}', 'danger')
        return redirect(url_for('admin_nodes'))

@app.route('/admin/user/create', methods=['POST'])
@login_required
@admin_required
def admin_user_create():
    username = request.form.get('username')
    email = request.form.get('email')
    password = request.form.get('password')
    is_admin = request.form.get('is_admin') == 'true'
    port_quota = int(request.form.get('port_quota', 5))
    
    if not username or not email or not password:
        return jsonify({'success': False, 'error': 'Missing fields'}), 400
    
    if User.get_by_username(username):
        return jsonify({'success': False, 'error': 'Username exists'}), 400
    
    if User.get_by_email(email):
        return jsonify({'success': False, 'error': 'Email exists'}), 400
    
    password_hash = generate_password_hash(password)
    api_key = generate_api_key()
    now = datetime.now().isoformat()
    
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('''INSERT INTO users 
            (username, email, password_hash, is_admin, created_at, last_login, api_key, preferences)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
            (username, email, password_hash, 1 if is_admin else 0, now, now, api_key, '{}'))
        user_id = cur.lastrowid
        
        if port_quota > 0:
            cur.execute('INSERT INTO port_allocations (user_id, allocated_ports, used_ports, updated_at) VALUES (?, ?, ?, ?)',
                       (user_id, port_quota, 0, now))
        
        conn.commit()
    
    log_activity(current_user.id, 'create_user', 'user', str(user_id), {'username': username})
    create_notification(user_id, 'success', 'Welcome!', f'Your account has been created by an administrator.')
    return jsonify({'success': True, 'user_id': user_id})

@app.route('/admin/user/<int:user_id>/regenerate-api', methods=['POST'])
@login_required
@admin_required
def admin_user_regenerate_api(user_id):
    new_key = generate_api_key()
    
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('UPDATE users SET api_key = ? WHERE id = ?', (new_key, user_id))
        conn.commit()
    
    log_activity(current_user.id, 'regenerate_user_api', 'user', str(user_id))
    create_notification(user_id, 'warning', 'API Key Regenerated', 'Your API key has been regenerated by an administrator.')
    return jsonify({'success': True, 'api_key': new_key})

@app.route('/admin/users/<int:user_id>/reset-password', methods=['POST'])
@login_required
@admin_required
def admin_user_reset_password(user_id):
    password = request.form.get('password')
    
    if not password or len(password) < 8:
        return jsonify({'success': False, 'error': 'Invalid password'}), 400
    
    password_hash = generate_password_hash(password)
    
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('UPDATE users SET password_hash = ? WHERE id = ?', (password_hash, user_id))
        conn.commit()
    
    log_activity(current_user.id, 'reset_user_password', 'user', str(user_id))
    create_notification(user_id, 'warning', 'Password Reset', 'Your password has been reset by an administrator.')
    return jsonify({'success': True})

@app.route('/share/vps/<int:vps_id>', methods=['POST'])
@login_required
def share_vps(vps_id):
    try:
        logger.info(f"=== Share VPS Request: VPS ID {vps_id}, User {current_user.id} ===")
        
        vps = get_vps_by_id(vps_id)
        if not vps:
            logger.error(f"VPS {vps_id} not found")
            return jsonify({'success': False, 'error': 'VPS not found'}), 404
            
        if vps['user_id'] != current_user.id:
            logger.error(f"Access denied: VPS owner is {vps['user_id']}, requester is {current_user.id}")
            return jsonify({'success': False, 'error': 'Access denied - you are not the owner'}), 403
        
        username = request.form.get('username') or (request.json.get('username') if request.is_json else None)
        if not username:
            logger.error("No username provided")
            return jsonify({'success': False, 'error': 'Username required'}), 400
        
        logger.info(f"Attempting to share with username: {username}")
        
        user = User.get_by_username(username)
        if not user:
            logger.error(f"User '{username}' not found")
            return jsonify({'success': False, 'error': f'User "{username}" not found'}), 404
        
        logger.info(f"Found user: {user.username} (ID: {user.id})")
        
        if user.id == current_user.id:
            logger.error("Attempted to share with self")
            return jsonify({'success': False, 'error': 'Cannot share with yourself'}), 400
        
        shared_with = vps.get('shared_with', []) or []
        if not isinstance(shared_with, list):
            shared_with = []
        
        logger.info(f"Current shared_with list: {shared_with}")
        
        if str(user.id) in [str(uid) for uid in shared_with]:
            logger.warning(f"VPS already shared with user {user.id}")
            return jsonify({'success': False, 'error': f'VPS already shared with {username}'}), 400
        
        shared_with.append(str(user.id))
        logger.info(f"New shared_with list: {shared_with}")
        
        logger.info(f"Calling update_vps with shared_with={shared_with}")
        update_vps(vps_id, shared_with=shared_with)
        
        updated_vps = get_vps_by_id(vps_id)
        logger.info(f"After update, shared_with in DB: {updated_vps.get('shared_with', [])}")
        
        log_activity(current_user.id, 'share_vps', 'vps', str(vps_id), {'shared_with': user.id, 'username': username})
        create_notification(user.id, 'info', 'VPS Shared', f'{current_user.username} shared VPS {vps["container_name"]} with you.')
        
        logger.info(f"Successfully shared VPS {vps_id} with user {user.id} ({username})")
        return jsonify({'success': True, 'message': f'VPS shared with {username}'})
        
    except Exception as e:
        logger.error(f"Error sharing VPS {vps_id}: {e}", exc_info=True)
        return jsonify({'success': False, 'error': f'Failed to share VPS: {str(e)}'}), 500

@app.route('/unshare/vps/<int:vps_id>', methods=['POST'])
@login_required
def unshare_vps(vps_id):
    try:
        vps = get_vps_by_id(vps_id)
        if not vps:
            return jsonify({'success': False, 'error': 'VPS not found'}), 404
            
        if vps['user_id'] != current_user.id:
            return jsonify({'success': False, 'error': 'Access denied - you are not the owner'}), 403
        
        user_id = request.form.get('user_id') or request.json.get('user_id') if request.is_json else None
        if not user_id:
            return jsonify({'success': False, 'error': 'User ID required'}), 400
        
        shared_with = vps.get('shared_with', []) or []
        if not isinstance(shared_with, list):
            shared_with = []
        
        user_id_str = str(user_id)
        
        if user_id_str not in [str(uid) for uid in shared_with]:
            return jsonify({'success': False, 'error': 'VPS not shared with this user'}), 400
        
        shared_with = [uid for uid in shared_with if str(uid) != user_id_str]
        update_vps(vps_id, shared_with=shared_with)
        
        log_activity(current_user.id, 'unshare_vps', 'vps', str(vps_id), {'unshared': user_id})
        create_notification(int(user_id), 'info', 'VPS Unshared', f'{current_user.username} removed your access to VPS {vps["container_name"]}.')
        
        logger.info(f"VPS {vps_id} unshared from user {user_id} by {current_user.id}")
        return jsonify({'success': True, 'message': 'Access removed successfully'})
        
    except Exception as e:
        logger.error(f"Error unsharing VPS {vps_id}: {e}", exc_info=True)
        return jsonify({'success': False, 'error': f'Failed to remove access: {str(e)}'}), 500

# ============================================================================
# API Routes
# ============================================================================
@app.route('/api/ping', methods=['GET'])
def api_ping():
    api_key = request.args.get('api_key')
    if not api_key:
        return jsonify({'error': 'API key required'}), 401
    
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('SELECT id FROM nodes WHERE api_key = ?', (api_key,))
        node = cur.fetchone()
    
    if not node:
        return jsonify({'error': 'Invalid API key'}), 401
    
    return jsonify({'status': 'ok', 'time': datetime.now().isoformat()})

@app.route('/api/execute', methods=['POST'])
def api_execute():
    api_key = request.args.get('api_key')
    if not api_key:
        return jsonify({'error': 'API key required'}), 401
    
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('SELECT id FROM nodes WHERE api_key = ?', (api_key,))
        node = cur.fetchone()
    
    if not node:
        return jsonify({'error': 'Invalid API key'}), 401
    
    data = request.get_json()
    command = data.get('command')
    
    if not command:
        return jsonify({'error': 'Command required'}), 400
    
    try:
        cmd = shlex.split(command)
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        
        return jsonify({
            'stdout': proc.stdout,
            'stderr': proc.stderr,
            'returncode': proc.returncode
        })
    except subprocess.TimeoutExpired:
        return jsonify({'error': 'Command timed out'}), 504
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/get_host_stats', methods=['GET'])
def api_get_host_stats():
    api_key = request.args.get('api_key')
    if not api_key:
        return jsonify({'error': 'API key required'}), 401
    
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('SELECT id FROM nodes WHERE api_key = ?', (api_key,))
        node = cur.fetchone()
    
    if not node:
        return jsonify({'error': 'Invalid API key'}), 401
    
    return jsonify({
        'cpu': get_host_cpu_usage(),
        'ram': get_host_ram_usage(),
        'disk': get_host_disk_usage(),
        'uptime': get_host_uptime(),
        'cpu_cores': os.cpu_count()
    })

@app.route('/api/get_container_stats', methods=['POST'])
def api_get_container_stats():
    api_key = request.args.get('api_key')
    if not api_key:
        return jsonify({'error': 'API key required'}), 401
    
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('SELECT id FROM nodes WHERE api_key = ?', (api_key,))
        node = cur.fetchone()
    
    if not node:
        return jsonify({'error': 'Invalid API key'}), 401
    
    data = request.get_json()
    container = data.get('container')
    
    if not container:
        return jsonify({'error': 'Container name required'}), 400
    
    try:
        status = run_sync(get_container_status(container, node['id']))
        cpu = run_sync(get_container_cpu_pct_local(container, node['id']))
        ram = run_sync(get_container_ram_local(container, node['id']))
        disk = run_sync(get_container_disk_local(container, node['id']))
        uptime = run_sync(get_container_uptime_local(container, node['id']))
        processes = run_sync(get_container_processes_local(container, node['id']))
        network = run_sync(get_container_network_local(container, node['id']))
        
        return jsonify({
            'status': status,
            'cpu': cpu,
            'ram': ram,
            'disk': disk,
            'uptime': uptime,
            'processes': processes,
            'network': network,
            'load_average': {'1min': 0.0, '5min': 0.0, '15min': 0.0}
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ============================================================================
# Enhanced Performance Monitoring Functions
# ============================================================================

async def get_enhanced_network_stats_safe(container_name: str, node_id: Optional[int] = None) -> Dict:
    try:
        if node_id and is_node_circuit_open(node_id):
            logger.info(f"Circuit breaker open for node {node_id}, skipping enhanced network stats")
            return {'rx': '0 B', 'tx': '0 B', 'total_rx': 0, 'total_tx': 0}
            
        return await get_enhanced_network_stats(container_name, node_id)
    except Exception as e:
        logger.warning(f"Enhanced network stats failed for {container_name}: {e}")
        if node_id:
            record_node_failure(node_id)
        return {'rx': '0 B', 'tx': '0 B', 'total_rx': 0, 'total_tx': 0}

async def get_disk_io_stats_safe(container_name: str, node_id: Optional[int] = None) -> Dict:
    try:
        if node_id and is_node_circuit_open(node_id):
            logger.info(f"Circuit breaker open for node {node_id}, skipping disk I/O stats")
            return {'read': '0 B', 'write': '0 B', 'read_bytes': 0, 'write_bytes': 0}
            
        return await get_disk_io_stats(container_name, node_id)
    except Exception as e:
        logger.warning(f"Disk I/O stats failed for {container_name}: {e}")
        if node_id:
            record_node_failure(node_id)
        return {'read': '0 B', 'write': '0 B', 'read_bytes': 0, 'write_bytes': 0}

async def get_system_info_safe(container_name: str, node_id: Optional[int] = None) -> Dict:
    try:
        if node_id and is_node_circuit_open(node_id):
            logger.info(f"Circuit breaker open for node {node_id}, skipping system info")
            return {'processes': 0, 'load_average': {'1min': 0.0, '5min': 0.0, '15min': 0.0}}
            
        return await get_system_info(container_name, node_id)
    except Exception as e:
        logger.warning(f"System info failed for {container_name}: {e}")
        if node_id:
            record_node_failure(node_id)
        return {'processes': 0, 'load_average': {'1min': 0.0, '5min': 0.0, '15min': 0.0}}

async def get_enhanced_network_stats(container_name: str, node_id: Optional[int] = None) -> Dict:
    try:
        result = await execute_lxc(
            container_name,
            f"exec {container_name} -- cat /proc/net/dev",
            node_id=node_id,
            timeout=3,
            operation_type="stats"
        )
        
        network_data = {'interfaces': {}, 'total_rx': 0, 'total_tx': 0, 'rx_rate': 0, 'tx_rate': 0}
        
        for line in result.split('\n')[2:]:
            if ':' in line:
                parts = line.split(':')
                if len(parts) >= 2:
                    interface = parts[0].strip()
                    stats = parts[1].split()
                    
                    if len(stats) >= 9 and interface != 'lo':
                        try:
                            rx_bytes = int(stats[0])
                            tx_bytes = int(stats[8])
                            
                            network_data['interfaces'][interface] = {
                                'rx_bytes': rx_bytes,
                                'tx_bytes': tx_bytes,
                                'rx_packets': int(stats[1]) if len(stats) > 1 else 0,
                                'tx_packets': int(stats[9]) if len(stats) > 9 else 0,
                                'rx_errors': int(stats[2]) if len(stats) > 2 else 0,
                                'tx_errors': int(stats[10]) if len(stats) > 10 else 0
                            }
                            
                            network_data['total_rx'] += rx_bytes
                            network_data['total_tx'] += tx_bytes
                        except (ValueError, IndexError) as e:
                            logger.warning(f"Error parsing network stats for interface {interface}: {e}")
                            continue
        
        network_data['rx'] = format_bytes(network_data['total_rx'])
        network_data['tx'] = format_bytes(network_data['total_tx'])
        
        return network_data
        
    except Exception as e:
        logger.error(f"Error getting network stats for {container_name}: {e}")
        return {'rx': '0 B', 'tx': '0 B', 'total_rx': 0, 'total_tx': 0}

async def get_disk_io_stats(container_name: str, node_id: Optional[int] = None) -> Dict:
    try:
        result = await execute_lxc(
            container_name,
            f"exec {container_name} -- cat /proc/diskstats",
            node_id=node_id,
            timeout=3,
            operation_type="stats"
        )
        
        total_read = 0
        total_write = 0
        
        for line in result.split('\n'):
            if not line.strip():
                continue
                
            parts = line.split()
            if len(parts) >= 14:
                try:
                    device = parts[2]
                    if not device.startswith('loop') and not device[-1].isdigit():
                        read_sectors = int(parts[5])
                        write_sectors = int(parts[9])
                        
                        total_read += read_sectors * 512
                        total_write += write_sectors * 512
                except (ValueError, IndexError) as e:
                    logger.warning(f"Error parsing disk stats line: {line.strip()}: {e}")
                    continue
        
        return {
            'read_bytes': total_read,
            'write_bytes': total_write,
            'read': format_bytes(total_read),
            'write': format_bytes(total_write)
        }
        
    except Exception as e:
        logger.error(f"Error getting disk I/O stats for {container_name}: {e}")
        return {'read': '0 B', 'write': '0 B', 'read_bytes': 0, 'write_bytes': 0}

async def get_system_info(container_name: str, node_id: Optional[int] = None) -> Dict:
    try:
        proc_result = await execute_lxc(
            container_name,
            f"exec {container_name} -- sh -c 'ps aux | wc -l'",
            node_id=node_id,
            timeout=3,
            operation_type="stats"
        )
        process_count = max(0, int(proc_result.strip()) - 1)
        
        load_result = await execute_lxc(
            container_name,
            f"exec {container_name} -- cat /proc/loadavg",
            node_id=node_id,
            timeout=3,
            operation_type="stats"
        )
        load_parts = load_result.strip().split()
        load_avg = {
            '1min': float(load_parts[0]) if len(load_parts) > 0 and load_parts[0].replace('.', '').isdigit() else 0.0,
            '5min': float(load_parts[1]) if len(load_parts) > 1 and load_parts[1].replace('.', '').isdigit() else 0.0,
            '15min': float(load_parts[2]) if len(load_parts) > 2 and load_parts[2].replace('.', '').isdigit() else 0.0
        }
        
        return {
            'processes': process_count,
            'load_average': load_avg
        }
        
    except Exception as e:
        logger.error(f"Error getting system info for {container_name}: {e}")
        return {'processes': 0, 'load_average': {'1min': 0.0, '5min': 0.0, '15min': 0.0}}

def format_bytes(bytes_value: int) -> str:
    if not bytes_value:
        return '0 B'
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_value < 1024.0:
            return f"{bytes_value:.1f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.1f} PB"

def store_vps_metrics_safe(vps_id: int, stats: Dict):
    try:
        store_vps_metrics(vps_id, stats)
    except Exception as e:
        logger.error(f"Error in background metrics storage for VPS {vps_id}: {e}")

def store_vps_metrics(vps_id: int, stats: Dict):
    try:
        with get_db() as conn:
            cur = conn.cursor()
            
            cur.execute('''CREATE TABLE IF NOT EXISTS vps_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                vps_id INTEGER NOT NULL,
                timestamp DATETIME NOT NULL,
                cpu_percent REAL,
                ram_used INTEGER,
                ram_total INTEGER,
                ram_percent REAL,
                disk_used INTEGER,
                disk_total INTEGER,
                disk_percent REAL,
                network_rx INTEGER,
                network_tx INTEGER,
                disk_read INTEGER,
                disk_write INTEGER,
                processes INTEGER,
                load_1min REAL,
                load_5min REAL,
                load_15min REAL,
                FOREIGN KEY (vps_id) REFERENCES vps (id)
            )''')
            
            cur.execute('CREATE INDEX IF NOT EXISTS idx_vps_metrics_vps_timestamp ON vps_metrics(vps_id, timestamp)')
            
            disk_percent = 0.0
            try:
                disk_pct_str = stats.get('disk', {}).get('use_percent', '0%')
                if isinstance(disk_pct_str, str):
                    disk_percent = float(disk_pct_str.replace('%', ''))
                else:
                    disk_percent = float(disk_pct_str)
            except (ValueError, TypeError):
                disk_percent = 0.0
            
            cur.execute('''INSERT INTO vps_metrics 
                (vps_id, timestamp, cpu_percent, ram_used, ram_total, ram_percent,
                 disk_used, disk_total, disk_percent, network_rx, network_tx,
                 disk_read, disk_write, processes, load_1min, load_5min, load_15min)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (
                    vps_id,
                    datetime.now().isoformat(),
                    float(stats.get('cpu', 0)),
                    int(stats.get('ram', {}).get('used', 0)),
                    int(stats.get('ram', {}).get('total', 0)),
                    float(stats.get('ram', {}).get('pct', 0)),
                    int(stats.get('disk', {}).get('used_bytes', 0)),
                    int(stats.get('disk', {}).get('total_bytes', 0)),
                    disk_percent,
                    int(stats.get('network', {}).get('total_rx', 0)),
                    int(stats.get('network', {}).get('total_tx', 0)),
                    int(stats.get('disk_io', {}).get('read_bytes', 0)),
                    int(stats.get('disk_io', {}).get('write_bytes', 0)),
                    int(stats.get('processes', 0)),
                    float(stats.get('load_average', {}).get('1min', 0.0)),
                    float(stats.get('load_average', {}).get('5min', 0.0)),
                    float(stats.get('load_average', {}).get('15min', 0.0))
                ))
            
            conn.commit()
            
            import random
            if random.randint(1, 10) == 1:
                try:
                    cur.execute('''DELETE FROM vps_metrics 
                        WHERE vps_id = ? AND timestamp < datetime('now', '-24 hours')''',
                        (vps_id,))
                    conn.commit()
                    logger.debug(f"Cleaned up old metrics for VPS {vps_id}")
                except Exception as cleanup_e:
                    logger.warning(f"Error cleaning up old metrics for VPS {vps_id}: {cleanup_e}")
            
    except Exception as e:
        logger.error(f"Error storing metrics for VPS {vps_id}: {e}")

def get_vps_metrics_history(vps_id: int, time_range: str, limit: int) -> List[Dict]:
    try:
        with get_db() as conn:
            cur = conn.cursor()
            
            time_filters = {
                '1m': "datetime('now', '-1 minutes')",
                '5m': "datetime('now', '-5 minutes')",
                '10m': "datetime('now', '-10 minutes')",
                '30m': "datetime('now', '-30 minutes')",
                '1h': "datetime('now', '-1 hours')",
                '6h': "datetime('now', '-6 hours')",
                '24h': "datetime('now', '-24 hours')"
            }
            
            time_filter = time_filters.get(time_range, time_filters['1h'])
            
            cur.execute(f'''SELECT timestamp, cpu_percent, ram_percent, disk_percent,
                network_rx, network_tx, disk_read, disk_write, processes,
                load_1min, load_5min, load_15min
                FROM vps_metrics 
                WHERE vps_id = ? AND timestamp >= {time_filter}
                ORDER BY timestamp DESC
                LIMIT ?''', (vps_id, limit))
            
            rows = cur.fetchall()
            
            metrics = []
            for row in rows:
                metrics.append({
                    'timestamp': row[0],
                    'cpu': row[1],
                    'ram': row[2],
                    'disk': row[3],
                    'network_rx': row[4],
                    'network_tx': row[5],
                    'disk_read': row[6],
                    'disk_write': row[7],
                    'processes': row[8],
                    'load_1min': row[9],
                    'load_5min': row[10],
                    'load_15min': row[11]
                })
            
            return list(reversed(metrics))
            
    except Exception as e:
        logger.error(f"Error getting metrics history for VPS {vps_id}: {e}")
        return []

def get_limit_for_range(time_range: str) -> int:
    limits = {
        '1m': 60,
        '5m': 150,
        '10m': 200,
        '30m': 300,
        '1h': 360,
        '6h': 720,
        '24h': 1440
    }
    return limits.get(time_range, 360)

# ============================================================================
# Resource Monitor Thread
# ============================================================================
resource_monitor_active = True

def resource_monitor():
    global resource_monitor_active
    backup_interval = AUTO_BACKUP_INTERVAL
    last_backup = time.time()
    last_stats_update = time.time()
    stats_cache = {}
    
    while resource_monitor_active:
        try:
            current_time = time.time()
            
            if current_time - last_stats_update >= 60:
                nodes = get_nodes()
                for node in nodes:
                    try:
                        stats = run_sync(get_host_stats(node['id']))
                        stats_cache[node['id']] = stats
                        
                        cpu = stats.get('cpu', 0)
                        ram = stats.get('ram', {}).get('percent', 0)
                        logger.info(f"Node {node['name']}: CPU {cpu:.1f}%, RAM {ram:.1f}%")
                        
                        cpu_threshold = int(get_setting('cpu_threshold', 90))
                        ram_threshold = int(get_setting('ram_threshold', 90))
                        
                        if cpu > cpu_threshold or ram > ram_threshold:
                            logger.warning(f"Node {node['name']} exceeded thresholds (CPU: {cpu:.1f}%, RAM: {ram:.1f}%).")
                            
                            if socketio:
                                socketio.emit('node_alert', {
                                    'node_id': node['id'],
                                    'node_name': node['name'],
                                    'cpu': cpu,
                                    'ram': ram
                                }, room='admins')
                                
                    except Exception as e:
                        logger.error(f"Error monitoring node {node.get('name', 'Unknown')}: {e}")
                
                log_node_health_summary()
                
                cleanup_expired_cache()
                
                cleanup_old_node_failures()
                
                cleanup_expired_reset_tokens()
                
                last_stats_update = current_time
            
            if int(current_time) % 300 == 0:
                try:
                    vps_list = get_all_vps()
                    for vps in vps_list:
                        if is_vps_whitelisted(vps) or is_vps_suspended(vps):
                            continue
                            
                        if vps['status'] == 'running':
                            try:
                                stats = run_sync(get_container_stats(vps['container_name'], vps['node_id']))
                                cpu = stats.get('cpu', 0)
                                ram = stats.get('ram', {}).get('pct', 0)
                                
                                cpu_threshold = int(get_setting('cpu_threshold', 90))
                                ram_threshold = int(get_setting('ram_threshold', 90))
                                
                                if cpu > cpu_threshold or ram > ram_threshold:
                                    reason = f"Auto-suspended: High resource usage (CPU {cpu:.1f}%, RAM {ram:.1f}%)"
                                    run_sync(execute_lxc(vps['container_name'], f"stop {vps['container_name']} --force", node_id=vps['node_id']))
                                    
                                    history = vps['suspension_history']
                                    history.append({
                                        'time': datetime.now().isoformat(),
                                        'reason': reason,
                                        'by': 'Auto Monitor'
                                    })
                                    
                                    update_vps(vps['id'], suspended=1, status='stopped', 
                                              suspension_history=history, suspended_reason=reason)
                                    logger.info(f"Auto-suspended {vps['container_name']}: {reason}")
                                    log_activity(None, 'auto_suspend', 'vps', str(vps['id']), {'reason': reason})
                                    create_notification(vps['user_id'], 'warning', 'VPS Auto-Suspended', 
                                                      f'Your VPS {vps["container_name"]} has been suspended due to high resource usage.')
                                    
                                    if socketio:
                                        socketio.emit('vps_suspended', {
                                            'vps_id': vps['id'],
                                            'reason': reason
                                        }, room=f'vps_{vps["id"]}')
                                        
                            except Exception as e:
                                logger.error(f"Auto resource check error for {vps['container_name']}: {e}")
                except Exception as e:
                    logger.error(f"Auto resource check error: {e}")
            
            if int(current_time) % 600 == 0:
                try:
                    with get_db() as conn:
                        cur = conn.cursor()
                        now = datetime.now().isoformat()
                        
                        cur.execute('''SELECT id, user_id, container_name, node_id, expires_at, expiration_days, hostname
                                      FROM vps 
                                      WHERE auto_suspend_enabled = 1 
                                      AND suspended = 0 
                                      AND expires_at IS NOT NULL 
                                      AND expires_at <= ?''', (now,))
                        
                        expired_vps_list = [dict(row) for row in cur.fetchall()]
                        
                        for vps in expired_vps_list:
                            try:
                                run_sync(execute_lxc(vps['container_name'], f"stop {vps['container_name']} --force", node_id=vps['node_id']))
                                
                                reason = f"Auto-suspended: VPS expired after {vps['expiration_days']} days"
                                
                                cur.execute('SELECT suspension_history FROM vps WHERE id = ?', (vps['id'],))
                                history_row = cur.fetchone()
                                history = json.loads(history_row[0]) if history_row and history_row[0] else []
                                history.append({
                                    'time': now,
                                    'reason': reason,
                                    'by': 'Auto Expiration System'
                                })
                                
                                cur.execute('''UPDATE vps 
                                              SET suspended = 1, status = 'stopped', 
                                              suspended_reason = ?, suspension_history = ?, updated_at = ?
                                              WHERE id = ?''',
                                           (reason, json.dumps(history), now, vps['id']))
                                conn.commit()
                                
                                logger.info(f"Auto-suspended expired VPS: {vps['container_name']}")
                                log_activity(None, 'auto_suspend_expired', 'vps', str(vps['id']), 
                                           {'reason': reason, 'expiration_days': vps['expiration_days']})
                                
                                create_notification(vps['user_id'], 'warning', 'VPS Expired and Suspended', 
                                                  f'Your VPS {vps["hostname"]} has been suspended because it expired after {vps["expiration_days"]} days. Contact admin for renewal.')
                                
                                if socketio:
                                    socketio.emit('vps_expired', {
                                        'vps_id': vps['id'],
                                        'container_name': vps['container_name'],
                                        'reason': reason
                                    }, room=f'user_{vps["user_id"]}')
                                    
                            except Exception as e:
                                logger.error(f"Failed to auto-suspend expired VPS {vps['container_name']}: {e}")
                        
                        warning_date = (datetime.now() + timedelta(days=3)).isoformat()
                        cur.execute('''SELECT id, user_id, container_name, expires_at, expiration_days, hostname
                                      FROM vps 
                                      WHERE auto_suspend_enabled = 1 
                                      AND suspended = 0 
                                      AND expires_at IS NOT NULL 
                                      AND expires_at <= ? 
                                      AND expires_at > ?''', (warning_date, now))
                        
                        expiring_soon = [dict(row) for row in cur.fetchall()]
                        
                        for vps in expiring_soon:
                            try:
                                expires_dt = datetime.fromisoformat(vps['expires_at'])
                                days_left = (expires_dt - datetime.now()).days
                                
                                create_notification(vps['user_id'], 'info', 'VPS Expiring Soon', 
                                                  f'Your VPS {vps["hostname"]} will expire in {days_left} days. Contact admin for renewal.',
                                                  expires_in=86400)
                                
                            except Exception as e:
                                logger.error(f"Failed to send expiration warning for VPS {vps['container_name']}: {e}")
                                
                except Exception as e:
                    logger.error(f"Auto expiration check error: {e}")
            
            if get_setting('backup_enabled', '1') == '1' and current_time - last_backup > backup_interval:
                backup_name = f"krysolvm_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
                backup_path = os.path.join('backups', backup_name)
                try:
                    os.makedirs('backups', exist_ok=True)
                    shutil.copy(DATABASE_PATH, backup_path)
                    if os.path.exists(f"{DATABASE_PATH}-wal"):
                        shutil.copy(f"{DATABASE_PATH}-wal", f"{backup_path}-wal")
                    if os.path.exists(f"{DATABASE_PATH}-shm"):
                        shutil.copy(f"{DATABASE_PATH}-shm", f"{backup_path}-shm")
                    logger.info(f"Database backup created: {backup_name}")
                    
                    backup_retention = int(get_setting('backup_retention', '7'))
                    backups = sorted([f for f in os.listdir('backups') if f.endswith('.db')])
                    while len(backups) > backup_retention:
                        old_backup = os.path.join('backups', backups.pop(0))
                        os.remove(old_backup)
                        if os.path.exists(f"{old_backup}-wal"):
                            os.remove(f"{old_backup}-wal")
                        if os.path.exists(f"{old_backup}-shm"):
                            os.remove(f"{old_backup}-shm")
                        logger.info(f"Removed old backup: {old_backup}")
                    
                    last_backup = current_time
                except Exception as e:
                    logger.error(f"Failed to create DB backup: {e}")
            
            try:
                with get_db() as conn:
                    cur = conn.cursor()
                    cur.execute('DELETE FROM notifications WHERE expires_at IS NOT NULL AND expires_at < ?',
                               (datetime.now().isoformat(),))
                    if cur.rowcount > 0:
                        logger.info(f"Cleaned {cur.rowcount} expired notifications")
            except Exception as e:
                logger.error(f"Failed to clean notifications: {e}")
            
            time.sleep(30)
        except Exception as e:
            logger.error(f"Error in resource monitor: {e}")
            time.sleep(60)

# ============================================================================
# Error Handlers
# ============================================================================
@app.errorhandler(404)
def not_found_error(error):
    if request.is_json or request.headers.get('Content-Type') == 'application/json':
        return jsonify({'success': False, 'error': 'Not found'}), 404
    return render_template('errors/404.html', panel_name=get_setting('site_name', 'KrysolVM')), 404

@app.errorhandler(500)
def internal_error(error):
    if request.is_json or request.headers.get('Content-Type') == 'application/json':
        return jsonify({'success': False, 'error': 'Internal server error'}), 500
    return render_template('errors/500.html', panel_name=get_setting('site_name', 'KrysolVM')), 500

@app.errorhandler(403)
def forbidden_error(error):
    if request.is_json or request.headers.get('Content-Type') == 'application/json':
        return jsonify({'success': False, 'error': 'Forbidden'}), 403
    return render_template('errors/403.html', panel_name=get_setting('site_name', 'KrysolVM')), 403

@app.errorhandler(401)
def unauthorized_error(error):
    if request.is_json or request.headers.get('Content-Type') == 'application/json':
        return jsonify({'success': False, 'error': 'Unauthorized'}), 401
    flash('Please log in to access this page', 'warning')
    return redirect(url_for('login'))

# ============================================================================
# Static file serving
# ============================================================================
@app.route('/static/<path:filename>')
def serve_static(filename):
    return send_from_directory('static', filename)

@app.route('/favicon.ico')
def favicon():
    favicon_path = get_setting('favicon', '/static/img/favicon.ico')
    if favicon_path.startswith('/static/'):
        return send_from_directory('static', favicon_path.replace('/static/', ''))
    return send_from_directory('static/img', 'favicon.ico')

# ============================================================================
# Health check endpoint
# ============================================================================
@app.route('/health')
def health_check():
    try:
        with get_db() as conn:
            conn.execute('SELECT 1')
        db_status = 'ok'
    except:
        db_status = 'error'
    
    try:
        lxc_check = subprocess.run(['lxc', '--version'], capture_output=True, text=True, timeout=5)
        lxc_status = 'ok' if lxc_check.returncode == 0 else 'error'
    except:
        lxc_status = 'error'
    
    return jsonify({
        'status': 'ok',
        'timestamp': datetime.now().isoformat(),
        'database': db_status,
        'lxc': lxc_status,
        'version': PANEL_VERSION,
        'uptime': get_host_uptime()
    })

@app.route('/api/test/vps/<int:vps_id>')
@login_required
def test_vps_data(vps_id):
    vps = get_vps_by_id(vps_id)
    if not vps:
        return jsonify({'error': 'VPS not found'}), 404
    
    return jsonify({
        'vps_id': vps['id'],
        'container_name': vps['container_name'],
        'suspended': vps.get('suspended'),
        'suspended_type': str(type(vps.get('suspended'))),
        'whitelisted': vps.get('whitelisted'),
        'whitelisted_type': str(type(vps.get('whitelisted'))),
        'os_version': vps.get('os_version'),
        'status': vps.get('status'),
        'is_suspended_check': is_vps_suspended(vps),
        'is_whitelisted_check': is_vps_whitelisted(vps)
    })

# ============================================================================
# Template filters
# ============================================================================
@app.template_filter('relative_time')
def relative_time_filter(dt):
    return relativeTime(dt)

@app.template_filter('parse_datetime')
def parse_datetime_filter(dt_string):
    if not dt_string:
        return None
    try:
        return datetime.fromisoformat(dt_string)
    except:
        return None

@app.template_filter('json_loads')
def json_loads_filter(s):
    if s:
        try:
            return json.loads(s)
        except:
            return {}
    return {}

@app.template_filter('get_os_icon')
def get_os_icon_filter(icon_name):
    icons = {
        'ubuntu': 'fab fa-ubuntu',
        'debian': 'fab fa-debian',
        'centos': 'fab fa-centos',
        'alpine': 'fas fa-mountain',
        'fedora': 'fab fa-fedora',
        'rocky': 'fas fa-mountain',
        'default': 'fab fa-linux'
    }
    return icons.get(icon_name, icons['default'])
    
@app.template_filter('format_bytes')
def format_bytes_filter(bytes):
    if not bytes:
        return '0 B'
    
    bytes = float(bytes)
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes < 1024.0:
            return f"{bytes:.1f} {unit}"
        bytes /= 1024.0
    return f"{bytes:.1f} PB"

@app.template_filter('truncate')
def truncate_filter(s, length=50):
    if not s:
        return ""

    s = str(s)

    if len(s) <= length:
        return s

    return s[:length] + "..."

# ============================================================================
# Main entry point
# ============================================================================
if __name__ == "__main__":
    init_db()
    migrate_discord_auth()
    logger.info(f"{PANEL_NAME} v{PANEL_VERSION} starting...")
    
    os.makedirs('static/uploads/profiles', exist_ok=True)
    os.makedirs('static/uploads/settings', exist_ok=True)
    os.makedirs('static/uploads/os_icons', exist_ok=True)
    os.makedirs('static/img', exist_ok=True)
    os.makedirs('templates', exist_ok=True)
    os.makedirs('templates/admin', exist_ok=True)
    os.makedirs('templates/errors', exist_ok=True)
    os.makedirs('backups', exist_ok=True)
    
    monitor_thread = threading.Thread(target=resource_monitor, daemon=True)
    monitor_thread.start()
    
    logger.info(f"Starting server on {HOST}:{PORT}")
    logger.info(f"SocketIO available: {SOCKETIO_AVAILABLE}")
    
    if DEBUG_MODE:
        if socketio:
            socketio.run(app, host=HOST, port=PORT, debug=True, allow_unsafe_werkzeug=True)
        else:
            app.run(host=HOST, port=PORT, debug=True, threaded=True)
    else:
        if socketio:
            logger.info("Starting with SocketIO support (production mode)")
            logger.info("Using Flask-SocketIO's built-in production server")
            logger.info("For best performance, ensure eventlet or gevent is installed:")
            logger.info("  pip install eventlet  OR  pip install gevent gevent-websocket")
            socketio.run(app, host=HOST, port=PORT, debug=False, allow_unsafe_werkzeug=True)
        elif HYPERCORN_AVAILABLE and ASGIREF_AVAILABLE:
            from asgiref.wsgi import WsgiToAsgi
            
            config = HyperConfig()
            config.bind = [f"{HOST}:{PORT}"]
            config.use_reloader = False
            config.errorlog = logging.getLogger('krysolvm_panel')
            config.accesslog = logging.getLogger('krysolvm_panel')
            config.workers = 4
            
            try:
                logger.info("Starting with Hypercorn (ASGI mode, no SocketIO)")
                asgi_app = WsgiToAsgi(app)
                asyncio.run(serve(asgi_app, config))
            except KeyboardInterrupt:
                logger.info("Shutting down...")
                resource_monitor_active = False
                sys.exit(0)
        else:
            logger.warning("Running with Flask development server (not recommended for production)")
            app.run(host=HOST, port=PORT, debug=False, threaded=True)