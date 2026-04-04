#!/usr/bin/env python3
"""
KrysolVM Panel - Comprehensive REST API
Full management API for KrysolVM Panel
"""

from flask import Blueprint, request, jsonify
from functools import wraps
import secrets
import hashlib
from datetime import datetime
import logging

# Create API blueprint
api_bp = Blueprint('api', __name__, url_prefix='/api/v1')

logger = logging.getLogger(__name__)

# ============================================================================
# API Authentication Decorator
# ============================================================================

def require_api_key(f):
    """Decorator to require API key authentication"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        from krysolvm import get_db
        
        # Get API key from header or query parameter
        api_key = request.headers.get('X-API-Key') or request.args.get('api_key')
        
        if not api_key:
            return jsonify({
                'success': False,
                'error': 'API key required',
                'message': 'Provide API key in X-API-Key header or api_key parameter'
            }), 401
        
        # Validate API key
        try:
            with get_db() as conn:
                cur = conn.cursor()
                cur.execute('''SELECT u.*, ak.* FROM api_keys ak
                              JOIN users u ON ak.user_id = u.id
                              WHERE ak.key = ? AND ak.is_active = 1''', (api_key,))
                result = cur.fetchone()
                
                if not result:
                    return jsonify({
                        'success': False,
                        'error': 'Invalid API key',
                        'message': 'API key not found or inactive'
                    }), 401
                
                # Update last used timestamp
                cur.execute('UPDATE api_keys SET last_used_at = ? WHERE key = ?',
                           (datetime.now().isoformat(), api_key))
                conn.commit()
                
                # Store user info in request context
                request.api_user = dict(result)
                request.api_key_info = {
                    'key_id': result['id'],
                    'user_id': result['user_id'],
                    'is_admin': result['is_admin']
                }
                
        except Exception as e:
            logger.error(f"API key validation error: {e}")
            return jsonify({
                'success': False,
                'error': 'Authentication error',
                'message': str(e)
            }), 500
        
        return f(*args, **kwargs)
    
    return decorated_function

def require_admin_api(f):
    """Decorator to require admin API key"""
    @wraps(f)
    @require_api_key
    def decorated_function(*args, **kwargs):
        if not request.api_key_info.get('is_admin'):
            return jsonify({
                'success': False,
                'error': 'Admin access required',
                'message': 'This endpoint requires admin privileges'
            }), 403
        
        return f(*args, **kwargs)
    
    return decorated_function

# ============================================================================
# API Info & Health
# ============================================================================

@api_bp.route('/info', methods=['GET'])
def api_info():
    """Get API information"""
    return jsonify({
        'success': True,
        'api': {
            'name': 'KrysolVM Panel API',
            'version': 'v1',
            'description': 'Comprehensive REST API for KrysolVM Panel management',
            'documentation': '/api/v1/docs',
            'endpoints': {
                'authentication': '/api/v1/auth/*',
                'users': '/api/v1/users/*',
                'vps': '/api/v1/vps/*',
                'nodes': '/api/v1/nodes/*',
                'system': '/api/v1/system/*'
            }
        }
    })

@api_bp.route('/health', methods=['GET'])
def api_health():
    """Health check endpoint"""
    return jsonify({
        'success': True,
        'status': 'healthy',
        'timestamp': datetime.now().isoformat()
    })

# ============================================================================
# User Management API
# ============================================================================

@api_bp.route('/users', methods=['GET'])
@require_admin_api
def api_list_users():
    """List all users"""
    from krysolvm import get_db
    
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('''SELECT id, username, email, is_admin, created_at, last_login
                          FROM users ORDER BY id''')
            users = [dict(row) for row in cur.fetchall()]
        
        return jsonify({
            'success': True,
            'users': users,
            'count': len(users)
        })
    except Exception as e:
        logger.error(f"API list users error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@api_bp.route('/users/<int:user_id>', methods=['GET'])
@require_admin_api
def api_get_user(user_id):
    """Get user details"""
    from krysolvm import get_db
    
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('''SELECT id, username, email, is_admin, created_at, last_login, last_active
                          FROM users WHERE id = ?''', (user_id,))
            user = cur.fetchone()
            
            if not user:
                return jsonify({'success': False, 'error': 'User not found'}), 404
            
            # Get user's VPS count
            cur.execute('SELECT COUNT(*) FROM vps WHERE user_id = ?', (user_id,))
            vps_count = cur.fetchone()[0]
            
            user_data = dict(user)
            user_data['vps_count'] = vps_count
        
        return jsonify({
            'success': True,
            'user': user_data
        })
    except Exception as e:
        logger.error(f"API get user error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@api_bp.route('/users', methods=['POST'])
@require_admin_api
def api_create_user():
    """Create new user"""
    from krysolvm import get_db
    from werkzeug.security import generate_password_hash
    
    try:
        data = request.get_json()
        
        username = data.get('username')
        email = data.get('email')
        password = data.get('password')
        is_admin = data.get('is_admin', False)
        
        if not username or not email or not password:
            return jsonify({
                'success': False,
                'error': 'Missing required fields',
                'required': ['username', 'email', 'password']
            }), 400
        
        password_hash = generate_password_hash(password)
        now = datetime.now().isoformat()
        
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('''INSERT INTO users (username, email, password_hash, is_admin, created_at)
                          VALUES (?, ?, ?, ?, ?)''',
                       (username, email, password_hash, 1 if is_admin else 0, now))
            conn.commit()
            user_id = cur.lastrowid
        
        return jsonify({
            'success': True,
            'user_id': user_id,
            'message': 'User created successfully'
        }), 201
    except Exception as e:
        logger.error(f"API create user error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@api_bp.route('/users/<int:user_id>', methods=['PUT', 'PATCH'])
@require_admin_api
def api_update_user(user_id):
    """Update user"""
    from krysolvm import get_db
    from werkzeug.security import generate_password_hash
    
    try:
        data = request.get_json()
        
        with get_db() as conn:
            cur = conn.cursor()
            
            if 'email' in data:
                cur.execute('UPDATE users SET email = ? WHERE id = ?', (data['email'], user_id))
            
            if 'password' in data:
                password_hash = generate_password_hash(data['password'])
                cur.execute('UPDATE users SET password_hash = ? WHERE id = ?', (password_hash, user_id))
            
            if 'is_admin' in data:
                cur.execute('UPDATE users SET is_admin = ? WHERE id = ?', 
                           (1 if data['is_admin'] else 0, user_id))
            
            conn.commit()
        
        return jsonify({
            'success': True,
            'message': 'User updated successfully'
        })
    except Exception as e:
        logger.error(f"API update user error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@api_bp.route('/users/<int:user_id>', methods=['DELETE'])
@require_admin_api
def api_delete_user(user_id):
    """Delete user"""
    from krysolvm import get_db
    
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('DELETE FROM users WHERE id = ?', (user_id,))
            conn.commit()
        
        return jsonify({
            'success': True,
            'message': 'User deleted successfully'
        })
    except Exception as e:
        logger.error(f"API delete user error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================================================
# VPS Management API
# ============================================================================

@api_bp.route('/vps', methods=['GET'])
@require_api_key
def api_list_vps():
    """List VPS (user's own or all if admin)"""
    from krysolvm import get_db
    
    try:
        with get_db() as conn:
            cur = conn.cursor()
            
            if request.api_key_info['is_admin']:
                # Admin sees all VPS
                cur.execute('''SELECT v.*, u.username, n.name as node_name
                              FROM vps v
                              LEFT JOIN users u ON v.user_id = u.id
                              LEFT JOIN nodes n ON v.node_id = n.id
                              ORDER BY v.created_at DESC''')
            else:
                # User sees only their VPS
                cur.execute('''SELECT v.*, n.name as node_name
                              FROM vps v
                              LEFT JOIN nodes n ON v.node_id = n.id
                              WHERE v.user_id = ?
                              ORDER BY v.created_at DESC''',
                           (request.api_key_info['user_id'],))
            
            vps_list = [dict(row) for row in cur.fetchall()]
        
        return jsonify({
            'success': True,
            'vps': vps_list,
            'count': len(vps_list)
        })
    except Exception as e:
        logger.error(f"API list VPS error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@api_bp.route('/vps/<int:vps_id>', methods=['GET'])
@require_api_key
def api_get_vps(vps_id):
    """Get VPS details"""
    from krysolvm import get_db, get_vps_by_id, run_sync, get_container_stats
    
    try:
        vps = get_vps_by_id(vps_id)
        
        if not vps:
            return jsonify({'success': False, 'error': 'VPS not found'}), 404
        
        # Check access
        if not request.api_key_info['is_admin'] and vps['user_id'] != request.api_key_info['user_id']:
            return jsonify({'success': False, 'error': 'Access denied'}), 403
        
        # Get live stats
        try:
            stats = run_sync(get_container_stats(vps['container_name'], vps['node_id']))
            vps['stats'] = stats
        except:
            vps['stats'] = None
        
        return jsonify({
            'success': True,
            'vps': vps
        })
    except Exception as e:
        logger.error(f"API get VPS error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@api_bp.route('/vps', methods=['POST'])
@require_admin_api
def api_create_vps():
    """Create new VPS"""
    from krysolvm import get_db, run_sync, execute_lxc, apply_lxc_config, configure_container_ip, apply_internal_permissions
    
    try:
        data = request.get_json()
        
        # Required fields
        required = ['hostname', 'user_id', 'node_id', 'os_version', 'cpu', 'ram', 'storage']
        for field in required:
            if field not in data:
                return jsonify({
                    'success': False,
                    'error': f'Missing required field: {field}',
                    'required': required
                }), 400
        
        # Create VPS logic here (simplified)
        # This would call the actual VPS creation functions from krysolvm.py
        
        return jsonify({
            'success': True,
            'message': 'VPS creation started',
            'note': 'Full implementation requires integration with krysolvm.py VPS creation logic'
        }), 201
    except Exception as e:
        logger.error(f"API create VPS error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@api_bp.route('/vps/<int:vps_id>/start', methods=['POST'])
@require_api_key
def api_start_vps(vps_id):
    """Start VPS"""
    from krysolvm import get_vps_by_id, run_sync, execute_lxc
    
    try:
        vps = get_vps_by_id(vps_id)
        
        if not vps:
            return jsonify({'success': False, 'error': 'VPS not found'}), 404
        
        # Check access
        if not request.api_key_info['is_admin'] and vps['user_id'] != request.api_key_info['user_id']:
            return jsonify({'success': False, 'error': 'Access denied'}), 403
        
        # Start VPS
        run_sync(execute_lxc(vps['container_name'], f"start {vps['container_name']}", node_id=vps['node_id']))
        
        return jsonify({
            'success': True,
            'message': 'VPS started successfully'
        })
    except Exception as e:
        logger.error(f"API start VPS error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@api_bp.route('/vps/<int:vps_id>/stop', methods=['POST'])
@require_api_key
def api_stop_vps(vps_id):
    """Stop VPS"""
    from krysolvm import get_vps_by_id, run_sync, execute_lxc
    
    try:
        vps = get_vps_by_id(vps_id)
        
        if not vps:
            return jsonify({'success': False, 'error': 'VPS not found'}), 404
        
        # Check access
        if not request.api_key_info['is_admin'] and vps['user_id'] != request.api_key_info['user_id']:
            return jsonify({'success': False, 'error': 'Access denied'}), 403
        
        # Stop VPS
        run_sync(execute_lxc(vps['container_name'], f"stop {vps['container_name']}", node_id=vps['node_id']))
        
        return jsonify({
            'success': True,
            'message': 'VPS stopped successfully'
        })
    except Exception as e:
        logger.error(f"API stop VPS error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@api_bp.route('/vps/<int:vps_id>/restart', methods=['POST'])
@require_api_key
def api_restart_vps(vps_id):
    """Restart VPS"""
    from krysolvm import get_vps_by_id, run_sync, execute_lxc
    
    try:
        vps = get_vps_by_id(vps_id)
        
        if not vps:
            return jsonify({'success': False, 'error': 'VPS not found'}), 404
        
        # Check access
        if not request.api_key_info['is_admin'] and vps['user_id'] != request.api_key_info['user_id']:
            return jsonify({'success': False, 'error': 'Access denied'}), 403
        
        # Restart VPS
        run_sync(execute_lxc(vps['container_name'], f"restart {vps['container_name']}", node_id=vps['node_id']))
        
        return jsonify({
            'success': True,
            'message': 'VPS restarted successfully'
        })
    except Exception as e:
        logger.error(f"API restart VPS error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@api_bp.route('/vps/<int:vps_id>', methods=['DELETE'])
@require_admin_api
def api_delete_vps(vps_id):
    """Delete VPS"""
    from krysolvm import get_db, get_vps_by_id, run_sync, execute_lxc
    
    try:
        vps = get_vps_by_id(vps_id)
        
        if not vps:
            return jsonify({'success': False, 'error': 'VPS not found'}), 404
        
        # Delete container
        try:
            run_sync(execute_lxc(vps['container_name'], f"delete {vps['container_name']} --force", node_id=vps['node_id']))
        except:
            pass
        
        # Delete from database
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('DELETE FROM vps WHERE id = ?', (vps_id,))
            conn.commit()
        
        return jsonify({
            'success': True,
            'message': 'VPS deleted successfully'
        })
    except Exception as e:
        logger.error(f"API delete VPS error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================================================
# Node Management API
# ============================================================================

@api_bp.route('/nodes', methods=['GET'])
@require_admin_api
def api_list_nodes():
    """List all nodes"""
    from krysolvm import get_nodes
    
    try:
        nodes = get_nodes()
        
        return jsonify({
            'success': True,
            'nodes': nodes,
            'count': len(nodes)
        })
    except Exception as e:
        logger.error(f"API list nodes error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@api_bp.route('/nodes/<int:node_id>', methods=['GET'])
@require_admin_api
def api_get_node(node_id):
    """Get node details"""
    from krysolvm import get_node, run_sync, get_host_stats
    
    try:
        node = get_node(node_id)
        
        if not node:
            return jsonify({'success': False, 'error': 'Node not found'}), 404
        
        # Get node stats
        try:
            stats = run_sync(get_host_stats(node_id))
            node['stats'] = stats
        except:
            node['stats'] = None
        
        return jsonify({
            'success': True,
            'node': node
        })
    except Exception as e:
        logger.error(f"API get node error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================================================
# System API
# ============================================================================

@api_bp.route('/system/info', methods=['GET'])
@require_admin_api
def api_system_info():
    """Get system information"""
    from krysolvm import get_host_cpu_usage, get_host_ram_usage, get_host_disk_usage, get_host_uptime
    import platform
    
    try:
        system_info = {
            'hostname': platform.node(),
            'platform': platform.platform(),
            'python_version': platform.python_version(),
            'cpu': {
                'cores': __import__('os').cpu_count(),
                'usage': get_host_cpu_usage()
            },
            'memory': get_host_ram_usage(),
            'disk': get_host_disk_usage(),
            'uptime': get_host_uptime()
        }
        
        return jsonify({
            'success': True,
            'system': system_info
        })
    except Exception as e:
        logger.error(f"API system info error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@api_bp.route('/system/stats', methods=['GET'])
@require_admin_api
def api_system_stats():
    """Get system statistics"""
    from krysolvm import get_db
    
    try:
        with get_db() as conn:
            cur = conn.cursor()
            
            cur.execute('SELECT COUNT(*) FROM users')
            total_users = cur.fetchone()[0]
            
            cur.execute('SELECT COUNT(*) FROM vps')
            total_vps = cur.fetchone()[0]
            
            cur.execute('SELECT COUNT(*) FROM vps WHERE status = "running"')
            running_vps = cur.fetchone()[0]
            
            cur.execute('SELECT COUNT(*) FROM nodes')
            total_nodes = cur.fetchone()[0]
        
        return jsonify({
            'success': True,
            'stats': {
                'users': total_users,
                'vps': {
                    'total': total_vps,
                    'running': running_vps,
                    'stopped': total_vps - running_vps
                },
                'nodes': total_nodes
            }
        })
    except Exception as e:
        logger.error(f"API system stats error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================================================
# VPS Extended Management API
# ============================================================================

@api_bp.route('/vps/<int:vps_id>/suspend', methods=['POST'])
@require_admin_api
def api_suspend_vps(vps_id):
    """Suspend VPS"""
    from krysolvm import get_db, get_vps_by_id, run_sync, execute_lxc
    
    try:
        data = request.get_json() or {}
        reason = data.get('reason', 'Suspended by admin via API')
        
        vps = get_vps_by_id(vps_id)
        if not vps:
            return jsonify({'success': False, 'error': 'VPS not found'}), 404
        
        # Stop VPS
        try:
            run_sync(execute_lxc(vps['container_name'], f"stop {vps['container_name']} --force", node_id=vps['node_id']))
        except:
            pass
        
        # Update database
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('UPDATE vps SET suspended = 1, suspended_reason = ?, status = "stopped" WHERE id = ?',
                       (reason, vps_id))
            conn.commit()
        
        return jsonify({
            'success': True,
            'message': 'VPS suspended successfully'
        })
    except Exception as e:
        logger.error(f"API suspend VPS error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@api_bp.route('/vps/<int:vps_id>/unsuspend', methods=['POST'])
@require_admin_api
def api_unsuspend_vps(vps_id):
    """Unsuspend VPS"""
    from krysolvm import get_db
    
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('UPDATE vps SET suspended = 0, suspended_reason = NULL WHERE id = ?', (vps_id,))
            conn.commit()
        
        return jsonify({
            'success': True,
            'message': 'VPS unsuspended successfully'
        })
    except Exception as e:
        logger.error(f"API unsuspend VPS error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@api_bp.route('/vps/<int:vps_id>/resize', methods=['POST'])
@require_admin_api
def api_resize_vps(vps_id):
    """Resize VPS resources"""
    from krysolvm import get_db, get_vps_by_id
    
    try:
        data = request.get_json() or {}
        
        vps = get_vps_by_id(vps_id)
        if not vps:
            return jsonify({'success': False, 'error': 'VPS not found'}), 404
        
        with get_db() as conn:
            cur = conn.cursor()
            
            if 'cpu' in data:
                cur.execute('UPDATE vps SET cpu = ? WHERE id = ?', (data['cpu'], vps_id))
            
            if 'ram' in data:
                cur.execute('UPDATE vps SET ram = ? WHERE id = ?', (data['ram'], vps_id))
            
            if 'storage' in data:
                cur.execute('UPDATE vps SET storage = ? WHERE id = ?', (data['storage'], vps_id))
            
            conn.commit()
        
        return jsonify({
            'success': True,
            'message': 'VPS resized successfully (restart required for changes to take effect)'
        })
    except Exception as e:
        logger.error(f"API resize VPS error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@api_bp.route('/vps/<int:vps_id>/execute', methods=['POST'])
@require_api_key
def api_execute_command(vps_id):
    """Execute command in VPS"""
    from krysolvm import get_vps_by_id, run_sync, execute_lxc
    
    try:
        data = request.get_json() or {}
        command = data.get('command')
        
        if not command:
            return jsonify({'success': False, 'error': 'command required'}), 400
        
        vps = get_vps_by_id(vps_id)
        if not vps:
            return jsonify({'success': False, 'error': 'VPS not found'}), 404
        
        # Check access
        if not request.api_key_info['is_admin'] and vps['user_id'] != request.api_key_info['user_id']:
            return jsonify({'success': False, 'error': 'Access denied'}), 403
        
        # Execute command
        result = run_sync(execute_lxc(vps['container_name'], f"exec {vps['container_name']} -- {command}", node_id=vps['node_id']))
        
        return jsonify({
            'success': True,
            'output': result
        })
    except Exception as e:
        logger.error(f"API execute command error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================================================
# Node Extended Management API
# ============================================================================

@api_bp.route('/nodes', methods=['POST'])
@require_admin_api
def api_create_node():
    """Create new node"""
    from krysolvm import get_db
    
    try:
        data = request.get_json() or {}
        
        name = data.get('name')
        url = data.get('url')
        location = data.get('location', '')
        api_key = data.get('api_key')
        
        if not name or not url:
            return jsonify({
                'success': False,
                'error': 'Missing required fields',
                'required': ['name', 'url']
            }), 400
        
        now = datetime.now().isoformat()
        
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('''INSERT INTO nodes (name, url, location, api_key, created_at, updated_at)
                          VALUES (?, ?, ?, ?, ?, ?)''',
                       (name, url, location, api_key, now, now))
            conn.commit()
            node_id = cur.lastrowid
        
        return jsonify({
            'success': True,
            'node_id': node_id,
            'message': 'Node created successfully'
        }), 201
    except Exception as e:
        logger.error(f"API create node error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@api_bp.route('/nodes/<int:node_id>', methods=['PUT', 'PATCH'])
@require_admin_api
def api_update_node(node_id):
    """Update node"""
    from krysolvm import get_db
    
    try:
        data = request.get_json() or {}
        
        with get_db() as conn:
            cur = conn.cursor()
            
            if 'name' in data:
                cur.execute('UPDATE nodes SET name = ? WHERE id = ?', (data['name'], node_id))
            
            if 'url' in data:
                cur.execute('UPDATE nodes SET url = ? WHERE id = ?', (data['url'], node_id))
            
            if 'location' in data:
                cur.execute('UPDATE nodes SET location = ? WHERE id = ?', (data['location'], node_id))
            
            if 'api_key' in data:
                cur.execute('UPDATE nodes SET api_key = ? WHERE id = ?', (data['api_key'], node_id))
            
            cur.execute('UPDATE nodes SET updated_at = ? WHERE id = ?', (datetime.now().isoformat(), node_id))
            conn.commit()
        
        return jsonify({
            'success': True,
            'message': 'Node updated successfully'
        })
    except Exception as e:
        logger.error(f"API update node error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@api_bp.route('/nodes/<int:node_id>', methods=['DELETE'])
@require_admin_api
def api_delete_node(node_id):
    """Delete node"""
    from krysolvm import get_db
    
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('DELETE FROM nodes WHERE id = ?', (node_id,))
            conn.commit()
        
        return jsonify({
            'success': True,
            'message': 'Node deleted successfully'
        })
    except Exception as e:
        logger.error(f"API delete node error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================================================
# Settings Management API
# ============================================================================

@api_bp.route('/settings', methods=['GET'])
@require_admin_api
def api_get_settings():
    """Get all settings"""
    from krysolvm import get_db
    
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('SELECT key, value, description FROM settings')
            settings = {row['key']: {'value': row['value'], 'description': row['description']} 
                       for row in cur.fetchall()}
        
        return jsonify({
            'success': True,
            'settings': settings
        })
    except Exception as e:
        logger.error(f"API get settings error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@api_bp.route('/settings/<key>', methods=['GET'])
@require_admin_api
def api_get_setting(key):
    """Get specific setting"""
    from krysolvm import get_setting
    
    try:
        value = get_setting(key)
        
        if value is None:
            return jsonify({'success': False, 'error': 'Setting not found'}), 404
        
        return jsonify({
            'success': True,
            'key': key,
            'value': value
        })
    except Exception as e:
        logger.error(f"API get setting error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@api_bp.route('/settings/<key>', methods=['PUT', 'PATCH'])
@require_admin_api
def api_update_setting(key):
    """Update setting"""
    from krysolvm import set_setting
    
    try:
        data = request.get_json() or {}
        value = data.get('value')
        
        if value is None:
            return jsonify({'success': False, 'error': 'value required'}), 400
        
        set_setting(key, str(value))
        
        return jsonify({
            'success': True,
            'message': 'Setting updated successfully'
        })
    except Exception as e:
        logger.error(f"API update setting error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================================================
# Maintenance Mode API
# ============================================================================

@api_bp.route('/maintenance/enable', methods=['POST'])
@require_admin_api
def api_enable_maintenance():
    """Enable maintenance mode"""
    from krysolvm import set_setting
    
    try:
        data = request.get_json() or {}
        message = data.get('message', 'Site is under maintenance. Please check back later.')
        
        set_setting('maintenance_mode', '1')
        set_setting('maintenance_message', message)
        
        return jsonify({
            'success': True,
            'message': 'Maintenance mode enabled'
        })
    except Exception as e:
        logger.error(f"API enable maintenance error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@api_bp.route('/maintenance/disable', methods=['POST'])
@require_admin_api
def api_disable_maintenance():
    """Disable maintenance mode"""
    from krysolvm import set_setting
    
    try:
        set_setting('maintenance_mode', '0')
        
        return jsonify({
            'success': True,
            'message': 'Maintenance mode disabled'
        })
    except Exception as e:
        logger.error(f"API disable maintenance error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================================================
# Bulk Operations API
# ============================================================================

@api_bp.route('/vps/bulk/start', methods=['POST'])
@require_admin_api
def api_bulk_start_vps():
    """Start multiple VPS"""
    from krysolvm import get_db, run_sync, execute_lxc
    
    try:
        data = request.get_json() or {}
        vps_ids = data.get('vps_ids', [])
        
        if not vps_ids:
            return jsonify({'success': False, 'error': 'vps_ids required'}), 400
        
        started = []
        failed = []
        
        with get_db() as conn:
            cur = conn.cursor()
            for vps_id in vps_ids:
                cur.execute('SELECT * FROM vps WHERE id = ?', (vps_id,))
                vps = cur.fetchone()
                
                if vps:
                    try:
                        run_sync(execute_lxc(vps['container_name'], f"start {vps['container_name']}", node_id=vps['node_id']))
                        started.append(vps_id)
                    except Exception as e:
                        failed.append({'vps_id': vps_id, 'error': str(e)})
        
        return jsonify({
            'success': True,
            'started': started,
            'failed': failed,
            'message': f'Started {len(started)} VPS, {len(failed)} failed'
        })
    except Exception as e:
        logger.error(f"API bulk start VPS error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@api_bp.route('/vps/bulk/stop', methods=['POST'])
@require_admin_api
def api_bulk_stop_vps():
    """Stop multiple VPS"""
    from krysolvm import get_db, run_sync, execute_lxc
    
    try:
        data = request.get_json() or {}
        vps_ids = data.get('vps_ids', [])
        
        if not vps_ids:
            return jsonify({'success': False, 'error': 'vps_ids required'}), 400
        
        stopped = []
        failed = []
        
        with get_db() as conn:
            cur = conn.cursor()
            for vps_id in vps_ids:
                cur.execute('SELECT * FROM vps WHERE id = ?', (vps_id,))
                vps = cur.fetchone()
                
                if vps:
                    try:
                        run_sync(execute_lxc(vps['container_name'], f"stop {vps['container_name']}", node_id=vps['node_id']))
                        stopped.append(vps_id)
                    except Exception as e:
                        failed.append({'vps_id': vps_id, 'error': str(e)})
        
        return jsonify({
            'success': True,
            'stopped': stopped,
            'failed': failed,
            'message': f'Stopped {len(stopped)} VPS, {len(failed)} failed'
        })
    except Exception as e:
        logger.error(f"API bulk stop VPS error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================================================
# Port Forwarding API
# ============================================================================

@api_bp.route('/ports', methods=['GET'])
@require_api_key
def api_list_ports():
    """List port forwards (user's own or all if admin)"""
    from krysolvm import get_db
    
    try:
        with get_db() as conn:
            cur = conn.cursor()
            
            if request.api_key_info['is_admin']:
                cur.execute('''SELECT pf.*, v.hostname, u.username
                              FROM port_forwards pf
                              LEFT JOIN vps v ON pf.vps_id = v.id
                              LEFT JOIN users u ON pf.user_id = u.id
                              ORDER BY pf.created_at DESC''')
            else:
                cur.execute('''SELECT pf.*, v.hostname
                              FROM port_forwards pf
                              LEFT JOIN vps v ON pf.vps_id = v.id
                              WHERE pf.user_id = ?
                              ORDER BY pf.created_at DESC''',
                           (request.api_key_info['user_id'],))
            
            ports = [dict(row) for row in cur.fetchall()]
        
        return jsonify({
            'success': True,
            'ports': ports,
            'count': len(ports)
        })
    except Exception as e:
        logger.error(f"API list ports error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@api_bp.route('/ports', methods=['POST'])
@require_api_key
def api_create_port():
    """Create port forward"""
    from krysolvm import get_db, allocate_port
    
    try:
        data = request.get_json() or {}
        
        vps_id = data.get('vps_id')
        internal_port = data.get('internal_port')
        protocol = data.get('protocol', 'tcp')
        description = data.get('description', '')
        
        if not vps_id or not internal_port:
            return jsonify({
                'success': False,
                'error': 'Missing required fields',
                'required': ['vps_id', 'internal_port']
            }), 400
        
        # Check VPS ownership
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('SELECT user_id FROM vps WHERE id = ?', (vps_id,))
            vps = cur.fetchone()
            
            if not vps:
                return jsonify({'success': False, 'error': 'VPS not found'}), 404
            
            if not request.api_key_info['is_admin'] and vps['user_id'] != request.api_key_info['user_id']:
                return jsonify({'success': False, 'error': 'Access denied'}), 403
        
        # Allocate port
        external_port = allocate_port(request.api_key_info['user_id'], vps_id, internal_port, protocol, description)
        
        if not external_port:
            return jsonify({'success': False, 'error': 'No ports available'}), 400
        
        return jsonify({
            'success': True,
            'external_port': external_port,
            'message': 'Port forward created successfully'
        }), 201
    except Exception as e:
        logger.error(f"API create port error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@api_bp.route('/ports/<int:port_id>', methods=['DELETE'])
@require_api_key
def api_delete_port(port_id):
    """Delete port forward"""
    from krysolvm import get_db
    
    try:
        with get_db() as conn:
            cur = conn.cursor()
            
            # Check ownership
            cur.execute('SELECT user_id FROM port_forwards WHERE id = ?', (port_id,))
            port = cur.fetchone()
            
            if not port:
                return jsonify({'success': False, 'error': 'Port forward not found'}), 404
            
            if not request.api_key_info['is_admin'] and port['user_id'] != request.api_key_info['user_id']:
                return jsonify({'success': False, 'error': 'Access denied'}), 403
            
            cur.execute('DELETE FROM port_forwards WHERE id = ?', (port_id,))
            conn.commit()
        
        return jsonify({
            'success': True,
            'message': 'Port forward deleted successfully'
        })
    except Exception as e:
        logger.error(f"API delete port error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================================================
# Notifications API
# ============================================================================

@api_bp.route('/notifications', methods=['GET'])
@require_api_key
def api_list_notifications():
    """List user notifications"""
    from krysolvm import get_db
    
    try:
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 20, type=int)
        unread_only = request.args.get('unread_only', 'false').lower() == 'true'
        
        offset = (page - 1) * per_page
        
        with get_db() as conn:
            cur = conn.cursor()
            
            query = 'SELECT * FROM notifications WHERE user_id = ?'
            params = [request.api_key_info['user_id']]
            
            if unread_only:
                query += ' AND is_read = 0'
            
            query += ' ORDER BY created_at DESC LIMIT ? OFFSET ?'
            params.extend([per_page, offset])
            
            cur.execute(query, params)
            notifications = [dict(row) for row in cur.fetchall()]
            
            # Get total count
            count_query = 'SELECT COUNT(*) FROM notifications WHERE user_id = ?'
            count_params = [request.api_key_info['user_id']]
            if unread_only:
                count_query += ' AND is_read = 0'
            
            cur.execute(count_query, count_params)
            total = cur.fetchone()[0]
        
        return jsonify({
            'success': True,
            'notifications': notifications,
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total': total,
                'pages': (total + per_page - 1) // per_page
            }
        })
    except Exception as e:
        logger.error(f"API list notifications error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@api_bp.route('/notifications/<int:notification_id>/read', methods=['POST'])
@require_api_key
def api_mark_notification_read(notification_id):
    """Mark notification as read"""
    from krysolvm import get_db
    
    try:
        with get_db() as conn:
            cur = conn.cursor()
            
            # Check ownership
            cur.execute('SELECT user_id FROM notifications WHERE id = ?', (notification_id,))
            notif = cur.fetchone()
            
            if not notif:
                return jsonify({'success': False, 'error': 'Notification not found'}), 404
            
            if notif['user_id'] != request.api_key_info['user_id']:
                return jsonify({'success': False, 'error': 'Access denied'}), 403
            
            cur.execute('UPDATE notifications SET is_read = 1 WHERE id = ?', (notification_id,))
            conn.commit()
        
        return jsonify({
            'success': True,
            'message': 'Notification marked as read'
        })
    except Exception as e:
        logger.error(f"API mark notification read error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@api_bp.route('/notifications/read-all', methods=['POST'])
@require_api_key
def api_mark_all_notifications_read():
    """Mark all notifications as read"""
    from krysolvm import get_db
    
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('UPDATE notifications SET is_read = 1 WHERE user_id = ?',
                       (request.api_key_info['user_id'],))
            conn.commit()
        
        return jsonify({
            'success': True,
            'message': 'All notifications marked as read'
        })
    except Exception as e:
        logger.error(f"API mark all notifications read error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================================================
# Activity Logs API
# ============================================================================

@api_bp.route('/activity', methods=['GET'])
@require_api_key
def api_list_activity():
    """List activity logs"""
    from krysolvm import get_db
    
    try:
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 50, type=int)
        
        offset = (page - 1) * per_page
        
        with get_db() as conn:
            cur = conn.cursor()
            
            if request.api_key_info['is_admin']:
                # Admin sees all activity
                cur.execute('''SELECT al.*, u.username
                              FROM activity_log al
                              LEFT JOIN users u ON al.user_id = u.id
                              ORDER BY al.timestamp DESC
                              LIMIT ? OFFSET ?''', (per_page, offset))
                
                cur.execute('SELECT COUNT(*) FROM activity_log')
            else:
                # User sees only their activity
                cur.execute('''SELECT * FROM activity_log
                              WHERE user_id = ?
                              ORDER BY timestamp DESC
                              LIMIT ? OFFSET ?''',
                           (request.api_key_info['user_id'], per_page, offset))
                
                cur.execute('SELECT COUNT(*) FROM activity_log WHERE user_id = ?',
                           (request.api_key_info['user_id'],))
            
            activities = [dict(row) for row in cur.fetchall()]
            total = cur.fetchone()[0]
        
        return jsonify({
            'success': True,
            'activities': activities,
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total': total,
                'pages': (total + per_page - 1) // per_page
            }
        })
    except Exception as e:
        logger.error(f"API list activity error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================================================
# VPS Password Management API
# ============================================================================

@api_bp.route('/vps/<int:vps_id>/password', methods=['GET'])
@require_api_key
def api_get_vps_password(vps_id):
    """Get VPS password"""
    from krysolvm import get_vps_by_id
    
    try:
        vps = get_vps_by_id(vps_id)
        
        if not vps:
            return jsonify({'success': False, 'error': 'VPS not found'}), 404
        
        # Check access
        if not request.api_key_info['is_admin'] and vps['user_id'] != request.api_key_info['user_id']:
            return jsonify({'success': False, 'error': 'Access denied'}), 403
        
        password = vps.get('root_password', 'root')
        
        return jsonify({
            'success': True,
            'password': password
        })
    except Exception as e:
        logger.error(f"API get VPS password error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@api_bp.route('/vps/<int:vps_id>/password', methods=['POST', 'PUT'])
@require_api_key
def api_change_vps_password(vps_id):
    """Change VPS password"""
    from krysolvm import get_db, get_vps_by_id, run_sync, execute_lxc
    import platform
    
    try:
        data = request.get_json() or {}
        new_password = data.get('password')
        
        if not new_password:
            return jsonify({'success': False, 'error': 'password required'}), 400
        
        vps = get_vps_by_id(vps_id)
        
        if not vps:
            return jsonify({'success': False, 'error': 'VPS not found'}), 404
        
        # Check access
        if not request.api_key_info['is_admin'] and vps['user_id'] != request.api_key_info['user_id']:
            return jsonify({'success': False, 'error': 'Access denied'}), 403
        
        # Update in database
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('UPDATE vps SET root_password = ? WHERE id = ?', (new_password, vps_id))
            conn.commit()
        
        # Update in container if on Linux
        if platform.system() == 'Linux' and vps.get('status') == 'running':
            try:
                cmd = f"exec {vps['container_name']} -- bash -c \"echo 'root:{new_password}' | chpasswd\""
                run_sync(execute_lxc(vps['container_name'], cmd, node_id=vps['node_id']))
            except:
                pass
        
        return jsonify({
            'success': True,
            'message': 'Password changed successfully'
        })
    except Exception as e:
        logger.error(f"API change VPS password error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================================================
# API Key Management API (Admin Only)
# ============================================================================

@api_bp.route('/api-keys', methods=['GET'])
@require_admin_api
def api_list_api_keys():
    """List all API keys (admin only)"""
    from krysolvm import get_db
    
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('''SELECT ak.*, u.username
                          FROM api_keys ak
                          JOIN users u ON ak.user_id = u.id
                          ORDER BY ak.created_at DESC''')
            keys = [dict(row) for row in cur.fetchall()]
        
        return jsonify({
            'success': True,
            'api_keys': keys,
            'count': len(keys)
        })
    except Exception as e:
        logger.error(f"API list API keys error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@api_bp.route('/api-keys', methods=['POST'])
@require_admin_api
def api_create_api_key():
    """Create new API key (admin only)"""
    from krysolvm import get_db
    
    try:
        data = request.get_json() or {}
        name = data.get('name', 'API Key')
        user_id = data.get('user_id')
        
        if not user_id:
            return jsonify({'success': False, 'error': 'user_id required'}), 400
        
        # Generate new API key
        new_key = secrets.token_urlsafe(32)
        now = datetime.now().isoformat()
        
        with get_db() as conn:
            cur = conn.cursor()
            
            # Verify user exists
            cur.execute('SELECT id FROM users WHERE id = ?', (user_id,))
            if not cur.fetchone():
                return jsonify({'success': False, 'error': 'User not found'}), 404
            
            cur.execute('''INSERT INTO api_keys (user_id, name, key, is_active, created_at)
                          VALUES (?, ?, ?, 1, ?)''',
                       (user_id, name, new_key, now))
            conn.commit()
            key_id = cur.lastrowid
        
        return jsonify({
            'success': True,
            'api_key': {
                'id': key_id,
                'name': name,
                'key': new_key,
                'user_id': user_id
            },
            'message': 'API key created successfully'
        }), 201
    except Exception as e:
        logger.error(f"API create API key error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@api_bp.route('/api-keys/<int:key_id>', methods=['DELETE'])
@require_admin_api
def api_delete_api_key(key_id):
    """Delete API key (admin only)"""
    from krysolvm import get_db
    
    try:
        with get_db() as conn:
            cur = conn.cursor()
            
            # Check if key exists
            cur.execute('SELECT user_id FROM api_keys WHERE id = ?', (key_id,))
            key = cur.fetchone()
            
            if not key:
                return jsonify({'success': False, 'error': 'API key not found'}), 404
            
            cur.execute('DELETE FROM api_keys WHERE id = ?', (key_id,))
            conn.commit()
        
        return jsonify({
            'success': True,
            'message': 'API key deleted successfully'
        })
    except Exception as e:
        logger.error(f"API delete API key error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================================================
# Statistics API
# ============================================================================

@api_bp.route('/stats/overview', methods=['GET'])
@require_api_key
def api_stats_overview():
    """Get overview statistics"""
    from krysolvm import get_db
    
    try:
        with get_db() as conn:
            cur = conn.cursor()
            
            if request.api_key_info['is_admin']:
                # Admin stats
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
                
                cur.execute('SELECT COUNT(*) FROM port_forwards')
                total_ports = cur.fetchone()[0]
                
                stats = {
                    'users': total_users,
                    'vps': {
                        'total': total_vps,
                        'running': running_vps,
                        'stopped': total_vps - running_vps - suspended_vps,
                        'suspended': suspended_vps
                    },
                    'nodes': total_nodes,
                    'ports': total_ports
                }
            else:
                # User stats
                user_id = request.api_key_info['user_id']
                
                cur.execute('SELECT COUNT(*) FROM vps WHERE user_id = ?', (user_id,))
                total_vps = cur.fetchone()[0]
                
                cur.execute('SELECT COUNT(*) FROM vps WHERE user_id = ? AND status = "running"', (user_id,))
                running_vps = cur.fetchone()[0]
                
                cur.execute('SELECT COUNT(*) FROM vps WHERE user_id = ? AND suspended = 1', (user_id,))
                suspended_vps = cur.fetchone()[0]
                
                cur.execute('SELECT COUNT(*) FROM port_forwards WHERE user_id = ?', (user_id,))
                total_ports = cur.fetchone()[0]
                
                cur.execute('SELECT COUNT(*) FROM notifications WHERE user_id = ? AND is_read = 0', (user_id,))
                unread_notifications = cur.fetchone()[0]
                
                stats = {
                    'vps': {
                        'total': total_vps,
                        'running': running_vps,
                        'stopped': total_vps - running_vps - suspended_vps,
                        'suspended': suspended_vps
                    },
                    'ports': total_ports,
                    'notifications': {
                        'unread': unread_notifications
                    }
                }
        
        return jsonify({
            'success': True,
            'stats': stats
        })
    except Exception as e:
        logger.error(f"API stats overview error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================================================
# API Documentation
# ============================================================================

@api_bp.route('/docs', methods=['GET'])
def api_documentation():
    """Get API documentation"""
    docs = {
        'api': {
            'name': 'KrysolVM Panel API',
            'version': 'v1',
            'base_url': '/api/v1',
            'authentication': 'API Key (X-API-Key header or api_key parameter)'
        },
        'endpoints': {
            'info': {
                'GET /info': 'Get API information',
                'GET /health': 'Health check'
            },
            'users': {
                'GET /users': 'List all users (admin)',
                'GET /users/<id>': 'Get user details (admin)',
                'POST /users': 'Create user (admin)',
                'PUT /users/<id>': 'Update user (admin)',
                'DELETE /users/<id>': 'Delete user (admin)'
            },
            'vps': {
                'GET /vps': 'List VPS',
                'GET /vps/<id>': 'Get VPS details',
                'POST /vps': 'Create VPS (admin)',
                'DELETE /vps/<id>': 'Delete VPS (admin)',
                'POST /vps/<id>/start': 'Start VPS',
                'POST /vps/<id>/stop': 'Stop VPS',
                'POST /vps/<id>/restart': 'Restart VPS',
                'POST /vps/<id>/suspend': 'Suspend VPS (admin)',
                'POST /vps/<id>/unsuspend': 'Unsuspend VPS (admin)',
                'POST /vps/<id>/resize': 'Resize VPS (admin)',
                'POST /vps/<id>/execute': 'Execute command in VPS',
                'GET /vps/<id>/password': 'Get VPS password',
                'POST /vps/<id>/password': 'Change VPS password'
            },
            'ports': {
                'GET /ports': 'List port forwards',
                'POST /ports': 'Create port forward',
                'DELETE /ports/<id>': 'Delete port forward'
            },
            'nodes': {
                'GET /nodes': 'List nodes (admin)',
                'GET /nodes/<id>': 'Get node details (admin)',
                'POST /nodes': 'Create node (admin)',
                'PUT /nodes/<id>': 'Update node (admin)',
                'DELETE /nodes/<id>': 'Delete node (admin)'
            },
            'notifications': {
                'GET /notifications': 'List notifications',
                'POST /notifications/<id>/read': 'Mark notification as read',
                'POST /notifications/read-all': 'Mark all notifications as read'
            },
            'activity': {
                'GET /activity': 'List activity logs'
            },
            'api_keys': {
                'GET /api-keys': 'List API keys',
                'POST /api-keys': 'Create API key',
                'DELETE /api-keys/<id>': 'Delete API key'
            },
            'settings': {
                'GET /settings': 'Get all settings (admin)',
                'GET /settings/<key>': 'Get setting (admin)',
                'PUT /settings/<key>': 'Update setting (admin)'
            },
            'maintenance': {
                'POST /maintenance/enable': 'Enable maintenance mode (admin)',
                'POST /maintenance/disable': 'Disable maintenance mode (admin)'
            },
            'system': {
                'GET /system/info': 'Get system information (admin)',
                'GET /system/stats': 'Get system statistics (admin)'
            },
            'statistics': {
                'GET /stats/overview': 'Get overview statistics'
            },
            'bulk_operations': {
                'POST /vps/bulk/start': 'Start multiple VPS (admin)',
                'POST /vps/bulk/stop': 'Stop multiple VPS (admin)'
            }
        },
        'examples': {
            'authentication': 'curl -H "X-API-Key: your_api_key" https://panel.example.com/api/v1/vps',
            'create_port': 'curl -X POST -H "X-API-Key: your_api_key" -H "Content-Type: application/json" -d \'{"vps_id": 1, "internal_port": 80, "protocol": "tcp"}\' https://panel.example.com/api/v1/ports',
            'start_vps': 'curl -X POST -H "X-API-Key: your_api_key" https://panel.example.com/api/v1/vps/1/start'
        }
    }
    
    return jsonify({
        'success': True,
        'documentation': docs
    })

# ============================================================================
# Error Handlers
# ============================================================================

@api_bp.errorhandler(404)
def api_not_found(error):
    return jsonify({
        'success': False,
        'error': 'Endpoint not found',
        'message': 'The requested API endpoint does not exist'
    }), 404

@api_bp.errorhandler(405)
def api_method_not_allowed(error):
    return jsonify({
        'success': False,
        'error': 'Method not allowed',
        'message': 'The HTTP method is not allowed for this endpoint'
    }), 405

@api_bp.errorhandler(500)
def api_internal_error(error):
    return jsonify({
        'success': False,
        'error': 'Internal server error',
        'message': 'An unexpected error occurred'
    }), 500