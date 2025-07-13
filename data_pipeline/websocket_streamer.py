"""
WebSocket Streaming Service

Low-latency WebSocket streaming service for real-time market data distribution.
Supports multiple subscription types, compression, and high-frequency updates.
"""

import asyncio
import json
import gzip
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Set, Optional, Any, Callable
from dataclasses import dataclass, asdict
from enum import Enum
import websockets
import jwt
from collections import defaultdict, deque
import uuid

logger = logging.getLogger(__name__)

class SubscriptionType(Enum):
    """Types of data subscriptions"""
    QUOTES = "quotes"
    TRADES = "trades"
    BARS = "bars"
    NEWS = "news"
    LEVEL2 = "level2"
    ALL = "all"

class CompressionType(Enum):
    """Compression options"""
    NONE = "none"
    GZIP = "gzip"

@dataclass
class ClientSession:
    """Client session information"""
    id: str
    websocket: Any
    subscriptions: Set[str]
    subscription_types: Set[SubscriptionType]
    compression: CompressionType
    connected_at: datetime
    last_activity: datetime
    authenticated: bool
    rate_limit_tokens: int
    max_rate_limit: int
    user_id: Optional[str] = None
    api_key: Optional[str] = None

@dataclass
class StreamMessage:
    """Standardized stream message"""
    type: str
    symbol: Optional[str]
    data: Dict[str, Any]
    timestamp: datetime
    sequence: int

class RateLimiter:
    """Token bucket rate limiter"""
    
    def __init__(self, max_tokens: int = 1000, refill_rate: int = 100):
        self.max_tokens = max_tokens
        self.refill_rate = refill_rate  # tokens per second
        self.tokens = max_tokens
        self.last_refill = time.time()
    
    def allow_request(self, tokens_needed: int = 1) -> bool:
        """Check if request is allowed under rate limit"""
        now = time.time()
        
        # Refill tokens based on time elapsed
        time_elapsed = now - self.last_refill
        tokens_to_add = int(time_elapsed * self.refill_rate)
        
        if tokens_to_add > 0:
            self.tokens = min(self.max_tokens, self.tokens + tokens_to_add)
            self.last_refill = now
        
        # Check if we have enough tokens
        if self.tokens >= tokens_needed:
            self.tokens -= tokens_needed
            return True
        
        return False

class MessageBuffer:
    """Message buffering for batch sending"""
    
    def __init__(self, max_size: int = 100, max_age_ms: int = 50):
        self.max_size = max_size
        self.max_age_ms = max_age_ms
        self.buffer: List[StreamMessage] = []
        self.created_at = time.time()
    
    def add_message(self, message: StreamMessage) -> bool:
        """Add message to buffer, return True if buffer should be flushed"""
        self.buffer.append(message)
        
        # Check if buffer should be flushed
        now = time.time()
        age_ms = (now - self.created_at) * 1000
        
        return len(self.buffer) >= self.max_size or age_ms >= self.max_age_ms
    
    def get_messages(self) -> List[StreamMessage]:
        """Get all messages and clear buffer"""
        messages = self.buffer.copy()
        self.buffer.clear()
        self.created_at = time.time()
        return messages
    
    def is_empty(self) -> bool:
        """Check if buffer is empty"""
        return len(self.buffer) == 0

class WebSocketStreamer:
    """
    High-performance WebSocket streaming service
    
    Features:
    - Sub-second latency streaming
    - Multiple subscription types
    - Rate limiting and authentication
    - Message compression
    - Connection health monitoring
    - Automatic reconnection support
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        
        # Server configuration
        self.host = self.config.get('host', 'localhost')
        self.port = self.config.get('port', 8765)
        self.max_connections = self.config.get('max_connections', 1000)
        self.auth_required = self.config.get('auth_required', False)
        self.jwt_secret = self.config.get('jwt_secret', 'your-secret-key')
        
        # Server state
        self.server = None
        self.is_running = False
        self.clients: Dict[str, ClientSession] = {}
        self.symbol_subscribers: Dict[str, Set[str]] = defaultdict(set)
        self.type_subscribers: Dict[SubscriptionType, Set[str]] = defaultdict(set)
        
        # Message handling
        self.message_sequence = 0
        self.client_buffers: Dict[str, MessageBuffer] = {}
        self.rate_limiters: Dict[str, RateLimiter] = {}
        
        # Performance tracking
        self.stats = {
            'connections_total': 0,
            'connections_active': 0,
            'messages_sent': 0,
            'messages_dropped': 0,
            'data_volume_mb': 0.0,
            'average_latency_ms': 0.0,
            'rate_limit_violations': 0,
            'auth_failures': 0,
            'start_time': None
        }
        
        # Background tasks
        self.cleanup_task = None
        self.health_monitor_task = None
        self.buffer_flush_task = None
        
        logger.info(f"WebSocketStreamer initialized on {self.host}:{self.port}")
    
    async def start(self) -> bool:
        """Start the WebSocket streaming server"""
        try:
            # Start WebSocket server
            self.server = await websockets.serve(
                self.handle_client_connection,
                self.host,
                self.port,
                max_size=1024*1024,  # 1MB max message size
                ping_interval=20,
                ping_timeout=10,
                close_timeout=10
            )
            
            # Start background tasks
            self.cleanup_task = asyncio.create_task(self._cleanup_loop())
            self.health_monitor_task = asyncio.create_task(self._health_monitor_loop())
            self.buffer_flush_task = asyncio.create_task(self._buffer_flush_loop())
            
            self.is_running = True
            self.stats['start_time'] = datetime.utcnow()
            
            logger.info(f"WebSocket streaming server started on {self.host}:{self.port}")
            return True
            
        except Exception as e:
            logger.error(f"Error starting WebSocket server: {e}")
            return False
    
    async def stop(self) -> None:
        """Stop the WebSocket streaming server"""
        try:
            self.is_running = False
            
            # Cancel background tasks
            for task in [self.cleanup_task, self.health_monitor_task, self.buffer_flush_task]:
                if task and not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
            
            # Close all client connections
            if self.clients:
                await asyncio.gather(
                    *[self._close_client(client_id) for client_id in list(self.clients.keys())],
                    return_exceptions=True
                )
            
            # Stop server
            if self.server:
                self.server.close()
                await self.server.wait_closed()
            
            logger.info("WebSocket streaming server stopped")
            
        except Exception as e:
            logger.error(f"Error stopping WebSocket server: {e}")
    
    async def handle_client_connection(self, websocket, path):
        """Handle new client connection"""
        client_id = str(uuid.uuid4())
        client_session = None
        
        try:
            # Check connection limit
            if len(self.clients) >= self.max_connections:
                await websocket.close(code=1013, reason="Server overloaded")
                return
            
            # Create client session
            client_session = ClientSession(
                id=client_id,
                websocket=websocket,
                subscriptions=set(),
                subscription_types=set(),
                compression=CompressionType.NONE,
                connected_at=datetime.utcnow(),
                last_activity=datetime.utcnow(),
                authenticated=not self.auth_required,
                rate_limit_tokens=1000,
                max_rate_limit=1000
            )
            
            # Register client
            self.clients[client_id] = client_session
            self.client_buffers[client_id] = MessageBuffer()
            self.rate_limiters[client_id] = RateLimiter()
            
            self.stats['connections_total'] += 1
            self.stats['connections_active'] += 1
            
            logger.info(f"Client connected: {client_id} from {websocket.remote_address}")
            
            # Send welcome message
            await self._send_to_client(client_id, {
                'type': 'welcome',
                'client_id': client_id,
                'server_time': datetime.utcnow().isoformat(),
                'auth_required': self.auth_required,
                'supported_compressions': [c.value for c in CompressionType],
                'supported_subscriptions': [s.value for s in SubscriptionType]
            })
            
            # Handle client messages
            async for message in websocket:
                await self._handle_client_message(client_id, message)
                
        except websockets.exceptions.ConnectionClosed:
            logger.info(f"Client disconnected: {client_id}")
        except Exception as e:
            logger.error(f"Error handling client {client_id}: {e}")
        finally:
            # Cleanup client
            if client_session:
                await self._cleanup_client(client_id)
    
    async def _handle_client_message(self, client_id: str, message: str) -> None:
        """Handle incoming message from client"""
        try:
            client = self.clients.get(client_id)
            if not client:
                return
            
            # Update last activity
            client.last_activity = datetime.utcnow()
            
            # Check rate limit
            if not self.rate_limiters[client_id].allow_request():
                self.stats['rate_limit_violations'] += 1
                await self._send_error(client_id, "RATE_LIMIT_EXCEEDED", "Rate limit exceeded")
                return
            
            # Parse message
            try:
                data = json.loads(message)
            except json.JSONDecodeError:
                await self._send_error(client_id, "INVALID_JSON", "Invalid JSON format")
                return
            
            message_type = data.get('type')
            
            # Handle different message types
            if message_type == 'auth':
                await self._handle_auth(client_id, data)
            elif message_type == 'subscribe':
                await self._handle_subscribe(client_id, data)
            elif message_type == 'unsubscribe':
                await self._handle_unsubscribe(client_id, data)
            elif message_type == 'set_compression':
                await self._handle_set_compression(client_id, data)
            elif message_type == 'ping':
                await self._handle_ping(client_id, data)
            elif message_type == 'get_stats':
                await self._handle_get_stats(client_id, data)
            else:
                await self._send_error(client_id, "UNKNOWN_MESSAGE_TYPE", f"Unknown message type: {message_type}")
                
        except Exception as e:
            logger.error(f"Error handling message from {client_id}: {e}")
            await self._send_error(client_id, "INTERNAL_ERROR", "Internal server error")
    
    async def _handle_auth(self, client_id: str, data: Dict[str, Any]) -> None:
        """Handle authentication"""
        try:
            client = self.clients[client_id]
            
            if not self.auth_required:
                await self._send_to_client(client_id, {
                    'type': 'auth_result',
                    'success': True,
                    'message': 'Authentication not required'
                })
                return
            
            # Validate token or API key
            token = data.get('token')
            api_key = data.get('api_key')
            
            if token:
                # JWT token authentication
                try:
                    payload = jwt.decode(token, self.jwt_secret, algorithms=['HS256'])
                    client.authenticated = True
                    client.user_id = payload.get('user_id')
                    
                    await self._send_to_client(client_id, {
                        'type': 'auth_result',
                        'success': True,
                        'user_id': client.user_id
                    })
                    
                except jwt.InvalidTokenError:
                    self.stats['auth_failures'] += 1
                    await self._send_error(client_id, "AUTH_FAILED", "Invalid token")
                    
            elif api_key:
                # API key authentication (implement your validation logic)
                if self._validate_api_key(api_key):
                    client.authenticated = True
                    client.api_key = api_key
                    
                    await self._send_to_client(client_id, {
                        'type': 'auth_result',
                        'success': True,
                        'api_key': api_key[:8] + '...'  # Show only first 8 chars
                    })
                else:
                    self.stats['auth_failures'] += 1
                    await self._send_error(client_id, "AUTH_FAILED", "Invalid API key")
            else:
                await self._send_error(client_id, "AUTH_FAILED", "Token or API key required")
                
        except Exception as e:
            logger.error(f"Error in authentication: {e}")
            await self._send_error(client_id, "AUTH_ERROR", "Authentication error")
    
    def _validate_api_key(self, api_key: str) -> bool:
        """Validate API key (implement your validation logic)"""
        # Mock validation - replace with actual validation
        return len(api_key) >= 32
    
    async def _handle_subscribe(self, client_id: str, data: Dict[str, Any]) -> None:
        """Handle subscription request"""
        try:
            client = self.clients[client_id]
            
            if not client.authenticated and self.auth_required:
                await self._send_error(client_id, "AUTH_REQUIRED", "Authentication required")
                return
            
            symbols = data.get('symbols', [])
            subscription_types = data.get('types', ['quotes'])
            
            # Validate symbols
            if not isinstance(symbols, list) or not symbols:
                await self._send_error(client_id, "INVALID_SYMBOLS", "Invalid or empty symbols list")
                return
            
            # Validate subscription types
            valid_types = []
            for sub_type in subscription_types:
                try:
                    valid_types.append(SubscriptionType(sub_type))
                except ValueError:
                    await self._send_error(client_id, "INVALID_SUBSCRIPTION_TYPE", f"Invalid subscription type: {sub_type}")
                    return
            
            # Add subscriptions
            for symbol in symbols:
                symbol = symbol.upper()
                client.subscriptions.add(symbol)
                self.symbol_subscribers[symbol].add(client_id)
            
            for sub_type in valid_types:
                client.subscription_types.add(sub_type)
                self.type_subscribers[sub_type].add(client_id)
            
            await self._send_to_client(client_id, {
                'type': 'subscription_result',
                'success': True,
                'symbols': symbols,
                'types': subscription_types,
                'total_subscriptions': len(client.subscriptions)
            })
            
            logger.info(f"Client {client_id} subscribed to {len(symbols)} symbols with {len(valid_types)} types")
            
        except Exception as e:
            logger.error(f"Error handling subscription: {e}")
            await self._send_error(client_id, "SUBSCRIPTION_ERROR", "Subscription error")
    
    async def _handle_unsubscribe(self, client_id: str, data: Dict[str, Any]) -> None:
        """Handle unsubscription request"""
        try:
            client = self.clients[client_id]
            
            symbols = data.get('symbols', [])
            subscription_types = data.get('types', [])
            
            # Remove symbol subscriptions
            for symbol in symbols:
                symbol = symbol.upper()
                client.subscriptions.discard(symbol)
                self.symbol_subscribers[symbol].discard(client_id)
                
                # Clean up empty symbol subscribers
                if not self.symbol_subscribers[symbol]:
                    del self.symbol_subscribers[symbol]
            
            # Remove type subscriptions
            for sub_type in subscription_types:
                try:
                    subscription_type = SubscriptionType(sub_type)
                    client.subscription_types.discard(subscription_type)
                    self.type_subscribers[subscription_type].discard(client_id)
                    
                    # Clean up empty type subscribers
                    if not self.type_subscribers[subscription_type]:
                        del self.type_subscribers[subscription_type]
                        
                except ValueError:
                    continue
            
            await self._send_to_client(client_id, {
                'type': 'unsubscription_result',
                'success': True,
                'symbols': symbols,
                'types': subscription_types,
                'remaining_subscriptions': len(client.subscriptions)
            })
            
        except Exception as e:
            logger.error(f"Error handling unsubscription: {e}")
            await self._send_error(client_id, "UNSUBSCRIPTION_ERROR", "Unsubscription error")
    
    async def _handle_set_compression(self, client_id: str, data: Dict[str, Any]) -> None:
        """Handle compression setting"""
        try:
            client = self.clients[client_id]
            compression = data.get('compression', 'none')
            
            try:
                client.compression = CompressionType(compression)
                
                await self._send_to_client(client_id, {
                    'type': 'compression_result',
                    'success': True,
                    'compression': compression
                })
                
            except ValueError:
                await self._send_error(client_id, "INVALID_COMPRESSION", f"Invalid compression type: {compression}")
                
        except Exception as e:
            logger.error(f"Error setting compression: {e}")
            await self._send_error(client_id, "COMPRESSION_ERROR", "Compression setting error")
    
    async def _handle_ping(self, client_id: str, data: Dict[str, Any]) -> None:
        """Handle ping request"""
        await self._send_to_client(client_id, {
            'type': 'pong',
            'timestamp': datetime.utcnow().isoformat(),
            'client_timestamp': data.get('timestamp')
        })
    
    async def _handle_get_stats(self, client_id: str, data: Dict[str, Any]) -> None:
        """Handle stats request"""
        stats = await self.get_stats()
        await self._send_to_client(client_id, {
            'type': 'stats',
            'data': stats
        })
    
    async def _send_to_client(self, client_id: str, message: Dict[str, Any]) -> bool:
        """Send message to specific client"""
        try:
            client = self.clients.get(client_id)
            if not client:
                return False
            
            # Serialize message
            message_json = json.dumps(message, default=str)
            
            # Apply compression if enabled
            if client.compression == CompressionType.GZIP:
                message_data = gzip.compress(message_json.encode('utf-8'))
            else:
                message_data = message_json
            
            # Send message
            await client.websocket.send(message_data)
            
            # Update stats
            self.stats['messages_sent'] += 1
            self.stats['data_volume_mb'] += len(message_data) / (1024 * 1024)
            
            return True
            
        except websockets.exceptions.ConnectionClosed:
            await self._cleanup_client(client_id)
            return False
        except Exception as e:
            logger.error(f"Error sending message to {client_id}: {e}")
            return False
    
    async def _send_error(self, client_id: str, error_code: str, error_message: str) -> None:
        """Send error message to client"""
        await self._send_to_client(client_id, {
            'type': 'error',
            'error_code': error_code,
            'error_message': error_message,
            'timestamp': datetime.utcnow().isoformat()
        })
    
    async def broadcast_market_data(self, symbol: str, data: Dict[str, Any], data_type: SubscriptionType = SubscriptionType.QUOTES) -> int:
        """Broadcast market data to subscribed clients"""
        try:
            # Get subscribers for this symbol and data type
            symbol_subs = self.symbol_subscribers.get(symbol, set())
            type_subs = self.type_subscribers.get(data_type, set())
            all_subs = self.type_subscribers.get(SubscriptionType.ALL, set())
            
            # Find clients subscribed to both symbol and data type
            target_clients = symbol_subs.intersection(type_subs).union(
                symbol_subs.intersection(all_subs)
            )
            
            if not target_clients:
                return 0
            
            # Create stream message
            self.message_sequence += 1
            stream_message = StreamMessage(
                type='market_data',
                symbol=symbol,
                data={
                    'data_type': data_type.value,
                    'symbol': symbol,
                    **data
                },
                timestamp=datetime.utcnow(),
                sequence=self.message_sequence
            )
            
            # Add to client buffers or send immediately
            sent_count = 0
            for client_id in target_clients:
                try:
                    client = self.clients.get(client_id)
                    if not client:
                        continue
                    
                    # Add to buffer
                    buffer = self.client_buffers.get(client_id)
                    if buffer:
                        should_flush = buffer.add_message(stream_message)
                        if should_flush:
                            await self._flush_client_buffer(client_id)
                        sent_count += 1
                        
                except Exception as e:
                    logger.warning(f"Error sending to client {client_id}: {e}")
            
            return sent_count
            
        except Exception as e:
            logger.error(f"Error broadcasting market data: {e}")
            return 0
    
    async def _flush_client_buffer(self, client_id: str) -> None:
        """Flush client message buffer"""
        try:
            buffer = self.client_buffers.get(client_id)
            if not buffer or buffer.is_empty():
                return
            
            messages = buffer.get_messages()
            if not messages:
                return
            
            # Create batch message
            batch_message = {
                'type': 'batch',
                'count': len(messages),
                'messages': [asdict(msg) for msg in messages],
                'timestamp': datetime.utcnow().isoformat()
            }
            
            await self._send_to_client(client_id, batch_message)
            
        except Exception as e:
            logger.error(f"Error flushing buffer for {client_id}: {e}")
    
    async def _buffer_flush_loop(self) -> None:
        """Periodically flush client buffers"""
        while self.is_running:
            try:
                # Flush buffers for all clients
                for client_id in list(self.clients.keys()):
                    await self._flush_client_buffer(client_id)
                
                # Sleep for 50ms
                await asyncio.sleep(0.05)
                
            except Exception as e:
                logger.error(f"Error in buffer flush loop: {e}")
                await asyncio.sleep(1)
    
    async def _cleanup_loop(self) -> None:
        """Periodically cleanup disconnected clients"""
        while self.is_running:
            try:
                current_time = datetime.utcnow()
                timeout_threshold = current_time - timedelta(minutes=5)
                
                disconnected_clients = []
                
                for client_id, client in self.clients.items():
                    # Check if client is inactive
                    if client.last_activity < timeout_threshold:
                        disconnected_clients.append(client_id)
                
                # Cleanup disconnected clients
                for client_id in disconnected_clients:
                    await self._cleanup_client(client_id)
                    logger.info(f"Cleaned up inactive client: {client_id}")
                
                # Sleep for 1 minute
                await asyncio.sleep(60)
                
            except Exception as e:
                logger.error(f"Error in cleanup loop: {e}")
                await asyncio.sleep(60)
    
    async def _health_monitor_loop(self) -> None:
        """Monitor connection health"""
        while self.is_running:
            try:
                # Update connection count
                self.stats['connections_active'] = len(self.clients)
                
                # Calculate average latency (simplified)
                # In a real implementation, this would track actual latency
                self.stats['average_latency_ms'] = 5.0  # Mock value
                
                # Sleep for 10 seconds
                await asyncio.sleep(10)
                
            except Exception as e:
                logger.error(f"Error in health monitor: {e}")
                await asyncio.sleep(10)
    
    async def _cleanup_client(self, client_id: str) -> None:
        """Cleanup client resources"""
        try:
            client = self.clients.get(client_id)
            if not client:
                return
            
            # Remove from symbol subscriptions
            for symbol in client.subscriptions:
                self.symbol_subscribers[symbol].discard(client_id)
                if not self.symbol_subscribers[symbol]:
                    del self.symbol_subscribers[symbol]
            
            # Remove from type subscriptions
            for sub_type in client.subscription_types:
                self.type_subscribers[sub_type].discard(client_id)
                if not self.type_subscribers[sub_type]:
                    del self.type_subscribers[sub_type]
            
            # Remove client resources
            self.clients.pop(client_id, None)
            self.client_buffers.pop(client_id, None)
            self.rate_limiters.pop(client_id, None)
            
            self.stats['connections_active'] = len(self.clients)
            
        except Exception as e:
            logger.error(f"Error cleaning up client {client_id}: {e}")
    
    async def _close_client(self, client_id: str) -> None:
        """Close client connection"""
        try:
            client = self.clients.get(client_id)
            if client and not client.websocket.closed:
                await client.websocket.close()
        except Exception as e:
            logger.error(f"Error closing client {client_id}: {e}")
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get streaming service statistics"""
        stats = self.stats.copy()
        
        # Add real-time stats
        stats.update({
            'connections_active': len(self.clients),
            'symbols_subscribed': len(self.symbol_subscribers),
            'total_subscriptions': sum(len(client.subscriptions) for client in self.clients.values()),
            'uptime_seconds': (datetime.utcnow() - self.stats['start_time']).total_seconds() if self.stats['start_time'] else 0
        })
        
        return stats
    
    def get_connected_clients(self) -> List[Dict[str, Any]]:
        """Get information about connected clients"""
        clients_info = []
        
        for client_id, client in self.clients.items():
            clients_info.append({
                'id': client_id,
                'connected_at': client.connected_at.isoformat(),
                'last_activity': client.last_activity.isoformat(),
                'authenticated': client.authenticated,
                'subscriptions_count': len(client.subscriptions),
                'subscription_types': [st.value for st in client.subscription_types],
                'compression': client.compression.value,
                'user_id': client.user_id,
                'remote_address': str(client.websocket.remote_address) if hasattr(client.websocket, 'remote_address') else 'unknown'
            })
        
        return clients_info