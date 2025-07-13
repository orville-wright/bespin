#!/usr/bin/env python3
"""
Bespin Trading Platform - Main Entry Point

Starts the FastAPI trading platform server with integrated strategy execution,
order management, and real-time market data processing.

Usage:
    python -m trading_platform.main [--host HOST] [--port PORT] [--simulation]
    
Example:
    python -m trading_platform.main --host 0.0.0.0 --port 8000 --simulation
"""

import argparse
import asyncio
import logging
import sys
import os
from pathlib import Path

import uvicorn
from fastapi import FastAPI

# Add the parent directory to the path for imports
sys.path.append(str(Path(__file__).parent.parent))

from trading_platform.api.app import create_app

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('trading_platform.log')
    ]
)

logger = logging.getLogger(__name__)


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Bespin Trading Platform - Quantitative Trading with Strategy Execution"
    )
    
    parser.add_argument(
        '--host',
        default='127.0.0.1',
        help='Host to bind the server to (default: 127.0.0.1)'
    )
    
    parser.add_argument(
        '--port',
        type=int,
        default=8000,
        help='Port to bind the server to (default: 8000)'
    )
    
    parser.add_argument(
        '--simulation',
        action='store_true',
        help='Run in simulation mode (default: True)'
    )
    
    parser.add_argument(
        '--reload',
        action='store_true',
        help='Enable auto-reload for development'
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Set logging level (default: INFO)'
    )
    
    parser.add_argument(
        '--config',
        help='Path to configuration file'
    )
    
    return parser.parse_args()


def setup_logging(log_level: str):
    """Setup logging configuration"""
    logging.getLogger().setLevel(getattr(logging, log_level))
    
    # Set specific loggers
    logging.getLogger('uvicorn').setLevel(logging.INFO)
    logging.getLogger('trading_platform').setLevel(getattr(logging, log_level))
    
    logger.info(f"Logging level set to {log_level}")


def validate_environment():
    """Validate required environment variables and dependencies"""
    required_dirs = [
        'trading_platform/core',
        'trading_platform/api',
        'trading_platform/models'
    ]
    
    for dir_path in required_dirs:
        if not os.path.exists(dir_path):
            logger.error(f"Required directory not found: {dir_path}")
            return False
    
    # Check for optional environment variables
    optional_env_vars = [
        'ALPACA_API_KEY',
        'ALPACA_SEC_KEY',
        'DATABASE_URL',
        'REDIS_URL'
    ]
    
    missing_vars = []
    for var in optional_env_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        logger.warning(f"Optional environment variables not set: {missing_vars}")
        logger.warning("Trading platform will run in simulation mode with limited functionality")
    
    return True


def print_banner():
    """Print startup banner"""
    banner = """
    ╔══════════════════════════════════════════════════════════════╗
    ║                   BESPIN TRADING PLATFORM                   ║
    ║                                                              ║
    ║  🚀 Quantitative Trading with Strategy Execution            ║
    ║  📊 Real-time Market Data Processing                        ║
    ║  ⚡ FastAPI + AsyncIO Architecture                          ║
    ║  💾 PostgreSQL + TimescaleDB + Redis                       ║
    ║  🔗 Integrated with Alpaca Broker                          ║
    ║                                                              ║
    ║  Built for the Bespin Financial Data Platform              ║
    ╚══════════════════════════════════════════════════════════════╝
    """
    print(banner)


def main():
    """Main entry point"""
    args = parse_arguments()
    
    print_banner()
    
    # Setup logging
    setup_logging(args.log_level)
    
    # Validate environment
    if not validate_environment():
        logger.error("Environment validation failed")
        sys.exit(1)
    
    # Create FastAPI app
    try:
        app = create_app()
        logger.info("FastAPI application created successfully")
    except Exception as e:
        logger.error(f"Failed to create FastAPI application: {e}")
        sys.exit(1)
    
    # Server configuration
    server_config = {
        'app': app,
        'host': args.host,
        'port': args.port,
        'reload': args.reload,
        'log_level': args.log_level.lower(),
        'access_log': True,
        'server_header': False,
        'date_header': False
    }
    
    # Log startup info
    logger.info("="*60)
    logger.info("BESPIN TRADING PLATFORM STARTUP")
    logger.info("="*60)
    logger.info(f"Server: http://{args.host}:{args.port}")
    logger.info(f"API Documentation: http://{args.host}:{args.port}/docs")
    logger.info(f"Health Check: http://{args.host}:{args.port}/health")
    logger.info(f"Simulation Mode: {args.simulation}")
    logger.info(f"Log Level: {args.log_level}")
    
    if args.reload:
        logger.info("Auto-reload enabled for development")
    
    logger.info("="*60)
    logger.info("AVAILABLE ENDPOINTS:")
    logger.info("  • /api/v1/strategies    - Strategy management")
    logger.info("  • /api/v1/orders        - Order operations")
    logger.info("  • /api/v1/positions     - Position tracking")
    logger.info("  • /api/v1/market-data   - Market data access")
    logger.info("  • /docs                 - Interactive API docs")
    logger.info("="*60)
    
    # Start server
    try:
        uvicorn.run(**server_config)
    except KeyboardInterrupt:
        logger.info("Shutdown requested by user")
    except Exception as e:
        logger.error(f"Server error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()