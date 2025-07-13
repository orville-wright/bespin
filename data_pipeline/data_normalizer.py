"""
Data Normalization and Cleaning Pipeline

Normalizes data from 25+ sources into standardized format.
Handles data cleaning, validation, deduplication, and enrichment.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Set, Tuple, Union
from dataclasses import dataclass, asdict
from decimal import Decimal, ROUND_HALF_UP
from enum import Enum
import pandas as pd
import numpy as np
import hashlib
import json
from collections import defaultdict, deque
import re

logger = logging.getLogger(__name__)

class DataQuality(Enum):
    """Data quality levels"""
    EXCELLENT = "excellent"    # < 1% error rate, < 100ms latency
    GOOD = "good"             # < 5% error rate, < 500ms latency  
    FAIR = "fair"             # < 10% error rate, < 1s latency
    POOR = "poor"             # > 10% error rate, > 1s latency

class NormalizationRule(Enum):
    """Data normalization rules"""
    PRICE_PRECISION = "price_precision"       # Standardize price precision
    VOLUME_SCALING = "volume_scaling"         # Normalize volume units
    TIMESTAMP_UTC = "timestamp_utc"           # Convert to UTC
    SYMBOL_FORMAT = "symbol_format"           # Standardize symbol format
    CURRENCY_CONVERSION = "currency_conversion" # Convert to base currency
    OUTLIER_DETECTION = "outlier_detection"   # Detect and flag outliers

@dataclass
class ValidationRule:
    """Data validation rule"""
    name: str
    field: str
    rule_type: str  # 'range', 'regex', 'custom'
    parameters: Dict[str, Any]
    severity: str   # 'error', 'warning', 'info'

@dataclass
class NormalizationConfig:
    """Configuration for data normalization"""
    price_decimal_places: int = 4
    volume_decimal_places: int = 0
    base_currency: str = "USD"
    symbol_format: str = "upper"  # 'upper', 'lower', 'as_is'
    timezone: str = "UTC"
    outlier_threshold_std: float = 3.0
    duplicate_window_seconds: int = 5
    validation_rules: List[ValidationRule] = None

@dataclass
class DataIssue:
    """Represents a data quality issue"""
    source: str
    symbol: str
    field: str
    issue_type: str
    severity: str
    description: str
    original_value: Any
    corrected_value: Any
    timestamp: datetime

class SourceProfiler:
    """Profiles data sources to understand their characteristics"""
    
    def __init__(self):
        self.source_profiles: Dict[str, Dict[str, Any]] = {}
        self.field_statistics: Dict[str, Dict[str, Any]] = defaultdict(dict)
        
    def profile_source(self, source: str, data_points: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Profile a data source based on sample data"""
        if not data_points:
            return {}
        
        profile = {
            'sample_count': len(data_points),
            'fields': {},
            'data_patterns': {},
            'quality_indicators': {}
        }
        
        # Analyze each field
        for field_name in data_points[0].keys():
            field_values = [dp.get(field_name) for dp in data_points if dp.get(field_name) is not None]
            
            if not field_values:
                continue
                
            field_profile = self._profile_field(field_name, field_values)
            profile['fields'][field_name] = field_profile
        
        # Store profile
        self.source_profiles[source] = profile
        
        return profile
    
    def _profile_field(self, field_name: str, values: List[Any]) -> Dict[str, Any]:
        """Profile a specific field"""
        profile = {
            'count': len(values),
            'null_count': sum(1 for v in values if v is None),
            'data_types': {},
            'patterns': []
        }
        
        # Analyze data types
        type_counts = defaultdict(int)
        for value in values:
            type_counts[type(value).__name__] += 1
        
        profile['data_types'] = dict(type_counts)
        
        # Field-specific analysis
        if field_name in ['price', 'bid', 'ask', 'open', 'high', 'low', 'close']:
            profile.update(self._profile_price_field(values))
        elif field_name in ['volume', 'bid_size', 'ask_size']:
            profile.update(self._profile_volume_field(values))
        elif field_name == 'symbol':
            profile.update(self._profile_symbol_field(values))
        elif field_name == 'timestamp':
            profile.update(self._profile_timestamp_field(values))
        
        return profile
    
    def _profile_price_field(self, values: List[Any]) -> Dict[str, Any]:
        """Profile price-related fields"""
        numeric_values = []
        for v in values:
            try:
                if isinstance(v, (int, float)):
                    numeric_values.append(float(v))
                elif isinstance(v, str):
                    # Try to parse string as number
                    numeric_values.append(float(v.replace(',', '').replace('$', '')))
            except (ValueError, TypeError):
                continue
        
        if not numeric_values:
            return {}
        
        return {
            'min_value': min(numeric_values),
            'max_value': max(numeric_values),
            'mean_value': sum(numeric_values) / len(numeric_values),
            'decimal_places': max(self._count_decimal_places(v) for v in numeric_values),
            'zero_count': sum(1 for v in numeric_values if v == 0),
            'negative_count': sum(1 for v in numeric_values if v < 0)
        }
    
    def _profile_volume_field(self, values: List[Any]) -> Dict[str, Any]:
        """Profile volume-related fields"""
        numeric_values = []
        for v in values:
            try:
                if isinstance(v, (int, float)):
                    numeric_values.append(int(v))
                elif isinstance(v, str):
                    numeric_values.append(int(v.replace(',', '')))
            except (ValueError, TypeError):
                continue
        
        if not numeric_values:
            return {}
        
        return {
            'min_volume': min(numeric_values),
            'max_volume': max(numeric_values),
            'mean_volume': sum(numeric_values) / len(numeric_values),
            'zero_volume_count': sum(1 for v in numeric_values if v == 0)
        }
    
    def _profile_symbol_field(self, values: List[Any]) -> Dict[str, Any]:
        """Profile symbol fields"""
        str_values = [str(v) for v in values if v is not None]
        
        if not str_values:
            return {}
        
        patterns = {
            'lengths': defaultdict(int),
            'case_patterns': defaultdict(int),
            'special_chars': defaultdict(int)
        }
        
        for symbol in str_values:
            patterns['lengths'][len(symbol)] += 1
            
            if symbol.isupper():
                patterns['case_patterns']['upper'] += 1
            elif symbol.islower():
                patterns['case_patterns']['lower'] += 1
            else:
                patterns['case_patterns']['mixed'] += 1
            
            # Count special characters
            special_chars = re.findall(r'[^A-Za-z0-9]', symbol)
            for char in special_chars:
                patterns['special_chars'][char] += 1
        
        return {
            'unique_count': len(set(str_values)),
            'patterns': {k: dict(v) for k, v in patterns.items()}
        }
    
    def _profile_timestamp_field(self, values: List[Any]) -> Dict[str, Any]:
        """Profile timestamp fields"""
        timestamp_values = []
        formats = defaultdict(int)
        
        for v in values:
            if isinstance(v, datetime):
                timestamp_values.append(v)
                formats['datetime_object'] += 1
            elif isinstance(v, str):
                # Try to identify timestamp format
                if re.match(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', v):
                    formats['iso_format'] += 1
                elif re.match(r'\d{10}', v):
                    formats['unix_timestamp'] += 1
                else:
                    formats['other_string'] += 1
            elif isinstance(v, (int, float)):
                formats['numeric'] += 1
        
        profile = {
            'format_distribution': dict(formats)
        }
        
        if timestamp_values:
            profile.update({
                'earliest': min(timestamp_values),
                'latest': max(timestamp_values),
                'timezone_aware': any(ts.tzinfo is not None for ts in timestamp_values)
            })
        
        return profile
    
    def _count_decimal_places(self, value: float) -> int:
        """Count decimal places in a float"""
        if value == int(value):
            return 0
        
        decimal_str = str(value)
        if '.' in decimal_str:
            return len(decimal_str.split('.')[1])
        return 0
    
    def get_source_profile(self, source: str) -> Optional[Dict[str, Any]]:
        """Get profile for a specific source"""
        return self.source_profiles.get(source)
    
    def get_recommended_normalization(self, source: str) -> Dict[str, Any]:
        """Get recommended normalization settings for a source"""
        profile = self.get_source_profile(source)
        if not profile:
            return {}
        
        recommendations = {}
        
        # Price precision recommendation
        price_fields = ['price', 'bid', 'ask', 'open', 'high', 'low', 'close']
        max_decimal_places = 0
        
        for field in price_fields:
            field_profile = profile['fields'].get(field, {})
            decimal_places = field_profile.get('decimal_places', 0)
            max_decimal_places = max(max_decimal_places, decimal_places)
        
        recommendations['price_decimal_places'] = min(max_decimal_places, 6)
        
        # Symbol format recommendation
        symbol_profile = profile['fields'].get('symbol', {})
        case_patterns = symbol_profile.get('patterns', {}).get('case_patterns', {})
        
        if case_patterns.get('upper', 0) > case_patterns.get('lower', 0):
            recommendations['symbol_format'] = 'upper'
        else:
            recommendations['symbol_format'] = 'lower'
        
        return recommendations

class DataNormalizer:
    """
    High-performance data normalization and cleaning pipeline
    
    Features:
    - Multi-source data normalization
    - Real-time data cleaning
    - Quality scoring and monitoring
    - Outlier detection
    - Duplicate detection and handling
    - Data enrichment
    """
    
    def __init__(self, config: NormalizationConfig = None):
        self.config = config or NormalizationConfig()
        
        # Initialize components
        self.profiler = SourceProfiler()
        self.validation_cache: Dict[str, bool] = {}
        self.duplicate_detector = DuplicateDetector(
            window_seconds=self.config.duplicate_window_seconds
        )
        
        # Statistics tracking
        self.stats = {
            'total_processed': 0,
            'normalization_applied': defaultdict(int),
            'validation_failures': defaultdict(int),
            'duplicates_detected': 0,
            'outliers_detected': 0,
            'data_issues': []
        }
        
        # Setup validation rules
        self.validation_rules = self._setup_validation_rules()
        
        logger.info("DataNormalizer initialized")
    
    def _setup_validation_rules(self) -> List[ValidationRule]:
        """Setup default validation rules"""
        rules = [
            ValidationRule(
                name="price_positive",
                field="price",
                rule_type="range",
                parameters={"min": 0.0001, "max": 1000000},
                severity="error"
            ),
            ValidationRule(
                name="volume_non_negative",
                field="volume",
                rule_type="range",
                parameters={"min": 0, "max": 1000000000},
                severity="error"
            ),
            ValidationRule(
                name="bid_ask_spread",
                field="spread",
                rule_type="custom",
                parameters={"max_spread_percent": 0.1},
                severity="warning"
            ),
            ValidationRule(
                name="symbol_format",
                field="symbol",
                rule_type="regex",
                parameters={"pattern": r"^[A-Z]{1,10}$"},
                severity="warning"
            ),
            ValidationRule(
                name="timestamp_recent",
                field="timestamp",
                rule_type="custom",
                parameters={"max_age_hours": 24},
                severity="warning"
            )
        ]
        
        # Add custom rules from config
        if self.config.validation_rules:
            rules.extend(self.config.validation_rules)
        
        return rules
    
    async def normalize_data_point(self, data_point: Dict[str, Any], source: str) -> Tuple[Dict[str, Any], List[DataIssue]]:
        """Normalize a single data point"""
        try:
            normalized_data = data_point.copy()
            issues = []
            
            # Apply normalization rules
            normalized_data, norm_issues = await self._apply_normalization_rules(normalized_data, source)
            issues.extend(norm_issues)
            
            # Validate data
            validation_issues = await self._validate_data_point(normalized_data, source)
            issues.extend(validation_issues)
            
            # Check for duplicates
            is_duplicate = self.duplicate_detector.is_duplicate(normalized_data)
            if is_duplicate:
                issues.append(DataIssue(
                    source=source,
                    symbol=normalized_data.get('symbol', 'unknown'),
                    field='duplicate',
                    issue_type='duplicate',
                    severity='warning',
                    description='Duplicate data point detected',
                    original_value=None,
                    corrected_value=None,
                    timestamp=datetime.utcnow()
                ))
                self.stats['duplicates_detected'] += 1
            
            # Detect outliers
            outlier_issues = await self._detect_outliers(normalized_data, source)
            issues.extend(outlier_issues)
            
            # Update statistics
            self.stats['total_processed'] += 1
            
            return normalized_data, issues
            
        except Exception as e:
            logger.error(f"Error normalizing data point: {e}")
            return data_point, [DataIssue(
                source=source,
                symbol=data_point.get('symbol', 'unknown'),
                field='processing',
                issue_type='error',
                severity='error',
                description=f'Normalization error: {str(e)}',
                original_value=None,
                corrected_value=None,
                timestamp=datetime.utcnow()
            )]
    
    async def _apply_normalization_rules(self, data_point: Dict[str, Any], source: str) -> Tuple[Dict[str, Any], List[DataIssue]]:
        """Apply normalization rules to data point"""
        normalized_data = data_point.copy()
        issues = []
        
        try:
            # Normalize symbol format
            if 'symbol' in normalized_data:
                original_symbol = normalized_data['symbol']
                if self.config.symbol_format == 'upper':
                    normalized_data['symbol'] = str(original_symbol).upper()
                elif self.config.symbol_format == 'lower':
                    normalized_data['symbol'] = str(original_symbol).lower()
                
                if normalized_data['symbol'] != original_symbol:
                    self.stats['normalization_applied']['symbol_format'] += 1
            
            # Normalize price fields
            price_fields = ['price', 'bid', 'ask', 'open', 'high', 'low', 'close']
            for field in price_fields:
                if field in normalized_data:
                    original_value = normalized_data[field]
                    normalized_value = self._normalize_price(original_value)
                    
                    if normalized_value != original_value:
                        normalized_data[field] = normalized_value
                        self.stats['normalization_applied']['price_precision'] += 1
            
            # Normalize volume fields
            volume_fields = ['volume', 'bid_size', 'ask_size']
            for field in volume_fields:
                if field in normalized_data:
                    original_value = normalized_data[field]
                    normalized_value = self._normalize_volume(original_value)
                    
                    if normalized_value != original_value:
                        normalized_data[field] = normalized_value
                        self.stats['normalization_applied']['volume_scaling'] += 1
            
            # Normalize timestamp
            if 'timestamp' in normalized_data:
                original_timestamp = normalized_data['timestamp']
                normalized_timestamp = self._normalize_timestamp(original_timestamp)
                
                if normalized_timestamp != original_timestamp:
                    normalized_data['timestamp'] = normalized_timestamp
                    self.stats['normalization_applied']['timestamp_utc'] += 1
            
            # Calculate derived fields
            if 'bid' in normalized_data and 'ask' in normalized_data:
                bid = float(normalized_data['bid'])
                ask = float(normalized_data['ask'])
                
                if bid > 0 and ask > 0:
                    normalized_data['spread'] = ask - bid
                    normalized_data['mid_price'] = (bid + ask) / 2
                    normalized_data['spread_percent'] = (ask - bid) / ((bid + ask) / 2)
            
        except Exception as e:
            logger.error(f"Error applying normalization rules: {e}")
            issues.append(DataIssue(
                source=source,
                symbol=data_point.get('symbol', 'unknown'),
                field='normalization',
                issue_type='error',
                severity='error',
                description=f'Normalization rule error: {str(e)}',
                original_value=None,
                corrected_value=None,
                timestamp=datetime.utcnow()
            ))
        
        return normalized_data, issues
    
    def _normalize_price(self, value: Any) -> Optional[float]:
        """Normalize price value"""
        if value is None:
            return None
        
        try:
            # Convert to float
            if isinstance(value, str):
                # Remove common currency symbols and commas
                cleaned_value = value.replace('$', '').replace(',', '').strip()
                price = float(cleaned_value)
            else:
                price = float(value)
            
            # Round to configured decimal places
            decimal_places = self.config.price_decimal_places
            multiplier = 10 ** decimal_places
            return round(price * multiplier) / multiplier
            
        except (ValueError, TypeError):
            return None
    
    def _normalize_volume(self, value: Any) -> Optional[int]:
        """Normalize volume value"""
        if value is None:
            return None
        
        try:
            # Convert to int
            if isinstance(value, str):
                # Remove commas
                cleaned_value = value.replace(',', '').strip()
                volume = int(float(cleaned_value))
            else:
                volume = int(float(value))
            
            return max(0, volume)  # Ensure non-negative
            
        except (ValueError, TypeError):
            return None
    
    def _normalize_timestamp(self, value: Any) -> Optional[datetime]:
        """Normalize timestamp value"""
        if value is None:
            return None
        
        try:
            if isinstance(value, datetime):
                # Convert to UTC if timezone-aware
                if value.tzinfo is not None:
                    return value.utctimetuple()
                return value
            
            elif isinstance(value, str):
                # Parse ISO format string
                if 'T' in value:
                    return datetime.fromisoformat(value.replace('Z', '+00:00'))
                else:
                    return datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
            
            elif isinstance(value, (int, float)):
                # Unix timestamp
                return datetime.utcfromtimestamp(float(value))
            
        except (ValueError, TypeError):
            pass
        
        return None
    
    async def _validate_data_point(self, data_point: Dict[str, Any], source: str) -> List[DataIssue]:
        """Validate data point against rules"""
        issues = []
        
        for rule in self.validation_rules:
            try:
                field_value = data_point.get(rule.field)
                
                if field_value is None:
                    continue
                
                is_valid = True
                error_message = ""
                
                if rule.rule_type == "range":
                    min_val = rule.parameters.get("min")
                    max_val = rule.parameters.get("max")
                    
                    numeric_value = float(field_value)
                    
                    if min_val is not None and numeric_value < min_val:
                        is_valid = False
                        error_message = f"Value {numeric_value} below minimum {min_val}"
                    elif max_val is not None and numeric_value > max_val:
                        is_valid = False
                        error_message = f"Value {numeric_value} above maximum {max_val}"
                
                elif rule.rule_type == "regex":
                    pattern = rule.parameters.get("pattern")
                    if pattern and not re.match(pattern, str(field_value)):
                        is_valid = False
                        error_message = f"Value '{field_value}' doesn't match pattern {pattern}"
                
                elif rule.rule_type == "custom":
                    is_valid, error_message = await self._apply_custom_validation(
                        rule, data_point, source
                    )
                
                if not is_valid:
                    issues.append(DataIssue(
                        source=source,
                        symbol=data_point.get('symbol', 'unknown'),
                        field=rule.field,
                        issue_type='validation',
                        severity=rule.severity,
                        description=f"{rule.name}: {error_message}",
                        original_value=field_value,
                        corrected_value=None,
                        timestamp=datetime.utcnow()
                    ))
                    
                    self.stats['validation_failures'][rule.name] += 1
                
            except Exception as e:
                logger.error(f"Error validating rule {rule.name}: {e}")
        
        return issues
    
    async def _apply_custom_validation(self, rule: ValidationRule, data_point: Dict[str, Any], source: str) -> Tuple[bool, str]:
        """Apply custom validation rule"""
        try:
            if rule.name == "bid_ask_spread":
                bid = data_point.get('bid')
                ask = data_point.get('ask')
                
                if bid and ask:
                    bid = float(bid)
                    ask = float(ask)
                    
                    if ask <= bid:
                        return False, f"Ask {ask} not greater than bid {bid}"
                    
                    mid_price = (bid + ask) / 2
                    spread_percent = (ask - bid) / mid_price
                    max_spread = rule.parameters.get('max_spread_percent', 0.1)
                    
                    if spread_percent > max_spread:
                        return False, f"Spread {spread_percent:.4f} exceeds maximum {max_spread}"
            
            elif rule.name == "timestamp_recent":
                timestamp = data_point.get('timestamp')
                
                if timestamp:
                    if isinstance(timestamp, str):
                        timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    
                    max_age_hours = rule.parameters.get('max_age_hours', 24)
                    age = datetime.utcnow() - timestamp
                    
                    if age.total_seconds() > max_age_hours * 3600:
                        return False, f"Timestamp {age} exceeds maximum age {max_age_hours} hours"
            
            return True, ""
            
        except Exception as e:
            return False, f"Custom validation error: {str(e)}"
    
    async def _detect_outliers(self, data_point: Dict[str, Any], source: str) -> List[DataIssue]:
        """Detect outliers in data point"""
        issues = []
        
        try:
            # Simple outlier detection for price fields
            price_fields = ['price', 'bid', 'ask', 'open', 'high', 'low', 'close']
            
            for field in price_fields:
                value = data_point.get(field)
                
                if value is not None:
                    numeric_value = float(value)
                    
                    # Check for extreme values (basic outlier detection)
                    if numeric_value <= 0:
                        issues.append(DataIssue(
                            source=source,
                            symbol=data_point.get('symbol', 'unknown'),
                            field=field,
                            issue_type='outlier',
                            severity='error',
                            description=f'Non-positive price: {numeric_value}',
                            original_value=numeric_value,
                            corrected_value=None,
                            timestamp=datetime.utcnow()
                        ))
                        self.stats['outliers_detected'] += 1
                    
                    elif numeric_value > 10000:  # Arbitrary high price threshold
                        issues.append(DataIssue(
                            source=source,
                            symbol=data_point.get('symbol', 'unknown'),
                            field=field,
                            issue_type='outlier',
                            severity='warning',
                            description=f'Unusually high price: {numeric_value}',
                            original_value=numeric_value,
                            corrected_value=None,
                            timestamp=datetime.utcnow()
                        ))
                        self.stats['outliers_detected'] += 1
            
        except Exception as e:
            logger.error(f"Error detecting outliers: {e}")
        
        return issues
    
    def get_normalization_stats(self) -> Dict[str, Any]:
        """Get normalization statistics"""
        return {
            'total_processed': self.stats['total_processed'],
            'normalization_applied': dict(self.stats['normalization_applied']),
            'validation_failures': dict(self.stats['validation_failures']),
            'duplicates_detected': self.stats['duplicates_detected'],
            'outliers_detected': self.stats['outliers_detected'],
            'recent_issues': self.stats['data_issues'][-100:]  # Last 100 issues
        }
    
    def get_data_quality_score(self, source: str = None) -> float:
        """Calculate data quality score"""
        total_processed = self.stats['total_processed']
        
        if total_processed == 0:
            return 1.0
        
        # Calculate error rate
        total_errors = sum(self.stats['validation_failures'].values())
        error_rate = total_errors / total_processed
        
        # Calculate quality score (1.0 = perfect, 0.0 = poor)
        quality_score = max(0.0, 1.0 - error_rate)
        
        return quality_score

class DuplicateDetector:
    """Detects duplicate data points within a time window"""
    
    def __init__(self, window_seconds: int = 5):
        self.window_seconds = window_seconds
        self.recent_hashes: deque = deque()
        self.hash_timestamps: Dict[str, datetime] = {}
    
    def is_duplicate(self, data_point: Dict[str, Any]) -> bool:
        """Check if data point is a duplicate"""
        try:
            # Create hash of relevant fields
            relevant_fields = ['symbol', 'price', 'bid', 'ask', 'volume', 'timestamp']
            hash_data = {}
            
            for field in relevant_fields:
                if field in data_point:
                    hash_data[field] = data_point[field]
            
            data_hash = hashlib.md5(
                json.dumps(hash_data, sort_keys=True, default=str).encode()
            ).hexdigest()
            
            current_time = datetime.utcnow()
            
            # Clean old hashes
            self._clean_old_hashes(current_time)
            
            # Check if hash exists in recent window
            if data_hash in self.hash_timestamps:
                return True
            
            # Add new hash
            self.hash_timestamps[data_hash] = current_time
            self.recent_hashes.append(data_hash)
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking for duplicates: {e}")
            return False
    
    def _clean_old_hashes(self, current_time: datetime) -> None:
        """Remove old hashes outside the time window"""
        cutoff_time = current_time - timedelta(seconds=self.window_seconds)
        
        # Remove old hashes
        while self.recent_hashes:
            old_hash = self.recent_hashes[0]
            hash_time = self.hash_timestamps.get(old_hash)
            
            if hash_time and hash_time < cutoff_time:
                self.recent_hashes.popleft()
                self.hash_timestamps.pop(old_hash, None)
            else:
                break