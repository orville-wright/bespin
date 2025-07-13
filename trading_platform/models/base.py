"""
Base data models and mixins for the trading platform
"""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel as PydanticBaseModel, Field
from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func


# SQLAlchemy Base
SQLAlchemyBase = declarative_base()


class TimestampMixin:
    """Mixin for adding timestamp fields to SQLAlchemy models"""
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())


class BaseModel(PydanticBaseModel):
    """
    Base Pydantic model with common configuration
    """
    
    class Config:
        # Allow use with SQLAlchemy models
        from_attributes = True
        # Validate on assignment
        validate_assignment = True
        # Use enum values
        use_enum_values = True
        # JSON encoders for custom types
        json_encoders = {
            datetime: lambda v: v.isoformat(),
        }


class BaseEntity(SQLAlchemyBase, TimestampMixin):
    """
    Base SQLAlchemy entity with common fields
    """
    __abstract__ = True
    
    id = Column(Integer, primary_key=True, index=True)
    
    def to_dict(self):
        """Convert to dictionary"""
        return {
            column.name: getattr(self, column.name)
            for column in self.__table__.columns
        }


class IDMixin(BaseModel):
    """Mixin for models with ID field"""
    id: Optional[int] = Field(None, description="Unique identifier")


class TimestampModel(BaseModel):
    """Pydantic model with timestamp fields"""
    created_at: Optional[datetime] = Field(None, description="Creation timestamp")
    updated_at: Optional[datetime] = Field(None, description="Last update timestamp")