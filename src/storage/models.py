"""
SQLAlchemy models for CEP data storage
"""

from datetime import datetime, timezone

from sqlalchemy import Column, String, DateTime, Index
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class CEP(Base):
    """
    Model for storing CEP data from ViaCEP API.
    """
    __tablename__ = 'ceps'

    # Primary key: CEP (8 digits, no hyphen)
    cep = Column(String(8), primary_key=True, nullable=False, index=True)

    # Address information
    logradouro = Column(String(255), nullable=True)
    complemento = Column(String(255), nullable=True)
    bairro = Column(String(255), nullable=True)
    localidade = Column(String(255), nullable=True)
    uf = Column(String(2), nullable=True, index=True)

    # Additional information
    ibge = Column(String(10), nullable=True)
    gia = Column(String(10), nullable=True)
    ddd = Column(String(2), nullable=True)
    siafi = Column(String(10), nullable=True)

    # Metadata
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)

    # Indexes for common queries
    __table_args__ = (
        Index('idx_cep_uf', 'uf'),
        Index('idx_cep_localidade', 'localidade'),
        Index('idx_cep_created_at', 'created_at'),
    )

    def __repr__(self) -> str:
        return f"<CEP(cep='{self.cep}', logradouro='{self.logradouro}', localidade='{self.localidade}', uf='{self.uf}')>"

    def to_dict(self) -> dict:
        """
        Convert CEP model to dictionary.

        Returns:
            Dictionary representation of CEP
        """
        return {
            'cep': self.cep,
            'logradouro': self.logradouro,
            'complemento': self.complemento,
            'bairro': self.bairro,
            'localidade': self.localidade,
            'uf': self.uf,
            'ibge': self.ibge,
            'gia': self.gia,
            'ddd': self.ddd,
            'siafi': self.siafi,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
        }

    @classmethod
    def from_viacep_response(cls, viacep_data: dict) -> 'CEP':
        """
        Create CEP model from ViaCEP API response.

        Args:
            viacep_data: Dictionary from ViaCEP API response

        Returns:
            CEP model instance
        """
        # Extract CEP (remove hyphen if present)
        cep_value = viacep_data.get('cep', '').replace('-', '').replace(' ', '')
        
        return cls(
            cep=cep_value,
            logradouro=viacep_data.get('logradouro') or None,
            complemento=viacep_data.get('complemento') or None,
            bairro=viacep_data.get('bairro') or None,
            localidade=viacep_data.get('localidade') or None,
            uf=viacep_data.get('uf') or None,
            ibge=viacep_data.get('ibge') or None,
            gia=viacep_data.get('gia') or None,
            ddd=viacep_data.get('ddd') or None,
            siafi=viacep_data.get('siafi') or None,
        )

