"""
Database connection and operations manager
"""

import os
from typing import Optional, List, Dict, Any
from contextlib import contextmanager
from datetime import datetime, timezone

from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.exc import SQLAlchemyError

from src.storage.models import Base, CEP
from src.utils.logger import setup_logger


class DatabaseManager:
    """
    Database manager for PostgreSQL operations.
    Handles connections, sessions, and CEP data operations.
    """

    def __init__(self, database_url: Optional[str] = None):
        """
        Initialize database manager.

        Args:
            database_url: PostgreSQL connection URL (optional, will use environment variables if not provided)
                Format: postgresql://user:password@host:port/database
        """
        if database_url:
            self.database_url = database_url
        else:
            # Try DATABASE_URL first, then construct from components
            database_url = os.getenv('DATABASE_URL')
            if database_url:
                self.database_url = database_url
            else:
                host = os.getenv('POSTGRES_HOST', 'localhost')
                port = os.getenv('POSTGRES_PORT', '5432')
                user = os.getenv('POSTGRES_USER', 'cep_user')
                password = os.getenv('POSTGRES_PASSWORD', 'cep_password')
                database = os.getenv('POSTGRES_DB', 'cep_processor')
                self.database_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        self.logger = setup_logger(name="database_manager")
        self.engine: Optional[Engine] = None
        self.SessionLocal: Optional[sessionmaker] = None
        self._session_factory: Optional[scoped_session] = None

    def connect(self) -> bool:
        """
        Create database engine and session factory.

        Returns:
            True if connected successfully, False otherwise
        """
        try:
            self.logger.info(f"Connecting to database: {self._mask_url(self.database_url)}")
            
            # Create engine with connection pooling
            self.engine = create_engine(
                self.database_url,
                pool_size=10,
                max_overflow=20,
                pool_pre_ping=True,  # Verify connections before using
                echo=False  # Set to True for SQL query logging
            )

            # Create session factory
            self.SessionLocal = sessionmaker(
                bind=self.engine,
                autocommit=False,
                autoflush=False
            )
            self._session_factory = scoped_session(self.SessionLocal)

            # Test connection
            with self.engine.connect() as conn:
                from sqlalchemy import text
                conn.execute(text("SELECT 1"))

            self.logger.info("Successfully connected to database")
            return True

        except SQLAlchemyError as e:
            self.logger.error(f"Failed to connect to database: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error connecting to database: {e}")
            return False

    def disconnect(self):
        """Close database connections."""
        try:
            if self._session_factory:
                self._session_factory.remove()
            if self.engine:
                self.engine.dispose()
            self.logger.info("Disconnected from database")
        except Exception as e:
            self.logger.error(f"Error disconnecting from database: {e}")

    def create_tables(self) -> bool:
        """
        Create all database tables.

        Returns:
            True if tables created successfully, False otherwise
        """
        try:
            if not self.engine:
                self.logger.error("Database not connected. Call connect() first.")
                return False

            self.logger.info("Creating database tables...")
            Base.metadata.create_all(bind=self.engine)
            self.logger.info("Database tables created successfully")
            return True

        except SQLAlchemyError as e:
            self.logger.error(f"Error creating tables: {e}")
            return False

    def drop_tables(self) -> bool:
        """
        Drop all database tables.

        Returns:
            True if tables dropped successfully, False otherwise
        """
        try:
            if not self.engine:
                self.logger.error("Database not connected. Call connect() first.")
                return False

            self.logger.warning("Dropping all database tables...")
            Base.metadata.drop_all(bind=self.engine)
            self.logger.info("Database tables dropped successfully")
            return True

        except SQLAlchemyError as e:
            self.logger.error(f"Error dropping tables: {e}")
            return False

    @contextmanager
    def get_session(self):
        """
        Get database session context manager.

        Yields:
            Database session

        Example:
            with db_manager.get_session() as session:
                cep = session.query(CEP).filter_by(cep='01310100').first()
        """
        if not self._session_factory:
            raise RuntimeError("Database not connected. Call connect() first.")

        session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def save_cep(self, viacep_data: Dict[str, Any]) -> bool:
        """
        Save CEP data to database.

        Args:
            viacep_data: Dictionary from ViaCEP API response

        Returns:
            True if saved successfully, False otherwise
        """
        try:
            with self.get_session() as session:
                cep_model = CEP.from_viacep_response(viacep_data)
                
                # Check if CEP already exists
                existing = session.query(CEP).filter_by(cep=cep_model.cep).first()
                
                if existing:
                    # Update existing record
                    for key, value in cep_model.to_dict().items():
                        if key not in ['created_at', 'updated_at']:
                            setattr(existing, key, value)
                    existing.updated_at = datetime.now(timezone.utc)
                    self.logger.debug(f"Updated CEP {cep_model.cep} in database")
                else:
                    # Insert new record
                    session.add(cep_model)
                    self.logger.debug(f"Saved CEP {cep_model.cep} to database")

            return True

        except SQLAlchemyError as e:
            self.logger.error(f"Error saving CEP to database: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error saving CEP: {e}")
            return False

    def save_multiple_ceps(self, viacep_data_list: List[Dict[str, Any]]) -> int:
        """
        Save multiple CEPs to database in batch.

        Args:
            viacep_data_list: List of dictionaries from ViaCEP API responses

        Returns:
            Number of CEPs successfully saved
        """
        saved_count = 0

        try:
            with self.get_session() as session:
                for viacep_data in viacep_data_list:
                    try:
                        cep_model = CEP.from_viacep_response(viacep_data)
                        
                        # Check if CEP already exists
                        existing = session.query(CEP).filter_by(cep=cep_model.cep).first()
                        
                        if existing:
                            # Update existing record
                            for key, value in cep_model.to_dict().items():
                                if key not in ['created_at', 'updated_at']:
                                    setattr(existing, key, value)
                            existing.updated_at = datetime.now(timezone.utc)
                        else:
                            # Insert new record
                            session.add(cep_model)
                        
                        saved_count += 1

                    except Exception as e:
                        self.logger.warning(f"Error processing CEP data: {e}")
                        continue

                session.commit()
                self.logger.info(f"Saved {saved_count}/{len(viacep_data_list)} CEPs to database")

        except SQLAlchemyError as e:
            self.logger.error(f"Error saving CEPs to database: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error saving CEPs: {e}")

        return saved_count

    def get_cep(self, cep: str) -> Optional[CEP]:
        """
        Get CEP from database.

        Args:
            cep: CEP to retrieve (8 digits, no hyphen)

        Returns:
            CEP model if found, None otherwise
        """
        try:
            # Normalize CEP (remove hyphen)
            cep_clean = cep.replace('-', '').replace(' ', '')

            with self.get_session() as session:
                cep_model = session.query(CEP).filter_by(cep=cep_clean).first()
                return cep_model

        except SQLAlchemyError as e:
            self.logger.error(f"Error retrieving CEP from database: {e}")
            return None

    def get_ceps_by_uf(self, uf: str, limit: Optional[int] = None) -> List[CEP]:
        """
        Get CEPs by state (UF).

        Args:
            uf: State code (e.g., 'SP', 'RJ')
            limit: Maximum number of results (None = no limit)

        Returns:
            List of CEP models
        """
        try:
            with self.get_session() as session:
                query = session.query(CEP).filter_by(uf=uf.upper())
                if limit:
                    query = query.limit(limit)
                return query.all()

        except SQLAlchemyError as e:
            self.logger.error(f"Error retrieving CEPs by UF from database: {e}")
            return []

    def get_ceps_by_localidade(self, localidade: str, limit: Optional[int] = None) -> List[CEP]:
        """
        Get CEPs by city (localidade).

        Args:
            localidade: City name
            limit: Maximum number of results (None = no limit)

        Returns:
            List of CEP models
        """
        try:
            with self.get_session() as session:
                query = session.query(CEP).filter_by(localidade=localidade)
                if limit:
                    query = query.limit(limit)
                return query.all()

        except SQLAlchemyError as e:
            self.logger.error(f"Error retrieving CEPs by localidade from database: {e}")
            return []

    def get_all_ceps(self, limit: Optional[int] = None, offset: int = 0) -> List[CEP]:
        """
        Get all CEPs from database.

        Args:
            limit: Maximum number of results (None = no limit)
            offset: Number of results to skip

        Returns:
            List of CEP models (detached from session)
        """
        try:
            with self.get_session() as session:
                query = session.query(CEP).offset(offset)
                if limit:
                    query = query.limit(limit)
                ceps = query.all()
                # Expunge all objects from session so they can be used after session closes
                for cep in ceps:
                    session.expunge(cep)
                return ceps

        except SQLAlchemyError as e:
            self.logger.error(f"Error retrieving all CEPs from database: {e}")
            return []

    def count_ceps(self) -> int:
        """
        Get total number of CEPs in database.

        Returns:
            Total count of CEPs
        """
        try:
            with self.get_session() as session:
                return session.query(CEP).count()

        except SQLAlchemyError as e:
            self.logger.error(f"Error counting CEPs: {e}")
            return 0

    def _mask_url(self, url: str) -> str:
        """
        Mask password in database URL for logging.

        Args:
            url: Database URL

        Returns:
            Masked URL
        """
        try:
            if '@' in url:
                parts = url.split('@')
                if '://' in parts[0]:
                    protocol_user_pass = parts[0]
                    rest = '@'.join(parts[1:])
                    if ':' in protocol_user_pass:
                        protocol_user = protocol_user_pass.rsplit(':', 1)[0]
                        return f"{protocol_user}:***@{rest}"
            return url
        except Exception:
            return "***"
