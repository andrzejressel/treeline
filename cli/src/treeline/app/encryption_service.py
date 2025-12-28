"""Service for database encryption operations."""

import base64
import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Dict

import duckdb
from argon2.low_level import Type, hash_secret_raw

from treeline.domain import EncryptionMetadata, EncryptionStatus, Fail, Ok, Result
from treeline.utils import get_logger

logger = get_logger("encryption")

DEFAULT_ARGON2_PARAMS: Dict[str, int] = {
    "time_cost": 3,
    "memory_cost": 65536,  # 64 MiB
    "parallelism": 4,
    "hash_len": 32,
}


class EncryptionService:
    """Service for database encryption operations.

    Handles encrypting and decrypting the database using DuckDB's native
    AES-256-GCM encryption with Argon2id key derivation.
    """

    def __init__(
        self,
        treeline_dir: Path,
        db_path: Path,
        backup_service: Any | None = None,
    ):
        """Initialize encryption service.

        Args:
            treeline_dir: Directory where treeline data is stored (~/.treeline)
            db_path: Path to the database file
            backup_service: Optional BackupService for creating safety backups
        """
        self.treeline_dir = treeline_dir
        self.db_path = db_path
        self.encryption_json_path = treeline_dir / "encryption.json"
        self.backup_service = backup_service

    def _load_metadata(self) -> EncryptionMetadata | None:
        """Load encryption metadata from encryption.json."""
        if not self.encryption_json_path.exists():
            return None
        try:
            with open(self.encryption_json_path) as f:
                data = json.load(f)
            return EncryptionMetadata(**data)
        except Exception as e:
            logger.error(f"Failed to load encryption metadata: {e}")
            return None

    def _save_metadata(self, metadata: EncryptionMetadata) -> Result[None]:
        """Save encryption metadata to encryption.json."""
        try:
            with open(self.encryption_json_path, "w") as f:
                json.dump(metadata.model_dump(), f, indent=2)
            return Ok()
        except Exception as e:
            return Fail(f"Failed to save encryption metadata: {e}")

    def _delete_metadata(self) -> Result[None]:
        """Delete encryption.json."""
        try:
            if self.encryption_json_path.exists():
                self.encryption_json_path.unlink()
            return Ok()
        except Exception as e:
            return Fail(f"Failed to delete encryption metadata: {e}")

    def _derive_key(self, password: str, salt: bytes, params: Dict[str, int]) -> bytes:
        """Derive encryption key from password using Argon2id.

        Args:
            password: User password
            salt: Random salt bytes
            params: Argon2 parameters (time_cost, memory_cost, parallelism, hash_len)

        Returns:
            Derived key bytes
        """
        return hash_secret_raw(
            secret=password.encode("utf-8"),
            salt=salt,
            time_cost=params["time_cost"],
            memory_cost=params["memory_cost"],
            parallelism=params["parallelism"],
            hash_len=params["hash_len"],
            type=Type.ID,
        )

    async def get_status(self) -> Result[EncryptionStatus]:
        """Get current encryption status.

        Returns:
            Result containing EncryptionStatus
        """
        metadata = self._load_metadata()
        if metadata is None or not metadata.encrypted:
            return Ok(EncryptionStatus(encrypted=False))
        return Ok(
            EncryptionStatus(
                encrypted=True,
                algorithm=metadata.algorithm,
                version=metadata.version,
            )
        )

    async def encrypt(self, password: str) -> Result[Dict[str, Any]]:
        """Encrypt the database with the given password.

        Creates a safety backup first (if backup_service provided), then
        encrypts the database using DuckDB's native encryption.

        Args:
            password: Encryption password

        Returns:
            Result containing dict with backup_name (if backup was created)
        """
        # Check if already encrypted
        metadata = self._load_metadata()
        if metadata and metadata.encrypted:
            return Fail("Database is already encrypted")

        # Check database exists
        if not self.db_path.exists():
            return Fail("Database file not found")

        # Create safety backup first
        backup_name = None
        if self.backup_service:
            logger.info("Creating safety backup before encryption...")
            backup_result = await self.backup_service.backup()
            if not backup_result.success:
                return Fail(f"Failed to create safety backup: {backup_result.error}")
            backup_name = backup_result.data.name
            logger.info(f"Created safety backup: {backup_name}")

        # Generate salt and derive key
        salt = os.urandom(16)
        params = DEFAULT_ARGON2_PARAMS.copy()
        key = self._derive_key(password, salt, params)
        key_hex = key.hex()

        # Create temp directory for export and temp file for new database
        with tempfile.TemporaryDirectory() as export_dir:
            fd, temp_path_str = tempfile.mkstemp(suffix=".duckdb")
            os.close(fd)
            temp_encrypted_path = Path(temp_path_str)
            temp_encrypted_path.unlink()  # Remove so DuckDB creates fresh

            try:
                logger.info("Encrypting database...")

                # Export original database to parquet files (handles table dependencies)
                conn = duckdb.connect(str(self.db_path), read_only=True)
                conn.execute(f"EXPORT DATABASE '{export_dir}' (FORMAT PARQUET)")
                conn.close()

                # Import into new encrypted database
                new_conn = duckdb.connect(":memory:")
                new_conn.execute(
                    f"ATTACH '{temp_encrypted_path}' AS enc (ENCRYPTION_KEY '{key_hex}')"
                )
                new_conn.execute("USE enc")
                new_conn.execute(f"IMPORT DATABASE '{export_dir}'")
                new_conn.close()

                # Replace original with encrypted version
                shutil.move(str(temp_encrypted_path), str(self.db_path))
                logger.info("Database encrypted successfully")

                # Save metadata
                new_metadata = EncryptionMetadata(
                    encrypted=True,
                    salt=base64.b64encode(salt).decode("ascii"),
                    algorithm="argon2id",
                    version=1,
                    argon2_params=params,
                )
                save_result = self._save_metadata(new_metadata)
                if not save_result.success:
                    return save_result

                return Ok({"backup_name": backup_name})

            except Exception as e:
                logger.error(f"Encryption failed: {e}")
                # Clean up temp file on error
                if temp_encrypted_path.exists():
                    temp_encrypted_path.unlink()
                return Fail(f"Encryption failed: {e}")

    async def decrypt(self, password: str) -> Result[Dict[str, Any]]:
        """Decrypt the database with the given password.

        Creates a safety backup first (if backup_service provided), then
        decrypts the database.

        Args:
            password: Current encryption password

        Returns:
            Result containing dict with backup_name (if backup was created)
        """
        metadata = self._load_metadata()
        if not metadata or not metadata.encrypted:
            return Fail("Database is not encrypted")

        # Derive key from password
        salt = base64.b64decode(metadata.salt)
        key = self._derive_key(password, salt, metadata.argon2_params)
        key_hex = key.hex()

        # Verify password by attempting to read the database
        try:
            test_conn = duckdb.connect(":memory:")
            test_conn.execute(
                f"ATTACH '{self.db_path}' AS enc (ENCRYPTION_KEY '{key_hex}', READ_ONLY)"
            )
            # Use the attached database and query its tables
            test_conn.execute("USE enc")
            test_conn.execute(
                "SELECT table_name FROM information_schema.tables LIMIT 1"
            )
            test_conn.close()
        except Exception as e:
            logger.debug(f"Password verification failed: {e}")
            return Fail("Invalid password")

        # Create safety backup first
        backup_name = None
        if self.backup_service:
            logger.info("Creating safety backup before decryption...")
            # Need to temporarily set the encryption key for backup to work
            # Actually, backup_service doesn't know about encryption, it just copies the file
            # which is fine - the backup will be encrypted
            backup_result = await self.backup_service.backup()
            if not backup_result.success:
                return Fail(f"Failed to create safety backup: {backup_result.error}")
            backup_name = backup_result.data.name
            logger.info(f"Created safety backup: {backup_name}")

        # Create temp directory for export and temp file for new database
        with tempfile.TemporaryDirectory() as export_dir:
            fd, temp_path_str = tempfile.mkstemp(suffix=".duckdb")
            os.close(fd)
            temp_decrypted_path = Path(temp_path_str)
            temp_decrypted_path.unlink()  # Remove so DuckDB creates fresh

            try:
                logger.info("Decrypting database...")

                # Export encrypted database to parquet files (handles table dependencies)
                conn = duckdb.connect(":memory:")
                conn.execute(
                    f"ATTACH '{self.db_path}' AS enc (ENCRYPTION_KEY '{key_hex}', READ_ONLY)"
                )
                conn.execute("USE enc")
                conn.execute(f"EXPORT DATABASE '{export_dir}' (FORMAT PARQUET)")
                conn.close()

                # Import into new unencrypted database
                new_conn = duckdb.connect(str(temp_decrypted_path))
                new_conn.execute(f"IMPORT DATABASE '{export_dir}'")
                new_conn.close()

                # Replace original with decrypted version
                shutil.move(str(temp_decrypted_path), str(self.db_path))
                logger.info("Database decrypted successfully")

                # Remove encryption metadata
                delete_result = self._delete_metadata()
                if not delete_result.success:
                    logger.warning(
                        f"Failed to delete encryption metadata: {delete_result.error}"
                    )

                return Ok({"backup_name": backup_name})

            except Exception as e:
                logger.error(f"Decryption failed: {e}")
                # Clean up temp file on error
                if temp_decrypted_path.exists():
                    temp_decrypted_path.unlink()
                return Fail(f"Decryption failed: {e}")

    def derive_key_for_connection(self, password: str) -> Result[str]:
        """Derive the encryption key for database connections.

        Used by the container to get the key for the repository.

        Args:
            password: Encryption password

        Returns:
            Result containing the key as a hex string for use with ATTACH
        """
        metadata = self._load_metadata()
        if not metadata or not metadata.encrypted:
            return Fail("Database is not encrypted")

        try:
            salt = base64.b64decode(metadata.salt)
            key = self._derive_key(password, salt, metadata.argon2_params)
            return Ok(key.hex())
        except Exception as e:
            return Fail(f"Failed to derive encryption key: {e}")

    def is_encrypted(self) -> bool:
        """Check if the database is encrypted.

        Returns:
            True if database is encrypted, False otherwise
        """
        metadata = self._load_metadata()
        return metadata is not None and metadata.encrypted
