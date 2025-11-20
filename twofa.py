# twofa.py
"""
Two-Factor Authentication (2FA) Manager for OTMS
Uses TOTP (Time-based One-Time Password) - compatible with Google Authenticator, Authy, etc.
"""

import pyotp
import qrcode
import secrets
import json
from io import BytesIO
from datetime import datetime
from typing import Optional, List, Tuple
from sqlalchemy.orm import Session
from models import User

class TwoFactorAuth:
    """Manage 2FA for users"""
    
    APP_NAME = "OTMS"
    ISSUER_NAME = "Oil Terminal Management System"
    
    @staticmethod
    def generate_secret() -> str:
        """Generate a new TOTP secret key"""
        return pyotp.random_base32()
    
    @staticmethod
    def generate_backup_codes(count: int = 10) -> List[str]:
        """Generate backup codes for account recovery"""
        codes = []
        for _ in range(count):
            # Generate 8-character alphanumeric codes
            code = ''.join(secrets.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789') for _ in range(8))
            # Format as XXXX-XXXX for readability
            formatted = f"{code[:4]}-{code[4:]}"
            codes.append(formatted)
        return codes
    
    @staticmethod
    def enable_2fa(session: Session, user_id: int) -> Tuple[str, List[str], str]:
        """
        Enable 2FA for a user.
        Returns: (secret, backup_codes, provisioning_uri)
        """
        user = session.query(User).filter(User.id == user_id).one_or_none()
        if not user:
            raise ValueError("User not found")
        
        if user.totp_enabled:
            raise ValueError("2FA is already enabled for this user")
        
        # Generate secret and backup codes
        secret = TwoFactorAuth.generate_secret()
        backup_codes = TwoFactorAuth.generate_backup_codes()
        
        # Store in database (backup codes as JSON)
        user.totp_secret = secret
        user.backup_codes = json.dumps(backup_codes)
        user.totp_enabled = False  # Not enabled until verified
        
        session.commit()
        
        # Generate provisioning URI for QR code
        totp = pyotp.TOTP(secret)
        provisioning_uri = totp.provisioning_uri(
            name=user.username,
            issuer_name=TwoFactorAuth.ISSUER_NAME
        )
        
        return secret, backup_codes, provisioning_uri
    
    @staticmethod
    def verify_and_enable(session: Session, user_id: int, token: str) -> bool:
        """
        Verify 2FA token and enable 2FA if valid.
        Returns True if successful.
        """
        user = session.query(User).filter(User.id == user_id).one_or_none()
        if not user:
            raise ValueError("User not found")
        
        if not user.totp_secret:
            raise ValueError("2FA setup not initiated for this user")
        
        # Verify token
        totp = pyotp.TOTP(user.totp_secret)
        if totp.verify(token, valid_window=1):  # Allow 30s window
            user.totp_enabled = True
            session.commit()
            return True
        
        return False
    
    @staticmethod
    def verify_token(session: Session, user_id: int, token: str) -> bool:
        """
        Verify a 2FA token for login.
        Returns True if valid.
        """
        user = session.query(User).filter(User.id == user_id).one_or_none()
        if not user:
            return False
        
        if not user.totp_enabled or not user.totp_secret:
            return False
        
        # Check TOTP token
        totp = pyotp.TOTP(user.totp_secret)
        if totp.verify(token, valid_window=1):
            return True
        
        # Check backup codes
        if user.backup_codes:
            backup_codes = json.loads(user.backup_codes)
            if token.upper() in backup_codes:
                # Remove used backup code
                backup_codes.remove(token.upper())
                user.backup_codes = json.dumps(backup_codes)
                session.commit()
                return True
        
        return False
    
    @staticmethod
    def disable_2fa(session: Session, user_id: int):
        """Disable 2FA for a user"""
        user = session.query(User).filter(User.id == user_id).one_or_none()
        if not user:
            raise ValueError("User not found")
        
        user.totp_secret = None
        user.totp_enabled = False
        user.backup_codes = None
        
        session.commit()
    
    @staticmethod
    def is_enabled(session: Session, user_id: int) -> bool:
        """Check if 2FA is enabled for a user"""
        user = session.query(User).filter(User.id == user_id).one_or_none()
        if not user:
            return False
        
        return user.totp_enabled and user.totp_secret is not None
    
    @staticmethod
    def generate_qr_code(provisioning_uri: str) -> BytesIO:
        """Generate QR code image from provisioning URI"""
        qr = qrcode.QRCode(version=1, box_size=10, border=5)
        qr.add_data(provisioning_uri)
        qr.make(fit=True)
        
        img = qr.make_image(fill_color="black", back_color="white")
        
        buf = BytesIO()
        img.save(buf, format='PNG')
        buf.seek(0)
        
        return buf
    
    @staticmethod
    def get_backup_codes(session: Session, user_id: int) -> List[str]:
        """Get remaining backup codes for a user"""
        user = session.query(User).filter(User.id == user_id).one_or_none()
        if not user or not user.backup_codes:
            return []
        
        return json.loads(user.backup_codes)
    
    @staticmethod
    def regenerate_backup_codes(session: Session, user_id: int) -> List[str]:
        """Regenerate backup codes for a user"""
        user = session.query(User).filter(User.id == user_id).one_or_none()
        if not user:
            raise ValueError("User not found")
        
        if not user.totp_enabled:
            raise ValueError("2FA is not enabled for this user")
        
        new_codes = TwoFactorAuth.generate_backup_codes()
        user.backup_codes = json.dumps(new_codes)
        session.commit()
        
        return new_codes