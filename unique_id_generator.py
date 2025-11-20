# unique_id_generator.py
"""
Unique ID Generator with Location Prefix
Generates unique transaction IDs in format: LOC-YYYYMMDD-NNNN
Example: AGGU-20250109-0001, BFS-20250109-0023
"""

from datetime import datetime
from sqlalchemy import func

class UniqueIDGenerator:
    """Generate unique IDs with location prefix"""
    
    @staticmethod
    def generate_transaction_id(session, location_code, model_class):
        """
        Generate unique transaction ID with location prefix
        
        Args:
            session: SQLAlchemy session
            location_code: Location code (e.g., 'AGGU', 'BFS')
            model_class: Model class (TankTransaction, OTRVessel, etc.)
            
        Returns:
            str: Unique ID in format LOC-YYYYMMDD-NNNN
        """
        # Get current date
        today = datetime.now().strftime("%Y%m%d")
        
        # Create prefix
        prefix = f"{location_code.upper()}-{today}"
        
        # Find the highest sequence number for today at this location
        # Query for IDs starting with this prefix
        existing_ids = session.query(model_class.id).filter(
            model_class.id.like(f"{prefix}-%")
        ).all()
        
        if not existing_ids:
            # First transaction of the day
            sequence = 1
        else:
            # Extract sequence numbers and find max
            sequences = []
            for (id_val,) in existing_ids:
                try:
                    # Extract last part after last hyphen
                    seq_str = id_val.split('-')[-1]
                    sequences.append(int(seq_str))
                except:
                    continue
            
            sequence = max(sequences) + 1 if sequences else 1
        
        # Generate unique ID
        unique_id = f"{prefix}-{sequence:04d}"
        
        return unique_id
    
    @staticmethod
    def generate_fso_operation_id(session, location_code, fso_vessel):
        """
        Generate unique FSO operation ID
        Format: LOC-FSO-YYYYMMDD-NNNN
        Example: AGGU-AGIP-20250109-0001
        """
        today = datetime.now().strftime("%Y%m%d")
        
        # Create prefix with FSO vessel abbreviation
        fso_abbrev = fso_vessel.replace(" ", "").replace("OML", "")[:6].upper()
        prefix = f"{location_code.upper()}-{fso_abbrev}-{today}"
        
        from models import FSOOperation
        
        # Find highest sequence
        existing_ids = session.query(FSOOperation.id).filter(
            FSOOperation.id.like(f"{prefix}-%")
        ).all()
        
        if not existing_ids:
            sequence = 1
        else:
            sequences = []
            for (id_val,) in existing_ids:
                try:
                    seq_str = id_val.split('-')[-1]
                    sequences.append(int(seq_str))
                except:
                    continue
            sequence = max(sequences) + 1 if sequences else 1
        
        unique_id = f"{prefix}-{sequence:04d}"
        return unique_id
    
    @staticmethod
    def validate_unique_id(session, unique_id, model_class):
        """
        Check if unique ID already exists
        
        Returns:
            bool: True if ID is unique, False if exists
        """
        existing = session.query(model_class).filter(
            model_class.id == unique_id
        ).first()
        
        return existing is None
    
    @staticmethod
    def parse_unique_id(unique_id):
        """
        Parse unique ID to extract components
        
        Returns:
            dict: {location, date, sequence}
        """
        try:
            parts = unique_id.split('-')
            if len(parts) >= 3:
                return {
                    'location': parts[0],
                    'date': parts[1],
                    'sequence': int(parts[2])
                }
        except:
            pass
        
        return None