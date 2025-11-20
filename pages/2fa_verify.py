"""
Auto-generated module for the "2FA Verify" page.
"""
from __future__ import annotations
import streamlit as st
import streamlit as st
import pandas as pd
from datetime import datetime, date, time, timedelta
from pathlib import Path
from ui import header
import sys
import os
import shutil
import asyncio
from io import BytesIO
import base64
import streamlit.components.v1 as components
from reportlab.lib.pagesizes import A4, landscape
from reportlab.pdfgen import canvas
from reportlab.lib.units import cm, mm
from reportlab.lib import colors
from reportlab.lib.utils import ImageReader
import math
from db import get_session, init_db, engine
from material_balance_calculator import MaterialBalanceCalculator as MBC
from sqlalchemy import text, func, and_, or_
from sqlalchemy.orm import joinedload
from sqlalchemy.exc import IntegrityError
import plotly.graph_objects as go
from logger import log_info, log_error, log_warning, log_debug
from security import SecurityManager
from task_manager import TaskManager
from typing import Any, Dict, Optional, List, Tuple
from collections import defaultdict
from functools import lru_cache
import re
from uuid import uuid4
import hashlib
import html
from timezone_utils import format_local_datetime, get_local_time
from recycle_bin import RecycleBinManager
import reportlab
import importlib
from models import Location
import streamlit as _stmod
from models import FSOOperation  # local import to avoid circular deps
import pandas as pd  # local import to avoid polluting top-level namespace
from permission_manager import PermissionManager
from security import SecurityManager
import base64
from io import BytesIO
from datetime import date, timedelta
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
from db import get_session
from models import OFSProductionEvacuationRecord
import time
from models import Operation  # this Enum is defined in models.py
from models import TankTransaction, Table11
import bisect
import re
from sqlalchemy import text
from models import Operation
import json
from datetime import date, timedelta
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4, landscape
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
import pandas as pd  # local import to avoid polluting global namespace
from sqlalchemy import or_
from models import Location, LocationTankerEntry
import streamlit.components.v1 as components
import json, base64
from datetime import date, timedelta, datetime, time
from pathlib import Path
from models import Location
from material_balance_calculator import MaterialBalanceCalculator as MBC
from reportlab.lib.pagesizes import A4  # portrait
from sqlalchemy import or_
from models import ReportDefinition
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from models import FSOOperation
from datetime import time as dt_time
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from models import FSOOperation
from sqlalchemy import and_, or_
from models import OTR  # your preferred model name
from models import OTRTransaction as OTRModel
import streamlit as _stmod
from models import MeterTransaction  # added Meter-2 fields earlier
from reportlab.lib.pagesizes import A4, landscape
from datetime import date, timedelta
import pandas as pd
import io, base64
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.units import cm
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER
from typing import Dict, Tuple, Optional
from models import Table11
from models import YadeCalibration
from models import YadeSampleParam
from typing import Dict, Tuple, List, Optional
import bisect
import bisect
import math
from models import (YadeDip, YadeSampleParam, TOAYadeStage, TOAYadeSummary, YadeVoyage)
import streamlit as st  # ensure st exists in this scope
from location_manager import LocationManager
from location_config import LocationConfig
from location_manager import LocationManager
from permission_manager import PermissionManager
from security import SecurityManager
from models import YadeVoyage, TOAYadeSummary, TOAYadeStage
from datetime import datetime, timedelta
from ui import header
from db import get_session
from pages.helpers import st_safe_rerun, archive_payload_for_delete

def render():
        # Delegate rendering of the Two‑Factor Authentication verification page to a
        # dedicated module. This keeps ``oil_app_ui.py`` uncluttered and makes the
        # 2FA workflow easier to maintain. See ``pages/two_fa_verify.py`` for
        # implementation details.
        from pages.two_fa_verify import render as render_two_fa_verify
        render_two_fa_verify()
    
    # ========================= MANAGE LOCATIONS (Admin Only) =========================