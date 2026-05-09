# src/whatstheodds/pipeline/search_producer.py

import logging
import os
from datetime import datetime

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
