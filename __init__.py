"""
LeenaAI FirestoreDB Package

A comprehensive Firestore and ChromaDB package for the LeenaAI real estate chatbot.
"""

__version__ = "1.0.0"
__author__ = "LeenaAI Team"
__email__ = "info@leenaai.com"

# Import main modules
from .firestore import *
from .vector import *
from .schemas import *
from .utils import *

__all__ = [
    "__version__",
    "__author__",
    "__email__",
] 