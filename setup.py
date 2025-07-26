#!/usr/bin/env python3
"""
Setup script for the LeenaAI Database Package
"""

from setuptools import setup, find_packages
import os

# Read the README file
def read_readme():
    readme_path = os.path.join(os.path.dirname(__file__), 'README.md')
    if os.path.exists(readme_path):
        with open(readme_path, 'r', encoding='utf-8') as f:
            return f.read()
    return "LeenaAI Database Package"

# Read requirements
def read_requirements():
    requirements_path = os.path.join(os.path.dirname(__file__), 'requirements.txt')
    if os.path.exists(requirements_path):
        with open(requirements_path, 'r', encoding='utf-8') as f:
            return [line.strip() for line in f if line.strip() and not line.startswith('#')]
    return []

setup(
    name="leenaai-firestoredb",
    version="1.0.0",
    description="Firestore and ChromaDB package for LeenaAI real estate chatbot",
    long_description=read_readme(),
    long_description_content_type="text/markdown",
    author="LeenaAI Team",
    author_email="info@leenaai.com",
    url="https://github.com/leenaai/leenaai-firestoredb",
    packages=find_packages(),
    include_package_data=True,
    install_requires=read_requirements(),
    python_requires=">=3.8",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    keywords="firestore chroma vector real-estate chatbot database",
    project_urls={
        "Bug Reports": "https://github.com/leenaai/leenaai-firestoredb/issues",
        "Source": "https://github.com/leenaai/leenaai-firestoredb",
        "Documentation": "https://github.com/leenaai/leenaai-firestoredb/blob/main/README.md",
    },
) 