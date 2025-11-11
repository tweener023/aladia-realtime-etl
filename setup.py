from setuptools import setup, find_packages

setup(
    name="aladia-realtime-etl",
    version="0.1.0",
    description="Real-time ETL Pipeline with Change Data Capture",
    author="Aladia Engineering Team",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.9",
    install_requires=[
        "pyspark>=3.5.0",
        "apache-beam[gcp]>=2.52.0",
        "psycopg2-binary>=2.9.9",
        "sqlalchemy>=2.0.23",
        "kafka-python>=2.0.2",
        "confluent-kafka>=2.3.0",
        "requests>=2.31.0",
        "fastapi>=0.104.1",
        "uvicorn>=0.24.0",
        "pydantic>=2.5.0",
        "python-dotenv>=1.0.0",
        "click>=8.1.7",
        "structlog>=23.2.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.3",
            "pytest-cov>=4.1.0",
            "black>=23.12.0",
            "flake8>=6.1.0",
            "mypy>=1.7.1",
        ]
    },
    entry_points={
        "console_scripts": [
            "etl-demo=src.demo:main",
            "etl-health=src.orchestration.health_check:main",
        ],
    },
)