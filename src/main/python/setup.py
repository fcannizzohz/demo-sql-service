from setuptools import setup, find_packages

setup(
    name="hazelcast_sqlalchemy",
    version="0.1.0",
    description="SQLAlchemy Hazelcast dialect for Superset",
    packages=find_packages(),
    install_requires=[
        "hazelcast-python-client>=5.5.0",
        "SQLAlchemy>=1.4"
    ],
    entry_points={
        "sqlalchemy.dialects": [
            "hazelcast.python = hazelcast_sqlalchemy.dialect:HazelcastDialect"
        ]
    },
)
