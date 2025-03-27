class MongoToolkitError(Exception):
    """Base exception for the MongoDB Toolkit."""
    pass

class ConfigurationError(MongoToolkitError):
    """Exception raised for errors in configuration."""
    pass

class SchemaError(MongoToolkitError):
    """Exception raised during schema generation."""
    pass

class ValidationError(MongoToolkitError):
    """Exception raised during query validation."""
    pass

class ExecutionError(MongoToolkitError):
    """Exception raised during query execution."""
    pass