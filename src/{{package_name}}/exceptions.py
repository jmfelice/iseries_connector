"""
Custom exceptions for {{package_name}}.
"""


class {{package_name}}Error(Exception):
    """Base exception for {{package_name}}."""

    pass


class ConfigurationError({{package_name}}Error):
    """Raised when there is a configuration error."""

    pass


class ValidationError({{package_name}}Error):
    """Raised when validation fails."""

    pass


class ResourceNotFoundError({{package_name}}Error):
    """Raised when a requested resource is not found."""

    pass 