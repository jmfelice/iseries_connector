"""
Core functionality for {{package_name}}.
"""

from typing import Any, Dict, Optional


class BaseClass:
    """Base class template with common functionality."""

    def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
        """Initialize the base class.

        Args:
            config: Optional configuration dictionary.
        """
        self.config = config or {}

    def example_method(self) -> str:
        """Example method that returns a string.

        Returns:
            A string message.
        """
        return "This is an example method"


class ExampleClass(BaseClass):
    """Example class that inherits from BaseClass."""

    def __init__(self, name: str, config: Optional[Dict[str, Any]] = None) -> None:
        """Initialize the example class.

        Args:
            name: The name of the instance.
            config: Optional configuration dictionary.
        """
        super().__init__(config)
        self.name = name

    def get_name(self) -> str:
        """Get the name of the instance.

        Returns:
            The name of the instance.
        """
        return self.name 