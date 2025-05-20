"""Tests for the core module."""

import pytest

from {{package_name}}.core import BaseClass, ExampleClass


def test_base_class_initialization():
    """Test BaseClass initialization."""
    base = BaseClass()
    assert isinstance(base.config, dict)
    assert len(base.config) == 0

    config = {"key": "value"}
    base = BaseClass(config)
    assert base.config == config


def test_base_class_example_method():
    """Test BaseClass example_method."""
    base = BaseClass()
    assert base.example_method() == "This is an example method"


def test_example_class_initialization():
    """Test ExampleClass initialization."""
    name = "test"
    example = ExampleClass(name)
    assert example.name == name
    assert isinstance(example.config, dict)
    assert len(example.config) == 0


def test_example_class_get_name():
    """Test ExampleClass get_name method."""
    name = "test"
    example = ExampleClass(name)
    assert example.get_name() == name 