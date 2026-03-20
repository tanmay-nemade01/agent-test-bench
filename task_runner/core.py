"""Thin orchestrator for package entrypoint compatibility."""

from .ui import render_app


def bootstrap():
    """Run the Streamlit UI."""
    render_app()
