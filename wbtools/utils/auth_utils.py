"""
Authentication utilities for AGR API access.

This module provides authentication token generation using AWS Cognito
via the agr_cognito_py library. The functions are re-exported for
backward compatibility with code that was using the old Okta-based
authentication.

Required environment variables for Cognito authentication:
- COGNITO_ADMIN_CLIENT_ID
- COGNITO_ADMIN_CLIENT_SECRET
- COGNITO_TOKEN_URL
- COGNITO_ADMIN_SCOPE
"""

from agr_cognito_py import (
    get_admin_token as get_authentication_token,
    generate_headers
)

__all__ = ['get_authentication_token', 'generate_headers']