# Prometheus Authentication Methods Guide

## Basic Authentication
```yaml
prometheus:
  base_url: "http://prometheus:9090"
  auth:
    username: "admin"
    password: "secret"
```

Basic authentication sends credentials with each request using HTTP Basic Auth. Ideal for simple setups but transmits credentials with every request.

**Security Considerations:**
- Always use HTTPS to encrypt credentials in transit
- Regularly rotate passwords
- Use strong passwords
- Consider rate limiting to prevent brute force attempts

## Token Authentication
```yaml
prometheus:
  base_url: "http://prometheus:9090"
  auth:
    token: "eyJhbGciOiJIUzI1NiIsIn..."
```

Bearer token authentication uses a pre-shared token in the Authorization header. Common in modern APIs and service-to-service communication.

**Security Considerations:**
- Tokens should be long and randomly generated
- Implement token expiration
- Use different tokens for different environments
- Store tokens securely using environment variables or secrets management
- Consider using JWT tokens for additional security features

## Certificate Authentication (mTLS)
```yaml
prometheus:
  base_url: "https://prometheus:9090"
  auth:
    cert_path: "/path/to/client.crt"
    key_path: "/path/to/client.key"
    verify_ssl: true
```

Mutual TLS authentication uses client certificates for bidirectional authentication. Most secure option, especially for production environments.

**Security Considerations:**
- Keep private keys secure and never share them
- Use strong key algorithms (e.g., RSA 2048+ bits)
- Implement proper certificate rotation
- Maintain a certificate revocation list (CRL)
- Store certificates and keys in secure locations with proper permissions
- Consider using a certificate management system for automation

## No Authentication
```yaml
prometheus:
  base_url: "http://prometheus:9090"
```

Running without authentication is only suitable for:
- Local development environments
- Internal networks with other security measures
- Testing purposes

**Security Considerations:**
- Never use in production
- Implement network-level security if used
- Consider using a reverse proxy for additional security layers

## Configuration Notes

1. Authentication methods are mutually exclusive:
```yaml
# INVALID - Multiple auth methods
auth:
  username: "admin"
  password: "secret"
  token: "eyJhbGciOiJIUzI1NiIsIn..."
```

2. Required pairs must be provided together:
```yaml
# INVALID - Missing password
auth:
  username: "admin"

# INVALID - Missing cert_path
auth:
  key_path: "/path/to/client.key"
```

3. SSL verification can be disabled (not recommended):
```yaml
auth:
  cert_path: "/path/to/client.crt"
  key_path: "/path/to/client.key"
  verify_ssl: false  # Only for testing/development
```
