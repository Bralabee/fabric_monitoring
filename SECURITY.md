# Security Policy

## Supported Versions

We provide security updates for the following versions:

| Version | Supported          | End of Support |
| ------- | ------------------ | -------------- |
| 0.3.x   | :white_check_mark: | Current        |
| 0.2.x   | :white_check_mark: | June 2026      |
| 0.1.x   | :x:                | Ended          |
| < 0.1   | :x:                | Ended          |

## Reporting a Vulnerability

We take security seriously. If you discover a security vulnerability, please follow these steps:

### 1. **DO NOT** Create a Public Issue
Security vulnerabilities should not be disclosed publicly until a fix is available.

### 2. Report Privately
Send details to the security team via one of these methods:

- **Preferred**: Create a private security advisory on GitHub
  - Navigate to the Security tab
  - Click "Report a vulnerability"
  - Fill out the form with details

- **Alternative**: Email the maintainers directly
  - Subject line: `[SECURITY] Brief description`
  - Include detailed information (see below)

### 3. Provide Detailed Information
Include in your report:

```markdown
## Vulnerability Description
[Clear description of the vulnerability]

## Affected Components
- Component: [e.g., Smart Merge algorithm, Authentication module]
- Version(s): [e.g., 0.2.0, 0.1.15]
- Environment: [e.g., Fabric Environment, Local, Docker]

## Impact Assessment
- Severity: [Critical/High/Medium/Low]
- Attack Vector: [Local/Network/Physical]
- Exploitability: [Easy/Moderate/Difficult]
- Impact: [Confidentiality/Integrity/Availability]

## Reproduction Steps
1. Step one
2. Step two
3. ...

## Proof of Concept
[Code/commands to demonstrate the vulnerability]
[DO NOT include actual exploits that could harm systems]

## Suggested Fix
[Optional: Your thoughts on how to fix it]

## Discoverer Credit
[Your name/alias if you want to be credited]
```

### 4. Response Timeline
- **Acknowledgment**: Within 48 hours
- **Initial Assessment**: Within 7 days
- **Fix Development**: Depends on severity (1-30 days)
- **Public Disclosure**: After fix is released and users have time to update

---

## Security Best Practices

### For Users

#### 1. Credential Management
```bash
# ✅ GOOD: Use environment variables
export FABRIC_TOKEN="your-token-here"
export TENANT_ID="your-tenant-id"

# ❌ BAD: Hardcoding credentials
token = "abc123-hardcoded-token"  # Never do this!
```

#### 2. .env File Security
```bash
# Always use .env for local development
cp .env.template .env
chmod 600 .env  # Restrict file permissions

# Ensure .env is in .gitignore
echo ".env" >> .gitignore
```

#### 3. Token Rotation
- Rotate service principal secrets every 90 days
- Use Azure Key Vault for production deployments
- Never commit tokens to version control
- Revoke tokens immediately if compromised

#### 4. Logging Considerations
```python
# ✅ GOOD: Mask sensitive data in logs
logger.info(f"Authenticated as {user_id[:4]}****")

# ❌ BAD: Logging full tokens
logger.debug(f"Token: {token}")  # Never log credentials!
```

#### 5. Network Security
- Use TLS/HTTPS for all API calls (enforced by default)
- Validate SSL certificates (never disable verification)
- Use private endpoints when available in Fabric

#### 6. Least Privilege Principle
```python
# Use minimal required permissions
# ✅ GOOD: Read-only service principal for monitoring
# ❌ BAD: Admin service principal when read-only would suffice
```

---

### For Developers

#### 1. Dependency Management
```bash
# Regularly update dependencies
pip list --outdated

# Check for known vulnerabilities
pip-audit

# Review dependency changes
git diff pyproject.toml
```

#### 2. Input Validation
```python
# Always validate user input
def process_workspace_id(workspace_id: str) -> str:
    """Validate and sanitize workspace ID."""
    if not re.match(r'^[a-f0-9\-]{36}$', workspace_id):
        raise ValueError("Invalid workspace ID format")
    return workspace_id.lower()
```

#### 3. SQL Injection Prevention
```python
# ✅ GOOD: Parameterized queries
cursor.execute("SELECT * FROM activities WHERE id = ?", (activity_id,))

# ❌ BAD: String formatting
cursor.execute(f"SELECT * FROM activities WHERE id = '{activity_id}'")
```

#### 4. Path Traversal Prevention
```python
# ✅ GOOD: Validate and normalize paths
from pathlib import Path

def safe_file_path(user_input: str, base_dir: Path) -> Path:
    """Ensure file path is within allowed directory."""
    requested_path = (base_dir / user_input).resolve()
    if not requested_path.is_relative_to(base_dir):
        raise ValueError("Path traversal attempt detected")
    return requested_path
```

#### 5. Authentication Testing
```python
# Always test authentication failures
def test_authentication_fails_with_invalid_token():
    with pytest.raises(AuthenticationError):
        client = FabricClient(token="invalid-token")
        client.get_workspaces()
```

#### 6. Secrets in Tests
```python
# ✅ GOOD: Use mock credentials in tests
@patch('fabric_client.get_token')
def test_api_call(mock_token):
    mock_token.return_value = "mock-token-for-testing"
    # Test implementation

# ❌ BAD: Using real credentials in test files
test_token = "real-production-token-here"  # Never!
```

---

## Known Security Considerations

### 1. Service Principal Permissions
The monitoring system requires specific Fabric permissions:

**Required Minimum Permissions**:
- `Workspace.Read.All` - Read workspace metadata
- `Item.Read.All` - Read item details
- `Capacity.Read.All` - Read capacity information

**Optional Permissions** (for enforcement):
- `Workspace.ReadWrite.All` - Modify workspace access

⚠️ **Warning**: Grant only necessary permissions. Use separate service principals for monitoring vs. enforcement.

### 2. Data Exposure
The system collects and stores:
- User email addresses
- Workspace names and GUIDs
- Item names and types
- Activity timestamps
- Error messages (may contain technical details)

**Mitigation**:
- Restrict access to generated reports
- Implement data retention policies
- Consider anonymizing user data in non-production environments
- Encrypt data at rest (use Azure Storage encryption)

### 3. Rate Limiting
The Fabric APIs have rate limits:
- 180 requests per hour (default)
- Aggressive polling may trigger throttling

**Mitigation**:
- Built-in rate limiting (configurable)
- Exponential backoff on 429 errors
- Monitor `API_RATE_LIMIT_REQUESTS_PER_HOUR` setting

### 4. Token Expiration
Service principal tokens expire:
- Default: 1 hour
- Long-running operations may fail

**Mitigation**:
- Implement token refresh logic
- Use Azure Managed Identity when possible
- Monitor authentication failures

---

## Compliance & Privacy

### GDPR Considerations
If deploying in regions subject to GDPR:

1. **Data Minimization**: Only collect necessary activity data
2. **Right to Erasure**: Implement data deletion procedures
3. **Data Portability**: Reports are in standard CSV/JSON formats
4. **Consent**: Ensure users are aware of monitoring
5. **Data Processing Agreement**: Review Microsoft Fabric terms

### Audit Logging
The system creates audit trails:
- API access logs (`logs/monitor_hub_pipeline.log`)
- Enforcement actions (`exports/enforcement_audit.json`)
- User activity reports

**Recommendation**: 
- Retain logs for compliance requirements (e.g., 90 days)
- Implement log rotation
- Secure log storage with appropriate access controls

---

## Security Checklist for Deployment

### Pre-Deployment
- [ ] Review all configuration files for hardcoded secrets
- [ ] Ensure .env is not committed to repository
- [ ] Validate service principal has minimum required permissions
- [ ] Test authentication failure scenarios
- [ ] Review generated reports for sensitive data exposure

### Deployment
- [ ] Use Azure Key Vault or equivalent for secret management
- [ ] Enable encryption at rest for data storage
- [ ] Configure network security (private endpoints if available)
- [ ] Implement rate limiting
- [ ] Set up monitoring and alerting

### Post-Deployment
- [ ] Monitor authentication logs for anomalies
- [ ] Regularly review access patterns
- [ ] Update dependencies monthly
- [ ] Conduct quarterly security reviews
- [ ] Test disaster recovery procedures

---

## Security Tools & Commands

### Scan for Secrets
```bash
# Install truffleHog
pip install truffleHog

# Scan repository
trufflehog git file://. --only-verified
```

### Dependency Vulnerability Scanning
```bash
# Install safety
pip install safety

# Check for known vulnerabilities
safety check --full-report

# Or use pip-audit
pip-audit
```

### Static Code Analysis
```bash
# Install bandit
pip install bandit

# Run security linting
bandit -r src/
```

---

## Incident Response Plan

### If Credentials Are Compromised

1. **Immediate Actions** (0-15 minutes)
   - Revoke compromised service principal/token
   - Rotate all related secrets
   - Notify security team

2. **Assessment** (15-60 minutes)
   - Determine scope of exposure
   - Review access logs for suspicious activity
   - Identify affected systems

3. **Remediation** (1-4 hours)
   - Deploy new service principal
   - Update all deployments with new credentials
   - Verify system functionality

4. **Post-Incident** (1-7 days)
   - Document incident
   - Review security procedures
   - Implement additional controls if needed
   - Communicate to stakeholders

### If Vulnerability Is Exploited

1. **Containment**
   - Isolate affected systems
   - Disable vulnerable components

2. **Eradication**
   - Apply security patch
   - Remove malicious artifacts

3. **Recovery**
   - Restore from clean backups
   - Verify system integrity

4. **Lessons Learned**
   - Root cause analysis
   - Update security procedures
   - Additional training if needed

---

## Contact Information

### Security Team
- **GitHub Security**: [Create Private Advisory](https://github.com/BralaBee-LEIT/fabric_monitoring/security/advisories/new)
- **Email**: Contact project maintainers via GitHub Issues (private security advisories preferred)

### Responsible Disclosure
We appreciate responsible disclosure and will:
- Acknowledge your contribution
- Keep you informed of progress
- Credit you in release notes (with your permission)
- Potentially offer bug bounty (if program exists)

---

## Updates to This Policy

This security policy is reviewed quarterly and updated as needed.

- **Last Review**: February 6, 2026
- **Next Review**: May 6, 2026
- **Policy Version**: 1.1

---

**Thank you for helping keep USF Fabric Monitoring secure!** 🔒
