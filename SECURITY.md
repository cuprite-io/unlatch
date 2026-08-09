# Security Policy

## Supported Versions

We currently support and provide security updates for the following versions:

| Version | Supported          |
| ------- | ------------------ |
| v0.x.x  | :white_check_mark: |

## Reporting a Vulnerability

We take the security of Unlatch seriously. If you believe you have found a security vulnerability, please do not open a public issue or Pull Request. Doing so exposes all active users to zero-day exploits.

Instead, please report vulnerabilities privately:
1. Navigate to the **Security and quality** tab of this repository on GitHub.
2. Click **Advisories**, then **New draft security advisory**.
3. Provide a detailed description, proof of concept, and impact details.

We follow coordinated disclosure and will work with you to patch the issue privately before releasing a public security advisory.

## Built-in Security Features

Unlatch is designed to safely handle concurrent access and prevent common denial-of-service vulnerabilities:
- **Preallocated Overflow Pool limits**: Chained overflow link buckets are allocated from a preallocated pool. This bounds initial heap growth, preventing unbounded memory allocation under key hash collision attacks.
- **Load Factor Control**: Enforces customizable maximum load factor thresholds to trigger table resizing before collision rates degrade map lookup performance.
- **Wait-Free Lookup Protection**: Read paths do not write to shared memory, which prevents denial-of-service (DoS) attacks targeting cache-line invalidation thrashing.
