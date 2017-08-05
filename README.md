# AWS Lambda function to backup Consul Snapshots to S3 (Snapper)

### Description
Snapper is an AWS Lambda Function that through the [Consul API] (https://www.consul.io/api/snapshot.html) downloads a Consul Snapshot which includes all state managed by Consul's Raft consensus protocol and stores it in S3

TLDR; AWS Lambda Function that makes backups of Consul snapshots and stores them in S3.

**Supports:**

* SSL
* Encrytion context for decrypting Consul ACL Token
* Path definitions

### Dependencies
 
 * Python 3.6
 * Boto
 
### Workflow
 
 1. Get Consul token
 2. Download Snapshot
 3. Upload Snapshot to S3

### Maintainers
Mikhael Santos
