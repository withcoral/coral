-- ansible Coral source review test SQL
-- Focused checks for reviewer-raised concerns plus security and drift signals.

-- 1. Prove interfaces expose ipv6_addresses as queryable JSON, not a JSON-encoded string column.
SELECT
  hostname,
  interface,
  ipv6_addresses
FROM ansible.interfaces
ORDER BY hostname, interface;

-- 2. Prove expected systemd services match service_facts-style unit names.
SELECT
  r.hostname,
  r.role,
  r.expected_service,
  s.name AS observed_service,
  COALESCE(s.state, 'missing') AS observed_state,
  COALESCE(s.status, 'missing') AS observed_status
FROM ansible.roles r
LEFT JOIN ansible.services s
ON s.hostname = r.hostname
AND s.name = r.expected_service
WHERE r.expected_service IS NOT NULL
ORDER BY r.hostname, r.role;

-- 3. Prove important-service checks accept both base names and .service unit names.
SELECT
  h.hostname,
  h.service_mgr,
  s.name AS observed_service,
  COALESCE(s.state, 'missing') AS observed_state,
  COALESCE(s.status, 'missing') AS observed_status
FROM ansible.hosts h
LEFT JOIN ansible.services s
ON s.hostname = h.hostname
AND LOWER(s.name) IN (
'nginx',
'nginx.service',
'postgresql',
'postgresql.service',
'rabbitmq-server',
'rabbitmq-server.service',
'datadog-agent',
'datadog-agent.service',
'ssh',
'ssh.service',
'sshd',
'sshd.service'
)
ORDER BY h.hostname, observed_service;

-- 4. Prove SELinux policy is a policy name, not a numeric policy version.
SELECT
  hostname,
  selinux_status,
  selinux_mode,
  selinux_policy
FROM ansible.security
WHERE selinux_policy IS NOT NULL
ORDER BY hostname;

-- 5. Confirm no normalized security row reports collected SSH host key values.
SELECT
  hostname,
  ssh_host_keys_collected
FROM ansible.security
WHERE ssh_host_keys_collected = true
ORDER BY hostname;

-- 6. Complex drift and risk check for agent-facing triage.
WITH role_service AS (
SELECT
  r.hostname,
  r.role,
  r.expected_service,
  COALESCE(s.state, 'missing') AS service_state,
  COALESCE(s.status, 'missing') AS service_status
FROM ansible.roles r
LEFT JOIN ansible.services s
ON s.hostname = r.hostname
AND s.name = r.expected_service
WHERE r.expected_service IS NOT NULL
),
mount_risk AS (
SELECT
  hostname,
  mount,
  ROUND((1.0 - CAST(size_available AS DOUBLE) / CAST(size_total AS DOUBLE)) * 100, 2) AS used_percent,
  ROW_NUMBER() OVER (
    PARTITION BY hostname
    ORDER BY (1.0 - CAST(size_available AS DOUBLE) / CAST(size_total AS DOUBLE)) DESC
  ) AS mount_rank
FROM ansible.mounts
WHERE size_total > 0
)
SELECT
  h.hostname,
  h.distribution,
  h.service_mgr,
  rs.role,
  rs.expected_service,
  rs.service_state,
  rs.service_status,
  sec.selinux_mode,
  sec.firewall_hint,
  mr.mount AS riskiest_mount,
  mr.used_percent
FROM ansible.hosts h
LEFT JOIN role_service rs
ON rs.hostname = h.hostname
LEFT JOIN ansible.security sec
ON sec.hostname = h.hostname
LEFT JOIN mount_risk mr
ON mr.hostname = h.hostname
AND mr.mount_rank = 1
WHERE LOWER(rs.service_state) NOT IN ('running', 'started')
ORDER BY h.hostname, rs.role;

-- 7. Confirm all advertised Ansible tables are registered in the Coral catalog.
SELECT
  table_name
FROM coral.tables
WHERE schema_name = 'ansible'
ORDER BY table_name;
